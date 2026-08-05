# store — 持久层读写代理

`store-ctl` 是项目里**唯一**对持久层后端(本地文件系统 / S3-compatible
object storage backend)读写的进程。所有数据进出最终都汇聚到这里:
`manifest-ctl` 通过 gRPC 把
chunk 与 Manifest 推过来,`cache-ctl tiered` 在 origin miss 时通过 gRPC
拉过来。store-ctl 负责后端抽象与数据面收发;generation 生命周期与运维(即 GC 与代管理控制平面)经其 admin 子命令承载。

## 1. 概述

### 1.1 为什么需要单独的 store 进程

跨 sandbox / 跨 manifest-ctl / 跨 cache-ctl 进程的物理字节,必须收敛到
**单一进程**写入,才能同时满足:

- **唯一写者**:fs / s3 后端的物理字节写入需要协调,否则 dedup 元数据
  (`__meta/generations`)与 staged 临时文件可能被并发修改坏掉;
- **后端可替换**:本地开发用 fs,远端使用 S3-compatible object storage
  backend,manifest-ctl / cache-ctl 不该感知差异;切换后端不能改客户端代码;
- **代次管理**:租户隔离靠 generation 划分(同代去重、跨代隔离),线上
  要支持"开新代"和"清退旧代"两个低频但关键的操作;
- **完整性可验证**:写入路径要能在服务端重算 ContentKey,拒绝 hash 算错
  的请求,免得污染整代去重域。

### 1.2 在系统拓扑中的位置

```
┌─ manifest-ctl ─┐       ┌─ cache-ctl tiered ─┐
│  store client   │       │  origin: store      │
└───────┬─────────┘       └───────┬─────────────┘
        │                         │
        └───────┬─────────────────┘
                │  gRPC (Put/Get/GetSalt)
                ▼
        ┌── store-ctl ────────┐
        │  fs / s3 backend     │
        │  __meta/generations  │
        └──────────────────────┘
```

### 1.3 设计原则

1. **后端抽象**:fs 与 s3 共享 `Backend` 接口(`Get / Put / OpenPut /
   Exists / ActiveGeneration`),客户端代码无差别;两者的 generation 维护、
   Get 多代回退、verify 语义完全一致,差异仅限物理后端。
2. **admin / serve 分离**:generation 生命周期(`init / rollout / purge`)
   由专用 admin 子命令承担,**不**塞进 yaml 字段或 serve 启动逻辑;serve
   只读后端 meta 决定 active,从不修改。
3. **fail-fast 而非自愈**:serve 遇到未初始化仓库直接报 `ErrUninitialised`
   + 提示 `run store-ctl init`,不静默创建——让运维错误立刻浮现。
4. **标准配置与凭据链**:`endpoint` / `bucket` 在 yaml 中显式声明;字符串
   字段支持 `${VAR}` 展开;静态 AK/SK 都为空时交给 AWS SDK 默认凭据链。
5. **单写者 + CAS**:s3 的 generation 元数据写入用 S3 条件头
   (`If-None-Match` / `If-Match`)防并发覆盖,但稳态运行仍假设 per-bucket
   单 store-ctl 实例做写入。

## 2. 命令行接口

### 2.1 公共 flag

```
Global Flags:
  --config string         YAML 配置文件路径(覆盖 STORE_CONFIG 环境变量)
```

`--config` 与 `STORE_CONFIG` 至少需要一项指向有效 YAML;两者皆缺则报错
(`config generate` 子命令除外)。所有子命令共享同一份 yaml(§3)。

### 2.2 命令矩阵

| 命令 | 用途 | 要求已 init | 修改状态 |
|---|---|---|---|
| `serve`   | 启动 gRPC daemon | 是 | 否(读) |
| `init`    | 在空仓库写入起始 generation | 否 | 是(创建 meta) |
| `rollout` | 追加新 generation 并设为 active | 是 | 是(改 meta) |
| `purge --generation G` | 删除一个非 active generation | 是 | 是(改 meta + 删数据) |
| `purge --all --confirm` | 彻底清空整个仓库 | 是 | 是(全删) |
| `info`    | 打印 generations 与对象数 | 是 | 否(读) |
| `config show` / `generate` | 配置检查/模板 | 否 | 否 |

`serve` 不自动 init;遇到未初始化仓库报 `ErrUninitialised`。

### 2.3 `store-ctl init`

```
store-ctl init --config FILE --generation G
```

写入第一个 generation(典型 `G1`)。**不可重入**,meta 已存在则报错。
整库重置必须 `purge --all --confirm` 后再 `init`。

### 2.4 `store-ctl serve`

```
store-ctl serve --config FILE
```

绑定 yaml `listen:` 接受 gRPC。`listen` 取 `host:port`(TCP)或一个 Unix
socket 路径(`/run/sandbox/store.sock` 或 `unix:///run/sandbox/store.sock`)——
为 socket 时,启动会建好父目录、清掉**上次遗留的死 socket**(仅当该路径本身是
无人监听的 socket;活动 socket 报 "address already in use",非 socket 路径绝不
删)、bind 后 chmod 0600(节点内同用户访问);客户端 `store.endpoint` 填同一路径
即可(裸路径或 `unix:///` 形式皆可,见 manifest.md §3)。active generation 在
daemon 生命周期内**不变**,换代见 §2.5。

yaml 若设了 `cache_listen`(§3.4),serve 还会在该地址额外起一个**只读** cache
wire server(§4.7),让用 cache 客户端的组件经 wire 协议直读 store 内容。

### 2.5 `store-ctl rollout`

```
store-ctl rollout --config FILE --generation G
```

追加新 generation 并设为 active(`G` 必须不存在)。**需要重启 serve** 才
能让运行中的 daemon 看到新 active。换代是控制平面操作,频率每代/月级别;
serve 不订阅 meta 变化避免引入额外 watcher。

### 2.6 `store-ctl purge`

```
store-ctl purge --config FILE --generation G    # 删除一个非 active 旧代
store-ctl purge --config FILE --all --confirm   # 清空整个仓库
```

- `--generation`:拒绝删 active(`ErrCannotDropActive`);未知名报
  `ErrGenerationNotFound`。顺序是先 CAS 改 meta,再删数据树。中途失败再次
  drop 同名因 NotFound 短路,数据残余可手动清。
- `--all`:必须配 `--confirm`(无 dry-run,先停 serve 再操作)。s3 上是
  `ListObjects` + 逐个 `DeleteObject`,几十万对象级别会跑分钟级。删后仓
  库变 uninitialised。

### 2.7 `store-ctl info`

```
store-ctl info --config FILE
```

只读,可与运行中的 serve 共存。打印:

- backend 类型(fs / s3)
- root / bucket+prefix
- active generation
- generations 列表
- 每代的 chunk / manifest / blob 对象数

### 2.8 `store-ctl config`

```
store-ctl config show     [--config <path>]
store-ctl config generate
```

`generate` 输出带注释的 daemon 配置模板(默认 fs backend)。

## 3. 配置

### 3.1 fs backend

```yaml
listen: 127.0.0.1:7100
backend: fs
stats_interval: 30s              # 周期自适应统计行(§5.7);缺省 30s,"0"/"off" 关闭
cache_listen: ""                 # 可选:非空则额外起只读 cache wire server(§3.4 / §4.7)
fs:
  root: /var/store               # 文件系统根目录(必填,init 时创建结构)
  verify_content_key: true       # 默认 true
```

### 3.2 S3-compatible object storage backend

```yaml
listen: 127.0.0.1:7100
backend: s3
s3:
  endpoint: https://example-s3-endpoint
  region: us-east-1
  bucket: kuasar-store
  prefix: production
  access_key: ${S3_ACCESS_KEY}
  secret_key: ${S3_SECRET_KEY}
  verify_content_key: true
  max_inflight: 64
  op_timeout: 10s
  max_object_size_bytes: 16777216
```

`endpoint` 与 `bucket` 必须在 yaml 中显式配置,`prefix` 可选。`region` 显式值
优先;未配置时固定为 `us-east-1`,不会从 endpoint hostname 推导。所有 S3
字符串字段都支持 `${VAR}` 展开。

静态 `access_key` / `secret_key` 必须同时提供;二者都为空时使用 AWS SDK
默认 credential provider chain。最小配置因此仍需给出存储位置:

```yaml
listen: 127.0.0.1:7100
backend: s3
s3:
  endpoint: https://example-s3-endpoint
  bucket: kuasar-store
```

Huawei OBS 可通过其 S3-compatible endpoint 使用;它仍然是 endpoint 服务,
不是独立 backend 类型。

### 3.3 verify_content_key

- `true`(默认):服务端重算 SHA256 并与客户端传的 ContentKey 比对;不一
  致 → 拒绝 + 清理 staged 数据。
- `false`:服务端信任客户端的 key,跳过 hash 重算;仅适合受信批量加载,
  不推荐生产常开。

### 3.4 cache_listen

可选(缺省空 = 关闭)。非空时,`serve` 在该地址(`host:port` 或 Unix socket,
解析规则同 `listen`)额外起一个**只读 cache wire server**:直接以
[`cache.md`](cache.md) §4.2 的二进制 wire 协议对外服务 chunk / manifest / blob
三个 partition 的对象读,数据直取后端——让用 cache 客户端的组件无需单独部署
cache-ctl 就能读到 store 内容。

```yaml
listen: 127.0.0.1:7100
backend: fs
cache_listen: 127.0.0.1:7101     # cache wire 协议(只读);与 listen 同解析规则
fs:
  root: /var/store
```

只读:`ObjectPut` 与所有 shard 操作一律拒绝(`writes not supported`);写仍走
store gRPC。它是**纯透传**——无 L1 缓存,每次 Get 直达后端;要收敛延迟仍按
§5.3 部署 cache-ctl tiered 以本 store 为 origin。设计细节见 §4.7。不引入额外
配置:idle / rpc 超时取固定默认(120s idle、无 per-rpc deadline)。

## 4. 设计

### 4.1 总体架构

```
┌──────── store-ctl 进程 ────────────────────────┐
│                                                  │
│  gRPC server (listen)                            │
│   └ StoreServer (Put/Get/GetSalt)               │
│                                                  │
│  Backend                                         │
│   ├ fs:  filepath I/O at root                   │
│   └ s3: aws-sdk-go-v2/s3                    │
│                                                  │
│  __meta/generations  ←──── single writer        │
└──────────────────────────────────────────────────┘
```

数据面与运维面分离:

- **数据面**:`serve` 在 `listen:` 端口注册 Store 这一个 gRPC 服务,含
  `Put`(客户端流)/ `Get`(服务端流)/ `GetSalt`,manifest-ctl / cache-ctl
  作为 gRPC 客户端连接;设了 `cache_listen` 时再在该端口起一个**只读** cache
  wire server(§4.7)——两者都是数据面。**没有**独立的 health listener
  (store-ctl 无 `health_listen` 字段);
- **运维面**:admin 子命令直接打开后端,**不**经过 gRPC,与运行中的 serve
  共存(读同一份 meta)。

### 4.2 Backend 接口

server 层的最小契约,fs 与 s3 都实现:

```
Get(ctx, partition, key)             → (found bool, data []byte, err error)
Put(ctx, partition, key, data)       → (isNew bool, err error)
OpenPut(partition)                   → PutHandle (流式 Put)
Exists(partition, key) bool          // 仅查 active gen
ActiveGeneration() string
```

admin 操作(`Rollout` / `Drop` / `Wipe` / `GenerationStats`)是 Store 类型上
的额外方法,**不**在 Backend 接口里——server 不需要这些。

### 4.3 fs 后端

布局:

```
{root}/
├── __meta/
│   ├── generations            ← 文本文件,每行一个 generation 名(最旧在首)
│   └── tmp/                   ← 流式 Put 的临时文件 staging
├── chunk/
│   ├── G1/                    ← generation 1
│   │   ├── a1/b2/a1b2c3...    ← 文件名 = lowercase-hex(SHA256(client bytes))
│   │   └── ...
│   └── G2/
├── manifest/
│   ├── G1/
│   └── G2/
└── blob/
    ├── G1/
    └── G2/
```

`chunk` / `manifest` / `blob` 是三个**内容寻址 partition**,机制完全一致
(同一 ContentKey 寻址、generation 分代、dedup、verify),仅作逻辑隔离:
`chunk` / `manifest` 由 manifest-ctl 写入,`blob` 复用同一 store API 存任意
内容寻址数据(经 store gRPC 写;读经 store gRPC 或 §4.7 的 cache wire)。

**ContentKey** = `SHA256(bytes-as-submitted-by-client)`,**没有 salt 参与
寻址**。dedup 仍天然成立——客户端的 convergent encryption 用 store-ctl
`GetSalt` 提供的 salt 派生加密 key(详见 [`manifest.md`](manifest.md)
§4.5),同 plaintext + 同 salt → 同 ciphertext → 同 ContentKey → 同存储。

**Put 原子性**:写入走 `__meta/tmp/put-XXXX` 临时文件 + `os.Rename` 到
`chunk/<gen>/aa/bb/<hash>`;rename 是 POSIX 原子操作,故障也不会落出半写
文件。Server 启动时扫一遍 `__meta/tmp/` 删掉所有遗留 `put-*`。

**verify_content_key**:写入时若启用,服务端重算 SHA256 并与客户端 ContentKey
比对,不一致 → `ErrKeyMismatch` + 删 staged 文件。

### 4.4 S3-compatible object storage backend

布局完全对应 fs:

```
<bucket>/<prefix>/__meta/generations
<bucket>/<prefix>/chunk/<gen>/<aa>/<bb>/<hash>
<bucket>/<prefix>/manifest/<gen>/<aa>/<bb>/<hash>
<bucket>/<prefix>/blob/<gen>/<aa>/<bb>/<hash>
```

**写入路径**:S3 API 没有"流式 Put + atomic rename"原语,改用"内存缓冲 +
单次 PutObject"。chunk 大小天然 ≤1 MiB,缓冲不会膨胀;每个 PutHandle 独占
一个 `bytes.Buffer`,Commit 时一次 PutObject 上去。

**dedup short-circuit**:Put 前先 HEAD 检查 active gen 路径;命中则跳过
上传(对应 fs 的 stat 检查)。

**请求编码兼容性**:`sdkclient` 固定使用 AWS SDK 的
`RequestChecksumCalculationWhenRequired` 与
`ResponseChecksumValidationWhenRequired`。普通 PutObject 保持原始 body 与
Content-Length,不注入 flexible checksum trailer 或 `aws-chunked`;TLS 与 SigV4
仍由 SDK 正常处理。该策略是 backend 默认行为,不暴露额外配置项。

**meta 并发**:s3 的 `__meta/generations` 写入用 S3 条件头:

- `init`:`If-None-Match: *`(只在 key 不存在时创建)
- `rollout / drop`:`If-Match: <etag>`(只在 etag 未变时改)

CAS 失败 → 重读 + 重试,有界次数(默认 5)后报错。这保证多个 store-ctl
实例并发 init 同一桶时只有一个赢,但稳态运行仍假定单写者。

**defensive wrapper**:s3 backend 在 SDK 之上加一层信号量
(`max_inflight`)+ 可选 per-op 超时(`op_timeout`,缺省不设上界——只受调用方
ctx 约束,§3.2)+ 响应大小封顶(`max_object_size_bytes`),防止远端服务抖动导致
store-ctl OOM 或队头阻塞。`op_timeout` 不设时,慢/卡调用靠 `STORE_CTL_DEBUG`
追踪暴露(§5.6),而不是被一个武断的 deadline 提前杀掉。

### 4.5 配置与凭据解析(s3)

解析顺序只有三层:

1. yaml 显式值;
2. yaml 字符串中的 `${VAR}` 环境变量展开;
3. 当静态 AK/SK 都为空时,使用 AWS SDK 默认 credential provider chain。

endpoint 与 bucket 没有自动发现来源,展开后为空会立即报错。region 也不识别
厂商或 hostname;空值统一使用 `us-east-1`。这让私有 endpoint、代理 endpoint
及不同 S3-compatible 服务保持同一签名和配置行为。请求固定使用 path-style
bucket addressing,不要求自定义 endpoint 提供 wildcard bucket DNS 或证书。

### 4.6 Generation 模型

generation 是 store 内部的代次划分:**同代去重、跨代不共享**。每个
chunk / manifest / blob 对象按 `<partition>/<gen>/...` 路径存,active generation
是新写入的目标,旧 generation 仍可被反向 Get 找到。

`__meta/generations` 是 source of truth,文本文件(fs)或对象(s3):

```
G1
G2
G3        ← 最后一行 = active
```

#### 不支持的操作

- 把已删除的 generation"恢复":删了就是删了,decommissioning 是单向操作;
- 改 active 为旧 generation:rollout 只追加,不能"回退"。要恢复旧代行为
  应该开新一代再用 reverse-search Get 找老数据;
- 重 `init`:不可重入,meta 已在则报 `ErrAlreadyInitialised`。

#### 反向 Get

Get 请求按 newest-first 顺序扫所有已知 generation,首次命中即返回:

```
for gen in [G3, G2, G1]:
    if backend.read(partition, gen, key): return
return not found
```

旧 generation 写入的 chunk 在 active 切到新代后仍可读。`purge --generation`
后该代的数据真正消失。

`Exists`(用于写路径上的 dedup short-circuit)**不**做反向扫描——只看
active 路径。在旧代存在不算 dedup(因为旧代会被 purge,留下来的引用就
dangling)。

### 4.7 内嵌只读 cache wire server(`cache_listen`)

store-ctl 默认只讲 store gRPC。设了 `cache_listen`(§3.4)后,它在该端口**额外**
起一个 cache wire server——直接讲 [`cache.md`](cache.md) §4.2 的二进制 wire 协议,
与 cache-ctl 的数据面同一套。于是任何用 cache 客户端的组件都能从 store-ctl 直读
内容,前面无需再架一台 cache-ctl。

- **复用而非另写**:wire server 取 `pkg/cache/server` 的 `WireServer` +
  `CacheHandler`(该包已与 rocks 解耦、纯 Go,store-ctl 仍 `CGO_ENABLED=0`);
  后端经 `cache.NewStoreOrigin(backend)` 包成 `cache.Getter`,进程内直连,无回环
  gRPC。
- **只读**:外面再裹一层拒写 Tier——`ObjectGet` 透传到后端,`ObjectPut` 与所有
  shard 操作回 `writes not supported`。写入仍只经 store gRPC,单写者语义不变。
- **三 partition**:wire Namespace `0x01/0x02/0x03` 映射 chunk / manifest / blob,
  与 store gRPC 服务的对象集一致。
- **纯透传**:无 L1 缓存,每次 `ObjectGet` 直达后端(fs stat+read / s3 GET)。
  要收敛延迟仍按 §5.3 部署 cache-ctl tiered 以本 store 为 origin——`cache_listen`
  解决的是"少一个进程也能讲 cache 协议",不是缓存。
- **零额外配置**:idle / rpc 超时用固定默认(120s idle、无 per-rpc deadline),
  不新增 YAML 旋钮。

## 5. 部署与运维

### 5.1 首次部署

```bash
# 一次性 init
store-ctl init --config /etc/store-ctl.yaml --generation G1

# 长驻 serve
store-ctl serve --config /etc/store-ctl.yaml
```

### 5.2 横向扩展

数据面(`serve` 的 Get/Put)**完全横向扩展**,任意多 store-ctl 实例并发
指向同一 fs root / s3 bucket+prefix 都安全:

- **Put**:客户端先算 `key = SHA256(client_bytes)` 再 PUT,两个并发写者
  写同样内容 → 同 key → 同路径 → 同字节。fs 上 `os.Rename` 是原子覆盖
  且字节相同,s3 PutObject 同 key 是幂等的。dedup 是**寻址层面**成立的,
  不需要写者协调。
- **Get**:纯读,反向扫 generation 列表,跨实例无副作用。

**meta 写入有 CAS 保护**:

- **s3**:`init` 用 `If-None-Match: *`,`rollout / purge --generation` 用
  `If-Match: <etag>`。多个实例并发改同一 bucket 的 meta,冲突触发
  `PreconditionFailed` → 重读重试或返回错误。**永不丢更新**。
- **fs**:`writeGenerationsFile` 用 temp + rename 原子写,但**没有** CAS。
  两个 admin 进程并发 `rollout` 同一 root 可能 lost update。fs 部署典型
  是单机本地开发,生产用 s3。

**结论**:

- 多副本 store-ctl serve 同源 fs/s3:数据路径任意并发,生产可直接横向
  扩展;
- admin 命令(init/rollout/purge):s3 自然 CAS-safe 多实例并发;fs 需要
  外部协调成单 admin(脚本编排即可);
- 不需要 Active-Standby 选主或写者锁。

### 5.3 部署 cache-ctl tiered(s3)

远端 GetObject 延迟取决于具体服务与网络拓扑。生产部署应把 cache-ctl tiered
放在 store-ctl 前面(rocksdb L1 + store-ctl origin)以收敛 hit 路径延迟;
否则每次 miss 都直接承担远端对象存储时延。详见 [`cache.md`](cache.md)。

### 5.4 容量监控(s3)

按所选服务监控 bucket 容量、对象数量、请求配额与计费。chunk 路径下对象数
约等于唯一 chunk 数;容量推算见
`orchestrator/release-builder/docs/kuasar-sandbox.md` §7.3。

### 5.5 e2e 验证

```bash
make test-e2e        # = test-e2e-cache + test-e2e-cluster(fs 后端全链路)
```

`test/e2e/e2e_cache.sh` 以 fs 后端 store-ctl 为 sidecar,覆盖 manifest-ctl ↔
cache-ctl(local / shard / tiered)↔ store-ctl 全链路;`e2e_store_cache_listen.sh`
单独验证 store-ctl 的 `cache_listen` 只读 wire 服务(manifest 经 cache 协议字节级
比对 store gRPC、blob namespace 路由、拒写);`e2e_cluster_rolling.sh`
验证 EC 集群 SIGHUP 滚动换 peer 后读全部成功、且不穿透 store-ctl origin。
s3 核心语义由 `pkg/store/s3` 的 fake S3 单元测试覆盖;`sdkclient` 使用
`httptest.Server` 捕获真实 AWS SDK 请求,验证 SigV4、错误映射、分页以及普通
PutObject 不使用 flexible checksum trailer / aws-chunked。真实云端 endpoint
集成需要外部凭据,不包含在常规 CI 中。

### 5.6 慢/卡操作追踪(`STORE_CTL_DEBUG`)

`op_timeout` 缺省不设上界(§3.2)后,一个卡死的后端不再 fail-fast,而是
静默 stall。把这种 stall 变可观测的是一个 env 门控的操作 tracer:

```bash
STORE_CTL_DEBUG=1 store-ctl serve --config store.yaml      # 开启
STORE_CTL_SLOW=2s STORE_CTL_DEBUG=1 store-ctl serve ...     # 自定慢阈值
```

- `STORE_CTL_DEBUG` truthy(非空且非 `0`/`false`)时启用;否则零开销
  (每 op 一次 atomic 读)
- **只有超过慢阈值**(`STORE_CTL_SLOW`,Go duration,默认 1 s)的 op 打一行
  (WARN);快 op 静默——稳态吞吐/时延看 §5.7 的周期统计行,这里只盯异常长尾
- 后台 reporter 周期性 dump **仍在飞**且已超阈值的 op(op 名 + 已卡时长),
  这样 stall 在卡住的后端上立即可见,而不必等一个 deadline
- 覆盖 s3 的 get / head / put 调用

生产默认关闭;排障时临时开启,定位远端对象存储、凭据或网络导致的长尾。

### 5.7 周期自适应统计行(`stats_interval`)

daemon 默认每 30 s(`stats_interval`,§3)向 stderr 打一行运行时统计,**仿
sandbox-ctl 的自适应输出**:有流量的周期打一行汇总,无流量的周期**静默**,启动
后先以 2 s 快采样捕捉冷启突发,空闲两拍后退回基准周期。`stats_interval: 0`/`off`
关闭。一行含:

- **吞吐**:get / put / salt 的每秒速率(自适应单位 1.2k/3.4M)
- **带宽**:get 出向、put 入向字节速率(MiB/s),put 去重比例
- **时延分布**:get / put 各自的 p50 / p99 / max(窗口直方图,桶同 sandbox-ctl)
- **并发**:`inflight`(当前在飞 Get+Put 计数)
- **错误**:本周期返回 gRPC 错误的请求数(`err`,0 时省略)

示例:

```
store stat | get 1.2k/s 92%hit 180MiB/s p50 80µs/p99 900µs/max 4.1ms · put 340/s 410MiB/s dedup 22% p50 1.2ms/p99 14ms/max 60ms | inflight 7
```

与 §5.6 的 `STORE_CTL_DEBUG` 正交:统计行给稳态全貌(默认开),tracer 给异常
长尾(opt-in)。

## 6. See Also

- [`manifest.md`](manifest.md) — 客户端,通过 gRPC `Put`/`Get`/`GetSalt`
  与 store-ctl 交互;chunk 加密在客户端发生
- [`cache.md`](cache.md) — `tiered` 模式的 origin 是一个 store gRPC 客户端
  指向 store-ctl
- 仓根 `README.md` / `Makefile` — 构建:store-ctl 是纯 Go 二进制,
  `make store-ctl`(或 `make build`)产出
- `orchestrator/release-builder/docs/kuasar-sandbox.md` §7.3 — 存储模型与容量推算
