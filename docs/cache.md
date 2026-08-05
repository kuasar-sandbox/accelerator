# cache — 分层内容缓存

`cache-ctl` 是项目里**跨 sandbox 共享**的内容缓存层。它把客户端反复
读取的 chunk 在节点本地与节点间集群里重复使用,把远端 S3-compatible
object storage 的热路径收敛到本地 RocksDB BlockCache。同一份二进制,
通过 YAML 配置切换三种运行形态(`local` / `shard` / `tiered`),
适配不同部署场景。

## 1. 概述

### 1.1 解决的问题

Agent 应用的镜像(1–5 GiB)和内存快照(~512 MiB)存储在远端 S3-compatible
object storage,直接拉取的延迟和带宽不可承受——万级并发同时拉取,聚合
带宽超过网络承载。

| 维度 | 无缓存 | 有缓存 |
|---|---|---|
| 镜像冷启动 | 秒级(全量拉取) | <500 ms(按需加载,缓存命中单次 IO) |
| 快照恢复 | 数百 ms | <80 ms(L1/L2 命中) |
| 网络带宽 | 线性增长 | 仅首次 miss 产生远端流量 |
| 远端对象存储成本 | 随并发线性增长 | 流量降 >99% |

99.9% 整体命中率(L1 内存 + L2 磁盘联合)意味着每 1000 次缓存查询最多 1
次穿透到远端对象存储。5 TiB 磁盘容量范围内,任何请求最多 1 次随机 IO
(Index 与 Bloom Filter 全部常驻内存)。

### 1.2 设计原则

- **数据面**:自定义 wire 协议(39 B 请求头 / 8 B 响应头,6 个 opcode)。
- **控制面**:`health_listen` 独立端口同时跑两个 gRPC 服务——标准 `grpc.health.v1.Health`
  探活,以及 `cac.cache.v1.Info`(`Get` 拉运行时计数快照)。
- **监听地址**:`listen` / `health_listen` 均取 `host:port`(TCP)或一个 Unix
  socket 路径(`/run/sandbox/cache.sock` 或 `unix:///...`;为 socket 时启动清死
  socket、chmod 0600)。数据面客户端 `cache.endpoint` 填同址即可;控制面经 socket
  时,`ping` / `info --endpoint` 用 `unix:///` 形式。
- **本地存储**:进程内 RocksDB,或通过 UDS/TCP 访问 Redis-compatible server;
  后者的配置、取消语义和外部 Dragonfly 部署示例见 [cache-redis.md](cache-redis.md)。
- **写入语义**:强制准入,无应用层 LRU/SLRU。淘汰由 CompactionFilter 在
  后台按 CMS 频率统计驱动。
- **填充语义**:fill-aside,读路径上隐式回填上层。

## 2. 命令行接口

### 2.1 `cache-ctl serve`

```
cache-ctl serve --config FILE
```

`--config` 与 `CACHE_CONFIG` 必有其一(flag 优先);两者皆缺则报错。
YAML 示例见 §3。

### 2.2 `cache-ctl config`

```
cache-ctl config show     [--config <path>]
cache-ctl config generate
```

`generate` 输出带注释模板(默认 tiered:rocksdb L1 + store origin)。

### 2.3 `cache-ctl object` — 完整对象操作

连 `local` 或 `tiered` 端点(数据端口,典型 7070):

```
cache-ctl object get --endpoint host:port --namespace chunk --hash HEX
cache-ctl object put --endpoint host:port --namespace chunk --hash HEX --value FILE|-
```

`--endpoint` 与 `CACHE_ENDPOINT` 二选一;`get` 输出到 stdout;
`--value -` 从 stdin 读。`put` 不可连 tiered(拒绝写,§3.1)。

### 2.4 `cache-ctl shard` — EC 分片操作

连 `shard` 端点:

```
cache-ctl shard get --endpoint host:port --namespace chunk --hash HEX
cache-ctl shard put --endpoint host:port --namespace chunk --hash HEX --idx N --total M --value FILE|-
```

`get` 不带分片编号——返回该 peer 实际持有的分片(`idx`/`total` 打到 stderr,
分片数据到 stdout)。`put` 把裸分片数据按 `[idx][total]` 前缀封装后写入
(`--total` 默认 5,须 `--idx < --total`),是单 peer 的调试入口;真实写入由
fill-aside 经 `EncodePrefixed` 完成。

### 2.5 `cache-ctl ping` / `info`

```
cache-ctl ping --endpoint host:port            # gRPC 健康探测 (--endpoint 指向 health_listen)
cache-ctl info --endpoint host:port [--json]   # 实时 stats(同 health_listen);--json 输出原始 JSON
cache-ctl info --rocks-path PATH               # 离线只读打开 RocksDB,查看属性
```

`ping` / `info --endpoint` 都指向**控制面**端口(`health_listen`,典型 7071),
**不是**数据端口,都不走 wire 协议。两者调不同 gRPC 服务:`ping` 调
`grpc.health.v1.Health/Check`;`info --endpoint` 调 `cac.cache.v1.Info/Get`,把运行时
计数快照拉回来(`--json` 输出原始 JSON,否则人类可读表格)。
`info --rocks-path` 走 RocksDB secondary instance(只读并行打开),不打扰
运行中的 cache-ctl。

### 2.6 `cache-ctl bench`

内置吞吐/延迟基准,通过 wire 协议向运行中的 cache-ctl 发压。用于快速
smoke 验证;不替代 `test/scripts/bench_cache.sh`(后者加 taskset CPU 固
定 + 参数扫描)。

```
cache-ctl bench --endpoint host:port [flags]

Flags:
  --concurrency int         (default 8)
  --duration duration       Go duration (default 10s)
  --value-size int          (default 262144)  # 256 KiB
  --mode string             "get" | "put" | "mixed";默认空,--prefill-endpoint 为空时解析为
                            "mixed",否则强制 "get"(显式传 put/mixed + 独立 prefill 端点报错)
  --namespace string        "chunk" | "manifest"
  --prefill int             get/mixed 模式预写对象数 (default 1000)
  --prefill-endpoint string 独立预写端点(默认同 --endpoint)
  --info-endpoint value     Info gRPC 端点(HealthListen)，可重复；第一个端点同时用于 bench 目标预热判定
  --access string           "seq" | "uniform" | "zipf" 读访问模式 (default "seq")
  --zipf-s float            Zipf 偏斜指数 s (>1,越大越偏;仅 --access zipf) (default 1.1)
  --cold-prefill int        额外只写 prefill 端点、不暖 bench 目标的冷 key 数(喂 L2-miss → L3)
  --miss-ratio float        命中冷 key 的读比例 → L2 miss → 透传 origin/L3 (需 --cold-prefill>0 + --prefill-endpoint)
  --timeout duration        客户端 per-op TCP deadline (default 10s;慢/卡 origin 大数据集 prefill 须调大)
  --cpu-profile string      bench 窗口内写 CPU profile 到文件
  --heap-profile string     bench 窗口结束后写 heap profile 到文件
  --trace string            bench 窗口内写执行 trace 到文件
```

#### 基准方法学(L2 内存/磁盘路径、L3 透传、aging)

多机端到端压测见 `test/scripts/bench_cache_remote.sh`(部署 N shard + origin + tiered,跑并发扫);
资源瓶颈归因配 `test/scripts/procmon.sh`(无依赖 /proc 采样 cpu/diskstats/net)+ `proc_analyze.py`。要点:

- **测「L2 命中(内存)」vs「L2 落盘」**靠工作集与 BlockCache(= `rocks.disk_bytes × mem_ratio`)之比控制:
  工作集 ≪ BlockCache → 全 RAM 命中;工作集 ≫ BlockCache(调小 `mem_ratio`)→ shard rocksdb 真实磁盘随机读。
- **`--access`**:`uniform` 把读均摊到整个工作集 → 暴露磁盘路径;`zipf` 模拟真实热点偏斜——但
  **偏斜过强会把热集塞进 BlockCache、反而掩盖磁盘路径**,测盘须用 `uniform`(或工作集远大于 cache)。
- **陷阱:EC tier 命中率 ≠ 命中 RAM**。EC hit% 只表示数据在 shard 集群里;BlockCache miss 时磁盘读
  发生在 shard rocksdb 内部、对该计数不可见。**判定是否落盘必须看 shard 主机的磁盘 IOPS(procmon `rd_iops`)**,不能看 hit%。
- **L3 透传建模**:`--cold-prefill N`(只写 origin、不暖 L2)+ `--miss-ratio f`,约 f 比例的读命中冷 key →
  L2 miss → 透传 origin。冷池要 > 压测窗内的冷读次数,否则 read-through 把重复冷读暖回 L2、实测透传率低于注入值。
- **测 aging/磁盘淘汰**:淘汰是频率式(见 §4.5,丢 `freq ≤ threshold` 的冷 key),**与 `disk_bytes` 容量无关**。
  短压测(总 ops ≪ `freq.reset_after`)不会触发——sketch 不衰减、无「冷」key;须用长窗 + 偏斜访问让冷尾衰减到阈值。
- **两个瓶颈区(结构性,与具体硬件无关)**:① 全 BlockCache 命中时,`value-size × EC 数据分片`扇入会先打满
  **tiered 节点网卡**(吞吐随并发饱和、延迟按 Little 定律线性增);② 工作集溢出落盘时,**shard 磁盘随机读**
  成为瓶颈,且经 EC `k`-of-`n` 同步等待放大尾延迟。两端 CPU 通常都不是瓶颈。容量规划核心 = 让热集驻留
  BlockCache——命中与落盘的吞吐、p99 可差一个数量级。

## 3. 配置

### 3.1 三形态对比

| 维度 | local | shard | tiered |
|---|---|---|---|
| 定位 | 单节点完整对象 KV | EC 分片 KV(集群成员) | 多级缓存代理 |
| handler 后端 | embedded RocksDB / Redis-compatible | embedded RocksDB / Redis-compatible | TieredCache(§4.6) |
| tier chain | 无 | 无 | 按 YAML `tiers:` 顺序 |
| 写操作 | 直写所选 backend | 直写所选 backend | 拒绝(`StatusError`) |
| 读操作 | 查 RocksDB | 查 RocksDB | 穿 tier chain,命中后 fill-aside |
| origin 凭证 | 不需要 | 不需要 | 需要(L3 读权限) |
| 典型部署 | 测试/调试/单节点 | L2 EC 集群 × 5 | 与 manifest-ctl 同节点(边车) |

### 3.2 local 模式

```yaml
mode: local
type: embedded
listen: 0.0.0.0:7070           # wire 数据面;host:port 或 Unix socket(/run/sandbox/cache.sock 或 unix:///...)
health_listen: 0.0.0.0:7071    # gRPC 健康检查(可省);同支持 Unix socket 路径
stats_interval: 30s            # 周期自适应统计行(§6.6);缺省 30s,"0"/"off" 关闭
rpc_timeout: ""                # 服务端每请求 wall-clock 上限。缺省/空/非法 = 0 =
                               # 无 per-request deadline:请求只受客户端连接 /
                               # 调用方取消约束。显式写 Go duration(如 "2s")才设上界

freq:
  counters: 8M                 # CMS sketch 4-bit 计数器数 (8M ≈ 4 MiB)
  reset_after: 1M              # 每 1M 次 Touch 后 halving
  reset_interval: 1h           # 低流量兜底:每小时 halving
  evict_threshold: 1           # CompactionFilter 淘汰阈值
  persist_interval: 5m

rocks:
  path: /var/cache/accel-l1
  disk_bytes: 1TiB             # BlockCache = disk_bytes × mem_ratio
  mem_ratio: 0.01              # 1% → 10 GiB
  direct_reads: true
  bloom_bits: 15
```

### 3.3 shard 模式

```yaml
mode: shard
type: embedded
listen: 0.0.0.0:7070
health_listen: 0.0.0.0:7071
rpc_timeout: 2s

freq:
  counters: 32M
  reset_after: 10M
  reset_interval: 6h
  persist_interval: 5m

rocks:
  path: /mnt/ssd/accel-l2
  disk_bytes: 5TiB
  mem_ratio: 0.08              # 8% → 400 GiB
  direct_reads: true
  block_size: 128KiB
  bloom_bits: 15
  write_buffer_bytes: 512MiB
  max_background_jobs: 16
```

### 3.4 tiered 模式

```yaml
mode: tiered
listen: 0.0.0.0:7070
health_listen: 0.0.0.0:7071
rpc_timeout: 2s

freq:
  counters: 8M
  reset_after: 1M
  reset_interval: 1h
  persist_interval: 5m

# tiers 数组的顺序即 tier chain 的查找顺序。最多一个 embedded。
tiers:
  - type: embedded             # 进程内嵌 RocksDB (L1)
    rocks:
      path: /var/cache/accel-l1
      disk_bytes: 1TiB
      mem_ratio: 0.01
      direct_reads: true
      bloom_bits: 15

  - type: ec                   # EC 客户端 → shard 集群
    cluster:
      data_shards: 4           # RS k=4(空/≤0 → 默认 4;见下「分片数自动收敛」)
      parity_shards: 1         # RS m=1 → 总 5 分片,25% 开销(空/≤0 → 默认 1)
      peers:
        - {id: l2-01, endpoint: 10.0.1.11:7070}
        - {id: l2-02, endpoint: 10.0.1.12:7070}
        - {id: l2-03, endpoint: 10.0.1.13:7070}
        - {id: l2-04, endpoint: 10.0.1.14:7070}
        - {id: l2-05, endpoint: 10.0.1.15:7070}
      pool: 2                  # 每 peer 并行 TCP 连接数(wire)
      timeout: 2s

origin:
  type: store                  # store | upstream(见下文「upstream 层」);cache-ctl 不直接访问文件系统
  store:
    endpoint: 10.0.1.50:7100   # store-ctl gRPC endpoint
    pool: 4                    # 独立 grpc.ClientConn 数(round-robin)
    timeout: 2s
  max_inflight: 16             # 到 origin 的最大并发 RPC 数
```

`origin.type: store` 把 cache-ctl 的 L3 fallback 指向一个 store-ctl 守护
进程——所有 origin miss 都通过 gRPC `Get` 流式拉回。cache-ctl 进程**没有
任何**文件系统读写权限,所有持久化都集中在 store-ctl。origin 另一合法值是
`type: upstream`(指向另一台 cache-ctl wire 端点,见下「upstream 层」)。

#### 分片数自动收敛(clamp)

EC 每个对象恰好放 `data + parity` 个分片(一片一 peer,Maglev 定位),因此
`data + parity` 不能超过 `len(peers)`——否则每次 Get 都在路由阶段失败
("need N nodes but only M")。构造 EC tier 时(`data≤0→4`、`parity≤0→1` 默认补全
之后)若发现 `data + parity > len(peers)=n`,自动把方案收敛到 n 个 peer 并打一条
WARN,**优先保住 parity(容错)、缩小 data**:

- `parity < n` → `data = n - parity`(保留配置的 parity);
- `parity ≥ n` → `data = 1, parity = n-1`(parity 单独都放不下,退化为最大冗余;
  `n == 1` 时即 `1+0`,无冗余单 peer 直通——分片缺失就是 miss,读写仍正常);
- `data + parity ≤ n` 原样保留(每对象扇出小于 peer 数是合法且可能有意为之:
  对象散布在 peer 子集上做负载均衡,每次 Get 读的分片更少 → EC 尾延迟放大更小)。

#### upstream 层(可选)

`type: upstream` 把另一台 cache-ctl 作为 tier chain 的一层——典型用法
是把同机房的 L2 聚合节点放在 embedded 之后、EC 之前,让热集群先在聚合
节点上收敛,减少对 shard 集群的读压力。**upstream endpoint 指向的远端
cache-ctl 必须运行在 `local` 模式**:tiered/shard 模式拒绝 Put,fill-aside
backfill 第一次失败才会被 TieredCache 视作"该层 miss"静默吞掉——读链
路仍可用,但 upstream tier 永远不被填充,等于退化成只读代理。loud failure
点在第一次 backfill,不在启动握手。

```yaml
tiers:
  - type: embedded
    rocks: { path: /var/cache/accel-l1, disk_bytes: 1TiB, mem_ratio: 0.01 }

  - type: upstream             # 远端必须是 local 模式
    endpoint: 10.0.1.50:7070
    pool: 4                    # reader / writer 各自的 ConnPool
    timeout: 2s
```

### 3.5 关键参数

| 参数 | 说明 |
|---|---|
| `listen` | wire 数据面 TCP 监听 |
| `health_listen` | gRPC 控制面监听,同端口跑两个服务:`grpc.health.v1.Health`(探活)+ `cac.cache.v1.Info`(`Get` 拉计数快照)。省略则两者都不启动 |
| `stats_interval` | 周期自适应 stderr 统计行的基准周期(§6.6)。缺省/空 = 30s(默认开);`0`/`off` 关闭。有流量的周期打一行(吞吐/带宽/时延 p50/p99/max/并发/命中级联/rocks 量规),空闲周期静默 |
| `freq.disable_eviction` | bool。关掉频率式 compaction-filter 淘汰:sketch 仍维护(供 stats),但 filter 永不挂载,任何 key 都不会按访问计数被淘汰。用于某台 local cache-ctl 充当下游 tiered 的 origin(bench 场景)——写入落一次就必须留住 |
| `pool` | 到单个 peer 的并行 TCP 连接数。wire 是 sync request/response,单连接会把并发请求串行化 |
| `max_inflight` | 到该 tier 的同步查询并发上限;`0` 表示不限制。异步 fill 使用各 backend 自身的连接池/并发控制。Redis tier 禁止设置,应使用 `redis.get_pool` / `redis.set_pool` |
| `rpc_timeout` | 服务端每请求 wall-clock 上限。**缺省/空/非法 = 0 = 无 per-request deadline**:请求只受客户端连接 / 调用方取消约束,不强加任意值。显式设有限值时,超时返回 `StatusError`,TieredCache 视作该层 miss 继续下一层。卡死请求的可观测性改由 `CACHE_CTL_DEBUG` 追踪(§6.5) |
| `timeout`(tier/origin) | 客户端对该 tier / origin 单次 RPC 的 wall-clock 上限。同 `rpc_timeout` 语义:缺省/空 = 0 = 不设上界,只受调用方 ctx / 连接约束 |
| `pprof_listen` | 非空时另起一个 HTTP listener 暴露 `/debug/pprof/*`(如 `127.0.0.1:6060`)。**生产留空**;仅离线诊断临时开启(§6.4) |

## 4. 设计

### 4.1 总体架构

```
   ┌─ manifest-ctl ──────────────────────────────────────────────────────────────┐
   │    load:    fetch  ── wire ObjectGet ───────────────┐                       │
   │    store:   ingest ── store gRPC Put ──┐            │                       │
   └────────────────────────────────────────┼────────────┼───────────────────────┘
                                            │            │
                              store gRPC    │            │  wire ObjectGet
                                            ▼            ▼
                       ┌─ store-ctl ──────────┐    ┌─ cache-ctl  tiered ──────────┐
                       │   gRPC server        │    │   wire server                │
                       │      │               │    │      │                       │
                       │      ▼               │    │      ▼                       │
                       │   fs / s3 backend    │    │   TieredCache                │
                       │   __meta/generations │    │     tier 0   embedded  (L1)  │
                       └──────────▲───────────┘    │     tier 1   ec ──────┐      │
                                  │                │     origin ──┐        │      │
                                  │                └──────────────┼────────┼──────┘
                                  │  origin (gRPC Get)            │        │
                                  └───────────────────────────────┘        │  wire ShardGet/Put × 5
                                                                           ▼
                                                              ┌─ cache-ctl  shard ──┐
                                                              │   wire server       │   × 5 nodes
                                                              │   RocksDB           │   (RS 4+1, Maglev)
                                                              └─────────────────────┘
```

cache-ctl 不参与写入路径——所有 Put 由 manifest-ctl → store-ctl 直接走;
读路径上 fill-aside 由 TieredCache 在内存中隐式触发(不再走自己的 wire
入口)。

### 4.2 Wire 协议

数据面采用自定义二进制协议,替代 gRPC + protobuf,减少帧开销与 cgo 边界
拷贝。

#### 帧布局

所有整数 little-endian。

```
Request  (固定 39 B 头 + 可选 Value):
  0   TotalLen     u32   整帧字节数
  4   Opcode       u8    0x01 ObjectGet / 0x02 ObjectPut
                          0x03 ShardGet  / 0x04 ShardPut / 0x05 Ping
                          0x06 CancelRequest(取消在途请求)
  5   Namespace    u8    0x01 chunk / 0x02 manifest / 0x03 blob(Ping 忽略)
  6   Flags        u8    保留
  7   Hash         32 B  SHA256(ciphertext);Ping 时全 0
  39  Value        可变  仅 Put 携带;Shard* 时 Value 首 2 B 为
                          [idx][total] 前缀,后接 shard_data

Response (固定 8 B 头 + 可选 ErrMsg + 可选 Value):
  0   TotalLen     u32
  4   Status       u8    0x00 Hit / 0x01 Miss / 0x02 Error / 0x03 Cancelled
  5   Reserved     u8
  6   ErrLen       u16   仅 Status=Error 时 > 0
  8   ErrMsg       UTF-8 文本,仅 Error
  +   Value        仅 Hit 时携带
```

| 常量 | 值 |
|---|---|
| `RequestHeaderSize`  | 39 B |
| `ResponseHeaderSize` | 8 B |
| `MaxFrameSize`       | 4 MiB |

`MaxFrameSize` 是单帧硬上限(头 + 载荷)。服务端与客户端读帧前均校验
`TotalLen ≤ MaxFrameSize`,超限视作协议错误直接断连。

#### 错误模型

**没有** gRPC 风格的错误码枚举。任何可恢复错误(RocksDB 故障、ctx 超时、
tier chain 全 miss、tiered 收到 Put)都返回 `StatusError` + 文本
`ErrMsg`。客户端按语义做三类映射:

| Status | wire 客户端 | TieredCache |
|---|---|---|
| `StatusHit` | `value, hit=true, nil` | 命中,fill-aside 上层并返回 |
| `StatusMiss` | `nil, hit=false, nil` | 向下一层查询 |
| `StatusError` | `nil, false, err(ErrMsg)` | **视作该层 miss**,继续下一层 |

把 `StatusError` 当成该层 miss,确保 tier chain 的可用性不被单层瞬时故障
拉低——下一层只要能响应,读请求整体就能成功。

#### 请求取消(CancelRequest / StatusCancelled)

客户端可在请求在途时发一个 `CancelRequest`(0x06)帧,服务端据此提前中止
正在处理的请求并回 `StatusCancelled`(0x03):取消会 cancel 该请求的处理
ctx(命中 ctx 的后端——tier chain 远端跳、origin IO——随之中断),已在该连接
上排队的待处理请求也一并回 `StatusCancelled`。这是 EC hedge 取消慢 peer 的
机制——`data` 个分片先到即可解码,其余在途 ShardGet 被取消,不必干等。

#### 不提供的操作

- **Delete**:内容寻址下不删除,淘汰由 §4.4 频率 sketch + CompactionFilter
  异步处理。
- **AdminService**:无独立 admin RPC。运行时计数可通过控制面 `cac.cache.v1.Info/Get`
  按需拉(pull-only,见 §1.2 / §6.3),另有默认开启的周期自适应 stderr 统计行
  (§6.6,`stats_interval`);离线诊断用 `cache-ctl info --rocks-path` 直接打开
  RocksDB(secondary instance 模式)。
- **Streaming**:所有请求/响应都是 one-shot;Get/Put 的 value 上限由
  `MaxFrameSize` 限制,超大对象必须在上层分片(EC tier 的职责)。

### 4.3 Key 编码与 opcode 分派

| Opcode | Wire Hash | RocksDB Key | Value | 说明 |
|---|---|---|---|---|
| `ObjectGet/Put` | 32 B SHA256(ciphertext) | `hash[32]` | raw object | 完整对象 |
| `ShardGet/Put`  | 32 B hash | `hash[32] + 0x00` (33 B) | `[1 idx][1 total][data]` | RS 分片 |

Wire 帧头只携带 32 字节 `Hash`——Object 与 Shard 都不需要告诉服务端"哪
个 idx",因为 Shard 的 idx/total 元数据写进了 Value 前缀。RocksDB 层通
过 33 字节 ShardKey 与 32 字节 ObjectKey 在长度上天然隔离,共用 CF 也不
冲突。

把 shard 身份(idx, total)放进 Value 而不是 Key,是为了**集群成员变更
时让幸存节点上的现有分片立即可用**——客户端不再需要"peer X 持有 idx Y"
的位置假设。

### 4.4 RocksDB 调优

| 参数 | 默认 | 说明 |
|---|---|---|
| `disk_bytes` | 1 TiB | 预期磁盘上限 |
| `mem_ratio` | 0.01 | BlockCache = disk_bytes × mem_ratio(最小 64 MiB) |
| `block_size` | 64 KiB | SST data block 粒度 |
| `bloom_bits` | 15 | 每 key Bloom Filter 位数 → 假阳性 ~0.001% |
| `compression` | none | 关闭(密文熵高,压缩无效) |
| `use_direct_reads` | true | 绕过 OS Page Cache |
| `write_buffer_bytes` | 256 MiB | memtable 大小 |
| `max_background_jobs` | 8 | 后台 flush + compaction worker |
| `compaction` | leveled | 读优化 |
| `pin_l0_filter_and_index` | true | Bloom + Index 常驻 BlockCache |

#### Column Family 隔离

chunk / manifest / blob 各用独立 CF:

- **chunk CF**:高频写入(fill-aside),大 value(256 KiB 级);
- **manifest CF**:低频写入,value 几 KiB 到若干 MiB;
- **blob CF**:任意内容寻址数据(store 的第三 partition,见 [`store.md`](store.md)
  §4.6),机制同 chunk——同样的 BlobDB / Bloom / compaction filter,跟着
  generation 一起淘汰。

独立 CF 让各类数据有独立 write buffer / Bloom / compaction 节奏,避免稀
有的 manifest 写入干扰高频 chunk compaction。

#### BlobDB 与写放大

三个 CF 都启用 RocksDB BlobDB,固定参数(不暴露 YAML):

- `min_blob_size = 4 KiB`:小于阈值的 value 内联到 SST,大于的旁路到独立
  `.blob` 文件。
- `blob_file_size = 256 MiB`:每个 blob 文件大小上限。
- BlobDB GC 开启:后台回收无引用的 blob 文件。

命中 blob 的大 value 在 LSM 里只留 ~50 bytes 的 key + blob-ref。写入以顺
序 append 为主,Compaction 只合并 key 层——写放大降至 1–3x(否则纯
LSM 是 10–30x)。

#### DirectReads

默认开启 `UseDirectReads`,绕过 OS Page Cache 直接从磁盘读。

- **L1 边车部署**:与 manifest-ctl 同节点;DirectReads 使 L1 的 I/O 完
  全自管(通过 BlockCache),不抢占节点其他进程的 Page Cache 份额。
- **L2 专用节点**:BlockCache 已占节点 RAM 的 80%,DirectReads 避免 data
  block 在 BlockCache + Page Cache 双重缓存。

### 4.5 频率统计与磁盘淘汰

5 TiB SSD 终究会满。Go 进程内维护一个 Count-Min Sketch(CMS),Get/Put
时 `Touch(key)` 累加访问频率;RocksDB 后台 Compaction 时调用注册的
CompactionFilter,filter 通过 CGO 回调 `ShouldEvict(key)` 查询 sketch——
低于阈值(默认 1)的 key 被物理删除。

```
every Get/Put:
    sketch.Touch(key)                   # O(1), 4-bit counter increment

every reset_after Touches OR every reset_interval (first wins):
    sketch.Reset()                      # halve all counters

RocksDB background compaction (filter on both chunk + manifest CF):
    for each key in SST:
        if sketch.ShouldEvict(key):
            return Remove
```

#### 恢复期保护

filter 在 DB Open 时挂到 CF Options,但此时 sketch 还没构造。CompactionFilter
内部有 `armed` 门闩,Open 期间 `armed=false`,Filter() 返回 preserve——
即使 startup 触发 recovery compaction 也不误删。sketch 构造完毕、从
`__freq_sketch__` 保留 key 恢复状态后,调用 `Arm()` 开闸。

#### 冷启动保护

进程重启后 CMS sketch 归零。如果不做保护,首次 Compaction 会把所有 key
(频率 = 0 ≤ evict_threshold = 1)全部删除。

保护:定期(默认 5 分钟)将 CMS sketch 序列化到 RocksDB 保留 key
`__freq_sketch__`。重启时自动恢复。

#### CMS 默认参数

| 参数 | L1 | L2 |
|---|---|---|
| `counters` | 8M | 32M |
| `reset_after` | 1M | 10M |
| `reset_interval` | 1h | 6h |
| `evict_threshold` | 1 | 1 |
| `persist_interval` | 5m | 5m |

#### 不做应用层 SLRU/LRU

Go 进程**不**维护 SLRU、LRU 等队列:

- 避免双重缓存(Go heap + RocksDB BlockCache,2× 内存浪费);
- GC 压力(256 KiB chunk 在 Go heap 中频繁分配/释放,GC 暂停影响 P99);
- RocksDB BlockCache 已提供 LRU 热度保护,CompactionFilter 提供磁盘淘汰。

Go 进程内存控制在 < 200 MiB(CMS sketch + 原子计数器 + wire 连接池)。

### 4.6 TieredCache 与读路径

```
manifest-ctl load
   │
   ▼
fetch.Fetcher.OpenManifest() → Stream.ReadAt()
   │
   ├─ binary-search manifest index for chunk range
   ├─ spawn N goroutines
   │   └─ per goroutine:
   │       ├─ client.ObjectGet(key) ─── wire ───► cache-ctl tiered
   │       │                                    │
   │       │                                    ▼
   │       │                               TieredCache.Get(ctx, p, key)
   │       │                                    │
   │       │                                    ├─ tier 0: embedded.Get(key)
   │       │                                    │   HIT → return ciphertext
   │       │                                    ├─ tier 1: ec.Get(key)
   │       │                                    │   5 × wire ShardGet
   │       │                                    │   ≥4 arrive → RS Reconstruct
   │       │                                    └─ origin: store.Get(key)
   │       │                                        HIT → return + fill-aside upper tiers
   │       ├─ crypto.Decrypt(key, ciphertext)
   │       └─ send to result channel
   │
   └─ read result channel in order, write out
```

### 4.7 fill-aside

读路径上 layer i 命中时,回填所有**更上层**的 cache tier(序号 < i):

```
TieredCache.Get hit at layer i  (layer 0..N-1 = tiers, N = origin):
    upper = min(i, N)
    for j := upper-1; j >= 0; j--:
        startFill(j, partition, key, blob)   # async, fire-and-forget
```

**Blob 生命周期**:startFill 不共用调用方的 blob,而是 **Clone** 出独立
handle 给 fill goroutine,`defer cloned.Release()`。原 blob 由 Get 调用
方持有直到外层 wire 响应写完。两者引用计数独立。

**异步、fire-and-forget**:读路径不等写完成。fill 默认**无 deadline**——
goroutine 挂在 TieredCache 的 baseCtx 下,关停时 `Close()` 统一取消并回收(§6.2),
卡死的 origin 不会泄漏 fill goroutine。测试和预热流程通过实际 Get/ShardGet
确认数据已可读,不依赖内部 goroutine 是否暂时排空。**幂等**:重复 fill 无副作用(rocks Put 覆盖,wire ShardPut
覆盖)。

#### EC 分片缺失修复

```
ec.Get(key):
    5 × wire ShardGet, concurrent
    ├─ 5 ok:           RS Reconstruct → HIT
    ├─ 4 ok + 1 miss:  Reconstruct → HIT, backfill missing shard async
    └─ 3 ok + 2 miss:  cannot rebuild → MISS; on origin HIT fill-aside
                       re-encodes all 5 shards
```

### 4.8 强制准入语义

Put / PutShard 永远成功写入 RocksDB,**不**做 admission filter。

理由:在本系统中,Set 由 fill-aside 触发——每次写入都代表一个刚被读取
的有效 chunk。引入 admission filter(如经典 W-TinyLFU)可能拒绝刚被读
取的 chunk,导致下次读仍穿透到 L3——与"按需加载、逐步升温"目标矛盾。

空间回收靠 §4.5 频率统计 + CompactionFilter,不靠 admission。

### 4.9 EC 客户端

EC 客户端只在 tiered 的 tier chain 中使用。

#### Reed-Solomon 4/5

- 数据分片 k=4,校验分片 m=1,总 5 分片;
- 任意 4 个分片可重建(容忍 1 节点故障);
- 存储开销 25%(vs 三副本 200%),延迟分布改善 ~20%。

#### Padding

原始 chunk 大小可能不被数据分片数(k)整除:

1. 写入 4 字节 little-endian 长度前缀;
2. Pad 到 k 的倍数;
3. RS Split → k 个数据分片;
4. 重建后按长度前缀截断。

#### Maglev 一致性哈希

分片到节点的映射用 Maglev 查找表算法。与经典一致性哈希环对比:

| 维度 | 经典环(150 VN) | Maglev |
|---|---|---|
| 查找 | O(N × log ring) | O(M log M)(1 次哈希 + 表遍历) |
| 均衡性 | ~8% 标准差 | ±1 表项(M=5 时 <0.01%) |
| 增减节点重映射 | ~1/N | ~1/M(接近理论最小) |
| 外部依赖 | 三方库 | 无——自实现 ~150 行 |

| 参数 | 值 |
|---|---|
| `TableSize` | 65537(质数) |
| 哈希 | FNV-1a(标准库);构建 2 个独立 FNV,查找 1 次 FNV |

#### 集群成员变更

EC router 持有 `atomic.Pointer[state]`,state = `(epoch, peers, maglev_table)`。
epoch 单调递增,**只**由显式配置变更(YAML + SIGHUP)递增——连接池瞬时
故障不改 epoch,不改 placement。这是防抖动核心保证:节点 50 ms 闪断不
触发全局 placement 重算。

加节点:编辑 YAML → `kill -HUP` → 新 Maglev 表生效。约 1/M 的 (key, idx)
迁移到新节点,其他 peer 命中正常。
减节点:同上。LocateN 不再返回被移除 peer。

**操作规程:一次只动 1 个 peer**。同时换 ≥2 个 peer 单 key miss 数超
parity(RS(4+1) parity=1),读路径 fallthrough origin——正确但慢。

### 4.10 跨形态混发的行为

wire handler 对所有 opcode 都响应,因此 `ObjectPut` 发到 shard 模式节点
**不会**被协议层拦截,会真正写入该节点的 RocksDB。这是有意设计——
`shard` 的语义是部署意图而非协议约束。真正需要防误写的是 tiered 模式,
由 §3.1 表中"写操作:拒绝"分支兜底。

## 5. 内存预算

### 5.1 L1(tiered 进程内嵌 embedded tier)

| 区域 | 1 TiB / 1% ratio | 100 GiB / 1% ratio |
|---|---|---|
| Go heap | < 200 MiB | < 200 MiB |
| RocksDB Index + Bloom (PinL0) | ~5 GiB | ~500 MiB |
| RocksDB BlockCache | 10 GiB | 1 GiB |
| OS Page Cache | 0(DirectReads) | 0 |
| **总** | **< 16 GiB** | **< 1.7 GiB** |

### 5.2 L2(shard 专用节点)

500 GiB RAM / 5 TiB SSD 机型:

| 区域 | 预算 |
|---|---|
| Go heap | < 50 MiB |
| RocksDB Index + Bloom (PinL0) | ~50 GiB |
| RocksDB BlockCache | ~400 GiB |
| Go runtime + wire 连接缓冲 | ~10 GiB |

**Index/Bloom 常驻论证**:5 TiB / 256 KiB ≈ 20M keys,每 key Bloom 15 bits
+ Index ~30 bytes ≈ 50 bytes/key → ~1 GiB Bloom + ~600 MiB Index per CF。
两 CF 留余量 → 50 GiB 上限。Bloom 全内存确保 miss 路径零磁盘 IO;Index
全内存确保 hit 路径单次 IO 直达 SST data block。

## 6. 运维

### 6.1 启动

```bash
# shard 节点(L2 集群成员)
cache-ctl serve --config /etc/cache/shard.yaml

# tiered 节点(与 manifest-ctl 同节点)
cache-ctl serve --config /etc/cache/tiered.yaml
```

### 6.2 信号处理

`SIGINT` / `SIGTERM` 关闭序列:

1. 若启用控制面,先把 gRPC health 状态置 `NOT_SERVING`(让编排器立刻
   下线);
2. wire server graceful stop:排空 in-flight,硬上限 5 s;
3. gRPC 控制面 server graceful stop(Health + Info 同在该 server);
4. tiered 模式调 `TieredCache.Close()` 取消在途 async fill;tier chain 反序
   级联关闭(embedded → redis → ec → upstream),local/shard 模式下关闭所选 backend。

### 6.3 运行时计数(pull-only)

精确累计计数按需经控制面 `cac.cache.v1.Info/Get` 拉取(`health_listen` 端口,见
§1.2)。`cache-ctl info --endpoint host:port` 即取一次快照(`--json` 出原始
JSON,否则人类可读表格),含 server hits/misses/fills、各 tier 与 origin 计数、
EC 各 peer 计数、embedded/local/shard 的 RocksDB CF 属性。bench 脚本用同一服务
取 bench 窗口前后的 delta。日常**观察**则看 §6.6 的周期统计行(默认开),无需主
动拉。

### 6.4 离线诊断

```bash
cache-ctl info --rocks-path /var/cache/accel-l1
```

RocksDB secondary instance 模式打开 DB,输出 property(estimate-num-keys、
disk usage、compaction stats)。**与运行中的 cache-ctl 共存**,不需停 daemon。

`pprof_listen` 非空时(§3.5)另起 `/debug/pprof/*` HTTP listener,可
`go tool pprof` 抓 CPU / heap / goroutine。生产留空,仅排障临时开启。

### 6.5 慢/卡请求追踪(`CACHE_CTL_DEBUG`)

`rpc_timeout` 缺省不设上界(§3.5)后,卡死的后端不再 fail-fast 而是静默
stall。env 门控的操作 tracer 把它变可观测:

```bash
CACHE_CTL_DEBUG=1 cache-ctl serve --config cache.yaml       # 开启
CACHE_CTL_SLOW=2s CACHE_CTL_DEBUG=1 cache-ctl serve ...      # 自定慢阈值
```

- `CACHE_CTL_DEBUG` truthy 时启用,否则零开销(每请求一次 atomic 读)
- **只有超过慢阈值**(`CACHE_CTL_SLOW`,Go duration,默认 1 s)的请求打一行
  (WARN);快请求静默——稳态吞吐/时延看 §6.6 的周期统计行,这里只盯异常长尾
- 后台 reporter 周期 dump **仍在飞**且超阈值的请求(op 名 + 已卡时长),
  卡死的 tier / origin 立即可见,不必等 deadline
- 覆盖 wire 服务端请求处理(一次请求一个 op;tier 链遍历与 origin 回源都在其内)

另有 `CACHE_CTL_TIMING=1`(进程启动时读)开启 EC.Get 的分阶段耗时日志
(LocateN / fan-out / decode 各段 + 各分片到达偏移),按 1% 采样,默认关、热
路径零开销。专测 EC hedge 的扇入与尾延迟,与上面的 `CACHE_CTL_DEBUG` 正交。

两者生产默认关闭;与 `pprof_listen`(§6.4)互补——tracer 看"哪些请求慢/卡",
pprof 看"卡在哪段代码"。

### 6.6 周期自适应统计行(`stats_interval`)

daemon 默认每 30 s(`stats_interval`,§3)向 stderr 打一行运行时统计,**仿
sandbox-ctl 的自适应输出**:有流量的周期打一行汇总,无流量的周期**静默**,启动
后先以 2 s 快采样捕捉冷启突发,空闲两拍后退回基准周期。`stats_interval: 0`/`off`
关闭。一行含:

- **吞吐**:get / put 的每秒速率(自适应单位 1.2k/3.4M)
- **带宽**:get 出向、put 入向字节速率(MiB/s)
- **命中**:本周期 server `hit%`;tiered 模式附命中级联(`hits L0 88% L1 6%
  origin 6%`,见各层命中占比)
- **时延分布**:get / put 各自 p50 / p99 / max(窗口直方图,桶同 sandbox-ctl)
- **并发**:`inflight`(在飞 get+put)、`conns`(当前 wire 连接数)
- **RocksDB 量规**:各 CF 的 keys / live-data-size / 运行中 compaction 数
- **错误**:本周期返回 `StatusError` 的请求数(`err`,0 时省略)

示例:

```
cache stat tiered | get 5.1k/s 620MiB/s p50 40µs/p99 700µs/max 9ms · hit 94% · put 220/s 30MiB/s p50 1.1ms/p99 8ms/max 40ms | inflight 18 conns 6 | hits L0 88% L1 6% origin 6% | rocks chunk 1.2M keys/3.4GiB
```

与 §6.3 的 pull-only Info 互补:统计行给稳态全貌(默认开、自适应、低噪),Info
给精确累计快照(bench / 脚本按需拉);与 §6.5 的 `CACHE_CTL_DEBUG` 正交(后者
只打异常长尾)。

## 7. 性能特征

延迟目标(实测基线与已采纳优化见 `orchestrator/release-builder/docs/perf.md` §1):

| 指标 | P50 | P99 |
|---|---|---|
| L1 SSD hit | <500 µs | — |
| L1 BlockCache hot hit | <100 µs | — |
| L2 hit | 550 µs | <4 ms |
| L3 hit (fs origin) | <50 ms | <200 ms |

命中率目标:L1 >65%,L2 >99.9%,L1+L2 联合 >99.95%。

## 8. See Also

- [`store.md`](store.md) — tiered 模式 origin = `store`(store gRPC 客户端)或
  `upstream`(另一台 cache-ctl);store 形态下 cache-ctl 自身**没有**任何文件系统
  读写权限,所有持久化集中在 store-ctl;store-ctl 亦可经 `cache_listen` 以**只读**
  方式直接讲本 wire 协议(chunk/manifest/blob,纯透传无 L1),省掉独立 cache-ctl
- [`manifest.md`](manifest.md) — manifest-ctl 通过 wire ObjectGet 调 cache-ctl
  tiered;Manifest 内 chunk hash = 这里的 wire Hash 字段
- `orchestrator/release-builder/docs/perf.md` §1 — cache 子系统实测延迟/吞吐基线与优化记录
- 仓根 `README.md` / `Makefile` — 构建:`make cache-ctl`(CGO,自动
  `deps-rocksdb` 后静态链 librocksdb;本仓唯一 CGO 二进制)
- `orchestrator/release-builder/docs/kuasar-sandbox.md` §4.5 — 缓存模型与命中率目标
