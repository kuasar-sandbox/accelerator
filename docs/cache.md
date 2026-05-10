# cache — 缓存系统设计

`cache-ctl` 是项目里**跨 sandbox 共享**的内容缓存层。它把客户端反复
读取的 chunk 在节点本地与节点间集群里重复使用,把热路径延迟从远端
OBS 的 ~50 ms 压到本地 RocksDB BlockCache 命中的 <100 µs。同一份二进
制,通过 YAML 配置切换三种运行形态(`local` / `shard` / `tiered`),
适配不同部署场景。

## 1. 概述

### 1.1 解决的问题

Agent 应用的镜像(1–5 GiB)和内存快照(~512 MiB)存储在远端 OBS,直接
拉取的延迟和带宽不可承受——万级并发同时拉取,聚合带宽超过网络承载。

| 维度 | 无缓存 | 有缓存 |
|---|---|---|
| 镜像冷启动 | 秒级(全量拉取) | <500 ms(按需加载,缓存命中单次 IO) |
| 快照恢复 | 数百 ms | <80 ms(L1/L2 命中) |
| 网络带宽 | 线性增长 | 仅首次 miss 产生远端流量 |
| OBS 成本 | 随并发线性增长 | 流量降 >99% |

99.9% 整体命中率(L1 内存 + L2 磁盘联合)意味着每 1000 次缓存查询最多 1
次穿透到 OBS。5 TiB 磁盘容量范围内,任何请求最多 1 次随机 IO(Index 与
Bloom Filter 全部常驻内存)。

### 1.2 一句话原则

- **数据面**:自定义 wire 协议(39 B 请求头 / 8 B 响应头,5 个 opcode)。
- **控制面**:gRPC 标准 health 探活独立端口。
- **存储引擎**:RocksDB(BlobDB 旁路大 value),关闭压缩(密文熵高)。
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
`--value -` 从 stdin 读。

### 2.4 `cache-ctl shard` — EC 分片操作

连 `shard` 端点:

```
cache-ctl shard get --endpoint host:port --namespace chunk --hash HEX --idx N
cache-ctl shard put --endpoint host:port --namespace chunk --hash HEX --idx N --value FILE|-
```

### 2.5 `cache-ctl ping` / `info`

```
cache-ctl ping --endpoint host:port            # gRPC 健康探测 (--endpoint 指向 health_listen)
cache-ctl info --endpoint host:port            # 实时 stats(同 health_listen)
cache-ctl info --rocks-path PATH               # 离线只读打开 RocksDB,查看属性
```

`ping` / `info --endpoint` 是**控制面**端口(`health_listen`,典型 7071),
**不是**数据端口——它调 gRPC `health.v1.Health/Check`,不走 wire 协议。
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
  --duration duration       (default "10s")
  --value-size int          (default 262144)  # 256 KiB
  --mode string             "get" | "put" | "mixed" (default "mixed")
  --namespace string        "chunk" | "manifest"
  --prefill int             get/mixed 模式预写对象数 (default 1000)
  --prefill-endpoint string 独立预写端点(默认同 --endpoint)
  --info-endpoint string    bench 窗口计数显示端点
```

## 3. 配置

### 3.1 三形态对比

| 维度 | local | shard | tiered |
|---|---|---|---|
| 定位 | 单节点完整对象 KV | EC 分片 KV(集群成员) | 多级缓存代理 |
| handler 后端 | 直连 RocksDB | 直连 RocksDB | TieredCache(§4.6) |
| tier chain | 无 | 无 | 按 YAML `tiers:` 顺序 |
| 写操作 | 直写 RocksDB | 直写 RocksDB | 拒绝(`StatusError`) |
| 读操作 | 查 RocksDB | 查 RocksDB | 穿 tier chain,命中后 fill-aside |
| origin 凭证 | 不需要 | 不需要 | 需要(L3 读权限) |
| 典型部署 | 测试/调试/单节点 | L2 EC 集群 × 5 | 与 manifest-ctl 同节点(边车) |

### 3.2 local 模式

```yaml
mode: local
listen: 0.0.0.0:7070           # wire 数据面
health_listen: 0.0.0.0:7071    # gRPC 健康检查(可省)
rpc_timeout: 2s                # 服务端每请求 wall-clock 上限

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
      data_shards: 4           # RS k=4
      parity_shards: 1         # RS m=1 → 总 5 分片,25% 开销
      peers:
        - {id: l2-01, endpoint: 10.0.1.11:7070}
        - {id: l2-02, endpoint: 10.0.1.12:7070}
        - {id: l2-03, endpoint: 10.0.1.13:7070}
        - {id: l2-04, endpoint: 10.0.1.14:7070}
        - {id: l2-05, endpoint: 10.0.1.15:7070}
      pool: 2                  # 每 peer 并行 TCP 连接数(wire)
      timeout: 2s

origin:
  type: store                  # 唯一合法值;cache-ctl 不直接访问文件系统
  store:
    endpoint: 10.0.1.50:7060   # store-ctl gRPC endpoint
    pool: 4                    # 独立 grpc.ClientConn 数(round-robin)
    timeout: 2s
  max_inflight: 16             # 到 origin 的最大并发 RPC 数
```

`origin.type: store` 把 cache-ctl 的 L3 fallback 指向一个 store-ctl 守护
进程——所有 origin miss 都通过 gRPC `Get` 流式拉回。cache-ctl 进程**没有
任何**文件系统读写权限,所有持久化都集中在 store-ctl。

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
| `health_listen` | gRPC 健康检查监听(`grpc.health.v1.Health`)。省略则不启动 |
| `pool` | 到单个 peer 的并行 TCP 连接数。wire 是 sync request/response,单连接会把并发请求串行化 |
| `max_inflight` | 客户端到该 tier 的最大并发对象请求数。embedded 推荐不设;origin 建议 16-32 |
| `rpc_timeout` | 服务端每请求处理超时。超时后返回 `StatusError`,TieredCache 视作该层 miss 继续下一层 |

## 4. 设计

### 4.1 总体架构

```
┌─── manifest-ctl ─────────────────────────────────────────────────┐
│  load:  fetch ── wire ObjectGet ────┐                            │
│  store: ingest ── store gRPC Put ──┐│                            │
└────────────────────────────────────┼┼─────────────────────────────┘
                                     ││
                  store gRPC Put/Get ││ wire ObjectGet
                                     ▼▼
                       ┌── store-ctl ────┐  ┌── cache-ctl tiered ───────┐
                       │  gRPC server     │  │  wire server              │
                       │  ▼               │  │  ▼                       │
                       │  fs/obs backend  │  │  TieredCache              │
                       │  __meta/gens     │  │  ├ tier 0: embedded (L1) │
                       └────────▲─────────┘  │  ├ tier 1: ec            │
                                │            │  └ origin → store gRPC   │
                                │ origin     └────────┬──────────────────┘
                                │                     │
                                └─────────────────────┘
                                                      │ wire ShardGet/Put × 5
                                                      ▼
                                            ┌─ cache-ctl shard ─┐
                                            │  wire server      │ × 5 nodes
                                            │  RocksDB          │
                                            └───────────────────┘
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
  5   Namespace    u8    0x01 chunk / 0x02 manifest(Ping 忽略)
  6   Flags        u8    保留
  7   Hash         32 B  SHA256(ciphertext);Ping 时全 0
  39  Value        可变  仅 Put 携带;Shard* 时 Value 首 2 B 为
                          [idx][total] 前缀,后接 shard_data

Response (固定 8 B 头 + 可选 ErrMsg + 可选 Value):
  0   TotalLen     u32
  4   Status       u8    0x00 Hit / 0x01 Miss / 0x02 Error
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

#### 不提供的操作

- **Delete**:内容寻址下不删除,淘汰由 §4.4 频率 sketch + CompactionFilter
  异步处理。
- **AdminService / Stats**:运行时指标走结构化日志;离线诊断用
  `cache-ctl info --rocks-path` 直接打开 RocksDB(secondary instance 模式)。
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

chunk 与 manifest 使用独立 CF:

- **chunk CF**:高频写入(fill-aside),大 value(256 KiB 级);
- **manifest CF**:低频写入,value 几 KiB 到若干 MiB。

独立 CF 让两类数据有独立 write buffer / Bloom / compaction 节奏,避免稀
有的 manifest 写入干扰高频 chunk compaction。

#### BlobDB 与写放大

两个 CF 都启用 RocksDB BlobDB,固定参数(不暴露 YAML):

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
每次 Get/Put:
    sketch.Touch(key)                   # O(1), 4-bit counter increment

每 reset_after 次 Touch 或 reset_interval 时间(先到先触发):
    sketch.Reset()                      # 所有 counter 减半

RocksDB 后台 Compaction(chunk + manifest CF 都挂 filter):
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
fetch.Fetcher.WriteTo()
   │
   ├─ manifest 二分查找确定 chunk 索引
   ├─ 并发 N 个 goroutine
   │   └─ 每 goroutine:
   │       ├─ client.ObjectGet(key) ─── wire ───► cache-ctl tiered
   │       │                                    │
   │       │                                    ▼
   │       │                               TieredCache.Get(ctx, p, key)
   │       │                                    │
   │       │                                    ├─ tier 0: embedded.Get(key)
   │       │                                    │   HIT → 返回密文
   │       │                                    ├─ tier 1: ec.Get(key)
   │       │                                    │   5 × wire ShardGet
   │       │                                    │   ≥4 返回 → RS Reconstruct
   │       │                                    └─ origin: store.Get(key)
   │       │                                        HIT → 返回 + fill-aside 所有上层
   │       ├─ crypto.Decrypt(key, ciphertext)
   │       └─ 写结果通道
   │
   └─ 按序读结果通道,写出
```

### 4.7 fill-aside

读路径上 layer i 命中时,回填所有**更上层**的 cache tier(序号 < i):

```
TieredCache.Get 命中 layer i  (layer 0..N-1 = tiers, N = origin):
    upper = min(i, N)
    for j := upper-1; j >= 0; j--:
        startFill(j, partition, key, blob)   # async, 30s timeout, fire-and-forget
```

**Blob 生命周期**:startFill 不共用调用方的 blob,而是 **Clone** 出独立
handle 给 fill goroutine,`defer cloned.Release()`。原 blob 由 Get 调用
方持有直到外层 wire 响应写完。两者引用计数独立。

**异步、fire-and-forget**:读路径不等写完成(30 s 超时);**幂等**:重
复 fill 无副作用(rocks Put 覆盖,wire ShardPut 覆盖)。

#### EC 分片缺失修复

```
ec.Get(key):
    5 × wire ShardGet → 并发
    ├─ 5 成功:RS Reconstruct → HIT
    ├─ 4 成功 + 1 缺失:Reconstruct → HIT,异步回填缺失分片
    └─ 3 成功 + 2 缺失:无法重建 → MISS;origin HIT 后 fill-aside 重编码所有 5 分片
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

原始 chunk 大小可能不被 4 整除:

1. 写入 4 字节 little-endian 长度前缀;
2. Pad 到 4 的倍数;
3. RS Split → 4 个数据分片;
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
`shard` 的语义是部署意图而非协议约束,集成测试让它同时承载两类操作反而
是特性。真正需要防误写的是 tiered 模式,由 §3.1 表中"写操作:拒绝"
分支兜底。

## 5. 内存预算

### L1(tiered 进程内嵌 embedded tier)

| 区域 | 1 TiB / 1% ratio | 100 GiB / 1% ratio |
|---|---|---|
| Go heap | < 200 MiB | < 200 MiB |
| RocksDB Index + Bloom (PinL0) | ~5 GiB | ~500 MiB |
| RocksDB BlockCache | 10 GiB | 1 GiB |
| OS Page Cache | 0(DirectReads) | 0 |
| **总** | **< 16 GiB** | **< 1.7 GiB** |

### L2(shard 专用节点)

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

1. 若启用健康服务,先把 gRPC health 状态置 `NOT_SERVING`(让编排器立刻
   下线);
2. 结构化日志输出最后一条快照,停止周期定时器;
3. wire server graceful stop:排空 in-flight,硬上限 5 s;
4. gRPC 健康 server graceful stop;
5. tier chain 反序级联关闭(embedded → ec → upstream),local/shard 模式
   下的 RocksDB。

### 6.3 结构化日志

每 N 秒向 stderr 输出 JSON 行:

```json
{"ts":"2026-04-10T03:00:00Z","mode":"tiered","hits":12345,"misses":67,"puts":89,"evictions":5}
```

### 6.4 离线诊断

```bash
cache-ctl info --rocks-path /var/cache/accel-l1
```

RocksDB secondary instance 模式打开 DB,输出 property(estimate-num-keys、
disk usage、compaction stats)。**与运行中的 cache-ctl 共存**,不需停 daemon。

## 7. 性能特征

延迟目标(由 [`perf.md`](perf.md) 测量):

| 指标 | P50 | P99 |
|---|---|---|
| L1 SSD hit | <500 µs | — |
| L1 BlockCache hot hit | <100 µs | — |
| L2 hit | 550 µs | <4 ms |
| L3 hit (fs origin) | <50 ms | <200 ms |

命中率目标:L1 >65%,L2 >99.9%,L1+L2 联合 >99.95%。

## 8. See Also

- [`store.md`](store.md) — tiered 模式 origin 是 store gRPC 客户端;cache-ctl
  自身**没有**任何文件系统读写权限,所有持久化集中在 store-ctl
- [`manifest.md`](manifest.md) — manifest-ctl 通过 wire ObjectGet 调 cache-ctl
  tiered;Manifest 内 chunk hash = 这里的 wire Hash 字段
- [`perf.md`](perf.md) §cache — 实测延迟与吞吐基线
- [`build.md`](build.md) — `make cache-ctl`(CGO + RocksDB)
- `PROPOSAL.md` §6.8 — 缓存模型与命中率目标
