# manifest — 内容寻址存储的统一出入口

`manifest-ctl` 是数据进出"内容寻址存储"的统一入口。任何字节流(EROFS
镜像、内存快照、磁盘镜像、用户文件)经 manifest-ctl `store` 写入后,产出
一个**Manifest** —— 一份包含 chunk 元数据 + 加密的密钥表的小型二进制文
件。Manifest 是后续读取(`load`)与跨节点引用(`manifest://<key>`)的
唯一句柄。

## 1. 概述

### 1.1 模块定位

```
   ┌─ input source ──┐      ┌─ manifest-ctl store ──────────────┐      ┌─ output ──────────┐
   │  sparse.Source  │ ───► │  chunker (data segments)          │ ───► │  Manifest         │
   │  (file/stdin/   │      │      ↓                            │      │  (binary file or  │
   │   tar stream)   │      │  convergent encrypt               │      │   stdout)         │
   └─────────────────┘      │      ↓                            │      └───────────────────┘
                            │  store-ctl Put(chunk_hash) ───────┼───►  chunk bytes land in
                            └───────────────────────────────────┘      store-ctl backend
```

读取方向相反:`manifest-ctl load` 从 Manifest 取 chunk 列表 → 经 cache-ctl
(若配置)穿到 store-ctl `Get` → 解密 → 输出由 `crypto.local` 决定编码的
tarstream 工件。

### 1.2 设计原则

- **写入路径**:`sparse.Source → chunker → encrypt → store.Put → Manifest`
  (Hole 段记为 manifest 空洞、不读取;Zero 段合成零字节过 chunker、免取数)
- **读取路径**:`Manifest → fetch → decrypt → io.Writer`
- **去重单位**:chunk(可变长 FastCDC 或固定长度)
- **跨用户隔离**:salt 区分 dedup 域,加密 + content-addressing 让 store 只
  见密文

### 1.3 不做什么

- 不直接管理后端字节(委派给 [`store.md`](store.md));
- 不维护缓存(委派给 [`cache.md`](cache.md));
- 不感知 docker / OCI / EROFS 等高层格式(它只看字节流);
- 不做 ACL —— customer key 的物理保护是用户责任。

## 2. 命令行接口

### 2.1 公共 flag

```
Global Flags:
  --manifest-config string    清单配置 YAML 路径(覆盖 MANIFEST_CONFIG 环境变量)
```

`--manifest-config` 与 `MANIFEST_CONFIG` 至少需要一项指向有效 YAML;两者
都缺则命令报错(`config generate` 子命令除外)。配置文件 schema 见 §3.1。

### 2.2 子命令一览

| 命令 | 功能 |
|---|---|
| `store`         | 写入数据 → 自动上传 manifest 并返回 hex content key |
| `load`          | 按 manifest key 读取明文数据 |
| `get-manifest`  | 按 hex key 取回 Manifest 原始字节(debug 用) |
| `info`          | 打印本地 Manifest 文件摘要(无需 customer key) |
| `verify`        | 通过 fetch 路径端到端验证 store 中 manifest 可用 |
| `diff`          | 比较两个本地 Manifest 的 chunk 重叠度(去重率分析) |
| `config show`   | 打印解析后的配置 YAML |
| `config generate` | 输出带注释的配置模板 |

输入 / manifest key 均为**位置参数**(匿名):`store` 的数据源、`info`
的 manifest 来源省略或 `-` = stdin;`load` / `get-manifest` / `verify`
以及 `info` 的 key 可带可选 `manifest://` 前缀(`manifest://<hex>` 与
裸 `<hex>` 等价)。位置参数须置于 flags 之后(Go stdlib flag 在首个非
flag 实参处停止解析)。

`pkg/manifest` 同时提供 canonical ref parser。`manifest://` 只允许一个 64 位
小写十六进制 content key;多层组合必须由调用方传入显式 ref 数组并使用
`fetch.NewLayered`,不再使用 `manifest://k1:k2`。文件引用为
`file://<path>[@sha256:<digest>][@location:<name>]`;带 location 时 path 必须是
basename,location 匹配 `[A-Za-z0-9][A-Za-z0-9._-]*`,只保存逻辑名称,不携带
宿主目录。允许数字开头以直接容纳 UUIDv7 sandbox/build ID。

### 2.3 `manifest-ctl store` — 数据写入

```
manifest-ctl store [flags] <path|->        # <path|-> 省略或 - = stdin

Flags:
  --extra-salt string         额外 salt 字节,叠加到 store 提供的 opaque salt
  --no-progress               禁用进度输出
```

输入是 **tarstream 工件**(平台镜像/快照的统一容器):size 与洞图都在信封里,
stdin 与文件都通过 `SourceFrom` 单遍完整验证,不物化中间文件;洞永远来自信封
元数据,不做文件系统探测或内容零扫描。`crypto.local=off|auto|required` 分别对应
plaintext-only、兼容 plaintext/encrypted、encrypted-only;ingest 结果发布前会验证
inner digest、marker、trailer 和 outer EOF。

stdout 输出一行 64 字符 hex —— 这是上传后的 manifest content key。stderr
是人类可读的摘要(默认开,`--no-progress` 关):

```
image size:   10.0 GiB
stored bytes: 1.0 GiB
chunks:       stored=2048 dedup=18432 zero=0
manifest key: a1b2c3d4...
```

示例:

```bash
# 文件 → manifest key
MKEY=$(manifest-ctl store disk.img)

# stdin → manifest key
cat disk.img | manifest-ctl store > disk.key

# docker save → 展平 → 入库(典型管道)
docker save myapp:v1 | flatten-ctl export --upload > app.key

# 额外 salt(隔离 dedup 域;flags 在位置参数前)
manifest-ctl store --extra-salt "tenant-xyz" snap.bin

# 切换分块 / 加密模式 → 改 YAML
```

### 2.4 `manifest-ctl load` — 数据读取

```
manifest-ctl load [flags] <hex|manifest://hex>

Args:
  <hex|manifest://hex>        要加载的 manifest content key(必填;可带可选
                              manifest:// 前缀)
Flags:
  --output string             输出路径 (default "-", stdout;终端拒写)
  --name string               产物 tar 条目名 (default "image")
  --offset uint               窗口起始偏移
  --length uint               窗口长度 (0 = 余下全部)
  --no-progress               禁用进度输出
```

输出是 **tarstream 工件**:manifest 空洞无损进信封洞图(没有"落洞还是填零"
的策略问题,原 `--hole` 旗标随之取消),IsZero chunk 由写出端本地合成零字节、
不取数。`crypto.local=off` 输出 byte-compatible plaintext,`auto|required` 输出
encrypted v1。要 raw 字节用 `flatten-ctl tar extract` 解包。

```bash
# 全量还原为工件(flags 在位置参数前)
manifest-ctl load --output disk.img a1b2c3d4...

# 窗口切片(切片本身也是合法工件)
manifest-ctl load --offset 4096 --length 65536 --output slice.img a1b2c3d4...

# 也可带 manifest:// 前缀
manifest-ctl load --output disk.img manifest://a1b2c3d4...
```

### 2.5 `manifest-ctl get-manifest` — 取回 manifest 字节

把 manifest 原始字节从 store 拿出来(主要用于 debug 或 `info`/`diff` 离
线场景)。

```
manifest-ctl get-manifest [--output -|FILE] <hex|manifest://hex>
```

```bash
# 拿出来直接看(也可直接 `manifest-ctl info manifest://<hex>`)
manifest-ctl get-manifest a1b2c3d4... | manifest-ctl info -

# 存档
manifest-ctl get-manifest --output disk.manifest a1b2c3d4...
```

### 2.6 `manifest-ctl info`

读取 manifest 并打印摘要。位置参数三选一:本地 manifest 文件路径、`-`
(或省略)= stdin、或 `manifest://<hex>`(从 store 取回 manifest 字节,
此时需 `--manifest-config`)。前两者无需 customer key、无需 store 连接。

```
manifest-ctl info [--manifest-config <path>] <file|-|manifest://hex>
```

```
version:       1
image size:    10.0 GiB (10737418240 bytes)
chunk mode:    cdc
chunk count:   20480
zero chunks:   0 (0 B)
holes:         0 (0 B)
min chunk:     128.0 KiB    (configured)
max chunk:     1.0 MiB    (configured)
avg chunk:     524.0 KiB

size distribution (data chunks):
       min(N)          P1          P5         P25         P50         P75         P95         P99      max(N)
  63.5K(1)       128.0K      256.0K      384.0K      512.0K      640.0K      896.0K        1.0M     1.0M(412)
  (max == configured max — 412/20480 (2.0%) chunks are forced CDC cuts)
key table:     655380 bytes (sealed)
manifest size: 1887436 bytes
```

`min chunk` / `max chunk` 标 `(configured)` 是因为它们来自 Manifest 头里记录的
**配置值**,不是实测;实测分布在下面的百分位表里(只统计非 zero 的数据 chunk,
`min(N)` / `max(N)` 括号内是取到该极值的 chunk 数;最后一行仅当实测最大值正好
等于配置上限时出现,提示有多少 chunk 是被 CDC 强制切的)。

查看 store 中的 manifest 直接 `manifest-ctl info manifest://<hex>`,等价于
`get-manifest … | info -` 管道。

### 2.7 `manifest-ctl verify`

通过 fetch 路径逐 chunk 端到端解密,验证 store 中的 manifest 全可用。

```
manifest-ctl verify [--no-progress] a1b2c3d4...        # 或 manifest://a1b2c3d4...
```

```
verified: 20480  skipped(zero): 0  holes: 0  failed: 0  total chunks: 20480
```

### 2.8 `manifest-ctl diff` — 去重率分析

```
manifest-ctl diff <manifest-a> <manifest-b>
```

两个参数都是本地 manifest 文件(用 `get-manifest` 取回);diff 不需要打开
store。

```
manifest A:  10.0 GiB, 20480 chunks (20480 unique)
manifest B:  10.1 GiB, 20512 chunks (20512 unique)
shared:      18432 chunks (9.0 GiB)
only in A:   2048 chunks (1.0 GiB)
only in B:   2080 chunks (1.1 GiB)
dedup ratio: 45.0%
```

### 2.9 `manifest-ctl config`

```
manifest-ctl config show     [--manifest-config <path>]
manifest-ctl config generate
```

`generate` 输出带注释的清单配置模板;`show` 打印加载后的有效配置(YAML)。

## 3. 配置

### 3.1 manifest-config.yaml

```yaml
manifest:
  key: "0a1b2c3d..."              # 32 字节 hex 客户密钥;Manifest 内嵌的密钥表用它密封
store:
  endpoint: 127.0.0.1:7100        # store-ctl 端点(必填):host:port 或 Unix socket
                                  # (/run/sandbox/store.sock 或 unix:///...);须与 store-ctl listen 一致
  pool: 4                         # 客户端并行 grpc.ClientConn 数 (round-robin)
  timeout: 5s                     # 单次 store RPC 超时
cache:
  endpoint: 127.0.0.1:7070        # 空 = 跳过 cache 层、直接走 store;同支持 Unix socket 路径
  pool: 4
  timeout: 2s
chunker:
  mode: cdc                       # cdc | fixed
  cdc:
    min: 128KiB
    avg: 512KiB
    max: 1MiB
  fixed:
    size: 512KiB
crypto:
  chunk: aes                      # aes(唯一支持)
  manifest: aes                   # aes(唯一支持)
  local: off                      # off | auto | required;缺省 off
```

字段说明:

- `manifest.key` — 客户密钥,**Manifest 中密钥表的密封密钥**。**不参与
  chunk 加密或寻址**。loss → 整个 Manifest 不可读。可留空,改由
  `$MANIFEST_KEY` 环境变量提供——这是密钥的**主要交付方式**,且 `$MANIFEST_KEY`
  存在时**覆盖**此处 YAML 的值(密钥懒解析、不写回 Config,故不会被
  `config show` 回显)。两者皆空时,真正用到密封/解封的命令才报错。
- `store.endpoint` — manifest-ctl 不直接读写持久层;所有 chunk / Manifest
  I/O 通过这个 gRPC 客户端打到 store-ctl 守护进程。取 `host:port`(TCP)或一个
  Unix socket(裸路径 `/run/sandbox/store.sock` 会被规范化为 gRPC 的
  `unix:///run/sandbox/store.sock`,显式 `unix:` 形式原样透传)——与 store-ctl
  的 `listen` 同址(同机经 socket 免 TCP 栈)。
- `cache.endpoint` — 空则 manifest-ctl `load` 路径直走 store gRPC;非空则
  通过 wire 协议穿 cache-ctl。同样接受 `host:port` 或 Unix socket 路径
  (`unix://path` 或裸 `/path`),与 cache-ctl 的 `listen` 同址。
- `chunker.mode` — `cdc`(FastCDC,变长)或 `fixed`(固定大小)。详见 §4.1。
- `crypto.chunk` / `crypto.manifest` — chunk 与 Manifest 的加密算法,均仅支持
  `aes`(§4.3 / §4.4);其它值在构造时即被拒绝。
- `crypto.local` — 本地存储兼容/强制 policy,不是算法选择。`off` 不启用本地
  codec,`auto` 同时接受历史 plaintext 与加密格式,`required` 只接受加密格式。
  缺省为 `off`。

### 3.2 加载顺序

**配置文件**的来源只有 CLI flag 与对应环境变量两种,**没有自动查找**:

```
--manifest-config FILE     ┐
                           ├─ precedence: flag > env; error when both absent
MANIFEST_CONFIG            ┘   (except `config generate`)
```

**customer key 是例外**:它可以来自配置文件的 `manifest.key`,**也可以**来自
`$MANIFEST_KEY` 环境变量,且后者存在时覆盖前者(§3.1)——所以即便共享的
MANIFEST_CONFIG 不含 key,命令仍能拿到密钥。

除文件外,`pkg/manifest.ParseConfig` 还支持从**内存 YAML 字节**装配 Config
(例如经 config-socket 投递给 sandbox-ctl),endpoint / crypto 等参数无需落盘;
customer key 通常由调用方单独设到 `Config.Manifest.Key`。

无单字段 CLI override —— 切换分块或加密模式直接改 YAML。

## 4. 设计

### 4.1 chunking

把字节流切成可去重单位。两种模式:

#### FastCDC (cdc)

变长内容定义分块,基于 rolling hash + Gear 表选择切点。

| 参数 | 默认 | 说明 |
|---|---|---|
| `min` | 128 KiB | 切点的最小长度;小于此值不切 |
| `avg` | 512 KiB | 期望平均长度;Gear mask 按此值设置 |
| `max` | 1 MiB   | 切点的最大长度;到此强制切 |

三个尺寸(以及 fixed 的 `size`)都必须是 4 KiB(page)的整数倍,否则配置
解析报错。

性质:对**插入/删除**有局部性 —— 在文件中段插入若干字节,只影响附近若干
个 chunk 的边界,其余 chunk 边界与之前一致 → dedup 命中率高。

#### Fixed-size (fixed)

固定大小切分(默认 512 KiB)。性质:实现简单,但对插入/删除**无**局部
性 —— 在中段插入几字节后,后续所有 chunk 都偏移,几乎全 miss。仅作 perf
基线对比用,生产推荐 cdc。

切换模式只需改 YAML `chunk.mode`,不影响 manifest-ctl 命令。

### 4.2 收敛加密(convergent encryption)

目标:**相同明文 + 相同 salt 生成相同密文**,允许跨用户在 store 上做去
重;同时 store 永远只见密文。

#### Key 派生

```
salt = server_salt (+ extra_salt mixing)   # 见 §4.5
key  = SHA256(salt || plaintext)           # convergent key
```

AES-CTR 的 IV 取全零——key 本身已由 `(salt, plaintext)` 唯一决定,同一
key 永远只加密同一明文,(key, IV) 对不会复用。由 `salt + plaintext` 决定
chunk 的密钥,因此同 salt 域内同 plaintext 必然产出同 ciphertext;同
ciphertext → 同 ContentKey(= `SHA256(ciphertext)`)→ store 上同一份字节。

#### Content key

Chunk 上传时,`SHA256(ciphertext)` 是 store 的寻址键;Manifest 上传时同样
按 `SHA256(manifest_bytes)`。Salt **不参与寻址** —— 寻址完全由密文哈希决定,
保留 salt 的隔离性,但允许 store 端以单一 KV 视图存储(详见 [`store.md`](store.md))。

### 4.3 加密模式 — chunk

YAML `crypto.chunk` 仅支持 `aes`;任何其它值在构造时即被拒绝(无明文回退):

| 模式 | 行为 | 用途 |
|---|---|---|
| `aes` | `[flag=0x01] + AES-256-CTR(key, plaintext)`(IV 全零,§4.2) | 唯一模式 |

chunk key 是收敛密钥 `SHA256(salt‖plaintext)`,由加密层内部派生(调用方只
传 salt、不传 key),故 (key, IV=0) 对不同明文不复用、CTR keystream 不重用。
flag byte 在解密时做格式校验。

### 4.4 加密模式 — Manifest

Manifest 中**密钥表 (key table)** 是一段编码了所有 chunk 加密 key 的二进
制:

| 模式 | 行为 |
|---|---|
| `aes` | AES-GCM(`manifest.key`, key_table) — customer key 解密 |

只要 `manifest.key` 不泄露,**chunk 在 store 上永远不可读**(密文),即
使 store 后端被入侵,store 自己也无法解密。

### 4.5 Salt 与 dedup 域

Salt 隔离 dedup 域 —— 同样的明文用不同 salt 派生不同 key → 不同 ciphertext
→ 不同 ContentKey → store 上是两份。

实际 salt 由两部分组合:

- `server_salt` — manifest-ctl 启动时调一次 `store-ctl GetSalt()` 取得
  store 提供的 opaque salt。Store 内部切换写入域时
  salt 变,不同写入域天然隔离。manifest consumer 不感知 store 的内部代次。
- `extra_salt` — `--extra-salt <bytes>` flag(§2.3),叠加到上面。

最终:

```
final_salt = server_salt                                                       # extra_salt 为空
final_salt = SHA256("accelerator-extra-salt-v1" || server_salt || extra_salt)  # extra_salt 非空
```

`extra_salt` 用于在同一 store salt 域内做更细粒度隔离(例如多租户)。

### 4.6 Manifest 二进制格式

Manifest 是一段紧凑的小型二进制(整数 little-endian):

```
   ┌──────────────── header (64 B) ───────────────┐
   │  magic            "MANI"   4 B               │
   │  version          u8       1 B               │
   │  chunk_mode       u8       1 B               │   1 = cdc  /  2 = fixed
   │  image_size       u64      8 B               │
   │  chunk_count      u32      4 B               │
   │  chunk_min/max    u32 × 2  8 B               │   configured bounds
   │  key_table off/len, hole_count, holes off    │
   │  (reserved padding to 64 B)                  │
   ├──────── chunk index (56 B per entry) ────────┤   sorted by image_offset
   │  image_offset     u64                        │
   │  plain_len        u32                        │
   │  flags            u32                        │   bit0 = zero-chunk
   │  cipher_hash      32 B                       │   store addressing key
   ├──────── hole extents (16 B per hole) ────────┤   entries + holes tile
   │  offset u64 / size u64                       │   [0, image_size) exactly
   ├───────────── key table ──────────────────────┤   sealed with the customer key
   │  AES-GCM( manifest.key,                      │
   │           [ key_0[32], key_1[32], … ] )      │
   └──────────────────────────────────────────────┘
```

零 chunk(明文全零,flags bit0)不加密、不入库、不占密钥表条目,读取时本地
合成零字节;hole 是外部声明的"无数据"区间(文件系统空洞等),与数据 chunk
无重叠、无缝隙地铺满整个镜像。

读路径:解析 header → 二分查找 chunk index 定位 offset → 用 `manifest.key`
解密 key table 得每 chunk 的对称 key → store/cache.Get(cipher_hash) → 解
密 → 写入 io.Writer。

### 4.7 写路径(细节)

```
sparse.Source                ← holes recorded & skipped; zero runs synthesized
   │
   ▼
chunker (cdc | fixed)        ← split per configured mode
   │  → plaintext_chunk[i]
   ▼
crypto.derive(salt, plain)
   │  → key, ciphertext
   ▼
sha256(ciphertext)           ← ContentKey
   │
   ▼
store-ctl Put(partition=chunk, key=ContentKey, ciphertext)
   │  server checks Exists first → dedup hit skips upload (SendAndClose)
   │  else stream-write to tmp → atomic rename to chunk/{gen}/aa/bb/<hash>
   ▼
manifest.append(chunk_meta, key)   ← accumulate index + key table
   │
   ▼
seal(manifest.key, key_table)
   ↓
emit Manifest
```

上图按单个 chunk 画顺序流,但 derive/encrypt/`Put` 那一段是**并发**执行的:
ingest 用一个有界 worker pool,并发度取 store 客户端连接池大小(`store.pool`
——round-robin RPC 调度下真正能同时在途的 `Put` 数就是它)。chunk 切分仍按
文件顺序、manifest 索引按文件顺序组装、进度回调串行化,所以产物字节序不变;
后端不暴露连接池信息时回退为串行。这把入库吞吐从"串行单 `Put` 往返"提升到
"池并发往返",对多 GiB snapshot `--upload` 影响显著(实测见
`platform/docs/perf.md` §2.4)。

### 4.8 读路径(细节)

```
manifest key
   │
   ├─ on-demand Getter: Get(partition=manifest)
   ▼
parse index + unseal key table
   │
   ▼
Stream (single manifest; callers use NewLayered for explicit top-to-bottom arrays)
   │
   ├─ RunAt: resolve executable sparse.Run (Hole / Zero / Data)
   │     └─ manifest Data additionally implements fetch.ChunkRun
   ├─ Run.ReadAt: read one already-resolved visible run
   ├─ Stream.ReadAt: collect runs, then fetch visible Data concurrently
   │     └─ on-demand Getter → cache/store → verify → decrypt
   │
   └─ optional Prefetcher.Prefetch
         └─ prefetch Getter → cache/store → Release
```

`Stream.RunAt` 返回不可变的 `sparse.Run`,其逻辑范围为
`[Offset(), End())`,`Run.ReadAt` 使用相对 `Offset()` 的 inner offset,且拒绝
跨越 `End()`。`RunAt` 只查询 sparse map、manifest index 或 tar extent map,
不读取 payload,不调用 cache/store Get,也不校验、解密或改变一次性 source 的
读取位置。manifest Data Run 保存首次解析得到的 chunk index,并额外实现
`fetch.ChunkRun`;Hole、Zero 和 tar/file Data Run 只实现普通 `sparse.Run`。

`Stream.ReadAt` 先收集覆盖请求窗口的全部 Run,完成解析后再修改目标 buffer;
随后只对 Data Run 并发调用 `Run.ReadAt`,Hole 和 Zero 直接填零。多层 Stream
采用 top-to-bottom 可见性:Data 和 Zero 遮挡下层,只有 Hole 或超出层大小才继续
向下。每个 upper Hole 会先收紧 bound,命中后直接返回最终 child Run,因此嵌套
overlay 不丢失 `ChunkRun` 能力,下层 chunk 也不能越过任一上层重新出现内容的
位置。部分读(`--offset` / `--length`)只触发涉及窗口的物理 chunks,不会扫描或
物化完整镜像。

支持预取的 Stream 额外实现 `fetch.Prefetcher`。`Prefetch(ctx)` 遍历整个逻辑
Stream,只对最终可见且实现包内 prefetch 能力的 `ChunkRun` 调用完整物理
chunk 的 cache Get;普通 Data Run、Hole 和 Zero 都是 no-op。成功命中后立即
Release,不读取 Blob、不校验、不解密、不 pin。预取作用于当前组合 Stream 的
完整可见视图,不再提供按 manifest key 选择叶子的接口。

同一 Fetcher 的 manifest metadata、普通 ReadAt 和所有 Stream 共用一个底层
Getter 与请求调度器。on-demand 请求从不等待已开始的 prefetch;存在任意
on-demand 时不再准入新的 prefetch,且每个 Fetcher 最多一个 prefetch Get 在
途。已开始的 prefetch 不抢占,可与后来到达的 on-demand 短暂重叠。不同
Fetcher 相互独立,调度器不创建后台 goroutine,也不拥有底层 Getter 生命周期。

Stream 层不维护 chunk seen set 或 singleflight。同一物理 chunk 经多个真实可
见区间暴露、并发调用 Prefetch,或同时被 Prefetch 和 ReadAt 访问时,允许产生
独立 Get;缓存层负责把后续请求转化为命中。这保持了读取与预取路径一致的对象
所有权和失败语义。

### 4.9 本地 immutable tarstream 加密

canonical tarstream 的 plaintext 结构保持为 payload、空
`.kuasar.sha256.<plainDigest>` marker 和两个 trailer block。未传 codec 时,
`tarstream.WriteTo` 的输出与旧格式逐 byte 相同,对外 identity 为
`sha256:<plainDigest>`。传入 customer-key-backed codec 时,完整结构进入
encrypted tarstream v1,对外 identity 固定为:

```text
hmac = HMAC-SHA256(customerKey, plainDigestRaw32Bytes)
```

scheme 名为 `hmac`,不派生 identity key,不加入 domain prefix。marker 中的
`plainDigest` 位于密文内,不得用于 key-bound 文件名、ref、日志或错误。
`@hmac:<digest>` 只是 identity scheme,不表示输入的物理编码;`auto` 读取历史
plaintext 时同样返回 `hmac`。

v1 固定 AES-256-GCM 和 4096-byte 独立认证 record,不提供算法或 record size 协商。
文件由 48-byte clear prefix、81-byte authenticated header 和连续 authenticated
records 组成。prefix 的 `[0:16]` 是 magic、version、prefix size 和 reserved 字段,
`[16:48]` 是每次写入由 `crypto/rand` 生成的 32-byte artifact salt。codec 对每个
artifact 仅执行一次以下派生并构造一个 AES-256-GCM 实例:

```text
artifactKey = HMAC-SHA256(
  customerKey,
  "kuasar/tarstream/aes-gcm/artifact-key/v1\x00" || artifactSalt
)
```

header 使用 sequence 0;data record `i` 使用 sequence `i+1`;12-byte GCM nonce 是
`0x00000000 || uint64BE(sequence)`。record wire 是 1-byte AES-GCM flag 后跟 GCM
ciphertext 和 16-byte tag,因此仍保持 17-byte overhead,不存储逐 record nonce。
header 绑定完整 plaintext 大小、packed payload 起点/大小和 record geometry;
每个 record 的 AAD 绑定包含 salt 的完整 prefix、authenticated header、record
index 与实际 plaintext 长度。写入先完成 sparse metadata sweep 和布局规划,再单遍
读取 Data extents;不会生成 plaintext staging 或 spool 文件。

随机 salt 使同一 customer key 和 plaintext 的两次写入具有不同物理 ciphertext,
但 logical identity 仍严格保持
`HMAC-SHA256(customerKey, plainDigestRaw32Bytes)`,所以 scheme、文件名和 dedup key
不变。

`SourceAt` 从 prefix 读取 salt、绑定 artifact codec,认证 header 及覆盖
marker/trailer 的 suffix records,随后按 record index O(1) 随机解密。这个 fast
path 不预读全部 data records,也不扫描并重算完整 plaintext digest。不同 artifact
使用不同派生 key,所以跨 artifact splice 的 donor data record 会在该 record 被
实际读取时认证失败;未被 fast open 读取的 donor record 不会提前触发错误。

`SourceFrom` 是 full-validation path。完整消费时它解密全部 records,重算 marker
前 plaintext tar bytes 的 SHA-256,验证空 marker、恰好两个 trailer blocks、
expected `sha256|hmac` 和 outer EOF。跨 artifact record splice 会在顺序读取到
该 record 时以 authentication failure 失败。
调用方主动停止消费时,尚未读取部分不具备完整验证结论;任何发布、上传或转换
路径必须消费全部 Data extents并传播终点错误。

## 5. 性能特征

测量入口:`platform/docs/perf.md` §2.1–2.2(冷/热 L1 状态下
manifest:// 加载端到端时长)。

主要决定项:

- chunk 模式(cdc 命中率高于 fixed,但分块本身略慢);
- 加密模式(aes 是 CPU 大头,~50–100% CPU 在大 chunk 上);
- store 端点延迟(本地 fs vs 远端 S3-compatible object storage,详见 [`store.md`](store.md))。

## 6. See Also

- [`store.md`](store.md) — manifest-ctl 通过 gRPC 把字节落到 store-ctl
- [`cache.md`](cache.md) — `load` 路径可选穿 cache-ctl 加速;cache-ctl 自身
  以 manifest 同款客户端从 store 取 chunk
- `guest-runtime/docs/flatten.md` — 镜像展平后经 `flatten-ctl export --upload`
  入库
- `sandboxer/docs/sandbox.md` — 沙箱通过 `manifest://<key>` 引用磁盘
  base 与快照
- `platform/docs/kuasar-sandbox.md` §4.1–4.4 — Manifest 抽象在系统中的位置
