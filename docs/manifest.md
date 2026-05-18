# manifest — 本地存储访问设计

`manifest-ctl` 是数据进出"内容寻址存储"的统一入口。任何字节流(EROFS
镜像、内存快照、磁盘镜像、用户文件)经 manifest-ctl `store` 写入后,产出
一个**Manifest** —— 一份包含 chunk 元数据 + 加密的密钥表的小型二进制文
件。Manifest 是后续读取(`load`)与跨节点引用(`manifest://<key>`)的
唯一句柄。

## 1. 概述

### 1.1 模块定位

```
   ┌─ input stream ──┐      ┌─ manifest-ctl store ──────────────┐      ┌─ output ──────────┐
   │  io.Reader      │ ───► │  chunker                          │ ───► │  Manifest         │
   │  (file/stdin/   │      │      ↓                            │      │  (binary file or  │
   │   pipe)         │      │  convergent encrypt               │      │   stdout)         │
   └─────────────────┘      │      ↓                            │      └───────────────────┘
                            │  store-ctl Put(chunk_hash) ───────┼───►  chunk bytes land in
                            └───────────────────────────────────┘      store-ctl backend
```

读取方向相反:`manifest-ctl load` 从 Manifest 取 chunk 列表 → 经 cache-ctl
(若配置)穿到 store-ctl `Get` → 解密 → 输出明文流。

### 1.2 一句话原则

- **写入路径**:`io.Reader → chunker → encrypt → store.Put → Manifest`
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

> 写入 manifest blob 始终由 `store` 内部完成,不再有独立的 `put-manifest`
> 子命令。

### 2.3 `manifest-ctl store` — 数据写入

```
manifest-ctl store [flags]

Flags:
  --input string              输入路径 (default "-", stdin)
  --extra-salt string         额外 salt 字节,叠加到 generation salt
  --detect-holes              对常规文件 lseek(SEEK_HOLE/SEEK_DATA) 检测稀疏空洞
  --no-progress               禁用进度输出
```

stdout 输出一行 64 字符 hex —— 这是上传后的 manifest content key。stderr
是人类可读的摘要(默认开,`--no-progress` 关):

```
image size:   10.0 GiB
stored bytes: 1.0 GiB
chunks:       stored=2048 dedup=18432 zero=0
generation:   gen-2026-05-01
manifest key: a1b2c3d4...
```

示例:

```bash
# 文件 → manifest key
MKEY=$(manifest-ctl store --input disk.img)

# stdin → manifest key
cat disk.img | manifest-ctl store > disk.key

# docker save → 展平 → 入库(典型管道)
docker save myapp:v1 | flatten-ctl export --input - --upload > app.key

# 额外 salt(隔离 dedup 域)
manifest-ctl store --input snap.bin --extra-salt "tenant-xyz"

# 切换分块 / 加密模式 → 改 YAML
```

### 2.4 `manifest-ctl load` — 数据读取

```
manifest-ctl load [flags]

Flags:
  --manifest-key string       要加载的 manifest 的 hex content key(必填)
  --output string             输出路径 (default "-", stdout)
  --offset uint               起始偏移
  --length uint               读取长度 (0 = 整个镜像)
  --hole string               空洞策略: error (默认) | zero | punch
  --no-progress               禁用进度输出
```

```bash
# 全量还原
manifest-ctl load --manifest-key a1b2c3d4... --output disk-restored.img

# 部分读
manifest-ctl load --manifest-key a1b2c3d4... --length 4096 | hexdump -C

# 稀疏镜像:把 manifest 空洞落成文件空洞
manifest-ctl load --manifest-key a1b2c3d4... --output disk.img --hole punch
```

### 2.5 `manifest-ctl get-manifest` — 取回 manifest 字节

把 manifest 原始字节从 store 拿出来(主要用于 debug 或 `info`/`diff` 离
线场景)。

```
manifest-ctl get-manifest --manifest-key HEX [--output -|FILE]
```

```bash
# 拿出来直接看
manifest-ctl get-manifest --manifest-key a1b2c3d4... | manifest-ctl info --manifest -

# 存档
manifest-ctl get-manifest --manifest-key a1b2c3d4... --output disk.manifest
```

### 2.6 `manifest-ctl info`

读取本地 manifest 文件并打印摘要(无需 customer key、无需 store 连接):

```
manifest-ctl info --manifest disk.manifest
```

```
version:       1
image size:    10.0 GiB (10737418240 bytes)
chunk mode:    cdc
chunk count:   20480
min chunk:     63.5 KiB
max chunk:     1.0 MiB
avg chunk:     524.0 KiB
key table:     655380 bytes (sealed)
manifest size: 1887436 bytes
```

要查看 store 中的 manifest,先 `get-manifest` 取回字节再管道给 `info`。

### 2.7 `manifest-ctl verify`

通过 fetch 路径逐 chunk 端到端解密,验证 store 中的 manifest 全可用。

```
manifest-ctl verify --manifest-key a1b2c3d4...
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
  endpoint: 127.0.0.1:7100        # store-ctl gRPC 端点(必填)
  pool: 4                         # 客户端并行 grpc.ClientConn 数 (round-robin)
  timeout: 5s                     # 单次 store RPC 超时
cache:
  endpoint: 127.0.0.1:7070        # 空 = 跳过 cache 层、直接走 store
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
  chunk: aes                      # aes | fake
  manifest: aes                   # aes | fake
```

字段说明:

- `manifest.key` — 客户密钥,**Manifest 中密钥表的密封密钥**。**不参与
  chunk 加密或寻址**。loss → 整个 Manifest 不可读。
- `store.endpoint` — manifest-ctl 不直接读写持久层;所有 chunk / Manifest
  I/O 通过这个 gRPC 客户端打到 store-ctl 守护进程。
- `cache.endpoint` — 空则 manifest-ctl `load` 路径直走 store gRPC;非空则
  通过 wire 协议穿 cache-ctl。
- `chunker.mode` — `cdc`(FastCDC,变长)或 `fixed`(固定大小)。详见 §4.1。
- `crypto.chunk` / `crypto.manifest` — chunk 与 Manifest 各自的加密模式
  (§4.3 / §4.4)。fake 是性能基线模式,不要在生产打开。

### 3.2 加载顺序

CLI flag 与对应环境变量是仅有的两种来源,**没有自动查找**:

```
--manifest-config FILE     ┐
                           ├─ 优先级:flag > env;两者皆缺则报错
MANIFEST_CONFIG            ┘   (config generate 除外)
```

不再支持单字段 CLI overrides(`--manifest-key` / `--chunk-mode` 等已移除)
—— 切换分块或加密模式直接改 YAML。

## 4. 设计

### 4.1 chunking

把字节流切成可去重单位。两种模式:

#### FastCDC (cdc)

变长内容定义分块,基于 rolling hash + Gear 表选择切点。

| 参数 | 默认 | 说明 |
|---|---|---|
| `min` | 64 KiB  | 切点的最小长度;小于此值不切 |
| `avg` | 512 KiB | 期望平均长度;Gear mask 按此值设置 |
| `max` | 1 MiB   | 切点的最大长度;到此强制切 |

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
key   = SHA256(salt || plaintext)        # convergent key
nonce = key[:12]                           # AES-CTR IV
salt  = SHA256(server_salt || extra_salt)  # 见 §4.5
```

由 `salt + plaintext` 决定 chunk 的密钥与 IV,因此同 salt 域内同 plaintext
必然产出同 ciphertext;同 ciphertext → 同 ContentKey(= `SHA256(ciphertext)`)
→ store 上同一份字节。

#### Content key

Chunk 上传时,`SHA256(ciphertext)` 是 store 的寻址键;Manifest 上传时同样
按 `SHA256(manifest_bytes)`。Salt **不参与寻址** —— 寻址完全由密文哈希决定,
保留 salt 的隔离性,但允许 store 端以单一 KV 视图存储(详见 [`store.md`](store.md))。

### 4.3 加密模式 — chunk

YAML `crypto.chunk`:

| 模式 | 行为 | 用途 |
|---|---|---|
| `aes`  | `[flag=0x01] + AES-256-CTR(key, IV, plaintext)` | 生产默认 |
| `fake` | `[flag=0x00] + HMAC(key, plaintext)[:32] + plaintext` | 性能基线;不要在生产打开 |

`fake` 模式专为对比测量收敛寻址 / 去重 / 上传开销时,排除 AES 加密的
CPU 影响 —— 由于密文 = 明文,生产部署不应允许使用。flag byte 在解密时
做模式校验,跨模式读取会失败。

### 4.4 加密模式 — Manifest

Manifest 中**密钥表 (key table)** 是一段编码了所有 chunk 加密 key 的二进
制:

| 模式 | 行为 |
|---|---|
| `aes`  | AES-GCM(`manifest.key`, key_table) — customer key 解密 |
| `fake` | HMAC + 明文(同 chunk 路径) |

只要 `manifest.key` 不泄露,**chunk 在 store 上永远不可读**(密文),即
使 store 后端被入侵,store 自己也无法解密。

### 4.5 Salt 与 dedup 域

Salt 隔离 dedup 域 —— 同样的明文用不同 salt 派生不同 key → 不同 ciphertext
→ 不同 ContentKey → store 上是两份。

实际 salt 由两部分组合:

- `server_salt` — manifest-ctl 启动时调一次 `store-ctl GetSalt()` 取得
  active-generation salt。Generation 切换时 salt 变,跨代天然隔离。
- `extra_salt` — `--extra-salt <bytes>` flag(§2.3),叠加到上面。

最终:

```
final_salt = SHA256(server_salt || extra_salt)
```

`extra_salt` 用于在同一 generation 内做更细粒度隔离(例如多租户)。

### 4.6 Manifest 二进制格式

Manifest 是一段紧凑的小型二进制:

```
   ┌──────────────── header ──────────────────┐
   │  magic        "MANI"   4 B               │
   │  version      u32      4 B               │
   │  chunk_mode   u8       1 B               │   0 = cdc  /  1 = fixed
   │  chunk_avg    u32      4 B               │
   │  chunk_min    u32      4 B               │
   │  chunk_max    u32      4 B               │
   │  image_size   u64      8 B               │
   │  chunk_count  u32      4 B               │
   │  ...                                     │
   ├──────────── chunk index ─────────────────┤   sorted by image_offset
   │  for each chunk:                         │
   │     image_offset   u64                   │   (implicit — derivable from cumulative)
   │     plain_len      u32                   │
   │     cipher_hash    32 B                  │   store addressing key
   │     flags          u8                    │   zero-chunk / aes / fake bit
   ├───────────── key table ──────────────────┤   sealed with the customer key
   │  AES-GCM( manifest.key,                  │
   │           [ key_0[32], key_1[32], … ] )  │
   └──────────────────────────────────────────┘
```

读路径:解析 header → 二分查找 chunk index 定位 offset → 用 `manifest.key`
解密 key table 得每 chunk 的对称 key → store/cache.Get(cipher_hash) → 解
密 → 写入 io.Writer。

### 4.7 写路径(细节)

```
io.Reader
   │
   ▼
chunker (cdc | fixed)        ← 按 mode 切
   │  → plaintext_chunk[i]
   ▼
crypto.derive(salt, plain)   ← key, nonce
   │  → key, ciphertext
   ▼
sha256(ciphertext)           ← ContentKey
   │
   ▼
store-ctl Put(partition=chunk, key=ContentKey, ciphertext)
   │  服务端先 Exists → dedup 命中即跳过 upload(SendAndClose)
   │  否则流式 Write 到临时 → atomic rename 到 chunk/{gen}/aa/bb/<hash>
   ▼
manifest.append(chunk_meta, key)   ← 累积索引 + 密钥表
   │
   ▼
seal(manifest.key, key_table)
   ↓
emit Manifest 文件
```

上图按单个 chunk 画顺序流,但 derive/encrypt/`Put` 那一段是**并发**执行的:
ingest 用一个有界 worker pool,并发度取 store 客户端连接池大小(`store.pool`
——round-robin RPC 调度下真正能同时在途的 `Put` 数就是它)。chunk 切分仍按
文件顺序、manifest 索引按文件顺序组装、进度回调串行化,所以产物字节序不变;
后端不暴露连接池信息时回退为串行。这把入库吞吐从"串行单 `Put` 往返"提升到
"池并发往返",对多 GiB snapshot `--upload` 影响显著(见 [`perf.md`](perf.md) §2.4)。

### 4.8 读路径(细节)

```
Manifest
   │
   ▼
parse(header, index, sealed_key_table)
   │
   ▼
unseal(manifest.key, sealed_key_table)
   │  → key[i]
   ▼
fetch.Range(offset, length) → ChunkIDs
   │
   ▼
for each ChunkID:
   ├─ cache.Get(ContentKey) ──→ store.Get fallback
   │  → ciphertext
   ▼
crypto.decrypt(key[i], nonce, ciphertext) → plaintext
   │
   ▼
io.Writer
```

`fetch.Range` 在 Manifest 的 chunk 索引上做二分查找,定位需要的 chunk 子
集 —— 部分读(`--offset` / `--length`)只触发涉及到的 chunk 拉取,稀疏访
问下不会读全镜像。

## 5. 性能特征

测量入口:[`perf.md`](perf.md) §cache(冷/热 L1 状态下 manifest-ctl
load 端到端时长)。

主要决定项:

- chunk 模式(cdc 命中率高于 fixed,但分块本身略慢);
- 加密模式(aes 是 CPU 大头,~50–100% CPU 在大 chunk 上;fake 是基线
  对比用);
- store 端点延迟(本地 fs vs 远端 OBS,详见 [`store.md`](store.md))。

## 6. See Also

- [`store.md`](store.md) — manifest-ctl 通过 gRPC 把字节落到 store-ctl
- [`cache.md`](cache.md) — `load` 路径可选穿 cache-ctl 加速;cache-ctl 自身
  以 manifest 同款客户端从 store 取 chunk
- [`flatten.md`](flatten.md) — 镜像展平后通常用 `flatten-ctl | manifest-ctl
  store` 入库
- [`sandbox.md`](sandbox.md) — 沙箱通过 `manifest://<key>` 引用磁盘 base
  与快照
- `PROPOSAL.md` §6.3-6.7 — Manifest 抽象在系统中的位置
