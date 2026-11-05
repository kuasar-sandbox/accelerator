[English](file-artifacts.md) | [简体中文](file-artifacts_zh.md)

# 文件工件与载体

本篇定义通用不可变文件加密、tarstream 与多 Manifest Bundle 契约。[manifest_zh.md](manifest_zh.md) 定义逻辑 Manifest/Chunk 对象与密钥，[store_zh.md](store_zh.md) 定义持久化和准入。本篇不定义 Sandbox E/S 内容或 OCI/EROFS 镜像语义；对应消费者按自身 [Sandbox 工件](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox_zh.md#artifact-model) 和 [镜像](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten_zh.md) 契约使用这些载体。

## 1. 不可变本地 tarstream 与加密

在 Kuasar 中，容器镜像、沙箱根盘（Sandbox E）以及虚拟机内存快照（Snapshot S）均需封装为单一不可变载体文件，以便在本地磁盘、共享文件系统与远端仓库之间持久化和分发；同时，运行时还需在无需预先解包的前提下，直接按逻辑偏移进行随机读取。

VM 内存增量和磁盘增量都具有**逻辑地址空间大、单层实际覆盖少**的特点（例如 8 GiB 逻辑空间仅修改 128 MiB）。在多层快照叠加读取时，单层中的任意逻辑区间必须精确区分以下三种状态：

```text
             Parent Layer : [ AAAA ]   [ AAAA ]   [ AAAA ]
                               ^          x          x
                               |          |          |
             Current Layer: [ Hole ]   [ Zero ]   [ BBBB ]  <-- Hole=未覆盖, Zero=显式清零, BBBB=新数据
                               |          |          |
                               v          v          v
             Merged Result: [ AAAA ]   [ 0000 ]   [ BBBB ]  <-- Hole穿透读父层, Zero遮蔽父层, Data覆盖
```

| 逻辑状态 | 本层是否覆盖 | 单层 `ReadAt` 字节结果   | 多层叠加语义                                  |
| -------- | ------------ | ------------------------ | --------------------------------------------- |
| **Hole** | 否           | `0x00`                   | 未覆盖，向上回退读取父层（得到 `AAAA`）       |
| **Zero** | 是           | `0x00`                   | 显式清零，遮蔽父层数据（直接返回零）          |
| **Data** | 是           | 实际保存的字节（可含零） | 显式写入，以本层保存的字节为准（返回 `BBBB`） |

新写出的 canonical tarstream 的 plaintext 结构依次为 **presence entry（`.kuasar.presence.v1`）**、**稀疏 payload 成员（含可选 dense metadata tail）**、**`.kuasar.digest.<plainDigest>` marker** 和**两个全零 TAR 尾块**。Reader 也接受不含 presence 的既有便携 carrier。Payload 元数据记录 `payloadSize` 边界；marker body 记录该边界与 `payloadCommitment`，完整读取时再用 payload 字节复验：

```text
[1. 业务逻辑文件视图] (logicalSize = payloadSize + tailSize)
  0                                                payloadSize        logicalSize
  +--------------+--------------+--------------+--------+------------------+
  |    Data A    |     Hole     |  Zero (4KiB) | Data B |  Dense ZIP Tail  |
  +------+-------+--------------+--------------+---+----+--------+---------+
         |              (omitted on wire)          |             |
         +----------------------+------------------+-------------+
                                v                                v
[2. 明文 Canonical tarstream 视图] (plaintextSize, 512 B aligned)
  0                                       packedStart       packedStart+packedSize
  +-------------------+-------------------+-------+----+--------+---------+----------------+
  |   Presence Entry  |  Payload Headers  |Data A |Data|ZIP Tail| Padding | Marker + 2*EOF |
  |(.kuasar.presence) |   & Sparse Map    |       | B  |        | (0~511B)| (Fixed 2048 B) |
  +-------------------+-------------------+-------+----+--------+---------+----------------+
  |<------------ packedStart ------------>|<---- packedSize --->|
                                          |
         +--------------------------------+
         |
         v  (Record 0 明文长度 F = packedStart mod 4096，模为 0 时 F = 4096，使 packedStart 对齐在记录边界)
[3. 加密信封视图 Encrypted v1] (encryptedSize = 129 + plaintextSize + 17 * N)
  0         48           129
  +---------+------------+--------------+--------------+-----+--------------+
  |48B Clear| 81B Sealed |   Record 0   |   Record 1   | ... |  Record N-1  |
  | Prefix  |   Header   | (Plain: F B) | (Plain: 4KiB)|     | (Plain<=4KiB)|
  +---------+------------+--------------+--------------+-----+--------------+
```

| 坐标空间           | 范围                 | 偏移含义与特点                                                                                        |
| ------------------ | -------------------- | ----------------------------------------------------------------------------------------------------- |
| **业务逻辑文件**   | `[0, logicalSize)`   | 前段 `[0, payloadSize)` 为稀疏 payload；后段 `[payloadSize, logicalSize)` 为可选 dense tail（如 ZIP） |
| **明文 tarstream** | `[0, plaintextSize)` | 包含前置元数据与紧密排列的 `packed data`，末尾固定为 `2048 B` 的 digest marker 与两个全零尾块         |
| **加密信封**       | `[0, encryptedSize)` | 将完整明文 tarstream 切分为 `N` 条独立认证记录，每条记录附加 `1 B flag + 16 B tag = 17 B` 开销        |

归档末尾固定的 `2048 B`（`4 × 512 B`）marker 与尾块布局如下：

```text
+-----------------------------------------------------------------------+ 0
| Marker Header (512 B): name = ".kuasar.digest.<64-char-lowercase-hex>"|  <-- 成员名即 plainDigest
+-------------------------------------------+---------------------------+ 512
| Marker Body (40 B)                        | Zero Padding (472 B)      |  <-- 保存 payloadCommitment
|   [0..8)  : payloadSize (uint64 BE)       |   [40..512) must be 0x00  |      供替换尾部时快速派生
|   [8..40) : payloadCommitment (32 B raw)  |                           |
+-------------------------------------------+---------------------------+ 1024
| Archive Trailer Blocks (2 * 512 B = 1024 B, must be all 0x00)         |
+-----------------------------------------------------------------------+ 2048 (TAR EOF)
```

### 1.1 页 presence 与 Zero 传输

所有新写出的 tarstream 均使用 presence v1 profile。逻辑 payload 可以为空，也可以以不足 4 KiB 的部分页结尾，上限为 **1 TiB**。Hole run 必须从页边界开始，在页边界或精确的 payload 末端结束。Writer 在元数据规划阶段拒绝超限或该格式无法表达的布局，此时尚未输出任何字节（包括加密 header）或读取 Data。Writer 不写旧格式，也不通过填充 payload 或改变 Hole 边界来满足约束。容量上限作用于可选且已声明的 dense metadata tail 之前的 payload；tail 沿用独立的 **64 MiB** 上限。格式顺序为：

```text
普通文件 .kuasar.presence.v1
带 KUASAR.presence=1 和稀疏元数据的 payload
原有 .kuasar.digest.<plainDigest> marker
两个全零 TAR 尾块
```

内部 presence entry 不属于尾部 ZIP。其数据体以固定 64 字节 little-endian 头部开始，随后紧跟位图字节：

```text
Byte Off  0                   8            12           16                  24
          +-------------------+------------+------------+-------------------+
          |  magic (8 B)      | version(4B)|pageSize(4B)|  payloadSize (8B) |
          | "KUPRES1\n"       |     1      |    4096    |  (no dense tail)  |
          +-------------------+------------+------------+-------------------+
         24                  32                                            60            64
          +-------------------+---------------------------------------------+------------+
          | bitmapLength (8B) |              reserved (28 B)                | CRC32 (4B) |
          | ceil(pages / 8)   |              must be all 0x00               | IEEE CRC32 |
          +-------------------+---------------------------------------------+------------+
         64                                                                64 + bitmapLength
          +------------------------------------------------------------------------------+
          |                    bitmap (1 bit per 4 KiB logical page)                     |
          +------------------------------------------------------------------------------+
```

| 相对偏移   | 长度 | 字段           | 编码与校验规则                                                           |
| ---------- | ---- | -------------- | ------------------------------------------------------------------------ |
| `[0, 8)`   | 8 B  | `magic`        | 固定为 `KUPRES1\n`（`4b 55 50 52 45 53 31 0a`）                          |
| `[8, 12)`  | 4 B  | `version`      | `uint32LE`，当前固定为 `1`                                               |
| `[12, 16)` | 4 B  | `pageSize`     | `uint32LE`，当前固定为 `4096`                                            |
| `[16, 24)` | 8 B  | `payloadSize`  | `uint64LE`，`[0, payloadSize)` 纳入位图管理，最大 `1 TiB`（`1<<40`）     |
| `[24, 32)` | 8 B  | `bitmapLength` | `uint64LE`，必须等于 `(ceil(payloadSize / 4096) + 7) / 8`，最大 `32 MiB` |
| `[32, 60)` | 28 B | `reserved`     | 必须全为 `0`                                                             |
| `[60, 64)` | 4 B  | `CRC32`        | `uint32LE`，IEEE CRC32，计算范围覆盖头部 `[0, 60)` + 完整 `bitmap`       |

设 `pages = ceil(payloadSize / 4096)`，逻辑页 `i`（`0 <= i < pages`）对应 `bitmap[i / 8]` 的第 `i % 8` 位（LSB 对应最低页号）。空 payload 的位图长度为零，但仍包含 64 B presence header 和 payload mark。最后一页止于精确的 `payloadSize`；未使用位及 TAR padding 必须为零。位图不包含已声明的 dense metadata tail，位图原始字节数最多 **32 MiB**（`1 GiB` payload 对应 `32 KiB` 位图）：

```text
示例: payloadSize = 16384 B (4 个 4 KiB 页)
  Page 0 [0, 4096)     : Data  --> bit 0 = 1
  Page 1 [4096, 8192)  : Hole  --> bit 1 = 0
  Page 2 [8192, 12288) : Zero  --> bit 2 = 1
  Page 3 [12288, 16384): Data  --> bit 3 = 1

  bitmap[0] 二进制排列 (bit 7 .. bit 0):
  +---+---+---+---+---+---+---+---+
  | 0 | 0 | 0 | 0 | 1 | 1 | 0 | 1 |  = 0x0d
  +---+---+---+---+---+---+---+---+
  |<-- unused --->|p3  p2  p1  p0    (未使用的尾位必须全为 0，否则拒绝)
```

Writer 仅对 **`[0, payloadSize)` 内单个 `sparse.Zero` run 覆盖的完整 4 KiB 对齐页**省略物理字节；不满足整页对齐的部分页 Zero 头尾片段（以及位于 dense tail 内的 Zero）在同一 profile 内按字面 Data 处理，保留在已存储 extent 中写出。全 Zero payload 不分配字面零 payload。任意字节长度的 dense 来源（包括 ZIP-only checkpoint 元数据）保留精确长度。普通全零 Data 不做探测或压缩：

```text
单个 Zero run 跨越非对齐边界: [2048, 10240)
  Page 0 [0, 4096)       Page 1 [4096, 8192)     Page 2 [8192, 12288)
  +---------+---------+ +---------------------+ +---------+---------+
  | (Other) |Zero Frag| |   Full 4 KiB Zero   | |Zero Frag| (Other) |
  +---------+----+----+ +----------+----------+ +----+----+---------+
                 |                 |                 |
                 v                 v                 v
         进入存储 extent      物理上省略不落盘    进入存储 extent
         (合成字面零落盘)     (仅靠 presence=1)   (合成字面零落盘)
```

读取端在 `[0, payloadSize)` 内联合查询 `presence` 位图与已存储的 payload extent 即可无歧义还原三态：

| `presence` 位 (`P`) | 是否存在已存储 payload extent | 物理载体是否落盘字节 | 还原出的 `RunAt` 状态        | 单层 `ReadAt` 返回字节 |
| ------------------- | ----------------------------- | -------------------- | ---------------------------- | ---------------------- |
| `P = 0`             | 否                            | 否                   | **Hole**                     | 合成 `0x00`            |
| `P = 1`             | 否                            | 否                   | **Zero**（整页省略）         | 合成 `0x00`            |
| `P = 1`             | 是                            | 是                   | **Data**（含非整页 Zero）    | 读取落盘字节           |
| `P = 0`             | 是                            | -                    | **格式非法**（直接报错拒绝） | -                      |

Presence entry 与 payload mark 必须唯一且匹配。版本、几何、校验和、未使用位、位置错误、重复、缺少 mark/entry 或 `P=0` 下存在 Data 均报错，不能回退旧格式。空名称选择跳过内部成员；ordinal API 保留物理 entry 序号，选中 payload 时附带读取其前置 presence。`Holes` 只报告 `P=0`，不包含 Zero。旧便携 carrier 继续可读，包括空 payload 及精确的字节级 Hole 边界。重写可读的旧 carrier 时，逻辑 source 满足新 writer 约束就统一写 presence v1；不满足则重写失败，不会输出旧格式 carrier。Writer 生成空 payload、以部分页结尾或超过原有 32 GiB 限制的 payload 前，reader 必须已支持这些 presence 扩展。

启用加密时，presence 位于认证记录之内，reader 先认证再分类。完整顺序消费即使没有 Data，或尾部是 Zero/Hole，也会验证 identity、padding、归档结束及外层 EOF。快速随机/部分访问仍具有原有验证范围限制。打开期间临时缓存一个元数据记录，避免重复解密；返回 source 前释放，不缓存之后的 payload 读取。

Reader/writer 共用的 extent 上限仍为 `1<<20`（含尾哨兵及最终 tail 合并结果）。Writer 在输出任何字节（包括加密 header）或读取 Data 之前拒绝超限。Presence 规划在分配前计入原索引预算，不增加 payload 位图或前缀索引；不能通过展开 Zero 为字面 Data 规避上限。测试及性能结果须区分索引预算、逻辑字节、传输字节和身份 CPU。

`go test ./pkg/manifest/crypto -run '^$' -bench '^BenchmarkPresenceTarStream$' -benchtime=3x` 比较 64 MiB 逻辑源中 Zero 占比为 0%、50% 和 100% 的明文及 AES 情形。50% 情形交替使用 1 MiB Data/Zero run。探针记录编码字节、写入耗时以及包含索引和认证暂存的打开分配量；预先构建的 wire buffer 不计入打开分配量。对旧实现使用相同基准源码，至少执行三轮成对对照。没有 Zero 的载体增加位图开销；全 Zero 载体可省去几乎全部传输字节，但仍有线性散列成本，包括顺序读取器打开无 Data 流时的验证。随机打开仍保持其局部验证边界。不能据此声称身份计算为常数时间，也不能把该结果当作磁盘 I/O 吞吐。

### 1.2 三层结构化内容身份（Digest 与 HMAC）

身份域及算法与物理编码方式解耦。`plainDigest` 不是对整个物理文件做平面 SHA-256，而是对逻辑覆盖区间、已覆盖字节、规范化成员名和边界做三层结构化散列（所有多字节整数均为 **big-endian** `uint64`）：

```text
+------------------------------------------------------------------+
| Layer 1: payloadCommitment                                       |
|   SHA256(                                                        |
|     "kuasar/tarstream/payload\x00"                               |
|     || uint64BE(payloadSize)                                     |
|     || uint64BE(presentIntervalCount)                            |  <-- 由 presence 合并出的连续已覆盖区间 (P=1)
|     || [uint64BE(offset) || uint64BE(length)]*                   |
|     || all present logical bytes in [0, payloadSize)             |  <-- 含 Data 字节 + 合成的整页省略 Zero 字节;
|   )                                                              |      Hole 字节不参与散列
+---------------------------------+--------------------------------+
                                  |
                                  |   +----------------------------+
                                  |   | Layer 2: tailDigest        |
                                  |   |   SHA256(                  |
                                  |   |     "kuasar/tarstream/"    |
                                  |   |     "tail\x00"             |
                                  |   |     || uint64BE(tailSize)  |  <-- 覆盖 [payloadSize, logicalSize)
                                  |   |     || tailBytes           |      的全部 dense tail 字节
                                  |   |   )                        |
                                  |   +-------------+--------------+
                                  v                 v
+------------------------------------------------------------------+
| Layer 3: plainDigest (written into .kuasar.digest.<hex>)         |
|   SHA256(                                                        |
|     "kuasar/tarstream/carrier\x00"                               |
|     || uint64BE(len(normalizedName)) || normalizedName           |  <-- 绑定规范化成员名、总长、payload 边界
|     || uint64BE(logicalSize)                                     |      以及前两层摘要
|     || uint64BE(payloadSize)                                     |
|     || payloadCommitment (32 B)                                  |
|     || tailDigest (32 B)                                         |
|   )                                                              |
+------------------------------------------------------------------+
```

- **编码表示无关性**：`payloadCommitment` 枚举裁剪到 `payloadSize` 的逻辑 `P=1` 区间，散列 Data 和合成的 Zero 字节；writer 计数后流式遍历区间，不建立另一份 extent 索引。旧字面零 carrier 与等价的省略 Zero carrier 具有相同逻辑身份，可复用已有内容寻址对象。Zero 散列的 CPU 仍随逻辑长度线性增长；省略优化减少传输及分配，不消除既有身份算法要求的工作。Payload 的传输元数据（权限、属主和时间）固定为 canonical 编码，不能在 identity 不变时改写。
- **仅替换尾部时的快速派生（`CarrierDigest` / `ComposeDigest`）**：完整 carrier 或只替换 dense metadata tail 的派生 source 可通过 `CarrierDigest` 直接给出 identity，不需要先把 payload 编码到 `io.Discard`。可派生的 dense metadata tail 上限为 **64 MiB**，writer 和 reader 使用同一边界并在 hash 前拒绝超限声明：

```text
Marker Body (existing)  --> (payloadSize, payloadCommitment) --+
                                                               +--> ComposeDigest --> new plainDigest (零 payload I/O)
Replacement ZIP Tail    --> new tailDigest + new logicalSize --+
```

- **明文与加密载体的外部身份映射**：未传 codec 时对外 identity 为 `digest:<plainDigest>`。传入 customer-key-backed codec 时，完整结构进入 encrypted tarstream v1，对外 identity 固定为：

```text
                     +-- Plaintext (no Codec) --> "digest:" + hex(plainDigest)
plainDigest (32 B) --+
                     +-- Encrypted (with Codec)-> "hmac:"   + hex(HMAC-SHA256(customerKey, plainDigestRaw32Bytes))
```

scheme 名为 `hmac`，不派生 identity key，不加入 domain prefix。marker 中的 `plainDigest` 和 `payloadCommitment` 位于密文内，不得用于 plaintext 文件名、ref、日志或错误。物理 carrier 决定 identity scheme：`auto` 读取 plaintext 返回 `@digest`，读取 encrypted carrier 返回 `@hmac`；两者不做隐式转换。

### 1.3 加密信封格式（encrypted tarstream v1）

encrypted tarstream v1 固定 **AES-256-GCM** 和 **4096-byte 独立认证 record**，不提供算法或 record size 协商。启用加密后，完整明文 canonical tarstream（从 presence entry 到末尾两个全零尾块）整体切分为连续认证记录。文件由 **48-byte clear prefix**、**81-byte authenticated header** 和连续 authenticated records 组成：

```text
Encrypted File Layout (encryptedSize = 48 + 81 + plaintextSize + 17 * recordCount):
  Offset 0              48                      129
  +---------------------+-----------------------+-----------------------+-----+-----------------------+
  |  48 B Clear Prefix  |  81 B Sealed Header   |      Record 0         | ... |     Record N-1        |
  | (magic+ver+len+salt)| 1B flag+64B enc+16Btag| 1B flag+F B enc+16Btag|     | 1B flag+K B enc+16Btag|
  +---------------------+-----------------------+-----------------------+-----+-----------------------+
                        |                       |<---- Sealed Complete Canonical Plaintext Stream --->|
                        +-- sequence = 0        +-- sequence = 1              +-- sequence = N
```

**1. 48 字节明文前缀（Clear Prefix，文件偏移 `[0, 48)`，Big-Endian）：**

prefix 的 `[0:16]` 是 magic、version、prefix size 和 reserved 字段，`[16:48]` 是每次写入由 `crypto/rand` 生成的 32-byte artifact salt：

```text
  0                   8         10        12            16                                  48
  +-------------------+---------+---------+-------------+-----------------------------------+
  |    magic (8 B)    | ver(2B) | size(2B)| reserved(4B)|        artifactSalt (32 B)        |
  | 89 "KTSENC\n"     |    1    |    48   |  0x00000000 |     random per-artifact salt      |
  +-------------------+---------+---------+-------------+-----------------------------------+
```

**2. 64 字节认证头部（解密后明文，文件偏移 `[48, 129)` 解密所得，Big-Endian）：**

header 绑定完整 plaintext 大小、packed payload 起点/大小和 record geometry：

```text
  0                   8                   16                  24                  32
  +-------------------+-------------------+-------------------+-------------------+
  | plaintextSize(8B) |  packedStart (8B) |  packedSize (8B)  |  recordCount (8B) |  <-- uint64 BE
  |  full plain bytes | packed data start | packed data bytes | data record count |
  +-------------------+-------------------+-------------------+-------------------+
 32          36          40                                                      64
  +-----------+-----------+-------------------------------------------------------+
  |recSize(4B)|firstSz(4B)|                    reserved (24 B)                    |  <-- uint32 BE + 24B 0x00
  |   4096    |  1..4096  |                   must be all 0x00                    |
  +-----------+-----------+-------------------------------------------------------+
```

解密 64 字节认证头部后，读取端分阶段执行以下几何一致性校验（任一校验失败均返回 `ErrMalformedEnvelope`）：
1. **头部参数自洽性（解密头部后立即校验）**：要求 `plaintextSize >= 2048` 且按 `512 B` 对齐，`recordSize == 4096`，首条记录明文长度 `firstSize == packedStart % 4096`（整除时为 `4096`），`packedStart + packedSize <= plaintextSize - 2048`，且声明的 `recordCount` 与根据 `(plaintextSize, firstSize)` 推导的记录总数一致。
2. **外层物理总长与尾界校验（按读取路径区分）**：随机读取（`SourceAt`）在打开时已知外层物理文件大小，会**立即核对**推算的 `encryptedSize` 与实际物理大小是否相等；流式读取（`SourceFrom`）面向未知总长的输入流，在顺序消费完全部记录后校验记录完整性、总记录数及外层物理 `EOF`（防止尾部截断或附加冗余字节）。
3. **内外层几何一致性（打开阶段解析完内层前缀元数据后校验）**：核对内层实际的 `packed data` 起始偏移与长度，要求与外层认证头部声明的 `packedStart`、`packedSize` 严格一致。

**3. 密钥派生、Nonce、AAD 与认证加解密流程：**

codec 对每个 artifact 仅执行一次 `artifactKey` 派生并构造一个 AES-256-GCM 实例。header 使用 sequence `0`；data record `i` 使用 sequence `i+1`；12-byte GCM nonce 是 `0x00000000 || uint64BE(sequence)`。record wire 是 1-byte AES-GCM flag（`0x01`）后跟 GCM ciphertext 和 16-byte tag，保持 **17-byte overhead**，不存储逐 record nonce。每个 record 的 AAD 绑定包含 salt 的完整 prefix、authenticated header、record index 与实际 plaintext 长度：

```text
customerKey (32 B) --+
                     +--> HMAC-SHA256("kuasar/tarstream/aes-gcm/artifact-key/v1\x00" || artifactSalt)
artifactSalt (32 B) -+         |
                               v
                       artifactKey (32 B) --> AES-256-GCM Instance (每个工件绑定一次)
                                                     |
  +--------------------------------------------------+----------------------------------+
  |                                                  v                                  |
  | Nonce (12 B) : 0x00000000 (4B) || uint64BE(sequence) (8B)                           |  <-- Header: seq=0; Record i: seq=i+1
  |                                                                                     |
  | Header AAD   : "kuasar/tarstream/header/v1\x00" || clearPrefix(48B)                 |
  | Record i AAD : "kuasar/tarstream/record/v1\x00" || clearPrefix(48B)                 |  <-- 绑定前缀、头部几何、序号与实际长度
  |                || plainHeader(64B) || uint64BE(i) || uint32BE(actualPlainLen)       |
  |                                                                                     |
  | Encrypt/Seal : 0x01 || GCM.Seal(nonce, recordPlaintext, AAD) --> (len + 17) B       |
  | Decrypt/Open : verify record[0]==0x01 --> GCM.Open in-place                         |  <-- 认证失败时 clear 清零暂存区
  +-------------------------------------------------------------------------------------+
```

由于 `recordAAD(i)` 同时绑定了 `clearPrefix`（含 salt）、`plainHeader`（含全局几何布局）、记录序号 `i` 与当前记录明文长度，**跨文件拼接记录、调换记录顺序、截断尾部记录或篡改任意字节**等异常均会被布局、长度或 GCM 认证检查拒绝。随机 salt 使同一 customer key 和 plaintext 的两次写入具有不同物理 ciphertext，但 logical identity 仍严格保持 `HMAC-SHA256(customerKey, plainDigestRaw32Bytes)`，所以 scheme、文件名和 dedup key 不变。

**4. 记录切分几何（首条记录对齐 `packedStart`）：**

为避免前置元数据使后续 `4 KiB` 对齐的业务页面普遍跨越两条加密记录，**Record 0 的明文长度 `F` 被设定为 `packedStart` 对 `4096` 取模的结果（整除时取 `4096`），从而确保业务数据区（`packed data`）的起始偏移 `packedStart` 始终对齐在某条记录的起始边界上**（例如下方示例中 `packedStart = 3072` 落在 `Record 1` 起点；若前置元数据更长，如 `packedStart = 7168`，则 `F = 3072`，`packedStart` 将落在 `Record 2` 起点。注意：该规则保证的是 `packedStart` 对齐于记录边界；后续单个 payload 页或 extent 是否跨记录，仍取决于其前方已存储 extent 的总长度是否保持 `4 KiB` 的整数倍）：

```text
Plaintext Stream (示例: packedStart = 3072, plaintextSize = 14336):
  0                  3072                 7168                11264               14336
  +-------------------+--------------------+--------------------+-------------------+
  |  Prefix Metadata  |   Data A (4096B)   |   Data B (4096B)   | ZIP(1K)+End(2048B)|
  +---------+---------+---------+----------+---------+----------+---------+---------+
            |                   |                    |                    |
            v                   v                    v                    v
         Record 0            Record 1             Record 2             Record 3
      Plain : 3072 B      Plain : 4096 B       Plain : 4096 B       Plain : 3072 B
      Cipher: 3089 B      Cipher: 4113 B       Cipher: 4113 B       Cipher: 3089 B
      (seq = 1)           (seq = 2)            (seq = 3)            (seq = 4)
```

设 $P = \text{packedStart}$，$L = \text{plaintextSize}$（在 canonical 格式中恒有 $L \ge P + 2048 > F$）：
- 首条内容记录明文长度：$F = ((P - 1) \bmod 4096) + 1$
- 内容记录总条数：$N = 1 + \lceil (L - F) / 4096 \rceil$（当 $L \le F$ 时 $N = 1$）
- 最终加密文件总大小：$\text{encryptedSize} = 48 + 81 + L + 17 \times N$

### 1.4 写入管线、随机读取寻址与验证矩阵

**1. 两阶段写入管线（`WriteTo`）：**

写入先完成 sparse metadata sweep 和布局规划，再单遍读取 Data extents；不会生成 plaintext staging 或 spool 文件：

```text
Input: ctx, io.Writer, name, sparse.Source, WriteOptions
  |
  v
+-------------------------------------------------------------------+
| Phase 1: Metadata Planning (makeWritePlanLimited)                 |  <-- 阶段一：仅规划校验，零字节输出、不读 Data
+-------------------------------------------------------------------+
  |  1. 规范化成员名，检查保留前缀 (.kuasar.digest. / .kuasar.presence.v1)
  |  2. 若源实现 IdentityProvider，获取声明的 payloadSize 边界
  |  3. 第 1 遍 RunAt (0..payloadSize)：校验容量(<=1TiB)与 Hole 4KiB 对齐，按内存预算分配 presence 位图
  |  4. 第 2 遍 RunAt (0..size)：填充位图，省略 payload 内完整 Zero 页，合并存储 extent，校验 dense tail (<=64MiB)
  |  5. 编码前置元数据与 presence header，算出精确 plaintextSize 与加密几何参数
  |
  v  (规划校验全部通过后才进入阶段二；若失败则直接返回，零字节输出)
+-------------------------------------------------------------------+
| Phase 2: Streaming Emission & Hashing (emitPlan + recordWriter)   |  <-- 阶段二：单遍读 Data、在线算 Hash、流式写出
+-------------------------------------------------------------------+
  |  1. [可选加密] 生成 32B 随机 salt，写出 48B Clear Prefix 与 81B Sealed Header
  |  2. 写出前置元数据 (presence + payload headers + sparse map)
  |  3. 按序遍历已存储 extents (copyExtent)：
  |       - Data 子区间：调用 run.ReadAt 单遍读取
  |       - 未省略的 Zero 子区间：直接 clear(chunk) 合成零字节（不调 ReadAt）
  |       - 经 io.MultiWriter 同时写入目标流与 identityAccumulator（自动补算省略的整页 Zero）
  |  4. [可选] 执行 WithBeforeCommit(ctx) 钩子（在落盘最终 marker 前拦截）
  |  5. 写出对齐填充 + .kuasar.digest.<hex> marker (40B body) + 2*512B 全零尾块
  |  6. [可选加密] Flush 最后一条加密记录，核对总字节数与总记录条数
  |
  v
Output: (scheme, hexDigest, nil)
```

**2. 随机读取寻址（`SourceAt`）与全量顺序验证（`SourceFrom`）：**

`SourceAt` 从 prefix 读取 salt、绑定 artifact codec，认证 header 及覆盖 marker/trailer（及已声明的 dense metadata tail）的记录，随后按 record index $O(1)$ 随机解密：

```text
业务逻辑偏移 x
  |
  +--> 1. 二分查找 extentAt(x)
  |         +-- 若落在未存储区间 (Hole 或省略的整页 Zero): 直接 clear 填充 0x00 返回
  |         |   (若调用的是 RunAt, 则结合 presence[x/4096] 精确返回 Hole 或 Zero 状态)
  |         +-- 若命中 extent[i] = [Oi, Oi + Li): 进入步骤 2
  |
  +--> 2. 映射为明文物理偏移 (O(1) 计算)
  |         plainOffset = packedStart + packedPrefix[i] + (x - Oi)
  |         +-- [明文载体] 直接调用底层 ReaderAt.ReadAt(p, plainOffset) --> 完成!
  |         +-- [加密载体] 进入步骤 3 (recordReaderAt)
  |
  +--> 3. 映射为加密记录序号与物理密文偏移 (O(1) 计算, 设首条记录明文长为 F)
  |         recordIndex(plainOffset) = 0                                   (当 plainOffset < F)
  |                                  = 1 + (plainOffset - F) / 4096        (当 plainOffset >= F)
  |         plainStart(idx)          = 0                                   (当 idx == 0)
  |                                  = F + (idx - 1) * 4096                (当 idx >= 1)
  |         cipherOffset(idx)        = 129 + plainStart(idx) + idx * 17
  |
  +--> 4. 批量合并 I/O 与逐记录 GCM 认证解密 (readBatch)
            - 相邻多条记录单次合并读取 (上限 maxCoalesceBytes = 1 MiB)
            - 从 sync.Pool 获取临时缓冲区, 逐条校验 AAD 与 GCM Tag 并原地解密
            - 拷贝目标切片 [plainOffset - plainStart(idx) : ...] 给调用方
            - 归还缓冲池前执行 clear() 彻底擦除明文/密文/AAD (本层不驻留明文缓存)
```

这个 fast path 不预读全部 data records，也不扫描并重算完整 plaintext digest。不同 artifact 使用不同派生 key，所以跨 artifact splice 的 donor data record 会在**该 record 被实际读取时**认证失败；未被 fast open 读取的 donor record 不会提前触发错误。

明文 `SourceAt` 不校验 payload 内容完整性：即使 `WithExpectedDigest` 匹配且调用方读完整个 payload，读取仍直接返回存储字节。加密载体仅对实际读取的记录执行 GCM 认证；两者均不会通过随机读取重算完整 `payloadCommitment`。需要明文完整性验证时，应完整消费 `SourceFrom` 并处理最终校验错误。

`SourceFrom` 是 full-validation path。完整消费时它解密全部 records，按权威 sparse map 重算 payload/tail commitment 与 carrier identity，验证 marker body、恰好两个 trailer blocks、expected `digest|hmac` 和 outer EOF。跨 artifact record splice 会在顺序读取到该 record 时以 authentication failure 失败。调用方主动停止消费时，尚未读取部分不具备完整验证结论；任何发布、上传或转换路径必须消费全部 Data extents 并传播终点错误。

| 校验维度                                               | `SourceAt`（随机访问打开）                                                                                                   | `SourceFrom`（流式顺序完整消费）                                                      | `ReadFrom` / `ReadSeekFrom`      |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | -------------------------------- |
| **加密前缀与 64B 认证头**                              | **打开时校验**（含物理文件总长与内层 `packedStart`/`packedSize` 核对）                                                       | **打开时校验**（含内层 `packedStart`/`packedSize` 核对）                              | 不支持加密载体                   |
| **Presence CRC32 与位图一致性**                        | **打开时校验**（含尾位清零、与存储 extents 无冲突）                                                                          | **打开时校验**                                                                        | **打开时校验**                   |
| **Canonical 传输头** | presence-v1 **打开时校验**；legacy 载体存在 digest marker 时在打开阶段要求 canonical 编码 | presence-v1 **打开时校验**；legacy canonical 校验在完整消费结束时执行（无 Data 时在打开阶段完成） | presence-v1 **打开时校验**；legacy 头仅解析，不强制 canonical |
| **Digest Marker 与 2×512B 尾块**                       | **打开时定位并校验**（仅在无 presence、无 codec、无 expected digest 时兼容打开无 marker 明文载体）                           | **消费完末个 Data 时校验**（始终要求有 marker；若无 Data 则在打开时完成）             | 不读取、不校验 marker            |
| **Dense Metadata Tail (`[payloadSize, logicalSize)`)** | **打开时完整读取（加密含 GCM 认证）并重算 `tailDigest`，校验 `composeDigest == plainDigest`**（含 marker 的 canonical 载体） | **随流读取并重算 `tailDigest`**                                                       | 仅当普通数据按需读取，不校验摘要 |
| **Payload 本体 (`[0, payloadSize)`)** | **明文：不校验内容完整性**，即使携带 `WithExpectedDigest` 并读完整个 payload。**加密：仅对实际读取的记录执行 GCM 认证**；两者均不重算完整 `payloadCommitment` | **全量校验**：重算完整 `payloadCommitment` 与 `plainDigest`，常数时间比对 marker 声明 | 仅按需读取，不校验摘要 |
| **`WithExpectedDigest` 锚点匹配** | **打开时匹配** marker 声明的 `digest`/`hmac`；不校验未读取的 payload，也不会为明文 payload 增加内容校验 | **完成时匹配**重算得到的 `digest`/`hmac` | 不支持 `ReadOption` |
| **外层物理 EOF（防尾部附加冗余字节）**                 | **打开时校验**：含 marker 的 canonical 载体要求 `markerEnd + 1024 == size`（加密载体还核对头部推算的物理总长）               | **完成时校验**：在两个全零块后再试读 1 字节确认 `io.EOF`                              | 不校验                           |

**3. 核心接口与格式配额速查：**

| 函数 / 接口                 | 适用场景                                                                 | 关键特征与限制                                                      |
| --------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------- |
| `WriteTo`                   | 将 `sparse.Source` 编码写出为 canonical tarstream（明文或加密 v1）       | 两阶段写出；返回 `(scheme, hexDigest, error)`                       |
| `SourceAt`                  | 本地文件、NFS 等支持 `io.ReaderAt` 且已知物理大小的随机稀疏读取          | 并发安全；打开即校验元数据和 dense tail，payload 按需读取           |
| `SourceFrom`                | 流式管道、上传接收、格式转换中的单遍顺序消费与全量校验                   | 单向递增 `ReadAt`（不可回退）；必须读完所有 Data 才触发最终验证结论 |
| `ReadFrom` / `ReadSeekFrom` | 仅需标准 `io.Reader` / `io.ReadSeeker` 逻辑字节视图及 `Holes()` 列表     | **仅支持明文载体**；不校验 `.kuasar.digest.`，不区分 Zero 与 Data   |
| `ReadSeekFromIndex`         | 按物理条目序号（0-based）寻址明文载体成员                                | 允许按序号访问内部成员，选中 payload 时附带前置 presence            |
| `CarrierDigest`             | 从已实现 `IdentityProvider` 的源直接导出目标写入策略下的外部身份         | **零 payload I/O**；支持直接导出 `digest:<hex>` 或 `hmac:<hex>`     |
| `ComposeDigest`             | 已知原 `payloadCommitment`，仅替换 dense tail 字节时计算新 `plainDigest` | 要求 `len(tail) == totalSize - payloadSize <= 64 MiB`               |

| 维度                         | 参数 / 约束值                     | 说明                                                                                            |
| ---------------------------- | --------------------------------- | ----------------------------------------------------------------------------------------------- |
| **归档与成员最大长度**       | `1 << 62` B（4 EiB）              | 超出则拒绝读写                                                                                  |
| **Payload 最大长度**         | `1 TiB`（`1 << 40` B）            | 由 `.kuasar.presence.v1` 管理，对应位图最大 `32 MiB`                                            |
| **Presence 页大小**          | `4096 B`（4 KiB）                 | `Hole` 必须以 4 KiB 对齐起止（仅允许在 `payloadSize` 处非对齐结束）                             |
| **Dense Metadata Tail 上限** | `64 MiB`（`64 << 20` B）          | 区间 `[payloadSize, logicalSize)` 必须完全 dense（不能含 Hole）                                 |
| **Sparse Extent 数量上限**   | `1 << 20`（1,048,576）            | 含末尾 `(logicalSize, 0)` 哨兵及合并后的 tail extent                                            |
| **索引内存预算核算基线**     | `2 MiB` + 按规划开销累计计费      | 未设置 `WithIndexMemoryLimit` 时不限制；启用后以 `2 MiB` 为基线累计核算规划阶段内存开销          |
| **空 Payload 最小载体大小**  | 明文 `5120 B` / 加密 `5283 B`     | 空 payload 仍包含 64 B presence 头、payload 标记与末尾 `2048 B` marker/尾块                     |
| **加密记录粒度与合并上限**   | 目标明文 `4096 B`（开销 `+17 B`） | 随机读取每次至少认证 1 条记录，相邻记录批量合并读取上限 `1 MiB`                                 |
| **显式 Zero 的散列开销**     | 与逻辑覆盖长度成正比              | 省略的整页 Zero 省去了磁盘/网络 I/O，但计算 `payloadCommitment` 时仍需向 SHA-256 喂入合成零字节 |

## 2. 多 Manifest Bundle 载体

本地 Manifest 快照使用支持 ZIP64 的标准 ZIP 容器,带强制尾部索引。当前 profile 直接要求
`bundle/index`，没有旧格式探测、无索引兼容或 Central Directory fallback。所有新
Bundle 的物理布局为：

```text
[0]     bundle/refs                                            # optional
[0|1]   bundle/admission/<generation>/<64-lowercase-hex-salt>
[...]   chunk/<64-lowercase-hex-content-key> |
        manifest/<64-lowercase-hex-content-key>
[last]  bundle/index                                           # required

        Central Directory
        ZIP64 EOCD + locator                                    # when required
        EOCD with an empty comment
```

Manifest/Chunk payload 仍是现有 physical object 的原始字节。Chunk Local Entry 继续按
Manifest 中首次逻辑出现的 ordinal 写出；只排序索引 record，不按 ContentKey 重排或
缓存全部 Chunk payload。共享对象仍按 ContentKey 去重，ManifestKey、Chunk key 和对象
physical bytes 均不改变。

全部 Local Entry 固定为 `zip.Store`、flags 0、无 data descriptor、无 local extra，
comment、时间、权限和平台字段也固定。`bundle/index` 必须恰好一个并且是最后一个
Local Entry；其 payload 最后 256 bytes 与真实 Central Directory 紧邻。Bundle 不使用
ZIP Deflate、ZIP encryption、整文件 SHA/HMAC、外层加密、自定义 pack、root entry 或
JSON/YAML metadata。旧 `admission/*`、旧的无索引 Bundle、重复或未知 entry、目录、
非法 key/admission 与截断由对应 profile/验证路径拒绝。普通 Open 有意延迟部分对象与
容器检查,不等于严格 full-container verifier([§2.4](#24-显式严格验证与-exact-upload))。

### 2.1 `bundle/index` v1

索引 payload 依次是 Chunk section、Manifest section 和固定 footer。两个 section 各自
按 ContentKey 严格递增，partition 由 section 隐含；重复或未排序 key 非法。每条 record
固定 48 bytes，以 little-endian 显式编码：

| byte range | field | encoding |
|---|---|---|
| `[0,32)` | ContentKey | 原始 32 bytes |
| `[32,40)` | payload DataOffset | `uint64`，对象 payload 的 archive 绝对 offset |
| `[40,44)` | payload Size | `uint32` |
| `[44,48)` | ZIP CRC32 | `uint32` |

`DataOffset` 不是 Local Header offset。`DataOffset + Size` 必须 checked-add 且完整位于
metadata prefix 之后、`bundle/index` Local Header 之前。Manifest/Chunk 的 individual
size 继续受各自 codec 上限约束。整个 ZIP 最多 100,000 个 entry；两个索引 section
合计最多 99,998 records,index payload 最多 4,800,160 bytes。两个限制同时生效;
可选 refs entry 也消耗一个 ZIP entry 名额。

footer 固定 256 bytes；除 magic 和 digest 外的整数均为 little-endian：

| byte range | field |
|---|---|
| `[0,16)` | magic `KUASARBNDLINDEX1` |
| `[16,18)` | version，固定 `1` |
| `[18,20)` | footer size，固定 `256` |
| `[20,22)` | record size，固定 `48` |
| `[22,24)` | reserved，必须全零 |
| `[24,32)` | index payload absolute offset |
| `[32,40)` | index payload total size |
| `[40,48)` | `bundle/index` Local Header absolute offset |
| `[48,56)` | metadata prefix end offset |
| `[56,64)` | Manifest section absolute offset |
| `[64,72)` | Manifest record count |
| `[72,80)` | Manifest section size |
| `[80,112)` | SHA-256 of the exact encoded Manifest section bytes |
| `[112,120)` | Chunk section absolute offset |
| `[120,128)` | Chunk record count |
| `[128,136)` | Chunk section size |
| `[136,168)` | SHA-256 of the exact encoded Chunk section bytes |
| `[168,252)` | reserved，必须全零 |
| `[252,256)` | CRC32C/Castagnoli of footer bytes `[0,252)` |

section size 必须精确等于 `count * 48`。Chunk section 从 index payload 起点开始，
Manifest section 必须紧随其后，footer 又必须紧随 Manifest section；三者不能重叠、留
gap 或覆盖 footer。若 `directoryOffset` 是 EOCD/ZIP64 EOCD 声明的真实 Central
Directory 起点，则必须同时满足：

```text
indexPayloadOffset + indexPayloadSize == directoryOffset
footerOffset + 256 == directoryOffset
```

section SHA-256 和 footer CRC32C 只用于发现索引损坏，不是签名或认证。对象内容安全仍
由 Manifest ContentKey、密封 key table/解密合同及 `manifest.verify_content` 策略承担。
chunk AES-CTR 本身不能被称为认证加密。

Writer 不依赖 `archive/zip.Writer` 的内部 flush 位置，而是按 canonical Local Header
布局维护逻辑 `nextOffset`：`dataOffset = nextOffset + 30 + len(name)`。Finalize 先检查
root Manifest，再分别排序并编码 Chunk/Manifest records，写出最后的 `bundle/index`，
最后才由标准 ZIP writer 写 Central Directory/ZIP64/EOCD。严格 verifier 和测试会把
每个 record 的 offset、size、CRC 与实际 CD/LFH/data range 交叉核对。

### 2.2 metadata prefix 与 Reader I/O

`bundle/refs` 存在时必须非空并作为第一个物理 Local File Header；admission 必须紧随
其后，否则 admission 必须是第一个 entry。admission payload 为空。`bundle/refs`
一行一个 canonical、按文件顺序搜索的 Bundle file ref：

```text
file://<basename>.bundle
file://<basename>.bundle@location:<name>
```

它禁止 `@manifest/@digest/@hmac`、`manifest://`、绝对路径、目录分隔符和非
`.bundle` 文件名。payload 必须是有效 UTF-8、仅 LF 换行且最后一行也以 LF 结束；
禁止 BOM、CR、空行、注释、首尾空白和重复 ref。Writer 保留调用方顺序，不排序；
最多 1024 项、1 MiB。空路径通过省略 entry 表达。`WriterOptions.Refs` 在
`NewWriter` 写出任何 byte 前完成验证；Writer 创建后 refs/admission 均不可变。
`Reader.Refs()` 返回副本。
preflight 只需发现 location/admission 时可使用 `OpenMetadata`/`ReadMetadata`；该入口
只验证连续 Local Header metadata prefix，不读取尾部、索引、Central Directory 或
对象。即使某个文件的 prefix 可被该 preflight 解析，也不会证明它是可消费的 Bundle，
更不构成无索引兼容；真正消费 Manifest/Chunk 必须使用 `Open`/`NewReader`，后者强制
要求合法 v1 索引。

普通 `Open`/`NewReader` 不调用 `archive/zip.NewReader`，也不读取 Central Directory、
对象 Local Header、Chunk index 或对象 payload。ReaderAt 远端路径的打开顺序固定为：

1. 从 EOF 读取 EOCD；ZIP64 时再各读取一次 locator 和 ZIP64 EOCD，得到真实
   `directoryOffset`，但不读取该 offset 开始的 CD bytes。
2. 从 `directoryOffset - 256` 读取 footer，验证 magic/version/checksum 和全部 section
   bounds。
3. 固定读取并验证 `bundle/index` 自己的 canonical Local Header。
4. 按 footer 的 `metadataPrefixEnd` 一次连续读取 metadata prefix，并在内存中解析
   refs/admission。
5. 一次连续读取 Manifest section，验证 digest、排序、重复、size/range 后建立 O(1)
   只读 map；Chunk section 保持未读。

因此 ZIP32 `Open` 是 5 次固定 range read，ZIP64 是 7 次；次数不随 Chunk 数增长。
文件打开仍优先使用 read-only mmap；ReaderAt 对象 payload 回退路径使用按
4 KiB～2 MiB 分级、每个 Reader 最多保留 32 MiB 的有界 buffer pool。

### 2.3 source selection、Chunk preparation 与普通 restore

一个 Bundle 从首版容纳根内存 Manifest、根/数据盘当前层 Manifest，以及必要时
收编的父层 Manifest。全部对象共用 admission entry 中的同一完整
`store.WriteAdmission`。`bundle.Writer` 在构造前取得一次 admission；后续多个
`Ingest` 只读取这份固定值。共享 Chunk 按 ContentKey 只写一份。Chunk 编码仍可
并行，但实现通过有界 ordinal reorder 等待逻辑顺序并串行 append ZIP，不产生无界
完成队列。

配置层用 `Config.NewBundleIngester` 装配不带 extra salt 的兼容路径；
`Config.NewBundleIngesterWithExtraSalt` 接受与 Store ingest 相同的可选 extra-salt
resolver。Bundle 记录基础 admission，实际 Chunk key 则由加密 Manifest key table
认证。完整验证和 exact upload 使用该认证 key 解密，不再从基础 admission salt
重复派生，因此读取方无需 extra salt。发布根之前仍验证物理 ContentKey、key-table
AAD、解码尺寸、布局、依赖闭包和目标 admission。

`manifest-ctl load --output-mode=bundle` 是该 profile 的可执行生产端。位置参数按
自上而下顺序 layering；`file://NAME.bundle@manifest:KEY[@location:NAME]` 会选择并
证明该 Bundle 内的 root。`--ref-location NAME=file:///absolute/directory` 避免把
host 路径写入持久引用。tail 去除/替换作用于所选逻辑 stream，而非此 Bundle 外层
ZIP carrier。

读侧仅在 `Fetcher.OpenManifest` 按以下顺序选择来源：

```text
当前 Bundle -> bundle/refs[0] -> bundle/refs[1] -> ... -> 默认 remote Cache/Store
```

路径和 `@location` 解析由调用方实现的 `bundle.SourceResolver` 完成；accelerator 只
消费 Reader 中已经验证的 canonical ref。resolver 可用 `ErrSourceUnavailable`
表达 sibling 不存在、location mapping 缺失或 located 文件不存在，此时尚未选源，
可保留诊断并继续。文件存在但 profile/ZIP 损坏必须返回普通错误并 fail closed。
引用 Bundle 自己的 `Refs()` 不参与搜索，路径必须由创建者提前展平。

| Manifest 来源 | Manifest/Chunk Getter | 失败语义 |
|---|---|---|
| current 或首个 refs Bundle 命中 | 该 Bundle-only | Chunk index、对象读取、解析或解密错误直接失败，不再搜索 |
| 所有 Bundle clean miss/unavailable | remote-only cache/store | Bundle 中碰巧同 key 的 Chunk 不参与 |

因此不存在对象级 Bundle fallback Getter；本地 Manifest 错误也不会改读后续
Bundle/Store。current/refs source selection 只查询 Open 时已经加载的 Manifest map；
clean miss 不读取 Chunk section。Bundle 一旦被选中，Reader 在返回 Stream 前一次连续
读取完整 Chunk section, 验证 digest/records/ranges 并建立 O(1) 只读 map.
准备阶段串行加载, 只缓存验证成功的索引; 失败或取消后, 后续调用可以重新加载.
remote source 没有 Bundle Reader, 因此不执行这一步.

准备完成后的 Manifest/Chunk `Get` 直接使用 record 中的 payload `DataOffset/Size`，只
做一次目标 payload range read；不会读取索引、对象 Local Header 或 Central Directory。
实际 Stream 的第一次 `Read` 因而不承担 O(Chunk records) 的索引初始化。外部
root selector 必须通过 `OpenRootManifest`/`SelectRoot` 证明 root 物理存在于 current
Bundle，不能从 refs 或 Store 间接取得。

普通 restore 不再预扫描 Manifest 的完整 Chunk closure。Manifest 声明一个缺失但本次
不会读取的 Chunk 时，`Open` 和 `OpenManifest` 可以成功，已存在 Chunk 仍可读取；只有
真正访问缺失 Chunk 时才失败，而且选定 source 后不得 fallback 到后续 Bundle 或 remote
Store。`manifest.verify_content=false` 仍不会隐式执行 physical SHA 扫描；为 true 时的
Manifest/Chunk physical hash 与 key table authentication 遵守普通读合同。无论开关取值,
普通 lazy restore 都不等于完整可用性验证。

### 2.4 显式严格验证与 exact upload

单 Bundle full verify/upload 与多 source `VerifyExactManifests` /
`UploadExactManifests` 均先运行显式 container verifier，不受
`manifest.verify_content` 影响。它读取完整 Central Directory，并按 CD 原始顺序核对
每个 Local Header、名称、method、flags、时间、attrs、extra、size、CRC 和连续 data
range，证明没有重排、gap、隐藏/重叠 entry；随后要求 index 是 sole final entry，并把
每个 Manifest/Chunk record 与 CD/LFH/data range 精确交叉校验，拒绝未索引对象、索引
不存在对象和额外 metadata。普通 `Open`/`Get` 不承担这些 O(entries) 检查。

严格路径随后验证完整 closure 和对象内容。缺失但未访问的 Chunk 在 `FullVerify` 立即
失败；exact upload 在任何 `Put` 前失败。多 source exact upload 固定执行：

1. 调用方在 OpenManifest 层为每个逻辑 key 选择 current/refs Bundle；已经在目标
   Store 严格存在的依赖不进入 Bundle upload plan。
2. 在任何 admission 或 Put 前，对 plan 中每个实际 source 运行完整
   CD/LFH/index container verifier。
3. 在任何 Put 前，对全部实际 source recorded admissions 调用
   `AdmitWriteFor(recorded.Generation)`；返回 Generation/Salt 必须逐字节相等。
4. 强制验证每个选定 Manifest physical ContentKey、解析并用 customer key 解封 key
   table，并证明该 Manifest 的完整 Chunk 闭包位于同一 source Bundle。
   在任何 Put 前拒绝跨 Manifest 的同一 ContentKey 对应不同 key 或 plaintext size。
5. 并发 worker 逐个处理各来源实际使用的 unique Chunk：强制验证物理 ContentKey，
   使用经过认证的 key-table key 解密/解压并验证解码长度，然后 Put 已验证的物理
   Chunk。实际密钥已包含写入时的 extra-salt 派生结果。这是逐 Chunk 先验证后上传，
   各 Chunk 按有界并发独立推进。
6. 只有全部 Chunk worker 成功后才上传依赖 Manifest,最后发布调用方指定的 current root。
   后续 Chunk 验证或上传失败时,先前已验证的 Chunk 可能留在 Store,但根不会发布。

每个对象使用其 source Bundle 的 recorded admission，不改投最新 generation，不
重新 chunk、压缩、加密、seal key table 或改写上层 `snapshot.cfg`，因此 root
ManifestKey 与 physical bytes 保持不变。任一 admission 预检、依赖验证或 Put 失败
时根不会发布。`bundle/refs` 和 `bundle/admission/*` 不是 Store object，不上传。
`FullVerify` 还拒绝未被任何本地 Manifest 引用的 Chunk；调用方解析
snapshot-specific metadata 后可通过 `ExpectedManifests` 提交精确的本地可达
Manifest 集，从而拒绝无关 Manifest，而无需让 accelerator 解释
`snapshot.cfg`。

读取错误分类、随机访问失败与恢复见[§4](#4-读取错误随机访问与恢复).

## 3. 共享后缀 ZIP 处理

`pkg/tailzip` 是追加在逻辑镜像字节之后的 ZIP 的格式无关实现。`Locate(io.ReaderAt, size, Options)` 返回 payload 边界和后缀长度，`Read` 返回用于提取的有界副本，`Prefix` 暴露未改变的稀疏 payload，`Append` 组合经过验证的后缀，并保留 payload 中的 Hole、显式 Zero 和 Data run。这些操作针对 carrier 内部的逻辑镜像字节（`logicalSize`），而不是物理归档末尾或外层 Manifest Bundle：

```text
Logical View (sparse.Source, Size = logicalSize):
  0                                      payloadSize                      logicalSize
  +-------------------------------------------+---------------------------------+
  |              Sparse Payload               |         Dense ZIP Tail          |
  |        (Image / Disk / VM Memory)         | [LocalHdr+Body...][CD][EOCD+Cmt]|
  +-------------------------------------------+--------------------+---+--------+
                                              ^                    |   |
                                              |                    |   +-- 1. 从逻辑 EOF 向前最多读取 65558 B (22 + 64KiB),
                                              |                    |          定位恰好结束于逻辑 EOF 的 EOCD
                                              +--------------------+------ 2. 根据 EOCD 中的 Central Directory 偏移与长度,
                                                                              校验目录结构并确定 ZIP 起点 (即 payloadSize)
```

默认 profile 接受既有镜像 writer 生成的 ZIP trailer，并把后缀及解码后的条目总量分别限制为 **64 MiB**。具有协议 schema 的调用方可以要求 entry 数量、已知 entry、顺序以及 `STORED` method。定位要求 EOCD 恰好结束于逻辑 EOF，拒绝 multi-disk 和不支持的 ZIP64 后缀，通过 `archive/zip` 检查整数与 Central Directory 边界，并读取每个 entry 验证 CRC。调用方可通过 `tailzip.ErrNotFound` 区分不存在；可识别但损坏或截断的 archive 会报错，不会被当作无 tail。

`pkg/image.AppendConfigZip`、`ReadConfig` 和 `ReadConfigFromFile` 使用这一公共机制。writer 保持历史确定性 entry 名称、`STORED` method、时间戳和字节布局；reader 继续接受已支持的 image ZIP profile。因此 `flatten.Build` 与 `flatten.BuildFromDir` 都间接通过 `pkg/tailzip` 生成 trailer，所有 image config 读取也共享同一 payload 边界实现。各上层业务产物在逻辑视图中的布局如下：

| 业务产物类型                | 逻辑 Payload `[0, payloadSize)` | 逻辑 Dense Tail `[payloadSize, logicalSize)`                 | 消费者解析包                 |
| --------------------------- | ------------------------------- | ------------------------------------------------------------ | ---------------------------- |
| **容器镜像 (Image)**        | EROFS 镜像数据                  | `ZIP(config.json)`                                           | `accelerator/pkg/image`      |
| **Sandbox E（稀疏盘形态）** | 稀疏 root 块设备镜像            | `ZIP(sandbox.runtime.cfg)`                                   | `sandboxer/pkg/sandboxfile`  |
| **Sandbox E（EROFS 形态）** | EROFS 镜像数据                  | `ZIP(config.json, sandbox.runtime.cfg)`                      | `sandboxer/pkg/sandboxfile`  |
| **Snapshot S（内存快照）**  | 稀疏 VM 内存空间                | `ZIP(config.json, state.json, snapshot.cfg[, memory.dirty])` | `sandboxer/pkg/snapshotfile` |

> 注意：Snapshot S 中的 `memory.dirty` 是业务层 dirty 页跟踪位图，与 tarstream 载体层的 `.kuasar.presence.v1` 相互独立，不可混用。

### 共享 reader/writer 与严格 profile

`WithIndexMemoryLimit(bytes)` 可选地在首字节输出之前限制 writer 规划预算。它以 2 MiB 为核算基线，并按规划阶段分配（包括为每个不连续 Data extent 计入 512 字节以覆盖切片扩容、sparse map 编码和元数据副本）累计计费。超限会在读取 payload 或写出 envelope 之前失败。此预算单独覆盖 writer 索引，不替代源、native 和 decoder 预算，也不限制调用方分配的源对象。省略此限制时保持原行为和 artifact 身份。

`tarstream.WriteTo` 接受 `WithBeforeCommit(check)`，用于调用方负责的最终源校验。它在消费全部 payload 和 metadata tail 字节之后、写出 carrier digest marker、归档尾和最终加密记录之前调用一次 check。空源、全 Hole 和全 Zero 源同样执行，不依赖额外 EOF 读取。更早的读写错误会跳过 check；check 错误会保留并使 artifact 不完整。调用方须让源在返回前保持稳定且打开，失败时丢弃输出，不发布文件、上传结果或转换缓存结果。接收方同时要求完整有效的 artifact 和生产者成功结束。此校验不提供原子快照或目标持久性。未配置此选项的 writer 保持原有字节和身份；`CarrierDigest` 不消费 payload，因此拒绝此选项。

`pkg/manifest/transfer.Reader` 打开选定的逻辑 source，并保留源所有权和验证能力。`transfer.Write` 接受 sparse source 与 `WriteOptions`；`BeforeCommit` 在写出 Store 根之前完成调用方源/输出校验。`manifest.RefLocations` 为 manifest-ctl 和 sandboxer 提供统一的合法 location 映射。

`tailzip.ReadCanonical` 与 `EncodeCanonical` 实现 S/E 使用的固定 raw-header profile（强制校验条目顺序、MS-DOS 固定时间戳 `1980-01-01 00:00:00`、禁止额外字段/注释/Data Descriptor）；`ReadFooter` 和 `Names` 提供有界几何信息与角色探测元数据。应用模块传入有序条目名、大小上限，并负责配置/schema 校验。`tailzip.Section` 保持 borrowed run 生命周期及未偏移 prefix 的 ChunkRun 能力；`Append` 保留权威 payload 边界和可复用的摘要 commitment。

Image reader/writer、两个 flatten builder、flatten-ctl 打包、sandboxer S/E reader/builder 及 image assembly/capture/restore 调用方共用这些 helper。Image ZIP 保持原有 writer profile，S/E 保持严格 profile。新 image tarstream envelope 将 EROFS prefix 声明为 payload、config ZIP 声明为 metadata tail；已有 carrier 继续按其原始身份声明读取。

Suffix ZIP 在构造 ZIP reader 前检查 EOCD 条目数量与有界目录布局。local offset 相对后缀起点；带绝对前缀偏移的归档明确报错，避免将 payload 当作元数据。ReaderAt 返回完整缓冲并附带普通 EOF 时接受数据；短读和标记过的源故障仍然返回错误。

规范后缀的 footer、名称和正文读取接口显式接受 `io.ReaderAt` 与逻辑大小。先读 footer 的流程要求真正随机访问；单调 sparse source 使用顺序传输路径。规范编码在输出前拒绝 ZIP64 条目数量。读取完整有界后缀时，即使已返回完整缓冲，也保留后端故障。

## 4. 读取错误、随机访问与恢复

Accelerator 不可变 reader 执行单次尝试，把业务重试/退避留给消费者。`pkg/readerr.Mark(err, retryable)` 保留 `Error()`/`Unwrap()`，只增加 `Retryable() bool`；标记 nil 仍返回 nil。未知错误不代表已确认永久失败。永久语义原因在包装和 joined errors 中保持可见，语义边界上的 marker 对其包装分支具有权威性。正常对象结束 EOF 与已确认缺失保留原接口；必需不可变对象查找把已确认缺失转换为永久原因。已验证格式/范围/完整性/认证失败在掌握语义的位置标记；网络 EOF、半帧/半流、后端内部取消和不透明访问/decode 错误保留原始原因，除非实际验证已经证明损坏。

固定大小 `ReaderAt` 请求只执行一次尝试。short-nil 和已声明字段的真实截断属于结构失败，不能据此拼接第二个响应。来源合同允许时完整普通 EOF 仍然合法；原始 `io.ErrUnexpectedEOF` 即使带完整 buffer 也仍是结构失败。显式源失败在 EOF 兼容处理前保留，随机访问 metadata adapter 不得让 `io.ReadFull` 吞掉完整 buffer 携带的源失败。顺序来源保留单遍语义，不增加 replay。`sparse.Dense` 即使读取失败，也按填充请求时实际消费的字节推进；重读此前 offset 会被拒绝，而不会把随后字节当作失败范围返回。

并行 range read 在返回前 join 所有 worker，包括取消路径。原失败保留，派生同级取消不能替换它，其他 worker 的永久原因仍保留在聚合结果中；返回后 worker 不得继续修改调用者 buffer。既有 Hole/Zero/Data 流式语义保持不变。

直接 CLI 不可变读取向命令所有者报告单次尝试错误。该合同不增加 retry 设置、fallback 来源、健康屏蔽、服务重启或兼容模式。测试覆盖半帧/半流清理、初始化恢复、来源隔离、joined permanent error 与 worker 生命周期；端到端重试策略仍由消费者负责。
