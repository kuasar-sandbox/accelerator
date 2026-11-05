# tarstream 存储格式详解

本文系统阐述 Kuasar accelerator 中的 `pkg/tarstream` 存储格式，涵盖其设计背景、基于双索引的三态稀疏数据表达、明文与加密载体的物理字节布局、三层结构化内容身份（Digest/HMAC）的计算机制，以及流式写入、随机读取与完整性验证的实现流程。

> 说明基于本地 accelerator 源码（基准提交 `cadf305`），重点描述当前写出的 **presence v1 + GNU PAX sparse 1.0** 格式与 **encrypted tarstream v1** 信封；对旧无 presence 载体的读取兼容行为单独说明。通用工件契约见 [文件工件与载体](file-artifacts_zh.md)。

## 目录

- [tarstream 存储格式详解](#tarstream-存储格式详解)
  - [目录](#目录)
  - [一、概述：为什么需要 tarstream](#一概述为什么需要-tarstream)
    - [1.1 快照数据的三态语义：Hole、Zero、Data](#11-快照数据的三态语义holezerodata)
    - [1.2 为什么普通文件或单独 GNU sparse TAR 不够](#12-为什么普通文件或单独-gnu-sparse-tar-不够)
    - [1.3 设计目标与体系位置](#13-设计目标与体系位置)
  - [二、tarstream 明文存储格式](#二tarstream-明文存储格式)
    - [2.1 三层坐标空间全景映射](#21-三层坐标空间全景映射)
    - [2.2 明文 Canonical TAR 的六段物理布局](#22-明文-canonical-tar-的六段物理布局)
    - [2.3 段 \[1\]：presence entry（逻辑覆盖位图）](#23-段-1presence-entry逻辑覆盖位图)
      - [2.3.1 64 字节头部布局（Little-Endian）](#231-64-字节头部布局little-endian)
      - [2.3.2 位图编码与尾位清零约束](#232-位图编码与尾位清零约束)
      - [2.3.3 Zero 页省略规则与三态还原真值表](#233-zero-页省略规则与三态还原真值表)
    - [2.4 段 \[2\] \& \[3\]：PAX 扩展头与业务成员 TAR Header](#24-段-2--3pax-扩展头与业务成员-tar-header)
    - [2.5 段 \[4\] \& \[5\]：GNU sparse 1.0 map 与 packed data](#25-段-4--5gnu-sparse-10-map-与-packed-data)
    - [2.6 段 \[6\]：digest marker 与归档结束块](#26-段-6digest-marker-与归档结束块)
    - [2.7 完整字节布局算例](#27-完整字节布局算例)
  - [三、ZIP 尾部与三层内容身份](#三zip-尾部与三层内容身份)
    - [3.1 逻辑 EOF 处的 ZIP 尾部定位](#31-逻辑-eof-处的-zip-尾部定位)
    - [3.2 镜像、Sandbox E、Snapshot S 的逻辑布局](#32-镜像sandbox-esnapshot-s-的逻辑布局)
    - [3.3 三层结构化内容身份（Digest）计算](#33-三层结构化内容身份digest计算)
    - [3.4 仅替换 ZIP 尾部时的身份快速派生](#34-仅替换-zip-尾部时的身份快速派生)
  - [四、tarstream 加密格式（encrypted v1）](#四tarstream-加密格式encrypted-v1)
    - [4.1 加密范围与物理信封布局](#41-加密范围与物理信封布局)
    - [4.2 48 B 明文前缀与 64 B 认证头部](#42-48-b-明文前缀与-64-b-认证头部)
    - [4.3 密钥派生、Nonce、AAD 与加解密流程](#43-密钥派生nonceaad-与加解密流程)
    - [4.4 记录切分几何：首条记录对齐 `packedStart`](#44-记录切分几何首条记录对齐-packedstart)
    - [4.5 加密载体的外部身份（HMAC）](#45-加密载体的外部身份hmac)
  - [五、写入、随机读取与验证机制](#五写入随机读取与验证机制)
    - [5.1 两阶段写入管线（`WriteTo`）](#51-两阶段写入管线writeto)
    - [5.2 明文与加密随机读取寻址（`SourceAt`）](#52-明文与加密随机读取寻址sourceat)
    - [5.3 验证强度对比：`SourceAt` vs `SourceFrom` vs `ReadFrom`](#53-验证强度对比sourceat-vs-sourcefrom-vs-readfrom)
    - [5.4 核心接口选型速查](#54-核心接口选型速查)
  - [六、格式约束、兼容性与常见误区](#六格式约束兼容性与常见误区)
    - [6.1 格式约束与性能特征速查表](#61-格式约束与性能特征速查表)
    - [6.2 旧载体兼容与降级边界](#62-旧载体兼容与降级边界)
    - [6.3 常见误区对照](#63-常见误区对照)

---

## 一、概述：为什么需要 tarstream

在 Kuasar 中，容器镜像、沙箱根盘（Sandbox E）以及虚拟机内存快照（Snapshot S）均需封装为单一文件，以便在本地磁盘、共享文件系统与远端仓库之间持久化和分发；同时，运行时还需在无需预先解包的前提下，直接按逻辑偏移进行随机读取。要理解为何普通文件或常规 `tar` 归档无法兼顾上述需求，首先需要明确增量快照数据的“三态”语义。

### 1.1 快照数据的三态语义：Hole、Zero、Data

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

### 1.2 为什么普通文件或单独 GNU sparse TAR 不够

明确 `Hole`、`Zero`、`Data` 的语义差异后，对比常见存储形式可以看出，传统方案难以同时兼顾“空间效率”与“语义完整性”：

```text
Ordinary File / Dense TAR:
  [Data A][0000...][0000...][Data B]  --> 传输大量零字节；且 Hole 与 Zero 混同为 0x00

GNU sparse 1.0 TAR only:
  sparse map: [Data A], [Data B]      --> 省去传输，但未存储区间无法区分是 Hole 还是显式 Zero

Kuasar tarstream (presence v1 + GNU sparse 1.0):
  presence bitmap (0=Hole, 1=Present)
                 +                    --> 既省去 Hole 与整页 Zero 的物理字节，又完整保留三态
  sparse map      (0=Omitted, 1=Data)
```

| 载体形式                                 | 省略 Hole 传输 | 省略整页 Zero 传输    | 区分 Hole 与 Zero | 标准 `tar` 可直接解包逻辑字节 |
| ---------------------------------------- | -------------- | --------------------- | ----------------- | ----------------------------- |
| 普通连续文件                             | 否             | 否                    | 否                | 不适用                        |
| 普通 Dense TAR                           | 否             | 否                    | 否                | 是                            |
| 宿主机 Sparse File 直接流式读取          | 否（读出零）   | 否                    | 否                | 不适用                        |
| 单独 GNU PAX sparse 1.0 TAR              | 是             | 否（若省略则变 Hole） | 否                | 是                            |
| **tarstream (presence v1 + sparse 1.0)** | **是**         | **是（4 KiB 整页）**  | **是**            | **是**                        |

> 注意：tarstream 的空间节省来自**稀疏跳过 Hole** 和**省略已声明的完整 4 KiB Zero 页**，不依赖通用压缩；若输入源将全零内容标记为普通 `Data`，writer 不会扫描字节将其自动转为 `Zero`。

### 1.3 设计目标与体系位置

为此，tarstream 在标准稀疏 TAR 的基础上引入轻量级的覆盖位图，并向上层业务提供统一的稀疏随机读取抽象。其在整体架构中的位置如下：

```text
+-------------------------------------------------------+
| Upper Consumers (Image / Sandbox E / Snapshot S)      |  <-- 上层业务：父层引用、叠加策略、配置解析、发布事务
+---------------------------+---------------------------+
                            |
                            v
+-------------------------------------------------------+
| Logical View: sparse.Source (Size + RunAt + ReadAt)   |  <-- 统一稀疏逻辑源：
|   [0, payloadSize)   : Sparse Payload (Hole/Zero/Data)|      前段稀疏三态 Payload
|   [payloadSize, Size): Optional Dense ZIP Tail        |      后段可选 Dense ZIP 元数据尾部
+---------------------------+---------------------------+
                            |
                            v
+-------------------------------------------------------+
| Plaintext Canonical tarstream                         |  <-- 明文载体：
|   .kuasar.presence.v1 + PAX + Sparse 1.0 + Marker     |      覆盖位图 + GNU 稀疏 TAR + 逻辑身份 Marker
+---------------------------+---------------------------+
                            |
                            v
+-------------------------------------------------------+
| Encrypted tarstream v1 (Optional via WithCodec)       |  <-- 加密信封：
|   48B Prefix + 81B Sealed Header + 4KiB Auth Records  |      明文前缀 + 认证头 + 4 KiB 独立认证记录
+-------------------------------------------------------+
```

---

## 二、tarstream 明文存储格式

确定“通过双索引表达三态数据”的设计思路后，接下来的关键在于：如何在保持标准 `tar` 工具兼容性的前提下，将该结构编码为单一归档文件。

tarstream 遵循标准 ustar/PAX 归档规范，在承载业务数据的稀疏成员之前置入一个轻量位图成员，用于记录各 4 KiB 逻辑页是否被本层覆盖；并在归档末尾追加一个定长尾标成员，用于记录整份载体的内容摘要。本章首先梳理载体涉及的三种偏移坐标空间，随后按物理存储顺序依次解析六个组成部分。

### 2.1 三层坐标空间全景映射

理解 tarstream 的关键是区分**业务逻辑偏移**、**明文 TAR 偏移**和**加密文件偏移**三个坐标空间：

```text
[1. 业务逻辑文件视图] (logicalSize = payloadSize + tailSize)
  0                                                payloadSize        logicalSize
  +--------------+--------------+--------------+--------+------------------+
  |    Data A    |     Hole     |  Zero (4KiB) | Data B |  Dense ZIP Tail  |
  +------+-------+--------------+--------------+---+----+--------+---------+
         |              (omitted on wire)          |             |
         +----------------------+------------------+-------------+
                                v                                v
[2. 明文 Canonical TAR 视图] (plaintextSize, 512 B aligned)
  0                                       packedStart       packedStart+packedSize
  +----------+--------+--------+----------+-------+----+--------+---------+----------------+
  |[1]Presen.| [2]PAX | [3]Hdr |[4]Sparse |Data A |Data|ZIP Tail| Padding |[6]Marker + EOF |
  |  Member  | ExtHdr | Member |   Map    |       | B  |        | (0~511B)| (Fixed 2048 B) |
  +----------+--------+--------+----------+-------+----+--------+---------+----------------+
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

| 坐标空间         | 范围                 | 偏移含义与特点                                                                                        |
| ---------------- | -------------------- | ----------------------------------------------------------------------------------------------------- |
| **业务逻辑文件** | `[0, logicalSize)`   | 前段 `[0, payloadSize)` 为稀疏 payload；后段 `[payloadSize, logicalSize)` 为可选 dense tail（如 ZIP） |
| **明文 TAR**     | `[0, plaintextSize)` | 包含 TAR 结构开销与紧密排列的 `packed data`；512 B 块对齐                                             |
| **加密信封**     | `[0, encryptedSize)` | 将完整明文 TAR 切分为 `N` 条独立认证记录，每条记录附加 `1 B flag + 16 B tag = 17 B` 开销              |

### 2.2 明文 Canonical TAR 的六段物理布局

明文 canonical tarstream 由六个物理段落组成（对应 **3 个真实 TAR 文件成员**：`[1] presence`、`[3] 业务成员`、`[6] digest marker`，其中 `[2] PAX` 是 `[3]` 的扩展头，`[4] sparse map` 与 `[5] packed data` 共同组成 `[3]` 的数据体）：

```text
明文 TAR 偏移 0
  |
  +-- 段 [1]: presence 普通文件成员 (.kuasar.presence.v1)
  |           +-- 512 B  ustar header (typeflag='0', mode=0444, size=64+len(bitmap))
  |           +--  64 B  presence 头部 (magic "KUPRES1\n" + 元数据 + CRC32)
  |           +--   M B  bitmap 位图字节
  |           +--   P B  零填充 (补齐至 512 B 边界)
  |
  +-- 段 [2]: 业务成员的 PAX 扩展头 (PaxHeaders.0/<base>)
  |           +-- 512 B  ustar header (typeflag='x', mode=0644, size=len(PAX records))
  |           +-- 512*k  排序后的 PAX key=value 记录 + 零填充至 512 B
  |
  +-- 段 [3]: 业务成员 TAR Header (<dir>/GNUSparseFile.0/<base>)
  |           +-- 512 B  ustar header (typeflag='0', mode=0644, size=storedSize)
  |
  +-- 段 [4]: GNU sparse 1.0 map (位于业务成员数据体开头)
  |           +-- 512*m  十进制文本 (含末尾 (logicalSize, 0) 哨兵) + 零填充至 512 B
  |
  +-- 段 [5]: packed data (紧随 sparse map 之后, 起点即 packedStart)
  |           +-- packedSize B  按逻辑偏移升序紧密拼接的已存储 extent (含 dense tail)
  |           +-- payloadPad B  零填充 (使 storedSize = mapLen + packedSize 补齐至 512 B)
  |
  +-- 段 [6]: 身份 marker 与归档结束块 (固定 4 * 512 = 2048 B)
              +-- 512 B  marker ustar header (.kuasar.digest.<64位小写hex>, typeflag='0', mode=0444, size=40)
              +-- 512 B  40 B marker body (payloadSize + payloadCommitment) + 472 B 零填充
              +-- 1024 B 两个 512 B 全零 TAR 结束块 (EOF)
```

> **Canonical 头部约束**：writer 生成的所有 ustar 头部将 `uid`、`gid`、`mtime` 固定编码为八进制 `0`，魔数为 `"ustar\x00"` + 版本 `"00"`。读取端对 **presence 头（段 `[1]`）、PAX 扩展头（段 `[2]`）和业务成员 TAR 头（段 `[3]`）**会与重构出的 writer 模板逐字节比对；而 **digest marker 头（段 `[6]`）**则按字段校验 ustar 魔数、头部校验和、`typeflag`（`'0'` 或 `0`）、`size == 40`、名称格式及 40 B 内容与 472 B 零填充，不要求与完整 writer 模板逐字节全等。

### 2.3 段 [1]：presence entry（逻辑覆盖位图）

以下按归档从前到后的物理顺序依次说明各段结构。位于归档起始位置的是段 `[1]` 覆盖位图：它以每 4 KiB 逻辑页 1 bit 的紧凑编码标记该页是否被本层覆盖，从而弥补标准稀疏 TAR 无法区分 `Hole` 与 `Zero` 的不足。

#### 2.3.1 64 字节头部布局（Little-Endian）

`.kuasar.presence.v1` 成员的数据体以固定 64 字节头部开始，所有多字节整数均为 **little-endian**：

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

#### 2.3.2 位图编码与尾位清零约束

设 `pages = ceil(payloadSize / 4096)`，逻辑页 `i`（`0 <= i < pages`）对应 `bitmap[i / 8]` 的第 `i % 8` 位（LSB 对应最低页号）：

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

- **容量开销**：`1 GiB` payload 对应 `32 KiB` 位图；`1 TiB` 上限对应 `32 MiB` 位图；空 payload（`payloadSize = 0`）时 `bitmapLength = 0`，但 64 B presence 头部依然写出。
- **Hole 对齐约束**：在 `[0, payloadSize)` 内，所有 `Hole` 的起点必须按 `4096 B` 对齐，终点必须按 `4096 B` 对齐或恰好位于 `payloadSize`。

#### 2.3.3 Zero 页省略规则与三态还原真值表

writer 仅对 **`[0, payloadSize)` 内单个 `sparse.Zero` run 覆盖的完整 4 KiB 对齐页**省略物理字节；不满足整页对齐的 Zero 头尾片段（以及位于 dense tail 内的 Zero）均保留在 sparse extent 中，作为字面零写出：

```text
单个 Zero run 跨越非对齐边界: [2048, 10240)
  Page 0 [0, 4096)       Page 1 [4096, 8192)     Page 2 [8192, 12288)
  +---------+---------+ +---------------------+ +---------+---------+
  | (Other) |Zero Frag| |   Full 4 KiB Zero   | |Zero Frag| (Other) |
  +---------+----+----+ +----------+----------+ +----+----+---------+
                 |                 |                 |
                 v                 v                 v
         进入 sparse extent   从 sparse map 省略   进入 sparse extent
         (合成字面零落盘)     (仅靠 presence=1)    (合成字面零落盘)
```

读取端在 `[0, payloadSize)` 内联合查询 `presence` 位图与 `GNU sparse map` 即可无歧义还原三态：

| `presence` 位 (`bit`) | 是否被 `sparse extent` 覆盖 | 物理 TAR 是否落盘字节 | 还原出的 `RunAt` 状态        | 单层 `ReadAt` 返回字节 |
| --------------------- | --------------------------- | --------------------- | ---------------------------- | ---------------------- |
| `0`                   | 否                          | 否                    | **Hole**                     | 合成 `0x00`            |
| `1`                   | 否                          | 否                    | **Zero**（整页省略）         | 合成 `0x00`            |
| `1`                   | 是                          | 是                    | **Data**（含非整页 Zero）    | 读取落盘字节           |
| `0`                   | 是                          | -                     | **格式非法**（直接报错拒绝） | -                      |

### 2.4 段 [2] & [3]：PAX 扩展头与业务成员 TAR Header

解析完段 `[1]` 的覆盖位图后，读取器即可区分未覆盖的 `Hole` 与本层已覆盖页。紧随其后的段 `[2]` 与段 `[3]` 为标准 TAR 描述业务文件元信息（真实文件名、逻辑总大小、落盘大小等）的头部结构。业务成员名经 `normalizeName`（去除前导 `/` 与 `./`）规范化后，生成如下两段头部：

| 头部              | ustar `name` 字段（最长 100 B）                                 | `typeflag` | `mode` | ustar `size` 字段                                        |
| ----------------- | --------------------------------------------------------------- | ---------- | ------ | -------------------------------------------------------- |
| **段 [2] PAX 头** | `"PaxHeaders.0/" + truncate(Base(name), 80)`                    | `'x'`      | `0644` | PAX 记录字节总长                                         |
| **段 [3] 业务头** | `truncate(Join(Dir(name), "GNUSparseFile.0", Base(name)), 100)` | `'0'`      | `0644` | `storedSize`（超八进制范围时写 `0`，由 PAX `size` 承载） |

段 `[2]` 数据体中的 PAX 记录采用标准 `"<len> <key>=<value>\n"` 格式，**严格按 key 字典序升序排列**：

| PAX Key（按字典序）   | 值含义                                               | 示例（成员名 `sparse/image`）            |
| --------------------- | ---------------------------------------------------- | ---------------------------------------- |
| `GNU.sparse.major`    | 固定 `"1"`                                           | `22 GNU.sparse.major=1\n`                |
| `GNU.sparse.minor`    | 固定 `"0"`                                           | `22 GNU.sparse.minor=0\n`                |
| `GNU.sparse.name`     | 规范化后的真实业务成员名                             | `32 GNU.sparse.name=sparse/image\n`      |
| `GNU.sparse.realsize` | `logicalSize`（十进制，含 dense tail）               | `29 GNU.sparse.realsize=17408\n`         |
| `KUASAR.payload.size` | `payloadSize`（十进制，划分 payload 与 dense tail）  | `29 KUASAR.payload.size=16384\n`         |
| `KUASAR.presence`     | 固定 `"1"`（声明前置 `.kuasar.presence.v1` 生效）    | `21 KUASAR.presence=1\n`                 |
| `path`                | 混淆后的 ustar 传输路径                              | `37 path=sparse/GNUSparseFile.0/image\n` |
| `size`                | `storedSize = len(sparseMap) + packedSize`（十进制） | `13 size=9728\n`                         |

### 2.5 段 [4] & [5]：GNU sparse 1.0 map 与 packed data

业务头部之后即为业务成员的数据体（段 `[4]` 与段 `[5]`）。其中**段 [4]（GNU sparse 1.0 map）**是以 `\n` 分隔的十进制文本偏移映射表，用于记录实际落盘的逻辑区间，末尾**始终追加一个 `(logicalSize, 0)` 哨兵 extent**（计入总数 `count`，最多 `1<<20` 个 extent）并补零对齐至 512 B 边界；**段 [5]（packed data）**紧随其后，按逻辑偏移升序连续存放所有实际落盘的数据字节：

```text
假设逻辑文件 (logicalSize = 17408, payloadSize = 16384) 已存储的逻辑区间为:
  Extent 0: [0, 4096)       --> Data A (4096 B)
  Extent 1: [12288, 17408)  --> Data B (4096 B) + 相邻合并的 ZIP Tail (1024 B) = 5120 B
  Sentinel: [17408, +0)     --> 固定结尾哨兵

+----------------------+       +-----------------------------------------------+
| [4] GNU sparse map   |       | [5] packed data (contiguous stored bytes)     |
+----------------------+       +-------------------+-------------------+-------+
| 3\n                  | (cnt) | Data A (4096 B)   | Data B (4096 B)   |ZIPTail|
| 0\n                  | ------+------------------>|                   |(1024B)|
| 4096\n               |       | TAR Offset:       | TAR Offset:       |       |
| 12288\n              | ------+-packedStart + 0   | packedStart + 4096|       |
| 5120\n               |       +-------------------+-------------------+-------+
| 17408\n              |
| 0\n                  | (sentinel)
| [0x00 pad to 512 B]  |
+----------------------+
```

三种核心长度关系如下：

| 长度名称      | 计算公式                           | 含义                                                                    |
| ------------- | ---------------------------------- | ----------------------------------------------------------------------- |
| `logicalSize` | `payloadSize + tailSize`           | 业务层看到的完整逻辑文件大小（对应 `GNU.sparse.realsize`）              |
| `packedSize`  | $\sum \text{extent}_i.\text{Size}$ | 所有实际落盘的业务数据字节总和（含 dense tail）                         |
| `storedSize`  | `mapLen + packedSize`              | 业务成员在 TAR 中的数据体长度（对应 PAX `size`，不含末尾 `payloadPad`） |

### 2.6 段 [6]：digest marker 与归档结束块

业务数据体按 512 B 补齐后，归档末尾固定为 `2048 B`（`4 × 512 B`）的收尾区域，用于写入内容身份标记（digest marker）与标准 TAR 归档结束块：

```text
+-----------------------------------------------------------------------+ 0
| Marker TAR Header (512 B)                                             |  <-- 成员名即 plainDigest
|   name = ".kuasar.digest.<64-char-lowercase-hex>"                     |
|   mode = 0444, size = 40, typeflag = '0', magic = "ustar\x0000"       |
+-------------------------------------------+---------------------------+ 512
| Marker Body (40 B)                        | Zero Padding (472 B)      |  <-- 保存 payloadCommitment
|   [0..8)  : payloadSize (uint64 BE)       |   [40..512) must be 0x00  |      供替换尾部时快速派生
|   [8..40) : payloadCommitment (32 B raw)  |                           |
+-------------------------------------------+---------------------------+ 1024
| Archive End Block 1 (512 B, must be all 0x00)                         |
+-----------------------------------------------------------------------+ 1536
| Archive End Block 2 (512 B, must be all 0x00)                         |
+-----------------------------------------------------------------------+ 2048 (TAR EOF)
```

### 2.7 完整字节布局算例

综合上述六段结构，以下结合一个包含 4 个逻辑页（`payloadSize = 16384`）与 `1024 B` 尾部元数据（总 `logicalSize = 17408`，成员名 `disk`）的具体算例，给出逻辑视图与明文 TAR 物理偏移的完整对应关系：

| 阶段/段落                | 明文 TAR 物理范围 | 长度   | 对应逻辑范围 / 内容说明                                                      |
| ------------------------ | ----------------- | ------ | ---------------------------------------------------------------------------- |
| **[1] presence header**  | `[0, 512)`        | 512 B  | `.kuasar.presence.v1` TAR header (`size = 65`)                               |
| **[1] presence body**    | `[512, 1024)`     | 512 B  | `64 B` header + `1 B` bitmap (`0x0d`) + `447 B` 零填充                       |
| **[2] PAX header**       | `[1024, 1536)`    | 512 B  | `PaxHeaders.0/disk` TAR header (`typeflag = 'x'`)                            |
| **[2] PAX records**      | `[1536, 2048)`    | 512 B  | 8 条排序后的 PAX 记录文本 + 零填充                                           |
| **[3] 业务 TAR header**  | `[2048, 2560)`    | 512 B  | `GNUSparseFile.0/disk` TAR header (`storedSize = 9728`)                      |
| **[4] sparse map**       | `[2560, 3072)`    | 512 B  | 2 个非空 extent + `(17408, 0)` 哨兵文本 + 零填充                             |
| **[5] packed: Data A**   | `[3072, 7168)`    | 4096 B | 逻辑 `[0, 4096)`（起点 `packedStart = 3072`）                                |
| *(省略 Hole 与 Zero)*    | *(无物理字节)*    | `0 B`  | 逻辑 `[4096, 8192)` Hole 与 `[8192, 12288)` Zero 整页均不落盘                |
| **[5] packed: Data B**   | `[7168, 11264)`   | 4096 B | 逻辑 `[12288, 16384)`                                                        |
| **[5] packed: ZIP tail** | `[11264, 12288)`  | 1024 B | 逻辑 `[16384, 17408)`（与 Data B 合并为同一 extent，总 `packedSize = 9216`） |
| **[6] marker header**    | `[12288, 12800)`  | 512 B  | `.kuasar.digest.<hex>` TAR header (`size = 40`)                              |
| **[6] marker body**      | `[12800, 13312)`  | 512 B  | `8 B` `payloadSize(16384)` + `32 B` `payloadCommitment` + `472 B` 零填充     |
| **[6] TAR 结束块**       | `[13312, 14336)`  | 1024 B | 两个 512 B 全零块（`plaintextSize = 14336`）                                 |

---

## 三、ZIP 尾部与三层内容身份

第二章定义了单个稀疏数据主体的明文归档结构。然而在实际业务场景中，一个可独立交付的工件除主体数据（payload）外，还需附带镜像配置、虚拟机设备状态或脏页位图等结构化元数据。

若将元数据拆分为外部伴随文件，在跨节点分发或远端存储上传时难以保证“数据与配置”的原子一致性；但若将元数据与数 GiB 的主体数据简单拼接并计算单一整体哈希，则每次更新数 KB 的尾部配置都必须重新读取并散列整个主体数据。为此，tarstream 采用了**逻辑末尾内嵌 ZIP 尾部**与**三层结构化内容身份**相结合的设计：将业务元数据封装为紧凑的 ZIP 归档追加于逻辑文件末尾，同时将主体数据与尾部元数据的摘要解耦计算，最终合成为整体内容身份。

### 3.1 逻辑 EOF 处的 ZIP 尾部定位

业务的 ZIP 尾部位于**逻辑文件的末尾（`logicalSize`）**，而非物理 TAR 文件的末尾（物理 TAR 末尾是 marker 与结束块）。定位 ZIP 时，先通过 `tarstream.SourceAt` 打开逻辑视图，再由 `tailzip.Locate` 从逻辑 EOF 逆向扫描：

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

- **单点边界真源**：ZIP 起点 `Tail.Offset` 即为业务层的 `payloadSize`，上层将 `[0, Tail.Offset)` 暴露给块设备或 UFFD，确保运行时不会读到尾部的元数据 ZIP。
- **确定性编码**：`tailzip.Encode` 生成确定性的 `STORED`（不压缩）ZIP；Sandbox E 与 Snapshot S 等严格消费者进一步使用 `tailzip.EncodeCanonical`，强制校验条目顺序、MS-DOS 固定时间戳（1980-01-01 00:00:00）、禁止额外字段/注释/Data Descriptor，且整个 ZIP tail 上限为 `64 MiB`。

### 3.2 镜像、Sandbox E、Snapshot S 的逻辑布局

tarstream 载体层统一提供“稀疏 payload + 可选 dense tail”布局，各上层业务在尾部 ZIP 中封装各自的元数据条目：

| 业务产物类型                | 逻辑 Payload `[0, payloadSize)` | 逻辑 Dense Tail `[payloadSize, logicalSize)`                 | 消费者解析包                 |
| --------------------------- | ------------------------------- | ------------------------------------------------------------ | ---------------------------- |
| **容器镜像 (Image)**        | EROFS 镜像数据                  | `ZIP(config.json)`                                           | `accelerator/pkg/image`      |
| **Sandbox E（稀疏盘形态）** | 稀疏 root 块设备镜像            | `ZIP(sandbox.runtime.cfg)`                                   | `sandboxer/pkg/sandboxfile`  |
| **Sandbox E（EROFS 形态）** | EROFS 镜像数据                  | `ZIP(config.json, sandbox.runtime.cfg)`                      | `sandboxer/pkg/sandboxfile`  |
| **Snapshot S（内存快照）**  | 稀疏 VM 内存空间                | `ZIP(config.json, state.json, snapshot.cfg[, memory.dirty])` | `sandboxer/pkg/snapshotfile` |

> 注意：Snapshot S 中的 `memory.dirty` 是业务层 dirty 页跟踪位图，与 tarstream 载体层的 `.kuasar.presence.v1` 相互独立，不可混用。

### 3.3 三层结构化内容身份（Digest）计算

tarstream 的 `plainDigest` **不是**对整个物理 TAR 文件做 SHA-256，而是对逻辑覆盖结构、有效字节、成员名和边界做三层树状散列（所有多字节整数均为 **big-endian** `uint64`）：

```text
+------------------------------------------------------------------+
| Layer 1: payloadCommitment                                       |
|   SHA256(                                                        |
|     "kuasar/tarstream/payload\x00"                               |
|     || uint64BE(payloadSize)                                     |
|     || uint64BE(presentIntervalCount)                            |  <-- 由 presence 合并出的连续已覆盖区间 (bit=1)
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

**该三层哈希结构具备以下两项核心设计性质：**
1. **编码表示无关性**：第一层按 `presence` 导出的逻辑已覆盖区间计算摘要，并为已省略的整页 `Zero` 注入合成零字节。因此，全零页无论在物理上按 `Data` 落盘还是按 `Zero` 省略，所计算的 `payloadCommitment` 完全一致；若将 `Zero` 篡改为 `Hole`，则会改变已覆盖区间列表，从而改变最终摘要。
2. **支持 $O(\text{tailSize})$ 尾部元数据更新**：`payloadCommitment` 独立持久化于 marker body 中，上层更新尾部配置时无需重新读取与散列主体 payload。

### 3.4 仅替换 ZIP 尾部时的身份快速派生

当上层仅替换 dense metadata tail（例如重写 Sandbox/Snapshot 尾部 ZIP 配置）而保持 `[0, payloadSize)` 不变时，可通过 `ComposeDigest` / `CarrierDigest` 直接合成新身份：

```text
Marker Body (existing)  --> (payloadSize, payloadCommitment) --+
                                                               +--> ComposeDigest --> new plainDigest (零 payload I/O)
Replacement ZIP Tail    --> new tailDigest + new logicalSize --+
```

---

## 四、tarstream 加密格式（encrypted v1）

前三章所述的明文格式解决了可信环境下的稀疏存储、元数据封装与完整性校验问题。但在生产环境中，快照与镜像工件通常需要存储在多租户共享介质（如共享 NFS 或远端对象存储）上，明文载体将面临两方面安全挑战：

- **敏感数据与内部结构暴露**：虚拟机内存快照与沙箱磁盘可能包含业务密钥、环境变量等运行时敏感信息；且即使仅对业务数据区加密，明文 TAR 的成员名称、稀疏覆盖位图（反映内存或磁盘空间分布特征）以及末尾的内容摘要仍会对外暴露。
- **整包加密与随机读取之间的矛盾**：若对整个 TAR 文件采用传统单流加密，读取端为按需访问任意 4 KiB 页面或读取文件末尾的 ZIP 配置，都必须从文件起始位置顺序解密，从而丧失随机读取能力；同时，无密钥的明文哈希也无法防御攻击者将载荷与末尾摘要一并替换。

为此，tarstream 在明文格式外层定义了**支持随机寻址的分块认证加密格式（encrypted v1）**：将完整的明文 Canonical TAR 切分为一系列独立的 `4 KiB` AES-256-GCM 认证记录。该设计既将内部文件名、位图、稀疏索引、业务数据与明文摘要完整封装于密文之中，又允许读取端按偏移独立定位并认证解密所需的记录块。

### 4.1 加密范围与物理信封布局

启用 `WithCodec` 后，第二章所述的**完整明文 Canonical TAR**（从段 `[1] presence` 到段 `[6] 两个全零结束块`）整体作为加密器的输入明文流，切分为一系列独立认证的 AES-256-GCM 记录：

```text
Encrypted File Layout (encryptedSize = 48 + 81 + plaintextSize + 17 * recordCount):
  Offset 0              48                      129
  +---------------------+-----------------------+-----------------------+-----+-----------------------+
  |  48 B Clear Prefix  |  81 B Sealed Header   |      Record 0         | ... |     Record N-1        |
  | (magic+ver+len+salt)| 1B flag+64B enc+16Btag| 1B flag+F B enc+16Btag|     | 1B flag+K B enc+16Btag|
  +---------------------+-----------------------+-----------------------+-----+-----------------------+
                        |                       |<----- Sealed Complete Canonical Plaintext TAR ----->|
                        +-- sequence = 0        +-- sequence = 1              +-- sequence = N
```

- **隐藏内部元数据**：业务成员名称、`presence` 位图、`sparse map`、ZIP 内容以及 `.kuasar.digest.<hex>` 中的明文摘要全部被加密保护，不会以明文暴露在磁盘上。
- **固定记录开销**：头部记录和每条数据记录均采用 `1 B flag (0x01) + n B ciphertext + 16 B GCM tag` 格式，单条记录固定增加 **17 字节**，不在记录中重复存储 nonce。

### 4.2 48 B 明文前缀与 64 B 认证头部

由于内层明文 TAR 已整体加密，读取端在打开文件时需首先获取密钥派生参数与分块几何信息。为此，加密信封在文件起始处依次布置了 `48 B` 明文前缀（存放魔数与随机盐值）与 `81 B` 认证头部（解密后为 `64 B` 全局几何参数）：

**1. 48 字节明文前缀（Clear Prefix，文件偏移 `[0, 48)`，Big-Endian）：**

```text
  0                   8         10        12            16                                  48
  +-------------------+---------+---------+-------------+-----------------------------------+
  |    magic (8 B)    | ver(2B) | size(2B)| reserved(4B)|        artifactSalt (32 B)        |
  | 89 "KTSENC\n"     |    1    |    48   |  0x00000000 |     random per-artifact salt      |
  +-------------------+---------+---------+-------------+-----------------------------------+
```

**2. 64 字节认证头部（解密后明文，文件偏移 `[48, 129)` 解密所得，Big-Endian）：**

```text
  0                   8                   16                  24                  32
  +-------------------+-------------------+-------------------+-------------------+
  | plaintextSize(8B) |  packedStart (8B) |  packedSize (8B)  |  recordCount (8B) |  <-- uint64 BE
  |  full TAR bytes   | packed data start | packed data bytes | data record count |
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
3. **内外层几何一致性（打开阶段解析完内层 TAR 前缀后校验）**：打开内层明文 TAR 并解析完前置元数据后，核对内层实际的 `packed data` 起始偏移与长度，要求与外层认证头部声明的 `packedStart`、`packedSize` 严格一致。

### 4.3 密钥派生、Nonce、AAD 与加解密流程

单纯将数据切分为独立加密块仍不足以防御块重排、跨文件拼接或尾部截断攻击。为此，tarstream 为每个工件派生独立的加密密钥，并将明文前缀、64 B 认证头部、记录序号及实际明文长度共同绑定至每条记录的 GCM 附加认证数据（AAD）中：

```text
customerKey (32 B) --+
                     +--> HMAC-SHA256("kuasar/tarstream/aes-gcm/artifact-key/v1\x00" || salt)
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

由于 `recordAAD(i)` 同时绑定了 `clearPrefix`（含 salt）、`plainHeader`（含全局几何布局）、记录序号 `i` 与当前记录明文长度，**跨文件拼接记录、调换记录顺序、截断尾部记录或篡改任意字节**等异常均会被布局、长度或 GCM 认证检查拒绝。

### 4.4 记录切分几何：首条记录对齐 `packedStart`

若从明文 TAR 偏移 `0` 处直接按固定 `4 KiB` 切分记录，前置 TAR 头部与元数据会使后续业务数据整体产生偏移，导致按 `4 KiB` 对齐的业务页面普遍跨越两条加密记录。为避免这一额外开销，**Record 0 的明文长度 `F` 被设定为 `packedStart` 对 `4096` 取模的结果（整除时取 `4096`），从而确保业务数据区（`packed data`）的起始偏移 `packedStart` 始终对齐在某条记录的起始边界上**（例如下方算例中 `packedStart = 3072` 落在 `Record 1` 起点；若前置元数据更长，如 `packedStart = 7168`，则 `F = 3072`，`packedStart` 将落在 `Record 2` 起点。注意：该规则保证的是 `packedStart` 对齐于记录边界；后续单个 payload 页或 extent 是否跨记录，仍取决于其前方已存储 extent 的总长度是否保持 `4 KiB` 的整数倍）：

```text
Plaintext TAR (以 2.7 节算例: packedStart = 3072, plaintextSize = 14336):
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
  （沿用 2.7 节算例：$48 + 81 + 14336 + 17 \times 4 = 14533\text{ B}$）

### 4.5 加密载体的外部身份（HMAC）

在加密模式下，内层的 `.kuasar.digest.<hex>` 已随 TAR 归档一并加密。为便于外部系统（如缓存管理或制品仓库）在不暴露明文内容指纹的前提下执行去重与完整性锚定，加密载体使用 `customerKey` 对内部 `plainDigest` 计算 `HMAC-SHA256` 作为外部身份标识：

```text
                     +-- Plaintext (no Codec) --> "digest:" + hex(plainDigest)
plainDigest (32 B) --+
                     +-- Encrypted (with Codec)-> "hmac:"   + hex(HMAC-SHA256(customerKey, plainDigest))
```

- **确定性去重与校验**：加密载体每次写入会随机生成新 `salt`（物理密文不同），但在同一 `customerKey` 下，相同逻辑内容派生的 `hmac:<hex>` 外部身份保持不变；不同 `customerKey` 则生成不同外部身份，避免跨租户泄露明文摘要。
- **区分两处 HMAC**：此处的 `KeyedDigest` 直接对 32 B 原始 `plainDigest` 计算 HMAC，**不带** domain prefix，且使用的是 `customerKey` 而非 `artifactKey`。

---

## 五、写入、随机读取与验证机制

前四章阐述了明文与加密载体的静态存储布局及密码学结构。在运行时实现中，还需解决两个核心工程问题：
- **流式单遍写入**：覆盖位图、稀疏映射表与加密认证头均位于文件前端，而内容摘要位于文件末尾，如何在不依赖外部临时文件、且仅顺序读取一遍源数据的前提下完成流式写出？
- **按需快开与全量校验的平衡**：沙箱在线启动要求快速打开并按需读取页面，而离线导入与格式转换则要求对全部字节执行完整性验证，不同读取入口的校验职责如何划分？

本章将围绕写入管线、随机读取寻址、验证职责划分以及接口选型展开说明。

### 5.1 两阶段写入管线（`WriteTo`）

`WriteTo` 严格遵循“先完成全部元数据规划与校验，再单遍顺序读取 Data 并流式输出”的两阶段管线：

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
  |  5. 编码 PAX、GNU sparse map、presence header，算出精确 plaintextSize 与加密几何参数
  |
  v  (规划校验全部通过后才进入阶段二；若失败则直接返回，零字节输出)
+-------------------------------------------------------------------+
| Phase 2: Streaming Emission & Hashing (emitPlan + recordWriter)   |  <-- 阶段二：单遍读 Data、在线算 Hash、流式写出
+-------------------------------------------------------------------+
  |  1. [可选加密] 生成 32B 随机 salt，写出 48B Clear Prefix 与 81B Sealed Header
  |  2. 写出段 [1]~[4] 前缀 (presence + PAX + 业务 Header + sparse map)
  |  3. 按序遍历已存储 extents (copyExtent)：
  |       - Data 子区间：调用 run.ReadAt 单遍读取
  |       - 未省略的 Zero 子区间：直接 clear(chunk) 合成零字节（不调 ReadAt）
  |       - 经 io.MultiWriter 同时写入目标流与 identityAccumulator（自动补算省略的整页 Zero）
  |  4. [可选] 执行 WithBeforeCommit(ctx) 钩子（在落盘最终 marker 前拦截）
  |  5. 写出 payloadPad + 段 [6] (.kuasar.digest.<hex> + 40B body + 2*512B 全零结束块)
  |  6. [可选加密] Flush 最后一条加密记录，核对总字节数与总记录条数
  |
  v
Output: (scheme, hexDigest, nil)
```

> **源稳定性与失败清理契约**：调用方必须在整个 `WriteTo` 执行期间保持 `src` 打开且稀疏布局稳定（若阶段二检测到 `RunAt` 结果与阶段一规划不一致，将立即报错中止）。阶段二或 `beforeCommit` 钩子返回错误时，已写出的不完整字节由调用方负责清理（例如移除未提交的临时文件）。

### 5.2 明文与加密随机读取寻址（`SourceAt`）

`SourceAt` 打开文件后，在内存中持有 `presence` 位图、排序后的 `extents` 数组及其前缀和数组 `packedPrefix`（`packedPrefix[i]` 为第 `i` 个 extent 在 `packed data` 内的起始偏移）。单次随机读取 `ReadAt(ctx, buf, x)` 的寻址链路如下：

```text
业务逻辑偏移 x
  |
  +--> 1. 二分查找 extentAt(x)
  |         +-- 若落在未存储区间 (Hole 或省略的整页 Zero): 直接 clear 填充 0x00 返回
  |         |   (若调用的是 RunAt, 则结合 presence[x/4096] 精确返回 Hole 或 Zero 状态)
  |         +-- 若命中 extent[i] = [Oi, Oi + Li): 进入步骤 2
  |
  +--> 2. 映射为明文 TAR 物理偏移 (O(1) 计算)
  |         tarOffset = packedStart + packedPrefix[i] + (x - Oi)
  |         +-- [明文载体] 直接调用底层 ReaderAt.ReadAt(p, tarOffset) --> 完成!
  |         +-- [加密载体] 进入步骤 3 (recordReaderAt)
  |
  +--> 3. 映射为加密记录序号与物理密文偏移 (O(1) 计算, 设首条记录明文长为 F)
  |         recordIndex(tarOffset) = 0                                   (当 tarOffset < F)
  |                                = 1 + (tarOffset - F) / 4096          (当 tarOffset >= F)
  |         plainStart(idx)        = 0                                   (当 idx == 0)
  |                                = F + (idx - 1) * 4096                (当 idx >= 1)
  |         cipherOffset(idx)      = 129 + plainStart(idx) + idx * 17
  |
  +--> 4. 批量合并 I/O 与逐记录 GCM 认证解密 (readBatch)
            - 相邻多条记录单次合并读取 (上限 maxCoalesceBytes = 1 MiB)
            - 从 sync.Pool 获取临时缓冲区, 逐条校验 AAD 与 GCM Tag 并原地解密
            - 拷贝目标切片 [tarOffset - plainStart(idx) : ...] 给调用方
            - 归还缓冲池前执行 clear() 彻底擦除明文/密文/AAD (本层不驻留明文缓存)
```

### 5.3 验证强度对比：`SourceAt` vs `SourceFrom` vs `ReadFrom`

不同读取入口在**打开阶段**与**读取阶段**执行的完整性/真实性校验范围存在明确分工：

| 校验维度                                               | `SourceAt`（随机访问打开）                                                                                                   | `SourceFrom`（流式顺序完整消费）                                                      | `ReadFrom` / `ReadSeekFrom`      |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | -------------------------------- |
| **加密前缀与 64B 认证头**                              | **打开时校验**（含物理文件总长与内层 `packedStart`/`packedSize` 核对）                                                       | **打开时校验**（含内层 `packedStart`/`packedSize` 核对）                              | 不支持加密载体                   |
| **Presence CRC32 与位图一致性**                        | **打开时校验**（含尾位清零、与 sparse extents 无冲突）                                                                       | **打开时校验**                                                                        | **打开时校验**                   |
| **Canonical PAX 与业务 TAR 头**                        | **打开时校验**（有 marker 时要求严格匹配 canonical 模板）                                                                    | **完成时校验**                                                                        | 仅解析，不强制 canonical         |
| **Digest Marker 与 2×512B 结束块**                     | **打开时定位并校验**（仅在无 presence、无 codec、无 expected digest 时兼容打开无 marker 明文 TAR）                           | **消费完末个 Data 时校验**（始终要求有 marker；若无 Data 则在打开时完成）             | 不读取、不校验 marker            |
| **Dense Metadata Tail (`[payloadSize, logicalSize)`)** | **打开时完整读取（加密含 GCM 认证）并重算 `tailDigest`，校验 `composeDigest == plainDigest`**（含 marker 的 canonical 载体） | **随流读取并重算 `tailDigest`**                                                       | 仅当普通数据按需读取，不校验摘要 |
| **Payload 本体 (`[0, payloadSize)`)**                  | **惰性按需校验**：不预读 payload、不重算 `payloadCommitment`；加密模式下仅在读取目标记录时验证其 GCM Tag                     | **全量校验**：重算完整 `payloadCommitment` 与 `plainDigest`，常数时间比对 marker 声明 | 仅按需读取，不校验摘要           |
| **`WithExpectedDigest` 锚点比对**                      | **打开时比对** marker 导出的 `digest`/`hmac`                                                                                 | **消费完成时比对**重算出的 `digest`/`hmac`                                            | 不支持 `ReadOption`              |
| **外层物理 EOF（防尾部附加冗余字节）**                 | **打开时校验**：含 marker 的 canonical 载体要求 `markerEnd + 1024 == size`（加密载体还核对头部推算的物理总长）               | **完成时校验**：在两个全零块后再试读 1 字节确认 `io.EOF`                              | 不校验                           |

> **关键提示**：向 `SourceAt` 传入 `WithExpectedDigest` 可以立即验证加密认证头、marker 声明以及完整的 `dense metadata tail`（如尾部 ZIP），但**不会触发对 `[0, payloadSize)` 主体 payload 的全量读取与校验**；若业务处于不可信流式导入/转换链路且需要验证每一字节 payload 与 `payloadCommitment` 一致，必须使用 `SourceFrom` 并完整消费至终点。

### 5.4 核心接口选型速查

| 函数 / 接口                 | 适用场景                                                                 | 关键特征与限制                                                      |
| --------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------- |
| `WriteTo`                   | 将 `sparse.Source` 编码写出为 canonical tarstream（明文或加密 v1）       | 两阶段写出；返回 `(scheme, hexDigest, error)`                       |
| `SourceAt`                  | 本地文件、NFS 等支持 `io.ReaderAt` 且已知物理大小的随机稀疏读取          | 并发安全；打开即校验元数据和 dense tail，payload 按需读取           |
| `SourceFrom`                | 流式管道、上传接收、格式转换中的单遍顺序消费与全量校验                   | 单向递增 `ReadAt`（不可回退）；必须读完所有 Data 才触发最终验证结论 |
| `ReadFrom` / `ReadSeekFrom` | 仅需标准 `io.Reader` / `io.ReadSeeker` 逻辑字节视图及 `Holes()` 列表     | **仅支持明文 TAR**；不校验 `.kuasar.digest.`，不区分 Zero 与 Data   |
| `ReadSeekFromIndex`         | 按物理条目序号（0-based，跳过 PAX 等元数据块）寻址明文 TAR 成员          | 与 `archive/tar.Reader.Next` 序号一致；允许按序号访问内部成员       |
| `CarrierDigest`             | 从已实现 `IdentityProvider` 的源直接导出目标写入策略下的外部身份         | **零 payload I/O**；支持直接导出 `digest:<hex>` 或 `hmac:<hex>`     |
| `ComposeDigest`             | 已知原 `payloadCommitment`，仅替换 dense tail 字节时计算新 `plainDigest` | 要求 `len(tail) == totalSize - payloadSize <= 64 MiB`               |

---

## 六、格式约束、兼容性与常见误区

在掌握核心格式与读写机制的基础上，本章进一步汇总工程接入与问题排查中涉及的容量与内存配额上限、历史版本载体的兼容策略，以及常见的使用误区，供开发与维护时参考。

### 6.1 格式约束与性能特征速查表

| 维度                         | 参数 / 约束值                     | 说明                                                                                              |
| ---------------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------- |
| **归档与成员最大长度**       | `1 << 62` B（4 EiB）              | 超出则拒绝读写                                                                                    |
| **Payload 最大长度**         | `1 TiB`（`1 << 40` B）            | 由 `.kuasar.presence.v1` 管理，对应位图最大 `32 MiB`                                              |
| **Presence 页大小**          | `4096 B`（4 KiB）                 | `Hole` 必须以 4 KiB 对齐起止（仅允许在 `payloadSize` 处非对齐结束）                               |
| **Dense Metadata Tail 上限** | `64 MiB`（`64 << 20` B）          | 区间 `[payloadSize, logicalSize)` 必须完全 dense（不能含 Hole）                                   |
| **Sparse Extent 数量上限**   | `1 << 20`（1,048,576）            | 含末尾 `(logicalSize, 0)` 哨兵及合并后的 tail extent                                              |
| **索引内存预算核算基线**     | `2 MiB` + 按规划开销累计计费      | 未设置 `WithIndexMemoryLimit` 时不限制；启用后以 `2 MiB` 为基线累计核算规划阶段内存开销            |
| **空 Payload 最小载体大小**  | 明文 `5120 B` / 加密 `5283 B`     | 明文含 presence(1024B)+PAX(1024B)+Header(512B)+Map(512B)+尾块(2048B)；加密外加 `48+81+17×2=163 B` |
| **加密记录粒度与合并上限**   | 目标明文 `4096 B`（开销 `+17 B`） | 随机读取每次至少认证 1 条记录，相邻记录批量合并读取上限 `1 MiB`                                   |
| **显式 Zero 的散列开销**     | 与逻辑覆盖长度成正比              | 省略的整页 Zero 省去了磁盘/网络 I/O，但计算 `payloadCommitment` 时仍需向 SHA-256 喂入合成零字节   |

### 6.2 旧载体兼容与降级边界

```text
读取端分流决策树 (SourceAt / SourceFrom):
  检查前 8 B 是否匹配加密魔数 (89 "KTSENC\n")?
    +-- 是 --> 强制走 Encrypted v1 解析 (任何认证/格式失败直接报错, 绝不降级回退为明文)
    +-- 否 --> 若配置了 required=true 则立即拒绝 (ErrPlaintextForbidden); 否则走明文解析:
                 +-- 首成员是 .kuasar.presence.v1?
                       +-- 是 --> 强制按 Presence v1 Canonical 规则校验 (要求必有 KUASAR.presence=1、
                       |          KUASAR.payload.size、canonical headers 及末尾 .kuasar.digest.<hex>)
                       +-- 否 --> 旧无 presence 明文载体:
                                    +-- 有 .kuasar.digest.<hex> marker: 按旧 canonical 规则校验 (支持字节级 Hole)
                                    +-- 无 marker: 仅 SourceAt 在未传 WithCodec 且未传 WithExpectedDigest 时兼容打开;
                                                   SourceFrom 始终要求存在合法 marker
```

- **无 marker 明文 TAR 的准入差异**：`SourceAt` 仅在同时满足“无 `.kuasar.presence.v1`、未传 `WithCodec`、未传 `WithExpectedDigest`”时兼容打开无 marker 的普通明文 TAR（返回的 `sparse.Source` 不实现 `Digester` / `IdentityProvider`）；一旦包含 presence、传入了 codec 或 expected digest，或者调用的是 `SourceFrom`，缺失 marker 均会报错拒绝（`ErrInvalidCanonicalTarstream`）。
- **只读兼容，写入不回退**：读取器继续支持无 presence 的历史明文载体（如旧 golden fixture 中 `3584 B` 的空载体）；但 `WriteTo` **只输出 presence v1 新格式**。若旧载体含有非 4 KiB 对齐的字节级 Hole，直接传给 `WriteTo` 会在规划阶段报错，不会静默填充或退回旧格式。
- **不支持的外部 TAR 变体**：遇到传统 GNU binary sparse（`typeflag = 'S'`）或 PAX `0.x`（`GNU.sparse.numblocks` / `GNU.sparse.map`）时，直接返回 `ErrUnsupportedEncoding`。

### 6.3 常见误区对照

| 常见误解                                          | 实际情况                                                                                                                                                                                         |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| “tarstream 就是普通 `tar.gz`”                     | 格式基于 `.kuasar.presence.v1` + GNU PAX sparse 1.0 + 可选 AES-256-GCM 记录信封，无内置 gzip 层                                                                                                  |
| “`ReadAt` 读出全零就代表该区间是 `Hole`”          | `Hole`、`Zero`、内容全零的 `Data` 在 `ReadAt` 下都返回零字节；保留分层语义必须调用 `RunAt`                                                                                                       |
| “用系统 `tar -xf` 解包后再打包能无损还原”         | `tar -xf` 能正确还原逻辑字节，但普通文件系统无法区分“未覆盖的 Hole”与“显式清零的 Zero”                                                                                                           |
| “可以在整个 TAR 文件的物理末尾直接找 ZIP EOCD”    | 物理 TAR 末尾是 `2048 B` 的 marker 与结束块；必须先打开 `sparse.Source` 逻辑视图再调 `tailzip.Locate`                                                                                            |
| “`SourceAt` 打开成功代表整个文件每一字节都未损坏” | `SourceAt` 打开时校验了头部、marker 和 **dense metadata tail**，但 `[0, payloadSize)` 的 payload 数据仅在读到对应记录时才验证 GCM Tag（且不重算 `payloadCommitment`）；全量验证需走 `SourceFrom` |