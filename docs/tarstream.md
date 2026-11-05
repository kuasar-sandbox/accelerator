# tarstream Storage Format Specification

This document provides a comprehensive specification of the `pkg/tarstream` storage format in Kuasar accelerator, covering its design background, dual-index representation of three-state sparse data, exact physical byte layouts for both plaintext and encrypted carriers, three-layer structured content identity (`Digest`/`HMAC`) computation, and the execution workflows for streaming emission, random access, and integrity verification.

> This specification is based on the local accelerator codebase (baseline commit `cadf305`) and focuses on the current **presence v1 + GNU PAX sparse 1.0** format and the **encrypted tarstream v1** envelope; backward-compatible read behavior for legacy presence-free carriers is documented separately. For general artifact contracts, see [File Artifacts and Carriers](file-artifacts.md).

## Table of Contents

- [tarstream Storage Format Specification](#tarstream-storage-format-specification)
  - [Table of Contents](#table-of-contents)
  - [1. Overview: Why tarstream Is Needed](#1-overview-why-tarstream-is-needed)
    - [1.1 Three-State Semantics of Snapshot Data: Hole, Zero, Data](#11-three-state-semantics-of-snapshot-data-hole-zero-data)
    - [1.2 Why Ordinary Files or Standalone GNU Sparse TAR Are Insufficient](#12-why-ordinary-files-or-standalone-gnu-sparse-tar-are-insufficient)
    - [1.3 Design Goals and Architectural Position](#13-design-goals-and-architectural-position)
  - [2. Plaintext tarstream Storage Format](#2-plaintext-tarstream-storage-format)
    - [2.1 Three-Layer Coordinate Space Mapping](#21-three-layer-coordinate-space-mapping)
    - [2.2 Six-Segment Physical Layout of Plaintext Canonical TAR](#22-six-segment-physical-layout-of-plaintext-canonical-tar)
    - [2.3 Segment \[1\]: presence entry (Logical Coverage Bitmap)](#23-segment-1-presence-entry-logical-coverage-bitmap)
      - [2.3.1 64-Byte Header Layout (Little-Endian)](#231-64-byte-header-layout-little-endian)
      - [2.3.2 Bitmap Encoding and Trailing Zero-Bit Constraint](#232-bitmap-encoding-and-trailing-zero-bit-constraint)
      - [2.3.3 Zero-Page Omission Rule and Three-State Truth Table](#233-zero-page-omission-rule-and-three-state-truth-table)
    - [2.4 Segments \[2\] \& \[3\]: PAX Extended Header and Payload TAR Header](#24-segments-2--3-pax-extended-header-and-payload-tar-header)
    - [2.5 Segments \[4\] \& \[5\]: GNU sparse 1.0 Map and Packed Data](#25-segments-4--5-gnu-sparse-10-map-and-packed-data)
    - [2.6 Segment \[6\]: Digest Marker and Archive End Blocks](#26-segment-6-digest-marker-and-archive-end-blocks)
    - [2.7 End-to-End Byte Layout Example](#27-end-to-end-byte-layout-example)
  - [3. ZIP Tail and Three-Layer Content Identity](#3-zip-tail-and-three-layer-content-identity)
    - [3.1 Locating the ZIP Tail at Logical EOF](#31-locating-the-zip-tail-at-logical-eof)
    - [3.2 Logical Layouts for Image, Sandbox E, and Snapshot S](#32-logical-layouts-for-image-sandbox-e-and-snapshot-s)
    - [3.3 Three-Layer Structured Content Identity (Digest) Computation](#33-three-layer-structured-content-identity-digest-computation)
    - [3.4 Fast Identity Derivation When Replacing Only the ZIP Tail](#34-fast-identity-derivation-when-replacing-only-the-zip-tail)
  - [4. Encrypted tarstream Format (encrypted v1)](#4-encrypted-tarstream-format-encrypted-v1)
    - [4.1 Encryption Scope and Physical Envelope Layout](#41-encryption-scope-and-physical-envelope-layout)
    - [4.2 48 B Clear Prefix and 64 B Authenticated Header](#42-48-b-clear-prefix-and-64-b-authenticated-header)
    - [4.3 Key Derivation, Nonce, AAD, and Seal/Open Workflow](#43-key-derivation-nonce-aad-and-sealopen-workflow)
    - [4.4 Record Slicing Geometry: Aligning `packedStart` via Record 0](#44-record-slicing-geometry-aligning-packedstart-via-record-0)
    - [4.5 External Identity for Encrypted Carriers (HMAC)](#45-external-identity-for-encrypted-carriers-hmac)
  - [5. Writing, Random Access, and Verification Mechanisms](#5-writing-random-access-and-verification-mechanisms)
    - [5.1 Two-Phase Write Pipeline (`WriteTo`)](#51-two-phase-write-pipeline-writeto)
    - [5.2 Plaintext and Encrypted Random-Access Addressing (`SourceAt`)](#52-plaintext-and-encrypted-random-access-addressing-sourceat)
    - [5.3 Verification Matrix: `SourceAt` vs `SourceFrom` vs `ReadFrom`](#53-verification-matrix-sourceat-vs-sourcefrom-vs-readfrom)
    - [5.4 Core Interface Selection Reference](#54-core-interface-selection-reference)
  - [6. Format Constraints, Compatibility, and Common Pitfalls](#6-format-constraints-compatibility-and-common-pitfalls)
    - [6.1 Format Quotas and Performance Characteristics Reference](#61-format-quotas-and-performance-characteristics-reference)
    - [6.2 Legacy Carrier Compatibility and Downgrade Boundaries](#62-legacy-carrier-compatibility-and-downgrade-boundaries)
    - [6.3 Common Misconceptions vs. Actual Behavior](#63-common-misconceptions-vs-actual-behavior)

---

## 1. Overview: Why tarstream Is Needed

In Kuasar, container images, sandbox root disks (Sandbox E), and virtual machine memory snapshots (Snapshot S) must all be packaged as single files for persistence and distribution across local disks, shared filesystems, and remote repositories; at the same time, the runtime must be able to perform direct random reads (`ReadAt`) by logical offset without prior extraction. Understanding why ordinary files or standard `tar` archives cannot satisfy these requirements begins with the "three-state" semantics of incremental snapshot data.

### 1.1 Three-State Semantics of Snapshot Data: Hole, Zero, Data

Incremental VM memory snapshots and disk deltas share a key characteristic: **a large logical address space with sparse per-layer modifications** (for example, only 128 MiB modified out of an 8 GiB logical space). When stacking multiple snapshot layers for reading, any logical interval within a single layer must unambiguously distinguish among three states:

```text
             Parent Layer : [ AAAA ]   [ AAAA ]   [ AAAA ]
                               ^          x          x
                               |          |          |
             Current Layer: [ Hole ]   [ Zero ]   [ BBBB ]  <-- Hole=uncovered, Zero=explicit zero, BBBB=new data
                               |          |          |
                               v          v          v
             Merged Result: [ AAAA ]   [ 0000 ]   [ BBBB ]  <-- Hole falls through, Zero masks parent, Data overrides
```

| Logical State | Covered by Current Layer | Single-Layer `ReadAt` Bytes       | Multi-Layer Stacking Semantics                                    |
| ------------- | ------------------------ | --------------------------------- | ----------------------------------------------------------------- |
| **Hole**      | No                       | `0x00`                            | Uncovered; falls back to the parent layer (returns `AAAA`)        |
| **Zero**      | Yes                      | `0x00`                            | Explicitly zeroed; masks parent layer data (returns zeros)        |
| **Data**      | Yes                      | Stored literal bytes (may hold 0) | Explicitly written; overrides parent with stored bytes (`BBBB`)   |

### 1.2 Why Ordinary Files or Standalone GNU Sparse TAR Are Insufficient

Once the semantic distinction among `Hole`, `Zero`, and `Data` is established, comparing common storage representations reveals that traditional formats cannot simultaneously preserve both **space efficiency** and **three-state semantic fidelity**:

```text
Ordinary File / Dense TAR:
  [Data A][0000...][0000...][Data B]  --> Transmits massive zero runs; conflates Hole and Zero as 0x00

GNU sparse 1.0 TAR only:
  sparse map: [Data A], [Data B]      --> Saves space, but unstored ranges cannot distinguish Hole from Zero

Kuasar tarstream (presence v1 + GNU sparse 1.0):
  presence bitmap (0=Hole, 1=Present)
                 +                    --> Omits physical bytes for Holes and full Zero pages while preserving all 3 states
  sparse map      (0=Omitted, 1=Data)
```

| Carrier Format                           | Omits Hole Bytes | Omits Full Zero Pages | Distinguishes Hole vs Zero | Directly Extractable by Standard `tar` |
| ---------------------------------------- | ---------------- | --------------------- | -------------------------- | -------------------------------------- |
| Ordinary Contiguous File                 | No               | No                    | No                         | N/A                                    |
| Ordinary Dense TAR                       | No               | No                    | No                         | Yes                                    |
| Direct Stream of Host Sparse File        | No (reads zeros) | No                    | No                         | N/A                                    |
| Standalone GNU PAX sparse 1.0 TAR        | Yes              | No (omitting => Hole) | No                         | Yes                                    |
| **tarstream (presence v1 + sparse 1.0)** | **Yes**          | **Yes (4 KiB pages)** | **Yes**                    | **Yes**                                |

> Note: Space savings in tarstream come from **skipping Holes** and **omitting declared full 4 KiB Zero pages**, not from general-purpose compression. If an input source reports all-zero bytes as ordinary `Data`, the writer will not scan byte contents to convert them into `Zero`.

### 1.3 Design Goals and Architectural Position

To bridge this gap, tarstream augments standard GNU sparse TAR with a lightweight coverage bitmap and exposes a unified sparse random-access abstraction to upper-layer consumers. Its position in the overall architecture is illustrated below:

```text
+-------------------------------------------------------+
| Upper Consumers (Image / Sandbox E / Snapshot S)      |  <-- Upper layers: parent refs, stacking, config parsing, publish tx
+---------------------------+---------------------------+
                            |
                            v
+-------------------------------------------------------+
| Logical View: sparse.Source (Size + RunAt + ReadAt)   |  <-- Unified sparse logical source:
|   [0, payloadSize)   : Sparse Payload (Hole/Zero/Data)|      Leading three-state sparse payload
|   [payloadSize, Size): Optional Dense ZIP Tail        |      Trailing optional dense ZIP metadata tail
+---------------------------+---------------------------+
                            |
                            v
+-------------------------------------------------------+
| Plaintext Canonical tarstream                         |  <-- Plaintext carrier:
|   .kuasar.presence.v1 + PAX + Sparse 1.0 + Marker     |      Coverage bitmap + GNU sparse TAR + logical identity marker
+---------------------------+---------------------------+
                            |
                            v
+-------------------------------------------------------+
| Encrypted tarstream v1 (Optional via WithCodec)       |  <-- Encrypted envelope:
|   48B Prefix + 81B Sealed Header + 4KiB Auth Records  |      Clear prefix + sealed header + 4 KiB authenticated records
+-------------------------------------------------------+
```

---

## 2. Plaintext tarstream Storage Format

With the dual-index concept established, the next question is how to encode this structure into a single archive file while maintaining compatibility with standard `tar` utilities.

tarstream adheres to standard ustar/PAX archive conventions: it places a lightweight bitmap member immediately before the sparse payload member to record whether each 4 KiB logical page is covered by the current layer, and appends a fixed-size marker member at the end of the archive to record the content identity of the carrier. This chapter first clarifies the three coordinate spaces involved and then examines the six physical segments in storage order.

### 2.1 Three-Layer Coordinate Space Mapping

Understanding tarstream requires distinguishing among three coordinate spaces: **logical file offsets**, **plaintext TAR offsets**, and **encrypted file offsets**:

```text
[1. Logical File View] (logicalSize = payloadSize + tailSize)
  0                                                payloadSize        logicalSize
  +--------------+--------------+--------------+--------+------------------+
  |    Data A    |     Hole     |  Zero (4KiB) | Data B |  Dense ZIP Tail  |
  +------+-------+--------------+--------------+---+----+--------+---------+
         |              (omitted on wire)          |             |
         +----------------------+------------------+-------------+
                                v                                v
[2. Plaintext Canonical TAR View] (plaintextSize, 512 B aligned)
  0                                       packedStart       packedStart+packedSize
  +----------+--------+--------+----------+-------+----+--------+---------+----------------+
  |[1]Presen.| [2]PAX | [3]Hdr |[4]Sparse |Data A |Data|ZIP Tail| Padding |[6]Marker + EOF |
  |  Member  | ExtHdr | Member |   Map    |       | B  |        | (0~511B)| (Fixed 2048 B) |
  +----------+--------+--------+----------+-------+----+--------+---------+----------------+
  |<------------ packedStart ------------>|<---- packedSize --->|
                                          |
         +--------------------------------+
         |
         v  (Record 0 plaintext length F = packedStart mod 4096, or 4096 when mod is 0, aligning packedStart to a record boundary)
[3. Encrypted Envelope View (Encrypted v1)] (encryptedSize = 129 + plaintextSize + 17 * N)
  0         48           129
  +---------+------------+--------------+--------------+-----+--------------+
  |48B Clear| 81B Sealed |   Record 0   |   Record 1   | ... |  Record N-1  |
  | Prefix  |   Header   | (Plain: F B) | (Plain: 4KiB)|     | (Plain<=4KiB)|
  +---------+------------+--------------+--------------+-----+--------------+
```

| Coordinate Space       | Range                | Offset Semantics and Characteristics                                                                                    |
| ---------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **Logical File View**  | `[0, logicalSize)`   | Leading `[0, payloadSize)` is the sparse payload; trailing `[payloadSize, logicalSize)` is the optional dense ZIP tail  |
| **Plaintext TAR View** | `[0, plaintextSize)` | Contains TAR structural headers/maps and contiguously concatenated `packed data`; 512 B block-aligned                   |
| **Encrypted Envelope** | `[0, encryptedSize)` | Slices the complete plaintext TAR into `N` authenticated records, each adding `1 B flag + 16 B tag = 17 B` of overhead |

### 2.2 Six-Segment Physical Layout of Plaintext Canonical TAR

A plaintext canonical tarstream consists of six physical segments (corresponding to **3 actual TAR file members**: `[1] presence`, `[3] payload member`, and `[6] digest marker`, where `[2] PAX` is the extended header for `[3]`, and `[4] sparse map` plus `[5] packed data` form the body of `[3]`):

```text
Plaintext TAR Offset 0
  |
  +-- Segment [1]: presence regular file member (.kuasar.presence.v1)
  |                +-- 512 B  ustar header (typeflag='0', mode=0444, size=64+len(bitmap))
  |                +--  64 B  presence header (magic "KUPRES1\n" + metadata + CRC32)
  |                +--   M B  bitmap bytes
  |                +--   P B  zero padding (aligned to 512 B boundary)
  |
  +-- Segment [2]: payload member PAX extended header (PaxHeaders.0/<base>)
  |                +-- 512 B  ustar header (typeflag='x', mode=0644, size=len(PAX records))
  |                +-- 512*k  sorted PAX key=value records + zero padding to 512 B
  |
  +-- Segment [3]: payload member TAR Header (<dir>/GNUSparseFile.0/<base>)
  |                +-- 512 B  ustar header (typeflag='0', mode=0644, size=storedSize)
  |
  +-- Segment [4]: GNU sparse 1.0 map (at the start of payload member body)
  |                +-- 512*m  decimal text (ending with (logicalSize, 0) sentinel) + zero padding to 512 B
  |
  +-- Segment [5]: packed data (immediately following sparse map, starting at packedStart)
  |                +-- packedSize B  stored extents concatenated in ascending logical offset order (including dense tail)
  |                +-- payloadPad B  zero padding (aligning storedSize = mapLen + packedSize to 512 B)
  |
  +-- Segment [6]: identity marker and archive end blocks (fixed 4 * 512 = 2048 B)
                   +-- 512 B  marker ustar header (.kuasar.digest.<64-char-lowercase-hex>, typeflag='0', mode=0444, size=40)
                   +-- 512 B  40 B marker body (payloadSize + payloadCommitment) + 472 B zero padding
                   +-- 1024 B two 512 B all-zero TAR end-of-archive blocks (EOF)
```

> **Canonical Header Constraint**: All ustar headers emitted by the writer encode `uid`, `gid`, and `mtime` as octal `0`, with magic `"ustar\x00"` and version `"00"`. On the read path, the **presence header (Segment `[1]`), PAX extended header (Segment `[2]`), and payload TAR header (Segment `[3]`)** are compared byte-for-byte against reconstructed writer templates; the **digest marker header (Segment `[6]`)** is validated field-by-field for ustar magic, header checksum, `typeflag` (`'0'` or `0`), `size == 40`, name format, and its 40 B body plus 472 B zero padding, without requiring a full byte-for-byte template match.

### 2.3 Segment [1]: presence entry (Logical Coverage Bitmap)

The following subsections examine each segment in physical order from start to end. Located at the very beginning of the archive, Segment `[1]` encodes a compact 1-bit-per-4-KiB-page coverage bitmap that records whether each logical page is covered by the current layer, addressing standard sparse TAR's inability to distinguish `Hole` from `Zero`.

#### 2.3.1 64-Byte Header Layout (Little-Endian)

The body of `.kuasar.presence.v1` begins with a fixed 64-byte header in which all multi-byte integers are **little-endian**:

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

| Relative Offset | Length | Field          | Encoding and Validation Rules                                                         |
| --------------- | ------ | -------------- | ------------------------------------------------------------------------------------- |
| `[0, 8)`        | 8 B    | `magic`        | Fixed as `KUPRES1\n` (`4b 55 50 52 45 53 31 0a`)                                      |
| `[8, 12)`       | 4 B    | `version`      | `uint32LE`, currently fixed at `1`                                                    |
| `[12, 16)`      | 4 B    | `pageSize`     | `uint32LE`, currently fixed at `4096`                                                 |
| `[16, 24)`      | 8 B    | `payloadSize`  | `uint64LE`, logical range `[0, payloadSize)` managed by bitmap; max `1 TiB` (`1<<40`) |
| `[24, 32)`      | 8 B    | `bitmapLength` | `uint64LE`, must equal `(ceil(payloadSize / 4096) + 7) / 8`, max `32 MiB`             |
| `[32, 60)`      | 28 B   | `reserved`     | Must be all `0`                                                                       |
| `[60, 64)`      | 4 B    | `CRC32`        | `uint32LE`, IEEE CRC32 covering header bytes `[0, 60)` plus the entire `bitmap`       |

#### 2.3.2 Bitmap Encoding and Trailing Zero-Bit Constraint

Let `pages = ceil(payloadSize / 4096)`. Logical page `i` (`0 <= i < pages`) maps to bit `i % 8` of `bitmap[i / 8]` (LSB corresponds to the lowest page index):

```text
Example: payloadSize = 16384 B (four 4 KiB pages)
  Page 0 [0, 4096)     : Data  --> bit 0 = 1
  Page 1 [4096, 8192)  : Hole  --> bit 1 = 0
  Page 2 [8192, 12288) : Zero  --> bit 2 = 1
  Page 3 [12288, 16384): Data  --> bit 3 = 1

  bitmap[0] binary layout (bit 7 .. bit 0):
  +---+---+---+---+---+---+---+---+
  | 0 | 0 | 0 | 0 | 1 | 1 | 0 | 1 |  = 0x0d
  +---+---+---+---+---+---+---+---+
  |<-- unused --->|p3  p2  p1  p0    (unused trailing bits must be 0, otherwise rejected)
```

- **Space overhead**: A `1 GiB` payload requires a `32 KiB` bitmap; the `1 TiB` maximum requires a `32 MiB` bitmap. When `payloadSize = 0`, `bitmapLength = 0`, but the 64 B presence header is still emitted.
- **Hole alignment constraint**: Within `[0, payloadSize)`, every `Hole` must start on a `4096 B` boundary and end on a `4096 B` boundary or exactly at `payloadSize`.

#### 2.3.3 Zero-Page Omission Rule and Three-State Truth Table

The writer omits physical bytes only for **complete 4 KiB-aligned pages covered by a single `sparse.Zero` run within `[0, payloadSize)`**; unaligned leading/trailing Zero fragments (as well as any Zero runs inside the dense tail) are retained in sparse extents and emitted as literal zero bytes:

```text
Single Zero run crossing unaligned boundaries: [2048, 10240)
  Page 0 [0, 4096)       Page 1 [4096, 8192)     Page 2 [8192, 12288)
  +---------+---------+ +---------------------+ +---------+---------+
  | (Other) |Zero Frag| |   Full 4 KiB Zero   | |Zero Frag| (Other) |
  +---------+----+----+ +----------+----------+ +----+----+---------+
                 |                 |                 |
                 v                 v                 v
         Kept in sparse extent Omitted from map      Kept in sparse extent
         (literal 0x00 stored) (presence=1 only)     (literal 0x00 stored)
```

By jointly querying the `presence` bitmap and the `GNU sparse map` within `[0, payloadSize)`, the reader reconstructs all three states without ambiguity:

| `presence` Bit (`bit`) | Covered by `sparse extent` | Physical Bytes Stored in TAR | Reconstructed `RunAt` State       | Single-Layer `ReadAt` Result |
| ---------------------- | -------------------------- | ---------------------------- | --------------------------------- | ---------------------------- |
| `0`                    | No                         | No                           | **Hole**                          | Synthesized `0x00`           |
| `1`                    | No                         | No                           | **Zero** (full page omitted)      | Synthesized `0x00`           |
| `1`                    | Yes                        | Yes                          | **Data** (incl. sub-page Zero)    | Stored physical bytes        |
| `0`                    | Yes                        | -                            | **Invalid format** (rejected)     | -                            |

### 2.4 Segments [2] & [3]: PAX Extended Header and Payload TAR Header

Once the Segment `[1]` bitmap is parsed, the reader can distinguish uncovered `Hole` pages from pages covered by the current layer. Immediately following are Segments `[2]` and `[3]`, which are standard TAR headers describing the payload file metadata (real member name, logical total size, stored size, etc.). After normalizing the member name via `normalizeName` (stripping leading `/` and `./`), the writer emits these two headers:

| Header                    | ustar `name` Field (max 100 B)                                  | `typeflag` | `mode` | ustar `size` Field                                                  |
| ------------------------- | --------------------------------------------------------------- | ---------- | ------ | ------------------------------------------------------------------- |
| **Segment [2] PAX Hdr**   | `"PaxHeaders.0/" + truncate(Base(name), 80)`                    | `'x'`      | `0644` | Total byte length of PAX records                                    |
| **Segment [3] Entry Hdr** | `truncate(Join(Dir(name), "GNUSparseFile.0", Base(name)), 100)` | `'0'`      | `0644` | `storedSize` (`0` if exceeding octal range; carried by PAX `size`)  |

The PAX records in the body of Segment `[2]` follow the standard `"<len> <key>=<value>\n"` format and are **strictly sorted in ascending lexicographical order by key**:

| PAX Key (Lexicographical) | Value Semantics                                                 | Example (member name `sparse/image`)     |
| ------------------------- | --------------------------------------------------------------- | ---------------------------------------- |
| `GNU.sparse.major`        | Fixed `"1"`                                                     | `22 GNU.sparse.major=1\n`                |
| `GNU.sparse.minor`        | Fixed `"0"`                                                     | `22 GNU.sparse.minor=0\n`                |
| `GNU.sparse.name`         | Normalized real payload member name                             | `32 GNU.sparse.name=sparse/image\n`      |
| `GNU.sparse.realsize`     | `logicalSize` (decimal, including dense tail)                   | `29 GNU.sparse.realsize=17408\n`         |
| `KUASAR.payload.size`     | `payloadSize` (decimal, boundary between payload and tail)      | `29 KUASAR.payload.size=16384\n`         |
| `KUASAR.presence`         | Fixed `"1"` (declares that preceding `.kuasar.presence.v1` applies) | `21 KUASAR.presence=1\n`             |
| `path`                    | Mangled ustar transport path                                    | `37 path=sparse/GNUSparseFile.0/image\n` |
| `size`                    | `storedSize = len(sparseMap) + packedSize` (decimal)            | `13 size=9728\n`                         |

### 2.5 Segments [4] & [5]: GNU sparse 1.0 Map and Packed Data

Following the payload TAR header is the payload member body (Segments `[4]` and `[5]`). **Segment [4] (GNU sparse 1.0 map)** is a newline-delimited decimal offset table recording the logical intervals that physically store bytes, **always terminated by a `(logicalSize, 0)` sentinel extent** (included in `count`, up to `1<<20` extents) and zero-padded to a 512 B boundary; **Segment [5] (packed data)** immediately follows, storing all physical data bytes contiguously in ascending logical offset order:

```text
Suppose a logical file (logicalSize = 17408, payloadSize = 16384) has stored logical extents:
  Extent 0: [0, 4096)       --> Data A (4096 B)
  Extent 1: [12288, 17408)  --> Data B (4096 B) + merged adjacent ZIP Tail (1024 B) = 5120 B
  Sentinel: [17408, +0)     --> Fixed trailing sentinel

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

The relationship among the three core size metrics is summarized below:

| Size Metric   | Formula                            | Semantics                                                                      |
| ------------- | ---------------------------------- | ------------------------------------------------------------------------------ |
| `logicalSize` | `payloadSize + tailSize`           | Full logical file size visible to upper layers (`GNU.sparse.realsize`)         |
| `packedSize`  | $\sum \text{extent}_i.\text{Size}$ | Total physically stored payload and dense tail bytes                           |
| `storedSize`  | `mapLen + packedSize`              | Payload member body size in TAR (`PAX size`, excluding trailing `payloadPad`)  |

### 2.6 Segment [6]: Digest Marker and Archive End Blocks

After the payload member body is padded to a 512 B boundary, the archive concludes with a fixed `2048 B` (`4 × 512 B`) trailer containing the content identity marker (digest marker) and standard TAR end-of-archive blocks:

```text
+-----------------------------------------------------------------------+ 0
| Marker TAR Header (512 B)                                             |  <-- Member name carries plainDigest
|   name = ".kuasar.digest.<64-char-lowercase-hex>"                     |
|   mode = 0444, size = 40, typeflag = '0', magic = "ustar\x0000"       |
+-------------------------------------------+---------------------------+ 512
| Marker Body (40 B)                        | Zero Padding (472 B)      |  <-- Persists payloadCommitment for
|   [0..8)  : payloadSize (uint64 BE)       |   [40..512) must be 0x00  |      fast tail-replacement derivation
|   [8..40) : payloadCommitment (32 B raw)  |                           |
+-------------------------------------------+---------------------------+ 1024
| Archive End Block 1 (512 B, must be all 0x00)                         |
+-----------------------------------------------------------------------+ 1536
| Archive End Block 2 (512 B, must be all 0x00)                         |
+-----------------------------------------------------------------------+ 2048 (TAR EOF)
```

### 2.7 End-to-End Byte Layout Example

Putting all six segments together, the table below shows the exact correspondence between the logical view and plaintext TAR physical offsets for a concrete example with 4 logical pages (`payloadSize = 16384`), a `1024 B` dense tail (`logicalSize = 17408`), and member name `disk`:

| Segment / Phase          | Plaintext TAR Range | Length | Corresponding Logical Range / Description                                       |
| ------------------------ | ------------------- | ------ | ------------------------------------------------------------------------------- |
| **[1] presence header**  | `[0, 512)`          | 512 B  | `.kuasar.presence.v1` TAR header (`size = 65`)                                  |
| **[1] presence body**    | `[512, 1024)`       | 512 B  | `64 B` header + `1 B` bitmap (`0x0d`) + `447 B` zero padding                    |
| **[2] PAX header**       | `[1024, 1536)`      | 512 B  | `PaxHeaders.0/disk` TAR header (`typeflag = 'x'`)                               |
| **[2] PAX records**      | `[1536, 2048)`      | 512 B  | 8 sorted PAX record lines + zero padding                                        |
| **[3] Payload TAR hdr**  | `[2048, 2560)`      | 512 B  | `GNUSparseFile.0/disk` TAR header (`storedSize = 9728`)                         |
| **[4] sparse map**       | `[2560, 3072)`      | 512 B  | 2 non-empty extents + `(17408, 0)` sentinel text + zero padding                 |
| **[5] packed: Data A**   | `[3072, 7168)`      | 4096 B | Logical `[0, 4096)` (starts at `packedStart = 3072`)                            |
| *(Omitted Hole & Zero)*  | *(No physical B)*   | `0 B`  | Logical `[4096, 8192)` Hole and `[8192, 12288)` full Zero page are omitted      |
| **[5] packed: Data B**   | `[7168, 11264)`     | 4096 B | Logical `[12288, 16384)`                                                        |
| **[5] packed: ZIP tail** | `[11264, 12288)`    | 1024 B | Logical `[16384, 17408)` (merged with Data B; total `packedSize = 9216`)        |
| **[6] marker header**    | `[12288, 12800)`    | 512 B  | `.kuasar.digest.<hex>` TAR header (`size = 40`)                                 |
| **[6] marker body**      | `[12800, 13312)`    | 512 B  | `8 B` `payloadSize(16384)` + `32 B` `payloadCommitment` + `472 B` zero padding  |
| **[6] TAR end blocks**   | `[13312, 14336)`    | 1024 B | Two 512 B all-zero blocks (`plaintextSize = 14336`)                             |

---

## 3. ZIP Tail and Three-Layer Content Identity

Chapter 2 defined the plaintext archive layout for a single sparse data body. In practical workloads, however, a self-contained deliverable artifact must carry structured metadata—such as image configurations, virtual machine device states, or dirty-page bitmaps—alongside the primary payload.

If metadata were stored in external sidecar files, cross-node distribution or object-storage uploads could easily suffer from partial-commit inconsistencies between data and configuration; conversely, if metadata were concatenated with a multi-GiB payload under a single flat SHA-256 hash, updating a few kilobytes of trailing configuration would require re-reading and re-hashing the entire payload. To resolve both issues, tarstream combines an **embedded logical ZIP tail** with a **three-layer structured content identity**, appending a compact ZIP archive at the end of the logical file while decoupling the payload commitment from the tail digest.

### 3.1 Locating the ZIP Tail at Logical EOF

The business ZIP tail resides at the **end of the logical file (`logicalSize`)**, not at the physical end of the TAR file (which is occupied by the digest marker and archive end blocks). To locate the ZIP tail, the consumer first opens the logical view via `tarstream.SourceAt` and then invokes `tailzip.Locate` to scan backward from logical EOF:

```text
Logical View (sparse.Source, Size = logicalSize):
  0                                      payloadSize                      logicalSize
  +-------------------------------------------+---------------------------------+
  |              Sparse Payload               |         Dense ZIP Tail          |
  |        (Image / Disk / VM Memory)         | [LocalHdr+Body...][CD][EOCD+Cmt]|
  +-------------------------------------------+--------------------+---+--------+
                                              ^                    |   |
                                              |                    |   +-- 1. Read up to 65558 B (22 + 64KiB) backward from
                                              |                    |          logical EOF to locate EOCD ending at EOF
                                              +--------------------+------ 2. Validate Central Directory offset/size from EOCD
                                                                              and determine ZIP start offset (= payloadSize)
```

- **Single source of truth for boundary**: The discovered ZIP start offset `Tail.Offset` defines `payloadSize`; upper layers expose only `[0, Tail.Offset)` to block devices or UFFD handlers, preventing runtime reads from spilling into trailing metadata.
- **Deterministic encoding**: `tailzip.Encode` produces deterministic `STORED` (uncompressed) ZIP archives; strict consumers such as Sandbox E and Snapshot S further use `tailzip.EncodeCanonical`, enforcing entry ordering, a fixed MS-DOS timestamp (`1980-01-01 00:00:00`), no extra fields/comments/data descriptors, and a `64 MiB` maximum tail size.

### 3.2 Logical Layouts for Image, Sandbox E, and Snapshot S

The tarstream carrier layer uniformly provides a "sparse payload + optional dense tail" layout, while upper-layer consumers package their respective metadata entries inside the trailing ZIP:

| Artifact Type                      | Logical Payload `[0, payloadSize)` | Logical Dense Tail `[payloadSize, logicalSize)`              | Consumer Package             |
| ---------------------------------- | ---------------------------------- | ------------------------------------------------------------ | ---------------------------- |
| **Container Image (Image)**        | EROFS image data                   | `ZIP(config.json)`                                           | `accelerator/pkg/image`      |
| **Sandbox E (Sparse Disk Mode)**   | Sparse root block device image     | `ZIP(sandbox.runtime.cfg)`                                   | `sandboxer/pkg/sandboxfile`  |
| **Sandbox E (EROFS Mode)**         | EROFS image data                   | `ZIP(config.json, sandbox.runtime.cfg)`                      | `sandboxer/pkg/sandboxfile`  |
| **Snapshot S (Memory Snapshot)**   | Sparse VM memory space             | `ZIP(config.json, state.json, snapshot.cfg[, memory.dirty])` | `sandboxer/pkg/snapshotfile` |

> Note: `memory.dirty` in Snapshot S is a business-level dirty-page tracking bitmap and is completely independent of the carrier-level `.kuasar.presence.v1` bitmap.

### 3.3 Three-Layer Structured Content Identity (Digest) Computation

Rather than computing a flat SHA-256 over the physical TAR bytes, tarstream derives `plainDigest` via a three-layer structured hash over logical coverage intervals, present bytes, normalized member name, and size boundaries (all multi-byte integers are **big-endian** `uint64`):

```text
+------------------------------------------------------------------+
| Layer 1: payloadCommitment                                       |
|   SHA256(                                                        |
|     "kuasar/tarstream/payload\x00"                               |
|     || uint64BE(payloadSize)                                     |
|     || uint64BE(presentIntervalCount)                            |  <-- Contiguous present intervals merged from presence (bit=1)
|     || [uint64BE(offset) || uint64BE(length)]*                   |
|     || all present logical bytes in [0, payloadSize)             |  <-- Stored Data bytes + synthesized Zero-page bytes;
|   )                                                              |      Hole bytes are excluded from hashing
+---------------------------------+--------------------------------+
                                  |
                                  |   +----------------------------+
                                  |   | Layer 2: tailDigest        |
                                  |   |   SHA256(                  |
                                  |   |     "kuasar/tarstream/"    |
                                  |   |     "tail\x00"             |
                                  |   |     || uint64BE(tailSize)  |  <-- Covers all dense tail bytes
                                  |   |     || tailBytes           |      in [payloadSize, logicalSize)
                                  |   |   )                        |
                                  |   +-------------+--------------+
                                  v                 v
+------------------------------------------------------------------+
| Layer 3: plainDigest (written into .kuasar.digest.<hex>)         |
|   SHA256(                                                        |
|     "kuasar/tarstream/carrier\x00"                               |
|     || uint64BE(len(normalizedName)) || normalizedName           |  <-- Binds normalized member name, logicalSize,
|     || uint64BE(logicalSize)                                     |      payloadSize, and both layer digests
|     || uint64BE(payloadSize)                                     |
|     || payloadCommitment (32 B)                                  |
|     || tailDigest (32 B)                                         |
|   )                                                              |
+------------------------------------------------------------------+
```

**This three-layer hash structure provides two key design properties:**
1. **Representation independence**: Layer 1 hashes logical present intervals derived from `presence` and feeds synthesized zero bytes for omitted full `Zero` pages. Consequently, an all-zero page produces the exact same `payloadCommitment` whether stored physically as `Data` or omitted as `Zero`; mutating `Zero` into `Hole` alters the present interval list and therefore changes the digest.
2. **$O(\text{tailSize})$ tail metadata updates**: Because `payloadCommitment` is persisted independently in the marker body, updating trailing metadata does not require re-reading or re-hashing the primary payload.

### 3.4 Fast Identity Derivation When Replacing Only the ZIP Tail

When an upper layer replaces only the dense metadata tail (for example, rewriting Sandbox/Snapshot trailing ZIP configurations) while keeping `[0, payloadSize)` unchanged, the new identity can be derived directly via `ComposeDigest` / `CarrierDigest`:

```text
Marker Body (existing)  --> (payloadSize, payloadCommitment) --+
                                                               +--> ComposeDigest --> new plainDigest (zero payload I/O)
Replacement ZIP Tail    --> new tailDigest + new logicalSize --+
```

---

## 4. Encrypted tarstream Format (encrypted v1)

The plaintext format described in Chapters 1–3 addresses sparse storage, metadata encapsulation, and accidental-corruption detection in trusted environments. In production deployments, however, snapshots and image artifacts are frequently stored on multi-tenant shared media (such as shared NFS or remote object storage), where a plaintext carrier faces two security challenges:

- **Exposure of sensitive data and internal structure**: VM memory snapshots and sandbox disks may contain runtime secrets, credentials, or environment variables; moreover, even if the payload data region alone were encrypted, the plaintext TAR headers would still expose member names, the `.kuasar.presence.v1` bitmap and sparse map would reveal memory/disk allocation patterns, and the trailing `.kuasar.digest.<hex>` would leak a cross-tenant content fingerprint.
- **Tension between whole-file encryption and random access**: Wrapping the entire TAR file in a single conventional cipher stream would force the reader to decrypt sequentially from offset `0` just to access a single 4 KiB page or read the trailing ZIP configuration, eliminating random-access capability; meanwhile, an unkeyed plaintext hash cannot prevent an attacker with write access from replacing both the payload and the trailing digest marker.

To address these requirements, tarstream defines an outer **random-access authenticated encryption format (encrypted v1)**: the complete plaintext Canonical TAR is sliced into independent `4 KiB` AES-256-GCM authenticated records. This encapsulates member names, bitmaps, sparse maps, payload data, and the plaintext digest marker inside ciphertext while allowing the reader to locate and authenticate individual records in $O(1)$ time.

### 4.1 Encryption Scope and Physical Envelope Layout

When `WithCodec` is enabled, the **entire plaintext Canonical TAR** described in Chapter 2 (from Segment `[1] presence` through Segment `[6] two all-zero end blocks`) serves as the input plaintext stream and is partitioned into a sequence of independently authenticated AES-256-GCM records:

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

- **Full metadata confidentiality**: Payload member names, the `presence` bitmap, the `sparse map`, the ZIP tail, and the `.kuasar.digest.<hex>` marker are all encrypted on disk.
- **Fixed per-record overhead**: Both the sealed header and every data record follow the layout `1 B flag (0x01) + n B ciphertext + 16 B GCM tag`, adding a fixed **17 bytes** per record without storing explicit nonces on disk.

### 4.2 48 B Clear Prefix and 64 B Authenticated Header

Because the inner plaintext TAR is fully encrypted, the reader must first obtain key-derivation parameters and record-slicing geometry when opening a file. Accordingly, the encrypted envelope places a `48 B` clear prefix (magic and per-artifact random salt) followed by an `81 B` sealed header (decrypting to `64 B` of global geometry parameters) at the beginning of the file:

**1. 48-Byte Clear Prefix (file offset `[0, 48)`, Big-Endian):**

```text
  0                   8         10        12            16                                  48
  +-------------------+---------+---------+-------------+-----------------------------------+
  |    magic (8 B)    | ver(2B) | size(2B)| reserved(4B)|        artifactSalt (32 B)        |
  | 89 "KTSENC\n"     |    1    |    48   |  0x00000000 |     random per-artifact salt      |
  +-------------------+---------+---------+-------------+-----------------------------------+
```

**2. 64-Byte Authenticated Header (decrypted from file offset `[48, 129)`, Big-Endian):**

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

After decrypting the 64-byte authenticated header, the reader performs geometric consistency checks in stages (any failure returns `ErrMalformedEnvelope`):
1. **Header self-consistency (verified immediately upon header decryption)**: Requires `plaintextSize >= 2048` and `512 B` alignment, `recordSize == 4096`, first record plaintext length `firstSize == packedStart % 4096` (`4096` when divisible), `packedStart + packedSize <= plaintextSize - 2048`, and `recordCount` matching the count derived from `(plaintextSize, firstSize)`.
2. **Outer physical size and EOF verification (distinguished by read path)**: Random access (`SourceAt`) knows the outer physical file size at open time and **immediately verifies** that the derived `encryptedSize` equals the physical size; streaming access (`SourceFrom`) operates on an `io.Reader` of unknown total length and therefore verifies record completeness, total record count, and outer physical `EOF` (preventing truncation or trailing extraneous bytes) as records are consumed through stream completion.
3. **Inner/outer geometry anchoring (verified during open after parsing the inner TAR prefix)**: Both `SourceAt` and `SourceFrom` parse the inner TAR headers and `sparse map` during open and verify that the inner TAR's actual `packed data` start offset and length match the `packedStart` and `packedSize` declared in the outer authenticated header.

### 4.3 Key Derivation, Nonce, AAD, and Seal/Open Workflow

Slicing data into independent encrypted blocks alone does not prevent block reordering, cross-file splicing, or tail truncation. To defend against these attacks, tarstream derives a unique per-artifact key and binds the clear prefix, the 64 B authenticated header, the record index, and the actual plaintext length into each record's GCM Additional Authenticated Data (AAD):

```text
customerKey (32 B) --+
                     +--> HMAC-SHA256("kuasar/tarstream/aes-gcm/artifact-key/v1\x00" || salt)
artifactSalt (32 B) -+         |
                               v
                       artifactKey (32 B) --> AES-256-GCM Instance (bound once per artifact)
                                                     |
  +--------------------------------------------------+----------------------------------+
  |                                                  v                                  |
  | Nonce (12 B) : 0x00000000 (4B) || uint64BE(sequence) (8B)                           |  <-- Header: seq=0; Record i: seq=i+1
  |                                                                                     |
  | Header AAD   : "kuasar/tarstream/header/v1\x00" || clearPrefix(48B)                 |
  | Record i AAD : "kuasar/tarstream/record/v1\x00" || clearPrefix(48B)                 |  <-- Binds prefix, header geometry,
  |                || plainHeader(64B) || uint64BE(i) || uint32BE(actualPlainLen)       |      record index, and actual length
  |                                                                                     |
  | Encrypt/Seal : 0x01 || GCM.Seal(nonce, recordPlaintext, AAD) --> (len + 17) B       |
  | Decrypt/Open : verify record[0]==0x01 --> GCM.Open in-place                         |  <-- Zeroes scratch buffer on auth failure
  +-------------------------------------------------------------------------------------+
```

Because `recordAAD(i)` binds `clearPrefix` (including `salt`), `plainHeader` (including global geometry), record index `i`, and the record's plaintext length, **cross-file record splicing, record reordering, tail record truncation, or arbitrary byte tampering** are all rejected by layout, length, or GCM authentication checks.

### 4.4 Record Slicing Geometry: Aligning `packedStart` via Record 0

If records were sliced at fixed `4 KiB` intervals starting blindly from plaintext TAR offset `0`, the leading TAR headers and metadata would shift all subsequent payload data out of alignment, causing `4 KiB`-aligned payload pages to straddle two encrypted records. To avoid this amplification, **the plaintext length `F` of Record 0 is set to `packedStart mod 4096` (`4096` when divisible), ensuring that the start offset `packedStart` of `packed data` always lands on the start boundary of a record** (for instance, in the example below `packedStart = 3072` lands at the start of `Record 1`; if the metadata prefix were larger, e.g., `packedStart = 7168`, then `F = 3072` and `packedStart` would land at the start of `Record 2`. Note that this rule guarantees record-boundary alignment for `packedStart`; whether an individual subsequent payload page or extent straddles a record boundary still depends on whether preceding stored extents maintain `4 KiB`-multiple lengths):

```text
Plaintext TAR (using Section 2.7 example: packedStart = 3072, plaintextSize = 14336):
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

Let $P = \text{packedStart}$ and $L = \text{plaintextSize}$ (in canonical format, $L \ge P + 2048 > F$ always holds):
- First data record plaintext length: $F = ((P - 1) \bmod 4096) + 1$
- Total data record count: $N = 1 + \lceil (L - F) / 4096 \rceil$ ($N = 1$ when $L \le F$)
- Final encrypted file size: $\text{encryptedSize} = 48 + 81 + L + 17 \times N$
  (In the Section 2.7 example: $48 + 81 + 14336 + 17 \times 4 = 14533\text{ B}$)

### 4.5 External Identity for Encrypted Carriers (HMAC)

In encrypted mode, the inner `.kuasar.digest.<hex>` marker is sealed inside ciphertext. To allow external systems (such as cache managers or artifact registries) to perform deduplication and integrity anchoring without exposing plaintext content fingerprints, encrypted carriers compute a keyed `HMAC-SHA256` over `plainDigest` using `customerKey` as their external identity:

```text
                     +-- Plaintext (no Codec) --> "digest:" + hex(plainDigest)
plainDigest (32 B) --+
                     +-- Encrypted (with Codec)-> "hmac:"   + hex(HMAC-SHA256(customerKey, plainDigest))
```

- **Deterministic deduplication and verification**: Each encrypted write generates a fresh random `salt` (producing distinct ciphertext), yet under the same `customerKey`, identical logical content always yields the same `hmac:<hex>` external identity; different `customerKey` values yield distinct identities, preventing cross-tenant fingerprint leakage.
- **Distinguishing the two HMAC usages**: `KeyedDigest` computes HMAC directly over the raw 32 B `plainDigest` **without** a domain prefix and uses `customerKey` rather than `artifactKey`.

---

## 5. Writing, Random Access, and Verification Mechanisms

Chapters 1–4 defined the static storage layouts and cryptographic structures for plaintext and encrypted carriers. At runtime, the implementation must address two core engineering challenges:
- **Single-pass streaming emission**: The coverage bitmap, sparse map, and encrypted header reside at the beginning of the file, whereas the content digest resides at the end. How can the writer emit the entire carrier in a single streaming pass over the source data without staging temporary files?
- **Balancing fast open against full verification**: Online sandbox startup requires opening carriers rapidly and reading pages on demand, whereas offline import and format conversion require verifying every byte of the archive. How are verification responsibilities partitioned across read entry points?

This chapter details the write pipeline, random-access addressing, verification matrix, and interface selection criteria.

### 5.1 Two-Phase Write Pipeline (`WriteTo`)

`WriteTo` follows a strict two-phase pipeline: **complete metadata planning and validation first, followed by a single sequential pass over `Data` extents for streaming emission and hashing**:

```text
Input: ctx, io.Writer, name, sparse.Source, WriteOptions
  |
  v
+-------------------------------------------------------------------+
| Phase 1: Metadata Planning (makeWritePlanLimited)                 |  <-- Phase 1: planning & validation only; 0 bytes emitted, no Data read
+-------------------------------------------------------------------+
  |  1. Normalize member name; reject reserved prefixes (.kuasar.digest. / .kuasar.presence.v1)
  |  2. Query declared payloadSize if src implements IdentityProvider
  |  3. Pass 1 over RunAt (0..payloadSize): validate capacity (<=1TiB) & 4KiB Hole alignment; allocate bitmap within budget
  |  4. Pass 2 over RunAt (0..size): populate bitmap, omit full Zero pages in payload, merge extents, validate dense tail (<=64MiB)
  |  5. Encode PAX, GNU sparse map, and presence header; compute exact plaintextSize and encryption geometry
  |
  v  (Phase 2 starts only after all planning checks pass; any failure returns with 0 bytes written)
+-------------------------------------------------------------------+
| Phase 2: Streaming Emission & Hashing (emitPlan + recordWriter)   |  <-- Phase 2: single-pass Data read, online hashing, streaming write
+-------------------------------------------------------------------+
  |  1. [Optional encryption] Generate 32B random salt; write 48B Clear Prefix and 81B Sealed Header
  |  2. Emit Segments [1]~[4] prefix (presence + PAX + payload TAR header + sparse map)
  |  3. Iterate stored extents in order (copyExtent):
  |       - Data sub-ranges: read once via run.ReadAt
  |       - Unomitted Zero sub-ranges: synthesize zero bytes via clear(chunk) (no ReadAt call)
  |       - Write simultaneously to output stream and identityAccumulator (which also folds in omitted full Zero pages)
  |  4. [Optional] Execute WithBeforeCommit(ctx) hook (intercepts before committing final marker)
  |  5. Emit payloadPad + Segment [6] (.kuasar.digest.<hex> + 40B body + 2*512B all-zero end blocks)
  |  6. [Optional encryption] Flush final encrypted record; verify total plaintext bytes and record count
  |
  v
Output: (scheme, hexDigest, nil)
```

> **Source Stability and Failure Cleanup Contract**: The caller must keep `src` open and its sparse layout immutable throughout `WriteTo` execution (if Phase 2 detects any discrepancy against the Phase 1 plan, it aborts immediately). If Phase 2 or the `beforeCommit` hook returns an error, any partially emitted bytes must be cleaned up by the caller (for example, by removing the uncommitted temporary file).

### 5.2 Plaintext and Encrypted Random-Access Addressing (`SourceAt`)

Once opened, `SourceAt` holds in memory the `presence` bitmap, the sorted `extents` slice, and its prefix-sum slice `packedPrefix` (where `packedPrefix[i]` is the byte offset of extent `i` within `packed data`). A random read `ReadAt(ctx, buf, x)` resolves logical offset `x` through the following chain:

```text
Logical Offset x
  |
  +--> 1. Binary search extentAt(x)
  |         +-- If in an unstored range (Hole or omitted full Zero page): fill 0x00 via clear() and return
  |         |   (For RunAt calls, combine with presence[x/4096] to return exact Hole or Zero state)
  |         +-- If hitting extent[i] = [Oi, Oi + Li): proceed to Step 2
  |
  +--> 2. Map to plaintext TAR physical offset (O(1) arithmetic)
  |         tarOffset = packedStart + packedPrefix[i] + (x - Oi)
  |         +-- [Plaintext carrier] Call underlying ReaderAt.ReadAt(p, tarOffset) directly --> Done!
  |         +-- [Encrypted carrier] Proceed to Step 3 (recordReaderAt)
  |
  +--> 3. Map to encrypted record index and physical ciphertext offset (O(1) arithmetic, first record length = F)
  |         recordIndex(tarOffset) = 0                                   (when tarOffset < F)
  |                                = 1 + (tarOffset - F) / 4096          (when tarOffset >= F)
  |         plainStart(idx)        = 0                                   (when idx == 0)
  |                                = F + (idx - 1) * 4096                (when idx >= 1)
  |         cipherOffset(idx)      = 129 + plainStart(idx) + idx * 17
  |
  +--> 4. Coalesced I/O and per-record GCM authenticated decryption (readBatch)
            - Coalesce adjacent records into a single read (capped at maxCoalesceBytes = 1 MiB)
            - Acquire scratch buffer from sync.Pool; verify AAD & GCM Tag and decrypt each record in place
            - Copy target slice [tarOffset - plainStart(idx) : ...] to caller buffer
            - Wipe plaintext/ciphertext/AAD via clear() before returning buffer to pool (no plaintext cache retained)
```

### 5.3 Verification Matrix: `SourceAt` vs `SourceFrom` vs `ReadFrom`

Different read entry points partition integrity and authenticity checks between **open time** and **read/completion time**:

| Verification Dimension                                  | `SourceAt` (Random-Access Open)                                                                                                  | `SourceFrom` (Sequential Full-Stream Consumption)                                        | `ReadFrom` / `ReadSeekFrom`            |
| ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | -------------------------------------- |
| **Encrypted Prefix & 64B Auth Header**                  | **Verified at open** (including physical file size and inner `packedStart`/`packedSize` match)                                   | **Verified at open** (including inner `packedStart`/`packedSize` match)                  | Encrypted carriers unsupported         |
| **Presence CRC32 & Bitmap Consistency**                 | **Verified at open** (including zero trailing bits and no conflict with sparse extents)                                          | **Verified at open**                                                                     | **Verified at open**                   |
| **Canonical PAX & Payload TAR Headers**                 | **Verified at open** (strict byte-for-byte match against canonical template when marker is present)                              | **Verified at completion**                                                               | Parsed only; canonical not enforced    |
| **Digest Marker & 2×512B End Blocks**                   | **Located and verified at open** (markerless plaintext TAR accepted only when no presence, no codec, and no expected digest)     | **Verified after consuming final Data** (always requires marker; verified at open if no Data) | Marker not read or verified       |
| **Dense Metadata Tail (`[payloadSize, logicalSize)`)**  | **Fully read (with GCM auth if encrypted) at open to recompute `tailDigest` and verify `composeDigest == plainDigest`** (canonical with marker) | **Read along stream to recompute `tailDigest`**                         | Read on demand as ordinary bytes; unverified |
| **Primary Payload (`[0, payloadSize)`)**                | **Verified lazily on demand**: payload not pre-read and `payloadCommitment` not recomputed; GCM Tag verified only for records actually read | **Fully verified**: recomputes full `payloadCommitment` and `plainDigest`, constant-time compared against marker | Read on demand; digest unverified |
| **`WithExpectedDigest` Anchor Match**                   | **Matched at open** against `digest`/`hmac` exported from marker                                                                 | **Matched at completion** against recomputed `digest`/`hmac`                             | `ReadOption` unsupported               |
| **Outer Physical EOF (No Trailing Extraneous Bytes)**   | **Verified at open**: canonical carriers with marker require `markerEnd + 1024 == size` (encrypted also checks derived `encryptedSize`) | **Verified at completion**: reads 1 extra byte after the two zero blocks to confirm `io.EOF` | Unverified                     |

> **Important Note**: Passing `WithExpectedDigest` to `SourceAt` immediately verifies the encrypted authenticated header, the marker declaration, and the entire `dense metadata tail` (such as the trailing ZIP), but **does not trigger a full read and verification of the primary payload `[0, payloadSize)`**. For untrusted streaming ingestion or conversion pipelines where every payload byte must be verified against `payloadCommitment`, `SourceFrom` must be used and consumed to completion.

### 5.4 Core Interface Selection Reference

| Function / Interface        | Target Scenario                                                                      | Key Characteristics and Constraints                                                |
| --------------------------- | ------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------- |
| `WriteTo`                   | Encode and emit a `sparse.Source` as a canonical tarstream (plaintext or encrypted v1) | Two-phase emission; returns `(scheme, hexDigest, error)`                         |
| `SourceAt`                  | Random sparse reads over local files or NFS supporting `io.ReaderAt` with known size | Concurrency-safe; verifies metadata and dense tail at open, reads payload on demand |
| `SourceFrom`                | Single-pass sequential consumption and full verification in streaming pipelines      | Monotonically increasing `ReadAt` (no backward seeks); final verification triggers after all Data is read |
| `ReadFrom` / `ReadSeekFrom` | Standard `io.Reader` / `io.ReadSeeker` logical byte view plus `Holes()` list         | **Plaintext TAR only**; does not verify `.kuasar.digest.` or distinguish Zero vs Data |
| `ReadSeekFromIndex`         | Address plaintext TAR members by physical entry index (0-based, skipping PAX headers) | Matches `archive/tar.Reader.Next` indexing; allows direct access to internal members |
| `CarrierDigest`             | Derive external identity under target write options from an `IdentityProvider` source | **Zero payload I/O**; directly exports `digest:<hex>` or `hmac:<hex>`             |
| `ComposeDigest`             | Compute new `plainDigest` when replacing only dense tail bytes with known `payloadCommitment` | Requires `len(tail) == totalSize - payloadSize <= 64 MiB`                |

---

## 6. Format Constraints, Compatibility, and Common Pitfalls

Building on the core format specification and read/write workflows, this final chapter summarizes the hard capacity and memory quotas, backward-compatibility rules for legacy archives, and common misconceptions encountered during integration and troubleshooting.

### 6.1 Format Quotas and Performance Characteristics Reference

| Dimension                           | Quota / Parameter                 | Description                                                                                            |
| ----------------------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------ |
| **Max Archive / Member Size**       | `1 << 62` B (4 EiB)               | Reads and writes exceeding this limit are rejected                                                     |
| **Max Payload Size**                | `1 TiB` (`1 << 40` B)             | Managed by `.kuasar.presence.v1`, corresponding to a maximum bitmap size of `32 MiB`                   |
| **Presence Page Size**              | `4096 B` (4 KiB)                  | `Hole` intervals must be 4 KiB-aligned (unaligned end permitted only at `payloadSize`)                 |
| **Max Dense Metadata Tail Size**    | `64 MiB` (`64 << 20` B)           | Range `[payloadSize, logicalSize)` must be completely dense (no Holes allowed)                         |
| **Max Sparse Extent Count**         | `1 << 20` (1,048,576)             | Includes trailing `(logicalSize, 0)` sentinel and merged tail extent                                   |
| **Index-Memory Accounting Baseline**| `2 MiB` + cumulative planning cost| Unbounded unless `WithIndexMemoryLimit` is set; charges `2 MiB` baseline plus cumulative planning allocations |
| **Min Empty-Payload Carrier Size**  | Plain `5120 B` / Enc `5283 B`     | Plaintext: presence(1024B)+PAX(1024B)+Header(512B)+Map(512B)+End(2048B); Encrypted adds `48+81+17×2=163 B` |
| **Encrypted Record Size & Batch**   | Target plain `4096 B` (`+17 B`)   | Random read authenticates at least 1 record; adjacent record coalescing capped at `1 MiB`              |
| **Explicit Zero Hashing Cost**      | Proportional to logical length    | Omitted full Zero pages save disk/network I/O, but synthesized zero bytes are still hashed into `payloadCommitment` |

### 6.2 Legacy Carrier Compatibility and Downgrade Boundaries

```text
Reader Dispatch Decision Tree (SourceAt / SourceFrom):
  Do the first 8 B match the encrypted magic (89 "KTSENC\n")?
    +-- Yes --> Enforce Encrypted v1 parsing (any auth/format failure errors out; never falls back to plaintext)
    +-- No  --> If required=true, reject immediately (ErrPlaintextForbidden); otherwise parse as plaintext:
                  +-- Is the first member .kuasar.presence.v1?
                        +-- Yes --> Enforce Presence v1 Canonical rules (requires KUASAR.presence=1,
                        |           KUASAR.payload.size, canonical headers, and trailing .kuasar.digest.<hex>)
                        +-- No  --> Legacy presence-free plaintext carrier:
                                      +-- Has .kuasar.digest.<hex> marker: validate under legacy canonical rules (byte-granularity Holes allowed)
                                      +-- No marker: accepted ONLY by SourceAt when neither WithCodec nor WithExpectedDigest is set;
                                                     SourceFrom always requires a valid marker
```

- **Admission differences for markerless plaintext TAR**: `SourceAt` accepts a markerless ordinary plaintext TAR only when `.kuasar.presence.v1` is absent, `WithCodec` is not provided, and `WithExpectedDigest` is not set (the returned `sparse.Source` does not implement `Digester` / `IdentityProvider`). Whenever presence is present, a codec or expected digest is supplied, or `SourceFrom` is used, a missing marker is rejected with `ErrInvalidCanonicalTarstream`.
- **Read-only compatibility, no write fallback**: Readers continue to support legacy presence-free plaintext carriers (such as the `3584 B` empty carrier in historical golden fixtures), whereas `WriteTo` **emits only the presence v1 format**. Passing a legacy carrier with non-4-KiB-aligned byte-granularity Holes directly to `WriteTo` fails during Phase 1 planning rather than silently padding or falling back.
- **Unsupported external TAR variants**: Legacy GNU binary sparse headers (`typeflag = 'S'`) and PAX `0.x` sparse formats (`GNU.sparse.numblocks` / `GNU.sparse.map`) are rejected with `ErrUnsupportedEncoding`.

### 6.3 Common Misconceptions vs. Actual Behavior

| Common Misconception                                                   | Actual Behavior                                                                                                                                                                                    |
| ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| "tarstream is just a standard `tar.gz` archive"                        | The format combines `.kuasar.presence.v1` + GNU PAX sparse 1.0 + an optional AES-256-GCM record envelope; there is no built-in gzip layer                                                          |
| "Reading all zeros from `ReadAt` means the range is a `Hole`"          | `Hole`, `Zero`, and all-zero `Data` all return zero bytes via `ReadAt`; preserving multi-layer stacking semantics requires calling `RunAt`                                                         |
| "Extracting with `tar -xf` and re-packing preserves everything"        | `tar -xf` reconstructs logical bytes accurately, but standard filesystems cannot distinguish an "uncovered Hole" from an "explicitly zeroed Zero page"                                             |
| "The ZIP EOCD can be found at the physical end of the TAR file"        | The physical end of the TAR file is the `2048 B` marker and end blocks; callers must open the `sparse.Source` logical view first and then invoke `tailzip.Locate`                                  |
| "A successful `SourceAt` open proves every byte in the file is intact" | `SourceAt` verifies headers, the marker, and the **dense metadata tail** at open time, but payload records in `[0, payloadSize)` are authenticated lazily as read; full verification requires `SourceFrom` |
