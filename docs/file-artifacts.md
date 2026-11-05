[English](file-artifacts.md) | [简体中文](file-artifacts_zh.md)

# File artifacts and carriers

This specification owns the generic immutable-file encryption, tarstream and multi-Manifest Bundle contracts. Logical Manifest/Chunk objects and keys remain in [manifest.md](manifest.md); persistence/admission belongs to [store.md](store.md). It does not define Sandbox E/S contents or OCI/EROFS image semantics. Those consumers apply these carriers according to their own [sandbox artifact](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox.md#artifact-model) and [image](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten.md) contracts.

## 1. Immutable local tarstreams and encryption

In Kuasar, container images, sandbox root disks (Sandbox E), and virtual machine memory snapshots (Snapshot S) are packaged as single immutable carrier files for persistence and distribution across local disks, shared filesystems, and remote repositories; at the same time, the runtime performs direct random reads (`ReadAt`) by logical offset without prior extraction.

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

New canonical plaintext tarstreams contain a **presence entry (`.kuasar.presence.v1`)**, the **sparse payload (with an optional dense metadata tail)**, a **`.kuasar.digest.<plainDigest>` marker** and **exactly two trailer blocks**. Readers also accept existing portable carriers without presence. Payload metadata defines its `payloadSize` boundary; the marker body records that boundary and the `payloadCommitment`. Full reads recompute the commitment from payload bytes:

```text
[1. Logical File View] (logicalSize = payloadSize + tailSize)
  0                                                payloadSize        logicalSize
  +--------------+--------------+--------------+--------+------------------+
  |    Data A    |     Hole     |  Zero (4KiB) | Data B |  Dense ZIP Tail  |
  +------+-------+--------------+--------------+---+----+--------+---------+
         |              (omitted on wire)          |             |
         +----------------------+------------------+-------------+
                                v                                v
[2. Plaintext Canonical tarstream View] (plaintextSize, 512 B aligned)
  0                                       packedStart       packedStart+packedSize
  +-------------------+-------------------+-------+----+--------+---------+----------------+
  |   Presence Entry  |  Payload Headers  |Data A |Data|ZIP Tail| Padding | Marker + 2*EOF |
  |(.kuasar.presence) |   & Sparse Map    |       | B  |        | (0~511B)| (Fixed 2048 B) |
  +-------------------+-------------------+-------+----+--------+---------+----------------+
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

| Coordinate Space        | Range                | Offset Semantics and Characteristics                                                                                    |
| ----------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **Logical File View**   | `[0, logicalSize)`   | Leading `[0, payloadSize)` is the sparse payload; trailing `[payloadSize, logicalSize)` is the optional dense ZIP tail  |
| **Plaintext tarstream** | `[0, plaintextSize)` | Contains prefix metadata and contiguously concatenated `packed data`, ending with a fixed `2048 B` marker + trailer     |
| **Encrypted Envelope**  | `[0, encryptedSize)` | Slices the complete plaintext tarstream into `N` authenticated records, each adding `1 B flag + 16 B tag = 17 B`        |

The fixed `2048 B` (`4 × 512 B`) trailing marker and trailer blocks have the following layout:

```text
+-----------------------------------------------------------------------+ 0
| Marker Header (512 B): name = ".kuasar.digest.<64-char-lowercase-hex>"|  <-- Member name carries plainDigest
+-------------------------------------------+---------------------------+ 512
| Marker Body (40 B)                        | Zero Padding (472 B)      |  <-- Persists payloadCommitment for
|   [0..8)  : payloadSize (uint64 BE)       |   [40..512) must be 0x00  |      fast tail-replacement derivation
|   [8..40) : payloadCommitment (32 B raw)  |                           |
+-------------------------------------------+---------------------------+ 1024
| Archive Trailer Blocks (2 * 512 B = 1024 B, must be all 0x00)         |
+-----------------------------------------------------------------------+ 2048 (TAR EOF)
```

### 1.1 Page presence and Zero transport

Every new tarstream uses the presence v1 profile. Its logical payload may be empty or end in a partial 4 KiB page, and is limited to **1 TiB**. Hole runs must start on a page boundary and end either on a page boundary or at the exact payload end. Writers reject oversized or unrepresentable layouts during metadata planning, before emitting any bytes (including an encrypted header) or reading Data. They do not write the old format, pad the payload or alter Hole boundaries. The size limit applies to the payload before its optional declared dense metadata tail; that tail retains its separate **64 MiB** limit. The profile's order is:

```text
regular .kuasar.presence.v1
payload with KUASAR.presence=1 and sparse metadata
existing .kuasar.digest.<plainDigest> marker
two zero TAR trailer blocks
```

The internal presence entry is not a tail ZIP member. Its body begins with a fixed 64-byte little-endian header followed immediately by the bitmap bytes:

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

With `pages = ceil(payloadSize / 4096)`, logical page `i` (`0 <= i < pages`) maps to bit `i % 8` of `bitmap[i / 8]` (LSB corresponds to the lowest page index), and exactly `ceil(pages / 8)` bitmap bytes follow the header. An empty payload has zero bitmap bytes (`bitmapLength = 0`) but still includes the 64 B presence header and payload mark. The last page ends at the exact `payloadSize`; unused bits and TAR padding must be zero. The bitmap excludes the declared dense metadata tail and contains at most **32 MiB** of bitmap bytes (a `1 GiB` payload requires a `32 KiB` bitmap):

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

The writer omits physical bytes only for **complete 4 KiB-aligned pages covered by a single `sparse.Zero` run within `[0, payloadSize)`**; partial-page Zero fragments (as well as any Zero runs inside the dense tail) remain literal Data within this same profile. All-Zero payloads do not allocate a literal-zero payload. Dense byte-sized sources, including ZIP-only checkpoint metadata, preserve their exact lengths. Ordinary all-zero Data is not detected or compressed:

```text
Single Zero run crossing unaligned boundaries: [2048, 10240)
  Page 0 [0, 4096)       Page 1 [4096, 8192)     Page 2 [8192, 12288)
  +---------+---------+ +---------------------+ +---------+---------+
  | (Other) |Zero Frag| |   Full 4 KiB Zero   | |Zero Frag| (Other) |
  +---------+----+----+ +----------+----------+ +----+----+---------+
                 |                 |                 |
                 v                 v                 v
         Kept in stored extent Omitted physically     Kept in stored extent
         (literal 0x00 stored) (presence=1 only)      (literal 0x00 stored)
```

By jointly querying the `presence` bitmap and the stored payload extents within `[0, payloadSize)`, the reader reconstructs all three states without ambiguity:

| `presence` Bit (`P`) | Stored Payload Extent Present | Physical Bytes Stored in Carrier | Reconstructed `RunAt` State    | Single-Layer `ReadAt` Result |
| -------------------- | ----------------------------- | -------------------------------- | ------------------------------ | ---------------------------- |
| `P = 0`              | No                            | No                               | **Hole**                       | Synthesized `0x00`           |
| `P = 1`              | No                            | No                               | **Zero** (full page omitted)   | Synthesized `0x00`           |
| `P = 1`              | Yes                           | Yes                              | **Data** (incl. sub-page Zero) | Stored physical bytes        |
| `P = 0`              | Yes                           | -                                | **Invalid format** (rejected)  | -                            |

Presence and its payload mark must occur exactly once and agree. Wrong version, geometry, checksum, unused bits, placement, duplicates, missing mark/entry or Data under `P=0` are errors, never a reason to retry legacy parsing. Empty-name source selection skips the internal member. Ordinal APIs retain physical entry ordinals; selecting the payload attaches the preceding presence entry. `Holes` reports only `P=0`, not Zero. Old portable carriers remain readable, including their empty payloads and exact byte-granularity Hole boundaries. Rewriting a readable old carrier always uses presence v1 when its logical source meets the new writer constraints; otherwise rewriting fails without emitting an old-format carrier. Readers must support these presence extensions before writers emit empty, partial-page or larger-than-32-GiB payloads.

Presence bytes are inside authenticated records when encryption is enabled. Readers authenticate them before classifying ranges. Full sequential consumption verifies identity, padding, archive end and outer EOF even with no Data, or trailing Zero/Hole. Fast random/partial access retains its existing limited verification scope. A one-record metadata window avoids repeated decryption during open and is released before returning the source; it does not cache later payload reads.

The existing shared reader/writer extent limit remains `1<<20`, including the end sentinel and the final tail merge. Writers reject excess final extents before emitting any bytes (including an encrypted header) or reading Data. Presence planning is charged to the existing index budget before allocation; it adds no payload bitmap or prefix index. Expanding Zero to literal Data cannot be used to evade the limit. Tests and benchmark results must report index budget, logical bytes, transported bytes and identity CPU separately.

`go test ./pkg/manifest/crypto -run '^$' -bench '^BenchmarkPresenceTarStream$' -benchtime=3x` compares 64 MiB logical sources with 0%, 50% and 100% Zero, in plaintext and AES. The 50% case alternates 1 MiB Data/Zero runs. It reports encoded bytes, write time, and open allocations including indexes and authentication scratch; the prebuilt wire buffer is excluded from open allocations. Apply the same benchmark source to the previous implementation and run at least three paired rounds. Zero-free carriers pay the bitmap overhead; all-Zero carriers save almost all wire bytes but still pay linear hashing cost, including validation when the sequential reader opens a stream with no Data. Random opening retains its partial verification boundary. These measurements do not imply constant-time identity or a disk-I/O throughput claim.

### 1.2 Three-layer structured content identity (Digest and HMAC)

The identity domain and algorithm are independent of physical encoding. Rather than computing a flat SHA-256 over the physical file bytes, tarstream derives `plainDigest` via a three-layer structured hash over logical coverage intervals, present bytes, normalized member name, and size boundaries (all multi-byte integers are **big-endian** `uint64`):

```text
+------------------------------------------------------------------+
| Layer 1: payloadCommitment                                       |
|   SHA256(                                                        |
|     "kuasar/tarstream/payload\x00"                               |
|     || uint64BE(payloadSize)                                     |
|     || uint64BE(presentIntervalCount)                            |  <-- Contiguous present intervals merged from presence (P=1)
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

- **Representation independence**: `payloadCommitment` enumerates logical `P=1` intervals clipped to `payloadSize` and hashes Data plus synthesized Zero bytes; the writer counts and streams those intervals without another extent index. A legacy literal-zero carrier and its equivalent omitted-Zero carrier have the same logical identity, so existing content-addressed objects may be reused. Hashing Zero still requires CPU linear in its logical length; omission saves transport/allocation, not the work required by the existing identity algorithm. Payload transport metadata (mode, ownership and timestamps) uses fixed canonical encoding and cannot change while retaining the same identity.
- **Fast derivation when replacing only the tail (`CarrierDigest` / `ComposeDigest`)**: A complete carrier, or a derived source replacing only its dense metadata tail, can report identity through `CarrierDigest` without first encoding the payload into `io.Discard`. A derived dense metadata tail is limited to **64 MiB**; readers and writers share this limit and reject oversized declarations before hashing:

```text
Marker Body (existing)  --> (payloadSize, payloadCommitment) --+
                                                               +--> ComposeDigest --> new plainDigest (zero payload I/O)
Replacement ZIP Tail    --> new tailDigest + new logicalSize --+
```

- **External identity mapping for plaintext and encrypted carriers**: Without a codec, the public identity is `digest:<plainDigest>`. With a customer-key-backed codec, the complete structure is encoded as encrypted tarstream v1, and its public identity is fixed as:

```text
                     +-- Plaintext (no Codec) --> "digest:" + hex(plainDigest)
plainDigest (32 B) --+
                     +-- Encrypted (with Codec)-> "hmac:"   + hex(HMAC-SHA256(customerKey, plainDigestRaw32Bytes))
```

The scheme is named `hmac`. It does not derive a separate identity key or add a domain prefix. The marker's plaintext digest and payload commitment remain inside ciphertext and must not be used in plaintext filenames, references, logs or errors. The physical carrier determines the identity scheme: `auto` returns `@digest` for a plaintext carrier and `@hmac` for an encrypted one, without implicit conversion.

### 1.3 Encrypted envelope format (encrypted tarstream v1)

encrypted tarstream v1 fixes **AES-256-GCM** and independently authenticated **4096-byte records**. It negotiates neither algorithm nor record size. When encryption is enabled, the complete plaintext canonical tarstream (from the presence entry through the two zero trailer blocks) is partitioned into contiguous authenticated records. The file comprises a **48-byte clear prefix**, an **81-byte authenticated header**, then authenticated records:

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

**1. 48-Byte Clear Prefix (file offset `[0, 48)`, Big-Endian):**

Prefix bytes `[0:16]` hold magic, version, prefix size and reserved fields; `[16:48]` hold a fresh 32-byte artifact salt from `crypto/rand`:

```text
  0                   8         10        12            16                                  48
  +-------------------+---------+---------+-------------+-----------------------------------+
  |    magic (8 B)    | ver(2B) | size(2B)| reserved(4B)|        artifactSalt (32 B)        |
  | 89 "KTSENC\n"     |    1    |    48   |  0x00000000 |     random per-artifact salt      |
  +-------------------+---------+---------+-------------+-----------------------------------+
```

**2. 64-Byte Authenticated Header (decrypted from file offset `[48, 129)`, Big-Endian):**

The header binds total plaintext size, packed-payload start/size and record geometry:

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

After decrypting the 64-byte authenticated header, the reader performs geometric consistency checks in stages (any failure returns `ErrMalformedEnvelope`):
1. **Header self-consistency (verified immediately upon header decryption)**: Requires `plaintextSize >= 2048` and `512 B` alignment, `recordSize == 4096`, first record plaintext length `firstSize == packedStart % 4096` (`4096` when divisible), `packedStart + packedSize <= plaintextSize - 2048`, and `recordCount` matching the count derived from `(plaintextSize, firstSize)`.
2. **Outer physical size and EOF verification (distinguished by read path)**: Random access (`SourceAt`) knows the outer physical file size at open time and **immediately verifies** that the derived `encryptedSize` equals the physical size; streaming access (`SourceFrom`) operates on an `io.Reader` of unknown total length and therefore verifies record completeness, total record count, and outer physical `EOF` (preventing truncation or trailing extraneous bytes) as records are consumed through stream completion.
3. **Inner/outer geometry anchoring (verified during open after parsing the inner prefix metadata)**: Both `SourceAt` and `SourceFrom` verify that the inner stream's actual `packed data` start offset and length match the `packedStart` and `packedSize` declared in the outer authenticated header.

**3. Key Derivation, Nonce, AAD, and Authenticated Seal/Open Workflow:**

Once per artifact, the codec derives `artifactKey` and constructs one AES-256-GCM instance. The header uses sequence `0`; data record `i` uses `i+1`. The 12-byte GCM nonce is `0x00000000 || uint64BE(sequence)`. Each wire record consists of a one-byte AES-GCM flag (`0x01`), ciphertext and a 16-byte tag: **17 bytes of overhead**, with no separately stored per-record nonce. Each record's AAD binds the complete prefix including salt, authenticated header, record index and actual plaintext length:

```text
customerKey (32 B) --+
                     +--> HMAC-SHA256("kuasar/tarstream/aes-gcm/artifact-key/v1\x00" || artifactSalt)
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

Because `recordAAD(i)` binds `clearPrefix` (including `salt`), `plainHeader` (including global geometry), record index `i`, and the record's plaintext length, **cross-file record splicing, record reordering, tail record truncation, or arbitrary byte tampering** are all rejected by layout, length, or GCM authentication checks. Random artifact salt makes two writes of the same plaintext/customer key produce different physical ciphertext, while the logical identity remains `HMAC-SHA256(customerKey, plainDigestRaw32Bytes)`, preserving the scheme, filename and logical deduplication key.

**4. Record Slicing Geometry (Aligning `packedStart` via Record 0):**

To prevent leading metadata from shifting subsequent `4 KiB`-aligned payload pages across two encrypted records, **the plaintext length `F` of Record 0 is set to `packedStart mod 4096` (`4096` when divisible), ensuring that the start offset `packedStart` of `packed data` always lands on the start boundary of a record** (for instance, in the example below `packedStart = 3072` lands at the start of `Record 1`; if the metadata prefix were larger, e.g., `packedStart = 7168`, then `F = 3072` and `packedStart` would land at the start of `Record 2`. Note that this rule guarantees record-boundary alignment for `packedStart`; whether an individual subsequent payload page or extent straddles a record boundary still depends on whether preceding stored extents maintain `4 KiB`-multiple lengths):

```text
Plaintext Stream (example: packedStart = 3072, plaintextSize = 14336):
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

### 1.4 Write pipeline, random-access addressing, and verification matrix

**1. Two-Phase Write Pipeline (`WriteTo`):**

Writing first sweeps sparse metadata and plans layout, then reads Data extents once; it does not create a plaintext staging/spool file:

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
  |  5. Encode prefix metadata and presence header; compute exact plaintextSize and encryption geometry
  |
  v  (Phase 2 starts only after all planning checks pass; any failure returns with 0 bytes written)
+-------------------------------------------------------------------+
| Phase 2: Streaming Emission & Hashing (emitPlan + recordWriter)   |  <-- Phase 2: single-pass Data read, online hashing, streaming write
+-------------------------------------------------------------------+
  |  1. [Optional encryption] Generate 32B random salt; write 48B Clear Prefix and 81B Sealed Header
  |  2. Emit prefix metadata (presence + payload headers + sparse map)
  |  3. Iterate stored extents in order (copyExtent):
  |       - Data sub-ranges: read once via run.ReadAt
  |       - Unomitted Zero sub-ranges: synthesize zero bytes via clear(chunk) (no ReadAt call)
  |       - Write simultaneously to output stream and identityAccumulator (which also folds in omitted full Zero pages)
  |  4. [Optional] Execute WithBeforeCommit(ctx) hook (intercepts before committing final marker)
  |  5. Emit alignment padding + .kuasar.digest.<hex> marker (40B body) + 2*512B all-zero trailer blocks
  |  6. [Optional encryption] Flush final encrypted record; verify total plaintext bytes and record count
  |
  v
Output: (scheme, hexDigest, nil)
```

**2. Random-Access Addressing (`SourceAt`) and Full Sequential Validation (`SourceFrom`):**

`SourceAt` reads prefix salt, binds the artifact codec, authenticates the header and suffix records covering marker/trailer (and any declared dense metadata tail), then decrypts records by index for $O(1)$ random access:

```text
Logical Offset x
  |
  +--> 1. Binary search extentAt(x)
  |         +-- If in an unstored range (Hole or omitted full Zero page): fill 0x00 via clear() and return
  |         |   (For RunAt calls, combine with presence[x/4096] to return exact Hole or Zero state)
  |         +-- If hitting extent[i] = [Oi, Oi + Li): proceed to Step 2
  |
  +--> 2. Map to plaintext physical offset (O(1) arithmetic)
  |         plainOffset = packedStart + packedPrefix[i] + (x - Oi)
  |         +-- [Plaintext carrier] Call underlying ReaderAt.ReadAt(p, plainOffset) directly --> Done!
  |         +-- [Encrypted carrier] Proceed to Step 3 (recordReaderAt)
  |
  +--> 3. Map to encrypted record index and physical ciphertext offset (O(1) arithmetic, first record length = F)
  |         recordIndex(plainOffset) = 0                                   (when plainOffset < F)
  |                                  = 1 + (plainOffset - F) / 4096        (when plainOffset >= F)
  |         plainStart(idx)          = 0                                   (when idx == 0)
  |                                  = F + (idx - 1) * 4096                (when idx >= 1)
  |         cipherOffset(idx)        = 129 + plainStart(idx) + idx * 17
  |
  +--> 4. Coalesced I/O and per-record GCM authenticated decryption (readBatch)
            - Coalesce adjacent records into a single read (capped at maxCoalesceBytes = 1 MiB)
            - Acquire scratch buffer from sync.Pool; verify AAD & GCM Tag and decrypt each record in place
            - Copy target slice [plainOffset - plainStart(idx) : ...] to caller buffer
            - Wipe plaintext/ciphertext/AAD via clear() before returning buffer to pool (no plaintext cache retained)
```

This fast path does not pre-read all data records or recompute the complete plaintext digest. Different artifacts use different derived keys, so a spliced donor record fails authentication **when that record is actually read**. A donor record outside the fast-open read set does not cause an early error.

Plaintext `SourceAt` does not verify payload integrity, even if `WithExpectedDigest` matches and the caller reads the entire payload; its reads return the stored bytes. Encrypted carriers receive GCM authentication only for records actually read; neither path recomputes the full `payloadCommitment` through random reads. For plaintext integrity, fully consume `SourceFrom` and propagate terminal validation errors.

`SourceFrom` is the full-validation path. Complete consumption decrypts every record, recomputes payload/tail commitments and carrier identity using the authoritative sparse map, and verifies the marker body, exactly two trailer blocks, expected `digest|hmac` and outer EOF. A cross-artifact record splice fails when sequential consumption reaches it. If a caller stops early, no full-validation conclusion applies to unread data. Publishing, uploading and converting paths must consume all Data extents and propagate terminal validation errors.

| Verification Dimension                                 | `SourceAt` (Random-Access Open)                                                                                                              | `SourceFrom` (Sequential Full-Stream Consumption)                                              | `ReadFrom` / `ReadSeekFrom`                   |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | --------------------------------------------- |
| **Encrypted Prefix & 64B Auth Header**                 | **Verified at open** (including physical file size and inner `packedStart`/`packedSize` match)                                               | **Verified at open** (including inner `packedStart`/`packedSize` match)                        | Encrypted carriers unsupported                |
| **Presence CRC32 & Bitmap Consistency**                | **Verified at open** (including zero trailing bits and no conflict with stored extents)                                                      | **Verified at open**                                                                           | **Verified at open**                          |
| **Canonical Transport Headers** | **Verified at open** for presence-v1; legacy carriers require canonical headers when a digest marker is present | **Verified at open** for presence-v1; legacy canonical validation occurs at full-consumption completion (at open if no Data) | **Verified at open** for presence-v1; legacy headers are parsed without canonical enforcement |
| **Digest Marker & 2×512B Trailer Blocks**              | **Located and verified at open** (markerless plaintext carrier accepted only when no presence, no codec, and no expected digest)             | **Verified after consuming final Data** (always requires marker; verified at open if no Data)  | Marker not read or verified                   |
| **Dense Metadata Tail (`[payloadSize, logicalSize)`)** | **Fully read (with GCM auth if encrypted) at open to recompute `tailDigest` and verify `composeDigest == plainDigest`** (canonical w/marker) | **Read along stream to recompute `tailDigest`**                                                | Read on demand as ordinary bytes; unverified  |
| **Primary Payload (`[0, payloadSize)`)** | **Plaintext: unverified**, even after a full read with `WithExpectedDigest`. **Encrypted: GCM authentication on records actually read**; neither path recomputes the full `payloadCommitment` | **Fully verified**: recomputes full `payloadCommitment` and `plainDigest`, compared vs. marker | Read on demand; digest unverified |
| **`WithExpectedDigest` Anchor Match** | **Matched at open** against the declared `digest`/`hmac` from the marker; does not validate unread payload or add plaintext payload verification | **Matched at completion** against recomputed `digest`/`hmac` | `ReadOption` unsupported |
| **Outer Physical EOF (No Trailing Extraneous Bytes)**  | **Verified at open**: canonical carriers with marker require `markerEnd + 1024 == size` (encrypted also checks derived `encryptedSize`)      | **Verified at completion**: reads 1 extra byte after the two zero blocks to confirm `io.EOF`   | Unverified                                    |

**3. Core APIs and Quotas Reference:**

| Function / Interface        | Target Scenario                                                                               | Key Characteristics and Constraints                                                         |
| --------------------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `WriteTo`                   | Encode and emit a `sparse.Source` as a canonical tarstream (plaintext or encrypted v1)        | Two-phase emission; returns `(scheme, hexDigest, error)`                                    |
| `SourceAt`                  | Random sparse reads over local files or NFS supporting `io.ReaderAt` with known size          | Concurrency-safe; verifies metadata and dense tail at open, reads payload on demand         |
| `SourceFrom`                | Single-pass sequential consumption and full verification in streaming pipelines               | Monotonically increasing `ReadAt` (no backward seeks); final verification after all Data    |
| `ReadFrom` / `ReadSeekFrom` | Standard `io.Reader` / `io.ReadSeeker` logical byte view plus `Holes()` list                  | **Plaintext carrier only**; does not verify `.kuasar.digest.` or distinguish Zero vs Data   |
| `ReadSeekFromIndex`         | Address plaintext carrier members by physical entry index (0-based)                           | Allows direct access to internal members; selecting payload attaches preceding presence     |
| `CarrierDigest`             | Derive external identity under target write options from an `IdentityProvider` source         | **Zero payload I/O**; directly exports `digest:<hex>` or `hmac:<hex>`                       |
| `ComposeDigest`             | Compute new `plainDigest` when replacing only dense tail bytes with known `payloadCommitment` | Requires `len(tail) == totalSize - payloadSize <= 64 MiB`                                   |

| Dimension                            | Quota / Parameter                  | Description                                                                                                          |
| ------------------------------------ | ---------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| **Max Archive / Member Size**        | `1 << 62` B (4 EiB)                | Reads and writes exceeding this limit are rejected                                                                   |
| **Max Payload Size**                 | `1 TiB` (`1 << 40` B)              | Managed by `.kuasar.presence.v1`, corresponding to a maximum bitmap size of `32 MiB`                                 |
| **Presence Page Size**               | `4096 B` (4 KiB)                   | `Hole` intervals must be 4 KiB-aligned (unaligned end permitted only at `payloadSize`)                               |
| **Max Dense Metadata Tail Size**     | `64 MiB` (`64 << 20` B)            | Range `[payloadSize, logicalSize)` must be completely dense (no Holes allowed)                                       |
| **Max Sparse Extent Count**          | `1 << 20` (1,048,576)              | Includes trailing `(logicalSize, 0)` sentinel and merged tail extent                                                 |
| **Index-Memory Accounting Baseline** | `2 MiB` + cumulative planning cost | Unbounded unless `WithIndexMemoryLimit` is set; charges `2 MiB` baseline plus cumulative planning allocations        |
| **Min Empty-Payload Carrier Size**   | Plain `5120 B` / Enc `5283 B`      | Empty payload still includes the 64 B presence header, payload mark, and `2048 B` marker/trailer blocks              |
| **Encrypted Record Size & Batch**    | Target plain `4096 B` (`+17 B`)    | Random read authenticates at least 1 record; adjacent record coalescing capped at `1 MiB`                            |
| **Explicit Zero Hashing Cost**       | Proportional to logical length     | Omitted full Zero pages save disk/network I/O, but synthesized zero bytes are still hashed into `payloadCommitment`  |

## 2. Multi-Manifest Bundle carrier

Local Manifest snapshots use a standard ZIP container with ZIP64 support and a **mandatory tail index**. The current profile requires `bundle/index` directly: no legacy-format probing, unindexed compatibility or Central Directory fallback.

```text
[0]     bundle/refs                                            # optional
[0|1]   bundle/admission/<generation>/<64-lowercase-hex-salt>
[...]   chunk/<64-lowercase-hex-content-key> |
        manifest/<64-lowercase-hex-content-key>
[last]  bundle/index                                           # required

        Central Directory
        ZIP64 EOCD + locator                                   # when required
        EOCD with an empty comment
```

Manifest and Chunk payloads are the unchanged bytes of their existing physical objects. Chunk local entries are appended in the ordinal of first logical appearance in the Manifest. Only index records are sorted; the writer neither reorders Chunk payloads by ContentKey nor buffers all of them. Shared objects deduplicate by ContentKey. Manifest keys, chunk keys and physical object bytes do not change.

Every local entry uses `zip.Store`, flags 0, no data descriptor and no local extra. Comments, timestamps, permissions and platform fields are fixed. Exactly one `bundle/index` must be the final local entry; its last 256 payload bytes immediately precede the real Central Directory.

A Bundle uses no ZIP Deflate, ZIP encryption, whole-file SHA/HMAC, outer encryption, custom pack format, root entry or JSON/YAML metadata. Old `admission/*` names, old unindexed Bundles, duplicate/unknown entries, directory entries, invalid keys/admissions and truncation are rejected by the appropriate profile/verification path. Ordinary open intentionally defers some object/container checks; it is not the strict full-container verifier ([§2.4](#24-explicit-strict-verification-and-exact-upload)).

### 2.1 `bundle/index` v1

The index payload is **Chunk section, Manifest section, fixed footer**, in that order. Each section is strictly increasing by ContentKey; its partition is implicit in its section. Duplicate or unsorted keys are invalid. Every record is exactly 48 bytes, explicitly little-endian:

| Byte range | Field | Encoding |
|---|---|---|
| `[0,32)` | ContentKey | Raw 32 bytes. |
| `[32,40)` | Payload DataOffset | `uint64`, absolute payload offset in the archive. |
| `[40,44)` | Payload Size | `uint32`. |
| `[44,48)` | ZIP CRC32 | `uint32`. |

DataOffset is **not** a Local Header offset. `DataOffset + Size` must use checked addition and lie entirely after the metadata prefix and before the `bundle/index` Local Header. Individual Manifest/Chunk sizes remain subject to their codec limits.

The ZIP is limited to **100,000 entries**. The two index sections together allow at most **99,998 object records**, and the index payload is bounded by **4,800,160 bytes**. Both limits apply; an optional refs entry also consumes a ZIP entry.

The footer is exactly 256 bytes. Integers are little-endian; magic and digests are raw bytes.

| Byte range | Field |
|---|---|
| `[0,16)` | Magic `KUASARBNDLINDEX1`. |
| `[16,18)` | Version, fixed at `1`. |
| `[18,20)` | Footer size, fixed at `256`. |
| `[20,22)` | Record size, fixed at `48`. |
| `[22,24)` | Reserved, all zero. |
| `[24,32)` | Absolute index-payload offset. |
| `[32,40)` | Total index-payload size. |
| `[40,48)` | Absolute `bundle/index` Local Header offset. |
| `[48,56)` | Metadata-prefix end offset. |
| `[56,64)` | Absolute Manifest-section offset. |
| `[64,72)` | Manifest record count. |
| `[72,80)` | Manifest-section size. |
| `[80,112)` | SHA-256 of exact encoded Manifest-section bytes. |
| `[112,120)` | Absolute Chunk-section offset. |
| `[120,128)` | Chunk record count. |
| `[128,136)` | Chunk-section size. |
| `[136,168)` | SHA-256 of exact encoded Chunk-section bytes. |
| `[168,252)` | Reserved, all zero. |
| `[252,256)` | CRC32C/Castagnoli of footer bytes `[0,252)`. |

Each section's size must equal exactly `count * 48`. The Chunk section starts at the index-payload beginning; the Manifest section follows immediately, followed immediately by the footer. They may neither overlap, leave gaps nor cover footer bytes.

If `directoryOffset` is the real Central Directory start declared by EOCD/ZIP64 EOCD, both equalities must hold:

```text
indexPayloadOffset + indexPayloadSize == directoryOffset
footerOffset + 256 == directoryOffset
```

Section SHA-256 and footer CRC32C detect index corruption; they are not signatures or authentication. Object-content protection follows the Manifest ContentKey, sealed key-table/decryption contract and `manifest.verify_content` policy. Chunk AES-CTR must not be described as authenticated encryption by itself.

The writer does not depend on `archive/zip.Writer`'s internal flush position. It maintains a logical `nextOffset` from canonical Local Header layout: `dataOffset = nextOffset + 30 + len(name)`. Finalize first checks the root Manifest, then sorts/encodes Chunk and Manifest records, writes the final `bundle/index`, and lets the standard ZIP writer emit Central Directory/ZIP64/EOCD. Strict verification and tests cross-check each record's offset, size and CRC against actual CD/LFH/data ranges.

### 2.2 Metadata prefix and Reader I/O

If present, `bundle/refs` must be nonempty and the first physical Local File Header. Admission follows immediately. Without refs, admission is first. The admission payload is empty.

Refs are canonical Bundle file references searched in file order, one per line:

```text
file://<basename>.bundle
file://<basename>.bundle@location:<name>
```

Refs prohibit `@manifest`, `@digest`, `@hmac`, `manifest://`, absolute paths, directory separators and filenames lacking the `.bundle` suffix. The payload must be valid UTF-8 using LF only, including a final LF. BOM, CR, empty lines, comments, surrounding whitespace and duplicate references are forbidden.

The writer preserves caller order without sorting. The limits are **1024 refs** and **1 MiB**. An empty list is represented by omitting the entry. `WriterOptions.Refs` is validated before `NewWriter` writes any byte. Refs and admission are immutable after construction; `Reader.Refs()` returns a copy.

A preflight needing only location/admission may use `OpenMetadata`/`ReadMetadata`. It validates only the contiguous Local Header metadata prefix, without reading the tail, index, Central Directory or objects. Successful prefix parsing proves neither that the Bundle is consumable nor that unindexed Bundles are supported. Actual Manifest/Chunk consumption requires `Open`/`NewReader`, which require a valid v1 index.

Ordinary `Open`/`NewReader` neither invokes `archive/zip.NewReader` nor reads Central Directory bytes, object Local Headers, the Chunk index or object payload. The remote ReaderAt opening sequence is fixed:

1. Read EOCD from EOF. For ZIP64, also read the locator and ZIP64 EOCD once each, obtaining the real directoryOffset without reading the CD bytes at that offset.
2. Read the footer at `directoryOffset - 256`; verify magic, version, checksum and all section bounds.
3. Read and verify the index entry's own canonical Local Header.
4. Read the metadata prefix contiguously using `metadataPrefixEnd`, then parse refs/admission in memory.
5. Read the complete Manifest section once, validate its digest, sort order, uniqueness, sizes and ranges, and build an O(1) read-only lookup map. Leave the Chunk section unread.

Thus the specified ZIP32 ReaderAt open uses **five fixed range reads**; ZIP64 uses **seven**. The count does not grow with Chunk count. Local file opening prefers read-only mmap. The ReaderAt object-payload fallback uses a bounded per-Reader buffer pool with 4 KiB–2 MiB size classes and at most **32 MiB retained**.

### 2.3 Source selection, Chunk preparation and ordinary restore

A Bundle can hold the root memory Manifest, current root/data-disk layer Manifests and, when needed, incorporated parent-layer Manifests. All objects share the complete `store.WriteAdmission` recorded in the admission entry. The caller obtains one admission before constructing `bundle.Writer`; subsequent Ingest calls read that fixed value. Shared chunks are written once per ContentKey.

Chunk encoding can run concurrently, but bounded ordinal reordering waits for logical order and appends ZIP bytes serially, avoiding an unbounded completion queue.

`Config.NewBundleIngester` connects the writer to the configured ingest path without extra salt for compatibility. `Config.NewBundleIngesterWithExtraSalt` accepts the same optional extra-salt resolver as Store ingest. The Bundle records the base admission, while each actual Chunk key is authenticated in the encrypted Manifest key table. Full verification and exact upload therefore decrypt with the authenticated key and do not attempt to derive it again from the base admission salt; readers never need the extra salt. Physical ContentKeys, the key-table AAD, decoded sizes, layout, closure, and the target admission are still verified before the root is published.

`manifest-ctl load --output-mode=bundle` is the executable producer for this
profile. Its positional inputs are layered top-to-bottom; a file reference of
the form `file://NAME.bundle@manifest:KEY[@location:NAME]` selects and proves a
root in that Bundle. `--ref-location NAME=file:///absolute/directory` keeps
host paths out of persistent references. Tail stripping/replacement is applied
to the selected logical stream, not to this outer ZIP carrier.

The read side chooses a source **only at `Fetcher.OpenManifest`**, in this order:

```text
current Bundle → bundle/refs[0] → bundle/refs[1] → ... → default remote Cache/Store
```

A caller-supplied `bundle.SourceResolver` resolves paths and locations. Accelerator consumes canonical references already validated by the Reader. A resolver may return `ErrSourceUnavailable` for a missing sibling, missing location mapping or missing located file. Source selection has not completed yet, so diagnostics may be retained while search continues. A present file with a corrupt profile/ZIP must return an ordinary error and fail closed. A referenced Bundle's own Refs are not recursively searched; the creator must flatten the search list.

| Manifest source | Getter for its Manifest and chunks | Failure behavior |
|---|---|---|
| Current Bundle or first refs Bundle containing the key. | That Bundle only. | Chunk-index, object-read, parse or decrypt errors fail directly; no further search. |
| All Bundles cleanly miss or are unavailable. | Remote cache/store only. | Coincidentally matching chunks in other Bundles are not used. |

There is no per-object fallback Getter mixing Bundles. A corrupt local Manifest does not cause a retry from another Bundle or Store. Current/refs source selection only queries the Manifest maps loaded at Open; a clean miss does not read a Chunk section.

Once a Bundle is selected, its Reader loads the entire Chunk section contiguously **before returning a Stream**, validates its digest/records/ranges and builds an O(1) immutable map. Preparation serializes concurrent loaders and caches only a successful validated index; after failure or cancellation, later calls can load it again. A remote source has no Bundle Reader and skips this preparation.

After preparation, Manifest/Chunk Get uses the record's payload DataOffset/Size for one target-payload range read, without rereading the index, object Local Header or Central Directory. The Stream's first data read therefore does not pay an O(Chunk-record-count) initialization cost.

An external root selector must use `OpenRootManifest`/`SelectRoot` to prove that the root physically exists in the **current Bundle**. It may not obtain the root indirectly from refs or Store.

Ordinary restore does **not** pre-scan the Manifest's complete chunk closure. A Manifest may name a missing chunk that the current workload never reads: Open/OpenManifest can succeed and present chunks remain readable. The missing chunk fails only when actually accessed, with no source fallback after selection.

`manifest.verify_content=false` does not implicitly trigger physical SHA scans. When enabled, the Manifest and Chunk physical hashes and key-table authentication are checked according to the normal read contract. Neither case makes ordinary lazy restore equivalent to a complete availability verification.

### 2.4 Explicit strict verification and exact upload

Single-Bundle full verify/upload and multi-source `VerifyExactManifests`/`UploadExactManifests` first run an explicit container verifier, regardless of `manifest.verify_content`.

It reads the complete Central Directory and, in its original order, checks every Local Header, name, method, flags, timestamp, attributes, extras, size, CRC and contiguous data range. This proves there are no reordered, hidden, overlapping or gapped entries. It requires the index to be the sole final entry and cross-checks every Manifest/Chunk record against CD/LFH/data ranges, rejecting unindexed objects, index references to nonexistent objects and extra metadata. Ordinary Open/Get does not perform these O(entry-count) checks.

Strict paths then verify the full closure and object content. A missing but unaccessed chunk fails FullVerify immediately; exact upload fails before any Put. Multi-source exact upload follows this order:

1. The caller selects the current/refs Bundle for each logical Manifest key at OpenManifest level. Dependencies already strictly verified in the destination Store do not enter the Bundle upload plan.
2. Before any admission or Put, run the complete CD/LFH/index container verifier on every actual source in that plan.
3. Before any Put, call `AdmitWriteFor(recorded.Generation)` for all actual source admissions. Returned Generation and Salt must match the recorded bytes exactly.
4. Force verification of every selected Manifest's physical ContentKey, parse it, unseal its table with the customer key, and prove its entire chunk closure resides in that same source Bundle. Before any Put, reject one ContentKey associated with inconsistent keys or plaintext sizes across Manifests.
5. Concurrent workers process every actually used unique chunk in each source: force physical ContentKey verification, decrypt/decompress with the authenticated key-table key, validate the decoded length, then Put that verified physical chunk. The actual keys already include any writer-side extra salt. This is per-chunk verify-then-upload, not a global plaintext-verification barrier.
6. Only after all chunk workers succeed, upload dependency Manifests and publish the caller-specified current root last. A later chunk verification or upload failure can leave earlier verified chunks in Store, but prevents publication of the root.

Each object uses its source Bundle's recorded admission. Upload neither redirects objects to the newest generation nor rechunks, recompresses, re-encrypts, reseals the table or rewrites upper-level `snapshot.cfg`. Root ManifestKey and physical bytes remain unchanged. Failure in admission preflight, dependency verification or Put prevents final root publication. Refs and admission entries are container metadata, not Store objects, and are not uploaded.

FullVerify also rejects chunks unreferenced by any local Manifest. After parsing snapshot-specific metadata, the caller can supply `ExpectedManifests` as the exact locally reachable Manifest set, rejecting unrelated Manifests without making accelerator interpret `snapshot.cfg`.

Read error classification, random-access failure and recovery are specified in [§4](#4-read-errors-random-access-and-recovery).

## 3. Shared suffix-ZIP handling

`pkg/tailzip` is the format-neutral implementation for a ZIP appended to logical image bytes. `Locate(io.ReaderAt, size, Options)` returns the payload boundary and suffix length, `Read` returns a bounded copy for extraction, `Prefix` exposes the unchanged sparse payload, and `Append` composes a validated suffix while retaining Hole, explicit Zero, and Data runs in the payload. These operations apply to logical image bytes (`logicalSize`), never to the physical archive end or an outer Manifest Bundle:

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

The default profile accepts ZIP trailers produced by existing image writers and limits both the suffix and its decoded entry total to **64 MiB**. Callers with a protocol schema can set entry-count, known-entry, order, and `STORED`-method requirements. Locating requires an EOCD which ends exactly at logical EOF, rejects multi-disk and unsupported ZIP64 suffixes, checks integer and central-directory bounds through `archive/zip`, and reads every entry to verify its CRC. Absence is distinguishable with `tailzip.ErrNotFound`; a recognizable malformed or truncated archive is an error rather than an absent tail.

`pkg/image.AppendConfigZip`, `ReadConfig`, and `ReadConfigFromFile` use this common mechanism. The writer retains the historical deterministic entry name, `STORED` method, timestamp and byte layout, while readers continue to accept the supported image ZIP profile. Both `flatten.Build` and `flatten.BuildFromDir` therefore produce their trailers through `pkg/tailzip` indirectly and all image config reads share the same payload-boundary implementation. Upper-layer artifacts structure their logical views as follows:

| Artifact Type                    | Logical Payload `[0, payloadSize)` | Logical Dense Tail `[payloadSize, logicalSize)`              | Consumer Package             |
| -------------------------------- | ---------------------------------- | ------------------------------------------------------------ | ---------------------------- |
| **Container Image (Image)**      | EROFS image data                   | `ZIP(config.json)`                                           | `accelerator/pkg/image`      |
| **Sandbox E (Sparse Disk Mode)** | Sparse root block device image     | `ZIP(sandbox.runtime.cfg)`                                   | `sandboxer/pkg/sandboxfile`  |
| **Sandbox E (EROFS Mode)**       | EROFS image data                   | `ZIP(config.json, sandbox.runtime.cfg)`                      | `sandboxer/pkg/sandboxfile`  |
| **Snapshot S (Memory Snapshot)** | Sparse VM memory space             | `ZIP(config.json, state.json, snapshot.cfg[, memory.dirty])` | `sandboxer/pkg/snapshotfile` |

> Note: `memory.dirty` in Snapshot S is a business-level dirty-page tracking bitmap and is completely independent of the carrier-level `.kuasar.presence.v1` bitmap.

### Shared readers, writers and strict profiles

`WithIndexMemoryLimit(bytes)` optionally bounds writer planning before the first output byte. It uses a 2 MiB accounting baseline and cumulatively charges planning allocations (including 512 bytes per disjoint data extent for slice growth, sparse-map encoding and copied metadata). Exceeding the limit fails before reading payload or writing an envelope. This accounts for the writer's indexes separately from source/native/decoder budgets; it does not bound the caller's source allocations. Omitted limits preserve existing behavior and artifact identity.

`tarstream.WriteTo` accepts `WithBeforeCommit(check)` for caller-owned final source validation. It calls the check once after consuming all payload and metadata-tail bytes, before the carrier digest marker, archive trailer and final encrypted record. Empty, all-Hole and all-Zero sources also run the check, without an extra EOF read. Earlier read/write errors skip it; a check error is preserved and leaves an incomplete artifact. Callers keep the source stable and open until return and must discard failed output rather than publish a file, upload or conversion-cache result. Receivers require both a complete valid artifact and successful producer completion. The check does not provide an atomic snapshot or destination durability. Unconfigured writers retain their existing bytes and identity; `CarrierDigest` rejects this option because it performs no payload consumption.

`pkg/manifest/transfer.Reader` opens the selected logical source and retains source ownership and verification. `transfer.Write` accepts a sparse source plus `WriteOptions`; its `BeforeCommit` hook completes caller-owned source/output validation before the Store root is emitted. `manifest.RefLocations` supplies the shared validated location mapping used by manifest-ctl and sandboxer.

`tailzip.ReadCanonical` and `EncodeCanonical` implement the fixed raw-header profile used by S/E (enforcing entry order, a fixed MS-DOS timestamp `1980-01-01 00:00:00`, and no extra fields/comments/data descriptors); `ReadFooter` and `Names` provide bounded geometry and role-detection metadata. Application modules supply ordered entry names, size limits and their config/schema validation. `tailzip.Section` preserves borrowed run lifetimes and unchanged prefix ChunkRun capabilities; `Append` preserves the authoritative payload boundary and compatible digest commitments.

The image reader/writer, both flatten builders, flatten-ctl packing, sandboxer S/E readers/builders and their image assembly/capture/restore callers share these helpers. Image ZIP bytes retain their existing writer profile; S/E keep their strict profile. New image tarstream envelopes declare the EROFS prefix as payload and the config ZIP as metadata tail. Existing carriers remain readable under their original identity declarations.

Suffix ZIP processing checks the EOCD entry count and bounded directory geometry before constructing the ZIP reader. Local offsets are relative to the suffix; absolute-prefix archives are rejected rather than treating payload as metadata. Full ReaderAt buffers accompanied by ordinary EOF are accepted. Short reads and marked source failures remain errors.

Canonical footer/name/body readers explicitly accept `io.ReaderAt` and a logical size. Their footer-first access requires genuine random access; monotone sparse sources use the sequential transfer path. Canonical encoding rejects ZIP64 entry counts before output. Complete bounded suffix reads retain backend failures even when the returned buffer is full.

## 4. Read errors, random access and recovery

Accelerator immutable readers perform one attempt and leave business retry/backoff to their consumer. `pkg/readerr.Mark(err, retryable)` preserves `Error()`/`Unwrap()` and adds only `Retryable() bool`; marking nil returns nil. Unknown errors are not confirmed permanent failures. Permanent semantic causes remain visible through wrappers and joined errors, and a marker at a semantic boundary is authoritative for its wrapped branch. Normal end-of-object EOF and confirmed absence retain their existing interfaces; required immutable lookup converts confirmed absence into a permanent cause. Validated format/range/integrity/authentication failures are marked where that meaning is known, while network EOF, partial frame/stream, backend-local cancellation and opaque access/decode errors retain their causes unless actual validation proves corruption.

A fixed-size `ReaderAt` request is one attempt. Short-nil results and real truncation of declared fields are structural failures, not permission to append a second response. Ordinary complete EOF remains valid where the source contract allows it; bare `io.ErrUnexpectedEOF` remains structural even with a full buffer. Explicit source failures are preserved before EOF compatibility handling, and the random-access metadata adapter must not let `io.ReadFull` hide a full buffer accompanied by a source failure. Sequential sources retain one-pass semantics rather than acquiring replay. `sparse.Dense` advances by bytes actually consumed while filling a request even when the read fails; rereading an earlier offset is rejected instead of returning following bytes for the failed range.

Parallel range reads join every worker before returning, including cancellation paths. The original failure is retained, derived sibling cancellation cannot replace it, and another worker's permanent cause remains in the aggregate. No worker may keep writing the caller's buffer after return. Existing Hole/Zero/Data streaming semantics stay unchanged.

Direct CLI immutable reads report their single-attempt error to the command owner. No retry setting, fallback source, health mask, service restart or compatibility mode is introduced by this contract. Tests cover partial frame/stream cleanup, initialization recovery, source isolation, joined permanent errors and worker lifetime; end-to-end retry policy remains consumer-owned.
