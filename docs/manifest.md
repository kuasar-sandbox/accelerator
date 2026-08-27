# manifest — 内容寻址存储的统一出入口

`manifest-ctl` 是数据进出"内容寻址存储"的统一入口。任何字节流(EROFS
镜像、内存快照、磁盘镜像、用户文件)经 manifest-ctl `store` 写入后,产出
一个**Manifest** —— 一份包含 chunk 元数据 + 加密的密钥表的小型二进制文
件。Manifest 是后续读取(`load`)与跨节点引用(`manifest://<key>`)的
唯一句柄。

## 1. 概述

### 1.1 模块定位

```
   ┌─ input source ──┐      ┌─ manifest-ctl store ──────────────────┐      ┌─ output ──────────┐
   │  sparse.Source  │ ───► │  chunker (data segments)              │ ───► │  Manifest         │
   │  (file/stdin/   │      │      ↓                                │      │  (binary file or  │
   │   tar stream)   │      │  Snappy candidate → RAW/Snappy        │      │   stdout)         │
   └─────────────────┘      │      ↓ encrypt                        │      └───────────────────┘
                            │  store-ctl Put(chunk_hash) ───────────┼───►  chunk bytes land in
                            └───────────────────────────────────────┘      store-ctl backend
```

读取方向相反:`manifest-ctl load` 从 Manifest 取 chunk 列表 → 经 cache-ctl
(若配置)穿到 store-ctl `Get` → 解密 → 输出由 `crypto.local` 决定编码的
tarstream 工件。

### 1.2 设计原则

- **写入路径**:`sparse.Source → chunker → Snappy/RAW 选择 → encrypt → store.Put → Manifest`
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
`file://<path>[@sha256:<digest>|@hmac:<digest>|@manifest:<key>][@location:<name>]`;
三个 identity qualifier 互斥。`@manifest` 选择 Manifest Bundle 中的根 Manifest，
tarstream opener 必须拒绝它；反之 Bundle opener 必须拒绝 `@sha256/@hmac`。带
location 时 path 必须是 basename,location 匹配
`[A-Za-z0-9][A-Za-z0-9._-]*`,只保存逻辑名称,不携带宿主目录。允许数字开头以直接
容纳 UUIDv7 sandbox/build ID。

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
compression:  raw=512 snappy=19968 logical=10.0 GiB encoded=2.1 GiB saved=7.9 GiB
manifest:     encoding=raw logical=1.8 MiB stored=1.8 MiB
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
  verify_content: true             # 普通读取是否复验 physical Manifest/Chunk SHA-256;缺省 true
  # write_generation: G3           # 可选;指定写入 generation
store:
  endpoint: 127.0.0.1:7100        # 远端读写必填;离线 Bundle 可省略:host:port 或 Unix socket
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
- `manifest.verify_content` — 缺省 `true`。普通 Bundle/cache/store Fetcher、
  `GetManifestBlob` 和 `CheckManifest` 都用同一策略：`true` 时在解析 Manifest
  前复验 `SHA256(physical Manifest) == requested key`，首次读取 Chunk 时复验
  `SHA256(physical Chunk) == CiphertextHash`；`false` 时 ContentKey 只作 locator，
  不执行这两次完整对象扫描。关闭后进程只记录一次清晰 WARNING；Manifest
  envelope/geometry、key table GCM、Chunk format/长度、解密/解压等结构检查仍然
  执行。`manifest-ctl verify`、Bundle full verify/upload/export 和对象修复始终
  强制开启 SHA 校验，不受该字段影响。
- `manifest.write_generation` — 可选写入 generation。有 `store.endpoint` 时，
  留空调用 `AdmitWrite()` 选择当前最新项，非空调用
  `AdmitWriteFor(generation)`，generation 已移除则失败；Store RPC 失败不会降级为
  本地派生。无 Store 的离线 Bundle writer 在字段留空时使用普通 generation 名
  `NONE`，显式配置时使用该值，两者都调用公共
  `store.SaltForGeneration` 派生 salt。`NONE` 不是保留字或协议特殊值。
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

AES-CTR 的 IV 取全零。key 始终由**原始 plaintext**派生,而固定 canonical
encoder 又保证同一 plaintext 只对应一个 RAW 或 Snappy payload,因此同一 `(key,
IV=0)` 不会加密两个不同 payload。由 `salt + plaintext` 决定 chunk 的密钥,
同 salt 域内同 plaintext 必然产出同 physical object;同 object → 同
ContentKey(= `SHA256(object)`)→ store 上同一份字节。

这个不变量要求 Go Snappy 版本、`snappy.Encode`、收益门槛和 format byte 固定。当前
项目尚未发布,本格式直接替换旧开发格式且 reader 不兼容旧对象。首次使用前必须
清空旧 salt domain 或换 generation salt,并禁止旧/new writer 混写。以后若改变
encoder 输出或门槛,必须更换 salt/key domain,不能只换 format byte。

canonical encoder 固定为 `github.com/golang/snappy` v1.0.0 的 block API。相同
输入的 encoded bytes 由 amd64、amd64 `noasm` 与 arm64 golden 共同约束；这项
逐字节一致性优先于更高压缩比。`klauspost/compress/s2` 的标准及 Snappy-compatible
encoder 会因架构实现产生不同 block bytes，因此不用于 canonical 写路径。

#### Content key

Chunk 上传时,`SHA256(physical_object)` 是 store 的寻址键;Manifest 上传时同样
按 `SHA256(physical_envelope)`。Salt **不参与寻址** —— 寻址完全由物理字节哈希决定,
保留 salt 的隔离性,但允许 store 端以单一 KV 视图存储(详见 [`store.md`](store.md))。

### 4.3 加密模式 — chunk

YAML `crypto.chunk` 仅支持 `aes`;任何其它值在构造时即被拒绝(无明文回退)。
每个非零 chunk 固定执行 Go Snappy block `snappy.Encode`。仅当同时满足以下 canonical
门槛才选 Snappy,否则选 RAW；这不是配置项：

```text
saved = rawSize - encodedSize
Snappy iff saved >= 4 KiB AND encodedSize * 4 <= rawSize * 3
```

物理格式：

| format byte | payload | physical object |
|---|---|---|
| `0x01` (`AESRaw`) | 原始 plaintext | `[0x01] || AES-256-CTR(key, IV=0, plaintext)` |
| `0x02` (`AESSnappy`) | `snappy.Encode(plaintext)` | `[0x02] || AES-256-CTR(key, IV=0, snappy_block)` |

chunk key 是收敛密钥 `SHA256(salt‖plaintext)`,由加密层内部派生(调用方只
传 salt、不传 key),故 (key, IV=0) 对不同明文不复用、CTR keystream 不重用。
format byte 未加密但被 physical ContentKey 覆盖。Manifest entry 仍只记录原始
plaintext size 与完整 physical hash,不增加 codec 字段。unknown format、长度不
匹配、损坏、截断或不满足固定收益门槛的 Snappy object 一律失败,没有兼容探测或
fallback。

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

- `server_salt` — 每次 ingest 调一次 `store-ctl AdmitWrite()`，取得 generation
  与 opaque salt；全部 chunk 和最终 manifest Put 复用该 admission。切换写入
  generation 后，新 ingest 使用新的 salt，已开始的 ingest 不会跨代。
- `extra_salt` — `--extra-salt <bytes>` flag(§2.3),叠加到上面。

最终:

```
final_salt = server_salt                                                       # extra_salt 为空
final_salt = SHA256("accelerator-extra-salt-v1" || server_salt || extra_salt)  # extra_salt 非空
```

`extra_salt` 用于在同一 store salt 域内做更细粒度隔离(例如多租户)。

### 4.6 Manifest 二进制格式

Manifest 的当前 Version1 physical envelope 是：

```text
[4 bytes "MANI"][1 byte encoding][payload]

encoding 0x00 (RAW): payload = logical manifest bytes after magic
encoding 0x01 (Snappy):  payload = snappy.Encode(logical manifest bytes after magic)
```

RAW/Snappy 使用与 Chunk 相同的固定 4 KiB + 75% 收益门槛。reader 不接受旧的
`[MANI][Version1]...` 表示。Snappy decode 前先调用 `snappy.DecodedLen`,完整 logical
manifest（含 magic）硬限制为 64 MiB；encoded length、table offset/length 和所有
整数加法均先做边界检查，并拒绝不满足固定收益门槛的 Snappy envelope。RAW 直接在
envelope payload 上解析,不为重建旧布局复制
完整 Manifest；Snappy 只保留一个有界 decoded buffer。

envelope 内的 logical Manifest 是一段紧凑二进制(整数 little-endian；magic 本身
是字节串 `MANI`)：

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

按 key 获取时先验证 `SHA256(physical envelope) == requested ManifestKey`,再解析
envelope。随后解析 header → 二分查找 chunk index 定位 offset → 用 `manifest.key`
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
   │  → key = SHA256(salt || original plain)
   ▼
Go Snappy candidate + fixed benefit rule
   │  → RAW or Snappy payload
   ▼
AES-CTR + format byte
   │  → physical object + SHA-256
   ▼
returned physical hash       ← ContentKey (ingest does not hash it again)
   │
   ▼
store-ctl Put(admission, partition=chunk, key=ContentKey, size, ciphertext)
	│  server checks Exists first → dedup hit skips upload (SendAndClose)
	│  else stream-write directly to chunk/{generation}/aa/bb/<hash>
   ▼
manifest.append(chunk_meta, key)   ← accumulate index + key table
   │
   ▼
seal(manifest.key, key_table)
   ↓
logical Manifest → RAW/Snappy physical envelope
   │
   ▼
store-ctl Put(same admission, partition=manifest, size, manifest)
```

上图按单个 chunk 画顺序流,但 derive/encrypt/`Put` 那一段是**并发**执行的:
ingest 用一个有界 worker pool,并发度取 store 客户端连接池大小(`store.pool`
——round-robin RPC 调度下真正能同时在途的 `Put` 数就是它)。chunk 切分仍按
文件顺序、manifest 索引按文件顺序组装、进度回调串行化,所以产物字节序不变;
后端不暴露连接池信息时回退为串行。这把入库吞吐从"串行单 `Put` 往返"提升到
"池并发往返",对多 GiB snapshot `--upload` 影响显著(实测见
`kuasar-sandbox/docs/perf.md` §2.4)。

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
   │     └─ on-demand Getter → cache/store → verify → decrypt → bounded chunk cache
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

每个 manifest Stream 内部维护独立的解密 chunk cache,仅服务 on-demand 部分读。
cache key 包含 `CiphertextHash`、chunk decrypt key 和 plaintext size,因此同一
Stream 内重复物理内容可以复用,不同密钥或逻辑大小不会错误别名。cache 同时受
`32 entries` 和 `32 MiB` 两个硬预算约束,按 LRU 淘汰;entry 自最后一次命中起空闲
`5s` 后过期。每个 Stream 只保留一个指向最早到期 entry 的 timer,即使后续没有
读请求也会主动回收陈旧数据,不为每个 entry 创建 timer 或常驻扫描 goroutine。

同一 cache key 的并发 miss 合并为一次 Get、SHA-256 校验、解密和可选 Snappy
decode,不同 key 仍可并行加载。加载失败不进入 cache;等待者如果自身 context 仍有效会重新尝试,不会
永久继承首个加载者的取消。partial miss 只分配一个 plaintext-sized cache buffer,
在 immutable Blob 仍存活时直接写入该 buffer；不复制或修改 ciphertext。淘汰、TTL
到期和 `Close()` 都会清零明文;正在复制
的 entry 先从 LRU 移除,待最后一个 reader 释放后再清零。

冷态完整物理 chunk 读取保持直通路径,避免一次性顺序扫描污染 cache;已有 cache
命中仍可服务完整读取。单个 plaintext chunk 超过 `32 MiB` 时同样直通且不准入。
RAW 整块由 AES 直接写 caller buffer,没有 plaintext allocation 或额外整块 copy；
RAW 超大 partial 按 CTR block/offset 直接解密请求范围。Snappy 整块把 encrypted payload
解到有界 encoded scratch,校验 `DecodedLen == entry.Size`,再直接 decode 到 caller
buffer；Snappy 超大 partial 因 block format 限制必须完整 decode 到有界临时 plaintext
后复制范围。

Snappy encoded scratch 使用显式 size class free-list,不是无峰值保证的 `sync.Pool`。
encode/decode 分别有独立的 `max(1, GOMAXPROCS)` CPU slot、192 MiB active byte budget
和 64 MiB retained budget（每方向合计最多 256 MiB codec-owned scratch）；常用最大
retained buffer 为 2 MiB,异常大 buffer 用后丢弃。slot 和 weighted byte admission
都响应调用方 context；RAW 读取不获取 decode slot。没有后台 goroutine。

`Prefetch` 仍只执行完整物理 chunk 的 cache Get 并立即 Release,不校验、解密或
填充上述明文 cache。因此 Prefetch 与 on-demand ReadAt 可以各自产生 Get,而重复
的 on-demand 部分读由 Stream 内部合并和复用。

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

### 4.10 多 Manifest ZIP Bundle

本地 Manifest 快照使用带强制尾部索引的标准 ZIP64。当前 profile 直接要求
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
非法 key/admission 与截断均 fail closed。

#### 4.10.1 `bundle/index` v1

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
合计最多 99,998 records，index payload 最多 4,800,160 bytes。

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
由 Manifest ContentKey、Chunk decrypt/authentication 及 `manifest.verify_content`
合同承担。

Writer 不依赖 `archive/zip.Writer` 的内部 flush 位置，而是按 canonical Local Header
布局维护逻辑 `nextOffset`：`dataOffset = nextOffset + 30 + len(name)`。Finalize 先检查
root Manifest，再分别排序并编码 Chunk/Manifest records，写出最后的 `bundle/index`，
最后才由标准 ZIP writer 写 Central Directory/ZIP64/EOCD。严格 verifier 和测试会把
每个 record 的 offset、size、CRC 与实际 CD/LFH/data range 交叉核对。

#### 4.10.2 metadata prefix 与 Reader I/O

`bundle/refs` 存在时必须非空并作为第一个物理 Local File Header；admission 必须紧随
其后，否则 admission 必须是第一个 entry。admission payload 为空。`bundle/refs`
一行一个 canonical、按文件顺序搜索的 Bundle file ref：

```text
file://<basename>.bundle
file://<basename>.bundle@location:<name>
```

它禁止 `@manifest/@sha256/@hmac`、`manifest://`、绝对路径、目录分隔符和非
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

#### 4.10.3 source selection、Chunk preparation 与普通 restore

一个 Bundle 从首版容纳根内存 Manifest、根/数据盘当前层 Manifest，以及必要时
收编的父层 Manifest。全部对象共用 admission entry 中的同一完整
`store.WriteAdmission`。`bundle.Writer` 在构造前取得一次 admission；后续多个
`Ingest` 只读取这份固定值。共享 Chunk 按 ContentKey 只写一份。Chunk 编码仍可
并行，但实现通过有界 ordinal reorder 等待逻辑顺序并串行 append ZIP，不产生无界
完成队列。

配置层用 `Config.NewBundleIngester` 装配该 writer；此入口固定不混入
`extra_salt`，保证 Chunk key 始终属于 recorded admission 的 canonical salt domain，
从而可在 exact upload 时逐对象验证且无需重写。

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
读取完整 Chunk section，验证 digest/records/ranges 并建立 O(1) 只读 map。并发准备由
`sync.Once` 合并为一次读取，错误或取消也由所有调用方共享。remote source 没有 Bundle
Reader，因此不执行这一步。

准备完成后的 Manifest/Chunk `Get` 直接使用 record 中的 payload `DataOffset/Size`，只
做一次目标 payload range read；不会读取索引、对象 Local Header 或 Central Directory。
实际 Stream 的第一次 `Read` 因而不承担 O(Chunk records) 的索引初始化。外部
root selector 必须通过 `OpenRootManifest`/`SelectRoot` 证明 root 物理存在于 current
Bundle，不能从 refs 或 Store 间接取得。

普通 restore 不再预扫描 Manifest 的完整 Chunk closure。Manifest 声明一个缺失但本次
不会读取的 Chunk 时，`Open` 和 `OpenManifest` 可以成功，已存在 Chunk 仍可读取；只有
真正访问缺失 Chunk 时才失败，而且选定 source 后不得 fallback 到后续 Bundle 或 remote
Store。`manifest.verify_content=false` 仍不会隐式执行 physical SHA 扫描；为 true 时的
Manifest SHA、Chunk ContentKey/decrypt authentication 语义保持不变。

#### 4.10.4 显式严格验证与 exact upload

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
5. 每个 source 中实际使用的唯一 Chunk 强制验证 physical ContentKey，解密/解压为
   原明文，并要求 `DeriveKey(sourceAdmission.Salt, plaintext)` 等于 key table 中的
   key；跨 Manifest 的同一 ContentKey 对应不同 key 或 plaintext size 时拒绝。
6. 先并发上传全部 Chunk，再上传依赖 Manifest，最后发布调用方指定的 current root。

每个对象使用其 source Bundle 的 recorded admission，不改投最新 generation，不
重新 chunk、压缩、加密、seal key table 或改写上层 `snapshot.cfg`，因此 root
ManifestKey 与 physical bytes 保持不变。任一 admission 预检、依赖验证或 Put 失败
时根不会发布。`bundle/refs` 和 `bundle/admission/*` 不是 Store object，不上传。
`FullVerify` 还拒绝未被任何本地 Manifest 引用的 Chunk；调用方解析
snapshot-specific metadata 后可通过 `ExpectedManifests` 提交精确的本地可达
Manifest 集，从而拒绝无关 Manifest，而无需让 accelerator 解释
`snapshot.cfg`。

## 5. 性能特征

测量入口:`kuasar-sandbox/docs/perf.md` §2.1–2.2(冷/热 L1 状态下
manifest:// 加载端到端时长)。

主要决定项:

- chunk 模式(cdc 命中率高于 fixed,但分块本身略慢);
- 固定 RAW/Snappy 选择与 AES（不可压缩内容保持 RAW；可压缩内容减少 hash/AES/wire bytes）；
- store 端点延迟(本地 fs vs 远端 S3-compatible object storage,详见 [`store.md`](store.md))。

可重复 microbenchmark 为 `BenchmarkCompressionCandidates`（raw、Go Snappy、S2、S2
Better、Zstd SpeedFastest 控制组）、`BenchmarkAESChunkCanonicalCodec` 和
`BenchmarkManifestAESPhysicalRead`。生产实现只使用 Go Snappy；benchmark 候选不会
进入配置或协商面。codec benchmark 另报告 `scratch-misses/op`,用于确认 warm
size class 没有每次重新分配 payload-sized buffer。

Bundle 尾索引的 4K/20K/100K Chunk 打开成本由 `BenchmarkBundleOpen4K`、
`BenchmarkBundleOpen20K` 和 `BenchmarkBundleOpen100K` 报告；Open 只加载
Manifest index，不读取 Chunk index、Central Directory、对象 Local Header 或对象
payload。`BenchmarkBundlePrepareChunks4K/20K` 报告选源后的单次 Chunk section
准备，`BenchmarkBundleGetAfterPrepare` 报告纯 O(1) lookup + payload read，
`BenchmarkBundleOpenHighRTT20K` 注入固定 ReaderAt RTT 并报告 `read-calls/op` 与
`read-bytes/op`。`BenchmarkVerifyContent` 分别报告 Manifest open 与冷态
1 MiB Chunk read 在 `verify_content=true|false` 下的差异。
`BenchmarkManifestSourceSearch` 报告 refs 为 0/1/8/32 时的 clean miss 与尾部 remote
fallback 成本；`BenchmarkMetadataOpen4K` / `BenchmarkMetadataOpen20K` 证明 prefix-only
metadata 读取不随 Chunk 数增长。

## 6. See Also

- [`store.md`](store.md) — manifest-ctl 通过 gRPC 把字节落到 store-ctl
- [`cache.md`](cache.md) — `load` 路径可选穿 cache-ctl 加速;cache-ctl 自身
  以 manifest 同款客户端从 store 取 chunk
- `guest-runtime/docs/flatten.md` — 镜像展平后经 `flatten-ctl export --upload`
  入库
- `sandboxer/docs/sandbox.md` — 沙箱通过 `manifest://<key>` 引用磁盘
  base 与快照
- `kuasar-sandbox/docs/kuasar-sandbox.md` §4.1–4.4 — Manifest 抽象在系统中的位置
