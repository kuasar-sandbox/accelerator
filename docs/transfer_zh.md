[English](transfer.md) | [简体中文](transfer_zh.md)

# 数据传输、检查与转换

`manifest-ctl` 与共享 transfer API 在 tarstream 文件、Manifest Bundle 和 Store 之间传输选定的逻辑内容。先完成本地完整往返，再查阅命令和配置。[数据契约](artifacts_zh.md) 定义引用、稀疏数据、格式和加密；本篇定义调用方式、变换、输出与调用者责任。

## 独立 Store 往返

在加入 VM 或 cache 前先验证这一流程。前提为 Linux、Bash、Python 3，以及同一
aggregate 所选的 `store-ctl`、`manifest-ctl` 和 guest-runtime 的 `flatten-ctl`。
其[纯 Go tar stream/extract 接口](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli_zh.md)
不需要 mkfs.erofs、KVM 或 root。先用所选二进制 help 确认子命令；缺少此接口的
旧版不适用。使用配套版本，不要分别下载各组件的 latest。

以下临时 FS Store 使用私有 UDS 和临时客户密钥，不需要 L2、MicroVM、registry、
公共 listener 或外部数据。[Store generation 初始化](store_zh.md) 和上文 [配置](#配置)
仍是权威契约。请在新 shell 执行；退出时总会停止本次子进程，仅成功时删除
临时目录。失败时保留私有目录并打印路径供诊断。

```bash
set -euo pipefail
umask 077
LAB=$(mktemp -d)
STORE_PID=
SUCCESS=0
cleanup() {
  result=$?
  trap - EXIT
  if [ -n "$STORE_PID" ]; then kill "$STORE_PID" 2>/dev/null || true; wait "$STORE_PID" 2>/dev/null || true; fi
  if [ "$result" -eq 0 ] && [ "$SUCCESS" -eq 1 ]; then
    rm -rf -- "$LAB"
  else
    printf 'Retained private diagnostics: %s (store.log)\n' "$LAB" >&2
  fi
  exit "$result"
}
trap cleanup EXIT
cat > "$LAB/store.yaml" <<EOF
listen: unix://$LAB/store.sock
backend: fs
fs:
  root: $LAB/objects
  verify_content_key: true
generations:
  file:
    path: $LAB/generations
EOF
store-ctl init --config "$LAB/store.yaml" --generation G1
store-ctl serve --config "$LAB/store.yaml" >"$LAB/store.log" 2>&1 &
STORE_PID=$!
for i in $(seq 1 50); do
  test -S "$LAB/store.sock" && break
  kill -0 "$STORE_PID"
  sleep 0.1
done
test -S "$LAB/store.sock"
export MANIFEST_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
cat > "$LAB/manifest.yaml" <<EOF
store:
  endpoint: unix://$LAB/store.sock
  timeout: 5s
manifest:
  verify_content: true
chunker:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
  local: off
EOF
printf 'Kuasar independent storage roundtrip\n' > "$LAB/payload"
flatten-ctl tar stream -f "$LAB/input.tar" "image:$LAB/payload"
KEY=$(manifest-ctl store --manifest-config "$LAB/manifest.yaml" --no-progress "$LAB/input.tar")
manifest-ctl verify --manifest-config "$LAB/manifest.yaml" --no-progress "$KEY"
manifest-ctl load --manifest-config "$LAB/manifest.yaml" --output "$LAB/output.tar" "$KEY"
flatten-ctl tar extract -f "$LAB/output.tar" "image:$LAB/restored"
cmp "$LAB/payload" "$LAB/restored"
printf 'PASS: payload bytes match\n'
unset MANIFEST_KEY
SUCCESS=1
```

预期获得完整 Manifest key、verify 零失败 chunk 和最终 PASS 行。比较解包后的
逻辑字节，load 后 tar 包装/名称可能不同。普通文件或通用 tar 不能替代平台的
稀疏/digest envelope。失败后按 stderr 打印路径检查保留目录的 `store.log`、
generation 初始化、socket 就绪与密钥；不要把结果未知的写操作当作读取失败重试。本流程
只验证本地字节往返，不验证稀疏 VM 状态、cache 层级、S3 或恢复。诊断结束后，
显式删除本次打印的私有测试目录。若需长期保留数据，应保留密钥、配置和 ref，
不使用此处测试清理；生成的密钥仅存在于本 shell 环境。

## 选择来源与输出

| 操作 | 输入 | 结果 |
| --- | --- | --- |
| `store` | 一个 tarstream 路径、带身份/location 的 file ref、选定 Bundle 根，或 Manifest key/ref；省略输入或 `-` 表示 tarstream stdin。 | Store 对象与根 Manifest key。 |
| `load` | 一个或多个逻辑 ref，最上层在前；stdin 必须是唯一来源且符合 tarstream。 | 按参数产生 tarstream/Bundle、Store 输出、逻辑窗口或提取的元数据。 |
| `info`、`verify`、`get-manifest`、`diff` | 分别遵守下文元数据/key/文件输入契约。 | 检查、验证或去重率证据。 |

任意 raw 文件不等于 tarstream source，文件名不能决定载体格式。多 Manifest Bundle 需要选根；即使关闭可选内容验证，显式身份仍然具有约束力。

## 命令行接口

### 公共 flag

```text
需要配置的子命令共用:
  --manifest-config string    Manifest YAML 路径,覆盖 MANIFEST_CONFIG
```

需要配置的命令必须由 `--manifest-config` 或 `MANIFEST_CONFIG` 指向有效 YAML。
**例外**:`config generate`、读取本地文件/stdin 的 `info`、本地文件 `diff`
不需要配置文件。schema 见 [manifest-config.yaml](#manifest-configyaml)。该 flag 放在子命令后,不放在子命令前。

### 子命令一览

| 命令 | 功能 |
|---|---|
| `store`         | 写入数据 → 自动上传 manifest 并返回 hex content key |
| `load` | 读取并变换一个或多个逻辑来源，按参数输出 tarstream、Bundle 和/或 Store |
| `get-manifest`  | 按 hex key 取回 Manifest 原始字节(debug 用) |
| `info`          | 打印 Manifest 元数据,不解封密钥表 |
| `verify`        | 经 fetch 验证 Manifest 及所有非零 chunk 的内容与可用性 |
| `diff`          | 比较两个本地 Manifest 的 chunk 重叠度(去重率分析) |
| `config show`   | 打印解析后的配置 YAML |
| `config generate` | 输出带注释的配置模板 |

输入和 Manifest key 均为**位置参数**:`store` 数据源、`info` 来源省略或 `-`
表示 stdin。`get-manifest` / `verify` 接受完整 64 字符小写 hex key 或
`manifest://<key>`。**远端 `info` 必须带 `manifest://` 前缀**;裸 hex 在 `info`
中被当成本地文件名。flags 放在位置参数前,因为 Go stdlib flag 在首个非 flag 处停止解析。

`load` 接受其命令小节定义的逻辑引用；完整的[规范引用语法](artifacts_zh.md#引用语法与命名位置)由数据契约统一定义。

### `manifest-ctl store` — 数据写入

```
manifest-ctl store [flags] <REF|->        # <path|-> 省略或 - = stdin

Flags:
  --extra-salt string         额外 salt 字节,叠加到 store 提供的 opaque salt
  --no-progress               禁用进度输出
```

输入是一个已选定的逻辑 source：本地 tarstream 路径、带身份或 location 的 file ref、Bundle selector，或者 Manifest key/ref。多 Manifest Bundle 必须指定 `@manifest:<key>`。文件提供有界随机读取视图，stdin 使用 tarstream 的单遍 `SourceFrom` 协议；两者共用 `pkg/manifest/transfer` 的 reader/writer。空洞来自 carrier 的权威元数据。`crypto.local=off|auto|required` 沿用已有本地 tarstream 加密策略。

Store 同样接受 `--ref-location`、`--strip-tail`、`--extract-tail PATH` 和 `--append-tail PATH`，语义与下述 load 一致。发布 Store 根 Manifest 前，完成源完整性、原始顺序输入的 marker/trailer/EOF 和所请求输出文件的收尾检查。

stdout 输出一行 64 字符 hex,即已上传 Manifest 的 content key。stderr 输出动态进度
与最终人类可读摘要。**`--no-progress` 只关闭动态进度,最终摘要仍输出**。以下为示意:

```
image size:   10.0 GiB
stored bytes: 1.0 GiB
chunks:       stored=2048 dedup=18432 zero=0
compression:  raw=512 snappy=19968 logical=10.0 GiB encoded=2.1 GiB saved=7.9 GiB
manifest:     encoding=raw logical=1.8 MiB stored=1.8 MiB
manifest key: a1b2c3d4...
```

以下示例假设已提供有效配置与 customer key。`disk.img` 和 `snap.bin` 虽然使用这些
后缀,但必须已经是 tarstream 工件;文中的缩略 key 均需换成完整 key。

```bash
# 工件文件 → manifest key
MKEY=$(manifest-ctl store disk.img)

# 工件 stdin → manifest key
cat disk.img | manifest-ctl store > disk.key

# docker archive → 展平 → 入库;只上传时 stdout 为 Manifest key
docker save myapp:v1 | flatten-ctl export --upload > app.key

# 额外 salt(隔离 dedup 域;flags 在位置参数前)
manifest-ctl store --extra-salt "tenant-xyz" snap.bin

# 切换分块 / 加密模式 → 改 YAML
```

### `manifest-ctl load` — 数据读取

```
manifest-ctl load [flags] REF...

Args:
  REF...                       自上而下的层：bare key、manifest://、本地路径、
                               file:// tarstream 或 Bundle @manifest 引用
Flags:
  --output string             输出路径 (default "-", stdout;终端拒写)
  --output-mode string        none|tarstream|bundle（默认 tarstream；带
                              --store 时默认 none）
  --store                     写入最终变换后的逻辑 source
  --extra-salt string         Store/Bundle ingest 的域隔离
  --ref-location N=file:///P  显式映射 location（可重复，P 必须绝对路径）
  --strip-tail                layering 前从每个 source 去掉 suffix ZIP
  --extract-tail PATH         保存第一个输入的原始 suffix ZIP
  --append-tail PATH          最后追加已验证 suffix；原有 tail 时要求 strip
  --name string               产物 tar 条目名 (default "image")
  --offset uint               窗口起始偏移
  --length uint               窗口长度 (0 = 余下全部)
  --no-progress               禁用进度输出
```

REF 按参数顺序通过 `fetch.NewLayered` 组合，第一个为顶层。Data 和显式零值覆盖低层，Hole 向下透传。普通本地路径可直接输入；64 字符十六进制 key 保持 Manifest 语义。同名本地文件可用 `./<name>` 指定。文件身份 qualifier 会被验证，named location 共用 `manifest.RefLocations` 解析。

固定顺序为：检查/提取原始 top tail；按需分别 strip 每个 source；分层读取；offset/length 窗口；append 新 tail。裸 source 可直接使用 `--append-tail`，原有 tail 时必须同时给出 `--strip-tail`；可识别的损坏 tail 报错。`--output-mode=none --extract-tail PATH` 仅提取有界原始元数据。tail 操作作用于逻辑内容而非外层 Bundle ZIP。保留的旧/新 tail 共用 64 MiB 预算，解码 ZIP 元数据另受限额约束。

`--store` 写入最终变换后的 source，缺省文件输出模式为 `none`，并可与两种文件输出组合。Store+Bundle 取得一次 admission，通过同一 Chunk/Manifest 编码流和有界背压写入两端；同配置得到相同物理对象及根 key。`--extra-salt` 同时适用于 Bundle-only 和 Store。Store+tarstream 通过有界管道接入既有 ingest 协议，也支持 stdin 输入。

使用命名输出文件时 Store key 输出到 stdout；二进制内容使用 stdout 时，标识和诊断输出到 stderr。Bundle root key 输出到 stderr。请求的文件以 exclusive create 直接写入；失败仅清理本次仍拥有的不完整文件。既有文件、输入输出 alias、输出路径冲突在写入前拒绝。成功结果包含源校验、文件关闭及 Store 根发布。后续目标失败可能留下已经独立完成的文件或已写入 Chunk，不影响旧源。

Tarstream 输出保留稀疏元数据。`crypto.local=off` 输出 plaintext；`auto|required` 输出 encrypted v1。不可寻址 stdin 使用 tarstream，Bundle 随机读取通过文件/ref 提供。上述 transfer 路径不生成完整镜像缓存或 staging 文件。

新 tarstream 输出统一使用 [presence v1](artifacts_zh.md#页-presence-与-zero-传输)：可选且已声明的 metadata tail 之前的最终 payload 上限为 1 TiB；支持空 payload 和任意字节长度的 dense 窗口，不填充数据。每个 Hole 必须从页边界开始，在页边界或精确的 payload 末端结束。`--offset`/`--length` 窗口或旧输入 carrier 含有无法表达的 Hole 边界时，在写出 tarstream 字节前拒绝，不改变边界，也不改写成旧格式。既有旧 tarstream 仍可读取。这些 tarstream 输出约束不会限制仅写 Store 或 Bundle 的字节粒度输出。

```bash
# 按 top → bottom 读取、合并层
manifest-ctl load --output merged.tar A B C

# 同一次操作生成相同的 Bundle 和 Store 对象
manifest-ctl load --store --output-mode=bundle --extra-salt tenant-a \
  --output merged.bundle A B C

# 合并 payload 后替换元数据尾部
manifest-ctl load --strip-tail --append-tail new-tail.zip \
  --output-mode=bundle --output merged.bundle A B C

# 只提取元数据，不输出 payload
manifest-ctl load --output-mode=none --extract-tail metadata.zip SOURCE

# stdin 同时写入 Store 和 tarstream 文件
cat source.tar | manifest-ctl load --store --output-mode=tarstream \
  --output copied.tar -
```

#### 传输身份与边界重建

显式 tarstream `@digest`/`@hmac` 身份和 Bundle `@manifest` selector 在关闭普通 `manifest.verify_content` 时仍然生效。内容转换完成载体校验后才报告成功。载体按字节格式或显式 selector 识别，与文件扩展名无关。载体声明 tail 时，提取和剥离要求该边界与合法 ZIP 后缀起点一致。

对未经修改的完整 Manifest/Bundle 根，tarstream 输出恢复合法相对偏移 ZIP 后缀的边界，使规范镜像信封身份在 Store 往返中保持一致；既有 tarstream 声明和显式窗口维持各自边界。Store 统计仅在最终 Put 实际创建 Manifest 时计入新增字节。tarstream-only load 报告逻辑进度，`--no-progress` 可关闭动态进度。

### `manifest-ctl get-manifest` — 取回 manifest 字节

取得 Manifest 物理对象的原始字节,主要用于 debug 或离线 `info`/`diff`。
实际取数经过配置的 cache/store 读路径。

```
manifest-ctl get-manifest [--output -|FILE] <hex|manifest://hex>
```

```bash
# 拿出来直接看(也可直接 `manifest-ctl info manifest://<hex>`)
manifest-ctl get-manifest a1b2c3d4... | manifest-ctl info -

# 存档
manifest-ctl get-manifest --output disk.manifest a1b2c3d4...
```

### `manifest-ctl info`

读取并打印 Manifest 摘要。来源三选一:本地文件、`-`/省略表示 stdin、
`manifest://<hex>` 表示经配置的远端路径取数。本地/stdin 不需要配置、customer key
或 store 连接;远端元数据读取需要配置和连接,但不会解封 customer key 保护的密钥表。

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
avg chunk:     512.0 KiB

size distribution (data chunks):
       min(N)          P1          P5         P25         P50         P75         P95         P99      max(N)
  63.5K(1)       128.0K      256.0K      384.0K      512.0K      640.0K      896.0K        1.0M     1.0M(412)
  (max == configured max — 412/20480 (2.0%) chunks are forced CDC cuts)
key table:     655389 bytes (sealed)
manifest size: 1802334 bytes
```

`min chunk` / `max chunk` 标 `(configured)` 是因为它们来自 Manifest 头里记录的
**配置值**,不是实测;实测分布在下面的百分位表里(只统计非 zero 的数据 chunk,
`min(N)` / `max(N)` 括号内是取到该极值的 chunk 数;最后一行仅当实测最大值正好
等于配置上限时出现,提示有多少 chunk 是被 CDC 强制切的)。

查看 store 中的 manifest 直接 `manifest-ctl info manifest://<hex>`,等价于
`get-manifest … | info -` 管道。

### `manifest-ctl verify`

通过 fetch 路径读取并解密所有非零 chunk,验证完整 Manifest 的内容与可用性。
即使普通读取关闭校验,本命令也强制 physical SHA-256 校验。

```
manifest-ctl verify [--no-progress] a1b2c3d4...        # 或 manifest://a1b2c3d4...
```

```
verified: 20480  skipped(zero): 0  holes: 0  failed: 0  total chunks: 20480
```

### `manifest-ctl diff` — 去重率分析

```
manifest-ctl diff <manifest-a> <manifest-b>
```

两个参数均为本地 Manifest 文件(可用 `get-manifest` 取回);diff 不打开 store,
也不加载配置或 customer key。

`shared` 与 `only in A/B` 统计不同的非零 ContentKey，不按重复 entry 位置累计。对应字节总数对每个唯一 chunk 的逻辑大小只求和一次，不是压缩/加密后的 Store 字节数。零条目与 hole 不进入这些集合；镜像大小、原始 entry 数和 hole 仍独立报告。同一 ContentKey 在单个输入内或两个输入间具有冲突的逻辑大小时拒绝比较。

CLI 的 `dedup ratio` 为 `1 - |A ∪ B| / (|A| + |B|)`，分母为空时取零。warm-pool 工作负载使用的成对重叠度（Jaccard）则为 `shared / (shared + onlyA + onlyB)`。两者都不保证通用的 VM 内存节省。以下仅为示例输出：

```
manifest A:  10.0 GiB, 20480 chunks (20480 unique)
manifest B:  10.1 GiB, 20512 chunks (20512 unique)
shared:      18432 chunks (9.0 GiB)
only in A:   2048 chunks (1.0 GiB)
only in B:   2080 chunks (1.1 GiB)
dedup ratio: 45.0%
```

### `manifest-ctl config`

```
manifest-ctl config show     [--manifest-config <path>]
manifest-ctl config generate
```

`generate` 输出带注释模板;`show` 打印已加载的 YAML 配置,不能据此证明后端连接和
延迟构造的所有组件都能成功。

## 配置

### manifest-config.yaml

```yaml
manifest:
  key: "0a1b2c3d..."              # 占位符:32 字节 hex 客户密钥,用于密封密钥表
  verify_content: true             # 普通读取是否复验 physical Manifest/Chunk SHA-256;缺省 true
  # write_generation: G3           # 可选;指定写入 generation
store:
  endpoint: 127.0.0.1:7100        # 远端写/直接 store 读必填;离线 Bundle 可省略:host:port 或 Unix socket
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
  ref: auto                       # off | auto | required，独立于 local
```

字段说明:

- `manifest.key` 是**密封 Manifest 密钥表**的 customer key,不派生 chunk 加密 key。
  丢失后无法解封该表。可不写入 YAML,由主要交付渠道 **非空 `MANIFEST_KEY`**
  提供。非空环境值覆盖 YAML;空环境值回退到 YAML。key 懒解析且不写回 Config,
  因此 `config show` 不回显环境提供的 key。**显式写入 YAML 的 key 仍在 Config
  中,可被 show 打印**。两者均空时,真正需要 key 的操作才失败。
- `manifest.verify_content` — 缺省 `true`。普通 Bundle/cache/store Fetcher、
  `GetManifestBlob` 和 `CheckManifest` 都用同一策略：`true` 时在解析 Manifest
  前复验 `SHA256(physical Manifest) == requested key`，首次读取 Chunk 时复验
  `SHA256(physical Chunk) == CiphertextHash`；`false` 时 ContentKey 只作 locator，
  不执行这两次完整对象扫描。关闭后进程只记录一次清晰 WARNING；Manifest
  envelope/geometry、key table GCM、Chunk format/长度、解密/解压等结构检查仍然
  执行。但 chunk AES-CTR 本身不是认证加密;关闭 physical hash 会去掉对应的 chunk
  内容完整性检查。`manifest-ctl verify`、Bundle full verify/upload/export 和对象修复始终
  强制开启 SHA 校验，不受该字段影响。
- `manifest.write_generation` — 可选写入 generation。有 `store.endpoint` 时，
  留空调用 `AdmitWrite()` 选择当前最新项，非空调用
  `AdmitWriteFor(generation)`，generation 已移除则失败；Store RPC 失败不会降级为
  本地派生。无 Store 的离线 Bundle writer 在字段留空时使用普通 generation 名
  `NONE`，显式配置时使用该值，两者都调用公共
  `store.SaltForGeneration` 派生 salt。`NONE` 不是保留字或协议特殊值。
- `store.endpoint` — 远端对象 I/O 经 gRPC 到 store-ctl;本地 Bundle 对象 I/O 遵守
  其独立 reader/writer 合同。取 `host:port`(TCP)或一个
  Unix socket(裸路径 `/run/sandbox/store.sock` 会被规范化为 gRPC 的
  `unix:///run/sandbox/store.sock`,显式 `unix:` 形式原样透传)——与 store-ctl
  的 `listen` 同址(同机经 socket 免 TCP 栈)。
- `cache.endpoint` — 空则 manifest-ctl `load` 路径直走 store gRPC;非空则
  通过 wire 协议穿 cache-ctl。同样接受 `host:port` 或 Unix socket 路径
  (`unix://path` 或裸 `/path`),与 cache-ctl 的 `listen` 同址。这是构造时选源,
  不是 cache 报错后自动绕过它的 failover。
- `chunker.mode` — `cdc`(FastCDC,变长)或 `fixed`(固定大小)。详见 [chunking](artifacts_zh.md#chunking)。
- `crypto.chunk` / `crypto.manifest` — chunk 与 Manifest 的加密算法,均仅支持
  `aes`([加密模式 — chunk](artifacts_zh.md#加密模式--chunk) / [加密模式 — Manifest](artifacts_zh.md#加密模式--manifest));其它值在构造时即被拒绝。
- `crypto.local` — 本地存储兼容/强制 policy,不是算法选择。`off` 不启用本地
  codec,`auto` 同时接受历史 plaintext 与加密格式,`required` 只接受加密格式。
  缺省为 `off`。
- `crypto.ref` 控制 sandboxer 命名 ref location 中的 tarstream。它独立缺省为 `auto`，接受明文和可用同一客户密钥解密的输入，新写入使用该密钥加密。发布图可以保留已有明文依赖。`off` 只接受和写入明文，`required` 只接受和写入密文。Manifest 和 Bundle 沿用现有编码。

请搭配完整支持 ref 读取、复用和发布的 sandboxer 启用此配置。旧 loader 可能忽略未知字段。升级到支持该策略的 sandboxer 后，原有 `local: off` 配置发布新的 ref 时需要密钥。若要保留原发布策略，将旧 `local` 值显式配置到 `ref`。原有 `local: required` 用户要继续严格拒绝明文 ref，应显式设置 `ref: required`。

### 加载顺序

**配置文件名**只来自 CLI flag 与对应环境变量,**没有自动查找**:

| 来源 | 优先级 |
|---|---|
| `--manifest-config FILE` | 非空时优先。 |
| `MANIFEST_CONFIG` | flag 为空时使用。 |
| 两者皆空 | 需要配置的命令报错;[公共 flag](#公共-flag) 列出例外。 |

**customer key** 有独立顺序:非空 `MANIFEST_KEY` 覆盖 YAML `manifest.key`。
共享配置可以省略 key,由其他渠道单独交付。

`pkg/manifest.ParseConfig` 还支持**内存 YAML 字节**,例如 config socket 向
sandbox-ctl 投递配置,endpoint/crypto 不必落盘。调用方可单独设置
`Config.Manifest.Key`;若随后使用 `CustomerKey()`,环境覆盖规则仍适用。

没有 chunker/crypto 单字段 CLI override;通过修改 YAML 调整。

## 使用共享 Go 入口

`pkg/manifest/transfer.Reader.Open` 返回持有来源资源的 `Opened` sparse source。报告成功前完成 `Opened.Verify(ctx)`，然后 Close 并传播关闭错误；Reader 自身另外关闭，释放远端 fetcher 并清理 key material。`transfer.Write` 对 sparse source 生成请求的载体/Store 输出，`BeforeCommit` 在 Store 根发布前完成调用方验证。`manifest.RefLocations` 与 CLI、sandboxer 共用位置映射校验。输入资源须保持稳定并存活到操作结束，终结验证与 Close 错误必须向上传播，失败输出不可发布。格式和读取完成规则见[数据契约](artifacts_zh.md)；精确 Go options 以 [`pkg/manifest/transfer`](../pkg/manifest/transfer/) 的公共 API 注释为准。

## 相关契约

- [store](store_zh.md):manifest-ctl 经 store-ctl gRPC 写入远端字节。
- [cache](cache_zh.md):可选读取加速;cache-ctl 可用同款 store client 作为 origin。
- [guest-runtime flatten](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli_zh.md):镜像展平后经 `flatten-ctl export --upload` 入库。
- [sandboxer](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/runtime_zh.md):磁盘 base 与快照的 Manifest key 引用。
- [系统架构](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/architecture_zh.md):Manifest 抽象在平台中的位置。

Manifest 来源选择与惰性恢复见[来源选择与惰性恢复](artifacts_zh.md#来源选择与惰性恢复).
