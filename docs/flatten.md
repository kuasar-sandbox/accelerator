# flatten — 容器镜像展平工具

把多层 OCI/Docker 镜像合并(展平)成单个 EROFS 文件系统镜像,作为
`manifest-ctl` 的输入,或直接挂为沙箱 blk0 base。

`flatten-ctl` 强调**确定性**——同一镜像每次展平出字节级相同的输出,
保证后续 chunk dedup、manifest content-key 跨次稳定。输出文件**自带 OCI
runtime config**(末尾追加 ZIP),沙箱启动时无需额外索引文件就能拿到
Entrypoint / Env / WorkingDir 等启动参数。

## 1. 概述

### 1.1 为什么要展平

容器镜像由若干 tar layer 叠加而成,运行时需要 overlayfs / fuse-overlay 等
文件系统驱动把它们合并成单一根目录。沙箱场景里我们换成更轻的方案:**离线
合并到一个 EROFS 镜像**——

- guest 用 vhost-user-blk 直接把它当 read-only 根挂上,无需 overlayfs;
- host 多个 sandbox 共享同一个 EROFS 文件的 page cache,密度成本明显降低;
- EROFS 是只读 + 不可变,与内容寻址 (manifest 的 chunk + content key) 天然契合。

### 1.2 输入与输出

输入,二选一:

- **远程 registry 镜像**——镜像引用如 `nginx:1.27`、`gcr.io/ns/app@sha256:...`;
  `flatten-ctl` 直接拉取并展平(§2.4),拉取凭据经 `FLATTEN_REGISTRY_*` 环境变量注入,
  层 blob 落入可跨进程共享的本地 OCI-layout 缓存;
- **Docker archive**(`docker save` 产物的 tar 流,stdin 或本地文件)。

(OCI layout 目录暂不直读,见 §5.5。)

输出:**单文件**,字节布局 `EROFS 镜像 + 末尾 ZIP archive`(详见 §3 镜像
格式)。ZIP 内含 OCI runtime config 投影,沙箱启动时直接读取。**两条源(registry /
docker-archive)汇入同一展平 sink,同一镜像内容产出逐字节相同的 EROFS。**

不做的事:不签名,不加密,不分块——加密/去重由 `manifest.md` 描述的下一阶
段处理。

### 1.3 在系统中的位置

```
   ┌─ docker save ──┐     ┌─ flatten-ctl ───────────┐     ┌─ manifest-ctl store ──────┐
   │  layered tar   │────►│  layer iter + merge     │────►│  chunk + crypto + dedup   │
   └────────────────┘     │  whiteout handling      │     │  → store-ctl gRPC Put     │
                          │  mtime → 0              │     └───────────────────────────┘
                          │  mkfs.erofs             │
                          │  + append config zip    │     ┌─ sandbox-ctl run ─────────┐
                          └─────────────────────────┘     │  blk0.base = file://...   │
                                                          │             or manifest://│
                                                          └───────────────────────────┘
```

OCI layout (`oci:./dir`) 可先经 `skopeo copy oci:./xxx docker-archive:/tmp/x.tar`
转成 docker-archive 再喂 flatten-ctl。

## 2. 命令行接口

五个子命令:`export`(展平,可选直接入库)、`verify`(确定性自检)、
`info`(检视已生成镜像 / `manifest://` 引用)、`cache`(检视/回收本地拉取缓存)、
`config`(输出/校验 flatten 配置)。

| 子命令 | 用途 |
|--------|------|
| `export` | registry 镜像或 docker-archive → 确定性 EROFS;`--upload` 时顺带 ingest 进 store 并打印 manifest key;`--with-referer` 经 Referrers 幂等跳过/回写(§2.4) |
| `verify` | 对同一输入展平两次,比对字节级 sha256,确认确定性(registry 源:拉一次→展两遍) |
| `info` | 读 EROFS superblock + 末尾 ZIP 里的 OCI runtime config 并打印 |
| `cache` | `cache info` 看缓存占用、`cache gc` 按 LRU 回收到上限(§2.5) |
| `config` | 输出规范化的 flatten 配置(`--config`/`FLATTEN_CONFIG`,加载即校验),或 `--template` 骨架;`-o <file>` 写文件(默认 stdout) |

`export` / `verify` 的输入是**位置参数**(匿名),按下列优先级自动判别 registry / 本地:

```
1. "-"                       → stdin docker-archive
2. "docker-archive:<path>"   → 本地文件(剥前缀)
3. os.Stat 命中常规文件       → 本地 docker-archive  (./app.tar、/abs/x.tar 自然命中)
4. 否则按 name.ParseReference → 远程 registry 引用
```

歧义(本地文件名恰好形如 `repo:tag`)用 `--registry` / `--archive` 强制。`info` 的位置
参数必填,接受 EROFS 文件路径或 `manifest://<hex>`。位置参数须置于 flags 之后(Go stdlib
flag 在首个非 flag 实参处停止解析)。

### 2.1 `flatten-ctl export`

```
flatten-ctl export [flags] <ref|path|->

  <ref|path|->            registry 镜像引用,或 docker-archive(省略/`-` = stdin)
  --output <path|->       EROFS 输出路径;`-` = stdout。--upload 关闭时必填,
                          --upload 开启时可省(产物默认丢弃,只要 manifest key)
  --upload                展平后把 EROFS ingest 进 store,stdout 打印 manifest key
  --manifest-config <p>   manifest 配置 YAML(覆盖 MANIFEST_CONFIG env);--upload 必需
  --config <p>            flatten 配置 YAML(覆盖 FLATTEN_CONFIG env):tmpdir/platform/
                          cache/referer(详见 §2.4)
  --platform <os/arch>    覆盖配置里的拉取 platform(os/arch[/variant])
  --no-progress           禁用 stderr 进度输出

  # 远程 registry 源(详见 §2.4)
  --print-digest          stdout 打印解析后的源镜像 digest(repo@sha256:..);与 --output - 互斥
  --registry / --archive  强制把位置参数当 registry 引用 / 本地文件(消歧)
  --with-referer          强制启用幂等 Referrers 流(亦可由配置 referer.enabled 默认开启);
                          命中则复用 manifest id 跳过重导,否则导出后回写(需 --upload;§2.4)
```

典型用法:

```bash
# 远程 registry 镜像 → 单文件
flatten-ctl export --output nginx.erofs nginx:1.27

# 远程镜像直接入库(stdout 即 manifest key);凭据走环境变量
export FLATTEN_REGISTRY_USERNAME=robot FLATTEN_REGISTRY_PASSWORD=…
flatten-ctl export --upload --manifest-config manifest.yaml --config flatten.yaml \
    registry.example.com/team/app@sha256:… > app.key

# docker save 管道 → 单文件输出(省略位置参数 = stdin)
docker save myapp:v1 | flatten-ctl export --output my-app.erofs

# 本地 docker-archive 文件(位置参数在 flags 之后)
flatten-ctl export --output my-app.erofs ./my-app.tar

# 幂等:已展平过即复用 manifest id,跳过拉取+展平+上传(或在 flatten.yaml 设 referer.enabled: true 省去 --with-referer)
flatten-ctl export --upload --with-referer --manifest-config manifest.yaml \
    --config flatten.yaml registry.example.com/team/app:v1 > app.key

# /tmp 不够大时在 flatten.yaml 设 tmpdir: /var/tmp,再 --config flatten.yaml
flatten-ctl export --output big.erofs --config flatten.yaml ./big.tar
```

### 2.2 `flatten-ctl verify`

```
flatten-ctl verify [--tmpdir D] [--no-progress] <path|->   # 省略/`-` = stdin
```

对同一输入展平两次,比对 sha256,确认字节级确定性。stderr 输出:

```
Pass 1: sha256:a1b2c3... (1.2 GiB)
Pass 2: sha256:a1b2c3... (1.2 GiB)
DETERMINISTIC
```

### 2.3 `flatten-ctl info` — 检视镜像

```
flatten-ctl info [--json] [--manifest-config <path>] <path|manifest://hex>

  <path|manifest://hex>          EROFS 文件路径,或 manifest://<hex>(位置参数,必填)
  --json                         机器可读 JSON 输出(默认人类可读)
  --manifest-config <path>       manifest 配置 YAML;`manifest://` 输入必需
                                 (经 cache-ctl + store-ctl 拉回再读 superblock)
```

读 EROFS superblock 拿 image size,从末尾 ZIP 解出 OCI runtime config 并
打印。

人类可读输出示例:

```
$ flatten-ctl info my-app.erofs
EROFS image size:  1.2 GiB (1287651328 bytes)
Architecture:      amd64
Os:                linux
User:              app
Entrypoint:        ["/usr/bin/foo"]
Cmd:               ["--config","/etc/foo.yaml"]
WorkingDir:        /app
Env (3):
  PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
  HOME=/root
  LANG=C.UTF-8
ExposedPorts:      8080/tcp
Labels:
  com.example.version=1.2.3
```

JSON 输出示例:

```json
{
  "erofs_size": 1287651328,
  "config": {
    "Architecture": "amd64",
    "Os": "linux",
    "User": "app",
    "Env": ["PATH=...", "HOME=/root", "LANG=C.UTF-8"],
    "Entrypoint": ["/usr/bin/foo"],
    "Cmd": ["--config", "/etc/foo.yaml"],
    "WorkingDir": "/app",
    "ExposedPorts": {"8080/tcp": {}},
    "Labels": {"com.example.version": "1.2.3"}
  }
}
```

无 ZIP trailer 时(老版本输出)`config` 为 `null`,人类模式打印
`(no OCI config trailer)` 提示。

辅助管道:

```bash
# 用 unzip 直接拎 config.json(ZIP 是标准格式,任何 ZIP 工具可读)
unzip -p my-app.erofs config.json | jq

# 或者经 flatten-ctl info --json 进 jq
flatten-ctl info --json my-app.erofs | jq '.config.Entrypoint'
```

### 2.4 远程拉取、本地缓存与 Referrers 回写

位置参数判定为 registry 引用时(§2 判别规则),`flatten-ctl` 经 [go-containerregistry]
直接拉取、展平,无需先 `docker save`。所有 registry 行为由 **`--config` YAML**
(或 `FLATTEN_CONFIG` env)配置,密钥不入文件:

```yaml
tmpdir: ""                     # 每次运行临时目录的父目录(空 = $TMPDIR 或 /tmp)
platform: linux/amd64          # 空 = 跟随宿主架构(linux/$GOARCH);--platform 覆盖
insecure: false                # 私有/dev registry 走 HTTP / 跳过 TLS 校验
pull_jobs: 4                   # 并发下载层数(下载并行、apply 串行)
cache:
  dir: ""                      # OCI-layout 持久缓存根;空(默认)= 临时缓存(tmpdir 下,跑完清理)
  max_size: 10GiB              # 上限,超出按 LRU 回收;"0" = 不限,仅手动 cache gc
referer:                       # --with-referer / referer.enabled 用(见下)
  enabled: false               # 置 true 默认启用幂等 Referrers 流(等价命令行 --with-referer)
  desc: acme-prod              # 公开 owner 描述
  key:  acme-prod              # HMAC 消息,默认 == desc
  validity: 720h               # 可选;写入 valid_at 的过期段
```

artifact_type 固定为常量 `application/vnd.kuasar.flatten-manifest.v1`(不可配置,保证跨工具/版本一致)。

**凭据(命名空间环境变量,匿名回落)**:`FLATTEN_REGISTRY_TOKEN`(Bearer,优先)或
`FLATTEN_REGISTRY_USERNAME` + `FLATTEN_REGISTRY_PASSWORD`(Basic);都不设则匿名拉公有
镜像。密钥只走 env(不上 argv、不入配置文件),契合展平数据面节点由管理面按任务下发租户
拉取凭据的模型(`deployment.md` §5)。

**本地缓存**:标准 **OCI image layout** 目录(`oci-layout` + `index.json` +
`blobs/<algo>/<hex>`),`crane`/`skopeo` 可直接检视。缓存命中判定 = `blobs/` 下该 digest
是否存在(内容寻址,多个 `flatten-ctl` 进程共享同一目录天然安全);blob 边下边校 digest、
原子落盘(temp→rename)。超出 `max_size` 时持 flock 按 LRU(mtime)回收到低水位,并以
grace 期保护近期写入的 blob 不被并发拉取误删。`cache.dir` 指向共享路径即跨任务去重,指向
每任务独立路径即隔离——由调用方按需配置。

**多架构**:registry 引用常指 manifest index,按 `platform` 选出具体 image 再展平;
`Architecture`/`Os` 仍原样投影进 config.json(§3.2),不做校验。

**确定性提醒**:tag 可变,`:latest` 不可复现;可复现构建请钉 `@sha256:`。`export` 把
解析到的 `repo@sha256:..` 打到 stderr(`--no-progress` 关闭),`--print-digest` 另打到
stdout。`verify` 对 registry 源先拉一次进缓存,再从同批缓存 blob 展两遍比对——对 tag 只
与其当前指向一样稳,对 digest 永远稳定。

#### Referrers 回写与幂等跳过(`--with-referer`)

展平产出的 manifest id(= `--upload` 入库的 manifest 内容键)可经 **OCI Referrers API**
回写到**源镜像所在 repo**,作为 registry 侧、按 owner 作用域的去重备忘,让重复 `export`
直接复用、跳过拉取+展平+上传。

启用 `--with-referer`(蕴含 `--upload`、需 manifest 配置;deliverable 是 stdout 的
manifest key,故与 `--output` / `--print-digest` 互斥):

1. 解析源 → 平台镜像 digest `D`;
2. `Referrers(repo@D)` 按 `artifact_type` 过滤,读各 referrer 注解,匹配 `owner` 且未过期
   (`valid_at`)→ 直接打印其 `id`,**不拉层 / 不展平 / 不上传**;
3. 未命中 → 拉取+展平+ingest 得 manifest key → 构造 referrer artifact(subject=`D`、
   `artifact_type`、注解 `owner`/`id`/`valid_at`)推回源 repo → 打印 key。

referrer 注解:

```
vnd.kuasar.flatten-manifest.owner    = <hmac-hex> <referer_desc>
vnd.kuasar.flatten-manifest.id       = <manifest_id>                        # ingest 产出的内容键
vnd.kuasar.flatten-manifest.valid_at = <import RFC3339>[ <expiry RFC3339>]
```

其中 `hmac = HMAC-SHA256(key = 客户秘钥 MANIFEST_KEY, msg = referer.key)`——按
(租户秘钥, referer key) 恒定、导出前即可算、无客户秘钥不可伪造,使不同租户在同一公有 base
镜像上的 referrer 互不碰撞。

**前提与注意**:OCI 规范要求 referrer 与 subject 同 repo → `--with-referer` 需对**源
repo 有 push 权限**(面向租户自有 registry;对只读上游写不进会**硬失败**)。referrer
artifact 含时间戳,本身每次不同,但展平产物 / manifest id 仍确定。对公开 base 镜像,
referrer(owner token / id / 时间)对能读该 repo 者可见——owner 经 HMAC、id 为不透明内容
键,但"某 owner 在某时刻展平过该镜像"这一事实会暴露。

[go-containerregistry]: https://github.com/google/go-containerregistry

### 2.5 `flatten-ctl cache` — 检视/回收拉取缓存

```
flatten-ctl cache info [--config <p>] [--cache-dir <D>]
flatten-ctl cache gc   [--config <p>] [--cache-dir <D>] [--cache-max-size <S>] [--no-progress]
```

`cache` 子命令面向**持久**缓存(`cache.dir` 显式配置时);默认临时缓存随 `export` 跑完即清，
无需 gc。`cache info` 打印缓存目录、blob 数、总占用与上限。`cache gc` 持 flock 按 LRU 回收到配置
(或 `--cache-max-size` 覆盖)的上限,`"0"` 清空(grace 期内的除外)。缓存目录默认取
`--config` 的 `cache.dir`,`--cache-dir` 覆盖。常驻场景可由 cron 周期跑 `cache gc`
强约束上限(`export` 每次拉取后也会顺带回收)。

## 3. 镜像格式

### 3.1 字节布局

```
偏移 [0,         erofs_end)      EROFS 文件系统(self-describing,
                                  superblock 在偏移 1024 标明 image size)
偏移 [erofs_end, EOF)             ZIP archive(STORED 模式无压缩)
                                    / config.json     OCI runtime config 投影
```

两段共存于一个文件,**互不干扰**:

- **EROFS 读路径**(kernel `mount -t erofs` / vhost-user-blk backend):
  从偏移 0 读 superblock,superblock 自带 `blocks << blkszbits` 的 image
  size,kernel 不读 size 之后的字节,trailing ZIP 自然不可见
- **ZIP 读路径**(任意标准 ZIP 工具,如 `unzip`):从文件**末尾**扫
  End-of-Central-Directory(EOCD),内部 offset 都是相对 ZIP 起点,EOCD
  扫描容忍前缀任意字节,EROFS 段的存在不影响 ZIP 解析

`erofs_end = blocks × (1 << blkszbits)`,从 superblock 解析。EROFS 镜像
endian-neutral,跨 host arch 可挂载。

### 3.2 config.json schema

ZIP 内 `config.json` 是 OCI image config 的运行时相关投影。**只保留启动需要
的字段**,跳过 `created` / `author` / `history` / `rootfs.diff_ids` 等会破坏
跨次确定性的元数据。

字段集合(运行时投影 schema):

```
Architecture     string                 (passthrough,例如 "amd64" / "arm64")
Os               string                 (passthrough,例如 "linux")
User             string,omitempty
Env              []string,omitempty
Entrypoint       []string,omitempty
Cmd              []string,omitempty
WorkingDir       string,omitempty
ExposedPorts     map[string]struct{},omitempty
Volumes          map[string]struct{},omitempty
StopSignal       string,omitempty
Labels           map[string]string,omitempty
Healthcheck      *Healthcheck,omitempty   (Test/Interval/Timeout/StartPeriod/Retries,
                                            Duration 字段是 OCI 原生 nanoseconds int64)
```

**为什么 Architecture / Os 不 omitempty**:即使空值也要写入,让下游观察者
看到"源镜像没填这两项"的问题,而不是默默继承默认值。flatten-ctl 是字节级
转换工具,不验证平台兼容(amd64 vs arm64 决策属于上层调度器)。

未列出的源字段(`OnBuild` / `ArgsEscaped` / `Domainname` / `Hostname` /
`AttachStdin` / `Tty` / `MacAddress` 等)被静默丢弃——不在沙箱启动模型中
使用。

### 3.3 跨次确定性来源

ZIP 部分:

- 单 entry,文件名固定 `config.json`
- 无压缩(stored,内容直接可见)
- 修改时间固定为纪元常量 `1980-01-01 00:00 UTC`,绝不用挂钟
- 确定性 JSON 序列化:map keys 按字典序、slice 顺序保留(Env/Cmd/Entrypoint
  语义需要)、零值字段抑制

EROFS 部分见 §4.3 元数据归一化。

两个运行时投影字段相等 → JSON 字节相等 → ZIP entry 字节相等 → 整文件
sha256 相等。

### 3.4 沙箱怎么用 config.json

`sandbox-ctl run` 在启动前从 boot.root.base 文件末尾解 ZIP,拿到
运行时投影作为 LaunchSpec 的 fallback。`sandbox.yaml` `launch.*`
字段优先(yaml override > image config),Env 合并:

| 字段 | 合并规则 |
|------|---------|
| `exec` | yaml `launch.exec` 优先;为空则取 `Entrypoint[0]`(或 `Cmd[0]`,若 Entrypoint 空) |
| `args` | yaml `launch.args` 优先;为空则取 `Entrypoint[1:] + Cmd` |
| `env` | image `Env` + yaml `launch.env`,后者覆盖同名 key |
| `workdir` | yaml `launch.workdir` 优先;为空则取 `WorkingDir` |
| `user` | yaml `launch.user` 优先;为空则取 `User`(命名用户由 guest 侧解析) |
| `stop_signal` | yaml `launch.stop_signal` 优先;为空则取 `StopSignal` |
| `volumes` | image `Volumes` 每个目录并入 `mounts`(等价 `type: empty`),显式 `mounts` 同 target 为准 |

因此 `launch.exec` 不再必填——若 image config 有 Entrypoint/Cmd 即可省略。
详见 [`sandbox.md`](sandbox.md) §3.3。

## 4. 算法

### 4.1 layer 迭代

docker-archive 是一个 tar:其中包含 `manifest.json` 描述层顺序、若干
`*/layer.tar` 是各层 tarball、`*/json` 是层元信息(image config)。

`flatten-ctl`:

1. 解析 `manifest.json` 取到层列表(底→顶顺序)与 image config 路径;
2. 读 image config,投影出 §3.2 的运行时字段集合;
3. 按顺序读每个 `layer.tar`,把所有 `tar entry` (header + content) 流式应用到
   一个内存中的虚拟文件树;
4. 上层 entry 覆盖下层同路径 entry。

### 4.2 whiteout 处理

OCI 镜像规范用特殊文件名表达"删除":

- `.wh.<name>` —— 删除同目录下的 `<name>`(file 或 dir);
- `.wh..wh..opq` —— 把所在目录标记为 opaque(其下所有底层条目都不可见)。

`flatten-ctl` 在合并时识别这两类 marker,把对应条目从虚拟文件树中移除,然
后**不**把 marker 本身写进输出——最终 EROFS 只看到合并后的可见文件。

### 4.3 元数据归一化(确定性的关键)

为保证字节级可重复,以下字段在写入 EROFS 之前被强制归一化:

| 字段 | 处理 |
|---|---|
| mtime / atime / ctime | 全部归零(epoch 0) |
| uid / gid | 保留原值(应用语义敏感) |
| inode 编号 | 由 `mkfs.erofs` 按确定顺序分配 |
| 文件遍历顺序 | 字典序(`mkfs.erofs` 内部) |
| hardlink | 归并到内容寻址(同 content + 同 metadata → 同 inode) |
| 扩展属性(xattr) | 保留;按 key 字典序写入 |

EROFS 自身的格式版本由 `mkfs.erofs` 决定(我们用 erofs-utils 1.9.x);项目
固化此版本,确保跨节点构建结果一致。

### 4.4 mkfs.erofs 调用

`flatten-ctl` 把合并后的虚拟文件树物化到一个临时目录,然后执行
`bin/<arch>/mkfs.erofs <output> <staging-dir>` 把它编码成 EROFS 镜像。

完成后追加 ZIP:以 append 方式打开输出文件,在 EROFS 段之后写一条 STORED
(无压缩)模式的 `config.json` entry(§3.3 的确定性约束)。

`mkfs.erofs` 由 `sandbox-deps` 仓构建产出(`make -C ../sandbox-deps erofs`);本仓
`make build` 只构建 flatten-ctl。flatten-ctl 启动时优先在自己的同目录查找
`mkfs.erofs`,其次走 `PATH`。

EROFS 格式 endian-neutral,所以 host arch 与 target arch 无关——任何
`mkfs.erofs` 都能产出可被任何 arch guest 挂载的镜像。

## 5. 设计决策

### 5.1 为什么是 EROFS 而非 ext4 / squashfs

- **EROFS 是只读** —— 与内容寻址 (chunk + content key) 天然兼容,加上
  不可变性反过来支持 host 跨 sandbox 共享。
- **EROFS 单文件** —— 输出是流式 / 单文件,适合 manifest 上传与挂载。
- **EROFS endian-neutral** —— 跨 host arch 构建跨 host arch 挂载,密度
  规划不受架构耦合。
- **squashfs** 跨 sandbox 共享要走 page cache 命中,密度退化。
- **ext4** 是可写的;每个 sandbox 要独立 mount + COW 才能复用,密度比 EROFS
  + overlayfs 组合差,而且需要 fsck 等运维。

### 5.2 为什么离线合并而非运行时 overlayfs

运行时 overlayfs 需要每 sandbox mount 一次,host 内核 mount 表线性增长,
密度上限受 mount 数量约束;EROFS 单 mount 跨多 sandbox 共享绕开这个瓶颈。

代价是镜像构建一次,所以更新流程多一步——但镜像构建本来就是 CI/CD 流水
线,这一步成本可摊销。

### 5.3 为什么 config.json 内嵌而不是边车文件

外加 `app.erofs.config.json` 边车文件的方案有两个问题:

- **拷贝 / 上传分裂**:用户必须记住"两个文件一对",`scp` 漏一个就坏
- **manifest:// 无副车机制**:manifest 是单一 reader,边车文件需要单独的
  manifest key + 单独的拉取流程

ZIP-at-end 让单一文件自描述,`manifest-ctl store` 一次喂入即完整,
`sandbox-ctl run` 一次解析即拿到全部启动配置。EROFS 与 ZIP 双格式天然不
冲突(superblock 自描述 size + ZIP 从尾部扫 EOCD),无需引入新协议层。

### 5.4 为什么投影而不是原样转发

OCI image config 字段繁多,大量与启动无关:`created` / `author` / `history`
是构建元数据,跨实例不同;`rootfs.diff_ids` 是层 hash,展平后不再有意义;
`OnBuild` 是构建指令而不是运行时设置。原样转发会:

- 破坏跨次确定性(timestamps 漂移)
- 让 manifest dedup 无谓波动(每次构建的 history 不同)
- 误导沙箱(应用看到 OnBuild 可能误用)

显式投影把"运行时启动需要什么"作为唯一标准,下游可预测。

### 5.5 不实现 / 暂不实现的功能

- **OCI layout 目录直读** —— `export` 现已支持直接从 registry 拉取(§2.4),且本地
  拉取缓存本身就是标准 OCI layout 目录;但"把任意 OCI layout 目录当输入展平"仍未
  直接支持。需要时上游可用 `skopeo copy oci:./dir docker-archive:x.tar` 转换,或
  `crane push` 到 registry 再拉
- **镜像签名/加密** —— manifest 层做(`manifest.md`)
- **层级保留** —— flatten-ctl 输出是合并后的单一 EROFS,层信息丢失。需要
  分层保留的场景(例如增量推送)由 manifest 层 chunk dedup 取代
- **动态平台选择** —— flatten-ctl 是字节级转换,Architecture/Os passthrough。
  多 arch 镜像选择由调用方决定(例如 `docker save --platform=...`)

## 6. 性能特征

构建时间:与镜像大小近似线性,主要成本在 layer tar 解压 + mkfs.erofs。
1 GiB 镜像 ~3-5 秒(SSD)。ZIP append 步骤 < 10 ms(单 entry,无压缩)。

确定性测试 (`--verify`):同镜像两次展平输出 sha256 一致,所有发布 build 跑
此检查。

`flatten-ctl info` 不解压 EROFS,仅读 superblock(128 字节)+ ZIP EOCD 扫描
+ 单 entry 解压,亚毫秒级。

## 7. See Also

- [`manifest.md`](manifest.md) —— 展平后的镜像经 manifest-ctl 入内容
  寻址存储;chunk dedup 跨镜像共享 layer-level 重复内容
- [`sandbox.md`](sandbox.md) §3.3 flattened image 内嵌 config.json ——
  沙箱 启动时如何使用 ZIP trailer 中的 OCI runtime config
- [`sandbox.md`](sandbox.md) §boot.root.base —— 用展平镜像作为 sandbox
  的只读根
- [`build.md`](build.md) —— `sandbox-deps` 构建 mkfs.erofs(`make -C ../sandbox-deps
  erofs`);本仓 `make build` 只构建 flatten-ctl,运行期经同目录 / `PATH` 定位 mkfs.erofs
- `PROPOSAL.md` §10.4 —— 展平在系统中的位置与目标
