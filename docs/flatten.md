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

输入:**Docker archive** (`docker save` 产物的 tar 流;OCI layout 暂不直
接支持,见下)。

输出:**单文件**,字节布局 `EROFS 镜像 + 末尾 ZIP archive`(详见 §3 镜像
格式)。ZIP 内含 OCI runtime config 投影,沙箱启动时直接读取。

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

三个子命令:`export`(展平,可选直接入库)、`verify`(确定性自检)、
`info`(检视已生成镜像 / `manifest://` 引用)。

| 子命令 | 用途 |
|--------|------|
| `export` | docker-archive → 确定性 EROFS 文件;`--upload` 时顺带 ingest 进 store 并打印 manifest key |
| `verify` | 对同一输入展平两次,比对字节级 sha256,确认确定性 |
| `info` | 读 EROFS superblock + 末尾 ZIP 里的 OCI runtime config 并打印 |

输入 `--input` 统一接受:`-`(stdin 的 docker-archive 流)或本地
docker-archive tar 路径;`info` 额外接受 `manifest://<hex>`。

### 2.1 `flatten-ctl export`

```
flatten-ctl export --input <path|-> --output <path|-> [flags]

  --input <path|->        docker-archive tar;`-` = stdin(默认)
  --output <path|->       EROFS 输出路径;`-` = stdout。--upload 关闭时必填,
                          --upload 开启时可省(产物默认丢弃,只要 manifest key)
  --upload                展平后把 EROFS ingest 进 store,stdout 打印 manifest
                          key(同时给 --output 则文件也写)
  --manifest-config <path>  manifest 配置 YAML(覆盖 MANIFEST_CONFIG env);
                          --upload 必需
  --tmpdir <D>            每次运行的临时工作目录的父目录(默认 $TMPDIR 或 /tmp);
                          /tmp 太小、镜像很大时改到大盘
  --no-progress           禁用 stderr 进度输出
```

典型用法:

```bash
# docker save 管道 → 单文件输出
docker save myapp:v1 | flatten-ctl export --input - --output my-app.erofs

# 本地 docker-archive 文件
flatten-ctl export --input ./my-app.tar --output my-app.erofs

# 展平后直接入库(stdout 即 manifest key)
docker save myapp:v1 | flatten-ctl export --input - --upload \
    --manifest-config manifest.yaml > app.key

# /tmp 不够大时把暂存挪到大盘
flatten-ctl export --input ./big.tar --output big.erofs --tmpdir /var/tmp
```

### 2.2 `flatten-ctl verify`

```
flatten-ctl verify --input <path|-> [--tmpdir D] [--no-progress]
```

对同一输入展平两次,比对 sha256,确认字节级确定性。stderr 输出:

```
Pass 1: sha256:a1b2c3... (1.2 GiB)
Pass 2: sha256:a1b2c3... (1.2 GiB)
DETERMINISTIC
```

### 2.3 `flatten-ctl info` — 检视镜像

```
flatten-ctl info --input <path|manifest://hex> [--json] [--manifest-config <path>]

  --input <path|manifest://hex>  EROFS 文件路径,或 manifest://<hex>
  --json                         机器可读 JSON 输出(默认人类可读)
  --manifest-config <path>       manifest 配置 YAML;`manifest://` 输入必需
                                 (经 cache-ctl + store-ctl 拉回再读 superblock)
```

读 EROFS superblock 拿 image size,从末尾 ZIP 解出 OCI runtime config 并
打印。

人类可读输出示例:

```
$ flatten-ctl info --input my-app.erofs
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
flatten-ctl info --json --input my-app.erofs | jq '.config.Entrypoint'
```

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

`mkfs.erofs` 在 `make build` 时由 `make deps-erofs` 构建到 `bin/<arch>/`,
flatten-ctl 启动时优先在自己的同目录查找 `mkfs.erofs`,其次走 `PATH`。

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

### 5.5 不实现的功能

- **OCI layout 直读** —— 上游可用 skopeo 做格式转换,flatten-ctl 不重复
  这部分基础设施
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
- [`build.md`](build.md) —— `make deps-erofs` 构建 mkfs.erofs;
  `make build` 把 flatten-ctl 与 mkfs.erofs 一并打到 `bin/<arch>/`
- `PROPOSAL.md` §10.4 —— 展平在系统中的位置与目标
