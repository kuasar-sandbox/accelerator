[English](image-preparation.md) | [简体中文](image-preparation_zh.md)

# 镜像制备：来源、布局与完成条件

Accelerator 维护 `pkg/flatten`、`pkg/image`、`pkg/remote` 中共享的 OCI/目录/归档制备库。本篇定义来源生命周期、镜像 payload、转换政策、Plan 和完成契约。[flatten-ctl 用法](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli_zh.md)归发布该 CLI 的 guest-runtime；通用稀疏数据、加密和载体由[数据契约](artifacts_zh.md)定义。

## 库入口与消费者责任

`PrepareImage`、`PrepareArchive`、`PrepareDir` 选择并制备不可变逻辑内容，返回的 `PreparedImage` 持有选定来源、layout、Plan 和 FD，直到输出结束并由 Close 等待工作退出。调用者负责执行资源、输入稳定性、目标 writer 和结果发布。以下章节定义精确边界；只写完 payload 不等于工件发布成功。

## 镜像 payload 与运行配置

### 字节布局

外层 `.img` 是 tarstream 工件,含一个 `image` payload 和 `.kuasar.digest.<hex>`
marker。以下偏移均相对于**解封后的 payload**:

| payload 偏移 | 内容 |
|---|---|
| `[0, erofs_end)` | 自描述 EROFS 文件系统;偏移 1024 的 superblock 给出 image size。 |
| `[erofs_end, payload EOF)` | STORED 无压缩 ZIP,含 OCI runtime config 投影 `config.json`。 |

两段不改变彼此的格式:

- **EROFS reader**:在 payload 偏移 1024 读 superblock,用 `blocks << blkszbits`
  确定文件系统边界,不把尾部 ZIP 当文件系统内容。平台 vhost-user-blk reader 先
  提供 payload 视图;直接 `mount -t erofs` 必须使用提取后的 payload,不能传外层 tar。
- **ZIP reader**:从 payload EOF 反向扫描 EOCD;ZIP 内部 offset 相对于 ZIP 自身
  起点,支持前缀数据的 reader 会处理 EROFS 前缀。直接 ZIP 工具也应接收提取后的 payload。

`erofs_end = blocks × (1 << blkszbits)`,由 superblock 解析。EROFS 有明确的磁盘
字节序,支持跨 Host 架构构建;Guest 仍需支持所用 EROFS 特性,应用二进制仍需匹配执行架构。

```bash
# 使用直接文件系统/ZIP 工具前,先解封工件
flatten-ctl tar extract -f my-app.img image:my-app.erofs
unzip -p my-app.erofs config.json
```

Image 打包使用 `accelerator/pkg/tailzip` 分离 EROFS payload 与配置后缀，再写入 tarstream envelope。声明的 payload commitment 覆盖 EROFS prefix，后缀使用独立的 metadata commitment。新生成器的 EROFS 布局和摘要可以变化；确定性配置 ZIP 及封装规则保持不变，已有工件按原有声明身份读取。

### config.json schema

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

### 跨次确定性来源

ZIP 部分:

- 单 entry,文件名固定 `config.json`
- 无压缩(stored,内容直接可见)
- 修改时间固定为纪元常量 `1980-01-01 00:00 UTC`,绝不用挂钟
- 确定性 JSON 序列化:map keys 按字典序、slice 顺序保留(Env/Cmd/Entrypoint
  语义需要)、零值字段抑制

EROFS 部分见 [布局与元数据政策](#布局与元数据政策) 元数据归一化。

相等的运行时投影产生相同的 JSON 与 ZIP 字节。**仅 config 相同不能推出整镜像
hash 相同**:rootfs 数据/元数据、构建工具/参数,以及工件稀疏图和容器编码也须
一致。比较 payload hash 或 manifest key 时应保存这些输入。

## 制备与输出生命周期

### 输入与层生命周期

`remote.Stream` 在打开层之前固定平台 manifest、config、descriptor 和对应 DiffID。
每次 `LayerInput.Open` 必须为同一不可变身份返回从零开始的新 body；prepared 操作
拥有它，精确关闭一次并检查 Close 错误。对于受支持的 layer media type，实际存储字节决定 plain/gzip/zstd 压缩格式，媒体标签不能覆盖内容识别；stored-byte checker 只计量和哈希一次嗅探前缀，共享 detector 也处理前导 zstd skippable 帧。
tar EOF 不等于流结束：有界尾部读取覆盖完整未压缩 DiffID、压缩 size/digest 和
decoder trailer。错误、超限或取消停止输入，不无限 drain。全部层成功前不输出工件。
`remote.Pull` 保留完整下载、可重开的 Source 合同；旧 `Build(Source, ...)` 仍接受
未压缩、可重开的层。

`PrepareArchive` 校验外层成员、offset/length、结束标志及索引预算，按 manifest
顺序开启层。可 seek 输入原位读取，管道只 spool 一次。`PrepareImage`、
`PrepareArchive`、`PrepareDir` 返回的 `PreparedImage` 持有选定源、layout、计划和 FD。
旧 `Flatten` 借用 reader；`PrepareArchive` 拥有并关闭传入 body。
选定的 config 和层必须是普通成员。既有 Skopeo 旧格式别名
`<hex>/layer.tar -> ../<diff-id>.tar` 仅在目标为选定普通层时接受，只建立索引，
不跟随或提取该链接。其他外层链接和 sparse 成员均拒绝。

### OCI 应用与稳定源

Linux 适配调用 **umoci v0.6.0** 层 API，不引入其 CLI、CAS 或 runtime bundle 生成。
保留数值 UID/GID、mode、普通目录和类型替换、implicit parent、硬链接身份及特殊文件。
迟到的 `.wh.<name>`、`.wh..wh..opq` 只影响下层条目，不删除同层新增条目。
权限不足明确失败。受支持的层 xattr 在私有 rootfs 中应用并核对；最终输出政策见 [布局与元数据政策](#布局与元数据政策)。
半应用树整体废弃，不原地重放同层模拟回滚。

私有 rootfs 在层应用后停止写入。已有目录须由调用者在准备和输出全程保持稳定；
选集和摘要只能检测变化，不能把并发目录变成原子快照。规划、读取和最终校验共享
同一选集，包括 `Skip`、`SkipMounts` 及全部自有 scratch/spool/layout/输出排除。
读取产生的 atime 和排除区域内部变化不导致失效；纳入选集的元数据、链接、对象及
目录成员变化会导致失效。

### 布局与元数据政策

`accelerator` 使用固定版本的外部模块 `github.com/kuasar-sandbox/go-erofs`，复用其
目录、inode/NID、硬链接、superblock、块分配和紧凑单设备 chunk 索引。调用方沿共同
选集按确定顺序扫描，以约 1 MiB 缓冲逐 4 KiB 计算完整 SHA-256 去重，并顺序写出
数据来源记录。库只规划并输出结构元数据，不暂存 payload。raw 与 artifact 共用
同一 KERPLAN v1 布局；新旧生成器不要求字节相同，新生成器自身保持确定性。

| 字段 | 处理 |
|---|---|
| 时间与 UUID | 零时间与全零 UUID 固定输出身份，不修改调用者目录。 |
| UID/GID/mode | 保留源的数值属主和权限位；层应用权限不足失败。 |
| inode、遍历、硬链接 | 共同选集的确定性排序和真实共享 inode；内容去重独立进行。 |
| xattr | 最终 EROFS **不输出 xattr、file capability、ACL 或安全标签**，即使它们已成功应用到私有 rootfs。 |
| 压缩与设备 | 不压缩、单设备、4 KiB chunk，无隐式 raw/data-blob fallback。 |

内部 Plan schema/能力、`image.ConversionSemantics` 和制品身份分别管理。Plan 声明
逻辑长度、源身份/路径、有序目标和源 offset、length、SHA-256 及 padding。Reader
拒绝未知能力、溢出、数量/字节超限、缺源/截断、重叠和越界。

库的 `LayerLimits.MetadataBytes` 和 `PlanLimits` 字段是可选准入配额。零值不再
额外限制层元数据、规划、落盘 Plan 或索引；整数/分配表示范围及格式边界检查仍然
生效。CLI 使用这些零值选项，因此增加 Builder 资源不再被这些固定的层/计划配额
抵消，不需要新的 CLI 参数或 Builder API。执行资源仍是外层内存/存储边界；独立
调用方必须约束自己的执行环境。这不保证任意大的输入都能完成。

显式正值保留原有保守计费。兼容字段名 `NativeMemoryBytes` 限制 Go 规划器的受控
分配，分给库和调用方去重/硬链接表，各占一半；扩容计入新旧存储并存。
`PlanBytes` 限制计划文件及结构元数据；`IndexMemoryBytes` 保留选集、reader 和
writer 的份额。原先的一百万来源上限改为可寻址来源切片边界。这些估算不是进程
RSS 硬上限；GC、栈及分配器残留还须计量。解码器、归档、路径、摘要和完成条件
检查仍然生效。

### 顺序输出与接收条件

规划完成后释放对构建索引的引用，再开始 emit；不强制全局 GC，也不承诺内存立即归还系统。layout 中计划 payload 尚未物化的洞标为
**Data**；`RunAt` 是可重复的纯元数据查询，不消费游标、不扫描零字节推断洞。
每个 payload 块进入 writer 前核验计划摘要。现有 runtime-config 投影、tailzip 和
加密 writer 保持不变。

可选 tarstream 最终校验钩子在 payload 完成后、carrier digest marker、完整尾和
加密终结记录之前执行，空源和全 Hole/Zero 源也执行。Prepared 输出用它重验选集和
Plan/layout 身份。接收方必须同时要求完整工件身份/尾以及 producer/ingester 成功，
才能 Ready 或写回转换结果；仅收到 payload 字节不表示成功。

显式 `WriteRaw` 使用同一 Plan；正常 CLI 不生成 raw 暂存。命名 raw 和 artifact
均使用 pending 文件原子提交。rename 是提交点，发生在输出/文件关闭、源校验及源资源
清理成功之后。后续空暂存目录清理不能撤销提交：CLI 向 stderr 告警；raw 文件 API 在
提供 `Options.CleanupWarning` 时同步调用它（nil 时静默），已提交文件仍返回成功。
`Close` 取消并等待活动工作，然后释放拥有的资源；
调用者提供的任意输出 writer 须自行让阻塞的 Write 返回。

共享 Go 生成器运行时不查找 mkfs，`MKFS_EROFS_PATH` 不选择该生成器。
独立的 Guest-runtime 仓库拥有 `make flatten-ctl`（静态 Go CLI）与 `make build`
（包括使用宿主预安装 erofs-utils 打包 runtime bundle）。这些命令在该仓的
[构建指南](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/build_zh.md)
定义，不是 Accelerator Makefile 目标。项目不再编译 mkfs 或维护其补丁；
独立 fsck/dump 校验使用宿主工具，历史 native 性能对照保留其原始源码版本。
没有隐式旧生成器或高空间 fallback。回退应恢复上一组源码/制品及对应转换语义版本。

### 摘要资源与输出顺序

块摘要计算使用已计入预算的 1 MiB 读取缓冲和固定摘要数组，最多两个 worker，
并受 `GOMAXPROCS` 限制。复用缓冲或取消清理前必须等待全部 worker 结束；
去重与布局分配仍按顺序执行。相邻块只有在全部 4096 字节比较相同后才复用
完整 SHA-256，worker 数量不改变块身份与输出顺序。

### 准入配额与转换兼容

`LayerLimits.MetadataBytes` 和三个 `PlanLimits` 字段改为显式启用的准入配额。
零值不再隐式施加原先 128 MiB 层估算或 512/512/128 MiB 规划/计划/索引默认值。
内部将零值计划配额归一化为受检查的整数/分配表示上限，不会预先分配相应大小。
原先的一百万来源上限改为可寻址来源切片边界；生产端和读取端继续检查字节数、
整数溢出和格式。显式正值保留原有保守计费及阶段份额，包括选集累计计费和表扩容
重叠。不新增 umoci 路径镜像表或选集生命周期计费模型。

回归跨越原先 6 GiB 唯一块及来源索引门槛，验证显式配额拒绝，并覆盖来源计数/
费用溢出和计划损坏。源变化、路径、摘要、解码器、归档及工件完整性检查保持不变。
Builder Guest/执行资源仍是外层内存边界；独立调用方必须自行约束执行环境。
这些估算不是总 RSS 限额，零配额也不保证任意大输入都能完成。
固定跨架构 golden 包含 tarstream 格式变更引入的 presence 载体。

payload 记录按来源首次出现顺序写入既有 KERPLAN v1 文件，不创建完整数据 blob
或 raw 中间文件。转换 lookup/writeback 使用 `umoci-go-erofs-no-xattr-v1`。层条目按
umoci `CleanPath` 规范化为根相对路径后再计费，并跳过指向解包根目录的条目，
根条目处理与 containerd/Moby 一致。原始 `..` 路径段遵循 umoci 的根内规范化，
不再单独拒绝；链接目标仍由 umoci 处理。镜像层输出根目录固定为 `0755`、UID/GID `0:0`，不受构建进程的 umask、身份或父目录
setgid 继承影响；目录来源输出保留其源根目录元数据。本次兼容性修复保留 v1，
既有 v1 转换缓存仍可复用，不会为了规范化根目录元数据而重新生成。缺失或不同的
政策值视作 miss；显式指定的旧工件和压缩层缓存仍可使用。Builder 与 Guest 必须
选择相同的转换政策。

## 层缓存与转换记录

共享 `pkg/remote` 库管理压缩层缓存和 OCI Referrers 转换记录，二者是不同身份与生命周期：层缓存保留源字节，转换记录引用已发布的 Manifest。CLI 的配置、命令和可见结果见 [flatten-ctl](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli_zh.md#远程拉取本地缓存与-referrers-回写)。

**本地缓存**：标准 OCI layout，包含 `oci-layout`、`index.json` 与 `blobs/<algo>/<hex>`。只有显式 `cache.dir` 才暂存压缩候选层。当前层先
取得预算，下一层完整 descriptor 能装入剩余字节预算时才预取；超预算层直接 stream
bypass，`cache.required=true` 则拒绝。EOF 不发布命中：完整压缩 size/digest、解压
DiffID/trailer 和 Close 全部成功才提交；命中仍在应用时重验。重复写入保留已有 inode。
读写 lease 防止 GC 删除活动数据；超过宽限期的遗留 pending 可回收。完成态缓存不随
任务删除；`max_size` 是 LRU 目标，不是即时文件系统配额。

artifact_type 固定为常量 `application/vnd.kuasar.flatten-manifest.v1`(不可配置,保证跨工具/版本一致)。

referrer 注解:

```
vnd.kuasar.flatten-manifest.owner    = <hmac-hex> <referer_desc>
vnd.kuasar.flatten-manifest.id       = <manifest_id>                        # ingest 产出的内容键
vnd.kuasar.flatten-manifest.conversion = umoci-go-erofs-no-xattr-v1
vnd.kuasar.flatten-manifest.valid_at = <import RFC3339>[ <expiry RFC3339>]
```

`lookup` 严格校验 `valid_at`。缺失或格式错误的时间、未来的 import 时间、已经到期
或早于 import 的 expiry 都作为 miss 跳过;存在多条有效记录时返回 import 时间最新者。

转换版本的接收与兼容边界见[准入配额与转换兼容](#准入配额与转换兼容)。记录以选定平台镜像 digest 为 subject，必须与源镜像位于同一 repository。`owner` 是调用者提供的不透明作用域；库不把 tenant `MANIFEST_KEY` 写入记录。宿主生成 owner token、选择上传与写回失败政策，Guest CLI 只接收其 `--owner`。

## 设计取舍与支持范围

### 为什么是 EROFS 而非 ext4 / squashfs

- **只读、不可变 base**:EROFS 适合内容寻址 chunk 和可复用 backing 数据。
- **单一 payload**:适合单工件传递、manifest 上传和块设备访问；CLI 从稳定源和
  Plan 顺序输出工件，不需要完整 raw 临时文件。
- **可移植磁盘格式**:可跨 Host 架构构建;应用仍须匹配实际执行架构。
- **squashfs 对比**:平台选择不压缩、chunk 布局的 EROFS 以配合数据/去重路径。
  squashfs 也可共享不可变 backing 数据,这里没有普适的密度劣势证据。
- **ext4 对比**:平台用 ext4 提供可写 upper。可变文件系统跨 sandbox 使用需要
  隔离/COW 策略,并有 fsck 等维护事项。这说明当前 EROFS base / ext4 upper
  的职责划分,不能当成所有方案之间的密度基准结论。

### 为什么离线合并而非运行时 overlayfs

离线合并省去每个 sandbox 单独组装原始容器层栈的步骤。平台经数据路径提供
一个不可变 base;可写沙箱仍可在 Guest 内挂 overlayfs。此方案并不是把一个
Host EROFS mount 当作多个 Guest 的挂载,本文也没有据此建立 Host mount 数量
与密度上限的定量关系。

代价是源镜像更新时多一次构建。镜像构建可在 CI/CD 流水线中完成一次,供多个
sandbox 复用,摊销该成本。

### 为什么 config.json 内嵌而不是边车文件

外加 `app.erofs.config.json` 边车文件的方案有两个问题:

- **拷贝 / 上传分裂**:用户必须记住"两个文件一对",`scp` 漏一个就坏
- **manifest:// 无副车机制**:manifest 是单一 reader,边车文件需要单独的
  manifest key + 单独的拉取流程

ZIP-at-end 使单个交付工件里的文件系统 payload 自描述。`manifest-ctl store`
可一次 ingest 完整内容,沙箱从同一 payload 读取启动配置。EROFS 自描述边界与
ZIP 的 EOCD 反向扫描允许两种内部格式共存;外层 tarstream 提供平台工件信封。

### 为什么投影而不是原样转发

OCI image config 字段繁多,大量与启动无关:`created` / `author` / `history`
是构建元数据,跨实例不同;`rootfs.diff_ids` 是层 hash,展平后不再有意义;
`OnBuild` 是构建指令而不是运行时设置。原样转发会:

- 破坏跨次确定性(timestamps 漂移)
- 让 manifest dedup 无谓波动(每次构建的 history 不同)
- 混淆构建指令与运行时配置的边界

显式投影把"运行时启动需要什么"作为唯一标准,下游可预测。

### 不实现 / 暂不实现的功能

- **任意 OCI layout 目录直读**:支持 registry 拉取([远程拉取、本地缓存与 Referrers 回写](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli_zh.md#远程拉取本地缓存与-referrers-回写)),缓存也使用 OCI layout,
  但不把任意该格式目录解码为 OCI 镜像。可先用
  `skopeo copy oci:./dir docker-archive:x.tar` 转换,或用 crane 推到 registry 后拉取。
  直接传普通本地目录会选择 rootfs 导出,不会识别 OCI layout。
- **展平阶段的镜像签名/加密**:加密与内容寻址由
  [manifest 阶段](transfer_zh.md)
  负责;这不构成镜像签名校验流程。
- **层级保留**:输出把各层合并为一个 EROFS payload。manifest chunk dedup 可复用
  不变内容,但不保留原始层结构。

## 性能与验证证据

构建工作量通常随输入字节数和文件数增长,主要成本包括下载、层解压、文件系统
条目应用与 Go EROFS 规划。缓存状态、元数据分布、存储、CPU 和工具版本均影响结果。
追加单个无压缩 ZIP entry 相对典型镜像构建较小,但仓库没有建立普适的
1 GiB/3–5 秒或 ZIP 小于 10 ms 保证。

确定性自检:用相同配置/工具链对不可变输入导出两次,按目标比较 payload/工件
hash,或复现 manifest key。单元测试覆盖确定性转换;`verify` 子命令已移除([命令行接口](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli_zh.md#命令行接口))。

`flatten-ctl info` 不解压或读取整个 EROFS,只经对应 payload reader 读 superblock
(128 字节)、扫描 ZIP 尾部并解析单个 stored entry。manifest 路径可能触发网络/
chunk 拉取,不能保证普适的亚毫秒延迟。

### 同条件展平测量

纯 Go 生成器使用 `--variant NAME,CLI`，省略原生工具。`--allow-layout-differences` 只允许不同生成器的制品身份不同；既有内容、元数据、硬链接和运行配置核验不变，各生成器重复构建仍须字节一致。`identity-comparison.json` 记录每个变体的轮数及重复身份核验结果。单轮资源测量仍可使用，但明确标为 `not_checked_single_trial`，不能作为确定性证据。比较重复输出至少需要两轮；性能验收仍保留三轮交替对照要求。

`test/scripts/bench_flatten.py` 使用预先构建的生产工具、真实 stdout pipe 和当前
artifact reader，默认交替顺序执行三轮。记录工具摘要、进程树采样 RSS、scratch
实际分配块/inode、已删除但仍打开的文件，并等待自有生产会话中的全部进程组退出后清理 scratch。
接收 artifact P 与可选 fsync 分开计量。`post_eof_teardown_seconds` 记录生产进程完成后，
经过自有进程/线程清理，直到接收方开始校验的区间；`receiver_validation_seconds`
记录接收方校验区间。`accepted_seconds` 仍为包含两者的完整耗时。旧记录没有
`post_eof_teardown_seconds` 时，`receiver_validation_seconds` 包含上述两个区间，
不能事后拆分。可选 `--progress-timing` 为既有 CLI 进度行与
stdout 首次可读数据标注时间，辅助阶段归因；准备完成后 writer 才能输出 artifact 头。部分宿主不提供 child rusage I/O 计数；
采样 RSS 与块峰值受记录的采样间隔限制。

历史字段 `scratch_blocks`、`deleted_open_blocks` 和 `artifact_blocks` 的数值是已分配的**字节数**（`st_blocks * 512`），不是块个数。为保持兼容，保留字段名和数值含义；新增 `metric_units` 将这些字段及进程 RSS 标为字节，将 `child_io_input_512blocks` 和 `child_io_output_512blocks` 标为 512 字节块的个数。单次采样和对应峰值采用相同单位。

只有所有轮次成功且通过所选身份政策后才写出汇总 `results.json`。各生成器重复构建的身份必须一致；只有显式启用语义对比选项时，才允许不同生成器的身份不同。被拒绝的对照保留各轮诊断，不生成成功汇总。

接收成功或缓存已核验 artifact 身份之前，当前 consumer 的 `info --json` 配置必须与
`--expected-config` 提供的固定投影 JSON 相同。仅当夹具明确不含追加配置时使用 JSON
`null`。记录包含预期配置摘要、保存的 consumer 输出，以及 consumer 和 fsck 的解析后
路径和 SHA-256 摘要。旧记录若缺少这些核验或工具摘要，仍属不完整证据；不得从生产工具
摘要或 artifact 身份推断补齐。

每轮记录生产工具配置的解析后路径、SHA-256 和文件身份（依次选择 `--config`、`FLATTEN_CONFIG`；null 表示默认配置）。发布成功结果前再次核验，配置被修改、替换或删除时本轮无效，不将配置正文复制进证据。输入仍须由调用者保持不变；检查不会生成快照，也不记录外部 Registry、代理或 CA 状态。旧记录缺少该配置证据时，不能据此证明拉取及缓存设置始终一致。

Linux 测量进程临时担任 child subreaper，仅 wait 本次自有会话中被收养的辅助进程，
将其 CPU/I/O 纳入子进程 rusage，不回收其他子进程。终止和收养子进程的等待操作在复核身份后使用 pidfd，并重复枚举会话，不向数字进程组号发送信号。测量主机须支持 pidfd 信号和等待，启动生产进程前检查该能力。旧脚本记录可能漏计孤儿辅助进程的
CPU/I/O。所有写入活动结束后，包括失败或取消路径，仅在自有 scratch 内恢复删除权限；
清理错误作为原始失败的补充说明，不替换原始错误。

解析 procfs stat 时保留进程名中的任意字节，包括无关进程的名称。进程枚举使用 procfs 的父 PID，并在核对会话首进程启动时间后纳入重新归属父进程的自有会话成员，不依赖可选的逐任务 `children` 接口。资源轮次没有样本，或所有样本均未观察到仍存活的自有进程时，明确判定测量无效，不发布零内存峰值。接收方在记录制品字节数前重试中断和正数短写；没有写入进展或存储错误会使本轮失败。scratch 遍历绑定目录 FD，并对目录项执行不跟随符号链接的相对 stat，避免目录替换为符号链接后计入 scratch 外部对象。proc 引用先绑定，再从同一个对象取得路径分类和 stat，避免句柄复用混合两个对象的信息。资源扫描保留每个已观测进程的启动时间和会话；整个引用读取完成后复核线程组，并重新枚举完整的存活线程身份集合；仅在身份集合仍一致时接受该进程的计量或错误，集合变化、已退出或被替换的线程不贡献引用样本。该检查不会将 procfs 变为原子快照，也不能捕获所有存活时间短于采样间隔的引用。scratch 遍历及停止写入后的目录清理，使用显式深度优先栈保留绑定的目录句柄，相对父目录打开子目录并核验一次身份；正常回退和异常路径均关闭扫描器及句柄。目录打开工作量随树大小线性增长，保留的句柄数随当前祖先深度增长。资源耗尽会明确报错并使本轮测量无效，不承诺用固定 FD 预算处理任意深度。回归检查操作量增长，以及成功、扫描错误和真实 EMFILE 后的句柄释放；超时只用于防止测试挂起。逐个检查存活线程的独立 FD/cwd/root 表，共享 inode 去重，共享 RSS 每个线程组仅计一次；主线程已退出但兄弟线程仍存活时，继续观测并终止该自有线程组；读取兄弟线程前后复核已记录的启动时间和会话，拒绝进程号复用造成的误计。已删除但仍打开的对象包含被删除后仍持有 FD 的 scratch 根目录，按 inode 链接数判定，不依赖 procfs 文件名后缀。同时检查进程的 `cwd` 和 `root` 引用：它们保留的已删除目录，其 inode 和已分配字节只计一次，并纳入历史 `deleted_open_*` 字段。仅由内存映射保留的 scratch inode 也须计入；无法计量的映射会使资源样本无效。清理失败时终止进度读取并保留 scratch，避免无限等待 stderr。

宽目录的块数/inode 遍历可能干扰管道接收和源文件 I/O。资源采样轮次之外，再在相同
资源、存储和缓存条件下使用 `--timing-only` 测量该影响。此模式仍核验身份和实际内容、
记录子进程 CPU/I/O 并等待自有资源清理，但将采样峰值和采样间隔记为 null。两类结果
都应保留；计时轮次不能作为内存、块数、inode 或打开文件的上界证据。

历史 #161 对照在相同条件下比较原实现、正确 applier 加旧 mkfs，以及 native Plan。
仅在该夹具记录的 native accelerator 基线独立副本上应用
`test/fixtures/flatten-old-layout.patch`，使用匹配的 guest CLI 和包含 exact-read 修复的
维护版 mkfs。保留源码提交和补丁摘要；该历史夹具不适用于当前 Go 规划器。

比较 Go 生成器时，以层应用、读取及传输行为相同的维护版 native Plan 为对照。
下例比较完整输出的语义，并检查每个生成器内部重复构建的身份。

在既有 Workbench 中使用 Python 3.11 或更新版本。输入、scratch parent 与 output root
须互不包含，output root 须不存在。Registry 输入必须固定 SHA-256 digest。提供按相对
路径索引的预期内容 JSON，其中 `.` 表示根目录：每项包含 `type`、`mode`、`uid`、`gid`、`mtime_ns`（固定输出时间政策下为 0），文件另含
`size`/`sha256`，符号链接含 `target`，特殊文件含 `device` 以及数值 `kind`（`stat.S_IFMT(st_mode)`，
区分 FIFO、socket、字符设备和块设备）。对选集内每组非目录对象（含符号链接和特殊文件）的
硬链接，以字典序最小的相对路径为规范路径，其他成员用 `hardlink_to` 指向该路径。
内容枚举在 Python 3.11 及更新版本上使用迭代方式，并报告目录读取错误；不可读子树使核验失败，不会被当作空目录。
每个首次出现的 artifact
身份都经所选 native fsck 提取，核验实际内容和元数据；额外提取在测量区间外执行。校验结束后，仅为本次自有提取目录树恢复删除所需的目录访问权限，不改变外部符号链接目标。scratch 遍历遇到权限或 I/O 错误，或无权检查自有进程打开的文件时，测量明确失败；采样期间正常消失的自有目录、进程或文件描述符不算测量失败。

```bash
python3 test/scripts/bench_flatten.py \
  --variant native,native/flatten-ctl,native/mkfs.erofs \
  --variant go,go/flatten-ctl --allow-layout-differences \
  --input fixtures/image.tar --expected fixtures/expected.json \
  --expected-config fixtures/runtime-config.json \
  --consumer go/flatten-ctl --fsck tools/fsck.erofs \
  --scratch-parent scratch --output-root results --trials 3 --fsync
```

历史 native 对照要求正常语义夹具具有相同 artifact 身份；Go/native 对照在独立核验
内容、元数据、链接和运行配置后，允许布局不同。低/高重复、多层覆盖、小文件、
受限网络与显式缓存分别使用固定夹具。通过 `--config` 使用持久缓存时，其占用与
生命周期另计，不视作临时 scratch。全链耗时包含并行重叠工作，不简单相加重叠阶段。
这些宿主测量不能证明 Guest ext4 或 Host COW 峰值；后两项须在实际 Guest/Builder
链路中计量，并覆盖接收端和自有资源清理。

### Go 生成器测量与已接受的取舍

链接进 CLI 的 Go 生成器移除了独立的运行时 mkfs 程序，但不承诺所有场景都更快。
[#178 候选对照](https://github.com/kuasar-sandbox/accelerator/issues/178#issuecomment-6177535650)
及[已接受的取舍](https://github.com/kuasar-sandbox/accelerator/issues/178#issuecomment-6177892609)
同时记录了收益和代价。其余语义、失败、确定性、静态制品及 Guest/Builder 验收要求不变。

对照使用 4 CPU、6 GiB 内存、固定输入、三轮交替运行、Go 1.26.8 和真实输出管道。
x86 宿主支持 AVX2，不支持 SHA-NI 或 AVX-512。native 对照采用相同的正确 umoci
层应用，以及维护中的 native Plan、读取和传输实现。计时与资源采样分开进行。
这些是固定夹具的测量，不是任意镜像的上界。

| 夹具 | Native EOF / 接收完成加 fsync（秒） | Go EOF / 接收完成加 fsync（秒） | 接收完成加 fsync 变化 | 生产进程及自有子进程 CPU 变化 |
| --- | --- | --- | --- | --- |
| 目录，4 GiB 低重复 | 57.198 / 61.353 | 61.919 / 62.821 | +2.4% | +22.1% |
| 目录，1 GiB 低重复 | 16.814 / 18.470 | 15.463 / 18.980 | +2.8% | +37.4% |
| 目录，4 GiB 高重复 | 11.028 / 11.072 | 1.225 / 1.257 | −88.6% | −86.8% |
| 目录，20,000 个小文件 | 1.092 / 1.662 | 1.194 / 1.802 | +8.4% | +12.9% |
| Registry，三层各 1 GiB 覆盖，初轮矩阵 | 45.460 / 48.926 | 53.600 / 55.011 | +12.4% | +24.5% |
| 同一覆盖夹具，控制回写及缓存条件的定向复测 | 41.426 / 47.384 | 42.205 / 47.591 | +0.4% | +14.3% |
| Registry，1 GiB，服务端限速 32 MiB/s | 47.988 / 50.995 | 45.092 / 51.339 | +0.7% | −2.8% |
| Registry，1 GiB，显式层缓存已预热 | 21.549 / 25.795 | 20.884 / 27.599 | +7.0% | +10.0% |

表中数值是完整逐轮测量的中位数。“接收完成加 fsync”包含接收方接受和接收端
fsync，EOF 单项不包含两者。初轮覆盖对照在规划前存在波动，因此定向复测预热相同
固定输入，并等待前轮回写结束；不增加资源，也不清除全局缓存。两组结果均保留。
生产进程 rusage I/O 计数不可用不表示没有 I/O；复测补充的 cgroup 设备计数包含
Registry、接收方、验证和清理。

4 GiB 低重复输入的采样 RSS 约从 137 增至 210 MiB，规划及索引 scratch 约从
64 增至 68 MiB；源目录和接收制品不计入该 scratch 数值。制品从 4,294,975,488
增至 4,299,173,888 字节，约增加 0.098%。Registry 夹具包含私有的 1 GiB 最终
rootfs，两版生产端 scratch 均约为 1.016 GiB，Go 约多 1 MiB。宿主测量不能证明
固定 Guest ext4 和 Host COW 目标，后两者仍需要独立的真实 VM 证据。

受测规划器使用两个哈希工作线程及外部 go-erofs 模块
`v0.0.0-20261014160540-827da6f90114`。4 GiB 低重复计时使用关闭 VCS 标记的诊断
构建；正常 Makefile 构建使用相同规划器，独立通过内容及资源检查，制品摘要一致。
正常 CLI 增加 18,909 字节，同时 runtime 移除独立的约 3.9 MB mkfs 程序。这些候选
记录不替代以真实合入源码和正式制品摘要标识的最终验收。ARM Guest/Builder 实机
例外仍明确列为未验证；原生 ARM 测试和静态包属于不同证据。

源码检查、真实挂载夹具及前置条件见[贡献指南](../CONTRIBUTING_zh.md#镜像制备验证)。[Prepared E2E](../test/e2e/README_zh.md)验证产物；包级测试或历史测量都不能建立普适的 VM/工作负载保证。
