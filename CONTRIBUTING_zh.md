[English](CONTRIBUTING.md) | [简体中文](CONTRIBUTING_zh.md)

# 为 accelerator 做贡献

本篇是源码修改、组件验证与发行准备的入口。公开贡献与共享 CI 政策由项目统一维护；以下组件流程说明应执行什么，以及所得证据能证明什么。

## 贡献流程

使用公开 Fork 和主题分支。贡献者无需上游写权限即可提交 issue、评审和 PR。维护者评审修改、安排受信验证并 squash merge；直接推送上游 `main` 不是贡献路径。

## 准备 Fork

把仓库 Fork 到个人 GitHub 账号，然后配置 HTTPS remote：

```bash
git clone https://github.com/<your-login>/accelerator.git
cd accelerator
git remote add upstream https://github.com/kuasar-sandbox/accelerator.git
git fetch upstream
git switch -c feat/short-description upstream/main
# 编辑、测试并提交
git push -u origin feat/short-description
```

维护修复从受支持的 `upstream/release/vMAJOR.MINOR.x` 开始，并把该分支设为 PR 目标。适用时启用 **Allow edits from maintainers**。

按以下方式推进 Fork 的 `main`，避免覆盖本地提交或未提交工作：

```bash
git fetch upstream
git switch main
git merge --ff-only upstream/main
git push origin main
```

切分支前先提交或安全保存未提交内容。若 `--ff-only` 因历史分叉而拒绝，检查本地提交并有意识地处理历史；日常同步不应 reset 或强推 Fork 的 main。每个新主题分支应从新获取的上游目标创建，不从无关功能分支创建。

## PR 要求

PR 应：

- 范围集中，清楚说明问题与解决方法；
- 有相关 issue 时提供链接；
- 行为变化时同步测试与文档；
- 不包含凭据、私钥、令牌、客户数据或无关生成文件；
- 说明已执行的验证，以及运行或兼容风险。

欢迎通过 Draft PR 提前征求意见，但 Draft 不是可合并候选。

## E2E 执行要求

每个 `test/e2e/cases/<suite>.<case>.sh` 必须在 shebang 下方以一个静态 `# e2e-requires: [...]` JSON 数组声明最低内核与服务能力，包括 helper 使用的能力；无需能力时显式写 `[]`。Owner 格式位于 `test/e2e/lib/requirements.json`，包含 `{"schema":1}`。缺失、重复或未知能力导致校验失败。正常固定源码选择会发现 case 增删和要求变更，无需平台维护 case allowlist。Runner label 与放置由受信平台配置决定，不由 case 决定。支持的名称、校验与历史输入兼容见[能力契约](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/test/README_zh.md#静态执行需求)。

## 文档

遵守[项目文档政策](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/CONTRIBUTING.md#documentation-contributions)。以 `name.md` / `name_zh.md` 维护完整英文/中文配对和互相可达的语言选择器，保留要求、标识符、示例、图表、事实和链接，记录有源码依据的修正。已有完整英文独有材料可以继续保留；摘要或语言检测通过都不等于完整翻译。

## 贡献许可

提交贡献即同意采用所修改文件适用的许可证。无其他明确声明的新项目文件按 [Apache License 2.0](LICENSE) 贡献。

仅提交有权贡献的内容。修改第三方或采用其他许可证的材料时，保留适用的 copyright、attribution、NOTICE 与 SPDX 声明。

## 评审要求

评审应具体且建设性。作者应解决评审对话，或解释建议为何不适用。维护者可以说明原因后关闭长期无进展、被替代、不安全或超出范围的 PR。

## CI 准入与合并证据

[中央 CI 契约](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci_zh.md)统一定义受信准入、workflow/status 标识符、companion、源码选择和合并证据。默认分支 wrapper 负责准入；候选 workflow 不取得控制面或发行凭据。任何贡献者均可提交公开 Fork PR，特权执行是独立信任决定。未获准入的外部候选应由维护者评审并准备保留作者与已评审源码的受信上游候选，而不是降低准入要求。

请求正常评审后的 squash merge 前：

1. 使 PR 进入 Ready，目标为 `main` 或受支持维护分支，并解决阻塞评审对话。
2. 验证当前精确 target base/reviewed head 的 integration 和必需测试；跳过、取消、失败或旧运行不构成成功。
3. 每个声明的 companion 独立验证，复核 repository、PR、base、head、target ref 与 integration 匹配记录的源码集合。
4. base/head/companion 变化后重新取得证据。只改正文不触发执行，必要时使用受支持的 Draft→Ready 事件。只有已诊断为瞬时失败且准入输入未变时才重跑旧事件。
5. 合并前再次核对实际源码集合；一个 companion 合并后，删除其过时声明，并对剩余候选重新验证。

不得制造 status、用手动 dispatch 绕过准入、削弱分支保护或在候选执行中保留 source-fetch 凭据。不使用 auto-merge、merge commit 或 rebase merge。精确当前检查和完整信任边界以中央契约为准。

## 构建与测试

构建使用环境提供的 Go，并继承 `GOROOT`、`GOTOOLCHAIN` 等工具链选择；发布自动化需要环境在 `PATH` 中提供支持 `api --slurp` 的 `gh`。项目不下载、替换或按固定二进制摘要认证这些环境工具。

```bash
make manifest-ctl store-ctl     # 纯 Go 命令行服务
make cache-ctl                  # 缓存服务及当前后端依赖
make cache-ctl NO_ROCKSDB=1     # 不带 RocksDB (-tags no_rocksdb, CGO_ENABLED=0)
make build                      # 全部组件二进制
make build TARGET_ARCH=aarch64  # 交叉编译;接受 amd64/arm64 别名
make vet                        # 检查面向下游的薄 Go 导出面
make test                       # 单元、后端及 source/helper 回归
make test-e2e-scripts           # 离线 E2E fixture/helper 回归，不是产品 E2E
```

产品 E2E 由平台统一的 prepared-workspace runner 对预构建制品执行；组件不再提供独立 owner `run_all.sh` 或 `make test-e2e` 入口。`prepare -> <suite>.<case>.sh -> run` 契约及 accelerator 用例前置条件见 [`test/e2e/README_zh.md`](test/e2e/README_zh.md)。

源码构建需要 Go 1.26.1 或更新版本，原生前置依赖随构建目标而异。当前源码通过仓库脚本说明并构建所需原生缓存依赖。真实对象存储测试必须使用显式测试凭据或本地 S3-compatible 服务;普通单元测试与本地文件系统测试不应要求生产云凭据。

`cache-ctl` 通常使用 CGO 以及 `deps/build-rocksdb.sh` 在本地构建的静态 `librocksdb.a`;`manifest-ctl`、`store-ctl` 与下游 import 面不需要 RocksDB。`make release VERSION=vX.Y.Z` 生成并检查本地发行 bundle;官方缓存载荷必须保留 RocksDB 支持。

### 私网与离线构建

EROFS 规划器依赖外部模块 `github.com/kuasar-sandbox/go-erofs`。离线构建前，应将 `go.mod` 选定的精确版本（包括适用时的伪版本）镜像或预先缓存，并按 `go.sum` 校验。可将 `GOPROXY` 指向部署镜像并使用对应 checksum database 策略，或使用已填充、经过验证的 module cache。无需检出 go-erofs 源码或配置本地替换。兄弟仓协调开发可使用[项目工作区](https://github.com/kuasar-sandbox/kuasar-sandbox)，但它不是本组件独立构建的前提。

## 构建与 ABI 要求

发布构建按原有 recipe 构建真实 RocksDB，不能以 `NO_ROCKSDB=1` 替代发布载荷。
Go 与 native 编译器由构建环境提供，保留既有工具链选择语义。
CI 在一次运行开始时选择已通过验证的 Workbench, 为原生 x86_64 与 aarch64 Runner
分别固定镜像身份. 普通构建、测试和打包使用 ordinary UID、只读根文件系统、移除
capabilities, 以及任务私有源码和缓存目录. `KUASAR_BUILD_JOBS` 将同一任务 CPU
预算传给 Go 和 RocksDB CMake 构建; 未配置预算的本地 native recipe 仍使用 `nproc`.
普通 Go RocksDB 测试保留上游绑定的压缩库链接参数，需要 Snappy、LZ4、Zstandard
与 zlib 开发库；这并不启用原有 native RocksDB recipe 的压缩功能。

Accelerator 的 [ABI 检查](scripts/ci-check-abi.py) 要求 `manifest-ctl` 与
`store-ctl` 保持静态。`cache-ctl` 保留正常 CGO/glibc 和静态 RocksDB、libstdc++、
libgcc，GLIBC 需求不得超过声明的 2.38 基线。ABI 失败阻止打包。
Native recipe、摘要、许可、来源清单与 link-map 校验仍为必需检查。
Publisher 在独立 Workbench 调用中从受信源码编译归档校验器, 该调用禁用缓存恢复和
保存. 发布步骤直接执行校验器, 不再次编译.

## Prepared 产品 E2E 与本地证据

[Accelerator E2E 指南](test/e2e/README_zh.md) 列出预构建产品、EROFS、Redis、
临时存储和权限要求，以及 accelerator 的 `basic.*.sh` / `storage.*.sh` / `image.*.sh` 产品 case。
产品 E2E 只通过平台统一的 prepared-workspace runner 执行；accelerator 不再拥有
`run_all.sh` 或 owner-suite 入口。本地 filesystem/cache 和 S3-compatible fixture
仅证明本地行为，不代表真实云覆盖。

普通无凭据验证先从精确预构建平台 release 准备 workspace，并显式选择非 OBS storage/image case：

```bash
RUNNER=/path/to/platform/test/e2e/e2e
RELEASE_DIR=/path/to/prebuilt/platform-release
WORK=/tmp/kuasar-e2e

"$RUNNER" prepare --release-dir "$RELEASE_DIR" --workdir "$WORK"
"$RUNNER" run --workdir "$WORK" \
  --include storage.cache.sh \
  --include storage.tiered-cache.sh \
  --include storage.cache-membership.sh \
  --include storage.cache-probe.sh \
  --include storage.service-drain.sh \
  --include storage.store-cache.sh \
  --include image.manifest.sh
```

`basic.accelerator-activation.sh` 单独验证 service-manager 生命周期和命名 listener 所有权. 在具备 systemd 的环境使用 `--include basic.accelerator-activation.sh`;其必需的 `["systemd"]` 声明将正式 CI 分配给既有 system executor. 实际 Store/Cache 请求用于验证恢复,存储协议/算法用例继续保持 storage 分类及 clean runtime. 自有资源和前置条件见 E2E guide.

凭据化 OBS 保持显式 opt-in，并通过 case ID 选择，而不是依赖 `OBS_E2E` marker。
明确配置 `OBS_BUCKET`、`OBS_ENDPOINT`、`OBS_AK` 和 `OBS_SK` 后，在同一个 prepared
workspace 上运行：

```bash
"$RUNNER" run --workdir "$WORK" --include storage.obs.sh
```

已选云端 case 如果无法满足 endpoint 或凭据前置条件则失败；未选择的云端 case
不能算通过。CI 使用同一个平台 runner，并选择 exact admitted case ID。

从 accelerator checkout 运行离线检查：

```bash
python3 scripts/ci-test-workflows.py ../kuasar-sandbox  # 使用实际平台路径
python3 scripts/test-native-build-budget.py
(umask 022; bash scripts/test-release.sh)
make test-e2e-scripts
```

Workflow 检查解析组件的实际 YAML，验证其分配、workspace 和 ABI 契约。
发布测试使用合成 binary/link-map fixture 与模拟 API 响应。脚本测试验证 fixture
和生命周期边界，不会完成真实 E2E 的编号断言。这些检查通过不能证明真实 RocksDB
构建、prepared 产品 E2E 或云端资格验证已通过。

## 镜像制备验证

生产规划和 Plan 测试夹具均使用固定版本的外部 `github.com/kuasar-sandbox/go-erofs` 模块，不查找或执行 mkfs。独立内容校验使用宿主 fsck/dump。Go 规划器测试验证原生工具路径无效时仍可工作；Plan 损坏、越界和成功尾回归继续执行。[镜像制备](docs/image-preparation_zh.md) 定义输入、源生命周期、计划和工件完成合同。

| 要求 | 实现 | 维护中的检查及证据范围 |
|---|---|---|
| 固定输入身份及完整校验 | `pkg/flatten/input.go`、`layer_stream.go`；`pkg/remote/stream.go` | 层 media/size/digest/DiffID/trailer/Close 与取消用例；实际本地 Registry → umoci → Plan → pipe → 当前工件 reader。 |
| 按内容识别层压缩格式 | `internal/layercompression`；流式层、Docker-save 适配器及旧 `remote.Pull` | 格式识别与解码器创建使用同一实现，支持的 media type 错标不影响 plain/gzip/zstd 内容识别，包含前导 zstd skippable 帧。覆盖错标、完整身份与尾部校验、嗅探阶段取消，以及实际 Registry `Prepare` 的无缓存、缓存未命中/命中和归档输出对照。 |
| 层文件系统语义 | `pkg/flatten/layer_apply_linux.go` | 实际 umoci 的 late whiteout/opaque、类型替换、implicit parent、链接、属性及空层；权限不足仍报错。 |
| Go 布局与顺序输出 | 固定 go-erofs 模块；`go_planner_linux.go`、`plan_linux.go` | 实际内容/元数据、选中硬链接、填充块共享、独立 fsck 提取、重复构建确定性、受控分配/Plan 预算、取消及清理。保留 Plan 损坏、越界、源变化、纯元数据可重复 RunAt 和成功尾检查。历史原生 Raw/Plan 及 exact-read 对照保留在其原始版本中；当前测试覆盖 Go 生成器。不要求 Go 与原生输出字节相同。 |
| 归档与入口等价性 | `pkg/flatten/archive_linux.go`、`raw_linux.go` | 可 seek 归档、pipe spool 与目录对比；截断/拼接归档及索引预算拒绝。Guest CLI 测试覆盖 file、stdout 和上传/失败分支。 |
| 持久缓存发布与所有权 | `pkg/remote/prepare_linux.go`、`prefetch_linux.go`、cache/GC | 未验证 EOF 不能命中；显式字节预算、bypass、当前层优先、重复摘要、并发 GC lease 和等待完成的取消。转换政策测试拒绝旧转换命中，同时保留显式旧工件读取。 |
| 稳定选集与自有资源清理 | `pkg/flatten/prepared_linux.go`、`selection_linux.go` | 容忍 atime/排除子树变化，检测纳入元数据变化；apply/output 真实 ENOSPC、bind/output 别名、保留原文件及 UID/GID/rwx/访问 ACL、继承目录 GID、特权空根 chmod、粘滞目录替换权限的提前检查、EMFILE 清理、拒绝预留文件与目录替换、只读私有目录树清理且不跟随外部符号链接，以及打开/已删除 FD 检查。 |

最后一行包含显式启用的真实挂载测试。普通用户编译测试程序，再在新的私有 mount
namespace 中以挂载权限仅运行这些用例。将 `TMPDIR` 设为私有测试目录，
`MKFS_EROFS_PATH` 指向所选 native 工具；全部挂载位于测试自有路径，在删除目录前
卸载。小容量 tmpfs 产生真实 ENOSPC，不会填满宿主文件系统。

```bash
CGO_ENABLED=0 go test -c -o /path/to/private/flatten-mount.test ./pkg/flatten
sudo unshare --mount --propagation private env \
  FLATTEN_TEST_MOUNTS=1 TMPDIR=/path/to/private/scratch \
  MKFS_EROFS_PATH=/path/to/selected/mkfs.erofs \
  /path/to/private/flatten-mount.test -test.v -test.run '^TestMounted'
```

这些测试建立本地合同。实际 runtime bundle/Guest 消费、Builder Phase A/B、静态包
校验、Guest ext4/Host COW 峰值及相同条件的全链性能仍须通过既有 prepared 产品与
Builder 测试入口，以精确 source/artifact set 验证。接收工件字节与 scratch 分开记录，
计入索引/decoder 内存及自有资源退出清理。包级测试或上述挂载夹具不能替代真实 VM
证据。

## 原生缓存验证

使用 `make test`、`make test-no-rocksdb`、`make test-release` 和原有架构/发布
目标。执行 `make deps-rocksdb` 后可运行专项测试：

```sh
export CGO_CPPFLAGS="-I$PWD/build/$(uname -m)/rocksdb/include"
export CGO_LDFLAGS="-L$PWD/build/$(uname -m)/rocksdb/lib -lrocksdb -lstdc++ -lm -lpthread -ldl"
go test -race -count=3 ./pkg/cache/rocks/...
GOEXPERIMENT=cgocheck2 go test ./pkg/cache/rocks/...
bash test/scripts/test_rocks_native.sh
```

最后一条命令需要 Valgrind，在不加载 Go runtime 的情况下测试真实 bridge 和
RocksDB 库，包括锁冲突、部分打开失败和反复打开/关闭。Go 测试覆盖 callback
生命周期、panic 恢复、并发关闭、损坏读取、只读/重开、BlobGC 和 inline/Blob
淘汰。恢复测试在正常 Close 与 SIGKILL 后分别核对已 Flush 前缀和未 Flush 尾部，
验证最新 Sketch checkpoint 的 WAL 恢复，以及 Flush 失败会返回且不遗留 DB 锁。
私有可选引擎计数器用于证明冷 Blob 过滤不读取 value；生产仍关闭 Statistics。

## 构建不含 RocksDB 的部署

`make cache-ctl NO_ROCKSDB=1` 构建同名正常 `cache-ctl` 产物，但编译掉 RocksDB 支持（`CGO_ENABLED=0`、`-tags no_rocksdb`）。只有 `pkg/cache/rocks` 的 RocksDB 实现文件带 build tag，其余代码无需感知。此构建中的 `rocks.Open` 返回 `not compiled` 错误，因此选择 `type: embedded` 的配置会在 daemon 启动时失败，错误指出该 tag。默认 `make cache-ctl` 产物不变。`make test-no-rocksdb` 在没有任何 `librocksdb` 的条件下编译、vet 和测试该带 tag 的导出面。

## 构建与打包组件发行版

所选 owner 源码在 `release/helper-inputs.json` 显式声明编译型 E2E helper 清单。当前清单为空。后续 helper 及其构建配方由此 owner 声明维护，聚合打包自动发现，无需平台文件名清单。

生产 flatten 规划与 Plan 夹具使用选定的外部 go-erofs 模块，不查找或执行 mkfs.erofs。发行流程冻结本组件获准的源码，不再依赖 guest-runtime 测试源码。每个原生架构在选定 Workbench 中使用真实 RocksDB 构建和打包组件；x86_64 另执行 `make test`、`make vet` 与 `make test-release`。独立文件系统校验使用环境中的 fsck/dump，不再执行 EROFS native-cache 恢复或 `--plan-version` 准入。Runtime 发布仍消费所选 accelerator Release。这些构建/打包检查不能替代 prepared 产品或 VM 证据。

使用组件 Makefile 从选定源码构建。`release.sh package` 使用匹配的
`bin/<arch>` 二进制,或显式指定的 `RELEASE_BIN_DIR`;只收集材料并生成 bundle,
不重新构建二进制,不重置源码或构建缓存。所选源码 checkout、依赖版本、原生
构建记录与产物应一并保留。

打包记录实际 Go 版本和生效的 module 替换。Go/module LICENSE、NOTICE 取自所选
编译器安装和匹配的 module 源码,保留嵌套路径。模块解析沿用正常 Go 缓存与路由,
下载模块的校验和须匹配二进制记录。组织命名空间本身不豁免模块的声明收集。
本组件没有内部兄弟组件依赖。官方包中不受支持的本地替换需要改用带版本的
module 输入。

许可证和声明放在 `share/licenses/<component>`。声明缺失、目录不可读或遍历不完整时收集失败。包验证检查必需文件、载荷身份及归档路径、类型和权限。下载使用 GitHub 原生资产大小和摘要。新包和 Release 不携带来源或历史测试记录、独立 `SHA256SUMS`、来源/Preview 标记。发布者使用人类说明，检查本次构建输出，并拒绝覆盖同名不同内容资产。

官方包要求三个 Linux 命令二进制匹配所选原生架构（x86_64 对应 amd64，aarch64 对应
arm64），并分别具有自身的 Accelerator main-package
身份;`cache-ctl` 必须包含 RocksDB。五个交付运维脚本取自所选源码树。
解包前拒绝额外/重复载荷、链接、错误属主或权限,以及其他组件的材料命名空间。

正常 cache 构建保留 `build/<arch>/cache-ctl.map`。打包读取该链接映射及匹配的
RocksDB 静态库/源码树;显式提供时使用 `RELEASE_ROCKSDB_SOURCE_DIR`。
记录实际 RocksDB、libstdc++、libgcc 等链接输入的摘要及 Debian/RPM 源包版本,
从对应包复制版权、许可、NOTICE 和引用的公共许可正文,不按包数据库摘要认证
已安装文件字节。RocksDB 材料必须包含 `AUTHORS`、`COPYING`、
`LICENSE.Apache`、`LICENSE.leveldb`。不同原生输入不能以相同材料名称相互覆盖;
RPM 收集要求完整枚举同源兄弟包。这些材料支持发行检视,不构成法律认证,
也不改变 RocksDB 后端或持久化策略。

本仓独立发布 `vX.Y.Z` 组件版本。已发布原生架构的组件归档包含三个服务二进制及发行合同选定的运维/性能辅助脚本。聚合 platform 包按显式文档/测试选择携带用户指南与规范 E2E 运行输入，内部设计文档和源码专用测试保留在源码仓。

项目主仓独立发布 `release-vX.Y.Z` 聚合版本,选择 `accelerator` 和其他发行单元的精确版本,验证资产并执行跨组件测试。聚合与组件版本号相互独立。

发布分支、Preview、Latest 协调及并发/取消处理以[项目发行文档](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release_zh.md)为准;普通使用可从[最新 Stable 聚合渠道](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest)进入。
