[English](README.md) | [简体中文](README_zh.md)

# Accelerator E2E 测试

本目录是 accelerator 负责的端到端测试，需要组装后的平台二进制集合。它调用真实的 flatten、manifest、store 和 cache API，并生成真实 EROFS。组装测试时必须保留整个 `test/e2e` 目录（包括 `lib/`）；fixture 不依赖任何私有源码。

在 accelerator 源码目录运行：

```sh
make test-e2e-scripts                          # 离线脚本/fixture 回归，不编译工具链
make test-e2e E2E_BIN=/path/to/assembled/bin/x86_64
# 也可直接运行组装后的测试：
BIN=/path/to/assembled/bin/x86_64 bash test/e2e/run_all.sh
# 仅运行 flatten/manifest 测试：
BIN=/path/to/assembled/bin/x86_64 bash test/e2e/e2e_manifest.sh
```

`E2E_BIN` 默认指向同级平台仓库的 `bin/$(TARGET_ARCH)`；直接运行 `run_all.sh` 必须设置 `BIN`。二进制应为适配测试主机架构的 Linux 本机版本。`make test-e2e-scripts` 需要 Python 3.9+、Bash 及下列常规工具，不需要组装后的二进制、Docker、Redis、sudo 权限、Go 或原生编译。它测试端口租约，并在明确替换 CLI 边界的情况下验证 fixture 和生命周期。这些边界测试不会完成 E2E 的编号断言，不能作为真实 E2E 通过的证据。`make test` 也包含这些回归，同时保留原有 RocksDB/单元测试的编译要求。

真实套件的要求：

- Linux、支持 `/dev/tcp` 的 Bash 4+、Python 3 标准库、OpenSSL、GNU coreutils/findutils、GNU grep（含 `-P`）、awk、sed、tar、unzip，以及 util-linux 的 `flock`；使用 Makefile 入口还需要 GNU Make。脚本使用 Linux 稀疏文件操作和 GNU 命令选项。
- 组装后的 `BIN` 包含可执行的 `flatten-ctl`、`manifest-ctl`、`store-ctl`、`cache-ctl`。使用正常启用 RocksDB 的 cache 二进制；`no_rocksdb` 不是默认设置，也不能替代必需覆盖。
- 支持去重和分块布局（`-Ededupe --chunksize=4096`）的真实 `mkfs.erofs`。查找顺序为 `MKFS_EROFS_PATH`、解析符号链接后的 `flatten-ctl` 所在目录、`PATH`。manifest 测试在启动 store 前检查该工具。
- flatten export 需要 root 或非交互式 `sudo -n`，以保留镜像层的 UID/GID。套件其他操作可使用普通用户。manifest 测试仅向 export 传递隔离的 HOME/Docker 配置、自有临时目录和解析后的 EROFS 工具路径，不使用 `sudo -E`。
- 必需的 Redis local/tiered/sharded 用例需要 `PATH` 中的 `redis-server`，或用 `REDIS_SERVER` 指定可执行文件。套件自行启动一次性的本地 Redis 进程，无需外部 Redis 服务。
- 可写且支持真实稀疏文件/`SEEK_HOLE` 的临时文件系统，有空间存放镜像和 store 副本，并允许本地回环 TCP 端口及 Unix socket。现有 cache/端口/rolling 脚本使用 `/tmp`；manifest 脚本遵循 `TMPDIR` 创建独立目录。其 store 就绪检测最多等待五秒，超时打印 store 日志并失败；这是启动边界而非性能断言。清理时向自有 store 发送 TERM，最多等待三秒，再 KILL 并回收进程、删除工作目录。INT/TERM 分别保留退出码 130/143。

`run_all.sh` 始终按以下顺序运行必需用例：

| 脚本 | 必需验证意图 |
| --- | --- |
| `port_lease_test.sh` | 128 次快速分配及 128 次并发分配的本地端口租约均不重复。 |
| `e2e_cache.sh` | Store 往返；embedded 和 Redis 的 local/shard/tiered 缓存；EC 读取、节点故障、上游填充/回写、慢节点取消、重启/回填及不访问 origin 的热读取。 |
| `e2e_store_cache_listen.sh` | Store 内嵌只读 cache 协议、generation 切换、旧 manifest 读取、blob 命名空间及拒绝写入。 |
| `e2e_cluster_rolling.sh` | 通过 SIGHUP 滚动修改 EC 成员，利用存活节点重建，且不回退到 origin。 |
| `e2e_manifest.sh` | 下列全部 12 个编号 flatten/manifest 用例。 |

Manifest 用例保持为：(1) flatten、尾部配置 ZIP、架构及真实 EROFS magic；(2) store/load 字节一致；(3) 重复存储时新增 chunk 为零；(4) 远端 manifest info；(5) verify；(6) 可重复的 get-manifest；(7) get-manifest/info 流式管道；(8) 跨镜像共享 chunk 的 diff；(9) fixed 与 CDC 的 diff；(10) 管道 stdin store/load；(11) 零块往返、至少 100 个 zero chunk、info 数量一致且 manifest 不超过 12 KiB；(12) 稀疏 hole 元数据及恢复后的字节一致。现有编号断言和 `Results: N passed, N failed` 输出格式仍是测试契约。

默认情况下，`lib/manifest_fixture.py` 仅用 Python 标准库生成两个可重复的 Docker 格式归档。每个归档包含两层：共享基础层提供 4 MiB 确定性的高熵内容，第二层用不同的 1 MiB 文件替换基础版本。时间戳、tar 元数据、配置摘要及 layer diff ID 固定且一致。数据镜像含 Linux/amd64 架构、运行时用户/环境/工作目录设置，以及 UID/GID 1000 的文件所有权。它们用于数据测试，不提供容器启动命令；即使测试主机为 arm64，镜像架构也只作为元数据处理。共享内容足以跨越多个 CDC 和 fixed 分块边界，不依赖大量零字节获得简单去重。归档直接输入 `flatten-ctl export`，默认流程不需要 Docker 或访问 registry。

显式设置 `IMAGE_A` 和/或 `IMAGE_B`，可用已在本地缓存的 Docker 镜像替换相应归档：

```sh
BIN=/path/to/assembled/bin/x86_64 IMAGE_A=already-cached:local \
    bash test/e2e/e2e_manifest.sh
```

仅此覆盖方式需要 Docker 及默认本地 daemon 的访问权限。脚本在启动 store 前 inspect/save 所有指定镜像；缓存缺失或 daemon 不可用会清晰失败。它不会 pull、打 tag 或删除镜像。未指定的另一个镜像继续使用生成的 fixture。Docker 使用全新且会清理的 HOME/`DOCKER_CONFIG`，忽略调用者凭据、context 和连接环境；调用者 tag 保持不变。

除非显式设置 `OBS_E2E=1`，否则 `e2e_obs.sh` 被排除。它是独立的、需要凭据的云测试，需要获授权的 OBS/S3 兼容 endpoint、bucket 和凭据（`OBS_BUCKET`，以及 endpoint/region/AK/SK 覆盖或脚本约定的 `~/.obsconfig` 自动发现），还需允许在独立 prefix 下创建、列举和删除对象。具体输入见 [OBS 脚本](e2e_obs.sh)。排除 OBS **不代表完成云端资格验证**；普通/离线运行不需要云凭据或云 API。

共享 runner 路由、验证 profile 和上线状态统一见[平台 CI 契约](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci_zh.md)。本文负责说明 accelerator 套件的要求与用例意图。
