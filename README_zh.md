[English](README.md) | [简体中文](README_zh.md)

# accelerator

首次启动沙箱，请按项目[快速开始](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart_zh.md)，用 `docker pull` 拉取 node-infra 镜像、`docker run` 启动节点，再在 Guest 中执行命令。部署持久节点或集群时，继续阅读平台部署指南并提供明确的站点配置。quickstart profile 使用专用数据卷；自定义工作负载使用已完成站点配置的实例。

独立部署组件或接入其他 runtime 时，再使用本仓指南。使用所选版本及架构配套的二进制、配置示例和指南。[Workbench](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/workbench/README_zh.md)用于源码开发、演示和集成验证。

`accelerator` 是 [Kuasar Sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox) 中面向镜像、快照和稀疏工件的数据访问、存储、加密与缓存基础组件。

它提供可复用的稀疏数据和工件抽象、本地与共享文件访问、基于 Manifest 的内容组织、文件系统与 S3-compatible store、分层缓存、完整性校验和加密、OCI 拉取、确定性 EROFS 镜像展平、按需读取与预取。

内容寻址和去重是其中的能力,不是组件的唯一用途。共享范围由部署安全域和密钥材料控制。部署可以为快照使用直接文件,为大规模分发使用 Manifest/对象存储,也可以组合两种路径。

## 推荐流程

1. **选择持久存储和可移植引用。** 节点本地工作可使用文件，共享文件工件使用一致挂载的共享文件系统，Manifest 对象使用 [Store](docs/store_zh.md)。一个节点上的路径不会自动在其他节点可用。
2. **制备或导入一次工件。** OCI/目录镜像使用已发布的 `flatten-ctl`；上传、导出、检查和验证使用 [manifest-ctl 传输](docs/transfer_zh.md)。在工件保留期间，同时保存完整返回引用、父层/依赖引用、所需 Runtime 身份以及客户密钥。
3. **从预期消费位置验证读取。** 先验证直接文件或 Store 路径，完成一次完整往返后，再增加缓存或把模板用于其他节点。
4. **按源端读取需求增加加速。** 从节点本地 `tiered` cache 加 Store origin 开始；容量和实测复用收益需要时再增加共享 EC 节点。单节点或集群不以五个 shard 服务为前置条件。内嵌 RocksDB 与独立管理的 Redis-compatible 后端均受支持。
5. **分别管理持久数据与缓存。** 备份权威字节、generation 列表、引用和密钥；观测实际 cache hit/miss、origin 延迟及内存/磁盘用量。缓存淘汰或重启不代表工件退休，Store generation rollout 也不是垃圾回收。

[首次 Store 往返](docs/transfer_zh.md#独立-store-往返)不依赖 VM、cache、KVM 或外部 mkfs.erofs 工具。平台模板发布及跨节点放置见[主项目指南](https://github.com/kuasar-sandbox/kuasar-sandbox/tree/main/docs)。

## 数据路径

| 路径 | 典型用途 | 特征 |
| --- | --- | --- |
| 本地文件 | 独立节点、本地 NVMe、临时或节点关联工件 | 直接文件语义,访问路径短 |
| 共享文件系统 | NAS、NFS 或其他一致挂载的文件系统 | 跨节点共享原生文件访问,不强制转换为分块对象 |
| Manifest 与对象存储 | 大规模分发、远端持久化、迁移和分层缓存 | 内容寻址对象、元数据描述的稀疏区段和按需读取 |

这些路径向下游暴露共同的逻辑数据与引用语义。因此 `sandboxer` 可以消费镜像或快照,不必让生命周期 API 绑定某种物理后端。

## 按完整任务或契约阅读

| 需要完成的任务 | 权威指南 |
| --- | --- |
| 导入、取回、检查、变换或比较工件 | [传输](docs/transfer_zh.md)：完整本地 Store 往返与精确 manifest-ctl 参数 |
| 实现稀疏 reader/writer，或解释引用与载体 | [数据契约](docs/artifacts_zh.md)：稀疏模型、身份、Chunk/Manifest、加密、tarstream 与 Bundle |
| 部署或嵌入权威持久服务 | [Store](docs/store_zh.md)：FS/S3、generation、可见性、管理与 App 生命周期 |
| 通过 broker 提供密钥，或开发 Secret plugin | [Secret plugin](docs/secret-plugins_zh.md)：引用、配置、缓存、密钥生命周期与插件开发 |
| 部署/嵌入加速层，诊断 miss 与恢复 | [Cache](docs/cache_zh.md)：tier、sharding/EC、RocksDB/Redis 后端与 App 生命周期 |
| 通过共享库制备 OCI、归档或目录镜像 | [镜像制备](docs/image-preparation_zh.md)：生命周期、镜像 payload、布局与发布 |

[flatten-ctl 操作](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli_zh.md)归发布该程序的 guest-runtime。每篇均有完整英文版，可通过语言选择器切换。

## 二进制

| 二进制 | 用途 | 构建方式 |
| --- | --- | --- |
| `manifest-ctl` | ingest、检查、fetch 和转换 Manifest 工件 | 纯 Go |
| `store-ctl` | 文件系统或 S3-compatible 内容存储服务及管理 | 纯 Go |
| `cache-ctl` | 本地、分片、分层和纠删码缓存服务 | 官方程序包含 RocksDB；使用其他后端的部署可选纯 Go 构建 |

`flatten-ctl` 随 `guest-runtime` 的 Runtime 发行单元发布,共享镜像展平实现仍在本仓维护。

## 存储安全

组件对进入受保护工件和 Manifest 路径的数据提供完整性校验与加密。平台身份凭据和内容保护密钥是不同的概念。密钥范围、salt 和部署安全域共同决定加密内容可以在哪里共享或去重。

内容加密不意味着任何威胁模型下都不泄露元数据,也不意味着所有租户私有数据都应全局去重。部署应根据数据分类与信任边界选择共享域。直接本地或共享文件路径也可应用项目定义的本地工件保护策略。

## 独立运行

命令行工具与 Store/Cache 服务可独立于完整平台使用：

- 用 `manifest-ctl` ingest 或 fetch 稀疏工件;
- 为 `store-ctl` 配置文件系统根目录或 S3-compatible endpoint;
- 以本地、分片或分层拓扑运行 `cache-ctl`;
- 将 `app/store.Run` 或 `app/cache.Run` 静态链接进定制服务二进制。官方 CLI 与定制程序平级调用同一生命周期函数；定制程序不 exec、也不依赖安装 `store-ctl`/`cache-ctl`。

配置示例使用本地路径、文档保留 endpoint 和占位凭据。生产部署应使用受保护的 endpoint、显式资源预算、持久的权威存储,以及适合所选缓存拓扑的可观测性。

## 公共 Go 导出面

下游 import 面保持精简,主要使用纯 Go,使 `sandboxer` 等消费者不必把存储服务端、对象存储 SDK、纠删码或数据库实现链接到自己的二进制中。

| 包 | 用途 |
| --- | --- |
| `pkg/sparse` | 权威 Hole/Zero/Data 表示与稀疏随机/顺序读取 |
| `pkg/manifest` 及子包 | 内容组织、分块、加密、ingest、fetch 和 prefetch |
| `pkg/store` 与 `pkg/store/client` | 内容存储协议和客户端 |
| `pkg/secret` | 通过本地 broker 解析密钥引用，并支持原生文件和字面值解码 |
| `pkg/secret/plugin` | 将自定义 Secret plugin 后端适配为 HTTP Resolve handler |
| `pkg/cache` 与 `pkg/cache/client` | 缓存协议、本地/分片/分层组合与客户端 |
| `app/store` 与 `app/cache` | 供官方及定制二进制复用的完整 Store/Cache 服务生命周期 |
| `pkg/tarstream` | 携带身份及完整性元数据的本地稀疏传输工件 |
| `pkg/flatten`、`pkg/image`、`pkg/remote`、`pkg/tar` | OCI/目录拉取、镜像配置与 EROFS 展平基础能力 |

重服务端后端保留在组件二进制和 server package 后面。稀疏 Run/Stream、chunk-window 与本地 tarstream 的详细契约归属于 [Manifest 稀疏读路径](docs/artifacts_zh.md#读路径细节)及[文件工件](docs/artifacts_zh.md),README 不另建一套协议。

## 构建并验证修改

在满足文档所述 Go/Native 前置条件的 Accelerator checkout 中运行：

```bash
make manifest-ctl store-ctl
make cache-ctl
make test
```

工具链前置、交叉/离线构建、no-RocksDB 选项、ABI/Native 检查、镜像夹具、包验证与完整 prepared E2E 命令见[贡献指南](CONTRIBUTING_zh.md#构建与测试)。单元或夹具通过不构成真实云或 VM 覆盖。

## 选择已发行产品

本组件发布 `vX.Y.Z`，聚合 `release-vX.Y.Z` 精确选择独立组件 Tag。通过[平台安装](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/installation_zh.md)选择发行渠道及匹配聚合版本；[Stable 聚合渠道](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest)指向稳定版本。部署时使用所选版本的发行清单及配套指南。精确工件清单、Native 材料收集与打包检查见[组件发行准备](CONTRIBUTING_zh.md#构建与打包组件发行版)。共享 Preview 保留/清理、Latest 对齐和发行线规则由[项目发行契约](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release_zh.md)统一定义。

## 项目边界

- MicroVM 生命周期与快照执行归属 [`sandboxer`](https://github.com/kuasar-sandbox/sandboxer);
- 网络分配与转发归属 [`connector`](https://github.com/kuasar-sandbox/connector);
- guest 内核、Runtime image 与 `flatten-ctl` 发行物归属 [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime);
- 节点与集群编排归属 [`orchestrator`](https://github.com/kuasar-sandbox/orchestrator);
- 系统设计、共享集成测试、Demo 和聚合发行归属 [`kuasar-sandbox/kuasar-sandbox`](https://github.com/kuasar-sandbox/kuasar-sandbox)。

## 贡献与安全

请阅读本仓[贡献指南](CONTRIBUTING_zh.md)与[组织贡献指南](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md)。公共数据格式或下游 package 契约变更需要关联 Companion PR 并执行精确源码的项目级验证。

不要在公开 Issue 中报告漏洞或真实存储凭据。请使用 [Kuasar Sandbox 安全政策](https://github.com/kuasar-sandbox/kuasar-sandbox/security/policy)和 GitHub 私密漏洞报告。

## License

项目原创内容采用 [Apache License 2.0](LICENSE)。保留源码与发行资产中原生库、vendor code、生成代码、格式及第三方工具的许可证、署名、NOTICE 和源码义务。
