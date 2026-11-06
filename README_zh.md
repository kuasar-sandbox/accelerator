[English](README.md) | [简体中文](README_zh.md)

# accelerator

首次运行跨组件沙箱或 Demo，推荐从项目[快速开始](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart_zh.md)
使用匹配聚合版本的 [workbench](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/workbench/README_zh.md)。
本组件仍可独立构建和部署，workbench 不是生产运行的强制依赖。预构建架构以所选
发布版的实际资产为准；源码支持某架构不意味着所有历史版本都提供该架构制品。

`accelerator` 是 [Kuasar Sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox) 中面向镜像、快照和稀疏工件的数据访问、存储、加密与缓存基础组件。

它提供可复用的稀疏数据和工件抽象、本地与共享文件访问、基于 Manifest 的内容组织、文件系统与 S3-compatible store、分层缓存、完整性校验和加密、OCI 拉取、确定性 EROFS 镜像展平、按需读取与预取。

内容寻址和去重是其中的能力,不是组件的唯一用途。共享范围由部署安全域和密钥材料控制。部署可以为快照使用直接文件,为大规模分发使用 Manifest/对象存储,也可以组合两种路径。

## 数据路径

| 路径 | 典型用途 | 特征 |
| --- | --- | --- |
| 本地文件 | 独立节点、本地 NVMe、临时或节点关联工件 | 直接文件语义,访问路径短 |
| 共享文件系统 | NAS、NFS 或其他一致挂载的文件系统 | 跨节点共享原生文件访问,不强制转换为分块对象 |
| Manifest 与对象存储 | 大规模分发、远端持久化、迁移和分层缓存 | 内容寻址对象、元数据描述的稀疏区段和按需读取 |

这些路径向下游暴露共同的逻辑数据与引用语义。因此 `sandboxer` 可以消费镜像或快照,不必让生命周期 API 绑定某种物理后端。

## 公共 Go 导出面

下游 import 面保持精简,主要使用纯 Go,使 `sandboxer` 等消费者不必把存储服务端、对象存储 SDK、纠删码或数据库实现链接到自己的二进制中。

| 包 | 用途 |
| --- | --- |
| `pkg/sparse` | 权威 Hole/Zero/Data 表示与稀疏随机/顺序读取 |
| `pkg/manifest` 及子包 | 内容组织、分块、加密、ingest、fetch 和 prefetch |
| `pkg/store` 与 `pkg/store/client` | 内容存储协议和客户端 |
| `pkg/cache` 与 `pkg/cache/client` | 缓存协议、本地/分片/分层组合与客户端 |
| `app/store` 与 `app/cache` | 供官方及定制二进制复用的完整 Store/Cache 服务生命周期 |
| `pkg/tarstream` | 携带身份及完整性元数据的本地稀疏传输工件 |
| `pkg/flatten`、`pkg/image`、`pkg/remote`、`pkg/tar` | OCI/目录拉取、镜像配置与 EROFS 展平基础能力 |

重服务端后端保留在组件二进制和 server package 后面。稀疏 Run/Stream、chunk-window 与本地 tarstream 的详细契约归属于 [Manifest 稀疏读路径](docs/artifacts_zh.md#读路径细节)及[文件工件](docs/artifacts_zh.md),README 不另建一套协议。

## 二进制

| 二进制 | 用途 | 构建方式 |
| --- | --- | --- |
| `manifest-ctl` | ingest、检查、fetch 和转换 Manifest 工件 | 纯 Go |
| `store-ctl` | 文件系统或 S3-compatible 内容存储服务及管理 | 纯 Go |
| `cache-ctl` | 本地、分片、分层和纠删码缓存服务 | 使用当前源码选择的后端依赖 |

`flatten-ctl` 随 `guest-runtime` 的 Runtime 发行单元发布,共享镜像展平实现仍在本仓维护。

## 存储安全

组件对进入受保护工件和 Manifest 路径的数据提供完整性校验与加密。平台身份凭据和内容保护密钥是不同的概念。密钥范围、salt 和部署安全域共同决定加密内容可以在哪里共享或去重。

内容加密不意味着任何威胁模型下都不泄露元数据,也不意味着所有租户私有数据都应全局去重。部署应根据数据分类与信任边界选择共享域。直接本地或共享文件路径也可应用项目定义的本地工件保护策略。

## 按完整任务或契约阅读

| 需要完成的任务 | 权威指南 |
| --- | --- |
| 导入、取回、检查、变换或比较工件 | [传输](docs/transfer_zh.md)：完整本地 Store 往返与精确 manifest-ctl 参数 |
| 实现稀疏 reader/writer，或解释引用与载体 | [数据契约](docs/artifacts_zh.md)：稀疏模型、身份、Chunk/Manifest、加密、tarstream 与 Bundle |
| 部署或嵌入权威持久服务 | [Store](docs/store_zh.md)：FS/S3、generation、可见性、管理与 App 生命周期 |
| 部署/嵌入加速层，诊断 miss 与恢复 | [Cache](docs/cache_zh.md)：tier、sharding/EC、RocksDB/Redis 后端与 App 生命周期 |
| 通过共享库制备 OCI、归档或目录镜像 | [镜像制备](docs/image-preparation_zh.md)：生命周期、镜像 payload、布局与发布 |

[flatten-ctl 操作](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli_zh.md)归发布该程序的 guest-runtime。每篇均有完整英文版，可通过语言选择器切换。

## 独立运行

三个服务可独立于完整平台部署:

- 用 `manifest-ctl` ingest 或 fetch 稀疏工件;
- 为 `store-ctl` 配置文件系统根目录或 S3-compatible endpoint;
- 以本地、分片或分层拓扑运行 `cache-ctl`;
- 将 `app/store.Run` 或 `app/cache.Run` 静态链接进定制服务二进制。官方 CLI 与定制程序平级调用同一生命周期函数；定制程序不 exec、也不依赖安装 `store-ctl`/`cache-ctl`。

配置示例使用本地路径、文档保留 endpoint 和占位凭据。生产部署应使用受保护的 endpoint、显式资源预算、持久的权威存储,以及适合所选缓存拓扑的可观测性。

## 构建并验证修改

在满足文档所述 Go/Native 前置条件的 Accelerator checkout 中运行：

```bash
make manifest-ctl store-ctl
make cache-ctl
make test
```

工具链前置、交叉/离线构建、no-RocksDB 选项、ABI/Native 检查、镜像夹具、包验证与完整 prepared E2E 命令见[贡献指南](CONTRIBUTING_zh.md#构建与测试)。单元或夹具通过不构成真实云或 VM 覆盖。

## 选择已发行产品

本组件发布 `vX.Y.Z`，聚合 `release-vX.Y.Z` 精确选择独立组件 Tag。经验证的组合从 [Stable 聚合渠道](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest)选择。精确工件清单、Native 材料收集与打包检查见[组件发行准备](CONTRIBUTING_zh.md#构建与打包组件发行版)。共享 Preview 保留/清理、Latest 对齐和发行线规则由[项目发行契约](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release_zh.md)统一定义。

## 项目边界

- MicroVM 生命周期与快照执行归属 [`sandboxer`](https://github.com/kuasar-sandbox/sandboxer);
- 网络分配与转发归属 [`connector`](https://github.com/kuasar-sandbox/connector);
- guest 内核、Runtime image 与 `flatten-ctl` 发行物归属 [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime);
- 节点与集群编排归属 [`orchestrator`](https://github.com/kuasar-sandbox/orchestrator);
- 系统设计、共享集成测试、Demo 和聚合发行归属 [`kuasar-sandbox/kuasar-sandbox`](https://github.com/kuasar-sandbox/kuasar-sandbox)。

## 贡献与安全

请阅读本仓[贡献指南](CONTRIBUTING.md)与[组织贡献指南](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md)。公共数据格式或下游 package 契约变更需要关联 Companion PR 并执行精确源码的项目级验证。

不要在公开 Issue 中报告漏洞或真实存储凭据。请使用 [Kuasar Sandbox 安全政策](https://github.com/kuasar-sandbox/kuasar-sandbox/security/policy)和 GitHub 私密漏洞报告。

## License

项目原创内容采用 [Apache License 2.0](LICENSE)。保留源码与发行资产中原生库、vendor code、生成代码、格式及第三方工具的许可证、署名、NOTICE 和源码义务。
