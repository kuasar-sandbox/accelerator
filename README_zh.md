[English](README.md) | [简体中文](README_zh.md)

# accelerator

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

<a id="导出面下游-import-的薄客户端"></a>

## 公共 Go 导出面

下游 import 面保持精简,主要使用纯 Go,使 `sandboxer` 等消费者不必把存储服务端、对象存储 SDK、纠删码或数据库实现链接到自己的二进制中。

| 包 | 用途 |
| --- | --- |
| `pkg/sparse` | 权威 Hole/Zero/Data 表示与稀疏随机/顺序读取 |
| `pkg/manifest` 及子包 | 内容组织、分块、加密、ingest、fetch 和 prefetch |
| `pkg/store` 与 `pkg/store/client` | 内容存储协议和客户端 |
| `pkg/cache` 与 `pkg/cache/client` | 缓存协议、本地/分片/分层组合与客户端 |
| `pkg/tarstream` | 携带身份及完整性元数据的本地稀疏传输工件 |
| `pkg/flatten`、`pkg/image`、`pkg/remote`、`pkg/tar` | OCI/目录拉取、镜像配置与 EROFS 展平基础能力 |

重服务端后端保留在组件二进制和 server package 后面。稀疏 Run/Stream、chunk-window 与本地 tarstream 的详细契约归属于 [Manifest §4.8–§4.10](docs/manifest_zh.md#48-读路径细节)及[文件工件](docs/file-artifacts_zh.md),README 不另建一套协议。

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

<a id="构建"></a>

## 构建与测试

```bash
make manifest-ctl store-ctl     # 纯 Go 命令行服务
make cache-ctl                  # 缓存服务及当前后端依赖
make cache-ctl NO_ROCKSDB=1     # 不带 RocksDB (-tags no_rocksdb, CGO_ENABLED=0)
make build                      # 全部组件二进制
make build TARGET_ARCH=aarch64  # 交叉编译;接受 amd64/arm64 别名
make vet                        # 检查面向下游的薄 Go 导出面
make test                       # 单元与后端测试
make test-e2e                   # 组件 owner suite;需要项目组装的 BIN
```

Go 与原生前置依赖随构建目标而异。当前源码通过仓库脚本说明并构建所需原生缓存依赖。真实对象存储测试必须使用显式测试凭据或本地 S3-compatible 服务;普通单元测试与本地文件系统测试不应要求生产云凭据。

`cache-ctl` 通常使用 CGO 以及 `deps/build-rocksdb.sh` 在本地构建的静态 `librocksdb.a`;`manifest-ctl`、`store-ctl` 与下游 import 面不需要 RocksDB。`make release VERSION=vX.Y.Z` 生成并检查本地发行 bundle;官方缓存载荷必须保留 RocksDB 支持。

<a id="私网--离线构建"></a>

### 私网与离线构建

本 module 没有跨仓 Go 依赖。可将 `GOPROXY` 指向部署镜像并使用对应 checksum database 策略,或在离线时使用已填充、经过验证的 module cache。兄弟仓协调开发可使用[项目工作区](https://github.com/kuasar-sandbox/kuasar-sandbox),但它不是本组件独立构建的前提。

## 独立运行

三个服务可独立于完整平台部署:

- 用 `manifest-ctl` ingest 或 fetch 稀疏工件;
- 为 `store-ctl` 配置文件系统根目录或 S3-compatible endpoint;
- 以本地、分片或分层拓扑运行 `cache-ctl`;
- 在其他 Go 服务中使用导出包,不导入重后端。

配置示例使用本地路径、文档保留 endpoint 和占位凭据。生产部署应使用受保护的 endpoint、显式资源预算、持久的权威存储,以及适合所选缓存拓扑的可观测性。

## 发行模型

发行工作流在上传前把已完成归档的 SHA-256 记录为 build job output。发布者通过
`RELEASE_ARCHIVE_SHA256` 接收这一独立值,在任何 Tag/Release 写入前核对;不能用
下载后从 bundle 重新计算的值代替。即使重算 bundle 自身的校验和,全部载荷与材料
仍须匹配该次已完成构建。本地打包和独立验证不要求这个发布输入。该记录不证明
编译器来源,也不构成对不可信候选代码的隔离。
可信发布端根据已验证请求生成标准发行正文及来源/Preview 标记。下载的
`release-notes.md` 只是本地 bundle 辅助说明,不能决定公开发行正文或对账来源。

打包从选定的 Accelerator commit 建立全新 checkout,重新构建三个 Go 载荷和
recipe pin 的 RocksDB 静态库。拒绝 `RELEASE_BIN_DIR`、`RELEASE_ROCKSDB_SOURCE_DIR`,
不复用预制二进制、被忽略的开发文件或已解压/原生构建缓存。可以把下载缓存字节
复制到本次所属工作区,但 RocksDB recipe 在编译前仍核对归一化源码树摘要。五个
交付辅助脚本和 RocksDB 许可正文取自这些全新选定源码树。构建命令使用私有
home、临时目录及 Go 缓存,不继承云/发布凭据或构建 flag 覆盖。保留无凭据的
HTTPS 路由、`GOSUMDB`(包括 checksum mirror)和 `GOTOOLCHAIN`;后两者默认
分别为 `sum.golang.org` 和 `local`。

cache 载荷的实际链接映射必须选中本次新构建的 `librocksdb.a`、`libstdc++.a`
和 `libgcc.a`。打包记录 RocksDB 静态库摘要,并收集每个实际链入的系统静态库/
启动对象的摘要、Debian/RPM 源包身份及对应版权、许可和 NOTICE 正文。已安装输入
及许可必须匹配包文件元数据,仅有包归属不足以证明字节完整。这些检查不证明已
失陷主机或包数据库可信,也不改变 RocksDB 后端或持久化策略。

发行打包记录全新构建上下文实际选定的 Go 编译器,在构建前后将其分发输入与匹配的
`golang.org/toolchain` 归档逐项比较;归档由配置的 checksum database 认证。这覆盖
编译器、标准库源码及该分发中的其他文件。完整 Go 安装中额外的非构建 `api`、
`doc`、`misc`、`test` 文件不在认证范围,也不作为发行许可来源;核对时处理标准的
`go.mod`/`_go.mod` 安装转换。Go 许可/NOTICE 正文来自已验证归档,包括编译器和
标准库内嵌依赖的材料,保留各自相对路径。独立验证还会
重新核对其字节、来源 URL 和 module h1。版本字符串或重算 bundle 校验和不能替代
来源核对。验证要求启用 checksum database 并取得匹配的归档/缓存;即使采用
`GOTOOLCHAIN=local`,也可能获取核验材料,但不切换构建编译器或静默启用工具链
自动选择。这些检查以可信构建主机为前提,不证明已失陷主机可信。

归档名称记录请求的发行版本。来源记录仅在本地 Git Tag 指向所选 commit 时保留该版本;打 Tag 前使用 `git:<commit>`。验证器把全部 Go 载荷和项目来源 URL/摘要绑定到同一 commit。发布者传入预期 commit,在任何 Tag 或 Release 写入前拒绝不同来源的 bundle。
归档验证逐项列举三个 Go 二进制和五个交付辅助脚本,拒绝额外或重复的载荷条目,
要求 Go 构建目标为 Linux/amd64,并拒绝官方 cache 载荷使用 `no_rocksdb`。
组件的许可/来源材料目录继续按组件隔离。
每个 CLI 还必须是 Accelerator module 中对应的 `cmd/<name>` 主入口。每个交付
辅助脚本都与所选 Git blob 比较,因此验证端的本地对象数据库须包含该精确 commit。
可信发布端读取源码历史,不执行候选辅助脚本。解包前,归档解析器限制单个普通文件
不超过 512 MiB,完整 gzip 展开流(含 padding)不超过 1 GiB,条目不超过 20,000。
打包器显式设置输出 umask,调用者的严格 umask 不会改变发行目录权限合同。
构建和发布保留相同的 Go 路由策略。

pin 的 RocksDB 源码提供 `AUTHORS`、`COPYING`、`LICENSE.Apache` 与
`LICENSE.leveldb`,归档必须包含全部四份材料。许可证收集拒绝不可读子目录和
不完整遍历。不同原生链接输入不能以相同材料名称相互覆盖声明。打包成功或失败
退出时均清理本次所属的只读 Go module 缓存,不操作其他工作区。官方包不支持
没有已认证 module 校验和的第三方本地 Go 替换,应选择带版本的 module 替换。
现有 Kuasar 兄弟仓本地替换和普通开发构建不变。

本仓独立发布 `vX.Y.Z` 组件版本。x86_64 组件归档包含三个服务二进制及发行合同选定的运维/性能辅助脚本。组件设计文档和 E2E 源码从选定 Tag 收集到项目平台归档。

项目主仓独立发布 `release-vX.Y.Z` 聚合版本,选择 `accelerator` 和其他发行单元的精确版本,验证资产并执行跨组件测试。聚合与组件版本号相互独立。

发布分支、Preview、Latest 协调及并发/取消处理以[项目发行文档](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release_zh.md)为准;普通使用可从[最新 Stable 聚合渠道](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest)进入。

## 文档

Manifest、Store 和 Cache 提供完整英中版本。既有英文后端指南仍可使用:

- [Manifest - 英文](docs/manifest.md) / [中文](docs/manifest_zh.md) - 稀疏 Manifest、ingest/fetch、分块、加密、密钥表和公共 SDK;
- [文件工件与载体](docs/file-artifacts_zh.md) - 不可变 tarstream 加密、Bundle 布局、访问及发布;
- [Store - 英文](docs/store.md) / [中文](docs/store_zh.md) - 文件系统与 S3-compatible store、代保留、完整性和显式 purge(不是可达性 GC);
- [Cache - 英文](docs/cache.md) / [中文](docs/cache_zh.md) - 本地、分片、分层与纠删码缓存;
- [`docs/cache-redis.md`(英文)](docs/cache-redis.md) - Redis-compatible UDS/TCP 后端与外部服务部署示例。

OCI/目录到 EROFS 的 CLI 用法归属于 [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime),因为 `flatten-ctl` 属于 Runtime 发行单元。

README 提供组件公开入口;完整设计指南定义格式、运行限制、失败行为和测量要求。

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
