[English](README.md) | [简体中文](README_zh.md)

# accelerator

面向镜像、快照和稀疏工件的数据访问与存储基础组件.它提供普通/稀疏数据表示、
本地工件与共享文件引用、Manifest、FS/S3-compatible store、分层缓存、完整性与
加密、OCI 拉取、EROFS 展平、按需读取和预取.

内容寻址和去重是其中一种能力,共享范围由 salt 与部署安全域决定,不是组件的唯一
价值.本仓是 [kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox) 平台的
存储/镜像基础设施,也可以独立演进和使用.

## 导出面(下游 import 的薄客户端)

下游项目(`sandboxer` 等)只 import 以下 **纯 Go、无 CGO** 的包,不会引入
RocksDB / AWS SDK / Reed-Solomon 等重依赖:

| 包 | 作用 |
|---|---|
| `pkg/sparse` | 平台统一稀疏模型:三态 `RunKind`(Hole/Zero/Data)+ 可执行 `Run` + `Source` 契约(`RunAt` 纯元数据解析,`Run.ReadAt` 在已解析区段内读取,`Source.ReadAt` 负责便利跨段读取,一次性源是一等公民),`ProbeHoles`(SEEK_HOLE)/`NewSource`/`Dense`;洞只来自权威元数据,禁止内容探洞 |
| `pkg/manifest` (+ `codec`/`crypto`/`chunker`/`fetch`/`ingest`) | 分块 + 收敛加密 + 内容寻址的读写 SDK;`fetch.Stream` = `sparse.Source` + Close,manifest Data Run 额外实现 `fetch.ChunkRun`,layered 透传最终 serving Run,`fetch.ResolveChunkWindow`按最终可见性解析同一物理chunk在anchor两侧的连续窗口;`fetch.OpenTarStream` 把本地 tarstream 工件零解包开成 Stream,`ingest.Ingest` 单趟消费任意 `sparse.Source` |
| `pkg/store` + `pkg/store/client` | 远端内容寻址存储代理的接口与客户端 |
| `pkg/cache` + `pkg/cache/client` | 分层缓存的接口与客户端 |
| `pkg/tarstream` | 单 payload 稀疏 tar 工件:`WriteTo` 写 GNU PAX sparse 1.0 + `.kuasar.digest.<hex>` carrier marker,边写边校验identity;marker保留payload boundary/commitment,使metadata-tail替换可用O(tail)工作量推导新identity;`SourceAt` 通过可选 `Digester` 零payload扫描读取身份;`ReadFrom` / `ReadSeekFrom` / `SourceFrom` 保持洞图精确往返与 GNU tar/archive-tar 互操作 |
| `pkg/{flatten,image,remote,tar}` | OCI/目录 → EROFS 展平的公共实现:`flatten` 负责源模型与导出,`image` 负责 RuntimeConfig 投影,`remote` 负责 OCI registry 拉取/referrer/cache,`tar` 负责展平结果解包 |

重后端(`*/server`、`pkg/cache/{rocks,redisstore,ec}`、`pkg/store/{fs,s3}`)只在 `cmd/` 与守护进程内
编译,不进入下游闭包。`flatten-ctl` 二进制随 `guest-runtime` 的
`runtime-vX.Y.Z` 发布,但其导入的
展平公共包仍在本仓维护,供 CLI、测试和运行时读取 config 的代码复用。

## 二进制

| 二进制 | 说明 | 链接 |
|---|---|---|
| `manifest-ctl` | 本地分块/加密/去重 + manifest 读写 CLI | 纯 Go |
| `store-ctl` | 内容寻址存储代理(fs / S3-compatible object storage backend + generation) | 纯 Go |
| `cache-ctl` | 分层缓存守护进程(local / shard / tiered + EC) | CGO,静态链 librocksdb |

**CGO 仅 `cache-ctl`**(librocksdb);其余两个二进制与全部导出面均 `CGO_ENABLED=0`。

## 构建

```bash
make manifest-ctl store-ctl     # 两个纯 Go 二进制
make cache-ctl                  # 自动 make deps-rocksdb 编出 librocksdb.a 再 CGO 静态链
make build                      # 全部三个
make build TARGET_ARCH=aarch64  # 交叉编译(别名 amd64 / arm64)
make vet                        # 校验薄客户端面(无需 librocksdb)
make test                       # 单元测试(含 rocks,需 librocksdb);e2e 见各 docs;zot 拉取 e2e 为 make e2e
make test-e2e                  # 运行 test/e2e/run_all.sh;需要项目主仓组装的完整 BIN
```

独立版本通过仓库 `main` 上受信任的 `Release` workflow 发布为 `vX.Y.Z`;发布件
`accelerator-vX.Y.Z-linux-x86_64.tar.gz` 包含三个服务二进制及性能/分析辅助脚本。
本仓文档与 `test/e2e/` 不进入组件包,由项目主仓聚合所选 tag 的源码并只放入
`platform-release-vX.Y.Z.tar.gz`。
本地可用 `make release VERSION=vX.Y.Z` 生成并校验相同布局的 release bundle。
当前组件 Release 构建并打包 Linux x86_64 目标;项目聚合随后针对所选真实发布资产组合
运行 BMS。组件打包成功不等于聚合验证通过。项目主仓的
每日协调器显式传入源码分支和精确 SHA;组件 `main` 用于主线,`release/vX.Y.x`
用于对应组件维护线。Preview 和维护分支 Stable 不更新 GitHub Latest;独立的幂等
Reconcile Latest 工作流按 `main` 源码提交先后协调主线 Stable,同一提交才比较 SemVer。
组件版本与平台聚合版本独立,
平台始终按精确 Tag 选择本组件。
同版本发布与删除共用完整 workflow mutation group;若 GitHub 合并 pending 请求,项目主仓
协调器会把 cancelled 状态作为未完成操作自动重跑,不会把它当作发布或 GC 已完成。

`deps/build-rocksdb.sh` 在 `build/<arch>/rocksdb/` 下编出无压缩的 `librocksdb.a`
(约数分钟,冷启)。

## 私网 / 离线构建

本仓自包含、无跨仓 Go 依赖,`GOPROXY` 指向内网镜像即可 `go build`。组织级多仓
协同开发见根 `go.work`([kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox))。

## 文档

- [docs/manifest_zh.md](docs/manifest_zh.md) — 二进制清单:分块 / 收敛加密 / 密钥表 / 读写 SDK。
- [docs/store_zh.md](docs/store_zh.md) — 内容寻址存储:分代目录布局 / fs·S3-compatible object storage backend / GC。
- [docs/cache_zh.md](docs/cache_zh.md) — 三层缓存 + 纠删码:local / shard / tiered 与旁路填充。
- [docs/cache-redis.md](docs/cache-redis.md) — Redis-compatible UDS/TCP 后端、取消语义与外部 Dragonfly 部署示例。
OCI/目录 → EROFS 展平 CLI 见 [guest-runtime flatten](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten_zh.md);完整确定性还取决于所选输入、配置与工具链。

## License

本仓库的项目原创内容采用 [Apache License 2.0](LICENSE).
贡献授权说明见 [CONTRIBUTING.md](CONTRIBUTING.md).
