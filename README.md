# accelerator

内容加速层:内容寻址存储 + 分层缓存 + 收敛加密,为 microVM 沙箱提供
镜像/快照的按需加载与跨租户去重。
是 [kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox) 平台的存储/镜像
基础设施,独立演进。

## 导出面(下游 import 的薄客户端)

下游项目(`sandboxer` 等)只 import 以下 **纯 Go、无 CGO** 的包,不会引入
RocksDB / AWS SDK / Reed-Solomon 等重依赖:

| 包 | 作用 |
|---|---|
| `pkg/sparse` | 平台统一稀疏模型:三态 `RunKind`(Hole/Zero/Data)+ `Source` 契约(元数据查询 RunAt 与数据读取 ReadAt 分离,一次性源是一等公民),`ProbeHoles`(SEEK_HOLE)/`NewSource`/`Dense`;洞只来自权威元数据,禁止内容探洞 |
| `pkg/manifest` (+ `codec`/`crypto`/`chunker`/`fetch`/`ingest`) | 分块 + 收敛加密 + 内容寻址的读写 SDK;`fetch.Stream` = `sparse.Source` + Close,`fetch.OpenTarStream` 把本地 tarstream 工件零解包开成 Stream,`ingest.Ingest` 单趟消费任意 `sparse.Source` |
| `pkg/store` + `pkg/store/client` | 远端内容寻址存储代理的接口与客户端 |
| `pkg/cache` + `pkg/cache/client` | 分层缓存的接口与客户端 |
| `pkg/tarstream` | 单文件稀疏流的 tar 信封:`WriteTo`(任意 `sparse.Source` → GNU PAX sparse 1.0,仅数据字节上线)/ `ReadFrom` / `ReadSeekFrom`(tar 内零拷贝随机)/ `SourceFrom`·`SourceAt` / `ReadSeekFromIndex`,洞图精确往返;互操作 GNU tar 与 archive/tar |
| `pkg/{flatten,image,remote,tar}` | OCI/目录 → EROFS 展平的公共实现:`flatten` 负责源模型与导出,`image` 负责 RuntimeConfig 投影,`remote` 负责 OCI registry 拉取/referrer/cache,`tar` 负责展平结果解包 |

重后端(`*/server`、`pkg/cache/{rocks,redisstore,ec}`、`pkg/store/{obs,fs}`)只在 `cmd/` 与守护进程内
编译,不进入下游闭包。`flatten-ctl` 二进制由 `guest-runtime` 发布,但其导入的
展平公共包仍在本仓维护,供 CLI、测试和运行时读取 config 的代码复用。

## 二进制

| 二进制 | 说明 | 链接 |
|---|---|---|
| `manifest-ctl` | 本地分块/加密/去重 + manifest 读写 CLI | 纯 Go |
| `store-ctl` | 内容寻址存储代理(fs / obs 后端 + generation) | 纯 Go |
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
```

`deps/build-rocksdb.sh` 在 `build/<arch>/rocksdb/` 下编出无压缩的 `librocksdb.a`
(约数分钟,冷启)。

## 私网 / 离线构建

本仓自包含、无跨仓 Go 依赖,`GOPROXY` 指向内网镜像即可 `go build`。组织级多仓
协同开发见根 `go.work`([kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox))。

## 文档

- [docs/manifest.md](docs/manifest.md) — 二进制清单:分块 / 收敛加密 / 密钥表 / 读写 SDK。
- [docs/store.md](docs/store.md) — 内容寻址存储:分代目录布局 / fs·obs 后端 / GC。
- [docs/cache.md](docs/cache.md) — 三层缓存 + 纠删码:local / shard / tiered 与旁路填充。
- [docs/cache-redis.md](docs/cache-redis.md) — Redis-compatible UDS 后端、取消语义与 Dragonfly 部署。
OCI/目录 → EROFS 确定性展平 CLI 见 `guest-runtime/docs/flatten.md`。
