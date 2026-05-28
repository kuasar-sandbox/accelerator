# sandbox-accelerator

存储加速层：内容寻址存储 + 分层缓存 + 收敛加密，为 microVM 沙箱提供镜像/快照的
按需加载与跨租户去重。是 [kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox)
的存储基础设施，独立演进。

## 导出面（下游 import 的薄客户端）

下游项目（`sandbox-runtime`、`sandbox-builder`）只 import 以下 **纯 Go、无 CGO** 的包，
不会引入 RocksDB / AWS SDK / Reed-Solomon 等重依赖：

| 包 | 作用 |
|---|---|
| `pkg/manifest` (+ `codec`/`crypto`/`chunker`/`fetch`/`ingest`) | 分块 + 收敛加密 + 内容寻址的读写 SDK |
| `pkg/store` + `pkg/store/client` | 远端存储代理的接口与客户端 |
| `pkg/cache` + `pkg/cache/client` | 分层缓存的接口与客户端 |

重后端（`*/server`、`pkg/cache/rocks`、`pkg/cache/ec`、`pkg/store/obs`、`pkg/store/fs`）
只在守护进程与 `cmd/` 内编译，不进入下游闭包。

## 守护进程

| 二进制 | 说明 | 链接 |
|---|---|---|
| `manifest-ctl` | 本地分块/加密/去重 + manifest 读写 CLI | 纯 Go |
| `store-ctl` | 内容寻址存储代理（fs / obs 后端 + generation） | 纯 Go |
| `cache-ctl` | 分层缓存守护进程（local / shard / tiered + EC） | CGO，静态链 librocksdb |

## 构建

```bash
make manifest-ctl store-ctl     # 纯 Go
make cache-ctl                  # 自动 make deps-rocksdb 编出 librocksdb.a 再 CGO 静态链
make build                      # 全部三个
make vet                        # 校验薄客户端面（无需 librocksdb）
make test                       # 单元测试（含 rocks，需 librocksdb）
```

`deps/build-rocksdb.sh` 在 `build/rocksdb/` 下编出无压缩的 `librocksdb.a`（约数分钟，冷启）。
其余原生依赖（vmlinux / cloud-hypervisor / mkfs.erofs）见 `sandbox-deps`。

## 私网 / 离线构建

本仓自包含、无跨仓 Go 依赖，`GOPROXY` 指向内网镜像即可 `go build`。
组织级多仓协同开发见根 `go.work`（[kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox)）。

设计细节见 `docs/manifest.md`、`docs/store.md`、`docs/cache.md`。
