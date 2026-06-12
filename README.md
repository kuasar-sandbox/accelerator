# sandbox-builder

容器镜像构建:把 OCI / docker-archive 或**远程 registry 镜像**确定性展平为 EROFS
rootfs,并在镜像尾部追加运行时配置(STORED-mode ZIP)。是
[kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox) 的镜像供给侧,独立演进。

## 组成

| 路径 | 角色 | 依赖 |
|---|---|---|
| `pkg/image` | **轻量读取面**:`RuntimeConfig` 类型、`ReadConfig`/`AppendConfigZip`/`ReadEROFSSize`、`ExtractRuntimeConfigFromJSON`。供工具与 `sandbox-runtime` 读取/inspect 展平镜像 | 仅 stdlib |
| `pkg/flatten` | 展平引擎:`Source` 接口 + 确定性 `Build` sink(OCI→EROFS,调用 `mkfs.erofs`)+ `Verify` | `pkg/image`、`internal/util` |
| `pkg/remote` | **远程拉取面**:registry 拉取 + 平台选择 + env 凭据 + TLS CA 配置 + OCI-layout blob 缓存(并发下载 / 原子写 / LRU 淘汰)+ OCI Referrers 回写与幂等跳过。产出 `flatten.Source` | go-containerregistry、`pkg/flatten` |
| `pkg/tar` | **tar 工具面**:规则化组装/提取 tar 流(`Rule`+`Create`/`Extract`),稀疏保持、重命名、stdio——create=exec GNU tar(≥1.28,`LocateTar` 三级定位),extract=纯 Go 单遍流式;`StreamWriter.WriteSparse` 把已知洞图的内存稀疏流直接发射为 PAX sparse 1.0 条目(纯 Go 真流式) | 仅 stdlib(仅 create 运行期依赖 GNU tar) |
| `cmd/flatten-ctl` | CLI:`export`(registry/docker-archive/rootfs 目录三源)/`verify`/`info`/`cache`/`config`/`tar`;`--upload` 经 `sandbox-accelerator/pkg/manifest` ingest 进 store | accelerator SDK、`pkg/remote`、`pkg/tar` |

依赖洁净:`sandbox-runtime` 只 import `pkg/image`(stdlib-only,读取镜像内嵌的
RuntimeConfig);go-containerregistry **只落在 `pkg/remote`**,不进 `pkg/image` /
`pkg/flatten` 的导入闭包。docker-archive 源与 registry 源汇入同一个 `flatten.Build`,
逐字节确定性一致。

## 构建

```bash
make flatten-ctl     # 纯 Go(CGO_ENABLED=0)
make vet test
make e2e             # opt-in:从真实 OCI-1.1 registry(zot)拉取+展平+Referrers 回写
```

运行期需要 `mkfs.erofs`(由 `sandbox-deps` 的 `deps/build-erofs.sh` 产出),定位优先级
`MKFS_EROFS_PATH` env > `flatten-ctl` 同目录 > `PATH`;`tar create` 同理需要 GNU tar
≥1.28(`TAR_PATH` env > 同目录 > `PATH`,`sandbox-deps` 的 `make tar` 产静态版;
`tar extract` 纯 Go 流式,无此依赖)。
展平保留镜像内文件属主(chown),`export`/`verify` 需以 root(或 CAP_CHOWN)运行;
rootfs 目录源只读源树,无 chown,完整 rootfs 导出同样建议 root(读权限)。

`make e2e`(`test/e2e/`)经 `make zot` 拉一份 zot registry 二进制,让 `flatten-ctl`
从真实 OCI-1.1 registry 拉取、展平并回写 Referrers;非 `make test` 的一部分,需
docker、`mkfs.erofs`、网络与同级 `sandbox-accelerator`。

## 依赖

- `sandbox-accelerator`(`pkg/manifest`,仅 `--upload` 路径)——本仓 `go.mod` 经
  `replace => ../sandbox-accelerator` 本地解析;组织级协同见根 `go.work`。
- `github.com/google/go-containerregistry`(registry 拉取与 Referrers,仅 `pkg/remote`)
  ——纯 Go,`CGO_ENABLED=0` 不变。固定 `v0.20.6`:最新 go-directive ≤ 1.24 的版本,
  匹配工作区 `go 1.24.0`(v0.20.8+ 要求 go 1.25,会触发工具链切换)。

设计细节见 `docs/flatten.md`。
