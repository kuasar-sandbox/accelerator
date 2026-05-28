# sandbox-builder

容器镜像构建：把 OCI / docker-archive 确定性展平为 EROFS rootfs，并在镜像尾部
追加运行时配置（STORED-mode ZIP）。是 [kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox)
的镜像供给侧，独立演进。

## 组成

| 路径 | 角色 | 依赖 |
|---|---|---|
| `pkg/image` | **轻量读取面**：`RuntimeConfig` 类型、`ReadConfig`/`AppendConfigZip`/`ReadEROFSSize`。供工具与 `sandbox-runtime` 读取/inspect 展平镜像 | 仅 stdlib |
| `pkg/flatten` | 展平引擎：OCI→EROFS（调用 `mkfs.erofs`），确定性输出 + `Verify` | `pkg/image`、`internal/util` |
| `cmd/flatten-ctl` | CLI：`export`/`verify`/`info`；`--upload` 经 `sandbox-accelerator/pkg/manifest` ingest 进 store | accelerator SDK |

`sandbox-runtime` 只 import `pkg/image`（读取镜像内嵌的 RuntimeConfig），不引入展平引擎。

## 构建

```bash
make flatten-ctl     # 纯 Go
make vet test
```

运行期需要 `mkfs.erofs`（由 `sandbox-deps` 的 `deps/build-erofs.sh` 产出），放到 PATH
或 `flatten-ctl` 同目录。

## 依赖

- `sandbox-accelerator`（`pkg/manifest`，仅 `--upload` 路径）——本仓 `go.mod` 经
  `replace => ../sandbox-accelerator` 本地解析；组织级协同见根 `go.work`。

设计细节见 `docs/flatten.md`。
