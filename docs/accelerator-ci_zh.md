[English](accelerator-ci.md) | [简体中文](accelerator-ci_zh.md)

# Accelerator CI 要求与检查

共享 runner 路由、profile、workflow 入口和上线状态由
[平台 CI 契约](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci_zh.md)
统一说明。本文记录 accelerator 的构建要求及其检查能够提供的证据。

## 构建与 ABI 要求

发布构建按原有 recipe 构建真实 RocksDB，不能以 `NO_ROCKSDB=1` 替代发布载荷。
Go 与 native 编译器由构建环境提供，保留既有工具链选择语义。
普通 Go RocksDB 测试保留上游绑定的压缩库链接参数，需要 Snappy、LZ4、Zstandard
与 zlib 开发库；这并不启用原有 native RocksDB recipe 的压缩功能。

Accelerator 的 [ABI 检查](../scripts/ci-check-abi.py) 要求 `manifest-ctl` 与
`store-ctl` 保持静态。`cache-ctl` 保留正常 CGO/glibc 和静态 RocksDB、libstdc++、
libgcc，GLIBC 需求不得超过声明的 2.38 基线。ABI 失败阻止打包。
Native recipe、摘要、许可、来源清单与 link-map 校验仍为必需检查。

## Owner E2E 与本地证据

[Accelerator E2E 指南](../test/e2e/README_zh.md) 列出组装后二进制、EROFS、Redis、
临时存储和权限要求，以及 flatten、manifest、store、cache 的各项必需用例。
组装 owner 套件时须保留完整 helper 目录。本地 filesystem/cache 和 S3-compatible
fixture 仅证明本地行为，不代表真实云覆盖。凭据化 OBS 仍需显式 `OBS_E2E=1`
运行；被排除的云测试不算通过。

从 accelerator checkout 运行离线检查：

```bash
python3 scripts/ci-test-workflows.py ../kuasar-sandbox  # 使用实际平台路径
(umask 022; bash scripts/test-release.sh)
make test-e2e-scripts
```

Workflow 检查解析组件的实际 YAML，验证其分配、workspace 和 ABI 契约。
发布测试使用合成 binary/link-map fixture 与模拟 API 响应。脚本测试验证 fixture
和生命周期边界，不会完成真实 E2E 的编号断言。这些检查通过不能证明真实 RocksDB
构建、owner E2E 或云端资格验证已通过。
