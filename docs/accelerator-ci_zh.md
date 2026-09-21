# CI 可移植性准备

[English](accelerator-ci.md) | [简体中文](accelerator-ci_zh.md)

仓库仍为 private 时,accelerator#134 / platform#128 仅为**已准备,未激活**。
本变更不发布源码,不调整可见性、计费或 quota,也不建立 private CI 的 public relay。

#152 workflow 在分配任何 runner 前,要求实际 caller 仓库可见性为 `public`,
且其完整名称与 `github.repository` 一致。所有 x86 发布、维护与集成 job 使用
`ubuntu-latest`,ARM 原生集成使用 `ubuntu-24.04-arm`。非公开 caller 不调度新版
hosted job。已部署的私有 workflow 保留至获授权的协调切换;私有调用被跳过不算验收。

## Bootstrap 与构建边界

每个 public 发布/维护 job 仅使用 `github.token`,将同一不可变 SHA 的平台工具
checkout 到 `trusted/platform`。Workflow 契约检查拒绝占位引用。Bootstrap 在 checkout 或执行请求的 accelerator 源码之前运行。

| Job | 共享 profile |
| --- | --- |
| 发布 preflight、publish、Preview delete | `release-control`:最小 control 工具与 `release.sh` 所需的环境 Go |
| Reconcile Latest、artifact cleanup | `control`:Git、curl、jq、Python/YAML 与归档工具 |
| Release build/test/package | `artifact-build` 或 `artifact-cross`:目标架构工具、native 依赖与 control 工具 |
| Public PR integration | 共享架构链路及独立必需源码检查,见[平台 CI](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci_zh.md) |

发布构建按原有 recipe 构建真实 RocksDB,不能以 `NO_ROCKSDB=1` 替代发布载荷。Go 与 native
编译器由构建环境提供,保留既有工具链选择语义。Control job 保留固定版本的 GitHub CLI installer。

精确发布源码位于 `src/accelerator`。构建、测试、native cache、打包与 artifact 上传
均使用该子目录,旁边的可信工具不会污染源码或 VCS stamping。Public cache 位于
bootstrap 的 `$RUNNER_TEMP/kuasar-hosted.*`,不使用固定 `/var/cache` 状态。
Hosted 构建/打包使用字面值 `bash` shell,通过 `taskset` 应用 bootstrap 的 CPU/内存
预算,也限制 RocksDB 使用 `nproc` 的并行度。

CI-only ABI 检查来自可信 workflow checkout。`manifest-ctl` 与 `store-ctl` 必须
保持静态;`cache-ctl` 保留正常 CGO/glibc 和静态 RocksDB、libstdc++、libgcc,
GLIBC 需求不得超过平台声明的 2.38 基线。ABI 失败阻止打包。Native recipe、摘要、
许可、来源清单与 link-map 校验保持不变。Build job 仍只有读权限;publish、维护和
artifact cleanup 保留分离权限、精确 version/source/Preview 校验、一天 bundle 保留期
和发布成功后的 artifact 删除。

## 覆盖与 rollout 证据

PR wrapper 保留共享 `ci-entry.yml@main`。#152 激活后,公开 caller 使用精确 baseline
加候选产品、准备好的 owner workspace 和按架构选择的 E2E。源码检查、UFFD gate
及适用的 x86 working-set smoke 仍为必需检查。手工演练或离线测试不能替代必需的 Integration E2E。
本地 filesystem/cache 和 S3-compatible fixture 仅证明本地行为,不代表真实云覆盖。
凭据化 OBS 仍需显式 `OBS_E2E=1` 运行;被排除的云测试不算通过。

从 accelerator checkout 运行离线检查:

```bash
python3 scripts/ci-test-workflows.py ../kuasar-sandbox  # 使用实际平台路径
(umask 022; bash scripts/test-release.sh)
```

Workflow 测试解析实际 YAML,验证非公开调用的分配 guard,执行 workspace/affinity shell,
并测试 ABI 接受与拒绝,无需 GitHub 或私有源码访问。原有发布测试使用合成
binary/link-map fixture 与模拟 API 响应,不等于真实 RocksDB/云资格验证。
平台 `ci/integration/test-ci-tools.sh` 仍为必需检查。

激活前,应正常合入评审后的共享实现,将所有发布 bootstrap checkout 固定到其完整
不可变 upstream SHA,并在标准 runner 验证真实候选源码、构建和发布行为。
Squash/rebase 后需更新 pin。Private 仓的准备或离线 fixture 通过,不代表 Public
CI 路由已经激活或已经验收。

普通 Go RocksDB 测试保留上游绑定的压缩库链接参数;build profile 显式提供 Snappy、LZ4、Zstandard 与 zlib 开发库,并不启用原有 native RocksDB recipe 的压缩功能。
