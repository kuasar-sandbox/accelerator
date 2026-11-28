[English](secret-plugins.md) | [简体中文](secret-plugins_zh.md)

# Secret plugin 密钥接入

Secret plugin 使消费进程通过本机 broker 获取实际密钥字节，配置与环境变量保留引用。
Accelerator 的 Manifest customer key 和显式配置的静态 S3 AK/SK 支持该机制，
包括 S3 generation source。SDK 同时提供文件、字面值适配，供其他消费端使用。

## 插件配置与引用

```bash
export KUASAR_SECRET_PLUGINS='kms::/run/secret-broker1.sock:cache_ttl=30s:cache_size=4k enc::/run/secret-broker2.sock:cache_ttl=30s'
export MANIFEST_KEY='@kms/tenant-a/manifest-v1'
```

多个插件声明以空白分隔，单个声明格式为：

```text
<plugin>::<UDS 绝对路径>:<参数>=<值>:...
```

插件名匹配 `[A-Za-z][A-Za-z0-9-]*`，区分大小写。UDS 路径不能包含冒号或空白。
重复名称、重复参数、未知参数及非法参数值均报错。

| 参数 | 默认值 | 含义 |
|---|---|---|
| `cache_ttl` | `30s` | 成功结果的有效期，采用 Go duration 语法 |
| `cache_size` | `4k` | 缓存条目数；小写 `k` 表示 1024，因此 `4k` 为 4096 条 |

任一参数为零即禁用缓存。负 duration、非法容量及整数溢出均被拒绝。
环境变量为空时不启用任何插件。

引用格式为 `@<plugin>/<key>`。第一个 `/` 分隔插件名和非空的不透明 key；
其余斜杠和下划线原样保留，不进行 URL 解码、大小写转换或路径归一化。
key 不能包含冒号和空白，因此可直接用于冒号分隔的 key ring。YAML 中使用引号：

```yaml
manifest:
  key: "@kms/tenant-a/manifest-v1"
```

`MANIFEST_KEY` 覆盖 `manifest.key`；解析结果必须为 32 字节。普通输入继续按 hex
解码。Manifest content ID 和 `manifest://` 引用属于内容标识，不使用密钥插件语法。

Store S3 的静态配置允许引用与字面值混用：

```yaml
s3:
  access_key: "@kms/store-access-key-v1"
  secret_key: "@kms/store-secret-key-v1"
```

两个静态字段必须同时提供。均为空时沿用 AWS 默认凭据链。应用显式注入的
CredentialsProvider 优先，被忽略的静态字段不触发插件解析。静态凭据只在 S3 client
创建时解析，插件 TTL 不是 STS 过期时间，也不会触发该 client 刷新凭据。

接入字段中的前导 `@` 为保留前缀。非法引用、未配置插件和 broker 错误直接返回失败，
不回退为 hex、文件或字面值。以 `@` 开头的原生文件名可用 `./@filename` 或绝对路径
表达；以 `@` 开头的静态凭据通过插件或应用已有的 CredentialsProvider 提供。

## Broker 协议

部署方提供 broker，并使各消费端 host 进程能够访问其 Unix domain socket。
SDK 通过该 socket 发送 HTTP 请求：

```http
POST /v1/secrets/resolve
Content-Type: application/json

{"key":"tenant-a/manifest-v1"}
```

请求 key 不包含 `@plugin/` 路由前缀。成功响应回显完全相同的 key，并使用标准、
带 padding 的 Base64 编码实际字节：

```json
{
  "key": "tenant-a/manifest-v1",
  "value": "base64:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
}
```

示例表示 32 字节。对称密钥返回原始字节，不返回 hex 文本；私钥消费端使用 PEM
字节；静态凭据使用凭据原文的字节。消费端负责长度和格式校验，通用 SDK 不限定
32 字节。

非成功 HTTP 状态、字段缺失、key 不匹配、非法 Base64 及超过 1 MiB 的响应均报错，
不跟随重定向。共享 broker 请求最长五秒；每个调用方可通过 context 提前停止等待。
错误信息不包含完整响应体或实际密钥。协议只提供 Resolve，不包含派生、远程签名或
解封操作。

## 缓存与使用者生命周期

每个 client 内的各插件实例使用独立 LRU 缓存。命中提升最近使用顺序但不延长 TTL；
TTL 从成功解析完成时计算。过期后解析失败不返回旧值，错误不进入缓存。
同插件、同 key 的并发请求合并；单个等待者取消不影响其他等待者。
返回字节使用独立副本。

TTL 只影响后续 Resolve。已有存储对象、TLS certificate 和静态 S3 client 保留构造时
绑定的材料。持久化加密密钥使用稳定、带版本的引用；改变引用对应的字节可能使新旧
对象使用不同密钥。EncryptionKey 的历史密钥仍通过 key ring 显式保留。

引用模式下环境和配置只保存引用文本，实际字节存在于消费进程内存及缓存中。
已有原始 hex 配置继续兼容，其原有环境暴露特征也继续存在。

## SDK 使用

```go
raw, err := secret.Resolve(ctx, "@kms/tenant-a/manifest-v1")
privateKey, err := secret.Resolve(ctx, configuredKey, os.ReadFile)
credential, err := secret.Resolve(ctx, configuredCredential, secret.Literal)
```

`secret.Resolve` 在首次处理插件引用时从 `KUASAR_SECRET_PLUGINS` 初始化进程 client。
普通输入不触发初始化。host task 的可信环境配置必须在此之前安装；之后更改环境
不会重新加载 client。需要独立配置实例的应用可使用 `secret.New(config)`，再调用
相同参数形式的 `client.Resolve`。

`manifest.Config.CustomerKeyContext(ctx)` 获取并校验 customer key，已有 `CustomerKey()`
方法使用 background context。已经持有密钥的 transfer 可使用 `NewFetcherWithKey`，
让本地和远端读取绑定相同字节。解析结果均不回写配置。
