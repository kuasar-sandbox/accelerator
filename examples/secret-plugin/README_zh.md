[English](README.md) | [简体中文](README_zh.md)

# 开发 Secret plugin

本样例是一个通过 Unix domain socket 提供 Secret plugin Resolve 协议的小型外部进程。
可复用的 `pkg/secret/plugin` handler 负责 HTTP 和 JSON，`resolveDemo` 是接入后端的
替换点。现有 `pkg/secret` client 通过它解析密钥引用。

样例只提供一个 key：`example-key`，其值为从 `0x00` 到 `0x1f` 的 32 个字节。
这些公开已知的字节仅用于演示和检查接入过程，不能用于保护真实数据。
运行样例不需要 KMS 账户、网络服务、VM 或原生库。

协议、引用语法、消费端缓存和密钥生命周期见
[Secret plugin](../../docs/secret-plugins_zh.md)。

## 构建并启动

使用 Linux 和 Go 1.26.1 或更新版本。在包含本样例的 Accelerator 源码版本中，
从仓库根目录运行以下命令：

```bash
plugin_dir=$(mktemp -d /tmp/secret-plugin.XXXXXX)
go build -o "$plugin_dir/secret-plugin-example" ./examples/secret-plugin
"$plugin_dir/secret-plugin-example" --socket "$plugin_dir/plugin.sock" &
plugin_pid=$!
```

看到 `demo Secret plugin listening on ...` 后再继续。唯一必填参数是 `--socket`，
值必须是不含冒号或空白的绝对路径。父目录须已存在、归当前用户所有，权限为 `0700`；
`mktemp -d` 会创建这样的目录。样例将 socket 权限设置为 `0600`，供同一用户运行的
消费进程访问。

目标路径已存在时，启动会失败。请使用新的私有目录，避免删除归属不明的文件或
socket。服务端设置了请求头、读取和写入的超时，并在收到 SIGINT 或 SIGTERM 时正常
退出。正常退出只删除本进程创建的 socket，目录留给调用方管理。

## 通过 SDK 解析

在同一 shell 中配置名为 `demo` 的插件：

```bash
export KUASAR_SECRET_PLUGINS="demo::$plugin_dir/plugin.sock"
```

插件名称由消费应用的配置决定，不会发送给后端。消费端解析 `@demo/example-key` 时，
回调收到的是 `example-key`。

下面的完整消费程序使用现有 SDK client，只输出字节数，不输出解析得到的值：

```bash
cat > "$plugin_dir/resolve.go" <<'EOF'
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "time"

    "github.com/kuasar-sandbox/accelerator/pkg/secret"
)

func main() {
    client, err := secret.New(os.Getenv(secret.Env))
    if err != nil {
        log.Fatal(err)
    }
    defer client.CloseIdleConnections()
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    value, err := client.Resolve(ctx, "@demo/example-key")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Resolved %d bytes\n", len(value))
}
EOF
go run "$plugin_dir/resolve.go"
```

预期输出为 `Resolved 32 bytes`。工作目录保持在仓库根目录，使消费程序使用本仓的
Go module。请求其他 key 会返回错误。执行以下命令，可验证真实 SDK/UDS 往返、
正常退出和 socket 归属检查：

```bash
go test -race ./examples/secret-plugin
```

完成后停止后台进程并清理样例文件：

```bash
kill -TERM "$plugin_pid"
wait "$plugin_pid"
test ! -e "$plugin_dir/plugin.sock"
unset KUASAR_SECRET_PLUGINS
rm "$plugin_dir/secret-plugin-example" "$plugin_dir/resolve.go"
rmdir "$plugin_dir"
```

## 接入实际后端

将 [main.go](main.go) 中的 `resolveDemo` 替换为自己的 resolver。回调签名为
`func(context.Context, string) ([]byte, error)`，传给 `plugin.NewHandler`。
回调接收不透明 key，从选定后端获取密钥并返回实际字节：对称密钥返回原始密钥字节，
TLS 私钥返回 PEM 内容，静态凭据返回凭据原文的字节。handler 负责构造 key 回显和
Base64 响应。

回调可能并发执行。应将 context 传给后端，为外部调用设置时间上限，获取失败时
返回错误。在后端定义允许的 key 映射，避免把任意标识直接当作文件系统路径。
日志中不要记录实际密钥或包含密钥的后端响应。handler 向调用方返回通用错误，
不暴露回调的错误文本。

部署方负责后端凭据与授权、插件进程，以及 socket 的访问权限。构建实际服务时，
按预期的消费进程账户调整目录和 socket 的所有权。能够访问 socket 的进程也就
能够请求该插件提供的密钥。

SDK 已提供各插件独立的缓存和并发请求合并，本样例不增加服务端缓存。
回调不会收到 `cache_ttl` 等插件配置参数，这些参数用于配置 client。
持久化密钥使用稳定、带版本的标识，并遵循协议指南中的消费端生命周期规则。

handler 和样例以 Go 源码形式随 Accelerator module 和仓库提供。组件原生架构
发行包包含运行程序，样例从源码构建。其他语言可直接实现相同的 HTTP-over-UDS 协议。
