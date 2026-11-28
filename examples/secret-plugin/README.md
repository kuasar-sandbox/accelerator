[English](README.md) | [简体中文](README_zh.md)

# Develop a Secret plugin

This example is a small external process that serves the Secret plugin Resolve
protocol over a Unix domain socket. The reusable `pkg/secret/plugin` handler
handles HTTP and JSON; `resolveDemo` is the backend integration point. The
existing `pkg/secret` client resolves references through it.

The example exposes one key, `example-key`, whose value is the 32 bytes from
`0x00` through `0x1f`. These publicly known bytes are demonstration data only.
Use them to check the integration, never to protect real data. The example does
not require a KMS account, network service, VM or native library.

For the protocol, reference syntax, consumer caching and key lifetime, see
[Secret plugins](../../docs/secret-plugins.md).

## Build and start

Use Linux and Go 1.26.1 or newer. Run these commands from the Accelerator
repository root, using a source revision that contains this example:

```bash
plugin_dir=$(mktemp -d /tmp/secret-plugin.XXXXXX)
go build -o "$plugin_dir/secret-plugin-example" ./examples/secret-plugin
"$plugin_dir/secret-plugin-example" --socket "$plugin_dir/plugin.sock" &
plugin_pid=$!
```

Wait for `demo Secret plugin listening on ...` before continuing. The sole
required option is `--socket`, an absolute path without colons or whitespace.
Its parent directory must already exist, be owned by the current user and have
mode `0700`; `mktemp -d` provides that directory. The example creates its socket
with mode `0600` for consumers running as the same user.

An existing path causes startup to fail. Use a fresh private directory instead
of deleting an unfamiliar file or socket. The server bounds header, read and
write times, and shuts down on SIGINT or SIGTERM. Normal shutdown removes only
the socket it created and leaves the directory for the caller to manage.

## Resolve through the SDK

In the same shell, configure a plugin named `demo`:

```bash
export KUASAR_SECRET_PLUGINS="demo::$plugin_dir/plugin.sock"
```

The plugin name is chosen by the consuming application's configuration; it is
not sent to the backend. The callback receives `example-key` when a consumer
resolves `@demo/example-key`.

The following complete consumer uses the existing SDK client. It prints the
byte count, without printing the resolved value:

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

The expected output is `Resolved 32 bytes`. Keep the working directory at the
repository root so the consumer uses its Go module. A different key returns an
error. To verify the example's real SDK/UDS roundtrip, shutdown and socket
ownership behavior, run:

```bash
go test -race ./examples/secret-plugin
```

Stop the background process and remove the example files when finished:

```bash
kill -TERM "$plugin_pid"
wait "$plugin_pid"
test ! -e "$plugin_dir/plugin.sock"
unset KUASAR_SECRET_PLUGINS
rm "$plugin_dir/secret-plugin-example" "$plugin_dir/resolve.go"
rmdir "$plugin_dir"
```

## Connect a real backend

Replace `resolveDemo` in [main.go](main.go) with your own resolver. The callback
has the signature `func(context.Context, string) ([]byte, error)` and is passed
to `plugin.NewHandler`. It receives an opaque key, retrieves the secret from
your chosen backend, and returns raw bytes. Symmetric keys are raw key bytes,
TLS private keys are PEM contents, and static credentials are the bytes of the
credential itself. The handler constructs the echoed key and Base64 response.

The callback can run concurrently. Pass its context to the backend and bound
external calls; return an error when retrieval fails. Define the allowed key
mapping in the backend instead of treating arbitrary identifiers as filesystem
paths. Avoid logging resolved values or backend responses that contain them.
The handler returns generic errors to callers rather than exposing callback
error text.

Deployment owns the backend's credentials and authorization, the plugin
process, and access to its socket. Adapt the directory and socket ownership to
the intended consumer accounts when building a real service. Socket access
grants the ability to request the secrets that the plugin exposes.

The SDK already provides per-plugin caching and request coalescing. This
example adds no server cache. The callback receives no plugin configuration
parameters such as `cache_ttl`; those configure the client. Use stable,
versioned identifiers for persistent keys and follow the consumer lifetime
rules in the protocol guide.

The handler and example are Go source delivered with the Accelerator module
and repository. The native component archives contain operational binaries;
the example is built from source. Other languages can implement the same
HTTP-over-UDS protocol directly.
