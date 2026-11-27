[English](secret-plugins.md) | [简体中文](secret-plugins_zh.md)

# Secret plugins

Secret plugins let a consuming process obtain secret bytes from a local broker
while configuration and process environments carry references. Accelerator uses
them for the Manifest customer key and explicitly configured static S3 access and
secret keys, including S3 generation sources. The SDK also supports native file
and literal decoders for other consumers.

## Configure plugins and references

```bash
export KUASAR_SECRET_PLUGINS='kms::/run/secret-broker1.sock:cache_ttl=30s:cache_size=4k enc::/run/secret-broker2.sock:cache_ttl=30s'
export MANIFEST_KEY='@kms/tenant-a/manifest-v1'
```

Each whitespace-separated declaration has the form:

```text
<plugin>::<absolute socket path>:<parameter>=<value>:...
```

Plugin names match `[A-Za-z][A-Za-z0-9-]*` and are case-sensitive. Socket paths
cannot contain colons or whitespace. Duplicate names, duplicate parameters,
unknown parameters and invalid values fail configuration parsing.

| Parameter | Default | Meaning |
|---|---|---|
| `cache_ttl` | `30s` | Successful-result lifetime, using Go duration syntax |
| `cache_size` | `4k` | Entry count; lowercase `k` means 1024, so `4k` is 4096 entries |

Either value may be zero to disable caching. Negative durations, invalid sizes
and integer overflow are rejected. No plugins are enabled when the environment
variable is empty.

A secret reference is `@<plugin>/<key>`. The first slash separates the plugin
from its opaque, nonempty key. Further slashes and underscores remain unchanged;
there is no URL decoding, case folding or path normalization. Keys cannot contain
colons or whitespace, allowing the same references in colon-separated key rings.
Quote references in YAML:

```yaml
manifest:
  key: "@kms/tenant-a/manifest-v1"
```

`MANIFEST_KEY` overrides `manifest.key`. The resolved value must be 32 bytes.
Ordinary inputs retain hex decoding. Manifest content IDs and `manifest://`
references are separate identifiers and do not use this plugin syntax.

For the Store S3 configuration, static credentials may mix references and literal
values:

```yaml
s3:
  access_key: "@kms/store-access-key-v1"
  secret_key: "@kms/store-secret-key-v1"
```

Both static fields must be supplied. Leaving both empty retains the AWS default
credential chain. An application-supplied credentials provider remains
authoritative and ignored static fields are not resolved. Static credentials are
resolved once when the S3 client is constructed; plugin TTL is not STS expiration
and does not trigger refresh of that client.

The leading `@` is reserved in integrated fields. Invalid references, missing
plugins and broker failures return errors without falling back to hex, a file or
a literal value. A native filename beginning with `@` can be expressed using
`./@filename` or an absolute path. A static credential beginning with `@` must be
supplied through a plugin or the application's existing credentials provider.

## Broker protocol

The deployment provides the broker and makes its Unix domain socket accessible
to each consuming host process. The SDK sends HTTP over that socket:

```http
POST /v1/secrets/resolve
Content-Type: application/json

{"key":"tenant-a/manifest-v1"}
```

The request omits the `@plugin/` routing prefix. A successful response echoes the
exact key and carries the actual bytes as standard padded Base64:

```json
{
  "key": "tenant-a/manifest-v1",
  "value": "base64:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
}
```

The sample represents 32 bytes. A symmetric key's decoded value is the raw key,
not its hex text. Private-key consumers expect PEM bytes, and static credentials
expect the bytes of the credential itself. Consumers validate their required
length or format; the generic SDK does not impose a 32-byte size.

Non-success HTTP status, missing fields, mismatched keys, invalid Base64 and
responses over 1 MiB fail resolution. Redirects are not followed. A shared broker
request has a five-second limit; each caller may stop waiting earlier through
its context. Error messages do not include response bodies or resolved values.
The protocol provides Resolve only, with no derivation, remote signing or unwrap
operation.

## Cache and consumer lifetime

Each client/plugin instance has its own LRU cache. A hit moves an entry to the
front without extending its TTL. Expiry begins when a successful resolution
completes. Expired values are never returned after a failed refresh, and errors
are not cached. Concurrent requests for the same plugin/key share one request;
cancelling one waiter does not cancel the others. Returned byte slices are
independent copies.

TTL affects subsequent Resolve calls. Existing storage objects, TLS certificates
and static S3 clients keep the material bound during their construction. Use
stable, versioned references for persistent encryption keys; changing the bytes
behind a reference can make newly constructed objects use a different key from
existing objects. Encryption-key rings retain historical keys explicitly.

With references, environments and configuration retain reference text. Actual
bytes remain in consumer memory and the cache. Existing raw hex configurations
continue to work with their original environment-exposure characteristics.

## SDK use

```go
raw, err := secret.Resolve(ctx, "@kms/tenant-a/manifest-v1")
privateKey, err := secret.Resolve(ctx, configuredKey, os.ReadFile)
credential, err := secret.Resolve(ctx, configuredCredential, secret.Literal)
```

`secret.Resolve` initializes the process client from `KUASAR_SECRET_PLUGINS` on its
first plugin reference. Native inputs do not initialize it. Install trusted host
task environment configuration before this point; later environment changes do
not reload the client. Applications needing a separately owned configuration can
use `secret.New(config)` and `client.Resolve` with the same arguments.

`manifest.Config.CustomerKeyContext(ctx)` resolves and validates a customer key;
the existing `CustomerKey()` method uses a background context. A transfer that
already holds a key can use `NewFetcherWithKey` to keep local and remote reads
bound to the same bytes. Neither path writes resolved material into configuration.
