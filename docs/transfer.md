[English](transfer.md) | [简体中文](transfer_zh.md)

# Data transfer, inspection and conversion

`manifest-ctl` and the shared transfer API move selected logical content between tarstream files, Manifest Bundles and Store. Start with the complete local roundtrip, then use the command and configuration reference. The [data contract](artifacts.md) defines references, sparse data, formats and encryption; this guide owns invocation, transformations, output and caller responsibilities.

## Independent Store roundtrip

Use this before adding a VM or cache. Prerequisites: Linux, Bash, Python 3,
`store-ctl` and `manifest-ctl` from one selected aggregate, plus that aggregate's
`flatten-ctl` from guest-runtime. Its [pure-Go tar stream/extract interface](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli.md#flatten-ctl-tar--tar-extraction-and-single-file-sparse-streams)
needs neither mkfs.erofs nor KVM/root. Check these subcommands in the selected
binary's help; older versions lacking this interface are outside this recipe.
Use matching selected versions, not unrelated latest component downloads.

The following disposable FS Store uses a private UDS and a generated test-only
customer key. No L2, MicroVM, registry, public listener or external data is involved.
[Store generation initialization](store.md#administrative-commands) and
[Configuration](#configuration) remain authoritative. Run in a new shell. Exit cleanup always stops this
child; only success removes its temporary directory. Failure retains the private
directory and prints its path for diagnosis.

```bash
set -euo pipefail
umask 077
LAB=$(mktemp -d)
STORE_PID=
SUCCESS=0
cleanup() {
  result=$?
  trap - EXIT
  if [ -n "$STORE_PID" ]; then kill "$STORE_PID" 2>/dev/null || true; wait "$STORE_PID" 2>/dev/null || true; fi
  if [ "$result" -eq 0 ] && [ "$SUCCESS" -eq 1 ]; then
    rm -rf -- "$LAB"
  else
    printf 'Retained private diagnostics: %s (store.log)\n' "$LAB" >&2
  fi
  exit "$result"
}
trap cleanup EXIT
cat > "$LAB/store.yaml" <<EOF
listen: unix://$LAB/store.sock
backend: fs
fs:
  root: $LAB/objects
  verify_content_key: true
generations:
  file:
    path: $LAB/generations
EOF
store-ctl init --config "$LAB/store.yaml" --generation G1
store-ctl serve --config "$LAB/store.yaml" >"$LAB/store.log" 2>&1 &
STORE_PID=$!
for i in $(seq 1 50); do
  test -S "$LAB/store.sock" && break
  kill -0 "$STORE_PID"
  sleep 0.1
done
test -S "$LAB/store.sock"
export MANIFEST_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
cat > "$LAB/manifest.yaml" <<EOF
store:
  endpoint: unix://$LAB/store.sock
  timeout: 5s
manifest:
  verify_content: true
chunker:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
  local: off
EOF
printf 'Kuasar independent storage roundtrip\n' > "$LAB/payload"
flatten-ctl tar stream -f "$LAB/input.tar" "image:$LAB/payload"
KEY=$(manifest-ctl store --manifest-config "$LAB/manifest.yaml" --no-progress "$LAB/input.tar")
manifest-ctl verify --manifest-config "$LAB/manifest.yaml" --no-progress "$KEY"
manifest-ctl load --manifest-config "$LAB/manifest.yaml" --output "$LAB/output.tar" "$KEY"
flatten-ctl tar extract -f "$LAB/output.tar" "image:$LAB/restored"
cmp "$LAB/payload" "$LAB/restored"
printf 'PASS: payload bytes match\n'
unset MANIFEST_KEY
SUCCESS=1
```

Expect a complete Manifest key, zero failed chunks from verify, and the final
PASS line. Compare extracted logical bytes: tar wrappers/names can differ after
load. A normal file or a generic tar is not a substitute for the platform
sparse/digest envelope. On failure inspect `store.log` in the retained directory
reported on stderr; check generation initialization, socket readiness and key availability;
do not retry an uncertain write as though it were a failed read. This validates
only a local byte roundtrip, not sparse VM state, cache tiers, S3 or recovery.
After diagnosis, explicitly remove only the reported private test directory.
For intentionally retained data, preserve the key/config/ref instead of using
this test-only cleanup; the generated key exists only in this shell environment.

## Choose sources and outputs

| Operation | Input | Result |
| --- | --- | --- |
| `store` | One tarstream path, identity/location file ref, selected Bundle root, or Manifest key/ref; omitted input or `-` is tarstream stdin. | Store objects and the root Manifest key. |
| `load` | One or more logical refs, topmost first; stdin must be the only source and must be tarstream. | A tarstream or Bundle, Store output, a logical window, or extracted metadata according to flags. |
| `info`, `verify`, `get-manifest`, `diff` | Each command has its own metadata/key/file contract below. | Inspection, verification or deduplication evidence. |

An arbitrary raw file is not a tarstream source. File names do not select a carrier format. Multi-Manifest Bundles need a root selector; explicit identities remain binding even when optional content verification is disabled.

## Command-line interface

### Shared flag

```text
Shared flag on subcommands that accept configuration:
  --manifest-config string    Manifest YAML path; overrides MANIFEST_CONFIG
```

Commands that use configuration require either `--manifest-config` or `MANIFEST_CONFIG` to name a valid YAML file. **Exceptions:** `config generate`, local-file/stdin `info`, and local-file `diff` do not need a configuration file. See [manifest-config.yaml](#manifest-configyaml) for the schema. The flag belongs after the subcommand, not before it.

### Subcommands

| Command | Purpose |
|---|---|
| `store` | Ingest data, upload the resulting Manifest, and return its hex content key. |
| `load` | Read and transform logical sources into tarstream, Bundle and/or Store outputs. |
| `get-manifest` | Fetch raw Manifest bytes by hex key, chiefly for inspection. |
| `info` | Print Manifest metadata without unsealing its key table. |
| `verify` | Verify the stored Manifest and all nonzero chunks through the fetch path. |
| `diff` | Compare chunk overlap between two local Manifest files. |
| `config show` | Print parsed configuration as YAML. |
| `config generate` | Print a commented configuration template. |

Data sources and Manifest keys are **positional arguments**. Omitting the `store` input or the `info` source, or passing `-`, selects stdin. `get-manifest` and `verify` accept either a bare 64-character lowercase hex key or `manifest://<key>`. **Remote `info` requires the `manifest://` prefix**; a bare hex argument there is treated as a local filename. Put flags before positional arguments: Go's standard `flag` parser stops at the first non-flag argument.

`load` accepts the logical references described in its command section. The complete canonical [reference grammar](artifacts.md#reference-syntax-and-named-locations) belongs to the data contract.

### `manifest-ctl store` — ingest

```text
manifest-ctl store [flags] <REF|->        # omitted input or "-" means stdin

Flags:
  --extra-salt string         Extra salt bytes mixed into the store's opaque salt
  --no-progress              Suppress live progress
```

The input is one selected logical source: a local tarstream path, an identity-bearing or located file reference, a Bundle selector, or a Manifest key/ref. A Bundle with multiple Manifests requires `@manifest:<key>`. Files are opened as bounded random-access views; stdin uses tarstream's one-pass `SourceFrom` protocol. Both paths use the shared `pkg/manifest/transfer` reader and writer. Holes come from authoritative carrier metadata. `crypto.local=off|auto|required` selects the existing local tarstream encryption policy.

Store also accepts `--ref-location`, `--strip-tail`, `--extract-tail PATH` and `--append-tail PATH` with the same meanings as load below. Source integrity, original sequential marker/trailer/EOF and requested output-file completion are checked before publishing the Store root Manifest.

Stdout contains one 64-character hex line: the uploaded Manifest content key. Stderr contains live progress and a human-readable final summary. **`--no-progress` suppresses live progress, not the final summary.** An illustrative summary is:

```text
image size:   10.0 GiB
stored bytes: 1.0 GiB
chunks:       stored=2048 dedup=18432 zero=0
compression:  raw=512 snappy=19968 logical=10.0 GiB encoded=2.1 GiB saved=7.9 GiB
manifest:     encoding=raw logical=1.8 MiB stored=1.8 MiB
manifest key: a1b2c3d4...
```

Examples assume a valid configuration and customer key. `disk.img` and `snap.bin` below are already tarstream artifacts, despite their filenames; abbreviated keys elsewhere in this guide are placeholders for complete keys.

```bash
# Artifact file → Manifest key
MKEY=$(manifest-ctl store disk.img)

# Artifact stdin → Manifest key
cat disk.img | manifest-ctl store > disk.key

# Docker archive → flatten → ingest; upload-only stdout is the Manifest key
docker save myapp:v1 | flatten-ctl export --upload > app.key

# Extra salt narrows the deduplication domain; flags precede the input
manifest-ctl store --extra-salt "tenant-xyz" snap.bin

# Change chunking or crypto policy through YAML
```

### `manifest-ctl load` — read data

```text
manifest-ctl load [flags] REF...

Args:
  REF...                      Top-to-bottom layers: a bare key, manifest://,
                              local paths/file:// tarstream, or Bundle @manifest ref
Flags:
  --output string            Output path (default "-", stdout; refuses a terminal)
  --output-mode string       none, tarstream, or bundle (default tarstream, or
                             none when --store is present)
  --store                    ingest the final transformed logical source
  --extra-salt string        domain separation for Store and/or Bundle ingest
  --ref-location N=file:///P map a named file location (repeatable; P absolute)
  --strip-tail               remove a suffix ZIP from every input before layering
  --extract-tail PATH        save the original first input suffix ZIP
  --append-tail PATH         append a validated suffix last; existing tails require strip
  --name string              Payload tar entry name (default "image")
  --offset uint              Start of the logical window
  --length uint              Window length (0 = the remaining bytes)
  --no-progress              Suppress live progress
```

References are layered in argument order with `fetch.NewLayered` (first is topmost). Data and explicit zero bytes cover lower layers; holes remain transparent. An ordinary local path is accepted directly; a 64-character hexadecimal key retains its Manifest meaning. Use `./<name>` for a local filename that is itself a complete key. File identity qualifiers are verified and named locations use the shared `manifest.RefLocations` parser.

The operation order is: inspect/extract the original top tail; strip each source tail when requested; layer; apply offset/length; append the new tail. A bare source can accept `--append-tail` directly. Existing tails require `--strip-tail`; recognizable malformed tails fail. `--output-mode=none --extract-tail PATH` extracts only the bounded original metadata. Tail operations address the logical content, not the outer Bundle archive. Retained old/new tail bytes share a 64 MiB budget; decoded ZIP metadata is bounded separately.

`--store` writes the final transformed source and defaults file output to `none`. It combines with either file output mode. Store+Bundle obtains one admission and uses one Chunk/Manifest encoding stream with bounded backpressure, so both outputs have identical physical objects and root keys for the same options. `--extra-salt` applies to both Bundle-only and Store writes. Store+tarstream uses a bounded pipe to feed the existing ingest protocol, including when input is stdin.

With a named output file, the Store key is printed on stdout. With binary stdout, identifiers and diagnostics go to stderr. Bundle output reports its root key on stderr. Requested files are created exclusively and written directly; failures clean up only still-owned incomplete files. Existing files, input/output aliases and conflicting output paths are rejected before writing. The returned success covers source validation, file closure and Store-root publication. A later target failure may leave an independently completed file or already-written chunks; it never authorizes changing an old source.

Tarstream output preserves sparse metadata. `crypto.local=off` emits plaintext; `auto|required` emit encrypted v1. Non-seekable stdin uses tarstream; random-access Bundle input is supplied by a file/ref. No complete image buffer or staging file is created by these transfer paths.

New tarstream output always uses [presence v1](artifacts.md#page-presence-and-zero-transport): the final payload before an optional declared metadata tail is limited to 1 TiB; empty and byte-sized dense windows are supported without padding. Each Hole must start on a page boundary and end on a page boundary or at the exact payload end. An `--offset`/`--length` window or old input carrier with unrepresentable Hole boundaries is rejected before tarstream bytes are written, without changing the boundaries or re-encoding in the old format. Existing old tarstreams remain readable. These tarstream output constraints do not restrict byte-granularity Store-only or Bundle output.

```bash
# Read and merge explicit top-to-bottom layers
manifest-ctl load --output merged.tar A B C

# Write matching Bundle and Store objects in one operation
manifest-ctl load --store --output-mode=bundle --extra-salt tenant-a \
  --output merged.bundle A B C

# Replace the metadata tail after payload merging
manifest-ctl load --strip-tail --append-tail new-tail.zip \
  --output-mode=bundle --output merged.bundle A B C

# Extract metadata without writing payload bytes
manifest-ctl load --output-mode=none --extract-tail metadata.zip SOURCE

# stdin can feed Store and a tarstream output together
cat source.tar | manifest-ctl load --store --output-mode=tarstream \
  --output copied.tar -
```

#### Transfer identities and reconstructed boundaries

Explicit tarstream `@digest`/`@hmac` identities and Bundle `@manifest` selectors remain binding when ordinary `manifest.verify_content` is disabled. Content conversion completes carrier verification before reporting success. Carrier selection uses its bytes or an explicit selector rather than the filename extension. When a carrier declares a tail, extraction and stripping require that boundary to match the validated ZIP suffix.

For an unchanged complete Manifest/Bundle root, tarstream output recovers the boundary of a valid relative ZIP suffix. This preserves canonical image envelope identities across Store roundtrips; existing tarstream declarations and explicit windows retain their boundaries. Store statistics count new Manifest bytes only when the final Put creates the object. Tarstream-only loads report logical progress unless `--no-progress` is selected.

### `manifest-ctl get-manifest` — retrieve Manifest bytes

Fetch the raw Manifest object, mainly for debugging or offline `info`/`diff`. Retrieval follows the configured cache/store path.

```text
manifest-ctl get-manifest [--output -|FILE] <hex|manifest://hex>
```

```bash
# Inspect fetched bytes; alternatively use info manifest://<hex>
manifest-ctl get-manifest a1b2c3d4... | manifest-ctl info -

# Save the physical Manifest verbatim
manifest-ctl get-manifest --output disk.manifest a1b2c3d4...
```

### `manifest-ctl info`

Read and summarize a Manifest from one of three sources: a local path, stdin (`-` or omitted), or `manifest://<hex>` through the configured remote reader. Local/stdin inspection needs neither a configuration, customer key nor store connection. Remote metadata retrieval needs configuration and connectivity but does not unseal the customer-key-protected key table.

```text
manifest-ctl info [--manifest-config <path>] <file|-|manifest://hex>
```

Illustrative output:

```text
version:       1
image size:    10.0 GiB (10737418240 bytes)
chunk mode:    cdc
chunk count:   20480
zero chunks:   0 (0 B)
holes:         0 (0 B)
min chunk:     128.0 KiB    (configured)
max chunk:     1.0 MiB    (configured)
avg chunk:     512.0 KiB

size distribution (data chunks):
       min(N)          P1          P5         P25         P50         P75         P95         P99      max(N)
  63.5K(1)       128.0K      256.0K      384.0K      512.0K      640.0K      896.0K        1.0M     1.0M(412)
  (max == configured max — 412/20480 (2.0%) chunks are forced CDC cuts)
key table:     655389 bytes (sealed)
manifest size: 1802334 bytes
```

The minimum/maximum marked `(configured)` are the bounds stored in the Manifest header, not measured extremes. The percentile distribution below covers nonzero data chunks. Parentheses in `min(N)`/`max(N)` count chunks at each observed extreme. The final diagnostic appears only when the measured maximum equals the configured cap, identifying forced-size CDC cuts.

`manifest-ctl info manifest://<hex>` is the direct equivalent of `get-manifest … | info -` for remote inspection.

### `manifest-ctl verify`

Fetch and decrypt every nonzero chunk to verify the entire Manifest's availability and content. This command forces physical SHA-256 checking even if ordinary reads disable it.

```text
manifest-ctl verify [--no-progress] a1b2c3d4...        # or manifest://a1b2c3d4...
```

```text
verified: 20480  skipped(zero): 0  holes: 0  failed: 0  total chunks: 20480
```

### `manifest-ctl diff` — deduplication analysis

```text
manifest-ctl diff <manifest-a> <manifest-b>
```

Both arguments are local Manifest files, which can be obtained with `get-manifest`. `diff` neither opens a store nor loads a configuration or customer key.

`shared` and `only in A/B` count distinct nonzero ContentKeys, not repeated entry positions. Their byte totals sum each unique chunk's logical size once, not compressed/encrypted Store bytes. Zero entries and holes are excluded from these sets; image size, raw entry count and holes remain separately reported. Conflicting logical sizes for the same ContentKey, within or across inputs, are rejected.

The CLI's `dedup ratio` is `1 - |A ∪ B| / (|A| + |B|)`, with zero for an empty denominator. Pairwise overlap (Jaccard), used by the warm-pool workload, is instead `shared / (shared + onlyA + onlyB)`. Neither is a universal VM-memory saving guarantee. Illustrative output:

```text
manifest A:  10.0 GiB, 20480 chunks (20480 unique)
manifest B:  10.1 GiB, 20512 chunks (20512 unique)
shared:      18432 chunks (9.0 GiB)
only in A:   2048 chunks (1.0 GiB)
only in B:   2080 chunks (1.1 GiB)
dedup ratio: 45.0%
```

### `manifest-ctl config`

```text
manifest-ctl config show     [--manifest-config <path>]
manifest-ctl config generate
```

`generate` prints a commented template. `show` prints the loaded configuration as YAML; it does not prove that all backend connections and deferred constructors will succeed.

## Configuration

### manifest-config.yaml

```yaml
manifest:
  key: "0a1b2c3d..."              # Placeholder: a 32-byte hex customer key seals the key table
  verify_content: true           # Recheck physical Manifest/Chunk SHA-256; default true
  # write_generation: G3         # Optional write generation
store:
  endpoint: 127.0.0.1:7100        # Required for remote writes/direct-store reads; optional for offline Bundles
                                 # TCP host:port or UDS (/run/sandbox/store.sock or unix:///...)
  pool: 4                        # Independent grpc.ClientConn instances, round-robin
  timeout: 5s                    # Per-store-RPC timeout
cache:
  endpoint: 127.0.0.1:7070        # Empty selects direct store; otherwise use cache wire; UDS also accepted
  pool: 4
  timeout: 2s
chunker:
  mode: cdc                      # cdc | fixed
  cdc:
    min: 128KiB
    avg: 512KiB
    max: 1MiB
  fixed:
    size: 512KiB
crypto:
  chunk: aes                     # Only supported value
  manifest: aes                  # Only supported value
  local: off                     # off | auto | required; default off
  ref: auto                      # off | auto | required, independent of local
```

Fields:

- `manifest.key` is the customer key that **seals the Manifest's key table**; it does not derive the chunk encryption key. Losing it prevents unsealing that Manifest's table. It may be omitted from YAML and supplied through **nonempty `MANIFEST_KEY`**, the primary delivery mechanism. A nonempty environment value overrides YAML; an empty one falls back to YAML. The key is resolved lazily and not written back into Config, so `config show` does not echo the environment-provided key. **A key explicitly stored in YAML remains in Config and can be printed by `show`.** If both sources are empty, operations needing the key fail.
- `manifest.verify_content` defaults to `true`. Ordinary Bundle/cache/store Fetchers, `GetManifestBlob` and `CheckManifest` share the policy. When enabled, they check `SHA256(physical Manifest) == requested key` before parsing and `SHA256(physical Chunk) == CiphertextHash` when first reading a chunk. When disabled, ContentKey is a locator and those complete-object hash scans are skipped. The process emits one clear warning. Envelope/geometry checks, key-table GCM authentication, chunk format/length validation, decryption and decompression remain. Chunk AES-CTR itself is not authenticated encryption; disabling the physical hash check removes that chunk-content integrity check. `manifest-ctl verify`, full Bundle verify/upload/export and object repair always force SHA verification.
- `manifest.write_generation` optionally selects a write generation. With `store.endpoint`, empty means `AdmitWrite()` selects the latest admitted generation; a value uses `AdmitWriteFor(generation)` and fails if it was removed. A Store RPC failure does not fall back to local derivation. An offline Bundle writer without Store uses the ordinary generation name `NONE` when unset, or the explicit value otherwise; both derive salt with public `store.SaltForGeneration`. `NONE` is neither reserved nor a special protocol value.
- `store.endpoint` points at the store-ctl gRPC service for remote object I/O. It accepts TCP `host:port` or a Unix socket. Bare `/run/sandbox/store.sock` is normalized to `unix:///run/sandbox/store.sock`; an explicit `unix:` target is passed through. Match store-ctl's `listen` address. Same-host UDS avoids TCP transport; local Bundle object I/O follows the separate Bundle reader/writer contract.
- `cache.endpoint`: empty selects direct-store reads; nonempty selects cache-ctl's wire protocol. It accepts `host:port`, explicit Unix targets or a bare socket path, matching cache-ctl's `listen`. This is source selection at construction, not error-triggered failover around a configured cache.
- `chunker.mode` is `cdc` (FastCDC) or `fixed`; see [Chunking](artifacts.md#chunking).
- `crypto.chunk` and `crypto.manifest` each accept only `aes`; other values fail when the crypto objects are constructed ([Chunk encryption format](artifacts.md#chunk-encryption-format)–4.4).
- `crypto.local` is a local-format compatibility/enforcement policy, not an algorithm selector. `off` does not enable the local codec; `auto` reads legacy plaintext or encrypted carriers; `required` accepts only encrypted carriers. The default is `off`.
- `crypto.ref` controls tarstreams in sandboxer named ref locations. Its independent default, `auto`, accepts plaintext or correctly keyed encrypted inputs and encrypts new output with the same customer key. Existing plaintext dependencies can remain in a published graph. `off` accepts and writes plaintext only. `required` accepts and writes encrypted tarstreams only. Manifest and Bundle keep their existing encoding.

Enable this policy with a sandboxer version that implements ref-location reads, reuse and publication. Older loaders can ignore an unknown field. With a supporting sandboxer, upgrading an existing `local: off` configuration makes new ref writes require a key. To preserve the former publication behavior, set `ref` to the previous `local` value. In particular, set `ref: required` to keep rejecting plaintext ref inputs after upgrading from `local: required`.

### Loading order

The **configuration filename** comes only from the CLI flag or its environment variable; there is **no automatic discovery**:

| Source | Precedence |
|---|---|
| `--manifest-config FILE` | First, when nonempty. |
| `MANIFEST_CONFIG` | Used when the flag is empty. |
| Neither | Error for commands that need configuration; [Shared flag](#shared-flag) lists the exceptions. |

The **customer key** has a separate precedence rule: nonempty `MANIFEST_KEY` overrides `manifest.key` from YAML. Thus a shared config file can omit the key while the command receives it separately.

`pkg/manifest.ParseConfig` also accepts **in-memory YAML bytes**, for example configuration delivered to sandbox-ctl over a config socket. Endpoint and crypto settings need not be written to disk. A caller may set the key separately in `Config.Manifest.Key`; the environment override still applies when `CustomerKey()` is used.

There are no per-field command-line overrides for chunker or crypto settings. Change the YAML to change those settings.

## Using the shared Go entry points

`pkg/manifest/transfer.Reader.Open` returns an owned `Opened` sparse source. Complete `Opened.Verify(ctx)` before success, then close it and propagate its close error; closing the Reader separately releases its remote fetcher and key material. `transfer.Write` applies the requested carrier/Store outputs to a sparse source; its `BeforeCommit` callback completes caller validation before publishing a Store root. `manifest.RefLocations` provides the same validated location mapping used by the CLI and sandboxer. Keep input resources stable and open until completion, propagate terminal validation/Close errors, and do not publish failed outputs. The format and read-completion rules are defined in [artifacts](artifacts.md). The public API comments in [`pkg/manifest/transfer`](../pkg/manifest/transfer/) define the exact Go options.

## Related contracts

- [store](store.md): manifest-ctl writes remote bytes through store-ctl gRPC.
- [cache](cache.md): optional acceleration for reads; cache-ctl can use the same store client as its origin.
- [guest-runtime flatten](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli.md): image flattening followed by `flatten-ctl export --upload`.
- [sandboxer](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/runtime.md): disk bases and snapshots referenced with Manifest keys.
- [system architecture](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/architecture.md): the Manifest abstraction in the overall platform.

Manifest source selection and lazy recovery are specified in [Source selection and lazy recovery](artifacts.md#source-selection-and-lazy-recovery).
