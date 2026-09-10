[English](README.md) | [简体中文](README_zh.md)

# accelerator

`accelerator` is the **data access, storage, encryption, and cache infrastructure** for images, snapshots, and sparse artifacts in [Kuasar Sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox).

It provides reusable sparse-data and artifact abstractions, local and shared-file access, Manifest-backed content organization, filesystem and S3-compatible stores, layered caches, integrity and encryption, OCI retrieval, deterministic EROFS image flattening, on-demand fetch, and prefetch.

Content addressing and deduplication are available capabilities, not the component's only purpose. Their sharing scope is controlled by deployment security domains and key material. A deployment can use direct files for snapshots, Manifest/object storage for large-scale distribution, or a combination of both.

## Data paths

| Data path | Typical use | Characteristics |
| --- | --- | --- |
| Local files | Standalone nodes, local NVMe, temporary or node-affine artifacts | Direct file semantics and a short access path |
| Shared filesystems | NAS, NFS, or another consistently mounted filesystem | Native file access shared across nodes; no mandatory chunk conversion |
| Manifest and object storage | Large-scale distribution, remote persistence, migration, and tiered caching | Content-addressed objects, metadata-described sparse ranges, and on-demand reads |

These paths expose common logical data and reference semantics to downstream components. `sandboxer` can therefore consume an image or snapshot without making the lifecycle API specific to one physical backend.

## Public Go surface

The downstream import surface is kept small and mostly pure Go, so consumers such as `sandboxer` do not pull storage-server, object-storage SDK, erasure-coding, or database implementations into their binaries.

| Package | Purpose |
| --- | --- |
| `pkg/sparse` | Authoritative Hole/Zero/Data representation and sparse random/sequential reads |
| `pkg/manifest` and subpackages | Content organization, chunking, encryption, ingest, fetch, and prefetch |
| `pkg/store` and `pkg/store/client` | Content-store protocol and client |
| `pkg/cache` and `pkg/cache/client` | Cache protocol, local/sharded/tiered composition, and clients |
| `pkg/tarstream` | Sparse local transport artifact with identity and integrity metadata |
| `pkg/flatten`, `pkg/image`, `pkg/remote`, `pkg/tar` | OCI/directory retrieval, image configuration, and EROFS flattening primitives |

Heavy server backends remain behind component binaries and server packages.
The detailed sparse Run/Stream, chunk-window and local tarstream contracts belong
to [Manifest §4.8–§4.10](docs/manifest.md#48-read-path-in-detail) and
[file artifacts](docs/file-artifacts.md), not a separate README protocol.

## Binaries

| Binary | Purpose | Build model |
| --- | --- | --- |
| `manifest-ctl` | Ingest, inspect, fetch, and transform Manifest-backed artifacts | Pure Go |
| `store-ctl` | Filesystem or S3-compatible content-store service and administration | Pure Go |
| `cache-ctl` | Local, sharded, tiered, and erasure-coded cache service | Uses the backend dependencies selected by the current source tree |

`flatten-ctl` is published with the `guest-runtime` Runtime release unit, while its shared image-flattening implementation is maintained here.

## Storage security

The component supports integrity verification and encryption for data that enters protected artifact and Manifest paths. Platform identity credentials and content-protection keys are separate concerns. Key scope, salt, and deployment security domains determine where encrypted content may be shared or deduplicated.

Do not interpret content encryption as a claim that every threat model leaks no metadata, or that all tenant-private data should be globally deduplicated. Deployments must choose a sharing domain appropriate for the data classification and trust boundary. Direct local or shared-file paths can also apply the project's documented local artifact protection policy.

## Build and test

```bash
make manifest-ctl store-ctl     # pure-Go command-line services
make cache-ctl                  # build the cache service and its current backend dependencies
make cache-ctl NO_ROCKSDB=1     # build cache-ctl without RocksDB (-tags no_rocksdb, CGO_ENABLED=0)
make build                      # all component binaries
make build TARGET_ARCH=aarch64  # cross-compile; amd64/arm64 aliases are accepted
make vet                        # validate the thin downstream-facing Go surface
make test                       # unit tests and backend tests
make test-e2e                   # component owner suite; requires the assembled project BIN
```

Go and native prerequisites vary by target. The current source tree documents and builds any native cache dependencies through repository scripts. Real object-storage tests must use explicit test credentials or a local S3-compatible service; ordinary unit and local-filesystem tests must not require production cloud credentials.

`cache-ctl` normally uses CGO and a locally built static `librocksdb.a` from
`deps/build-rocksdb.sh`; `manifest-ctl`, `store-ctl` and the downstream import
surface do not require RocksDB. `make release VERSION=vX.Y.Z` creates and checks
a local release bundle; the official cache payload must retain RocksDB support.

### Private-network and offline builds

This module has no cross-repository Go dependencies. Point `GOPROXY` at the
deployment's mirror, with its checksum-database policy, or use a populated
verified module cache when offline. Coordinated sibling development can use the
[project workspace](https://github.com/kuasar-sandbox/kuasar-sandbox); it is not a
prerequisite for this component's standalone build.

## Running independently

The three services can be deployed independently of the full platform:

- use `manifest-ctl` to ingest or fetch sparse artifacts;
- run `store-ctl` with a filesystem root or an S3-compatible endpoint;
- run `cache-ctl` in a local, sharded, or tiered topology;
- use the exported packages in another Go service without importing heavy backends.

Configuration examples must use local paths, documentation-reserved endpoints, and placeholder credentials. Production deployments should use protected endpoints, explicit resource budgets, durable authoritative storage, and observability appropriate to the selected cache topology.

## Release model

The release workflow records the completed archive's SHA-256 as a build-job
output before uploading it. The publisher receives that independent value as
`RELEASE_ARCHIVE_SHA256` and checks it before any Tag or Release write; a value
recalculated from the downloaded bundle is not a substitute. This binds every
payload and material file to that completed build, even if the bundle's own
checksums are regenerated. Local packaging and standalone validation do not
require this publication input. The receipt does not attest compiler provenance
or isolate untrusted candidate code.

Go dependency and toolchain downloads use fresh private module/VCS state, an
enabled checksum database and `GOAUTH=off`. They clear persisted Go settings, private-module
bypasses, Git configuration and caller credentials while retaining validated,
credential-free routing. Uploaded Go record keys must match the exact official
payload names before any source or toolchain download; path aliases are rejected.
These release checks do not change ordinary development module authentication.
No organization-owned Go module is exempt from module checksum and notice
validation in this component; it has no separately collected internal Go dependency.
Source inventories reject duplicate or excessive records before per-row work;
each metadata table is capped at 16 MiB and the source inventory at 16,384 rows.
Only project, RocksDB, official per-payload Go toolchain and system-link input
source kinds are accepted. System rows require their own material directory,
a SHA-256 input digest and consistent Debian/RPM source-package/version fields.
These are record-consistency checks; the trusted build verifies actual installed
input/notice bytes, and its independent archive digest binds them for publication.
The host archive parser ignores persisted Go settings and ambient build flags,
uses the local compiler and targets the host instead of an inherited cross target.
RPM notice collection checks both the installed-package listing and every
same-source sibling file listing. A partial failure aborts collection even when
another sibling supplied valid notices; partial output is not complete coverage.
Source metadata is limited to `SOURCES.tsv`, `GO-BUILD-INFO.tsv`,
`GO-MODULES.tsv` and `MATERIALS.sha256`; only license material has dynamic
nested paths. Extra source files/directories are rejected before extraction.

The trusted publisher generates the standard release text and source/Preview
markers from its validated request. Downloaded `release-notes.md` is a local
bundle aid, not an authority for the public release body or reconciliation.

Packaging creates a fresh checkout of the selected Accelerator commit and rebuilds
all three Go payloads and the recipe-pinned RocksDB static library. It rejects
`RELEASE_BIN_DIR` and `RELEASE_ROCKSDB_SOURCE_DIR`; prebuilt binaries, ignored
development files and extracted/native build caches are not reused. Cached
download bytes may be copied into the owned workspace, but the RocksDB recipe
checks the normalized source-tree digest before compiling them. The five shipped
helpers and RocksDB license texts are copied from those fresh selected trees.
Build commands use a private home, temporary directory and Go caches without
cloud/release credentials or inherited build-flag overrides. Credential-free
HTTPS routing, `GOSUMDB` (including a checksum mirror) and `GOTOOLCHAIN` are
preserved; the latter two default to `sum.golang.org` and `local`.

The cache payload's actual linker map must select the newly built
`librocksdb.a`, `libstdc++.a` and `libgcc.a`. Packaging records the RocksDB
library digest and collects each linked system archive/startup object's digest,
Debian/RPM source-package identity and corresponding copyright/license/NOTICE
bytes. Installed inputs and notices must match package-file metadata; ownership
alone is insufficient. These checks do not attest a compromised host or package
database, and do not change RocksDB's backend or durability policy.

Release packaging records the Go compiler selected in the fresh build context,
then compares its distribution inputs before and after building with the matching
`golang.org/toolchain` archive authenticated by the configured checksum database.
This covers the compiler, standard-library sources and other files in that
distribution; extra non-build `api`, `doc`, `misc` and `test` files in a full Go
installation are not authenticated or used as release license sources. The
standard `go.mod`/`_go.mod` installation transformation is accounted for.
Go license/notice bytes, including nested compiler and standard-library dependency
materials, come from the verified archive with their relative paths retained.
Standalone validation
rechecks their bytes, source URL and module h1. A version string or recomputed
bundle checksum cannot substitute for that source check. Verification requires
an enabled checksum database and its matching archive/cache; it may fetch
verification material with `GOTOOLCHAIN=local` but does not switch the build
compiler or silently enable automatic toolchain selection. These checks assume
the trusted build host and do not attest a compromised host.

The archive name records the requested release version. Source records retain
that version only when its local Git tag points to the selected commit; before
tagging they use `git:<commit>`. Validation binds every Go payload and the project
source URL/digest to the same commit. The publisher supplies its expected commit
and rejects a different-source bundle before any Tag or Release write.
Archive validation enumerates the three Go binaries and five shipped helper
scripts, rejects extra or duplicate payload entries, requires Linux/amd64 Go
build targets, and rejects `no_rocksdb` in the official cache payload. Component
license/source material directories remain independently namespaced.
Each CLI must also identify its own `cmd/<name>` main package in the Accelerator
module. Every shipped helper is compared with the selected Git blob, so validation
requires that exact commit in the local object database. The trusted publisher
reads source history without executing candidate helpers. The archive parser
limits regular members to 512 MiB, the entire expanded gzip stream (including
padding) to 1 GiB and the entry count to 20,000 before extraction. Packaging sets
its output umask explicitly; a caller's restrictive umask does not change the
published directory contract. Build and publish retain the same Go routing policy.

The pinned RocksDB source supplies `AUTHORS`, `COPYING`, `LICENSE.Apache` and
`LICENSE.leveldb`; standalone validation compares this complete file set and its
bytes with the verified native-source digest binding, not the bundle's own
checksums. Project notices, including nested `LICENSES`, are reconstructed from
the selected commit's Git blobs and compared in full. Changed, missing and extra
material is rejected without executing candidate source files.
All four RocksDB notices are mandatory archive materials. License collection
refuses unreadable subtrees and incomplete traversals. Distinct native link
inputs cannot overwrite notices under a shared material name. Packaging cleans
its own read-only Go module cache on success and failure without touching other
workspaces. Third-party local Go replacements without authenticated module
checksums are not supported in official packages; use versioned module
replacements. Existing Kuasar sibling replacements and ordinary development
builds are unchanged.

This repository publishes independent component versions named `vX.Y.Z`. The x86_64 component archive contains the three service binaries and the documented operational/performance helper scripts selected by the release contract. Component design documents and E2E sources are collected from the selected tag into the project platform archive.

The project repository publishes aggregate versions named `release-vX.Y.Z`, selecting exact versions of `accelerator` and the other release units, validating their assets, and running cross-component tests. Aggregate and component version numbers are independent.

Release branches, Preview, Latest reconciliation and concurrency/cancellation
handling are owned by the [project release documentation](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release.md).
For ordinary use, start from the [latest Stable aggregate channel](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest).

## Documentation

Manifest, Store and Cache have complete English and Chinese editions. Existing English backend guides remain available:

- [Manifest — English](docs/manifest.md) / [Chinese](docs/manifest_zh.md) — sparse manifests, ingest/fetch, chunking, encryption, key tables, and public SDKs;
- [File artifacts and carriers](docs/file-artifacts.md) — immutable tarstream encryption, Bundle layout, access and publication.

- [Store — English](docs/store.md) / [Chinese](docs/store_zh.md) — filesystem and S3-compatible stores, generation retention, integrity, and explicit purge (not reachability GC);
- [Cache — English](docs/cache.md) / [Chinese](docs/cache_zh.md) — local, sharded, tiered, and erasure-coded caches;
- [`docs/cache-redis.md`](docs/cache-redis.md) — Redis-compatible UDS/TCP backend and external-service deployment examples.

OCI/directory-to-EROFS CLI usage is documented with [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime) because `flatten-ctl` is part of the Runtime release unit.

The README provides the public component entry path; the complete design guides contain formats, operating limits, failure behavior and measurement requirements.

## Project boundaries

- MicroVM lifecycle and snapshot execution belong to [`sandboxer`](https://github.com/kuasar-sandbox/sandboxer);
- network allocation and forwarding belong to [`connector`](https://github.com/kuasar-sandbox/connector);
- the guest kernel, runtime image, and `flatten-ctl` release artifact belong to [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime);
- node and cluster orchestration belong to [`orchestrator`](https://github.com/kuasar-sandbox/orchestrator);
- system-level design, shared integration tests, demos, and aggregate releases belong to [`kuasar-sandbox/kuasar-sandbox`](https://github.com/kuasar-sandbox/kuasar-sandbox).

## Contributing and security

Read the repository-specific [contribution guide](CONTRIBUTING.md) and the [organization contribution guide](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md). Changes to public data formats or downstream package contracts require linked companion pull requests and exact-source project validation.

Do not report vulnerabilities or real storage credentials in public issues. Use the [Kuasar Sandbox Security Policy](https://github.com/kuasar-sandbox/kuasar-sandbox/security/policy) and GitHub private vulnerability reporting.

## License

Original project content is licensed under the [Apache License 2.0](LICENSE). Preserve the license, attribution, NOTICE, and source obligations of native libraries, vendored code, generated code, formats, and third-party tools included in source or release assets.
