[English](README.md) | [简体中文](README_zh.md)

# accelerator

For the first cross-component sandbox or Demo, follow the project
[Quick Start](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart.md)
with the matching aggregate release and
[workbench](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/workbench/README.md).
This component can still be built/deployed independently; workbench is not a
production runtime dependency. Published architectures are specific to the
selected release; source support does not imply all historical assets exist.

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
| `app/store` and `app/cache` | Complete reusable Store/Cache service lifecycles for official and custom executables |
| `pkg/tarstream` | Sparse local transport artifact with identity and integrity metadata |
| `pkg/flatten`, `pkg/image`, `pkg/remote`, `pkg/tar` | OCI/directory retrieval, image configuration, and EROFS flattening primitives |

Heavy server backends remain behind component binaries and server packages.
The detailed sparse Run/Stream, chunk-window and local tarstream contracts belong
to [Manifest sparse read path](docs/artifacts.md#read-path-in-detail) and
[file artifacts](docs/artifacts.md), not a separate README protocol.

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

## Choose a complete task or contract

| Need | Authoritative guide |
| --- | --- |
| Ingest, fetch, inspect, transform or compare an artifact | [Transfer](docs/transfer.md), including a complete local Store roundtrip and exact manifest-ctl options |
| Implement sparse readers/writers or interpret a reference/carrier | [Artifacts](docs/artifacts.md): sparse model, identity, Chunk/Manifest, encryption, tarstream and Bundle |
| Deploy or embed authoritative persistence | [Store](docs/store.md): FS/S3, generations, visibility, administration and App lifecycle |
| Deploy or embed acceleration and diagnose misses/recovery | [Cache](docs/cache.md): tiers, sharding/EC, RocksDB/Redis backends and App lifecycle |
| Prepare an OCI, archive or directory image through the shared library | [Image preparation](docs/image-preparation.md): lifetime, image payload, layout and publication |

[flatten-ctl operations](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli.md) belong to guest-runtime, which publishes the executable. Each guide has a complete Chinese edition via its language selector.

## Running independently

The three services can be deployed independently of the full platform:

- use `manifest-ctl` to ingest or fetch sparse artifacts;
- run `store-ctl` with a filesystem root or an S3-compatible endpoint;
- run `cache-ctl` in a local, sharded, or tiered topology;
- link `app/store.Run` or `app/cache.Run` into a custom service binary. The official CLIs are peer callers of these same lifecycle functions; custom binaries do not exec or depend on `store-ctl`/`cache-ctl`.

Configuration examples must use local paths, documentation-reserved endpoints, and placeholder credentials. Production deployments should use protected endpoints, explicit resource budgets, durable authoritative storage, and observability appropriate to the selected cache topology.

## Build and validate changes

From an Accelerator checkout with the documented Go/native prerequisites:

```bash
make manifest-ctl store-ctl
make cache-ctl
make test
```

Use [Contributing](CONTRIBUTING.md#build-and-test) for toolchain prerequisites, cross/offline builds, the no-RocksDB option, ABI/native checks, image fixtures, package validation and complete prepared E2E commands. A unit or fixture pass does not establish real-cloud or VM coverage.

## Select a released product

This component publishes `vX.Y.Z`; aggregate `release-vX.Y.Z` versions select exact independent component tags. Use the [Stable aggregate channel](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest) for a validated combination. Exact artifact inventories, native material collection and packaging checks are in [component release preparation](CONTRIBUTING.md#build-and-package-a-component-release). Shared Preview retention, cleanup, Latest reconciliation and release-line rules have one authority in the [project release contract](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release.md).

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
