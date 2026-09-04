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
make build                      # all component binaries
make build TARGET_ARCH=aarch64  # cross-compile; amd64/arm64 aliases are accepted
make vet                        # validate the thin downstream-facing Go surface
make test                       # unit tests and backend tests
make e2e                        # OCI/Zot retrieval and flattening coverage where documented
make test-e2e                   # component owner suite; requires the assembled project BIN
```

Go and native prerequisites vary by target. The current source tree documents and builds any native cache dependencies through repository scripts. Real object-storage tests must use explicit test credentials or a local S3-compatible service; ordinary unit and local-filesystem tests must not require production cloud credentials.

## Running independently

The three services can be deployed independently of the full platform:

- use `manifest-ctl` to ingest or fetch sparse artifacts;
- run `store-ctl` with a filesystem root or an S3-compatible endpoint;
- run `cache-ctl` in a local, sharded, or tiered topology;
- use the exported packages in another Go service without importing heavy backends.

Configuration examples must use local paths, documentation-reserved endpoints, and placeholder credentials. Production deployments should use protected endpoints, explicit resource budgets, durable authoritative storage, and observability appropriate to the selected cache topology.

## Release model

This repository publishes independent component versions named `vX.Y.Z`. The x86_64 component archive contains the three service binaries and the documented operational/performance helper scripts selected by the release contract. Component design documents and E2E sources are collected from the selected tag into the project platform archive.

The project repository publishes aggregate versions named `release-vX.Y.Z`, selecting exact versions of `accelerator` and the other release units, validating their assets, and running cross-component tests. Aggregate and component version numbers are independent.

See the [project release documentation](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release.md) and the [latest Stable aggregate release](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest).

## Documentation

Detailed design and reference documents are currently maintained primarily in Chinese:

- [`docs/manifest.md`](docs/manifest.md) — sparse manifests, ingest/fetch, chunking, encryption, key tables, and public SDKs;
- [`docs/store.md`](docs/store.md) — filesystem and S3-compatible stores, generation layout, integrity, and garbage collection;
- [`docs/cache.md`](docs/cache.md) — local, sharded, tiered, and erasure-coded caches;
- [`docs/cache-redis.md`](docs/cache-redis.md) — Redis-compatible UDS/TCP backend and external-service deployment examples.

OCI/directory-to-EROFS CLI usage is documented with [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime) because `flatten-ctl` is part of the Runtime release unit.

The English README contains the complete public component entry path. Translating every detailed design document is not required to build or contribute to the component.

## Project boundaries

- MicroVM lifecycle and snapshot execution belong to [`sandboxer`](https://github.com/kuasar-sandbox/sandboxer);
- network allocation and forwarding belong to [`connector`](https://github.com/kuasar-sandbox/connector);
- the guest kernel, runtime image, and `flatten-ctl` release artifact belong to [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime);
- node and cluster orchestration belong to [`orchestrator`](https://github.com/kuasar-sandbox/orchestrator);
- system-level design, shared BMS, demos, and aggregate releases belong to [`kuasar-sandbox/kuasar-sandbox`](https://github.com/kuasar-sandbox/kuasar-sandbox).

## Contributing and security

Read the [organization contribution guide](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md). Changes to public data formats or downstream package contracts require linked companion pull requests and exact-source project validation.

Do not report vulnerabilities or real storage credentials in public issues. Use the [Kuasar Sandbox Security Policy](https://github.com/kuasar-sandbox/kuasar-sandbox/security/policy) and GitHub private vulnerability reporting.

## License

Original project content is licensed under the [Apache License 2.0](LICENSE). Preserve the license, attribution, NOTICE, and source obligations of native libraries, vendored code, generated code, formats, and third-party tools included in source or release assets.
