# Kuasar Sandbox Accelerator

[简体中文](README.md) · [Kuasar Sandbox project](https://github.com/kuasar-sandbox/kuasar-sandbox)

`accelerator` is the data-access, storage, encryption, and cache foundation used by Kuasar Sandbox images, snapshots, and sparse artifacts. It provides composable primitives for local files, shared file storage, manifest/object-storage paths, content organization, OCI image ingestion, and filesystem-image construction.

The component is broader than chunking or deduplication. Deployments can select the data model that fits the artifact and infrastructure instead of forcing every snapshot or image through one storage backend.

`accelerator` can be used as part of the complete Kuasar Sandbox platform or integrated independently. The project repository owns system-level architecture, cross-component BMS/E2E, demos, and aggregate release selection.

## Data paths

Kuasar Sandbox supports three principal data paths.

### Local files

Local file references provide a short path for single-node deployments, local NVMe, development environments, and workloads with explicit node affinity.

### Shared file storage

Snapshots and other artifacts can remain normal files on NAS, NFS, or another trusted shared filesystem. Multiple nodes can use the same logical file location without first converting the artifact into content chunks.

### Manifest, object storage, and layered caches

For large-scale distribution, remote persistence, and cross-node restore, data can be described by a manifest and stored in a filesystem or S3-compatible object backend. Readers fetch only the ranges they need and can use node-local and distributed caches.

These paths are alternatives and can coexist. For example, a deployment may use shared files for mutable snapshots and object storage plus caches for stable images.

## Main capabilities

- common references for local and remote artifact locations;
- random and sequential access to regular and sparse data;
- explicit handling of data, zero ranges, and holes to avoid unnecessary I/O;
- local artifact formats with integrity and optional encryption protection;
- content-defined or fixed organization of suitable data into addressable objects;
- manifests that describe logical sparse content independently of one storage backend;
- filesystem and S3-compatible stores;
- node-local, sharded, and layered cache services;
- OCI image pulling and credential-aware registry access;
- deterministic filesystem-image construction, including EROFS-based roots where configured;
- fetch and prefetch interfaces for on-demand memory-page and disk-block consumers;
- generation/security-domain controls for data lifecycle and sharing scope.

The exact package, service, CLI, URI, and on-disk/wire-format contracts are documented in this repository's `docs/` tree.

## Snapshot and deduplication model

Snapshot efficiency is not based on assuming that independently executed virtual machines will produce identical memory.

For template fan-out, the system explicitly shares the immutable parent layers and keeps each instance's changes in separate child layers. That structural reuse exists regardless of the content-level duplicate ratio.

Content addressing and deduplication remain useful for artifacts with stable repeated content, such as base images, language runtimes, toolchains, and common image blocks. Their scope must follow the configured security domain; identical bytes do not automatically imply that data should be shared across unrelated tenants.

## Encryption and trust boundaries

The component supports protection of local and remote artifacts, manifests, and stored content. Public documentation and examples must keep these distinctions clear:

- API or platform identity credentials are not the same as content-protection keys;
- content-protection keys must not be exposed to the guest merely because the guest consumes the data;
- caches and stores belong to an explicit trust and sharing domain;
- content-addressed encryption can reveal content equality or existence depending on the construction and deployment policy;
- test keys, salts, endpoints, buckets, and credentials must be fixed non-production examples;
- local files, shared storage, object stores, and cache services require their own access-control and operational hardening.

Do not describe any encryption mode as providing absolute confidentiality under every threat model.

## Component boundaries

| Component | Accelerator interaction |
| --- | --- |
| [`sandboxer`](https://github.com/kuasar-sandbox/sandboxer) | Consumes file/manifest references and sparse data for memory, block-device, snapshot, and prefetch paths |
| [`orchestrator`](https://github.com/kuasar-sandbox/orchestrator) | Configures stores/caches, coordinates template and snapshot references, and supplies identity/key material according to deployment policy |
| [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime) | Provides runtime and kernel inputs and image-building consumers |
| [`connector`](https://github.com/kuasar-sandbox/connector) | Has no data-plane dependency on storage internals; both components integrate through the higher-level platform |
| [`kuasar-sandbox`](https://github.com/kuasar-sandbox/kuasar-sandbox) | Selects exact versions and validates the complete storage/runtime path |

Heavy dependencies such as object-storage SDKs, RocksDB or other native storage libraries, compression implementations, and EROFS/OCI tools remain owned by this component rather than leaking into unrelated component APIs.

## Build and test

This repository contains Go code and optional native dependencies. For a standalone checkout, keep workspace overrides disabled unless you intentionally use the six-repository sibling workspace:

```bash
GOWORK=off go test ./...
GOWORK=off go build ./...
```

Use the repository `Makefile`, current Chinese README, and `docs/` tree as the authoritative source for:

- native dependency setup;
- generated files and CLI/service targets;
- filesystem and S3-compatible Store tests;
- cache service tests;
- OCI/EROFS tool prerequisites;
- integration and performance-test environments.

### Test levels

- Unit and local-filesystem tests must not require production cloud credentials.
- Object-storage tests should use an explicitly configured S3-compatible endpoint or an isolated local substitute.
- Tests that require real credentials must be restricted to trusted CI and must not expose them to external fork code or logs.
- Cross-component image, snapshot, and restore paths run through the project BMS using an exact source set.

A cached dependency or internal endpoint on a maintainer machine must not be the only way a documented build succeeds.

## Documentation

- [Project English overview](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/README_EN.md)
- [English Quick Start](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart_en.md)
- [Project architecture](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/kuasar-sandbox.md)
- [English release overview](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/releases_en.md)
- [Manifest documentation](docs/manifest.md)
- [Store documentation](docs/store.md)
- [Cache documentation](docs/cache.md)

Detailed design documents may remain Chinese during the initial source-publication phase. Build prerequisites, public data contracts, security boundaries, and release usage must retain an accurate English entry point.

## Releases

The component publishes independent accelerator versions. Aggregate Kuasar Sandbox releases select one exact accelerator version together with exact connector, sandboxer, orchestrator, Runtime, and VMLinux versions.

- [Component releases](https://github.com/kuasar-sandbox/accelerator/releases)
- [Aggregate releases](https://github.com/kuasar-sandbox/kuasar-sandbox/releases)

Use one complete aggregate release. Do not mix similarly named archives from different stable or preview source sets.

## Contributing

Read the project [English contribution guide](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/CONTRIBUTING_EN.md). Keep format, crypto, reference, manifest, and storage-interface changes focused and document compatibility. Exported contracts that affect sandboxer or orchestrator may require linked companion pull requests and exact-source-set validation.

## Security

Do not report vulnerabilities in public issues. Use the project [English Security Policy](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/SECURITY_EN.md) and GitHub private vulnerability reporting.

Never publish real object-storage credentials, tenant keys, manifests containing sensitive names, internal endpoints, customer artifacts, cache contents, or unredacted request headers/logs.

## License

Project-owned code is licensed under the [Apache License 2.0](LICENSE). Native libraries, compression code, object-storage SDKs, OCI/EROFS tools, generated files, and other third-party material retain their own copyright, license, notice, and redistribution requirements. Consult the repository's license-scope and release-notice documentation before redistributing binaries.