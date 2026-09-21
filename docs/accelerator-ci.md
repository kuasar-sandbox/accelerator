# Accelerator CI requirements and checks

[English](accelerator-ci.md) | [简体中文](accelerator-ci_zh.md)

The [platform CI contract](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci.md)
owns shared runner routing, profiles, workflow entry points and rollout status.
This guide records accelerator build requirements and the evidence provided by
its checks.

## Build and ABI requirements

The release build uses real RocksDB through the existing recipe;
`NO_ROCKSDB=1` is not a release substitute. Go and native compilers come from the
build environment, retaining the existing toolchain selection semantics.
Normal Go RocksDB tests retain the upstream binding's compression-library link
flags and require Snappy, LZ4, Zstandard and zlib development libraries. This does
not enable compression in the existing native RocksDB recipe.

The accelerator [ABI check](../scripts/ci-check-abi.py) requires `manifest-ctl`
and `store-ctl` to remain static. `cache-ctl` retains normal CGO/glibc with static
RocksDB, libstdc++ and libgcc; required GLIBC versions may not exceed the declared
2.38 baseline. ABI failures stop packaging. Native recipes, checksums, licenses,
source inventories and link-map validation remain required.

## Owner E2E and local evidence

The [accelerator E2E guide](../test/e2e/README.md) lists the assembled-binary,
EROFS, Redis, scratch-storage and privilege prerequisites and each required
flatten, manifest, store and cache case. Keep its complete helper tree with the
owner suite. Local filesystem/cache and S3-compatible fixtures demonstrate local
behavior; they do not establish real cloud coverage. Credentialed OBS remains
an explicit `OBS_E2E=1` run. An excluded cloud case is not a pass.

Offline checks from the accelerator checkout:

```bash
python3 scripts/ci-test-workflows.py ../kuasar-sandbox  # use the actual platform path
(umask 022; bash scripts/test-release.sh)
make test-e2e-scripts
```

The workflow checks parse the component's actual YAML and exercise its allocation,
workspace and ABI contracts. Release tests use synthetic binaries/link maps and
mocked API responses. Script tests exercise fixtures and lifecycle boundaries;
they do not complete the numbered real E2E assertions. Passing these checks is
not evidence of a real RocksDB build, actual owner E2E or cloud qualification.
