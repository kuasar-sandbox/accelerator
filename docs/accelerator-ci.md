[English](accelerator-ci.md) | [简体中文](accelerator-ci_zh.md)

# Accelerator CI requirements and checks

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

## Prepared product E2E and local evidence

The [accelerator E2E guide](../test/e2e/README.md) lists the prepared-product,
EROFS, Redis, scratch-storage and privilege prerequisites and each accelerator
`storage.*.sh` / `image.*.sh` product case. Product E2E is executed only through
the platform-owned prepared-workspace runner; accelerator no longer owns a
`run_all.sh` or an owner-suite entry point. Local filesystem/cache and
S3-compatible fixtures demonstrate local behavior; they do not establish real
cloud coverage.

For ordinary non-credentialed validation, prepare a workspace from the exact
prebuilt platform release and explicitly select the five non-OBS accelerator
cases:

```bash
RUNNER=/path/to/platform/test/e2e/e2e
RELEASE_DIR=/path/to/prebuilt/platform-release
WORK=/tmp/kuasar-e2e

"$RUNNER" prepare --release-dir "$RELEASE_DIR" --workdir "$WORK"
"$RUNNER" run --workdir "$WORK" \
  --include storage.cache.sh \
  --include storage.tiered-cache.sh \
  --include storage.cache-membership.sh \
  --include storage.store-cache.sh \
  --include image.manifest.sh
```

Credentialed OBS remains opt-in and is selected by case ID, not by an
`OBS_E2E` marker. With `OBS_BUCKET`, `OBS_ENDPOINT`, `OBS_AK` and `OBS_SK`
explicitly configured, run the same prepared workspace with:

```bash
"$RUNNER" run --workdir "$WORK" --include storage.obs.sh
```

A selected cloud case that cannot satisfy its endpoint or credential
prerequisites fails; an excluded cloud case is not a pass. CI uses the same
platform runner and exact admitted case IDs.

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
not evidence of a real RocksDB build, prepared product E2E or cloud qualification.
