[English](README.md) | [简体中文](README_zh.md)

# Accelerator product E2E cases

Accelerator product E2E is consumed through the platform-owned prepared-workspace runner. The component repository owns the case files and accelerator-specific helpers, but it no longer owns a `run_all.sh` or a separate product-E2E runner.

The execution model is deliberately small:

```text
prebuilt products -> prepare -> <suite>.<case>.sh -> run
```

A case ID is its filename. The suite is the first filename segment. Accelerator currently contributes the `storage` and `image` cases below:

| Case | Contract |
| --- | --- |
| `storage.cache.sh` | Filesystem/local cache and Store round trips, cache protocol behavior, read-only tiered write rejection and related cache correctness. |
| `storage.tiered-cache.sh` | Redis/tiered/sharded/EC behavior, upstream fill/writeback, failure/recovery, restart/refill and origin-free warm reads. |
| `storage.cache-membership.sh` | Rolling EC cache membership and reconstruction with surviving peers. |
| `storage.store-cache.sh` | Store's embedded read-only cache protocol, generation rollout, old-manifest reads, blob namespace and rejected writes. |
| `image.manifest.sh` | Real flatten/EROFS plus Manifest store/load, dedup, verification, sparse/zero-block and image-diff contracts. |
| `storage.obs.sh` | Explicit credentialed OBS/S3-compatible round trip. This case is not part of ordinary offline execution. |

The case files are intentionally regular `100644` files. The common runner invokes selected cases with `bash`; executable mode is not a second case contract.

## Running product E2E

Use the runner and cases shipped by the Kuasar platform test bundle or an exact integration prepared workspace. Do not build accelerator, compile a test helper, discover sibling source repositories, or fall back to source while executing product E2E.

A typical aggregate-release workflow is:

```sh
RUNNER=/path/to/platform/test/e2e/e2e
RELEASE_DIR=/path/to/prebuilt/platform-release
WORK=/tmp/kuasar-e2e

"$RUNNER" prepare --release-dir "$RELEASE_DIR" --workdir "$WORK"
"$RUNNER" run --workdir "$WORK" --suite storage --suite image
```

The platform CI uses the same runner. During component-candidate validation it selects the exact accelerator case IDs admitted from that candidate, so another component's `image.*` cases are not accidentally included.

A selected case fails when a required product or execution prerequisite is missing. Architecture, root privileges and host services are execution conditions, not suites and not successful skips.

## Source/helper regressions

Checks that validate fixtures or test-helper behavior without exercising a product stay outside product E2E:

```sh
make test-e2e-scripts
```

This target runs the port-lease regression and offline Manifest/cache boundary tests. It may use source-level test fixtures and stubs because it is a source/helper gate, not artifact E2E. `make test` includes these regressions as part of the normal source test path.

## Runtime prerequisites

The real cases require the prerequisites of the selected contract rather than one global owner-runner environment. Common requirements include Linux, Bash 4+, Python 3, GNU userland tools, writable scratch space, and the exact prepared binaries exposed through `BIN`.

Manifest/flatten coverage requires a real `mkfs.erofs` supporting the layout used by the product. Root or noninteractive `sudo -n` is needed where flatten export must preserve image ownership. The case does not use `sudo -E` and forwards only owned scratch/config paths and the resolved EROFS tool into that privileged operation.

Tiered/cache cases that exercise Redis require `redis-server` on `PATH` or the explicitly supplied server executable. They start disposable owned instances; no external Redis service is implied.

`storage.obs.sh` is selected explicitly and fails closed unless all required cloud inputs are present:

- `OBS_BUCKET`
- `OBS_ENDPOINT`
- `OBS_AK`
- `OBS_SK`

`OBS_REGION` and `OBS_PREFIX` are optional. The case does not treat `~/.obsconfig` as credentials because `store-ctl` does not consume that file. It uses an isolated per-run prefix and attempts to purge that prefix during cleanup. Ordinary storage/image runs require no OBS credentials or cloud API access.

## Manifest fixtures and offline behavior

`lib/manifest_fixture.py` creates reproducible Docker-format data archives using Python stdlib for source/helper regression and prepared-fixture generation. The images carry architecture, Linux runtime metadata, UID/GID ownership, environment and working-directory information, while their layer data provides meaningful shared/different content for dedup and chunking assertions.

The artifact E2E path consumes a prepared manifest-fixture directory. It does not generate product fixtures as a hidden fallback. Explicit `IMAGE_A`/`IMAGE_B` development overrides, when supported by the case, only use images already cached in the local Docker daemon; they never pull, retag, or delete caller images.

Shared runner routing, provenance validation, architecture lanes and rollout rules are maintained in the [platform CI contract](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci.md). This guide owns only accelerator case intent and prerequisites.
