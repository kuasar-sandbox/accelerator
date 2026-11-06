[English](CONTRIBUTING.md) | [简体中文](CONTRIBUTING_zh.md)

# Contributing to accelerator

Use this entry for source changes, component validation and release preparation. Public contribution and shared CI policy are maintained by the project; the component-specific steps below show what to run and what the evidence proves.

## Contribution workflow

Use a public Fork and a topic branch. Contributors can open issues, review changes
and propose PRs without upstream write access. Maintainers review changes, arrange
trusted validation and squash merge; direct pushes to upstream `main` are not the
contribution path.

## Prepare a Fork

Fork the repository to your GitHub account, then configure HTTPS remotes:

```bash
git clone https://github.com/<your-login>/accelerator.git
cd accelerator
git remote add upstream https://github.com/kuasar-sandbox/accelerator.git
git fetch upstream
git switch -c feat/short-description upstream/main
# edit, test, and commit
git push -u origin feat/short-description
```

For a maintenance fix, start from the supported `upstream/release/vMAJOR.MINOR.x`
branch instead and target that branch in the PR. Enable **Allow edits from
maintainers** when appropriate.

To advance your Fork's `main` without overwriting local commits or uncommitted work:

```bash
git fetch upstream
git switch main
git merge --ff-only upstream/main
git push origin main
```

Commit or safely preserve uncommitted work before switching. If `--ff-only`
refuses because histories diverged, inspect the local commits and resolve the
history deliberately; do not reset or force-push the Fork's main branch as a
routine synchronization step. Create each new topic branch from the freshly
fetched upstream target, not from an unrelated feature branch.

## Pull request requirements

A pull request should:

- have a focused scope and a clear description of the problem and solution;
- link the relevant issue when one exists;
- include or update tests and documentation when behavior changes;
- contain no credentials, private keys, tokens, customer data, or unrelated generated files;
- describe validation already performed and any operational or compatibility risk.

Draft pull requests are welcome for early feedback, but they are not merge candidates.

## E2E execution requirements

Every `test/e2e/cases/<suite>.<case>.sh` starts with one static
`# e2e-requires: [...]` JSON array below its shebang. The owner format is
`test/e2e/lib/requirements.json` with `{"schema":1}`. Declare the minimum kernel
and service capabilities used by the case and its helpers; use `[]` explicitly
when none are needed. Missing, duplicate or unknown capabilities fail validation.
Normal pinned source selection discovers additions, removals and requirement
changes without a platform case allowlist. Runner labels and placement belong
to the trusted platform configuration, not cases. See the
[capability contract](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/test/README.md#static-execution-requirements)
for supported names, checks and historical-input compatibility.

## Documentation

Follow the [project documentation policy](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/CONTRIBUTING.md#documentation-contributions). Maintain full English/Chinese pairs at `name.md` and `name_zh.md` with reciprocal selectors. Preserve requirements, identifiers, examples, diagrams, facts and links; record source-backed corrections. Existing complete English-only material may remain English-only. A summary or language-detector pass is not a complete translation.

## Licensing of contributions

By submitting a contribution, you agree that it is licensed under the license
that applies to the files being changed. New project files without a different
explicit license declaration are contributed under the
[Apache License 2.0](LICENSE).

Only submit work that you have the right to contribute. Preserve applicable
copyright, attribution, NOTICE, and SPDX declarations when modifying
third-party or differently licensed material.

## Review expectations

Be constructive and specific. Authors should resolve review conversations or explain why a requested change is not applicable. Maintainers may close stale, superseded, unsafe, or out-of-scope pull requests with an explanation.

## CI admission and merge evidence

The [central CI contract](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci.md) is the authority for trusted admission, workflow/status identifiers, companions, source selection and merge evidence. The default-branch wrapper owns admission; candidate workflows do not acquire control-plane or release credentials. Any contributor may open a public Fork PR; privileged execution is a separate trust decision. External candidates that are not admitted need maintainer review and a trusted upstream candidate preserving authorship and the reviewed source, rather than weaker admission.

Before requesting a normal reviewed squash merge:

1. Make the PR ready, target `main` or a supported maintenance branch, and resolve blocking review conversations.
2. Validate the current exact target-base/reviewed-head integration and required tests; skipped, cancelled, failed or old runs do not establish success.
3. Validate each declared companion independently and recheck its repository, PR, base, head, target ref and integration against the recorded source set.
4. Refresh evidence after base/head/companion changes. Body edits do not trigger execution; use the supported Draft-to-Ready event when necessary. Rerun an old event only for a diagnosed transient failure with unchanged admitted inputs.
5. Immediately before merge recheck the live source set. After a companion merges, remove obsolete declarations and rerun remaining candidates.

Do not manufacture statuses, add manual dispatches to bypass admission, weaken branch protection or retain source-fetch credentials in candidate execution. Auto-merge, merge commits and rebase merges are not used. The central contract supplies exact current checks and full trust-boundary details.

## Build and test

Builds use environment-provided Go and inherit its `GOROOT` and `GOTOOLCHAIN` selection. Release automation requires a working `gh` with `api --slurp` support on `PATH`; the project does not install, replace, or authenticate these environment tools against fixed binary digests.

```bash
make manifest-ctl store-ctl     # pure-Go command-line services
make cache-ctl                  # build the cache service and its current backend dependencies
make cache-ctl NO_ROCKSDB=1     # build cache-ctl without RocksDB (-tags no_rocksdb, CGO_ENABLED=0)
make build                      # all component binaries
make build TARGET_ARCH=aarch64  # cross-compile; amd64/arm64 aliases are accepted
make vet                        # validate the thin downstream-facing Go surface
make test                       # unit tests and backend/source-helper regressions
make test-e2e-scripts           # offline E2E fixture/helper regressions; not product E2E
```

Product E2E is executed by the platform-owned prepared-workspace runner against prebuilt artifacts; the component no longer has a separate owner `run_all.sh` or `make test-e2e` entrypoint. See [`test/e2e/README.md`](test/e2e/README.md) for the `prepare -> <suite>.<case>.sh -> run` contract and accelerator case prerequisites.

Source builds require Go 1.26.1 or newer. Native prerequisites vary by target. The current source tree documents and builds any native cache dependencies through repository scripts. Real object-storage tests must use explicit test credentials or a local S3-compatible service; ordinary unit and local-filesystem tests must not require production cloud credentials.

`cache-ctl` normally uses CGO and a locally built static `librocksdb.a` from
`deps/build-rocksdb.sh`; `manifest-ctl`, `store-ctl` and the downstream import
surface do not require RocksDB. `make release VERSION=vX.Y.Z` creates and checks
a local release bundle; the official cache payload must retain RocksDB support.

### Private-network and offline builds

The EROFS planner depends on the external `github.com/kuasar-sandbox/go-erofs`
module. Mirror or pre-cache the exact version selected by `go.mod`, including
its pseudo-version when applicable, and verify it against `go.sum` before
building offline. Point `GOPROXY` at the deployment's mirror, with its
checksum-database policy, or use a populated verified module cache. A checkout
of go-erofs or a local replacement is not required. Coordinated sibling
development can use the [project workspace](https://github.com/kuasar-sandbox/kuasar-sandbox);
it is not a prerequisite for this component's standalone build.

## Build and ABI requirements

The release build uses real RocksDB through the existing recipe;
`NO_ROCKSDB=1` is not a release substitute. Go and native compilers come from the
build environment, retaining the existing toolchain selection semantics.
CI uses the admitted Workbench image selected once for the run, pinned separately
for native x86_64 and aarch64 runners. Ordinary builds, tests and packaging use an
ordinary UID with a read-only root, dropped capabilities and private source/cache
directories. `KUASAR_BUILD_JOBS` supplies the same task CPU budget to Go and the
RocksDB CMake build; local native recipes without a budget still use `nproc`.
Normal Go RocksDB tests retain the upstream binding's compression-library link
flags and require Snappy, LZ4, Zstandard and zlib development libraries. This does
not enable compression in the existing native RocksDB recipe.

The accelerator [ABI check](scripts/ci-check-abi.py) requires `manifest-ctl`
and `store-ctl` to remain static. `cache-ctl` retains normal CGO/glibc with static
RocksDB, libstdc++ and libgcc; required GLIBC versions may not exceed the declared
2.38 baseline. ABI failures stop packaging. Native recipes, checksums, licenses,
source inventories and link-map validation remain required. The publisher uses a
standalone archive validator compiled from its trusted source in a separate
Workbench invocation with cache restore/save disabled; publishing executes the
validator without compiling it again.

## Prepared product E2E and local evidence

The [accelerator E2E guide](test/e2e/README.md) lists the prepared-product,
EROFS, Redis, scratch-storage and privilege prerequisites and each accelerator
`basic.*.sh` / `storage.*.sh` / `image.*.sh` product case. Product E2E is executed only through
the platform-owned prepared-workspace runner; accelerator no longer owns a
`run_all.sh` or an owner-suite entry point. Local filesystem/cache and
S3-compatible fixtures demonstrate local behavior; they do not establish real
cloud coverage.

For ordinary non-credentialed validation, prepare a workspace from the exact
prebuilt platform release and explicitly select the non-OBS accelerator storage/image
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
  --include storage.cache-probe.sh \
  --include storage.service-drain.sh \
  --include storage.store-cache.sh \
  --include image.manifest.sh
```

`basic.accelerator-activation.sh` separately validates service-manager lifecycle and named listener ownership. Run it with `--include basic.accelerator-activation.sh` on a systemd-capable environment; its mandatory `["systemd"]` declaration routes formal CI to the existing system executor. The actual Store/Cache requests verify recovery, while storage protocol/algorithm cases keep their storage classification and clean runtime. See the E2E guide for owned resources and prerequisites.

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
python3 scripts/test-native-build-budget.py
(umask 022; bash scripts/test-release.sh)
make test-e2e-scripts
```

The workflow checks parse the component's actual YAML and exercise its allocation,
workspace and ABI contracts. Release tests use synthetic binaries/link maps and
mocked API responses. Script tests exercise fixtures and lifecycle boundaries;
they do not complete the numbered real E2E assertions. Passing these checks is
not evidence of a real RocksDB build, prepared product E2E or cloud qualification.

## Image preparation validation

Production planning and Plan test fixtures use the fixed external `github.com/kuasar-sandbox/go-erofs` module without resolving or executing mkfs. Independent content validation uses host fsck/dump. Go planner tests cover an invalid native-tool path; Plan corruption, bounds and success-tail regressions remain active. [Image preparation](docs/image-preparation.md) defines input, source lifetime, Plan and artifact completion contracts.

| Requirement | Implementation | Maintained checks and evidence scope |
|---|---|---|
| Fixed input identity and complete validation | `pkg/flatten/input.go`, `layer_stream.go`; `pkg/remote/stream.go` | Layer media/size/digest/DiffID/trailer/Close and cancellation cases; actual local Registry → umoci → Plan → pipe → current artifact reader. |
| Content-based layer compression | `internal/layercompression`; streaming layers, Docker-save adapter and legacy `remote.Pull` | One detector and decoder implementation accepts plain/gzip/zstd bytes independently of supported media-type labels, including leading zstd skippable frames. Checks cover mismatched labels, complete identities/trailers, sniff-time cancellation, and actual Registry `Prepare` without cache, on cache miss/hit, and against archive output. |
| Layer filesystem semantics | `pkg/flatten/layer_apply_linux.go` | Actual umoci late whiteout/opaque, replacements, implicit parents, links, attributes and empty-layer cases; permission failures remain errors. |
| Go layout and sequential emission | Fixed go-erofs module; `go_planner_linux.go`, `plan_linux.go` | Actual content/metadata, selected hard links, padded-block sharing, independent fsck extraction, deterministic repeated builds, controlled allocation/Plan budgets, cancellation and cleanup. Existing Plan corruption, bounds, source mutation, metadata-only repeated RunAt and final-tail checks remain. Historical native Raw/Plan and exact-read controls remain in their original revisions; current tests exercise the Go generator. Go/native output identity is not required. |
| Archive and entry-point equivalence | `pkg/flatten/archive_linux.go`, `raw_linux.go` | Seekable archive, pipe spool and directory comparisons; truncated/concatenated archive and index-budget rejection. Guest CLI tests cover file, stdout and upload/error paths. |
| Persistent cache publication and ownership | `pkg/remote/prepare_linux.go`, `prefetch_linux.go`, cache/GC | Unverified EOF is not a hit; explicit byte budget, bypass, current-layer priority, duplicates, concurrent GC leases and joined cancellation. Conversion-policy tests retain explicit old-artifact reading while rejecting stale conversion hits. |
| Stable selection and owned cleanup | `pkg/flatten/prepared_linux.go`, `selection_linux.go` | Atime/excluded-child tolerance, included metadata changes, actual ENOSPC during apply/output, bind/output aliases, previous-file/UID/GID/rwx/access-ACL preservation, inherited directory GID, privileged empty-root chmod, fail-fast sticky-directory replacement permissions, EMFILE cleanup, reserved-file and directory substitution rejection, read-only private-tree cleanup without following external symlinks, and open/deleted FD checks. |

The last row includes opt-in real mount tests. Compile the test executable as an
ordinary user, then run only those cases with mount privilege in a new private
mount namespace. Set `TMPDIR` to a private test directory and `MKFS_EROFS_PATH`
to the selected native tool; all mounts are on test-owned paths and are removed
before their directories. The small tmpfs limits create real ENOSPC without
filling the host filesystem.

```bash
CGO_ENABLED=0 go test -c -o /path/to/private/flatten-mount.test ./pkg/flatten
sudo unshare --mount --propagation private env \
  FLATTEN_TEST_MOUNTS=1 TMPDIR=/path/to/private/scratch \
  MKFS_EROFS_PATH=/path/to/selected/mkfs.erofs \
  /path/to/private/flatten-mount.test -test.v -test.run '^TestMounted'
```

These tests establish local contracts. Actual runtime bundle/Guest consumption,
Builder Phase A/B, static package validation, Guest ext4/Host COW peaks and
matched end-to-end performance require the existing prepared-product and Builder
test paths with the exact source/artifact set. Record receiving artifact bytes
separately from scratch; include index/decoder memory and owned teardown. Package
tests or these mount fixtures do not replace real VM evidence.

## Native cache validation

Use `make test`, `make test-no-rocksdb`, `make test-release` and the normal
architecture/release targets. For focused native tests after `make deps-rocksdb`:

```sh
export CGO_CPPFLAGS="-I$PWD/build/$(uname -m)/rocksdb/include"
export CGO_LDFLAGS="-L$PWD/build/$(uname -m)/rocksdb/lib -lrocksdb -lstdc++ -lm -lpthread -ldl"
go test -race -count=3 ./pkg/cache/rocks/...
GOEXPERIMENT=cgocheck2 go test ./pkg/cache/rocks/...
bash test/scripts/test_rocks_native.sh
```

The last command needs Valgrind and exercises the actual bridge/library without
the Go runtime, including locked/partial opens and repeated open/close. The
Go tests cover callback lifetime, panic recovery, concurrent shutdown, corrupt
reads, readonly/reopen, BlobGC and inline/blob eviction. Recovery tests separate
flushed prefixes from unflushed tails across normal Close and SIGKILL, check
WAL recovery of the latest sketch checkpoint, and verify that flush errors are
returned without retaining the DB lock. A private opt-in engine
counter proves cold-blob filtering avoids value reads; statistics remain
disabled in production.

## Build a deployment without RocksDB

`make cache-ctl NO_ROCKSDB=1` builds the normal `cache-ctl` artifact with
RocksDB support compiled out (`CGO_ENABLED=0`, `-tags no_rocksdb`). Only
the RocksDB implementation files in `pkg/cache/rocks` carry the build tag;
the rest of the codebase is unaware of it. `rocks.Open` returns a
`not compiled` error in this build, so configs selecting `type: embedded`
fail at daemon startup with an error naming the tag. The default
`make cache-ctl` artifact is unchanged. `make test-no-rocksdb` compiles,
vets, and tests the tagged surface without any `librocksdb` present.

## Build and package a component release

The selected owner source explicitly declares its compiled E2E helper inventory in `release/helper-inputs.json`. The current inventory is empty. Future helpers and their build recipe belong in this owner declaration; aggregate packaging discovers them without a platform filename list.

Production flatten planning and Plan fixtures use the selected external go-erofs module; they do not resolve or execute mkfs.erofs. The release workflow freezes this component's admitted source with no guest-runtime test-source dependency. Each native architecture builds and packages the component with real RocksDB in the selected Workbench; x86_64 also runs `make test`, `make vet` and `make test-release`. Independent filesystem validation uses the environment's fsck/dump tools. There is no EROFS native-cache restore or `--plan-version` admission step. Runtime publication continues to consume the selected accelerator release. These build/package checks do not substitute for prepared product or VM evidence.

Build from the selected source with the component Makefile. `release.sh package`
uses the matching binaries in `bin/<arch>`, or an explicit `RELEASE_BIN_DIR`;
it collects materials and creates the bundle without rebuilding those binaries
or resetting source/build caches. Keep the selected source checkouts, dependency
versions and native build records together with the outputs.

Packaging records the actual Go versions and effective module replacements.
Go/module LICENSE and NOTICE files come from the selected compiler installation
and matching module sources, preserving nested paths. Module resolution uses the
normal Go cache and routing; downloaded module checksums must match the binaries.
An organization namespace alone does not exempt modules from notice collection.
This component has no internal sibling dependencies. Unsupported local replacements
need versioned module inputs for the official package.

Licenses and notices live under `share/licenses/<component>`. Collection fails on missing notices, unreadable subtrees or partial traversals. Package validation checks required files, payload identities and archive paths, types and modes. Downloads use native GitHub asset sizes and digests. New packages and Releases contain no source or historical test records, independent `SHA256SUMS`, or source/Preview markers. The publisher uses human release notes, checks the current build outputs, and refuses to replace existing assets with different bytes.

The official package requires the three Linux command binaries for its selected
native architecture (amd64 for x86_64, arm64 for aarch64), with
their own Accelerator main-package identities; `cache-ctl` must include RocksDB.
The five shipped operational helpers are copied from the selected source tree.
Before extraction the validator rejects extra/duplicate payloads, links,
incorrect ownership or modes, and foreign material namespaces.

The normal cache build retains `build/<arch>/cache-ctl.map`. Packaging reads
that link map and the matching RocksDB library/source tree, including
`RELEASE_ROCKSDB_SOURCE_DIR` when supplied. It records the actual RocksDB,
libstdc++, libgcc and other linked system inputs with their digests and Debian/RPM
source-package versions. Copyright, license, NOTICE and referenced common-license
texts are copied from those packages; installed file bytes are not authenticated
against package-database digests. The RocksDB material set must include
`AUTHORS`, `COPYING`, `LICENSE.Apache` and `LICENSE.leveldb`.
Distinct native inputs cannot overwrite a shared material name. RPM collection
requires complete same-source sibling listings. These materials support release
review, not legal certification, and do not change the RocksDB backend or durability.

This repository publishes independent component versions named `vX.Y.Z`. Each published native-architecture component archive contains the three service binaries and the documented operational/performance helper scripts selected by the release contract. The aggregate platform archive carries selected user guides and canonical E2E runtime inputs from its explicit documentation/test selections. Internal design documents and source-only tests remain in the source repositories.

The project repository publishes aggregate versions named `release-vX.Y.Z`, selecting exact versions of `accelerator` and the other release units, validating their assets, and running cross-component tests. Aggregate and component version numbers are independent.

Release branches, Preview, Latest reconciliation and concurrency/cancellation
handling are owned by the [project release documentation](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release.md).
For ordinary use, start from the [latest Stable aggregate channel](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest).
