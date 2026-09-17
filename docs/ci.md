# CI portability preparation

[English](ci.md) | [简体中文](ci_zh.md)

Accelerator#134 / platform#128 is **prepared, not activated** while the repositories
are private. This change does not publish source, change visibility, billing or
quota, or establish a public relay for private CI.

After authorized publication and rollout, `github.event.repository.private == false`
selects `ubuntu-24.04`. Private release/maintenance jobs retain exactly
`[self-hosted, Linux, X64, kuasar-control]`, and private release builds retain
`[self-hosted, Linux, X64, kuasar-e2e]`. Private PR control and source E2E retain
their existing pools, including E2E's `kvm` and `cgroup-v2` labels. Other shared
workflow callers and exact-assets routing are unchanged.

## Bootstrap and build boundaries

Every public release/maintenance job checks out platform tooling at one immutable
SHA in `trusted/platform`, using only `github.token`. Placeholder references
are rejected by the workflow contract check.
Bootstrap runs before requested accelerator source is checked out or executed.

| Jobs | Shared profile |
| --- | --- |
| Release preflight, publish, Preview delete | `release-control`: minimal control tools and pinned Go needed by `release.sh` |
| Reconcile Latest, artifact cleanup | `control`: Git, curl, jq, Python/YAML and archive tools |
| Release build/test/package | `accelerator`: pinned Go, CMake, build-essential, pkg-config, binutils and control tools |
| Public PR source E2E | Existing full `source` profile, including Redis, unzip and OpenSSL |

The accelerator profile does not set up KVM or Docker or compile Kernel/EROFS.
It builds real RocksDB using the existing recipe; `NO_ROCKSDB=1` is not a release
substitute. Go uses the shared verified 1.26.5 distribution and existing toolchain
selection semantics. The pinned GitHub CLI installer is retained in control jobs.

Exact release source lives in `src/accelerator`. Build, tests, native caches,
packaging and artifact upload use that subtree, so sibling trusted tooling does
not dirty the source or interfere with VCS stamping. Public caches live under
the bootstrap's `$RUNNER_TEMP/kuasar-hosted.*` root; public jobs have no fixed
`/var/cache` state. Private jobs retain their original mirrors and persistent
tarball cache and do not run apt or hosted bootstrap. Hosted build/package shells
use literal `bash` and apply the bootstrap CPU/memory budget with `taskset`, also
bounding RocksDB's `nproc` parallelism.

The trusted workflow checkout supplies the CI-only ABI check. `manifest-ctl` and
`store-ctl` must remain static. `cache-ctl` retains normal CGO/glibc with static
RocksDB, libstdc++ and libgcc; required GLIBC versions may not exceed the platform's
declared 2.38 baseline. ABI failures stop packaging. Native recipes, checksums,
licenses, source inventories and link-map validation remain intact. Build jobs
stay read-only; publishing, maintenance and artifact cleanup retain their separate
permissions, exact version/source/Preview checks, one-day bundle retention and
successful-publication artifact deletion.

## Coverage and rollout evidence

The PR wrapper retains shared `ci-entry.yml@main`. Public guest-runtime and
accelerator callers use the same source-set assembly, native builds, binary
assembly, owner E2E, UFFD gate and complete working-set smoke. The required
Integration E2E check is not replaced by a manual rehearsal or these offline tests.
Local filesystem/cache and S3-compatible fixtures demonstrate local behavior;
they do not establish real cloud coverage. Credentialed OBS remains an explicit
`OBS_E2E=1` run. An excluded cloud case is not a pass.

Offline checks from the accelerator checkout:

```bash
python3 scripts/ci-test-workflows.py ../kuasar-sandbox  # use the actual platform path
(umask 022; bash scripts/test-release.sh)
```

The workflow tests parse actual YAML, exercise public/private branches and
workspace/affinity shells, and test ABI acceptance/rejection without GitHub or
private source access. Existing release tests use synthetic binaries/link maps
and mocked API responses, not an actual RocksDB/cloud qualification. Platform's
`ci/integration/test-ci-tools.sh` remains required.

Before activation, normally merge the reviewed shared implementation, pin all
release bootstrap checkouts to its immutable upstream SHA, and qualify actual
candidate source/build/release behavior on a standard runner. Refresh pins after
squash or rebase. Passing offline fixtures or preparing a private repository
does not establish that its Public CI route is active or accepted.
