# CI portability preparation

[English](accelerator-ci.md) | [简体中文](accelerator-ci_zh.md)

Accelerator#134 / platform#128 is **prepared, not activated** while the repositories
are private. This change does not publish source, change visibility, billing or
quota, or establish a public relay for private CI.

The #152 workflows require the actual caller's repository visibility to be
`public` and its full name to match `github.repository` before allocating any
runner. All x86 release, maintenance and integration jobs use `ubuntu-latest`;
native ARM integration uses `ubuntu-24.04-arm`. Non-public callers schedule no
new hosted jobs. Existing deployed private workflows remain in place until the
coordinated authorized cutover; a skipped private invocation is not acceptance.

## Bootstrap and build boundaries

Every public release/maintenance job checks out platform tooling at one immutable
SHA in `trusted/platform`, using only `github.token`. Placeholder references
are rejected by the workflow contract check.
Bootstrap runs before requested accelerator source is checked out or executed.

| Jobs | Shared profile |
| --- | --- |
| Release preflight, publish, Preview delete | `release-control`: minimal control tools and pinned Go needed by `release.sh` |
| Reconcile Latest, artifact cleanup | `control`: Git, curl, jq, Python/YAML and archive tools |
| Release build/test/package | `artifact-build` or `artifact-cross`: target-aware tools, native dependencies and control tools |
| Public PR integration | Shared architecture lanes and separate required source checks; see [platform CI](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci.md) |

The release build uses real RocksDB through the existing recipe; `NO_ROCKSDB=1` is not a release
substitute. Go uses the shared verified 1.26.5 distribution and existing toolchain
selection semantics. The pinned GitHub CLI installer is retained in control jobs.

Exact release source lives in `src/accelerator`. Build, tests, native caches,
packaging and artifact upload use that subtree, so sibling trusted tooling does
not dirty the source or interfere with VCS stamping. Public caches live under
the bootstrap's `$RUNNER_TEMP/kuasar-hosted.*` root; public jobs have no fixed
`/var/cache` state. Hosted build/package shells
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

The PR wrapper retains shared `ci-entry.yml@main`. After #152 activation, public
callers use exact baseline plus candidate products, prepared owner workspaces
and architecture-specific E2E. Source checks, the UFFD gate and applicable x86
working-set smoke retain their required roles. The required
Integration E2E check is not replaced by a manual rehearsal or these offline tests.
Local filesystem/cache and S3-compatible fixtures demonstrate local behavior;
they do not establish real cloud coverage. Credentialed OBS remains an explicit
`OBS_E2E=1` run. An excluded cloud case is not a pass.

Offline checks from the accelerator checkout:

```bash
python3 scripts/ci-test-workflows.py ../kuasar-sandbox  # use the actual platform path
(umask 022; bash scripts/test-release.sh)
```

The workflow tests parse actual YAML, exercise non-public allocation guards and
workspace/affinity shells, and test ABI acceptance/rejection without GitHub or
private source access. Existing release tests use synthetic binaries/link maps
and mocked API responses, not an actual RocksDB/cloud qualification. Platform's
`ci/integration/test-ci-tools.sh` remains required.

Before activation, normally merge the reviewed shared implementation, pin all
release bootstrap checkouts to its immutable upstream SHA, and qualify actual
candidate source/build/release behavior on a standard runner. Refresh pins after
squash or rebase. Passing offline fixtures or preparing a private repository
does not establish that its Public CI route is active or accepted.

Normal Go RocksDB tests retain the upstream binding’s compression-library link flags; the accelerator profile explicitly supplies Snappy, LZ4, Zstandard and zlib development libraries. This does not enable compression in the existing native RocksDB recipe.
