[English](image-preparation.md) | [简体中文](image-preparation_zh.md)

# Image preparation: sources, layout and completion

Accelerator owns the shared OCI/directory/archive preparation libraries in `pkg/flatten`, `pkg/image` and `pkg/remote`. This guide defines their source lifetime, image payload, conversion policy, Plan and completion contracts. [flatten-ctl usage](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli.md) belongs to guest-runtime, which publishes that CLI. Generic sparse data, encryption and carriers are defined in [artifacts](artifacts.md).

## Library entry points and consumer responsibilities

`PrepareImage`, `PrepareArchive` and `PrepareDir` select and prepare immutable logical content. Their `PreparedImage` retains the selected source, layout, Plan and descriptors until output finishes and Close joins the work. Callers own execution-resource limits, stable input, the requested writer and publication of the final result. The sections below define the exact boundaries; successful payload writes alone are not successful artifact publication.

## Image payload and runtime configuration

### Byte layout

There are two layers of format. The delivered `.img` file is a tarstream artifact with one `image` payload and a `.kuasar.digest.<hex>` marker. Within the **unwrapped payload**, the layout is:

| Payload offsets | Contents |
|---|---|
| `[0, erofs_end)` | Self-describing EROFS filesystem; the superblock at offset 1024 provides the image size. |
| `[erofs_end, payload EOF)` | Uncompressed (STORED) ZIP archive containing `config.json`, the projected OCI runtime configuration. |

The EROFS and ZIP portions coexist without changing either format:

- **EROFS reader:** reads the superblock at payload offset 1024 and uses `blocks << blkszbits` for the filesystem extent. Bytes after that extent do not belong to the mounted filesystem. A platform vhost-user-blk reader first exposes the payload; direct `mount -t erofs` needs the extracted payload rather than the tar wrapper.
- **ZIP reader:** scans backward from payload EOF for End-of-Central-Directory (EOCD). The appended ZIP's offsets are relative to its own beginning, and supported ZIP readers account for the EROFS prefix. Direct ZIP tools should also receive the extracted payload.

`erofs_end = blocks × (1 << blkszbits)`, parsed from the superblock. EROFS has a defined on-disk byte order and supports cross-host-architecture construction; the guest still needs compatible EROFS features, and the application binaries still need the correct execution architecture.

```bash
# Unwrap the artifact before using a direct filesystem/ZIP tool
flatten-ctl tar extract -f my-app.img image:my-app.erofs
unzip -p my-app.erofs config.json
```

Image packing uses `accelerator/pkg/tailzip` to split the EROFS payload and configuration suffix before writing the tarstream envelope. The declared payload commitment covers the EROFS prefix; the suffix has its own metadata commitment. The new generator may change EROFS layout and digests; deterministic configuration ZIP and envelope rules remain unchanged, and existing artifacts are read with their original declared identities.

### config.json schema

The ZIP's config.json is the runtime-relevant projection of the OCI image configuration. It retains launch fields and omits `created`, `author`, `history`, `rootfs.diff_ids` and other metadata that would introduce unrelated build variation.

Projected schema:

```text
Architecture     string                 (passed through, e.g. "amd64" / "arm64")
Os               string                 (passed through, e.g. "linux")
User             string,omitempty
Env              []string,omitempty
Entrypoint       []string,omitempty
Cmd              []string,omitempty
WorkingDir       string,omitempty
ExposedPorts     map[string]struct{},omitempty
Volumes          map[string]struct{},omitempty
StopSignal       string,omitempty
Labels           map[string]string,omitempty
Healthcheck      *Healthcheck,omitempty   (Test/Interval/Timeout/StartPeriod/Retries;
                                            durations are native nanosecond int64 values)
```

**Why Architecture and Os do not use omitempty:** even empty values are emitted so downstream readers can see that the source omitted them, rather than silently inheriting defaults. `flatten-ctl` performs byte-level conversion; platform compatibility decisions, including amd64 versus arm64, belong to the caller/scheduler.

Unlisted fields such as `OnBuild`, `ArgsEscaped`, `Domainname`, `Hostname`, `AttachStdin`, `Tty` and `MacAddress` are silently discarded because they are not part of this sandbox launch projection.

### Sources of determinism

The ZIP portion uses:

- One entry, always named `config.json`.
- STORED mode without compression.
- Fixed modification time `1980-01-01 00:00 UTC`, never wall-clock time.
- Deterministic JSON: lexically sorted map keys, preserved slice order for Env/Cmd/Entrypoint semantics, and omission of optional zero-valued fields.

EROFS metadata normalization is described in [Layout and metadata policy](#layout-and-metadata-policy).

Equal runtime projections produce equal JSON and ZIP bytes. **Equal config alone does not imply equal whole-image hashes**: the rootfs data and metadata, selected build tools/options, and artifact sparse map/wrapper must also match. Preserve these inputs when comparing payload hashes or manifest keys.

## Preparation and output lifecycle

### Input and layer lifetime

`remote.Stream` pins the platform manifest, config, descriptors and matching
DiffIDs before opening layers. Each `LayerInput.Open` returns a fresh body for
that immutable identity. Preparation owns it, closes it exactly once, and checks
Close errors. For supported layer media types, stored bytes determine plain/gzip/zstd compression; media-type labels do not override content detection. The stored-byte checker counts and hashes the sniffed prefix once. The shared detector also handles leading zstd skippable frames.
Tar EOF is not stream EOF: bounded tail reads verify the entire decoded DiffID,
compressed size/digest and decoder trailer. Errors or cancellation stop input;
they do not drain an unbounded failed body. No artifact is emitted until every
layer passes. `remote.Pull` retains its complete-download, reopenable Source
contract; legacy `Build(Source, ...)` still accepts uncompressed reopenable layers.

`PrepareArchive` bounds its outer index, validates members, offsets, lengths and
archive termination, and opens layers in manifest order. A seekable archive stays
in place; a pipe is spooled once. `PrepareImage`, `PrepareArchive` and `PrepareDir`
all produce a `PreparedImage` owning its selected source, layout, plan and FDs.
Legacy `Flatten` borrows its reader; `PrepareArchive` owns and closes its body.
Selected config and layers must be regular members. The existing Skopeo legacy
alias `<hex>/layer.tar -> ../<diff-id>.tar` is accepted only when its target is a
selected regular layer; it is indexed but never followed or extracted. Other
outer links and sparse members are rejected.

### OCI application and stable source

The Linux adapter uses **umoci v0.6.0** layer APIs without its CLI, CAS or runtime
bundle generation. It preserves numeric UID/GID, mode, ordinary directory and
type-replacement semantics, implicit parents, hardlink identity and special files.
Late `.wh.<name>` and `.wh..wh..opq` markers affect lower-layer entries without
deleting same-layer additions. Permission failures fail preparation. Supported
layer xattrs are applied and checked in the private rootfs; see the output policy
in [Layout and metadata policy](#layout-and-metadata-policy). A half-applied tree is discarded, never repaired by replaying a layer in place.

Private rootfs writes end after layer application. An existing directory must
remain stable for the complete prepare/output lifetime; selection and digest
checks detect changes but cannot create an atomic snapshot of a concurrent tree.
One selection, including `Skip`, `SkipMounts` and all owned scratch/spool/layout/
output exclusions, is shared by planning, reading and final validation. Reading
atime or modifying excluded contents does not invalidate the selected tree.
Changes to included metadata, links, objects or directory membership do.

### Layout and metadata policy

`accelerator` pins the external module `github.com/kuasar-sandbox/go-erofs` for
directory/inode/NID placement, hardlinks, superblock, block allocation and compact
single-device chunk indexes. The caller scans the shared selection deterministically,
hashes each 4 KiB chunk with full SHA-256 using an approximately 1 MiB buffer, and
writes ordered source records. The library only plans and writes structural metadata,
without spooling payload. Raw and artifact APIs share KERPLAN v1. Old and new
generators need not produce identical bytes; the new generator remains deterministic.

| Field | Handling |
|---|---|
| Timestamps and UUID | Zero timestamps and an all-zero UUID fix output identity without changing a directory source. |
| UID/GID/mode | Preserve numeric source ownership and permission bits. Insufficient permissions fail layer application. |
| Inodes/order/hardlinks | Deterministic shared selection and real shared-inode hardlinks; content dedup remains separate. |
| Extended attributes | Final EROFS policy: **no xattrs, file capabilities, ACLs or security labels are emitted**, even when applied successfully to the private rootfs. |
| Compression/devices | Uncompressed, single-device, 4 KiB chunks; no hidden raw/data-blob fallback. |

Plan schema and capabilities are internal and versioned independently of
`image.ConversionSemantics` and artifact identity. The Plan declares logical
length, source identity/path, ordered target/source offsets, lengths, SHA-256 and
padding. The reader rejects unsupported features, overflow, excessive counts or
bytes, missing/truncated inputs, overlaps and out-of-bounds references.

The library's `LayerLimits.MetadataBytes` and `PlanLimits` fields are optional
admission quotas. Zero imposes no additional layer, planning, on-disk Plan or
index quota; checked integer/allocation and format boundaries still apply. The
CLI uses these zero-valued options, so increasing Builder resources is no longer
masked by these fixed layer/plan quotas. No new CLI flag or Builder API is
required. Execution resources remain the outer memory/storage boundary;
standalone callers must bound their environment. This does not guarantee that
arbitrarily large input will complete.

Explicit positive values keep the existing conservative accounting. The legacy
name `NativeMemoryBytes` bounds controlled Go planner allocations, split equally
between the library and caller dedup/hardlink tables; growth accounts for old
and new storage together. `PlanBytes` bounds Plan files and structural metadata;
`IndexMemoryBytes` retains selection, reader and writer shares. The former
one-million-source cap is replaced by an addressable source-slice bound. These
estimates are not process RSS limits: GC, stacks and allocator retention still
need measurement. Decoder, archive, path, digest and completion checks remain
in force.

### Sequential output and acceptance

Planning releases references to its indexes before emission, without forcing global GC or promising immediate return of memory to the OS. Layout
holes occupied by planned payload are reported as **Data**. `RunAt` is a repeatable
metadata query and does not consume a data cursor or infer holes by scanning zeros.
Each payload block is checked against its planned digest before entering a writer.
The existing runtime-config projection, tailzip and encryption writer are retained.

An optional tarstream final-validation hook runs after payload completion and
before the carrier digest marker, complete tail or encryption final record,
including empty and all-Hole/Zero sources. Prepared output uses it to revalidate
source selection and Plan/layout identity. A receiver must require the complete
artifact identity/tail and successful producer/ingester completion before Ready
or conversion writeback. Observing payload bytes alone is insufficient.

`WriteRaw` is an explicit API using the same plan. The normal CLI never generates
raw staging. Named raw and artifact outputs use pending files and atomic commit.
Rename is the commit point, after output/file-close, source validation and source
resource cleanup succeed. Subsequent empty-stage cleanup cannot undo that commit:
the CLI warns on stderr; raw file APIs synchronously call `Options.CleanupWarning`
when supplied (nil stays silent), while returning success for the committed file.
`Close` cancels active work, joins it and releases owned resources; an arbitrary
caller-supplied output writer must itself return from a blocked Write.

The shared Go generator does not search for mkfs at execution time;
`MKFS_EROFS_PATH` does not select its generator. The separate Guest-runtime
repository owns `make flatten-ctl` (the static Go CLI) and `make build` (including
runtime-bundle packing with preinstalled host erofs-utils). Follow its
[build guide](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/build.md)
for those commands; they are not Accelerator Makefile targets. The project no
longer builds mkfs or maintains its patches. Independent fsck/dump validation
uses host tools; historical native performance controls retain their original
source revisions.
There is no implicit old-generator or high-space fallback. Rollback selects the
previous source/artifact set and its matching conversion semantics.

### Hashing resources and output order

Block hashing uses the admitted 1 MiB read buffer and a fixed digest array, with
at most two workers bounded by `GOMAXPROCS`. All workers join before buffer reuse
or cancellation cleanup. Deduplication and layout allocation remain sequential.
An adjacent block can reuse a full SHA-256 only after comparing all 4096 bytes;
worker count does not change block identities or output order.

### Admission limits and conversion compatibility

`LayerLimits.MetadataBytes` and the three `PlanLimits` fields are opt-in
admission quotas. Zero no longer imposes the former 128 MiB layer estimate or
512/512/128 MiB planning/plan/index defaults. Internally, zero-valued plan quotas
normalize to checked integer/allocation ceilings, without allocating those
amounts. The former one-million-source cap is replaced by the addressable source
slice bound; both producer and reader still enforce byte, overflow and format
checks. Explicit positive quotas retain the existing conservative accounting and
phase shares, including cumulative selection charges and table-growth overlap.
No duplicate umoci path table or new selection-lifetime model is introduced.

Regressions cross the former 6 GiB unique-block and source-index thresholds,
check opt-in rejection, and exercise source-count/charge overflow and corrupt
plans. Source mutation, path, digest, decoder, archive and artifact-integrity
checks remain unchanged. Builder guest/execution resources remain the outer
memory boundary; standalone callers must bound their own execution environment.
These estimates are not total RSS limits, and zero quotas do not guarantee that
arbitrarily large input will complete. The fixed cross-architecture golden
includes the presence carrier introduced by the tarstream format change.

All payload records remain in first-seen source order and are streamed to the
existing KERPLAN v1 files. No full payload blob or raw intermediate is created.
Conversion lookup and writeback use `umoci-go-erofs-no-xattr-v1`. Layer entry names
use umoci `CleanPath`, become root-relative before metadata accounting, and
entries naming the extraction root are skipped, matching containerd/Moby root
handling. Raw `..` components follow umoci normalization within the image root
instead of a separate rejection rule; link targets remain under umoci handling.
Image-layer output uses root mode `0755` and UID/GID `0:0`,
independently of the builder umask, identity or inherited setgid directories.
Directory-source output retains its source root metadata. The compatibility fix
retains v1: existing v1 conversion results remain reusable and are not regenerated
to normalize root metadata. Missing or different policy values miss; explicitly
selected old artifacts and the compressed-layer cache remain usable. Builder and
Guest must select the same conversion policy.

## Layer caching and conversion records

The shared `pkg/remote` library manages compressed-layer caching and OCI Referrers conversion records. These have distinct identities and lifetimes: the layer cache retains source bytes; a conversion record references a published Manifest. Configuration, commands and visible results belong to [flatten-ctl](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli.md#remote-pulling-local-caching-and-referrers-writeback).

**Local cache:** a standard OCI layout with `oci-layout`, `index.json` and
`blobs/<algo>/<hex>`. Export stages a candidate only with explicit `cache.dir`.
The current layer gets budget first; at most the next layer is prefetched when
its complete descriptor fits the remaining byte budget. An oversize layer streams
directly, or fails if `cache.required` is true. EOF alone never publishes a hit:
compressed size/digest, complete decoded DiffID/trailer and Close must all pass.
Hits are reverified during application. Duplicate writers keep the committed
inode. Reader/writer leases prevent GC from unlinking active data; abandoned
pending files become GC-eligible after the grace period. Completed persistent
blobs outlive the export. Cache `max_size` is an LRU target, not a filesystem quota.

The artifact type is fixed at `application/vnd.kuasar.flatten-manifest.v1` and cannot be configured, ensuring tools and versions use the same identifier.

Referrer annotations:

```text
vnd.kuasar.flatten-manifest.owner    = <hmac-hex> <referer_desc>
vnd.kuasar.flatten-manifest.id       = <manifest_id>                        # Ingest content key
vnd.kuasar.flatten-manifest.conversion = umoci-go-erofs-no-xattr-v1
vnd.kuasar.flatten-manifest.valid_at = <import RFC3339>[ <expiry RFC3339>]
```

`lookup` strictly validates `valid_at`. Missing/malformed timestamps, future import times, expired records and expiry before import are skipped as misses. If several records are valid, the newest import wins.

Conversion-version admission and compatibility are defined in [Admission limits and conversion compatibility](#admission-limits-and-conversion-compatibility). Records use the selected platform image digest as subject and share the source image repository. `owner` is an opaque caller-supplied scope; the library does not write the tenant `MANIFEST_KEY` into the record. The host creates the owner token and decides upload/writeback-failure policy; the guest CLI receives only `--owner`.

## Design choices and supported scope

### Why EROFS rather than ext4 or squashfs

- **Read-only, immutable base:** EROFS matches content-addressed chunks and reusable backing data.
- **Single payload:** it fits a single image artifact, manifest upload and block-device access; the CLI emits it sequentially from the stable source and Plan.
- **Portable on-disk format:** the filesystem can be constructed across host architectures, while applications still need the correct execution architecture.
- **Squashfs comparison:** the platform selects uncompressed, chunk-oriented EROFS for its data and deduplication path. Squashfs can also share immutable backing data; there is no evidence here for a universal density disadvantage.
- **Ext4 comparison:** ext4 supplies the writable upper layer in this platform. A mutable filesystem requires an isolation/COW strategy when used across sandboxes and has its own maintenance considerations, such as fsck. This is the reason for the current EROFS-base/ext4-upper split, not a universal benchmark proving one format denser than every alternative.

### Why merge offline instead of stacking container layers at runtime

Offline merging removes the need to assemble the original container layer stack separately for each sandbox. The platform exposes one immutable base through its data path; writable sandboxes may still mount guest overlayfs over that base. It does **not** rely on one host EROFS mount being shared as the guest mount, and no host mount-count density bound is established here.

The cost is an image-build step when the source changes. That work can be performed once in a CI/CD image pipeline and reused by many sandboxes.

### Why embed config.json instead of using a sidecar

A separate `app.erofs.config.json` introduces two problems:

- **Split copying/uploading:** operators must keep two files together; omitting one breaks startup metadata.
- **Separate manifest lookup:** a manifest exposes one payload reader, so a sidecar would need its own key and fetch.

ZIP-at-end keeps the filesystem payload self-describing within the one delivered artifact. `manifest-ctl store` can ingest it together, and sandbox startup obtains launch configuration from the same payload. EROFS's self-described extent and ZIP's backward EOCD scan allow the two inner formats to coexist; the outer tarstream supplies the platform artifact envelope.

### Why project configuration instead of forwarding it verbatim

OCI image config has many non-launch fields. `created`, `author` and `history` are build metadata; `rootfs.diff_ids` describes layers that no longer exist as separate layers; `OnBuild` is a build instruction rather than a runtime setting. Forwarding them would:

- Introduce unrelated timestamp/build variation into output.
- Change manifest deduplication unnecessarily when history changes.
- Blur the boundary between build instructions and runtime configuration.

An explicit projection makes the supported launch fields predictable for downstream consumers.

### Unsupported or deferred features

- **Direct arbitrary OCI-layout input:** registry pulling is supported ([Remote pulling, local caching and Referrers writeback](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli.md#remote-pulling-local-caching-and-referrers-writeback)), and the pull cache uses OCI layout, but export does not interpret an arbitrary OCI-layout directory as an image source. Convert it with `skopeo copy oci:./dir docker-archive:x.tar`, or push with crane and pull from the registry. Passing that directory as a normal local directory would select rootfs export, not OCI-layout decoding.
- **Image signing/encryption in flattening:** encryption and content addressing belong to the [manifest stage](transfer.md). This does not establish an image-signature verification workflow.
- **Layer preservation:** the output merges all layers into one EROFS payload. Downstream manifest chunk deduplication can reuse unchanged content, but it does not preserve the original layer structure.

## Performance and validation evidence

Build work generally grows with input bytes and file count. Major costs are layer download/decompression, filesystem entry application and Go EROFS planning; cache state, metadata distribution, storage, CPU and tool versions all matter. Appending one uncompressed ZIP entry is small relative to a typical image build, but the repository does not establish a universal 1 GiB/3–5 second or sub-10 ms result.

Check determinism by exporting an immutable input twice with the same configuration/toolchain and comparing payload/artifact hashes as appropriate, or reproducing manifest keys. Unit tests cover deterministic conversion; `verify` is no longer a CLI command ([Command-line interface](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten-cli.md#command-line-interface)).

`flatten-ctl info` does not decompress or load the full EROFS filesystem. It reads the superblock (128 bytes), scans the ZIP tail and decodes the single stored entry through the relevant payload reader. Manifest input can require network/chunk fetches, so latency is not universally sub-millisecond.

### Matched flatten measurements

For a linked Go generator, use `--variant NAME,CLI` without a native tool. `--allow-layout-differences` permits different artifact identities across generators only after the existing content, metadata, hard-link and runtime-config checks; repeated builds of each generator must still be byte-identical. `identity-comparison.json` records the trial count and repeated-identity check for each variant. A one-trial resource pass remains valid for resource measurements but explicitly reports `not_checked_single_trial`; it is not determinism evidence. Use at least two trials to compare repeated output, and retain the three alternating trials required for performance acceptance.

`test/scripts/bench_flatten.py` uses prebuilt producers, a real stdout
pipe and the current artifact reader. It alternates variant order for three
trials by default, records executable hashes, sampled process-tree RSS, allocated
scratch blocks/inodes and deleted-open files, and waits for the owned producer
session, including children in different process groups, before cleaning scratch.
The Linux harness temporarily acts as a child subreaper and waits only adopted
members of that owned session, so their CPU/I/O is included in child rusage;
unrelated children are not reaped. Termination and adopted-child waits use pidfds after rechecking process identity, with repeated session enumeration; numeric process-group IDs are not signal targets. The measurement host must support pidfd signaling and waits, checked before launching a producer. Records from older harnesses may omit orphan
helpers' CPU/I/O. After all writers stop, cleanup restores deletion access only
within owned scratch, including failure/cancellation paths. A cleanup error is
attached to the original failure instead of replacing it. Receiving artifact P and its optional fsync are
reported separately. `post_eof_teardown_seconds` measures the interval from producer
completion through owned process/thread cleanup to the start of consumer validation;
`receiver_validation_seconds` measures the consumer validation interval.
`accepted_seconds` retains the full interval including both. Older records without
`post_eof_teardown_seconds` include both intervals in `receiver_validation_seconds`;
they cannot be split retrospectively. Optional `--progress-timing` timestamps existing CLI progress
lines and the first available stdout bytes for phase attribution; preparation
ends before the artifact writer can emit its header. Child rusage I/O counters may be unavailable on some hosts;
sampled RSS and block peaks have the recorded sampling resolution.

Allocated storage values in the historical `scratch_blocks`, `deleted_open_blocks` and `artifact_blocks` fields are **bytes** (`st_blocks * 512`), not block counts. Their names and numeric meaning are retained for compatibility. `metric_units` now labels these fields and process RSS as bytes; it labels `child_io_input_512blocks` and `child_io_output_512blocks` as counts of 512-byte blocks. The same allocation units apply to individual samples and reported peaks.

The aggregate `results.json` is published only after all rounds succeed and the selected identity policy passes. Repeated builds of each generator must match; cross-generator identities may differ only with the explicit semantic-comparison option. Rejected comparisons retain per-round diagnostics without a success aggregate.

The current consumer's `info --json` config must equal the fixed projected JSON
provided by `--expected-config` before acceptance or caching a verified artifact
identity. Use JSON `null` only when the fixture intentionally has no appended
config. Records include the expected config hash, saved consumer output, and the
resolved paths and SHA-256 hashes of both consumer and fsck. Older records without
these checks or tool hashes remain incomplete evidence; do not infer them from
the producer hash or artifact identity.

Each round records the resolved producer config path, SHA-256 and file identity (`--config`, then `FLATTEN_CONFIG`; null means defaults). It checks the config again before publishing a successful result; changed, replaced or removed inputs invalidate the round. Config contents are not copied into evidence. Inputs must still remain immutable; these checks do not create a snapshot or capture external registry, proxy or CA state. Older records without this config evidence cannot establish unchanged pull/cache settings.

Walking a wide tree for block/inode samples can perturb the pipe receiver and
source I/O. Procfs stat parsing preserves arbitrary process-name bytes, including names of unrelated processes. Process enumeration follows procfs parent PIDs and includes owned-session
members after reparenting, checking the leader start time; it does not require
the optional per-task `children` interface. A resource pass without samples, or whose samples never observe a live owned process, fails
instead of publishing zero peaks. The receiver retries interrupted and positive short writes before recording artifact bytes; a write without progress or a storage error fails the run. Scratch traversal uses bound directory descriptors and no-follow entry stats, so directory-to-symlink replacement cannot redirect its samples outside scratch. Proc references are bound before both pathname classification and stat, preventing descriptor reuse from mixing two objects. Resource scans retain each observed process's start time and session. Per-process reference contributions and errors are accepted only after rechecking the group and task identities across the complete scan, including re-enumeration of the full live-thread identity set; changed sets, exited or replaced tasks contribute no reference sample. These checks do not make procfs atomic or capture every reference shorter than the sampling interval. Scratch traversal and stopped-tree cleanup use an explicit depth-first stack of bound directory handles, opening each child relative to its parent and checking its identity once. Backtracking and exception paths close the scans and handles. Directory-opening work grows linearly with the tree size; retained handles grow with the active ancestor depth. Resource exhaustion is an error that invalidates the round, not a promise to handle arbitrary depth with a fixed FD budget. Regressions check operation growth and handle release on success, scan failure and actual EMFILE; the timeout is only a hang guard. Each live task is inspected for private FD/cwd/root tables; shared inodes are deduplicated and shared RSS is counted once per thread group. A zombie group leader remains observable and stoppable while a sibling is live; its recorded start time and session are rechecked around sibling reads to reject PID reuse. Deleted-open accounting includes a retained, removed scratch root and uses inode link count,
not the procfs filename suffix. Process `cwd` and `root` references are also inspected: a removed directory they retain contributes its inode and allocated bytes once, including in the historical `deleted_open_*` fields. A scratch inode retained only by a memory mapping must also be accounted for; an unmeasured mapping invalidates the resource sample. Failed teardown aborts the progress reader and
retains scratch instead of waiting indefinitely for stderr. Pair the resource pass with `--timing-only` under the same resource,
storage and cache conditions to measure that effect. Timing-only results keep
identity/content checks, child CPU/I/O accounting and owned teardown, but record
sampled peaks and the sampling interval as null. Retain both results; a timing
pass cannot establish a memory, block, inode or open-file bound.

For the historical #161 comparison, use the previous implementation, the
correct applier with old mkfs, and native Plan under the same conditions. Apply
`test/fixtures/flatten-old-layout.patch` only to an isolated copy of its recorded
native accelerator baseline, with the matching guest CLI and maintained mkfs
including the exact-read fix. Retain source commits and the patch digest. This
historical fixture is not a patch for the current Go planner.

For the Go generator comparison, use the maintained native Plan implementation
with the same correct applier, reads and transport as the control. The example
below compares complete outputs semantically and checks repeated identities
within each generator.

Run in the existing Workbench with Python 3.11 or newer. Input, scratch parent and
output root must be disjoint; the output root must not exist. A Registry input
must include its fixed SHA-256 digest. Supply a fixed expected-content JSON keyed
by relative path, including `.` for the root directory: each entry includes `type`, `mode`, `uid`, `gid`, and `mtime_ns` (0 under the fixed output timestamp policy), plus file
`size`/`sha256`, symlink `target` or special-file `device` and numeric `kind` (`stat.S_IFMT(st_mode)`, distinguishing
FIFO, socket, character and block device). For each non-directory
hard-link group (including symlinks and special files) in the selected tree, the lexicographically first relative path
is canonical; other members include `hardlink_to` naming that path. Content enumeration
is iterative on Python 3.11 and newer and reports directory scan errors; an unreadable
subtree invalidates verification instead of being treated as empty. The first occurrence of
each artifact identity is extracted with the selected native fsck, and its actual
contents and metadata must match. This extra extraction is outside measurement. After verification, only the owned
extraction tree has directory access restored for removal; external symlink
targets remain untouched. Scratch permission or I/O errors, including denied inspection of an owned process's open files, fail the measurement; directories removed by the producer and processes or file descriptors that disappear during sampling are tolerated.

```bash
python3 test/scripts/bench_flatten.py \
  --variant native,native/flatten-ctl,native/mkfs.erofs \
  --variant go,go/flatten-ctl --allow-layout-differences \
  --input fixtures/image.tar --expected fixtures/expected.json \
  --expected-config fixtures/runtime-config.json \
  --consumer go/flatten-ctl --fsck tools/fsck.erofs \
  --scratch-parent scratch --output-root results --trials 3 --fsync
```

The historical native controls require matching normal-corpus identities.
Go/native comparisons allow different layouts after independent content,
metadata, link and runtime-config checks. Cover low/high repetition, layer
overwrites, small files, constrained network and explicit cache
as separate fixed fixtures. Record persistent cache occupancy/lifetime separately
when supplying `--config`; it is never temporary scratch. End-to-end elapsed time
includes overlapped work; do not add overlapping stage durations. These host
measurements do not establish Guest ext4 or Host COW peaks. Measure those through
the real Guest/Builder path, including the receiver and owned teardown.

### Go generator measurements and accepted tradeoff

The linked Go generator removes the separate runtime mkfs executable. It does
not promise a universal speedup. The [#178 candidate comparison](https://github.com/kuasar-sandbox/accelerator/issues/178#issuecomment-6177535650)
and [accepted tradeoff](https://github.com/kuasar-sandbox/accelerator/issues/178#issuecomment-6177892609)
record the measured costs as well as the gains. All other semantic, failure,
determinism, static-package and Guest/Builder acceptance requirements remain.

The comparison used 4 CPUs, 6 GiB memory, fixed inputs, three alternating pairs,
Go 1.26.8, and a real output pipe. The x86 host supports AVX2 but not SHA-NI or
AVX-512. The native control uses the same correct umoci applier and maintained
native Plan/read/transport behavior. Timing and sampled-resource passes are
separate. These are fixture measurements, not bounds for arbitrary images.

| Fixture | Native EOF / accepted + fsync (s) | Go EOF / accepted + fsync (s) | Accepted + fsync change | Producer/owned-child CPU change |
| --- | --- | --- | --- | --- |
| Directory, 4 GiB low repetition | 57.198 / 61.353 | 61.919 / 62.821 | +2.4% | +22.1% |
| Directory, 1 GiB low repetition | 16.814 / 18.470 | 15.463 / 18.980 | +2.8% | +37.4% |
| Directory, 4 GiB high repetition | 11.028 / 11.072 | 1.225 / 1.257 | −88.6% | −86.8% |
| Directory, 20,000 small files | 1.092 / 1.662 | 1.194 / 1.802 | +8.4% | +12.9% |
| Registry, three 1 GiB overwrite layers, initial matrix | 45.460 / 48.926 | 53.600 / 55.011 | +12.4% | +24.5% |
| Same overwrite fixture, writeback/cache-controlled follow-up | 41.426 / 47.384 | 42.205 / 47.591 | +0.4% | +14.3% |
| Registry, 1 GiB, 32 MiB/s server | 47.988 / 50.995 | 45.092 / 51.339 | +0.7% | −2.8% |
| Registry, 1 GiB, warm explicit layer cache | 21.549 / 25.795 | 20.884 / 27.599 | +7.0% | +10.0% |

Each cell is a median of complete per-round measurements. Accepted + fsync
includes receiver acceptance and receiver fsync; EOF alone does not. The initial
overwrite comparison showed variance before planning. The bounded follow-up
warms the same fixed inputs and waits for prior writeback to settle without
changing resources or flushing global caches. Both results are retained.
Unavailable producer rusage I/O counters do not mean zero I/O; follow-up cgroup
device counters include the Registry, receiver, verification and cleanup.

For low-repetition 4 GiB input, sampled RSS increased from about 137 to 210 MiB,
and planner/index scratch from about 64 to 68 MiB. The source directory and
received artifact are outside that scratch figure. The artifact grew from
4,294,975,488 to 4,299,173,888 bytes (about 0.098%). Registry fixtures include the
private 1 GiB final rootfs: producer scratch is about 1.016 GiB for both versions,
with about 1 MiB extra in Go. Host measurements do not establish the fixed Guest
ext4 or Host COW limits, which require separate real VM evidence.

The measured planner uses two hashing workers and external go-erofs module
`v0.0.0-20261014160540-827da6f90114`. The 4 GiB low-repetition timing series uses
a diagnostic build with VCS stamping disabled; the normal Makefile build has
the same planner and independently passes content/resource checks with the same
artifact digest. The normal CLI grows by 18,909 bytes while the runtime removes
the separate approximately 3.9 MB mkfs payload. These candidate records do not
substitute for final acceptance identified by actual merged sources and formal
artifact hashes. The ARM Guest/Builder hardware exception remains explicitly
unverified; native ARM tests and static packages are separate evidence.

Source checks, real mount fixtures and their prerequisites are in [Contributing](../CONTRIBUTING.md#image-preparation-validation). [Prepared E2E](../test/e2e/README.md) tests the produced artifacts; neither package-level tests nor historical measurements establish a universal VM or workload guarantee.
