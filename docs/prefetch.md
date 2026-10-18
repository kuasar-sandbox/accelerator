[English](prefetch.md) | [简体中文](prefetch_zh.md)

# Manifest prefetch concurrency

`fetch.Prefetcher.Prefetch` warms cache/store objects for a composed Stream before on-demand reads. This page is the admission and walk contract for that path. Formats, decrypt, and the bounded plaintext chunk cache remain in [Manifest §4.8](manifest.md#48-read-path-in-detail). Cache connection pools remain in [cache](cache.md).

## 1. Role

Restore and other cold reads often touch many physical chunks. Prefetch issues full-object cache Gets for finally visible Manifest `ChunkRun`s so a later UFFD fault or `ReadAt` can hit an already-filled object. Completion means the backend accepted or finished its Get; it is not a residency or readiness guarantee.

Prefetch does not verify, decrypt, pin, or populate the Stream plaintext cache. A successful Get is Released immediately without reading the Blob.

## 2. Why Gets overlap

Each cache Get is typically cheap. Waiting for the next Get on the same Fetcher is not: a serial walk pays one unix-socket or RPC round trip per visible Data chunk. Snapshot warmup therefore used to be dominated by that queue, not by GetCF or AES.

`prefetchStream` now hands visible Data chunks to a worker pool of size `maxPrefetchGets`. The request scheduler admits the same number of prefetch Gets at once. The two limits are the same constant so the walk cannot start more Gets than admission will allow.

Overlapping round trips can cut serial wait. That is an implementation property of this Fetcher, not a platform SLO. Record workload, cache state and revisions when quoting restore wall time; see the [project performance guide](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/perf.md).

## 3. Admission — `requestScheduler`

One Fetcher owns one scheduler. Every Stream opened from that Fetcher shares it. Different Fetchers are independent.

The Fetcher exposes two logical Getters over one underlying `cache.Getter`:

| Getter | Used by | Admission |
| --- | --- | --- |
| On-demand | Manifest metadata, `Stream.ReadAt`, `Run.ReadAt` | Starts immediately. Never waits for prefetch. |
| Prefetch | `Prefetcher.Prefetch` | Waits until `onDemand == 0` and fewer than `maxPrefetchGets` prefetch Gets are in flight. |

`maxPrefetchGets` is **8**. It is an internal const, not a YAML or CLI knob. The value is kept below a typical sandboxer `cache.pool` of **16** so an on-demand UFFD fault can still `Acquire` a cache connection while prefetch is running.

Rules:

- While any on-demand Get is in flight, no new prefetch Get is admitted.
- A prefetch Get that already entered the inner Getter is not cancelled when on-demand starts. The two may overlap briefly.
- Ending a prefetch Get does not wake another prefetch while on-demand is still present.
- Cancellation that wins before the inner Get does not leak a prefetch slot.
- The scheduler creates no background goroutine and does not own the underlying Getter's lifetime.

## 4. Walk — `prefetchStream`

`manifestStream.Prefetch` and `layeredStream.Prefetch` both call `prefetchStream`.

The walk is single-threaded over logical offsets. Hole and Zero runs, and Data runs that are not package `prefetchChunkRun`s, are no-ops. Each eligible Data run is a job: `ChunkRun.prefetch` → `loadChunkAt(..., loadPrefetch)` → prefetch Getter Get → Release.

Eight workers pull jobs. A full job channel back-pressures the walk, so one `Prefetch` call never has more than eight chunk prefetches in flight. The first error cancels the walk context; started jobs are joined before return. Concurrent `Prefetch` calls on the same Stream remain independent: they must not share one Blob handle.

Prefetch operates on the current composed Stream's full visible view. There is no manifest-key API for selecting a leaf to prefetch.

## 5. Compatibility

- Public `Stream` and `Prefetcher` APIs are unchanged.
- Failed prefetch must not poison a later `ReadAt`.
- `sandboxer` consumes this module through `replace => ../accelerator`. Restores see the new admission only after `sandbox-ctl` is rebuilt against this tree.
- If a deployment's cache Get pool is smaller than 16, lower `maxPrefetchGets` in source before relying on headroom for UFFD faults. Do not treat 8 as safe against an arbitrary smaller pool.

## 6. See also

- [Manifest §4.8](manifest.md#48-read-path-in-detail): Run/Stream read path, plaintext cache, and ciphertext-only prefetch.
- [cache](cache.md): cache Get protocol and connection pools.
- [sandboxer](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox.md): snapshot restore consumers of Manifest fetch.
