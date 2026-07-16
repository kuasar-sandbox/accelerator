# Cache backend BMS A/B: 2026-07-17

## Decision

Keep the Redis-compatible backend as an explicit deployment option, but do not
make Dragonfly the default backend for the EC shard path. On the tested equal
eight-CPU service budget, Dragonfly met the latency target for direct local and
tiered-L1 RAM hits through concurrency 4. The five-peer EC path met the target
only at concurrency 1; at concurrency 4 its P50 was 1.95 ms, while embedded
RocksDB remained at 772 us.

The result does not qualify Dragonfly SSD tiering. The available BMS has a
virtual rotational block device rather than production-class NVMe. Offload,
defragmentation, near-full capacity, and repair-storm acceptance remain blocked
on a representative NVMe host.

## Revisions and environment

- Accelerator source and benchmark harness:
  `57b7ae972806aa0e86d948e18bf38b990c39141d`; source archive SHA-256
  `8c4ebd4ea6b06ecf76f2171f052fb64fa7f3fc754ddd428fa56da9ed85326691`.
- cache-ctl binary SHA-256:
  `b8d33e002dcf82f22bfb97117e4e9fa08d8106e67ad863a7065c32fa7006242f`.
- Dragonfly: v1.39.0, official x86_64 archive SHA-256
  `81f88cfd0096c550e415a700c97eb5bf606db1df6106cc03584e878fd7aa126a`.
  The verified archive and source archive were copied from the operator host;
  the BMS did not download either from an international endpoint.
- Host: 88 logical CPUs, Intel Xeon Gold 6266C, 375 GiB RAM, two NUMA nodes,
  openEuler kernel 6.6.0, cgroup v2.
- Storage: 150 GiB ext4 root on `VBS fileIO`, reported rotational. No NVMe was
  available.

The GitHub Actions runner was stopped for the measurement and restored by an
EXIT trap. All processes were pinned to NUMA node 0. Embedded used CPUs 0-7.
The Redis path used CPUs 0-3 for cache-ctl and CPUs 4-7 for Dragonfly, preserving
the same total service CPU budget. The client used CPUs 8-11. For EC, five
independent Dragonfly instances shared the four backend CPUs because each shard
peer requires an independent physical key namespace.
Dragonfly ran with `--version_check=false` and an empty `--dbfilename`: the
disposable benchmark did not contact the vendor version service or write a
shutdown snapshot.

## Workload

- UDS between cache-ctl and Dragonfly; unchanged cache wire protocol between
  the benchmark client, tiered coordinator, and shard peers.
- 512 KiB values, 1,000-key uniform hot working set.
- GET concurrency sweep: 1, 4, 16, 32; five seconds per point.
- Local-only additions: PUT concurrency 1 and 50/50 mixed concurrency 8.
- Three fresh-process repetitions per backend and scenario. Tables report the
  independent median of throughput, P50, and P99.9.
- Dragonfly local/tiered-L1: four proactor threads and 4 GiB max memory.
- Dragonfly EC: one proactor thread and 1 GiB max memory per shard peer.
- Dragonfly cache mode enabled; SSD tiering disabled for this RAM-hot run.

The complete six-scenario, three-repetition run took 585 seconds (9 minutes 45
seconds), of which the 18 measured commands accounted for 582.24 seconds. This
is below the 30-minute project limit.

## GET results

Latency cells are `P50 / P99.9`.

| Scenario | Concurrency | Embedded ops/s | Embedded latency | Dragonfly ops/s | Dragonfly latency |
| --- | ---: | ---: | ---: | ---: | ---: |
| local | 1 | 2,323 | 422 / 773 us | 1,872 | 532 us / 2.29 ms |
| local | 4 | 6,922 | 544 us / 1.26 ms | 4,198 | 838 us / 3.59 ms |
| local | 16 | 10,566 | 1.33 / 6.39 ms | 6,619 | 1.99 / 10.8 ms |
| local | 32 | 10,984 | 1.99 / 15.7 ms | 7,931 | 3.05 / 24.4 ms |
| tiered L1 | 1 | 2,394 | 411 / 695 us | 1,870 | 532 us / 2.29 ms |
| tiered L1 | 4 | 7,044 | 530 us / 1.28 ms | 4,223 | 835 us / 3.48 ms |
| tiered L1 | 16 | 10,880 | 1.28 / 6.21 ms | 6,575 | 2.01 / 11.0 ms |
| tiered L1 | 32 | 11,056 | 1.75 / 16.1 ms | 7,917 | 3.00 / 26.0 ms |
| EC 4+1 | 1 | 1,921 | 495 us / 1.03 ms | 1,287 | 748 us / 1.42 ms |
| EC 4+1 | 4 | 4,868 | 772 us / 1.87 ms | 2,031 | 1.95 / 3.26 ms |
| EC 4+1 | 16 | 5,742 | 2.73 / 4.25 ms | 2,032 | 7.70 / 14.0 ms |
| EC 4+1 | 32 | 5,849 | 5.43 / 7.40 ms | 2,096 | 15.2 / 16.6 ms |

The first multi-connection EC point is the decisive regression: embedded met
both P50 < 1 ms and P99.9 < 5 ms at concurrency 4, while Dragonfly exceeded the
P50 target and delivered 42% of embedded throughput. The one-host EC setup
amplifies process scheduling overhead compared with five physical shard nodes,
but it applies the same total CPU budget and is the current reproducible BMS
acceptance topology. A distributed rerun may add evidence; it must not replace
this failed equal-budget result.

## Write and mixed results

| Mode | Embedded ops/s | Embedded P50 / P99.9 | Dragonfly ops/s | Dragonfly P50 / P99.9 |
| --- | ---: | ---: | ---: | ---: |
| PUT c1 | 1,796 | 455 us / 9.03 ms | 2,092 | 520 / 893 us |
| 50/50 mixed c8 | 3,249 | 2.29 / 13.0 ms | 3,090 | 2.11 / 6.39 ms |

Dragonfly produced a substantially tighter write tail in this RAM/cache-mode
test, while its read path paid the extra UDS/RESP process boundary. This is a
useful local-cache tradeoff, not evidence for SSD behavior.

## Observability note

All 18 reports recorded `State reset: recreated per point`. Across 78 Redis
counter snapshots, `reconnects`, `backend-errors`, and `protocol-errors` were
zero. Every snapshot ended with zero GET/SET inflight work, zero pool waiters,
and zero draining responses; all tier fill/repair gauges also ended at zero.

The benchmark ends each fixed-duration window by cancelling its client
context. Local high-concurrency reads therefore recorded a small number of
cancelled responses and late bytes, while the EC path recorded the expected
larger counts from 4-of-5 early cancellation. Those responses drained without
closing a Redis connection. Dragonfly logged only its known UDS warning that
TCP user timeout is unsupported on the socket; it logged no backend failure or
external version-check attempt.

## License gate

Dragonfly v1.39.0 uses BSL 1.1. Its Additional Use Grant permits production use
as part of another product or service only when that product is not an in-memory
data-store product/service and the licensed work is not made available as a
competing third-party managed service. The proposed topology keeps Dragonfly as
an internal, non-exposed cache implementation behind cache-ctl, so the recorded
technical deployment shape fits those conditions. An authorized organization
owner must still record approval before production rollout; this benchmark is
not legal approval.

## Remaining production qualification

1. Run a working set larger than the configured Dragonfly RAM budget on the
   target NVMe class with uniform cold reads.
2. Capture offloaded entries, pending reads/stashes, throttling, disk IOPS, and
   cache-ctl Redis cancellation/reconnect/error deltas.
3. Repeat during active offload, defragmentation, 80%, 90%, and near-disk-budget
   states.
4. Exercise a slow cancelled peer and a one-peer cold restart/repair storm.
5. Keep Dragonfly disabled as the EC production default unless that complete
   matrix meets P50 < 1 ms and P99.9 < 5 ms at the declared load point.
