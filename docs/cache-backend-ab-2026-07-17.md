# Cache backend BMS A/B: 2026-07-17

## Decision

Keep the Redis-compatible backend as an explicit deployment option, but do not
make Dragonfly the default backend for the EC shard path. On the tested equal
eight-CPU service budget, Dragonfly met the latency target for direct local and
tiered-L1 RAM hits through concurrency 4. The five-peer EC path met the target
only at concurrency 1; at concurrency 4 its P50 was 1.94 ms, while embedded
RocksDB remained at 760 us.

The result does not qualify Dragonfly SSD tiering. The available BMS has a
virtual rotational block device rather than production-class NVMe. Offload,
defragmentation, near-full capacity, and repair-storm acceptance remain blocked
on a representative NVMe host.

## Revisions and environment

- Accelerator source: `bfdd96f32902b3a1982cc6b29d478df824e606f6`.
  The Actions workspace was assembled from a GitHub tarball and therefore had
  no repository-local `.git`; every unchanged tracked file was checksum-matched
  against this revision before accepting the result.
- Benchmark harness: `7bbf6a2` (the change adding backend selection).
- cache-ctl binary SHA-256:
  `212135927286ece05ccf6ed2d98309c2bb5c57d3505c539c210e67c046032d54`.
- Dragonfly: v1.39.0, official x86_64 archive SHA-256
  `81f88cfd0096c550e415a700c97eb5bf606db1df6106cc03584e878fd7aa126a`.
  The 19 MiB archive was downloaded and verified on the operator host, then
  copied to BMS; the BMS did not download it from an international endpoint.
- Host: 88 logical CPUs, Intel Xeon Gold 6266C, 375 GiB RAM, two NUMA nodes,
  openEuler kernel 6.6.0, cgroup v2.
- Storage: 100 GiB ext4 root on `VBS fileIO`, reported rotational. No NVMe was
  available.

The GitHub Actions runner was stopped for the measurement and restored by an
EXIT trap. All processes were pinned to NUMA node 0. Embedded used CPUs 0-7.
The Redis path used CPUs 0-3 for cache-ctl and CPUs 4-7 for Dragonfly, preserving
the same total service CPU budget. The client used CPUs 8-11. For EC, five
independent Dragonfly instances shared the four backend CPUs because each shard
peer requires an independent physical key namespace.

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

The complete six-scenario, three-repetition run took about 9 minutes 51 seconds,
below the 30-minute project limit.

## GET results

Latency cells are `P50 / P99.9`.

| Scenario | Concurrency | Embedded ops/s | Embedded latency | Dragonfly ops/s | Dragonfly latency |
| --- | ---: | ---: | ---: | ---: | ---: |
| local | 1 | 2,351 | 417 / 727 us | 1,874 | 530 us / 2.32 ms |
| local | 4 | 7,066 | 531 us / 1.27 ms | 4,235 | 832 us / 3.52 ms |
| local | 16 | 10,635 | 1.32 / 6.19 ms | 6,676 | 2.00 / 10.4 ms |
| local | 32 | 11,398 | 1.57 / 16.3 ms | 8,071 | 2.99 / 22.5 ms |
| tiered L1 | 1 | 2,410 | 406 / 689 us | 1,869 | 532 us / 2.31 ms |
| tiered L1 | 4 | 7,202 | 522 us / 1.24 ms | 4,252 | 824 us / 3.50 ms |
| tiered L1 | 16 | 10,953 | 1.27 / 6.19 ms | 6,691 | 1.97 / 10.3 ms |
| tiered L1 | 32 | 11,723 | 1.41 / 16.2 ms | 8,034 | 3.01 / 23.8 ms |
| EC 4+1 | 1 | 1,922 | 500 us / 1.03 ms | 1,288 | 749 us / 1.41 ms |
| EC 4+1 | 4 | 4,950 | 760 us / 1.81 ms | 2,052 | 1.94 / 3.19 ms |
| EC 4+1 | 16 | 5,723 | 2.73 / 4.33 ms | 2,105 | 7.60 / 8.88 ms |
| EC 4+1 | 32 | 5,858 | 5.43 / 7.32 ms | 2,110 | 15.2 / 16.5 ms |

The first multi-connection EC point is the decisive regression: embedded met
both P50 < 1 ms and P99.9 < 5 ms at concurrency 4, while Dragonfly exceeded the
P50 target and delivered 41% of embedded throughput. The one-host EC setup
amplifies process scheduling overhead compared with five physical shard nodes,
but it applies the same total CPU budget and is the current reproducible BMS
acceptance topology. A distributed rerun may add evidence; it must not replace
this failed equal-budget result.

## Write and mixed results

| Mode | Embedded ops/s | Embedded P50 / P99.9 | Dragonfly ops/s | Dragonfly P50 / P99.9 |
| --- | ---: | ---: | ---: | ---: |
| PUT c1 | 1,856 | 453 us / 8.43 ms | 2,082 | 518 / 882 us |
| 50/50 mixed c8 | 3,363 | 1.80 / 12.8 ms | 3,020 | 2.31 / 6.50 ms |

Dragonfly produced a substantially tighter write tail in this RAM/cache-mode
test, while its read path paid the extra UDS/RESP process boundary. This is a
useful local-cache tradeoff, not evidence for SSD behavior.

## Observability note

The benchmark ends by cancelling the client context. A small number of final
in-flight calls therefore appear in generic tier/origin `errors`. On the EC
path, a locally cancelled pool wait currently increments generic peer
`errors`, while a peer-confirmed `StatusCancelled` reply increments
`cancelled`. In this controlled run both rose with 4-of-5 early cancellation
and the benchmark window boundary, so generic peer/tier errors cannot be read
as Redis backend failures. The harness change accompanying this report adds
benchmark-window deltas for Redis `cancelled`, `reconnects`, `backend_errors`,
and `protocol_errors`, so subsequent runs can distinguish those classes
directly.

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
