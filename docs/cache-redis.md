# Redis-compatible cache backend

`cache-ctl` can use a Redis-compatible server over a Unix domain socket (UDS)
or a direct TCP connection for all physical cache roles:

- `mode: local`: complete objects;
- `mode: shard`: EC shard values;
- `mode: tiered`, `type: redis`: a node-local complete-object tier.

The Accelerator configuration and implementation refer only to the Redis
protocol. Dragonfly is the recommended production server when the working set
must extend to local NVMe; it is not a separate backend type.

## Configuration

Direct local and shard daemons select the backend at the top level:

```yaml
mode: shard # or local
type: redis
listen: 0.0.0.0:7070
health_listen: 0.0.0.0:7071
rpc_timeout: 2s

redis:
  endpoint: unix:///run/kuasar-cache/redis.sock
  get_pool: 32
  set_pool: 8
  timeout: 2s
```

A tiered daemon declares Redis directly in the ordered tier list:

```yaml
mode: tiered
tiers:
  - type: redis
    redis:
      endpoint: 10.0.1.60:6379
      get_pool: 32
      set_pool: 8
      timeout: 2s
  - type: ec
    cluster: ...
origin: ...
```

There is no `type: dragonfly` and no generic `storage` wrapper. Existing
in-process RocksDB configurations use `type: embedded`.

Do not set the generic tier `max_inflight` on a Redis tier. `redis.get_pool`
and `redis.set_pool` are the independent hard concurrency budgets; keeping
them separate prevents asynchronous fill and repair writes from consuming the
read-side budget. Validation rejects the ambiguous combined limit.

`redis.endpoint` accepts an absolute socket path, `unix:/abs/path`,
`unix:///abs/path`, or one TCP `host:port`. Bracket IPv6 literals, for example
`[2001:db8::60]:6379`. UDS remains the recommended node-local low-latency
transport. TCP is plain RESP2 and has no AUTH/ACL or TLS support in this
backend; use it only on a trusted network protected by bind rules and a
firewall. Redis Cluster discovery is not supported.

## Data model

The implementation uses binary RESP2 bulk strings and only GET/SET. A key is a
fixed 35-byte value:

```text
version[1] | partition[1] | kind[1] | SHA-256[32]
```

`partition` separates chunk, manifest, and blob. `kind` separates complete
objects from EC shards, so identical content hashes cannot collide. Object
values are stored unchanged. Shard values retain the existing
`[idx][total][shard_data]` format.

Only a RESP nil bulk response is a confirmed cache miss. Timeout, EOF, `-ERR`,
and malformed RESP are backend errors and must not trigger EC repair or tier
fallthrough.

## Connection and cancellation behavior

GET and SET use separate, eagerly connected, fixed-size worker pools. One
connection carries at most one in-flight command, making RESP FIFO ownership
explicit and preventing fill traffic from consuming the read pool.

When an object or shard GET is cancelled, the caller returns immediately. The
worker reads and discards the late response before returning the connection to
the pool. A normal cancellation never closes the external wire connection or
the Redis backend connection. A malformed response or real UDS/TCP I/O failure
closes and reconnects only that worker using the configured transport.

SET uses `net.Buffers` for header/key/value `writev`. The caller's value remains
borrowed until the socket write completes. If cancellation arrives after the
write, the caller can return while the worker drains `+OK`; the worker never
retains the caller's payload after `Fill` returns.

## Readiness and observability

The gRPC health service probes a reserved key over a dedicated Redis
connection. Probe traffic does not use the data pools or alter hit/miss
counters. A failed Redis probe marks the daemon `NOT_SERVING`; it does not
change EC membership or placement epochs.

`cache-ctl info` exposes backend type, endpoint, transport, pool
size/connectivity, in-flight and draining counts, cancellations, late bytes,
reconnects, protocol errors, backend errors, and GET/SET latency percentiles.

## Dragonfly deployment

The sample unit is
[`deploy/systemd/dragonfly-cache.service`](../deploy/systemd/dragonfly-cache.service).
Its flag set is pinned to
[Dragonfly v1.39.0](https://github.com/dragonflydb/dragonfly/releases/tag/v1.39.0).
Release assembly must obtain the official `dragonfly-<arch>.tar.gz` once,
verify the selected architecture against
[`deploy/dragonfly-v1.39.0.sha256`](../deploy/dragonfly-v1.39.0.sha256), and
publish the archive plus checksum through the internal China-accessible
artifact service. The checked-in hashes are the SHA-256 digests published in
the v1.39.0 GitHub Release asset metadata for both supported architectures.
The target host must not download Dragonfly at install or service start time.
Verify the internally distributed archive before extraction:

```bash
sha256sum --check --ignore-missing deploy/dragonfly-v1.39.0.sha256
```

Install the verified binary exactly as:

```text
/usr/local/libexec/kuasar-cache/dragonfly-v1.39.0
```

Copy `deploy/systemd/dragonfly.env.example` to
`/etc/kuasar-cache/dragonfly.env` and size it for the host. The example is:

```bash
DRAGONFLY_MAX_MEMORY=412316860416
DRAGONFLY_TIER_PREFIX=/mnt/ssd/dragonfly/tier
DRAGONFLY_TIER_BYTES=5497558138880
DRAGONFLY_TIER_OFFLOAD_THRESHOLD=0.4
DRAGONFLY_TIER_UPLOAD_THRESHOLD=0.2
DRAGONFLY_PROACTOR_THREADS=16
```

`DRAGONFLY_TIER_BYTES` is a total disk budget; Dragonfly divides it across the
configured proactor threads. It must be a multiple of 256 MiB and large enough
to provide at least 256 MiB per thread. Provision the tier-prefix parent on the
NVMe filesystem and grant `kuasar-cache` write access before starting the unit.

Run Dragonfly and `cache-ctl` in separate cpusets and align them with the NVMe
NUMA node. The unit disables the TCP listener and exposes only
`/run/kuasar-cache/redis.sock`. It also disables Dragonfly's daily release
check, so the service performs no version-site request from the target host.
This unit is the recommended node-local topology. A separately managed remote
server may expose TCP, but its bind/firewall policy and network latency are
operator responsibilities.

Dragonfly v1.39.0 is distributed under BSL 1.1. Its Additional Use Grant allows
use as part of another product or service when that offering is not an
in-memory data-store product/service and does not expose Dragonfly as a
competing managed service. The platform owner must record that this deployment
fits those conditions before production rollout; otherwise obtain a commercial
license.

Dragonfly's SSD tier is cache capacity, not authoritative persistence. A local
or tiered instance can refill after restart. For RS(4+1), restart at most one
shard peer at a time and rate-limit repair while that peer is cold.

## Backend A/B benchmark

The current BMS hot-path result and deployment decision are recorded in
[`cache-backend-ab-2026-07-17.md`](cache-backend-ab-2026-07-17.md).

The repository's existing cache wire benchmark can select the physical store
without changing its client workload:

```bash
# Embedded RocksDB baseline.
SERVER_CORES=0-7 CLIENT_CORES=8-11 \
  PREFILL=1000 DURATION=5s \
  ACCESS=uniform GET_CONCS='1 4 16 32' MIXED_CONCS=8 \
  bash test/scripts/bench_cache.sh

# Redis-compatible local backend. Start a disposable, empty server first.
BENCH_BACKEND=redis REDIS_RESET=flushdb \
  REDIS_ENDPOINT=unix:///run/kuasar-bench/redis.sock \
  SERVER_CORES=0-3 BACKEND_CORES=4-7 CLIENT_CORES=8-11 \
  PREFILL=1000 DURATION=5s \
  ACCESS=uniform GET_CONCS='1 4 16 32' MIXED_CONCS=8 \
  bash test/scripts/bench_cache.sh
```

`bench_cache.sh` manages only cache-ctl processes. The Redis-compatible server
is an external deployment component, so the operator must pin its process to
`BACKEND_CORES`, give the benchmark an isolated namespace, and stop it after
the run.
`BACKEND_CORES` is recorded in the report but is not applied by the script.
Report both cache-ctl and backend CPU allocations; comparing an unbounded
Dragonfly process with a bounded embedded process is not a valid A/B.
`REDIS_RESET=flushdb` is deliberately explicit and destructive: every endpoint
must be dedicated to the benchmark. The harness clears it before the first
point and between points, then restarts its cache-ctl daemons. Embedded runs
recreate their RocksDB paths instead. This keeps capacity and cold-key state
identical across the sweep. External cache-ctl mode cannot reset backing state
and accepts only one point per invocation. In that mode, Redis endpoint, pool
and timeout fields are reported only when explicitly provided; otherwise the
report marks them as unknown.
With no phase override, external mode runs one GET point at concurrency 1.
Setting `GET_CONCS`, `PUT_CONCS`, or `MIXED_CONCS` makes unspecified phases
empty, and the selected phase must still contain exactly one point. A pool size
of zero is reported as its effective cache-ctl default (32 GET, 8 SET).

For the EC path, provide exactly five independent Redis endpoints:

```bash
BENCH_SCENARIO=tiered-shard-l2 BENCH_BACKEND=redis REDIS_RESET=flushdb \
  REDIS_SHARD_ENDPOINTS='unix:///run/df1.sock unix:///run/df2.sock unix:///run/df3.sock unix:///run/df4.sock unix:///run/df5.sock' \
  SERVER_CORES=0-3 BACKEND_CORES=4-7 CLIENT_CORES=8-11 \
  ACCESS=uniform GET_CONCS='1 4 16 32' \
  bash test/scripts/bench_cache.sh
```

Each shard peer owns a physical storage namespace. Pointing multiple shard
peers at the same Redis namespace is invalid because their values for one
content key represent different EC shard indexes and would overwrite each
other. In production those endpoints normally reside on different nodes; a
single-host benchmark must use separate server instances or otherwise
isolated namespaces.

Hot-hit acceptance requires a working set that fits the configured memory.
SSD-tier acceptance requires a uniform working set larger than the server's
RAM budget, observed offloaded-entry and pending-I/O metrics, and storage that
matches the production NVMe class. Results from a virtual block device may be
used for functional tiering tests but not for the production latency claim.

[Dragonfly SSD tiering](https://www.dragonflydb.io/docs/managing-dragonfly/tiering)
currently requires Linux 5.19 or newer with `io_uring`. Capacity and latency
acceptance must include active offload/defragmentation and near-full disk
states; the systemd sample is deployment scaffolding, not a performance
qualification result.
