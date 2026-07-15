# Redis-compatible local cache backend

`cache-ctl` can use a Redis-compatible server over a local Unix socket for all
physical cache roles:

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
  socket: /run/kuasar-cache/redis.sock
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
      socket: /run/kuasar-cache/redis.sock
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

Only absolute Unix socket paths are accepted. The backend cannot be configured
with a TCP endpoint, AUTH credentials, Redis Cluster, or a remote service.

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

GET and SET use separate, eagerly connected, fixed-size UDS worker pools. One
connection carries at most one in-flight command, making RESP FIFO ownership
explicit and preventing fill traffic from consuming the read pool.

When an object or shard GET is cancelled, the caller returns immediately. The
worker reads and discards the late response before returning the connection to
the pool. A normal cancellation never closes the external wire connection or
the Redis UDS connection. A malformed response or real UDS I/O failure closes
and reconnects only that worker.

SET uses `net.Buffers` for header/key/value `writev`. The caller's value remains
borrowed until the socket write completes. If cancellation arrives after the
write, the caller can return while the worker drains `+OK`; the worker never
retains the caller's payload after `Fill` returns.

## Readiness and observability

The gRPC health service probes a reserved key over a dedicated Redis
connection. Probe traffic does not use the data pools or alter hit/miss
counters. A failed Redis probe marks the daemon `NOT_SERVING`; it does not
change EC membership or placement epochs.

`cache-ctl info` exposes backend type, socket, pool size/connectivity,
in-flight and draining counts, cancellations, late bytes, reconnects, protocol
errors, backend errors, and GET/SET latency percentiles.

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

Dragonfly v1.39.0 is distributed under BSL 1.1. Its Additional Use Grant allows
use as part of another product or service when that offering is not an
in-memory data-store product/service and does not expose Dragonfly as a
competing managed service. The platform owner must record that this deployment
fits those conditions before production rollout; otherwise obtain a commercial
license.

Dragonfly's SSD tier is cache capacity, not authoritative persistence. A local
or tiered instance can refill after restart. For RS(4+1), restart at most one
shard peer at a time and rate-limit repair while that peer is cold.

[Dragonfly SSD tiering](https://www.dragonflydb.io/docs/managing-dragonfly/tiering)
currently requires Linux 5.19 or newer with `io_uring`. Capacity and latency
acceptance must include active offload/defragmentation and near-full disk
states; the systemd sample is deployment scaffolding, not a performance
qualification result.
