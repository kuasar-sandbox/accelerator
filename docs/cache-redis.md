# Redis-compatible cache backend

`cache-ctl` can use a Redis-compatible server over a Unix domain socket (UDS)
or a direct TCP connection for all physical cache roles:

- `mode: local`: complete objects;
- `mode: shard`: EC shard values;
- `mode: tiered`, `type: redis`: a node-local complete-object tier.

The Accelerator configuration and implementation refer only to the Redis
protocol. Dragonfly is one example of a separately managed external server,
not a separate backend type or an Accelerator release component.

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

## External Dragonfly deployment example

The sample unit is
[`deploy/systemd/dragonfly-cache.service`](../deploy/systemd/dragonfly-cache.service).
Its flag set is pinned to
[Dragonfly v1.39.0](https://github.com/dragonflydb/dragonfly/releases/tag/v1.39.0).
Accelerator does not download, bundle, publish, or redistribute the Dragonfly
binary. An operator choosing this external service must obtain and manage it
separately under its own software-supply and license policy. The checked-in
hashes are the SHA-256 digests published in the v1.39.0 GitHub Release asset
metadata for both supported architectures, so an operator can verify its
separately acquired archive. The target host should install an already
verified artifact and must not download Dragonfly at service start time:

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
This unit is a sample node-local topology. A separately managed remote server
may expose TCP, but its bind/firewall policy and network latency are operator
responsibilities.

Dragonfly v1.39.0 is distributed under BSL 1.1. Because Accelerator neither
contains nor redistributes it, that license does not change the Accelerator
release license or backend acceptance. Operators remain responsible for the
terms of any external Redis-compatible server they select.

Dragonfly's SSD tier is cache capacity, not authoritative persistence. A local
or tiered instance can refill after restart. For RS(4+1), restart at most one
shard peer at a time and rate-limit repair while that peer is cold.

## Backend A/B benchmark

The complete backend A/B commands, workload controls, CPU accounting, isolated endpoint requirements, destructive-reset safeguards and SSD-tier acceptance criteria are maintained in [Cache §7.1](cache.md#71-backend-and-wire-benchmarks) ([Chinese edition](cache_zh.md)). This guide owns Redis-compatible backend configuration and external-service deployment.

## Build without RocksDB

`make cache-ctl NO_ROCKSDB=1` builds the normal `cache-ctl` artifact with
RocksDB support compiled out (`CGO_ENABLED=0`, `-tags no_rocksdb`). Only
the RocksDB implementation files in `pkg/cache/rocks` carry the build tag;
the rest of the codebase is unaware of it. `rocks.Open` returns a
`not compiled` error in this build, so configs selecting `type: embedded`
fail at daemon startup with an error naming the tag. The default
`make cache-ctl` artifact is unchanged. `make test-no-rocksdb` compiles,
vets, and tests the tagged surface without any `librocksdb` present.
