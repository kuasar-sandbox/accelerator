#!/bin/bash
set -euo pipefail

# Cache performance smoke benchmark.
#
# Spins up cache-ctl daemon(s) pinned to a fixed set of CPU cores, runs a
# GET/PUT concurrency sweep from a client pinned to a disjoint core set,
# and prints a human-readable report of throughput + latency percentiles.
#
# Scenarios (BENCH_SCENARIO env var, default "local"):
#
#   local            1 local cache-ctl (today's default behaviour)
#   tiered-l1        1 local (store role) + 1 tiered (L1 selected backend,
#                    origin: upstream → local)
#   tiered-shard-l2  1 local (store role) + 5 shard (L2 EC cluster) +
#                    1 tiered (no L1, tiers=[ec], origin: upstream → local)
#
# BENCH_BACKEND selects the physical store used by the measured cache-ctl
# daemon(s):
#
#   embedded          in-process RocksDB (default)
#   redis             external Redis-compatible server over UDS or TCP
#
# Redis server lifecycle is deliberately external to this protocol-neutral
# harness. local/tiered-l1 require REDIS_ENDPOINT; tiered-shard-l2 requires
# five whitespace-separated REDIS_SHARD_ENDPOINTS, one independent storage
# namespace per physical shard peer.
#
# External mode (skip spin-up, bench a pre-started instance):
#
#   BENCH_EXTERNAL_ENDPOINT=ip:port            [required]
#   BENCH_EXTERNAL_PREFILL_ENDPOINT=ip:port    [optional; default=external endpoint]
#
# Usage:
#   bash test/scripts/bench_cache.sh
#   BENCH_SCENARIO=tiered-l1 bash test/scripts/bench_cache.sh
#   BENCH_SCENARIO=tiered-shard-l2 bash test/scripts/bench_cache.sh
#   BENCH_BACKEND=redis REDIS_ENDPOINT=unix:///run/dragonfly/redis.sock \
#     bash test/scripts/bench_cache.sh
#   BENCH_BACKEND=redis BENCH_SCENARIO=tiered-shard-l2 \
#     REDIS_SHARD_ENDPOINTS='unix:///run/df1.sock ... unix:///run/df5.sock' \
#     bash test/scripts/bench_cache.sh
#   BENCH_EXTERNAL_ENDPOINT=127.0.0.1:7700 bash test/scripts/bench_cache.sh
#
#   SERVER_CORES=0   CLIENT_CORES=1       bash test/scripts/bench_cache.sh
#   SERVER_CORES=0-1 CLIENT_CORES=2-5     bash test/scripts/bench_cache.sh
#   VALUE_SIZE=262144 DURATION=5s         bash test/scripts/bench_cache.sh
#   GET_CONCS="1 4 16" PUT_CONCS="1 2"    bash test/scripts/bench_cache.sh
#   GET_CONCS="" PUT_CONCS="1 4 8"        bash test/scripts/bench_cache.sh  # PUT only

BENCH_SCENARIO="${BENCH_SCENARIO:-local}"
BENCH_BACKEND_EXPLICIT="${BENCH_BACKEND+x}"
BENCH_BACKEND="${BENCH_BACKEND:-embedded}"
BENCH_EXTERNAL_ENDPOINT="${BENCH_EXTERNAL_ENDPOINT:-}"
BENCH_EXTERNAL_PREFILL_ENDPOINT="${BENCH_EXTERNAL_PREFILL_ENDPOINT:-}"
REDIS_ENDPOINT_EXPLICIT="${REDIS_ENDPOINT+x}"
REDIS_SHARD_ENDPOINTS_EXPLICIT="${REDIS_SHARD_ENDPOINTS+x}"
REDIS_GET_POOL_EXPLICIT="${REDIS_GET_POOL+x}"
REDIS_SET_POOL_EXPLICIT="${REDIS_SET_POOL+x}"
REDIS_TIMEOUT_EXPLICIT="${REDIS_TIMEOUT+x}"
REDIS_ENDPOINT="${REDIS_ENDPOINT:-}"
REDIS_SHARD_ENDPOINTS="${REDIS_SHARD_ENDPOINTS:-}"
REDIS_GET_POOL="${REDIS_GET_POOL:-32}"
REDIS_SET_POOL="${REDIS_SET_POOL:-8}"
REDIS_TIMEOUT="${REDIS_TIMEOUT:-5s}"
REDIS_RESET="${REDIS_RESET:-}"
BACKEND_CORES="${BACKEND_CORES:-}"

SERVER_CORES="${SERVER_CORES:-0-1}"
CLIENT_CORES="${CLIENT_CORES:-2-3}"
VALUE_SIZE="${VALUE_SIZE:-524288}"
DURATION="${DURATION:-10s}"
PREFILL="${PREFILL:-2000}"
# Use ${VAR-default} (no colon) so users can pass empty string to skip a phase,
# e.g. GET_CONCS="" to run PUT only.
GET_CONCS="${CONCS:-${GET_CONCS-1 2 4 8}}"
PUT_CONCS="${CONCS:-${PUT_CONCS-1}}"
MIXED_CONCS="${MIXED_CONCS-}"
ACCESS="${ACCESS:-seq}"
ZIPF_S="${ZIPF_S:-1.1}"
COLD_PREFILL="${COLD_PREFILL:-0}"
MISS_RATIO="${MISS_RATIO:-0}"
TIMEOUT="${TIMEOUT:-10s}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="${BIN:-$PROJECT_ROOT/bin}"
WORKDIR=$(mktemp -d /tmp/acc-bench-XXXXXX)
RAW="$WORKDIR/raw.tsv"
: > "$RAW"

# Endpoints resolved during setup. BENCH_ENDPOINT is the bench target
# (where reads go); PREFILL_ENDPOINT is where writes land. For the
# local scenario both are the same; for tiered scenarios they differ.
BENCH_ENDPOINT=""
PREFILL_ENDPOINT=""
HEALTH_ENDPOINT=""
SHARD_HEALTH_ENDPOINTS=()
BENCH_ROUND=0
BENCH_RUN_ID="${BENCH_RUN_ID:-$(date +%s%N)-$$}"

# ALL_PIDS holds every daemon PID we spawn so cleanup can reap them on
# exit. External mode leaves this array empty and the trap is a noop.
# ALL_LOGS holds the path to each daemon's captured stdout+stderr log;
# cleanup prints tail -50 of each log on non-zero exit so daemon
# crashes stay diagnosable without re-running under `tee`.
ALL_PIDS=()
ALL_LOGS=()

stop_daemons() {
    for pid in "${ALL_PIDS[@]:-}"; do
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    ALL_PIDS=()
}

cleanup() {
    local rc=$?
    stop_daemons
    if [ "$rc" -ne 0 ]; then
        for log in "${ALL_LOGS[@]:-}"; do
            if [ -n "$log" ] && [ -s "$log" ]; then
                echo "===== $log =====" >&2
                tail -50 "$log" >&2
            fi
        done
    fi
    if [ -z "${KEEP_WORKDIR:-}" ]; then
        rm -rf "$WORKDIR"
    else
        echo "KEEP_WORKDIR=1; logs + pprof files at $WORKDIR" >&2
    fi
}
trap cleanup EXIT

# ── Preflight ─────────────────────────────────────────────────────────────
if ! command -v taskset >/dev/null 2>&1; then
    echo "WARN: taskset not found, running unbound" >&2
    SERVER_CORES=""
    CLIENT_CORES=""
fi

NCPU=$(nproc 2>/dev/null || echo 1)
if [ -n "$SERVER_CORES" ] && [ "$NCPU" -lt 4 ]; then
    echo "WARN: only $NCPU cores available, degrading to SERVER=0 CLIENT=1" >&2
    SERVER_CORES="0"
    CLIENT_CORES="1"
fi

# Count CPUs in a taskset-style spec: "0" → 1, "0-1" → 2, "0,2,4" → 3.
count_cores() {
    local spec=$1
    [ -z "$spec" ] && { echo 0; return; }
    local total=0 part
    local IFS=','
    for part in $spec; do
        if [[ $part == *-* ]]; then
            total=$((total + ${part##*-} - ${part%%-*} + 1))
        else
            total=$((total + 1))
        fi
    done
    echo "$total"
}

case "$BENCH_BACKEND" in
    embedded|redis) ;;
    *)
        echo "ERROR: BENCH_BACKEND must be embedded or redis" >&2
        exit 1
        ;;
esac

if [ -z "$BENCH_EXTERNAL_ENDPOINT" ] && [ "$BENCH_BACKEND" = redis ]; then
    case "$BENCH_SCENARIO" in
        local|tiered-l1)
            if [ -z "$REDIS_ENDPOINT" ]; then
                echo "ERROR: BENCH_BACKEND=redis with $BENCH_SCENARIO requires REDIS_ENDPOINT" >&2
                exit 1
            fi
            ;;
        tiered-shard-l2)
            read -r -a REDIS_SHARD_ENDPOINT_ARRAY <<< "$REDIS_SHARD_ENDPOINTS"
            if [ "${#REDIS_SHARD_ENDPOINT_ARRAY[@]}" -ne 5 ]; then
                echo "ERROR: redis tiered-shard-l2 requires exactly 5 REDIS_SHARD_ENDPOINTS" >&2
                exit 1
            fi
            declare -A seen_redis_shard_endpoints=()
            for endpoint in "${REDIS_SHARD_ENDPOINT_ARRAY[@]}"; do
                if [ -n "${seen_redis_shard_endpoints[$endpoint]+x}" ]; then
                    echo "ERROR: redis tiered-shard-l2 requires distinct REDIS_SHARD_ENDPOINTS; duplicate: $endpoint" >&2
                    exit 1
                fi
                seen_redis_shard_endpoints[$endpoint]=1
            done
            ;;
    esac
    if [ "$REDIS_RESET" != flushdb ]; then
        echo "ERROR: Redis benchmarks require REDIS_RESET=flushdb and dedicated benchmark-only endpoints" >&2
        exit 1
    fi
fi

miss_ratio_nonzero() {
    awk -v ratio="$MISS_RATIO" 'BEGIN { exit !(ratio + 0 != 0) }'
}

if [ "$COLD_PREFILL" -eq 0 ] && miss_ratio_nonzero; then
    echo "ERROR: MISS_RATIO requires COLD_PREFILL > 0" >&2
    exit 1
fi

if [ "$COLD_PREFILL" -ne 0 ] || miss_ratio_nonzero; then
    if [ "$BENCH_SCENARIO" = local ] && [ -z "$BENCH_EXTERNAL_PREFILL_ENDPOINT" ]; then
        echo "ERROR: COLD_PREFILL/MISS_RATIO require a split prefill endpoint" >&2
        exit 1
    fi
fi

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}

flush_redis_endpoints() {
    python3 - "$@" <<'PY'
import socket
import sys

for endpoint in sys.argv[1:]:
    if endpoint.startswith("unix://"):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        address = endpoint[len("unix://"):]
    elif endpoint.startswith("unix:"):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        address = endpoint[len("unix:"):]
    elif endpoint.startswith("/"):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        address = endpoint
    else:
        if endpoint.startswith("["):
            host, port = endpoint[1:].rsplit("]:", 1)
        else:
            host, port = endpoint.rsplit(":", 1)
        family = socket.AF_INET6 if ":" in host else socket.AF_INET
        sock = socket.socket(family, socket.SOCK_STREAM)
        address = (host, int(port))
    sock.settimeout(10)
    try:
        sock.connect(address)
        sock.sendall(b"*1\r\n$7\r\nFLUSHDB\r\n")
        with sock.makefile("rb") as reader:
            reply = reader.readline(64)
        if reply != b"+OK\r\n":
            raise RuntimeError(f"unexpected FLUSHDB reply {reply!r}")
    finally:
        sock.close()
    print(f"reset Redis benchmark endpoint {endpoint}", file=sys.stderr)
PY
}

reset_redis_backends() {
    if [ "$BENCH_SCENARIO" = tiered-shard-l2 ]; then
        flush_redis_endpoints "${REDIS_SHARD_ENDPOINT_ARRAY[@]}"
    else
        flush_redis_endpoints "$REDIS_ENDPOINT"
    fi
}

# spawn_daemon <config-path> <health-port> <label>
# Spawns cache-ctl serve with the given config, pinned to SERVER_CORES,
# and waits for the health endpoint to report SERVING. Daemon stdout+
# stderr is captured to $WORKDIR/$label.log so the cleanup trap can
# dump it on failure — never route daemon output to /dev/null, it hides
# the only signal available when startup fails. Appends the new PID to
# ALL_PIDS and the log path to ALL_LOGS. Fatal on timeout.
spawn_daemon() {
    local config=$1
    local health=$2
    local label=$3
    local log="$WORKDIR/$label.log"

    if [ -n "$SERVER_CORES" ]; then
        taskset -c "$SERVER_CORES" env CACHE_CTL_TIMING="${CACHE_CTL_TIMING:-}" "$BIN/cache-ctl" serve --config "$config" >"$log" 2>&1 &
    else
        env CACHE_CTL_TIMING="${CACHE_CTL_TIMING:-}" "$BIN/cache-ctl" serve --config "$config" >"$log" 2>&1 &
    fi
    local pid=$!
    ALL_PIDS+=("$pid")
    ALL_LOGS+=("$log")

    for _ in $(seq 1 100); do
        if "$BIN/cache-ctl" ping --endpoint "127.0.0.1:$health" 2>/dev/null | grep -q SERVING; then
            return 0
        fi
        sleep 0.1
    done
    echo "ERROR: cache-ctl daemon '$label' did not become ready" >&2
    echo "--- tail of $log ---" >&2
    tail -30 "$log" >&2 || true
    return 1
}

# ── Scenario helpers ──────────────────────────────────────────────────────

# Benchmark-owned Rocks stores keep the prefilled dataset intact. Production
# eviction would turn a backend-hit measurement into a miss/fallthrough test.
start_local_only() {
    local data health rocks config
    data=$(free_port)
    health=$(free_port)
    rocks="$WORKDIR/local-rocks"
    config="$WORKDIR/local.yaml"
    cat > "$config" <<EOF
mode: local
type: $BENCH_BACKEND
listen: 127.0.0.1:$data
health_listen: 127.0.0.1:$health
rpc_timeout: 5s
EOF
    if [ "$BENCH_BACKEND" = embedded ]; then
        cat >> "$config" <<EOF
freq:
  counters: 1M
  reset_after: 100K
  disable_eviction: true
rocks:
  path: $rocks
  disk_bytes: 4GiB
  mem_ratio: 0.1
  direct_reads: false
  bloom_bits: 10
EOF
    else
        cat >> "$config" <<EOF
redis:
  endpoint: $REDIS_ENDPOINT
  get_pool: $REDIS_GET_POOL
  set_pool: $REDIS_SET_POOL
  timeout: $REDIS_TIMEOUT
EOF
    fi
    spawn_daemon "$config" "$health" "local"
    BENCH_ENDPOINT="127.0.0.1:$data"
    PREFILL_ENDPOINT="127.0.0.1:$data"
    HEALTH_ENDPOINT="127.0.0.1:$health"
}

# start_local_as_origin spawns a local cache-ctl intended to act as the
# upstream origin for a downstream tiered instance. The frequency-based
# compaction filter is disabled (freq.disable_eviction: true) — an
# origin daemon must never evict once-touched keys, otherwise prefill
# data vanishes before the bench rounds start. Returns the data endpoint
# via the global LOCAL_ORIGIN_EP, health via LOCAL_ORIGIN_HP.
start_local_as_origin() {
    local data health rocks config
    data=$(free_port)
    health=$(free_port)
    rocks="$WORKDIR/origin-rocks"
    config="$WORKDIR/origin.yaml"
    cat > "$config" <<EOF
mode: local
type: embedded
listen: 127.0.0.1:$data
health_listen: 127.0.0.1:$health
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
  disable_eviction: true
rocks:
  path: $rocks
  disk_bytes: 4GiB
  mem_ratio: 0.1
  direct_reads: false
  bloom_bits: 10
EOF
    spawn_daemon "$config" "$health" "origin-local"
    LOCAL_ORIGIN_EP="127.0.0.1:$data"
    LOCAL_ORIGIN_HP="127.0.0.1:$health"
}

start_tiered_l1() {
    start_local_as_origin

    local data health rocks config
    data=$(free_port)
    health=$(free_port)
    rocks="$WORKDIR/tiered-l1-rocks"
    config="$WORKDIR/tiered-l1.yaml"
    cat > "$config" <<EOF
mode: tiered
listen: 127.0.0.1:$data
health_listen: 127.0.0.1:$health
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
  disable_eviction: true
tiers:
  - type: $BENCH_BACKEND
EOF
    if [ "$BENCH_BACKEND" = embedded ]; then
        cat >> "$config" <<EOF
    rocks:
      path: $rocks
      disk_bytes: 4GiB
      mem_ratio: 0.1
      direct_reads: false
      bloom_bits: 10
EOF
    else
        cat >> "$config" <<EOF
    redis:
      endpoint: $REDIS_ENDPOINT
      get_pool: $REDIS_GET_POOL
      set_pool: $REDIS_SET_POOL
      timeout: $REDIS_TIMEOUT
EOF
    fi
    cat >> "$config" <<EOF
origin:
  type: upstream
  upstream:
    endpoint: $LOCAL_ORIGIN_EP
    pool: 4
    timeout: 10s
  max_inflight: 16
EOF
    spawn_daemon "$config" "$health" "tiered-l1"
    BENCH_ENDPOINT="127.0.0.1:$data"
    PREFILL_ENDPOINT="$LOCAL_ORIGIN_EP"
    HEALTH_ENDPOINT="127.0.0.1:$health"
}

start_tiered_shard_l2() {
    start_local_as_origin

    # Five shard-mode daemons forming the L2 EC cluster.
    local shard_peers=()
    local shard_eps=()
    for i in 1 2 3 4 5; do
        local sdata shealth srocks sconfig
        sdata=$(free_port)
        shealth=$(free_port)
        srocks="$WORKDIR/shard-$i-rocks"
        sconfig="$WORKDIR/shard-$i.yaml"
        cat > "$sconfig" <<EOF
mode: shard
type: $BENCH_BACKEND
listen: 127.0.0.1:$sdata
health_listen: 127.0.0.1:$shealth
rpc_timeout: 5s
EOF
        if [ "$BENCH_BACKEND" = embedded ]; then
            cat >> "$sconfig" <<EOF
freq:
  counters: 1M
  reset_after: 100K
  disable_eviction: true
rocks:
  path: $srocks
  disk_bytes: 4GiB
  mem_ratio: 0.1
  direct_reads: false
  bloom_bits: 10
EOF
        else
            cat >> "$sconfig" <<EOF
redis:
  endpoint: ${REDIS_SHARD_ENDPOINT_ARRAY[$((i - 1))]}
  get_pool: $REDIS_GET_POOL
  set_pool: $REDIS_SET_POOL
  timeout: $REDIS_TIMEOUT
EOF
        fi
        spawn_daemon "$sconfig" "$shealth" "shard-$i"
        shard_peers+=("  - id: s$i")
        shard_peers+=("    endpoint: 127.0.0.1:$sdata")
        shard_eps+=("127.0.0.1:$sdata")
        if [ "$BENCH_BACKEND" = redis ]; then
            SHARD_HEALTH_ENDPOINTS+=("127.0.0.1:$shealth")
        fi
    done

    local data health pprof config
    data=$(free_port)
    health=$(free_port)
    pprof=$(free_port)
    config="$WORKDIR/tiered-shard-l2.yaml"
    {
        cat <<EOF
mode: tiered
listen: 127.0.0.1:$data
health_listen: 127.0.0.1:$health
pprof_listen: 127.0.0.1:$pprof
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
tiers:
  - type: ec
    cluster:
      data_shards: 4
      parity_shards: 1
      pool: 2
      timeout: 10s
      peers:
EOF
        for line in "${shard_peers[@]}"; do
            printf "    %s\n" "$line"
        done
        cat <<EOF
origin:
  type: upstream
  upstream:
    endpoint: $LOCAL_ORIGIN_EP
    pool: 4
    timeout: 10s
  max_inflight: 16
EOF
    } > "$config"
    spawn_daemon "$config" "$health" "tiered-shard-l2"
    BENCH_ENDPOINT="127.0.0.1:$data"
    PREFILL_ENDPOINT="$LOCAL_ORIGIN_EP"
    HEALTH_ENDPOINT="127.0.0.1:$health"
    PPROF_ENDPOINT="127.0.0.1:$pprof"
}

reset_owned_scenario() {
    stop_daemons
    rm -rf "$WORKDIR/local-rocks" "$WORKDIR/origin-rocks" \
        "$WORKDIR/tiered-l1-rocks" "$WORKDIR"/shard-*-rocks
    SHARD_HEALTH_ENDPOINTS=()
    BENCH_ENDPOINT=""
    PREFILL_ENDPOINT=""
    HEALTH_ENDPOINT=""
    PPROF_ENDPOINT=""
    if [ "$BENCH_BACKEND" = redis ]; then
        reset_redis_backends
    fi
    case "$BENCH_SCENARIO" in
        local)           start_local_only ;;
        tiered-l1)       start_tiered_l1 ;;
        tiered-shard-l2) start_tiered_shard_l2 ;;
    esac
}

# ── Resolve endpoints ─────────────────────────────────────────────────────
if [ -n "$BENCH_EXTERNAL_ENDPOINT" ]; then
    BENCH_ENDPOINT="$BENCH_EXTERNAL_ENDPOINT"
    PREFILL_ENDPOINT="${BENCH_EXTERNAL_PREFILL_ENDPOINT:-$BENCH_EXTERNAL_ENDPOINT}"
    echo "External mode: bench=$BENCH_ENDPOINT prefill=$PREFILL_ENDPOINT" >&2
else
    if [ "$BENCH_BACKEND" = redis ]; then
        reset_redis_backends
    fi
    case "$BENCH_SCENARIO" in
        local)           start_local_only ;;
        tiered-l1)       start_tiered_l1 ;;
        tiered-shard-l2) start_tiered_shard_l2 ;;
        *)
            echo "ERROR: unknown BENCH_SCENARIO $BENCH_SCENARIO" >&2
            exit 1
            ;;
    esac
fi

if [ -n "$BENCH_EXTERNAL_ENDPOINT" ]; then
    active_rounds=0
    if [ "$PREFILL_ENDPOINT" != "$BENCH_ENDPOINT" ]; then
        active_rounds=$(wc -w <<<"$GET_CONCS")
    else
        active_rounds=$(( $(wc -w <<<"$GET_CONCS") + $(wc -w <<<"$PUT_CONCS") + $(wc -w <<<"$MIXED_CONCS") ))
    fi
    if [ "$active_rounds" -gt 1 ]; then
        echo "ERROR: external mode permits one sweep point because the harness cannot reset external state" >&2
        exit 1
    fi
fi

# ── Bench driver ──────────────────────────────────────────────────────────
# run_bench <mode> <conc> [extra-flags...]
# Runs cache-ctl bench and appends raw numbers to $RAW as a TSV row.
# Format: mode \t conc \t ops \t thrpt \t bw \t p50_us \t p99_us \t p999_us
run_bench() {
    local mode=$1
    local conc=$2
    shift 2

    if [ "$BENCH_ROUND" -gt 0 ]; then
        reset_owned_scenario
    fi
    BENCH_ROUND=$((BENCH_ROUND + 1))
    local key_salt="$BENCH_RUN_ID-round-$BENCH_ROUND-$mode-$conc"
    local out="$WORKDIR/bench-$mode-$conc.out"
    printf "  running %s c=%s\n" "$mode" "$conc" >&2

    # Always pass --prefill-endpoint; when BENCH_ENDPOINT == PREFILL_ENDPOINT
    # the bench tool short-circuits to the same conn pool.
    local prefill_flag=()
    if [ "$PREFILL_ENDPOINT" != "$BENCH_ENDPOINT" ]; then
        prefill_flag=(--prefill-endpoint "$PREFILL_ENDPOINT")
    fi

    # Thread the bench target's HealthListen port through first, followed by
    # Redis EC shard Info endpoints. cache-ctl bench snapshots each daemon
    # before/after the window so the counters exclude prefill noise.
    local info_flag=()
    if [ -n "$HEALTH_ENDPOINT" ]; then
        info_flag=(--info-endpoint "$HEALTH_ENDPOINT")
        for endpoint in "${SHARD_HEALTH_ENDPOINTS[@]}"; do
            info_flag+=(--info-endpoint "$endpoint")
        done
    fi

    # Run the bench; capture stdout+stderr so the awk parser below
    # can scrape latency rows. Non-zero exit dumps the captured file
    # to stderr — never let a bench failure silently rm the only
    # evidence via the cleanup trap.
    # Always capture pprof during the bench window. Files land in
    # $WORKDIR and are dumped alongside daemon logs on non-zero exit.
    # Set KEEP_WORKDIR=1 to retain them for post-mortem analysis.
    local profile_flags=()
    profile_flags+=(--cpu-profile "$WORKDIR/bench-$mode-$conc.cpu.pprof")
    profile_flags+=(--heap-profile "$WORKDIR/bench-$mode-$conc.heap.pprof")
    # runtime/trace is optional — enabled when PROFILE_TRACE=1.
    if [ -n "${PROFILE_TRACE:-}" ]; then
        profile_flags+=(--trace "$WORKDIR/bench-$mode-$conc.trace")
    fi

    # Capture bench-TARGET CPU profile in the background via its
    # pprof_listen endpoint. Duration matches DURATION so the window
    # aligns with the bench measurement window. Delayed start (prefill
    # + observable warm verification usually take a few seconds — pprof pulling
    # for DURATION will cover the bench window plus a bit of tail).
    local target_pprof_pid=""
    if [ -n "${PPROF_ENDPOINT:-}" ]; then
        local secs=${DURATION%s}
        ( sleep 2 && curl -sS -o "$WORKDIR/target-$mode-$conc.cpu.pprof" \
            "http://$PPROF_ENDPOINT/debug/pprof/profile?seconds=$secs" ) &
        target_pprof_pid=$!
    fi

    local rc=0
    local workload_flags=(
        --access "$ACCESS"
        --zipf-s "$ZIPF_S"
        --timeout "$TIMEOUT"
        --cold-prefill "$COLD_PREFILL"
        --miss-ratio "$MISS_RATIO"
        --key-salt "$key_salt"
    )
    if [ -n "$CLIENT_CORES" ]; then
        taskset -c "$CLIENT_CORES" "$BIN/cache-ctl" bench \
            --endpoint "$BENCH_ENDPOINT" \
            "${prefill_flag[@]}" \
            "${info_flag[@]}" \
            "${profile_flags[@]}" \
            "${workload_flags[@]}" \
            --concurrency "$conc" --duration "$DURATION" \
            --value-size "$VALUE_SIZE" --mode "$mode" "$@" >"$out" 2>&1 || rc=$?
    else
        "$BIN/cache-ctl" bench \
            --endpoint "$BENCH_ENDPOINT" \
            "${prefill_flag[@]}" \
            "${info_flag[@]}" \
            "${profile_flags[@]}" \
            "${workload_flags[@]}" \
            --concurrency "$conc" --duration "$DURATION" \
            --value-size "$VALUE_SIZE" --mode "$mode" "$@" >"$out" 2>&1 || rc=$?
    fi
    if [ -n "$target_pprof_pid" ]; then
        wait "$target_pprof_pid" 2>/dev/null || true
        # Also grab a heap snapshot immediately after the bench finishes.
        curl -sS -o "$WORKDIR/target-$mode-$conc.heap.pprof" \
            "http://$PPROF_ENDPOINT/debug/pprof/heap" 2>/dev/null || true
    fi

    if [ "$rc" -ne 0 ]; then
        echo "ERROR: cache-ctl bench mode=$mode c=$conc exited $rc" >&2
        echo "--- $out ---" >&2
        cat "$out" >&2
        return "$rc"
    fi

    awk -v mode="$mode" -v conc="$conc" '
        /^  ops:/        { ops=$2 }
        /^  throughput:/ { thrpt=$2 }
        /^  bandwidth:/  { bw=$2 }
        /^    p50:/      { p50=$2 }
        /^    p99:/      { p99=$2 }
        /^    p99\.9:/   { p999=$2 }
        END {
            printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
                   mode, conc, ops, thrpt, bw, p50, p99, p999
        }' "$out" >> "$RAW"

    # Surface every bench-window counter section printed by cache-ctl bench
    # (only present when at least one --info-endpoint was supplied).
    awk '/^  ── bench-window counters ──$/,0' "$out" >&2
}

# ── Runs ──────────────────────────────────────────────────────────────────
# Every owned round starts with empty measured and origin state. Embedded
# stores are recreated; dedicated Redis endpoints are FLUSHDB'd only after the
# caller explicitly opts in. A run-unique key salt also prevents accidental
# reuse when external mode runs its single operator-reset point.
#
# In non-local scenarios we always run GET (the bench target may be a
# tiered instance that rejects writes). PUT rounds are skipped unless
# the scenario is local or explicitly single-endpoint.
if [ "$PREFILL_ENDPOINT" != "$BENCH_ENDPOINT" ]; then
    # Split prefill/bench — only GET makes sense.
    for c in $GET_CONCS; do
        run_bench get "$c" --prefill "$PREFILL"
    done
else
    for c in $GET_CONCS; do
        run_bench get "$c" --prefill "$PREFILL"
    done
    for c in $PUT_CONCS; do
        run_bench put "$c"
    done
    for c in $MIXED_CONCS; do
        run_bench mixed "$c" --prefill "$PREFILL"
    done
fi

# ── Report ────────────────────────────────────────────────────────────────
# Daemon counter hit rates are printed per-round by cache-ctl bench itself
# (see run_bench above) so they exclude prefill/warm noise. No post-loop
# snapshot here.
SCOUNT=$(count_cores "$SERVER_CORES")
CCOUNT=$(count_cores "$CLIENT_CORES")
REPORT_BACKEND="$BENCH_BACKEND"
if [ -n "$BENCH_EXTERNAL_ENDPOINT" ] && [ -z "$BENCH_BACKEND_EXPLICIT" ]; then
    REPORT_BACKEND="external/unknown"
fi
REPORT_BACKEND_ENDPOINT="$REDIS_ENDPOINT"
if [ "$BENCH_SCENARIO" = tiered-shard-l2 ]; then
    REPORT_BACKEND_ENDPOINT="$REDIS_SHARD_ENDPOINTS"
fi
REPORT_REDIS_GET_POOL="$REDIS_GET_POOL"
REPORT_REDIS_SET_POOL="$REDIS_SET_POOL"
REPORT_REDIS_TIMEOUT="$REDIS_TIMEOUT"
if [ -n "$BENCH_EXTERNAL_ENDPOINT" ]; then
    if [ "$BENCH_SCENARIO" = tiered-shard-l2 ]; then
        if [ -z "$REDIS_SHARD_ENDPOINTS_EXPLICIT" ] || [ -z "$REDIS_SHARD_ENDPOINTS" ]; then
            REPORT_BACKEND_ENDPOINT="unknown"
        fi
    elif [ -z "$REDIS_ENDPOINT_EXPLICIT" ] || [ -z "$REDIS_ENDPOINT" ]; then
        REPORT_BACKEND_ENDPOINT="unknown"
    fi
    if [ -z "$REDIS_GET_POOL_EXPLICIT" ] || [ -z "$REDIS_GET_POOL" ]; then REPORT_REDIS_GET_POOL="unknown"; fi
    if [ -z "$REDIS_SET_POOL_EXPLICIT" ] || [ -z "$REDIS_SET_POOL" ]; then REPORT_REDIS_SET_POOL="unknown"; fi
    if [ -z "$REDIS_TIMEOUT_EXPLICIT" ] || [ -z "$REDIS_TIMEOUT" ]; then REPORT_REDIS_TIMEOUT="unknown"; fi
fi
REPORT_RESET="recreated per point"
if [ -n "$BENCH_EXTERNAL_ENDPOINT" ]; then
    REPORT_RESET="operator-owned; one point only"
fi

awk -v scores="${SERVER_CORES:-unbound}" -v ccores="${CLIENT_CORES:-unbound}" \
    -v backend="$REPORT_BACKEND" -v backend_ep="$REPORT_BACKEND_ENDPOINT" \
    -v bcores="${BACKEND_CORES:-external/unbound}" \
    -v redis_get_pool="$REPORT_REDIS_GET_POOL" -v redis_set_pool="$REPORT_REDIS_SET_POOL" \
    -v redis_timeout="$REPORT_REDIS_TIMEOUT" \
    -v scount="$SCOUNT" -v ccount="$CCOUNT" \
    -v valsz="$VALUE_SIZE" -v duration="$DURATION" -v prefill="$PREFILL" \
    -v access="$ACCESS" -v zipf_s="$ZIPF_S" -v cold_prefill="$COLD_PREFILL" \
    -v miss_ratio="$MISS_RATIO" -v timeout="$TIMEOUT" \
    -v state_reset="$REPORT_RESET" \
    -v scenario="$BENCH_SCENARIO" -v bench_ep="$BENCH_ENDPOINT" -v prefill_ep="$PREFILL_ENDPOINT" '
function commify(n,   s, out, len) {
    s = sprintf("%d", n)
    len = length(s)
    out = ""
    while (len > 3) {
        out = "," substr(s, len-2, 3) out
        len -= 3
    }
    return substr(s, 1, len) out
}
function fmt_lat(us,   ms) {
    if (us + 0 < 1000) return sprintf("%d us", us)
    ms = us / 1000.0
    if (ms < 10)  return sprintf("%.2f ms", ms)
    if (ms < 100) return sprintf("%.1f ms", ms)
    return sprintf("%.0f ms", ms)
}
function fmt_size(b) {
    if (b >= 1048576 && b % 1048576 == 0) return (b/1048576) " MiB"
    if (b >= 1024    && b % 1024    == 0) return (b/1024)    " KiB"
    return b " B"
}
function fmt_cores(spec, n) {
    if (spec == "unbound") return "unbound"
    if (n == 1) return spec " (1 core)"
    return spec " (" n " cores)"
}
function emit_section(label, n,   i, HDR, ROW) {
    HDR = "  %4s   %13s   %12s   %9s   %9s   %9s\n"
    ROW = "  %4d   %8s op/s   %6d MiB/s   %9s   %9s   %9s\n"
    printf "  ── %s ──\n", label
    printf HDR, "conc", "throughput", "bandwidth", "p50", "p99", "p99.9"
    printf HDR, "----", "-------------", "------------", "---------", "---------", "---------"
    for (i = 0; i < n; i++) {
        if (label == "GET") {
            printf ROW, g_conc[i], commify(g_thrpt[i]), g_bw[i]+0,
                   fmt_lat(g_p50[i]), fmt_lat(g_p99[i]), fmt_lat(g_p999[i])
        } else if (label == "PUT") {
            printf ROW, p_conc[i], commify(p_thrpt[i]), p_bw[i]+0,
                   fmt_lat(p_p50[i]), fmt_lat(p_p99[i]), fmt_lat(p_p999[i])
        } else {
            printf ROW, m_conc[i], commify(m_thrpt[i]), m_bw[i]+0,
                   fmt_lat(m_p50[i]), fmt_lat(m_p99[i]), fmt_lat(m_p999[i])
        }
    }
    print ""
}
BEGIN {
    FS = "\t"
    ng = 0; np = 0; nm = 0
}
{
    mode = $1; conc = $2; ops = $3; thrpt = $4; bw = $5
    p50 = $6; p99 = $7; p999 = $8
    if (mode == "get") {
        g_conc[ng] = conc; g_thrpt[ng] = thrpt; g_bw[ng] = bw
        g_p50[ng] = p50; g_p99[ng] = p99; g_p999[ng] = p999
        ng++
    } else if (mode == "put") {
        p_conc[np] = conc; p_thrpt[np] = thrpt; p_bw[np] = bw
        p_p50[np] = p50; p_p99[np] = p99; p_p999[np] = p999
        np++
    } else if (mode == "mixed") {
        m_conc[nm] = conc; m_thrpt[nm] = thrpt; m_bw[nm] = bw
        m_p50[nm] = p50; m_p99[nm] = p99; m_p999[nm] = p999
        nm++
    }
}
END {
    print ""
    print "═══════════════════════════════════════════════════════════════════════════"
    print "                          Cache Bench Report"
    print "═══════════════════════════════════════════════════════════════════════════"
    printf "  Scenario     : %s\n", scenario
    printf "  Backend      : %s\n", backend
    if (backend == "redis" && backend_ep != "") printf "  Redis EP(s)  : %s\n", backend_ep
    printf "  Bench EP     : %s\n", bench_ep
    if (prefill_ep != bench_ep) {
        printf "  Prefill EP   : %s\n", prefill_ep
    }
    printf "  Server cores : %s\n", fmt_cores(scores, scount)
    if (backend == "redis" || backend == "external/unknown") {
        printf "  Backend cores: %s\n", bcores
    }
    if (backend == "redis") {
        printf "  Redis pools  : get=%s set=%s timeout=%s\n", redis_get_pool, redis_set_pool, redis_timeout
    }
    printf "  Client cores : %s\n", fmt_cores(ccores, ccount)
    printf "  Value size   : %s\n", fmt_size(valsz+0)
    printf "  Prefill      : %s warm / %s cold keys\n", prefill, cold_prefill
    printf "  Access       : %s (zipf-s=%s)\n", access, zipf_s
    printf "  Miss ratio   : %s\n", miss_ratio
    printf "  State reset  : %s\n", state_reset
    printf "  Op timeout   : %s\n", timeout
    printf "  Duration     : %s per run\n", duration
    print ""
    if (ng > 0) emit_section("GET", ng)
    if (np > 0) emit_section("PUT", np)
    if (nm > 0) emit_section("MIXED", nm)
}
' "$RAW"
