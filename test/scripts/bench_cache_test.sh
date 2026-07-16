#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TMP="$(mktemp -d)"
cleanup() {
    rm -rf "$TMP"
}
trap cleanup EXIT

fail() {
    echo "bench-cache-test: $*" >&2
    exit 1
}

mkdir -p "$TMP/bin"
cat >"$TMP/bin/taskset" <<'EOF'
#!/bin/bash
set -euo pipefail
[ "$1" = -c ]
shift 2
exec "$@"
EOF
cat >"$TMP/bin/cache-ctl" <<'EOF'
#!/bin/bash
set -euo pipefail
[ "$1" = bench ]
shift
mode=""
conc=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --mode) mode=$2; shift 2 ;;
        --concurrency) conc=$2; shift 2 ;;
        *) shift ;;
    esac
done
printf '%s\t%s\n' "$mode" "$conc" >>"$MOCK_BENCH_LOG"
cat <<'REPORT'
  ops: 10
  throughput: 2 op/s
  bandwidth: 1 MiB/s
    p50: 10 us
    p99: 20 us
    p99.9: 30 us
REPORT
EOF
chmod +x "$TMP/bin/taskset" "$TMP/bin/cache-ctl"

default_log="$TMP/default.log"
default_report="$TMP/default.report"
env PATH="$TMP/bin:$PATH" BIN="$TMP/bin" MOCK_BENCH_LOG="$default_log" \
    BENCH_EXTERNAL_ENDPOINT=127.0.0.1:7700 \
    bash "$SCRIPT_DIR/bench_cache.sh" >"$default_report" 2>"$TMP/default.stderr"
[ "$(wc -l <"$default_log")" -eq 1 ] || fail "external default did not run exactly one point"
grep -qx $'get\t1' "$default_log" || fail "external default was not GET concurrency 1"

pool_report="$TMP/pool.report"
env PATH="$TMP/bin:$PATH" BIN="$TMP/bin" MOCK_BENCH_LOG="$TMP/pool.log" \
    BENCH_EXTERNAL_ENDPOINT=127.0.0.1:7700 BENCH_BACKEND=redis \
    REDIS_GET_POOL=0 REDIS_SET_POOL=0 GET_CONCS=4 \
    bash "$SCRIPT_DIR/bench_cache.sh" >"$pool_report" 2>"$TMP/pool.stderr"
grep -q 'Redis pools  : get=32 set=8 ' "$pool_report" \
    || fail "zero Redis pool sizes were not reported as their effective defaults"
grep -qx $'get\t4' "$TMP/pool.log" || fail "explicit external GET point changed"

if env PATH="$TMP/bin:$PATH" BIN="$TMP/bin" MOCK_BENCH_LOG="$TMP/multiple.log" \
    BENCH_EXTERNAL_ENDPOINT=127.0.0.1:7700 GET_CONCS=1 PUT_CONCS=1 \
    bash "$SCRIPT_DIR/bench_cache.sh" >/dev/null 2>"$TMP/multiple.stderr"; then
    fail "external mode accepted multiple points"
fi
grep -q 'external mode requires exactly one point' "$TMP/multiple.stderr" \
    || fail "external multi-point rejection was not diagnostic"

echo "bench-cache-test: PASS"
