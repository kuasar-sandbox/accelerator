#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TMPDIR=$(mktemp -d /tmp/acc-port-lease-test-XXXXXX)
E2E_PORT_LEASE_FILE="$TMPDIR/ports"
source "$SCRIPT_DIR/lib/port_lease.sh"
trap 'rm -rf "$TMPDIR"' EXIT

rapid_count=128
concurrent_count=128
count=$((rapid_count + concurrent_count))

for i in $(seq 1 "$rapid_count"); do
    e2e_free_port >"$TMPDIR/port-rapid-$i"
done

pids=()
for i in $(seq 1 "$concurrent_count"); do
    (e2e_free_port >"$TMPDIR/port-concurrent-$i") &
    pids+=("$!")
done
for pid in "${pids[@]}"; do
    wait "$pid"
done

cat "$TMPDIR"/port-* >"$TMPDIR/allocated"
if grep -qvE '^[0-9]+$' "$TMPDIR/allocated"; then
    echo "port lease test: allocator returned a non-numeric value" >&2
    exit 1
fi

allocated=$(wc -l <"$TMPDIR/allocated")
unique=$(sort -n -u "$TMPDIR/allocated" | wc -l)
leased=$(wc -l <"$E2E_PORT_LEASE_FILE")
if [ "$allocated" -ne "$count" ] || [ "$unique" -ne "$count" ] || [ "$leased" -ne "$count" ]; then
    echo "port lease test: allocated=$allocated unique=$unique leased=$leased expected=$count" >&2
    exit 1
fi

echo "port lease test: PASS ($rapid_count rapid + $concurrent_count concurrent unique leases)"
