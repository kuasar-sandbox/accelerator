#!/bin/bash
# e2e-requires: []
set -euo pipefail

source "${E2E_LIB:?E2E_LIB is required}/common.sh"
ACCELERATOR_LIB="$E2E_LIB/accelerator"

# Similarity-based delta compression (docs/delta.md).
# -----------------------------------------------------------
# This file is the end-to-end test for similarity-based delta compression. 
# It stores three synthetic images through two Manifest writers that differ 
# only in delta.enabled, then checks that similar images are stored as deltas, 
# unrelated images are not, and a reader can still reconstruct the original bytes.

# Setup
# -----------------------------------------------------------
# It builds three 24 MiB payloads and packs each as a tarstream image:

# - Image A is incompressible random data. It is the base.
# - Image B is A with a 256-byte patch in every 32 KiB window, about 0.8% of 
#   the bytes changed. Every storage chunk differs from A, so nothing deduplicates, 
#   but every chunk is a delta candidate.
# - Image C is unrelated random data of the same size. It must not be 
#   delta-encoded against anything.

# Two store-ctl sidecars run with the same filesystem backend. One Manifest 
# writer has delta.enabled: true, the other has it false. Chunks are a fixed 
# 256 KiB, so each image is 96 chunks. A third config is a reader with delta 
# disabled, pointed at the delta-on store.

# What each test checks
# -----------------------------------------------------------

# Test 1. The first image is stored on its own. Nothing similar exists yet, 
# so the delta summary reports zero delta chunks, and loading it back matches 
# the original payload hash.

# # Test 2. Image B is stored with delta on. It must produce delta chunks, at 
# least half of its stored chunks must be deltas, and the summary must report 
# bytes saved.

# # Test 3. Image C is stored with delta on and must produce zero delta chunks,
#  because it is not similar to A or B.

# # Test 4. Image B round-trips byte for byte through a delta-on reader and 
# through a reader that has no delta.enabled. A reader that rolled out before 
# writers must still reconstruct a delta-encoded image.

# Test 5. The manifest tools understand the delta table:

# - Image B’s manifest is version 2. Image A’s stays version 1.
# - info reports the same delta-chunk count as the store summary, and a 
#   plausible number of distinct bases.
# - deps lists one delta line per delta chunk. Every base of B is an independent 
#   chunk of A. No base is itself a delta, so the chain depth is exactly one.
# - verify succeeds with zero failures and reports the same delta-chunk count.

# Test 6. store-ctl delta-gc on the delta-on store finds exactly as many delta 
# objects as image B’s delta chunks, and zero orphans. The delta-off store holds 
# no delta objects.

# # Test 7. The writer’s representation index is sealed to a snapshot. After a 
# new process starts, storing image B again yields the same manifest key, 
# stores no new chunks, and reports representation-index hits. Those hits 
# only happen if the snapshot was reloaded. The snapshot must also not contain 
# any base chunk key in the clear.

# # Test 8. With delta off, image B’s store summary has no delta: line and 
# the manifest stays version 1, but the payload still round-trips. The 
# case then times a full load of image B with delta on versus off and 
# prints the ratio. That timing is reported only. The pass/fail 
# contract is correctness, because machines differ.
# -----------------------------------------------------------

BIN="${BIN:?BIN is required}"

die() { echo "  ERROR: $*" >&2; exit 1; }

for tool in python3 openssl stat sha256sum awk grep sed date mktemp mkdir rm cat head sleep dirname env; do
    command -v "$tool" >/dev/null || die "required tool not found: $tool"
done
for binary in store-ctl manifest-ctl flatten-ctl; do
    [ -x "$BIN/$binary" ] || die "required executable not found: $BIN/$binary (set BIN to the assembled platform binaries)"
done
BIN="$(cd "$BIN" && pwd)"
[ -r "$ACCELERATOR_LIB/port_lease.sh" ] || die "required prepared accelerator helper missing: $ACCELERATOR_LIB/port_lease.sh"

TMPDIR=$(mktemp -d "${TMPDIR:-/tmp}/acc-delta-e2e-XXXXXX")

PASS=0
FAIL=0
STORE_PIDS=()

stop_store() {
    local pid=$1 attempt
    kill -TERM "$pid" 2>/dev/null || true
    for ((attempt = 0; attempt < 30; attempt++)); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.1
    done
    if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid" 2>/dev/null || true
    fi
    wait "$pid" 2>/dev/null || true
}

cleanup() {
    local status=$? pid
    trap - EXIT
    trap '' INT TERM
    for pid in "${STORE_PIDS[@]}"; do
        stop_store "$pid"
    done
    rm -rf -- "$TMPDIR" || { [ "$status" -ne 0 ] || status=1; }
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

E2E_PORT_LEASE_FILE="$TMPDIR/ports"
# shellcheck source=/dev/null
source "$ACCELERATOR_LIB/port_lease.sh"
KEY=$(openssl rand -hex 32)

ok() {
    PASS=$((PASS + 1))
    echo "  PASS: $1"
}

fail() {
    FAIL=$((FAIL + 1))
    echo "  FAIL: $1"
}

assert_eq() {
    if [ "$1" = "$2" ]; then
        ok "$3"
    else
        fail "$3 (expected '$1', got '$2')"
    fi
}

# Millisecond wall clock for the start-up timings in test 8.
now_ms() { date +%s%3N; }

# ============================================================
echo ""
echo "=== Prepare similar image payloads ==="
# A is incompressible, so a delta cannot be mistaken for Snappy.
# B is A with a 256-byte patch in every 32 KiB window: every chunk differs
# (nothing dedups) and every chunk is a delta candidate.
# C is the same size and unrelated, so it must not be delta-encoded.
PAYLOAD_MIB=24
python3 - "$TMPDIR" "$PAYLOAD_MIB" <<'PY'
import hashlib, sys
out, mib = sys.argv[1], int(sys.argv[2])
size = mib << 20
a = bytearray(hashlib.shake_256(b"accelerator delta fixture base v1").digest(size))
with open(f"{out}/image-a.bin", "wb") as f:
    f.write(a)
b = bytearray(a)
window, patch = 32 << 10, 256
for i in range(size // window):
    off = i * window + (hashlib.sha256(i.to_bytes(4, "big")).digest()[0] << 7) % (window - patch)
    b[off:off + patch] = hashlib.shake_256(b"patch" + i.to_bytes(4, "big")).digest(patch)
with open(f"{out}/image-b.bin", "wb") as f:
    f.write(b)
with open(f"{out}/image-c.bin", "wb") as f:
    f.write(hashlib.shake_256(b"accelerator delta fixture unrelated v1").digest(size))
PY
for v in a b c; do
    # The store takes tarstream artifacts. No root: ownership is not preserved.
    "$BIN/flatten-ctl" tar stream -f "$TMPDIR/image-$v.tar" "image:$TMPDIR/image-$v.bin"
    [ -s "$TMPDIR/image-$v.tar" ] || die "packaging image-$v failed"
done
HA=$(sha256sum "$TMPDIR/image-a.bin" | awk '{print $1}')
HB=$(sha256sum "$TMPDIR/image-b.bin" | awk '{print $1}')
[ "$HA" != "$HB" ] || die "fixture images A and B are identical"

# SHA-256 of the extracted payload: the bytes a container would see.
payload_hash() {
    rm -f "$1.payload"
    "$BIN/flatten-ctl" tar extract -f "$1" --dense --no-chown "image:$1.payload"
    sha256sum "$1.payload" | awk '{print $1}'
}
ok "fixture: three ${PAYLOAD_MIB} MiB tarstream images (A, similar B, unrelated C)"

# ============================================================
echo ""
echo "=== Spin up two store-ctl sidecars (delta on / delta off writers) ==="
# Sets STORE_PORT. Runs in the main shell so STORE_PIDS reaches the cleanup trap.
start_store() {
    local name=$1 port root
    port=$(e2e_free_port)
    root="$TMPDIR/store-$name"
    cat > "$TMPDIR/store-$name.yaml" <<EOF
listen: 127.0.0.1:$port
backend: fs
fs:
  root: $root
  verify_content_key: true
EOF
    "$BIN/store-ctl" init --config "$TMPDIR/store-$name.yaml" --generation G1
    "$BIN/store-ctl" serve --config "$TMPDIR/store-$name.yaml" >"$TMPDIR/store-$name.log" 2>&1 &
    local pid=$! ready=0 attempt
    STORE_PIDS+=("$pid")
    for ((attempt = 0; attempt < 50; attempt++)); do
        kill -0 "$pid" 2>/dev/null || break
        if (echo >/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then
            ready=1
            break
        fi
        sleep 0.1
    done
    if [ "$ready" -ne 1 ]; then
        echo "  ERROR: store-ctl ($name) did not become ready on 127.0.0.1:$port within 5 seconds" >&2
        cat "$TMPDIR/store-$name.log" >&2
        exit 1
    fi
    echo "  store-ctl $name listen=127.0.0.1:$port root=$root"
    STORE_PORT=$port
}
start_store on
PORT_ON=$STORE_PORT
start_store off
PORT_OFF=$STORE_PORT

# Fixed 256 KiB chunks: 24 MiB / 256 KiB = 96 chunks per image.
# The snapshot path is the only delta setting off the documented default;
# test 7 reads that snapshot back after a new process starts.
write_cfg() {
    local path=$1 port=$2 enabled=$3
    cat > "$path" <<EOF
manifest:
  key: "$KEY"
  verify_content: true
store:
  endpoint: 127.0.0.1:$port
  pool: 4
  timeout: 30s
chunker:
  mode: fixed
  fixed:
    size: 256KiB
crypto:
  chunk: aes
  manifest: aes
delta:
  enabled: $enabled
  index:
    snapshot_path: $TMPDIR/delta-$enabled.snap
    snapshot_interval: "1s"
EOF
}
write_cfg "$TMPDIR/on.yaml" "$PORT_ON" true
write_cfg "$TMPDIR/off.yaml" "$PORT_OFF" false
# Delta disabled, pointed at the delta-on store. Readers roll out before
# writers, so this config must still read a delta-encoded image.
cat > "$TMPDIR/reader-off.yaml" <<EOF
manifest:
  key: "$KEY"
  verify_content: true
store:
  endpoint: 127.0.0.1:$PORT_ON
  pool: 4
  timeout: 30s
chunker:
  mode: fixed
  fixed:
    size: 256KiB
crypto:
  chunk: aes
  manifest: aes
EOF
ON=(--manifest-config "$TMPDIR/on.yaml")
OFF=(--manifest-config "$TMPDIR/off.yaml")
READER_OFF=(--manifest-config "$TMPDIR/reader-off.yaml")

# Pull NAME=<n>, NAME=<size>, and stored=<n> out of a store summary line.
delta_field() { echo "$1" | sed -nE "s/.*[[:space:]]$2=([0-9]+).*/\1/p"; }
delta_size() { echo "$1" | sed -nE "s/.*[[:space:]]$2=([0-9.]+ [KMGT]?i?B).*/\1/p"; }
chunks_stored() { echo "$1" | grep -E '^chunks:' | sed -E 's/.*stored=([0-9]+).*/\1/'; }

# ============================================================
echo ""
echo "=== Test 1: First image is stored independently (nothing to delta against) ==="
# No similar base exists yet, so A is stored as independent chunks.
MKEY_A_ON=$("$BIN/manifest-ctl" store "${ON[@]}" --no-progress "$TMPDIR/image-a.tar" 2>"$TMPDIR/a-on.stderr")
A_DELTA_LINE=$(grep -E '^delta:' "$TMPDIR/a-on.stderr" || true)
echo "  $A_DELTA_LINE"
[ -n "$A_DELTA_LINE" ] || fail "delta-on store summary has no delta: line"
assert_eq "0" "$(delta_field "$A_DELTA_LINE" chunks)" "image A: no delta chunks (no similar base exists)"
"$BIN/manifest-ctl" load "${ON[@]}" --output "$TMPDIR/a-on.rt" --no-progress "$MKEY_A_ON"
assert_eq "$HA" "$(payload_hash "$TMPDIR/a-on.rt")" "image A roundtrip (delta on)"

# ============================================================
echo ""
echo "=== Test 2: Similar image is delta encoded against the first ==="
# Most of B's chunks must be deltas, and the summary must report bytes saved.
MKEY_B_ON=$("$BIN/manifest-ctl" store "${ON[@]}" --no-progress "$TMPDIR/image-b.tar" 2>"$TMPDIR/b-on.stderr")
B_CHUNKS_LINE=$(grep -E '^chunks:' "$TMPDIR/b-on.stderr")
B_DELTA_LINE=$(grep -E '^delta:' "$TMPDIR/b-on.stderr" || true)
echo "  $B_CHUNKS_LINE"
echo "  $B_DELTA_LINE"
B_STORED=$(chunks_stored "$B_CHUNKS_LINE")
B_DELTA=$(delta_field "$B_DELTA_LINE" chunks)
B_SAVED=$(delta_size "$B_DELTA_LINE" saved)
if [ -n "$B_DELTA" ] && [ "$B_DELTA" -gt 0 ]; then
    ok "image B: $B_DELTA of $B_STORED stored chunks are deltas (saved $B_SAVED)"
else
    fail "image B: expected delta chunks > 0 (summary: $B_DELTA_LINE)"
fi
if [ "$B_DELTA" -ge $((B_STORED / 2)) ]; then
    ok "image B: at least half of the chunks were delta encoded"
else
    fail "image B: only $B_DELTA of $B_STORED chunks were delta encoded"
fi
case "$B_SAVED" in
    ""|"0 B"|"0.0 "*) fail "image B: delta saved nothing ('$B_SAVED')" ;;
    *) ok "image B: delta saved $B_SAVED" ;;
esac

# ============================================================
echo ""
echo "=== Test 3: Unrelated image is not delta encoded ==="
# C shares no base with A or B, so it must produce zero delta chunks.
MKEY_C_ON=$("$BIN/manifest-ctl" store "${ON[@]}" --no-progress "$TMPDIR/image-c.tar" 2>"$TMPDIR/c-on.stderr")
C_DELTA_LINE=$(grep -E '^delta:' "$TMPDIR/c-on.stderr" || true)
echo "  $C_DELTA_LINE"
assert_eq "0" "$(delta_field "$C_DELTA_LINE" chunks)" "image C: no delta chunks (no similar base)"

# ============================================================
echo ""
echo "=== Test 4: Delta-encoded image round trips with delta on and with a delta-off reader ==="
# Both readers must reconstruct B byte for byte. The delta-on load is also
# the first start-up sample reported in test 8.
T0=$(now_ms)
"$BIN/manifest-ctl" load "${ON[@]}" --output "$TMPDIR/b-on.rt" --no-progress "$MKEY_B_ON"
LOAD_B_ON_MS=$(( $(now_ms) - T0 ))
assert_eq "$HB" "$(payload_hash "$TMPDIR/b-on.rt")" "image B roundtrip (delta-on reader)"
"$BIN/manifest-ctl" load "${READER_OFF[@]}" --output "$TMPDIR/b-reader-off.rt" --no-progress "$MKEY_B_ON"
assert_eq "$HB" "$(payload_hash "$TMPDIR/b-reader-off.rt")" "image B roundtrip (reader without delta.enabled reads deltas)"

# ============================================================
echo ""
echo "=== Test 5: Version 2 manifest: info, deps and verify understand the delta table ==="
# B is version 2; A, which has no deltas, stays version 1.
"$BIN/manifest-ctl" get-manifest "${ON[@]}" --output "$TMPDIR/b.manifest" "$MKEY_B_ON"
"$BIN/manifest-ctl" info "$TMPDIR/b.manifest" > "$TMPDIR/b-info.txt"
INFO_VERSION=$(grep -E '^version:' "$TMPDIR/b-info.txt" | awk '{print $2}')
INFO_DELTA=$(grep -E '^delta chunks:' "$TMPDIR/b-info.txt" | awk '{print $3}')
INFO_BASES=$(grep -E '^delta bases:' "$TMPDIR/b-info.txt" | awk '{print $3}')
assert_eq "2" "$INFO_VERSION" "info: delta-encoded manifest is version 2"
assert_eq "$B_DELTA" "$INFO_DELTA" "info: delta chunk count matches the store summary"
if [ -n "$INFO_BASES" ] && [ "$INFO_BASES" -gt 0 ] && [ "$INFO_BASES" -le "$B_DELTA" ]; then
    ok "info: $INFO_BASES distinct bases for $B_DELTA delta chunks"
else
    fail "info: implausible base count '$INFO_BASES' for $B_DELTA delta chunks"
fi

"$BIN/manifest-ctl" get-manifest "${ON[@]}" --output "$TMPDIR/a.manifest" "$MKEY_A_ON"
"$BIN/manifest-ctl" info "$TMPDIR/a.manifest" > "$TMPDIR/a-info.txt"
assert_eq "1" "$(grep -E '^version:' "$TMPDIR/a-info.txt" | awk '{print $2}')" "info: manifest without deltas stays version 1"

# Each base of B is an independent chunk of A. A base is never itself a
# delta, so the chain depth is exactly one.
"$BIN/manifest-ctl" deps -quiet "$TMPDIR/a.manifest" > "$TMPDIR/a-deps.txt"
"$BIN/manifest-ctl" deps -quiet "$TMPDIR/b.manifest" > "$TMPDIR/b-deps.txt"
DEPS_DELTA=$(grep -c '^delta ' "$TMPDIR/b-deps.txt" || true)
DEPS_BASES=$(grep -c '^base ' "$TMPDIR/b-deps.txt" || true)
assert_eq "$B_DELTA" "$DEPS_DELTA" "deps: one delta line per delta chunk"
assert_eq "$INFO_BASES" "$DEPS_BASES" "deps: base lines match info's distinct base count"
MISSING_BASES=0
while read -r _ hex _; do
    grep -q "^chunk $hex " "$TMPDIR/a-deps.txt" || MISSING_BASES=$((MISSING_BASES + 1))
done < <(grep '^base ' "$TMPDIR/b-deps.txt")
assert_eq "0" "$MISSING_BASES" "deps: every base of B is an independent chunk of A"
assert_eq "0" "$(grep -c '^delta ' "$TMPDIR/a-deps.txt" || true)" "deps: image A has no delta objects"

OUTPUT=$("$BIN/manifest-ctl" verify "${ON[@]}" --no-progress "$MKEY_B_ON" 2>&1)
echo "  $OUTPUT" | tail -1
if echo "$OUTPUT" | grep -q "failed: 0" && echo "$OUTPUT" | grep -q "(delta: $B_DELTA)"; then
    ok "verify: image B verifies with $B_DELTA delta chunks and 0 failures"
else
    fail "verify: unexpected summary: $OUTPUT"
fi

# ============================================================
echo ""
echo "=== Test 6: Store holds no orphaned delta objects; delta-gc is consistent ==="
# Delta objects in the delta-on store equal B's delta chunks, with zero
# orphans. The delta-off store holds none.
GC_OUT=$("$BIN/store-ctl" delta-gc --config "$TMPDIR/store-on.yaml" --generation G1 2>&1)
echo "$GC_OUT" | sed 's/^/  /'
GC_DELTA_OBJS=$(echo "$GC_OUT" | sed -nE 's/^delta objects:[[:space:]]+([0-9]+).*/\1/p')
GC_ORPHANS=$(echo "$GC_OUT" | sed -nE 's/^orphans:[[:space:]]+([0-9]+).*/\1/p')
assert_eq "$B_DELTA" "$GC_DELTA_OBJS" "delta-gc: delta objects in the store equal image B's delta chunks"
assert_eq "0" "$GC_ORPHANS" "delta-gc: no orphaned delta objects after ingest"
GC_OFF=$("$BIN/store-ctl" delta-gc --config "$TMPDIR/store-off.yaml" --generation G1 2>&1)
assert_eq "0" "$(echo "$GC_OFF" | sed -nE 's/^delta objects:[[:space:]]+([0-9]+).*/\1/p')" "delta-gc: delta-off store holds no delta objects"

# ============================================================
echo ""
echo "=== Test 7: Writer restart reloads the sealed snapshot ==="
# Each manifest-ctl run is a new process. Storing B again stores nothing new
# and reports repHits only if the representation index came back from the
# snapshot. The snapshot must not contain a chunk key in the clear.
SNAP="$TMPDIR/delta-true.snap"
if [ -s "$SNAP" ]; then
    ok "snapshot written at $SNAP ($(stat -c %s "$SNAP") bytes)"
else
    fail "snapshot missing or empty at $SNAP"
fi
A_FIRST_KEY=$(grep '^chunk ' "$TMPDIR/a-deps.txt" | head -1 | awk '{print $2}')
if python3 - "$SNAP" "$A_FIRST_KEY" <<'PY'
import sys
data = open(sys.argv[1], "rb").read()
sys.exit(0 if bytes.fromhex(sys.argv[2]) not in data else 1)
PY
then
    ok "snapshot does not contain a base key in the clear"
else
    fail "snapshot contains a chunk key in the clear"
fi
MKEY_B_AGAIN=$("$BIN/manifest-ctl" store "${ON[@]}" --no-progress "$TMPDIR/image-b.tar" 2>"$TMPDIR/b-again.stderr")
AGAIN_DELTA_LINE=$(grep -E '^delta:' "$TMPDIR/b-again.stderr" || true)
echo "  $AGAIN_DELTA_LINE"
assert_eq "$MKEY_B_ON" "$MKEY_B_AGAIN" "restoring image B again yields the same manifest key"
assert_eq "0" "$(chunks_stored "$(grep -E '^chunks:' "$TMPDIR/b-again.stderr")")" "second store of image B stores no new chunks"
REP_HITS=$(delta_field "$AGAIN_DELTA_LINE" repHits)
if [ -n "$REP_HITS" ] && [ "$REP_HITS" -gt 0 ]; then
    ok "second store of image B had $REP_HITS representation-index hits from the reloaded snapshot"
else
    fail "second store of image B had no representation-index hits (snapshot not reloaded?)"
fi

# ============================================================
echo ""
echo "=== Test 8: Delta-off baseline and start-up latency ==="
# Delta off stores B as a version 1 manifest and prints no delta summary.
# The timings below are the host's open-and-read path. They are reported,
# not asserted; per-chunk latency is covered by fetchbench (docs/delta.md).
"$BIN/manifest-ctl" store "${OFF[@]}" --no-progress "$TMPDIR/image-a.tar" 2>/dev/null >/dev/null
MKEY_B_OFF=$("$BIN/manifest-ctl" store "${OFF[@]}" --no-progress "$TMPDIR/image-b.tar" 2>"$TMPDIR/b-off.stderr")
assert_eq "" "$(grep -E '^delta:' "$TMPDIR/b-off.stderr" || true)" "delta-off writer prints no delta summary"
"$BIN/manifest-ctl" get-manifest "${OFF[@]}" --output "$TMPDIR/b-off.manifest" "$MKEY_B_OFF"
assert_eq "1" "$("$BIN/manifest-ctl" info "$TMPDIR/b-off.manifest" | grep -E '^version:' | awk '{print $2}')" "delta-off manifest is version 1"
T0=$(now_ms)
"$BIN/manifest-ctl" load "${OFF[@]}" --output "$TMPDIR/b-off.rt" --no-progress "$MKEY_B_OFF"
LOAD_B_OFF_MS=$(( $(now_ms) - T0 ))
assert_eq "$HB" "$(payload_hash "$TMPDIR/b-off.rt")" "image B roundtrip (delta off)"

# Best of a few loads, so one cold start does not dominate the report.
ROUNDS=3
best_ms() {
    local cfg=$1 key=$2 best= i t0 t
    for ((i = 0; i < ROUNDS; i++)); do
        rm -f "$TMPDIR/timing.rt"
        t0=$(now_ms)
        "$BIN/manifest-ctl" load --manifest-config "$cfg" --output "$TMPDIR/timing.rt" --no-progress "$key"
        t=$(( $(now_ms) - t0 ))
        if [ -z "$best" ] || [ "$t" -lt "$best" ]; then best=$t; fi
    done
    echo "$best"
}
BEST_ON=$(best_ms "$TMPDIR/on.yaml" "$MKEY_B_ON")
BEST_OFF=$(best_ms "$TMPDIR/off.yaml" "$MKEY_B_OFF")
echo "  start-up (full load of ${PAYLOAD_MIB} MiB image B, $B_DELTA delta chunks): first run on=${LOAD_B_ON_MS}ms off=${LOAD_B_OFF_MS}ms; best of $ROUNDS on=${BEST_ON}ms off=${BEST_OFF}ms"
if [ "$BEST_OFF" -gt 0 ]; then
    echo "  delta-on / delta-off full-load ratio: $(awk -v a="$BEST_ON" -v b="$BEST_OFF" 'BEGIN { printf "%.2f", a / b }')"
fi
ok "start-up latency measured for delta on and off (reported above)"

# ============================================================
echo ""
echo "========================================="
echo "Results: $PASS passed, $FAIL failed"
echo "========================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
