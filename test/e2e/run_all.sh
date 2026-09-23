#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${BIN:?BIN must point to the prepared platform binary directory}"
for binary in cache-ctl store-ctl manifest-ctl flatten-ctl; do
    [ -x "$BIN/$binary" ] || { echo "missing prepared product: $BIN/$binary" >&2; exit 1; }
done

# Transitional owner entry while #172 still selects component run_all.sh.
# Candidate cases consume the final E2E_LIB contract from immutable prepared
# files. No product/helper compilation or source fallback is allowed here.
FRAMEWORK_LIB="${E2E_LIB:-$SCRIPT_DIR/../lib}"
[ -r "$FRAMEWORK_LIB/common.sh" ] || { echo "missing prepared framework helper: common.sh" >&2; exit 1; }
[ -d "$SCRIPT_DIR/lib" ] || { echo "missing prepared accelerator helper directory" >&2; exit 1; }
CASE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/accelerator-e2e-lib.XXXXXX")"
cleanup() { rm -rf "$CASE_ROOT"; }
trap cleanup EXIT INT TERM
install -m 0644 "$FRAMEWORK_LIB/common.sh" "$CASE_ROOT/common.sh"
mkdir -p "$CASE_ROOT/accelerator"
cp -a "$SCRIPT_DIR/lib/." "$CASE_ROOT/accelerator/"

run_case() {
    local case="$1"
    echo "==> accelerator/$case"
    env BIN="$BIN" E2E_LIB="$CASE_ROOT" KUASAR_ARTIFACT_E2E="${KUASAR_ARTIFACT_E2E:-1}" \
        MANIFEST_FIXTURE_DIR="${MANIFEST_FIXTURE_DIR:-}" \
        bash "$SCRIPT_DIR/cases/$case"
}

run_case storage.cache.sh
run_case storage.tiered-cache.sh
run_case storage.cache-membership.sh
run_case storage.store-cache.sh

# image.manifest still carries two historical relative helper references while
# its final direct-case path is being corrected. Stage only that script in the
# private run directory so the candidate product assertions execute against the
# exact prepared products without mutating the prepared workspace.
mkdir -p "$CASE_ROOT/staged/cases" "$CASE_ROOT/staged/lib/accelerator" "$CASE_ROOT/staged/lib"
cp "$SCRIPT_DIR/cases/image.manifest.sh" "$CASE_ROOT/staged/cases/image.manifest.sh"
cp -a "$SCRIPT_DIR/lib/." "$CASE_ROOT/staged/lib/accelerator/"
cp -a "$SCRIPT_DIR/lib/." "$CASE_ROOT/staged/lib/"
echo "==> accelerator/image.manifest.sh"
env BIN="$BIN" E2E_LIB="$CASE_ROOT" SCRIPT_DIR="$CASE_ROOT/staged" \
    KUASAR_ARTIFACT_E2E="${KUASAR_ARTIFACT_E2E:-1}" \
    MANIFEST_FIXTURE_DIR="${MANIFEST_FIXTURE_DIR:-}" \
    bash "$CASE_ROOT/staged/cases/image.manifest.sh"

if [ "${OBS_E2E:-0}" = 1 ]; then
    run_case storage.obs.sh
else
    echo "==> accelerator/storage.obs.sh excluded: credentialed OBS case was not selected"
fi
