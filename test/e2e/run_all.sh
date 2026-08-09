#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${BIN:?BIN must point to the assembled platform binary directory}"
export BIN

run_case() {
    local script="$1"
    echo
    echo "========================================="
    echo "  accelerator/$script"
    echo "========================================="
    bash "$SCRIPT_DIR/$script"
}

run_case port_lease_test.sh
run_case e2e_cache.sh
run_case e2e_store_cache_listen.sh
run_case e2e_cluster_rolling.sh
run_case e2e_manifest.sh

if [ "${OBS_E2E:-0}" = "1" ]; then
    run_case e2e_obs.sh
else
    echo "==> accelerator/e2e_obs.sh excluded: set OBS_E2E=1 for the credentialed OBS suite"
fi
