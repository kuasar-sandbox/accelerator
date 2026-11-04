#!/usr/bin/env bash

set -euo pipefail
umask 022

NAME=accelerator
TOOL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="${RELEASE_SOURCE_ROOT:-$TOOL_ROOT}"
[ -d "$ROOT" ] && [ ! -L "$ROOT" ] || { echo "release: invalid selected source root" >&2; exit 1; }
ROOT="$(cd "$ROOT" && pwd -P)"
WORK="$(mktemp -d)"
trap 'chmod -R u+w "$WORK"; rm -rf "$WORK"' EXIT
# shellcheck source=scripts/release-materials.sh
source "${TOOL_ROOT:-$ROOT}/scripts/release-materials.sh"
# shellcheck source=scripts/release-native-materials.sh
source "${TOOL_ROOT:-$ROOT}/scripts/release-native-materials.sh"

fail() {
  echo "release: $*" >&2
  exit 1
}

validate_version() {
  [[ "$1" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-preview\.[0-9]{8}(\.[1-9][0-9]*)?)?$ ]] \
    || fail "version must match vX.Y.Z or vX.Y.Z-preview.YYYYMMDD[.N]"
}

normalize_arch() {
  case "$1" in
    amd64|x86_64) printf 'x86_64\n' ;;
    arm64|aarch64) printf 'aarch64\n' ;;
    *) fail "unsupported release architecture: $1" ;;
  esac
}

archive_name() {
  local version="$1" arch
  validate_version "$version"
  arch="$(normalize_arch "$2")"
  printf '%s-%s-linux-%s.tar.gz\n' "$NAME" "$version" "$arch"
}

copy_file() {
  local source="$1" destination="$2"
  [ -f "$ROOT/$source" ] || fail "missing release input: $ROOT/$source"
  mkdir -p "$(dirname "$STAGE/$destination")"
  install -m 0644 "$ROOT/$source" "$STAGE/$destination"
}

copy_executable() {
  local source="$1" destination="$2"
  [ -x "$source" ] || fail "missing executable release input: $source"
  mkdir -p "$(dirname "$STAGE/$destination")"
  install -m 0755 "$source" "$STAGE/$destination"
}

copy_root_executable() {
  local source="$1" destination="$2"
  local selected="$ROOT/$source"
  [ -x "$selected" ] || fail "missing executable release input: $source"
  mkdir -p "$(dirname "$STAGE/$destination")"
  install -m 0755 "$selected" "$STAGE/$destination"
}

# Inspect headers without executing target payloads on the build host.
check_target_binary() {
  local file="$1" machine
  case "$2" in
    x86_64) machine='Advanced Micro Devices X86-64' ;;
    aarch64) machine='AArch64' ;;
    *) fail "invalid target: $2" ;;
  esac
  LC_ALL=C readelf -h "$file" | awk -F: -v machine="$machine" '
    { gsub(/^[ \t]+|[ \t]+$/, "", $1); gsub(/^[ \t]+|[ \t]+$/, "", $2) }
    $1 == "Class" { class++; if ($2 != "ELF64") bad=1 }
    $1 == "Data" { data++; if ($2 != "2\047s complement, little endian") bad=1 }
    $1 == "Machine" { arch++; if ($2 != machine) bad=1 }
    END { exit bad || class != 1 || data != 1 || arch != 1 }
  ' || fail "${3:-payload} has the wrong ELF target ($2): $file"
}

check_go_binary() {
  local file="$1" info expected
  expected="github.com/kuasar-sandbox/accelerator/cmd/$(basename "$1")"
  local target_arch="$2" go_arch
  case "$target_arch" in x86_64) go_arch=amd64 ;; aarch64) go_arch=arm64 ;; *) fail "invalid target: $target_arch" ;; esac
  info="$(go version -m "$file" 2>/dev/null)" \
    || fail "Go build info is missing from $file"
  awk -F '\t' -v expected="$expected" '
    $2 == "path" { paths++; if ($3 != expected) bad=1 }
    $2 == "mod" { modules++; if ($3 != "github.com/kuasar-sandbox/accelerator") bad=1 }
    END { exit bad || paths != 1 || modules != 1 }
  ' <<< "$info" || fail "Go release payload must have its expected main package: $file"
  awk -F '\t' -v expected_arch="$go_arch" '
    $2 == "build" && $3 ~ /^GOOS=/ { os++; if ($3 != "GOOS=linux") bad=1 }
    $2 == "build" && $3 ~ /^GOARCH=/ { arch++; if ($3 != "GOARCH=" expected_arch) bad=1 }
    END { exit bad || os != 1 || arch != 1 }
  ' <<< "$info" || fail "Go release payload must target linux/$go_arch: $file"
  check_target_binary "$file" "$target_arch"
}

validate_archive_paths() {
  local archive="$1"
  if [ "${RELEASE_ARCHIVE_VALIDATOR+x}" = x ]; then
    [ -x "$RELEASE_ARCHIVE_VALIDATOR" ] || fail "trusted release archive validator is not executable"
    "$RELEASE_ARCHIVE_VALIDATOR" "$archive" \
      || fail "$archive contains an unsafe type, mode or ownership, or violates the exact entry contract"
    return
  fi
  # This standard-library-only host parser is independent of the product module.
  GO111MODULE=off GOENV=off GOFLAGS='' GOWORK=off GOTOOLCHAIN=local GOOS='' GOARCH='' \
    GOAMD64=v1 CGO_ENABLED=0 GOEXPERIMENT='' go run "${TOOL_ROOT:-$ROOT}/scripts/release-archive-validator.go" "$archive" \
    || fail "$archive contains an unsafe type, mode or ownership, or violates the exact entry contract"
}


require_rocksdb_payload() {
  if go version -m "$1" | awk -F '\t' '
    $2 == "build" && $3 ~ /^-tags=/ {
      value=substr($3, 7)
      gsub(/"/, "", value)
      count=split(value, tags, /[, ]+/)
      for (i=1; i <= count; i++) if (tags[i] == "no_rocksdb") found=1
    }
    END { exit !found }
  '; then
    fail "official accelerator release must include RocksDB support"
  fi
}

validate_bundle() {
  [ "$#" -eq 3 ] || fail "usage: release.sh validate <version> <arch> <bundle-dir>"
  local version="$1" arch archive bundle="$3"
  arch="$(normalize_arch "$2")"
  archive="$(archive_name "$version" "$arch")"
  [ -s "$bundle/release-notes.md" ] || fail "release-notes.md is missing"
  [ -d "$bundle/assets" ] || fail "assets directory is missing"

  local expected="$WORK/expected-assets" actual="$WORK/actual-assets"
  printf '%s\n' "$archive" | LC_ALL=C sort > "$expected"
  find "$bundle/assets" -mindepth 1 -maxdepth 1 -printf '%f\n' | LC_ALL=C sort > "$actual"
  cmp -s "$expected" "$actual" \
    || { diff -u "$expected" "$actual" >&2 || true; fail "bundle contains an unexpected asset set"; }
  [ -s "$bundle/assets/$archive" ] && [ ! -L "$bundle/assets/$archive" ] \
    || fail "release archive must be a nonempty regular file"


  validate_archive_paths "$bundle/assets/$archive"
  local extract="$WORK/extract"
  rm -rf "$extract"
  mkdir -p "$extract"
  tar -xzf "$bundle/assets/$archive" -C "$extract"
  local file
  for file in manifest-ctl store-ctl cache-ctl; do
    check_go_binary "$extract/bin/$file" "$arch"
  done
  require_rocksdb_payload "$extract/bin/cache-ctl"
  release_materials_require_rocksdb_notices "$extract"
  release_materials_validate "$extract" "$NAME"
  for file in libstdc++.a libgcc.a; do
    release_materials_require_license "$extract" "$NAME" "system/$file"
  done
  for file in manifest-ctl store-ctl cache-ctl; do
    release_materials_validate_go "$extract" "$NAME" "bin/$file"
  done
  for file in manifest-ctl store-ctl cache-ctl; do
    [ -x "$extract/bin/$file" ] || fail "$archive is missing executable bin/$file"
    check_go_binary "$extract/bin/$file" "$arch"
  done
  for file in test/scripts/bench_cache.sh test/scripts/bench_cache_remote.sh \
    test/scripts/dedup_report.sh test/scripts/procmon.sh \
    test/scripts/proc_analyze.py; do
    [ -f "$extract/$file" ] || fail "$archive is missing $file"
  done
}

package_release() {
  [ "$#" -eq 3 ] || fail "usage: release.sh package <version> <arch> <output-dir>"
  local version="$1" arch output="$3" archive epoch bin_dir rocksdb_source project_sha
  arch="$(normalize_arch "$2")"
  archive="$(archive_name "$version" "$arch")"
  if [ -z "$output" ] || [ "$output" = / ] || [ "$output" = . ]; then
    fail "unsafe output directory: $output"
  fi
  [ ! -e "$output" ] || fail "output already exists: $output"
  epoch="${SOURCE_DATE_EPOCH:-0}"
  [[ "$epoch" =~ ^[0-9]+$ ]] || fail "SOURCE_DATE_EPOCH must be an integer"

  STAGE="$WORK/stage"
  rm -rf "$STAGE"
  mkdir -p "$STAGE"
  project_sha="$(release_materials_resolve_git_source "$ROOT" "" accelerator)"
  bin_dir="${RELEASE_BIN_DIR:-$ROOT/bin/$arch}"
  copy_executable "$bin_dir/manifest-ctl" bin/manifest-ctl
  copy_executable "$bin_dir/store-ctl" bin/store-ctl
  copy_executable "$bin_dir/cache-ctl" bin/cache-ctl
  check_go_binary "$STAGE/bin/manifest-ctl" "$arch"
  check_go_binary "$STAGE/bin/store-ctl" "$arch"
  check_go_binary "$STAGE/bin/cache-ctl" "$arch"
  require_rocksdb_payload "$STAGE/bin/cache-ctl"
  copy_root_executable test/scripts/bench_cache.sh test/scripts/bench_cache.sh
  copy_root_executable test/scripts/bench_cache_remote.sh test/scripts/bench_cache_remote.sh
  copy_root_executable test/scripts/dedup_report.sh test/scripts/dedup_report.sh
  copy_root_executable test/scripts/procmon.sh test/scripts/procmon.sh
  copy_root_executable test/scripts/proc_analyze.py test/scripts/proc_analyze.py

  rocksdb_source="${RELEASE_ROCKSDB_SOURCE_DIR:-$ROOT/build/src/rocksdb}"
  local project_version
  project_version="$(release_materials_git_version "$ROOT" "$version" "$project_sha")"
  release_materials_require_go_revision "$STAGE/bin/manifest-ctl" "$project_sha"
  release_materials_require_go_revision "$STAGE/bin/store-ctl" "$project_sha"
  release_materials_require_go_revision "$STAGE/bin/cache-ctl" "$project_sha"
  release_materials_init "$STAGE" "$WORK/materials" "$NAME"
  release_materials_copy_licenses "$ROOT" project
  release_materials_copy_licenses "$rocksdb_source" rocksdb
  release_materials_record_source 'bin/*,test/scripts/*' accelerator "$project_version" \
    "https://github.com/kuasar-sandbox/accelerator/commit/$project_sha" \
    "git:$project_sha" project
  release_materials_record_source bin/cache-ctl rocksdb v9.7.4 \
    'https://github.com/facebook/rocksdb/archive/refs/tags/v9.7.4.tar.gz' \
    'sha256-tree:1341893a5951347a7f658151c10f0b15e0ddd67c28b3804fdfed9a4a7736f52b' rocksdb
  local rocks_library="$ROOT/build/$arch/rocksdb/lib/librocksdb.a"
  release_native_cache_inputs "$ROOT/build/$arch/cache-ctl.map" "$rocks_library" "$ROOT/build/$arch"
  release_materials_record_source bin/cache-ctl rocksdb-static-library v9.7.4 \
    'https://github.com/facebook/rocksdb/archive/refs/tags/v9.7.4.tar.gz' \
    "sha256:$(sha256sum "$rocks_library" | awk '{print $1}')" rocksdb
  release_materials_add_go_binary "$STAGE/bin/manifest-ctl" bin/manifest-ctl
  release_materials_add_go_binary "$STAGE/bin/store-ctl" bin/store-ctl
  release_materials_add_go_binary "$STAGE/bin/cache-ctl" bin/cache-ctl
  release_materials_finish

  mkdir -p "$output/assets"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime="@$epoch" \
    --pax-option=delete=atime,delete=ctime -czf "$output/assets/$archive" -C "$STAGE" .
  cat > "$output/release-notes.md" <<EOF
$NAME $version for Linux.

Download the archive for your architecture, verify its native GitHub asset size and digest, then extract it into a Kuasar Sandbox deployment root. Documentation and E2E suites from this exact tag are collected by the aggregate platform release.
EOF
  validate_bundle "$version" "$arch" "$output"
  echo "==> prepared $output for $version"
}

command -v go >/dev/null || fail "go is required"
case "${1:-}" in
  archive-name) shift; [ "$#" -eq 2 ] || fail "usage: release.sh archive-name <version> <arch>"; archive_name "$@" ;;
  package) shift; package_release "$@" ;;
  validate) shift; validate_bundle "$@" ;;
  *) fail "usage: release.sh <archive-name|package|validate> ..." ;;
esac
