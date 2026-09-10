#!/usr/bin/env bash

set -euo pipefail
umask 022

NAME=accelerator
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'chmod -R u+w "$WORK"; rm -rf "$WORK"' EXIT
# shellcheck source=scripts/release-materials.sh
source "$ROOT/scripts/release-materials.sh"
# shellcheck source=scripts/release-native-materials.sh
source "$ROOT/scripts/release-native-materials.sh"

fail() {
  echo "release: $*" >&2
  exit 1
}

validate_version() {
  [[ "$1" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-preview\.[0-9]{8})?$ ]] \
    || fail "version must match vX.Y.Z or vX.Y.Z-preview.YYYYMMDD"
}

normalize_arch() {
  case "$1" in
    amd64|x86_64) printf 'x86_64\n' ;;
    *) fail "unsupported release architecture: $1; current release target is x86_64" ;;
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
  local selected="$WORK/go-build/$NAME/$source"
  [ -x "$selected" ] || fail "missing executable release input: $source"
  mkdir -p "$(dirname "$STAGE/$destination")"
  install -m 0755 "$selected" "$STAGE/$destination"
}

stage_release_go_source() {
  local source="$1" sha="$2" destination="$3"
  [ ! -e "$destination" ] || fail "fresh release checkout already exists"
  mkdir -p "$destination"
  local -a git_env=(env -i PATH="$PATH" GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null)
  "${git_env[@]}" git -C "$destination" init --quiet --template=
  "${git_env[@]}" git -C "$destination" fetch --quiet --depth=1 "$source" "$sha"
  "${git_env[@]}" git -C "$destination" -c advice.detachedHead=false checkout --quiet --detach "$sha"
}

build_release_go_payloads() {
  local arch="$1" proxy="${GOPROXY:-https://proxy.golang.org,direct}" route variable value
  local sumdb="${GOSUMDB:-sum.golang.org}" sumdb_identity sumdb_url sumdb_extra
  local toolchain="${GOTOOLCHAIN:-local}"
  local -a routes build_env
  IFS=',|' read -r -a routes <<< "$proxy"
  for route in "${routes[@]}"; do
    case "$route" in direct|off) continue ;; esac
    [[ "$route" == https://?* && "$route" != *[@?#[:space:]]* ]] \
      || fail "release Go proxy routing must use credential-free HTTPS"
  done
  [[ "$sumdb" != *$'\n'* && "$sumdb" != *$'\r'* ]] \
    || fail "release checksum database routing must be a single line"
  read -r sumdb_identity sumdb_url sumdb_extra <<< "$sumdb"
  [[ "$sumdb_identity" =~ ^[A-Za-z0-9._+/:=-]+$ && -z "$sumdb_extra" ]] \
    || fail "invalid release checksum database identity"
  if [ -n "$sumdb_url" ]; then
    [[ "$sumdb_url" == https://?* && "$sumdb_url" != *[@?#[:space:]]* ]] \
      || fail "release checksum database routing must use credential-free HTTPS"
  fi
  [[ "$toolchain" =~ ^(local|auto|path|go[0-9]+\.[0-9]+(\.[0-9]+|beta[0-9]+|rc[0-9]+)?(\+(auto|path))?)$ ]] \
    || fail "invalid release Go toolchain selection"
  mkdir -p "$WORK/go-home" "$WORK/go-cache" "$WORK/go-mod" "$WORK/native-tmp"
  chmod 0700 "$WORK/go-home" "$WORK/go-cache" "$WORK/go-mod" "$WORK/native-tmp"
  build_env=(env -i PATH="$PATH" HOME="$WORK/go-home" LANG=C
    GOWORK=off GOENV=off GOFLAGS=-mod=readonly GOPROXY="$proxy" GOSUMDB="$sumdb" GOTOOLCHAIN="$toolchain"
    GOCACHE="$WORK/go-cache" GOMODCACHE="$WORK/go-mod" TMPDIR="$WORK/native-tmp"
    GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null)
  for variable in HTTP_PROXY HTTPS_PROXY ALL_PROXY NO_PROXY http_proxy https_proxy all_proxy no_proxy \
    SSL_CERT_FILE SSL_CERT_DIR; do
    value="${!variable:-}"
    [ -n "$value" ] || continue
    case "$variable" in
      HTTP_PROXY|HTTPS_PROXY|ALL_PROXY|http_proxy|https_proxy|all_proxy)
        [[ "$value" != *[@?#[:space:]]* ]] || fail "release build cannot pass an authenticated proxy"
        ;;
    esac
    build_env+=("$variable=$value")
  done
  RELEASE_MATERIALS_GO_ENV="$WORK/go-build-toolchain.json"
  "${build_env[@]}" go -C "$WORK/go-build/$NAME" env -json GOROOT GOVERSION GOHOSTOS GOHOSTARCH \
    > "$RELEASE_MATERIALS_GO_ENV"
  RELEASE_MATERIALS_WORK="$WORK/go-toolchain-before-build" \
    GOMODCACHE="$WORK/go-mod" GOPROXY="$proxy" GOSUMDB="$sumdb" \
    release_materials_verify_build_go "$RELEASE_MATERIALS_GO_ENV"
  "${build_env[@]}" make --no-print-directory -C "$WORK/go-build/$NAME" TARGET_ARCH="$arch" \
    GOLDFLAGS_STATIC="-linkmode=external -extldflags \"-static-libstdc++ -static-libgcc -Wl,-Map,$WORK/cache-ctl.map\"" build
}

check_go_binary() {
  local file="$1" info expected
  expected="github.com/kuasar-sandbox/accelerator/cmd/$(basename "$1")"
  info="$(go version -m "$file" 2>/dev/null)" \
    || fail "Go build info is missing from $file"
  awk -F '\t' -v expected="$expected" '
    $2 == "path" { paths++; if ($3 != expected) bad=1 }
    $2 == "mod" { modules++; if ($3 != "github.com/kuasar-sandbox/accelerator") bad=1 }
    END { exit bad || paths != 1 || modules != 1 }
  ' <<< "$info" || fail "Go release payload must have its expected main package: $file"
  awk -F '\t' '
    $2 == "build" && $3 ~ /^GOOS=/ { os++; if ($3 != "GOOS=linux") bad=1 }
    $2 == "build" && $3 ~ /^GOARCH=/ { arch++; if ($3 != "GOARCH=amd64") bad=1 }
    END { exit bad || os != 1 || arch != 1 }
  ' <<< "$info" || fail "Go release payload must target linux/amd64: $file"
}

validate_copied_source_files() {
  local extract="$1" sha="$2" source
  [[ "$sha" =~ ^[0-9a-f]{40}$ ]] || fail "invalid selected source commit"
  git -C "$ROOT" cat-file -e "$sha^{commit}" 2>/dev/null \
    || fail "selected source commit is unavailable; fetch that exact commit before validation"
  for source in test/scripts/bench_cache.sh test/scripts/bench_cache_remote.sh \
    test/scripts/dedup_report.sh test/scripts/procmon.sh test/scripts/proc_analyze.py; do
    git -C "$ROOT" cat-file blob "$sha:$source" | cmp -s - "$extract/$source" \
      || fail "release helper bytes differ from selected source: $source"
  done
}

validate_archive_paths() {
  local archive="$1"
  go run "$ROOT/scripts/release-archive-validator.go" "$archive" \
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
  printf '%s\n' "$archive" SHA256SUMS | LC_ALL=C sort > "$expected"
  find "$bundle/assets" -mindepth 1 -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort > "$actual"
  cmp -s "$expected" "$actual" \
    || { diff -u "$expected" "$actual" >&2 || true; fail "bundle contains an unexpected asset set"; }
  [ "$(grep -cve '^[[:space:]]*$' "$bundle/assets/SHA256SUMS")" -eq 1 ] \
    || fail "SHA256SUMS must contain exactly one entry"
  local digest listed extra
  read -r digest listed extra < "$bundle/assets/SHA256SUMS"
  listed="${listed#\*}"
  if ! [[ "$digest" =~ ^[0-9a-f]{64}$ ]] \
    || [ "$listed" != "$archive" ] || [ -n "${extra:-}" ]; then
    fail "SHA256SUMS does not describe the expected archive"
  fi
  (cd "$bundle/assets" && sha256sum --quiet -c SHA256SUMS) \
    || fail "SHA256SUMS validation failed"

  validate_archive_paths "$bundle/assets/$archive"
  local extract="$WORK/extract"
  rm -rf "$extract"
  mkdir -p "$extract"
  tar -xzf "$bundle/assets/$archive" -C "$extract"
  local file
  for file in manifest-ctl store-ctl cache-ctl; do
    check_go_binary "$extract/bin/$file"
  done
  local project_sha rocks_digest
  project_sha="$(go version -m "$extract/bin/manifest-ctl" | \
    awk -F '\t' '$2 == "build" && $3 ~ /^vcs.revision=/ {print substr($3, 14)}')"
  validate_copied_source_files "$extract" "$project_sha"
  require_rocksdb_payload "$extract/bin/cache-ctl"
  release_materials_validate "$extract" "$NAME"
  release_materials_require_project_source "$extract" "$NAME" 'bin/*,test/scripts/*' "$version" \
    bin/manifest-ctl bin/store-ctl bin/cache-ctl
  release_materials_require_source "$extract" "$NAME" 'bin/cache-ctl' 'rocksdb' "v9.7.4" \
    'https://github.com/facebook/rocksdb/archive/refs/tags/v9.7.4.tar.gz' \
    'sha256-tree:1341893a5951347a7f658151c10f0b15e0ddd67c28b3804fdfed9a4a7736f52b'
  rocks_digest="$(awk -F '\t' '$1 == "bin/cache-ctl" && $2 == "rocksdb-static-library" {print $5}' \
    "$extract/share/sources/$NAME/SOURCES.tsv")"
  [[ "$rocks_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "missing or invalid RocksDB static-library digest"
  release_materials_require_source "$extract" "$NAME" bin/cache-ctl rocksdb-static-library v9.7.4 \
    'https://github.com/facebook/rocksdb/archive/refs/tags/v9.7.4.tar.gz' "$rocks_digest"
  for file in libstdc++.a libgcc.a; do
    release_materials_require_source "$extract" "$NAME" bin/cache-ctl "system:$file" ""
  done
  release_materials_require_go "$extract" "$NAME" 'bin/manifest-ctl'
  release_materials_require_go "$extract" "$NAME" 'bin/store-ctl'
  release_materials_require_go "$extract" "$NAME" 'bin/cache-ctl'
  for file in manifest-ctl store-ctl cache-ctl; do
    [ -x "$extract/bin/$file" ] || fail "$archive is missing executable bin/$file"
    check_go_binary "$extract/bin/$file"
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
  [ -z "${RELEASE_BIN_DIR:-}" ] \
    || fail "RELEASE_BIN_DIR is not supported: release payloads are rebuilt from selected sources"
  [ -z "${RELEASE_ROCKSDB_SOURCE_DIR:-}" ] \
    || fail "RELEASE_ROCKSDB_SOURCE_DIR is not supported: RocksDB is built from the pinned source"
  project_sha="$(release_materials_resolve_git_source "$ROOT" "" accelerator)"
  mkdir -p "$WORK/go-build"
  stage_release_go_source "$ROOT" "$project_sha" "$WORK/go-build/$NAME"
  # Only downloaded bytes may be reused; the fresh recipe verifies their
  # normalized source digest before compiling. No extracted/native cache is copied.
  if [ -f "$ROOT/build/tarball/rocksdb-9.7.4.tar.gz" ]; then
    mkdir -p "$WORK/go-build/$NAME/build/tarball"
    install -m 0644 "$ROOT/build/tarball/rocksdb-9.7.4.tar.gz" \
      "$WORK/go-build/$NAME/build/tarball/rocksdb-9.7.4.tar.gz"
  fi
  build_release_go_payloads "$arch"
  bin_dir="$WORK/go-build/$NAME/bin/$arch"
  copy_executable "$bin_dir/manifest-ctl" bin/manifest-ctl
  copy_executable "$bin_dir/store-ctl" bin/store-ctl
  copy_executable "$bin_dir/cache-ctl" bin/cache-ctl
  check_go_binary "$STAGE/bin/manifest-ctl"
  check_go_binary "$STAGE/bin/store-ctl"
  check_go_binary "$STAGE/bin/cache-ctl"
  require_rocksdb_payload "$STAGE/bin/cache-ctl"
  copy_root_executable test/scripts/bench_cache.sh test/scripts/bench_cache.sh
  copy_root_executable test/scripts/bench_cache_remote.sh test/scripts/bench_cache_remote.sh
  copy_root_executable test/scripts/dedup_report.sh test/scripts/dedup_report.sh
  copy_root_executable test/scripts/procmon.sh test/scripts/procmon.sh
  copy_root_executable test/scripts/proc_analyze.py test/scripts/proc_analyze.py

  rocksdb_source="$WORK/go-build/$NAME/build/src/rocksdb"
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
  local rocks_library="$WORK/go-build/$NAME/build/$arch/rocksdb/lib/librocksdb.a"
  release_native_cache_inputs "$WORK/cache-ctl.map" "$rocks_library" "$WORK/native-tmp"
  release_materials_record_source bin/cache-ctl rocksdb-static-library v9.7.4 \
    'https://github.com/facebook/rocksdb/archive/refs/tags/v9.7.4.tar.gz' \
    "sha256:$(sha256sum "$rocks_library" | awk '{print $1}')" rocksdb
  release_materials_add_go_binary "$STAGE/bin/manifest-ctl" bin/manifest-ctl
  release_materials_add_go_binary "$STAGE/bin/store-ctl" bin/store-ctl
  release_materials_add_go_binary "$STAGE/bin/cache-ctl" bin/cache-ctl
  GOMODCACHE="$WORK/go-mod" release_materials_finish

  mkdir -p "$output/assets"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime="@$epoch" \
    --pax-option=delete=atime,delete=ctime -czf "$output/assets/$archive" -C "$STAGE" .
  (cd "$output/assets" && sha256sum "$archive" > SHA256SUMS)
  cat > "$output/release-notes.md" <<EOF
$NAME $version for Linux $arch.

Extract the archive into a Kuasar Sandbox deployment root and verify it with \`SHA256SUMS\`. Documentation and E2E suites from this exact tag are collected by the aggregate platform release.
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
