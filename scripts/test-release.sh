#!/usr/bin/env bash

set -euo pipefail

# This suite compiles private synthetic modules; product source gates select
# their own verified workspace outside these standalone packaging fixtures.
export GOWORK=off

# Archive fixtures target x86_64 even when this gate runs on an ARM host.
# Validators run natively; payload fixtures are inspected, never executed.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "test-release: $*" >&2
  exit 1
}

# shellcheck source=scripts/release-materials.sh
source "$ROOT/scripts/release-materials.sh"

bash "$ROOT/scripts/test-release-materials.sh"
PYTHONDONTWRITEBYTECODE=1 python3 "$ROOT/scripts/test-selected-go-materials.py"
PYTHONDONTWRITEBYTECODE=1 python3 "$ROOT/scripts/test-release-go-environment.py"
bash "$ROOT/scripts/test-release-license-traversal.sh"
bash "$ROOT/scripts/test-release-cleanup.sh"
GOWORK=off go test -race "$ROOT/scripts/release-archive-validator.go" "$ROOT/scripts/release-archive-validator_test.go"
bash "$ROOT/deps/test-common.sh"
bash "$ROOT/scripts/test-release-native-materials.sh"
bash "$ROOT/scripts/test-release-rpm-enumeration.sh"
PYTHONDONTWRITEBYTECODE=1 python3 "$ROOT/scripts/test-release-validator-environment.py"

init_fixture_repo() {
  local directory="$1"
  shift
  git -C "$directory" init -q
  git -C "$directory" config --local user.name "Chen Xiaohui"
  git -C "$directory" config --local user.email "graych@gmail.com"
  git -C "$directory" add -- "$@"
  git -C "$directory" commit -q -m "test: create release source fixture"
  git -C "$directory" rev-parse HEAD
}

mkdir -p "$TMP/git-source"
printf 'fixture license\n' > "$TMP/git-source/LICENSE"
fixture_git_sha="$(init_fixture_repo "$TMP/git-source" LICENSE)"
[ "$(release_materials_git_version "$TMP/git-source" v1.2.3 "$fixture_git_sha")" = "git:$fixture_git_sha" ] \
  || fail "untagged source was recorded as a component release"
git -C "$TMP/git-source" tag v1.2.3 "$fixture_git_sha"
[ "$(release_materials_git_version "$TMP/git-source" v1.2.3 "$fixture_git_sha")" = v1.2.3 ] \
  || fail "matching source tag was not retained"
if (release_materials_git_version "$TMP/git-source" v1.2.3 \
  0000000000000000000000000000000000000000 >/dev/null 2>&1); then
  fail "source version resolver accepted a tag for another commit"
fi
[ "$(release_materials_resolve_git_source "$TMP/git-source" "$fixture_git_sha" fixture)" = "$fixture_git_sha" ] \
  || fail "clean source worktree did not resolve to its selected commit"
if (release_materials_resolve_git_source "$TMP/git-source" \
  0000000000000000000000000000000000000000 fixture >/dev/null 2>&1); then
  fail "source resolver accepted a commit that differs from the selected commit"
fi
printf 'untracked source\n' > "$TMP/git-source/untracked.go"
if (release_materials_resolve_git_source "$TMP/git-source" "" fixture >/dev/null 2>&1); then
  fail "source resolver accepted a dirty source worktree"
fi
printf 'LICENSE.generated\n' > "$TMP/git-source/.git/info/exclude"
printf 'ignored material\n' > "$TMP/git-source/LICENSE.generated"
if (
  release_materials_init "$TMP/ignored-material/stage" "$TMP/ignored-material/work" fixture
  release_materials_copy_licenses "$TMP/git-source" project >/dev/null 2>&1
); then
  fail "license collection accepted material absent from the selected commit"
fi
# Package validation checks actual paths, permissions, notices and payloads.
material_root="$TMP/material-validation"
material_unit=validation
mkdir -p "$material_root/share/licenses/$material_unit/project" "$TMP/material-validation-work"
printf 'fixture license\n' > "$material_root/share/licenses/$material_unit/project/LICENSE"
find "$material_root/share" -type d -exec chmod 0755 {} +
find "$material_root/share" -type f -exec chmod 0644 {} +
WORK="$TMP/material-validation-work" release_materials_validate "$material_root" "$material_unit"
for mutation in parent-mode file-mode link fifo empty missing unsafe-path source-record; do
  altered="$TMP/material-$mutation"
  cp -a "$material_root" "$altered"
  license="$altered/share/licenses/$material_unit/project/LICENSE"
  case "$mutation" in
    parent-mode) chmod 0777 "$altered/share" ;;
    file-mode) chmod 0666 "$license" ;;
    link) rm "$license"; ln -s /etc/passwd "$license" ;;
    fifo) rm "$license"; mkfifo "$license" ;;
    empty) : > "$license" ;;
    missing) rm "$license" ;;
    unsafe-path) mv "$license" "${license} unsafe" ;;
    source-record) mkdir -p "$altered/share/sources/$material_unit"; printf 'retired\n' > "$altered/share/sources/$material_unit/SOURCES.tsv" ;;
  esac
  if (WORK="$TMP/material-validation-work" release_materials_validate "$altered" "$material_unit" >/dev/null 2>&1); then
    fail "license validator accepted $mutation"
  fi
done
if (WORK="$TMP/material-validation-work" release_materials_validate_go "$material_root" "$material_unit" bin/missing >/dev/null 2>&1); then
  fail "validator accepted missing Go binary"
fi

bash "$ROOT/scripts/test-preview-line.sh"
bash "$ROOT/scripts/test-delete-preview.sh"

# The component passes every input unchanged to the fixed trusted helper.
mkdir -p "$TMP/source-entry/trusted/platform/release"
cat > "$TMP/source-entry/trusted/platform/release/producer-inputs.py" <<'PYENTRY'
import os, sys
from pathlib import Path
unit = os.environ['ENTRY_UNIT']
prefix = unit + '-' if unit in ('runtime', 'vmlinux') else ''
assert sys.argv[1:] == ['admit', unit, 'release/v1.2.x', '1' * 40,
                       prefix + 'v1.2.3', os.environ['GITHUB_REPOSITORY'], 'producer-source.tar']
if os.environ.get('ENTRY_REJECT'): raise SystemExit(23)
Path('producer-source.tar').write_text('admitted fixture')
PYENTRY
(
  cd "$TMP/source-entry"
  export GITHUB_REPOSITORY=kuasar-sandbox/accelerator ENTRY_UNIT=accelerator
  bash "$ROOT/scripts/validate-release-source.sh" release/v1.2.x 1111111111111111111111111111111111111111 v1.2.3 accelerator
  [ -f producer-source.tar ] || fail "source admission was not preserved"
  rm producer-source.tar
  status=0
  ENTRY_REJECT=1 bash "$ROOT/scripts/validate-release-source.sh" release/v1.2.x 1111111111111111111111111111111111111111 v1.2.3 accelerator || status=$?
  [ "$status" -eq 23 ] || fail "source admission failure was not preserved"
  [ ! -e producer-source.tar ] || fail "rejected admission produced an artifact"
)
rm "$TMP/source-entry/trusted/platform/release/producer-inputs.py"
if (cd "$TMP/source-entry"; bash "$ROOT/scripts/validate-release-source.sh" main 1111111111111111111111111111111111111111 v1.2.3 connector) >/dev/null 2>&1; then
  fail "source validator accepted a missing trusted helper"
fi
bash -n "$ROOT/scripts/delete-preview.sh" "$ROOT/scripts/validate-release-source.sh"
grep -Fqx 'run-name: Release ${{ inputs.version }} @${{ inputs.source_sha }}' \
  "$ROOT/.github/workflows/release.yml" \
  || fail "release run identity does not pin source_sha"
workflow="$ROOT/.github/workflows/release.yml"
for workflow in release.yml delete-preview.yml; do
  [ "$(grep -Fc 'group: component-mutation-${{ github.repository }}-${{ inputs.version }}' \
    "$ROOT/.github/workflows/$workflow")" -eq 1 ] \
    || fail "$workflow does not hold exactly one full-workflow mutation lock"
done
[ ! -e "$ROOT/.github/workflows/reconcile-latest.yml" ] || fail "retired Latest writer remains"
PYTHONDONTWRITEBYTECODE=1 python3 "$ROOT/scripts/test-release-publisher.py"

expected_cases=(
  test/e2e/cases/basic.accelerator-activation.sh
  test/e2e/cases/image.manifest.sh
  test/e2e/cases/storage.cache-membership.sh
  test/e2e/cases/storage.cache-probe.sh
  test/e2e/cases/storage.cache.sh
  test/e2e/cases/storage.obs.sh
  test/e2e/cases/storage.service-drain.sh
  test/e2e/cases/storage.store-cache.sh
  test/e2e/cases/storage.tiered-cache.sh
)
mapfile -t actual_cases < <(git -C "$ROOT" ls-files -- 'test/e2e/cases/*.sh' | sort)
mapfile -t sorted_expected_cases < <(printf '%s\n' "${expected_cases[@]}" | sort)
[ "${#actual_cases[@]}" -eq "${#sorted_expected_cases[@]}" ] \
  || fail "unexpected number of accelerator product E2E cases"
for i in "${!sorted_expected_cases[@]}"; do
  [ "${actual_cases[$i]}" = "${sorted_expected_cases[$i]}" ] \
    || fail "accelerator E2E case set differs from the expected prepared cases"
done
for case_path in "${expected_cases[@]}"; do
  [ "$(git -C "$ROOT" ls-files -s -- "$case_path" | awk '{print $1}')" = 100644 ] \
    || fail "$case_path must be a non-executable platform case file (100644)"
done
for retired in test/e2e/e2e_manifest.sh test/e2e/e2e_obs.sh test/e2e/run_all.sh; do
  if git -C "$ROOT" ls-files --error-unmatch -- "$retired" >/dev/null 2>&1; then
    fail "$retired is retired and must not remain in the source tree"
  fi
done

mkdir -p "$TMP/bin" "$TMP/src" "$TMP/rocksdb"
fixture_root="$TMP/project"
mkdir -p "$fixture_root/scripts" "$fixture_root/LICENSES"
install -m 0644 "$ROOT/LICENSE" "$fixture_root/LICENSE"
printf 'fixture nested project notice\n' > "$fixture_root/LICENSES/NOTICE.txt"
printf 'fixture project attribution\n' > "$fixture_root/NOTICE"
printf '/bin/\n/build/\n' > "$fixture_root/.gitignore"
install -m 0755 "$ROOT/scripts/release.sh" "$fixture_root/scripts/release.sh"
install -m 0755 "$ROOT/scripts/release-materials.sh" "$fixture_root/scripts/release-materials.sh"
install -m 0644 "$ROOT/scripts/release-native-materials.sh" "$fixture_root/scripts/release-native-materials.sh"
# Synthetic link inputs exercise the normal material collection without a real RocksDB build.
cat >> "$fixture_root/scripts/release-native-materials.sh" <<'EOF'
release_native_system_input() {
  local input="$1" label="system/$(basename "$1")"
  case "$(basename "$input")" in libstdc++.a|libgcc.a) ;; *) fail "unexpected fixture system input" ;; esac
  mkdir -p "$RELEASE_MATERIALS_STAGE/share/licenses/$RELEASE_MATERIALS_UNIT/$label"
  printf 'synthetic fixture compiler-runtime notice\n' \
    > "$RELEASE_MATERIALS_STAGE/share/licenses/$RELEASE_MATERIALS_UNIT/$label/LICENSE"
  release_materials_record_source "$2" "system:$(basename "$input")" 1.0 \
    deb-source:fixture@1.0 "sha256:$(sha256sum "$input" | awk '{print $1}');package:fixture" "$label"
}
EOF
install -m 0644 "$ROOT/scripts/release-archive-validator.go" "$fixture_root/scripts/release-archive-validator.go"
install -m 0755 "$ROOT/scripts/publish-release.sh" "$fixture_root/scripts/publish-release.sh"
install -m 0644 "$ROOT/scripts/release-publisher.py" "$fixture_root/scripts/release-publisher.py"
printf 'module github.com/kuasar-sandbox/accelerator\n\ngo 1.24\n' > "$fixture_root/go.mod"
for binary in manifest-ctl store-ctl cache-ctl; do
  mkdir -p "$fixture_root/cmd/$binary"
  printf 'package main\nfunc main() {}\n' > "$fixture_root/cmd/$binary/main.go"
done
mkdir -p "$fixture_root/test"
cp -a "$ROOT/test/scripts" "$fixture_root/test/scripts"
cat > "$fixture_root/Makefile" <<'EOF'
.PHONY: build
build:
	mkdir -p bin/x86_64 build/src/rocksdb build/x86_64/rocksdb/lib build/test-system
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=true -o bin/x86_64/manifest-ctl ./cmd/manifest-ctl
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=true -o bin/x86_64/store-ctl ./cmd/store-ctl
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=true -o bin/x86_64/cache-ctl ./cmd/cache-ctl
	for notice in AUTHORS COPYING LICENSE.Apache LICENSE.leveldb; do printf 'fixture RocksDB %s\n' "$$notice" > "build/src/rocksdb/$$notice"; done
	printf 'fresh synthetic RocksDB archive\n' > build/x86_64/rocksdb/lib/librocksdb.a
	printf 'synthetic stdc++ archive\n' > build/test-system/libstdc++.a
	printf 'synthetic gcc archive\n' > build/test-system/libgcc.a
	@map="$(CURDIR)/build/x86_64/cache-ctl.map"; \
	  printf 'LOAD %s\n' "$(CURDIR)/build/x86_64/rocksdb/lib/librocksdb.a" \
	    "$(CURDIR)/build/test-system/libstdc++.a" "$(CURDIR)/build/test-system/libgcc.a" > "$$map"
EOF
fixture_project_sha="$(init_fixture_repo "$fixture_root" LICENSE LICENSES NOTICE .gitignore scripts go.mod cmd test/scripts Makefile)"
(cd "$fixture_root" && GOWORK=off GOOS=linux GOARCH=amd64 go build -buildvcs=true -o "$TMP/go-fixture" ./cmd/manifest-ctl)
release_materials_require_go_revision "$TMP/go-fixture" "$fixture_project_sha"
printf '// dirty fixture\n' >> "$fixture_root/cmd/manifest-ctl/main.go"
(cd "$fixture_root" && GOWORK=off GOOS=linux GOARCH=amd64 go build -buildvcs=true -o "$TMP/dirty-go-fixture" ./cmd/manifest-ctl)
if (release_materials_require_go_revision "$TMP/dirty-go-fixture" "$fixture_project_sha" >/dev/null 2>&1); then
  fail "release accepted a binary built from dirty source"
fi
printf 'package main\nfunc main() {}\n' > "$fixture_root/cmd/manifest-ctl/main.go"
if (release_materials_require_go_revision "$TMP/go-fixture" \
  0000000000000000000000000000000000000000 >/dev/null 2>&1); then
  fail "release accepted a binary built from another commit"
fi
GOWORK=off GO111MODULE=off go build -o "$TMP/unstamped-go-fixture" "$fixture_root/cmd/manifest-ctl/main.go"
if (release_materials_require_go_revision "$TMP/unstamped-go-fixture" "$fixture_project_sha" >/dev/null 2>&1); then
  fail "release accepted a binary without source stamping"
fi
GOWORK=off make --no-print-directory -C "$fixture_root" build
for binary in manifest-ctl store-ctl cache-ctl; do
  install -m 0755 "$fixture_root/bin/x86_64/$binary" "$TMP/bin/$binary"
done

mkdir -p "$TMP/no-rocksdb-bin"
install -m 0755 "$TMP/bin/manifest-ctl" "$TMP/no-rocksdb-bin/manifest-ctl"
install -m 0755 "$TMP/bin/store-ctl" "$TMP/no-rocksdb-bin/store-ctl"
(cd "$fixture_root" && GOWORK=off GOOS=linux GOARCH=amd64 go build -buildvcs=true -tags no_rocksdb \
  -o "$TMP/no-rocksdb-bin/cache-ctl" ./cmd/cache-ctl)
if SOURCE_DATE_EPOCH=1700000000 RELEASE_BIN_DIR="$TMP/no-rocksdb-bin" \
  RELEASE_SOURCE_ROOT="$fixture_root" "$fixture_root/scripts/release.sh" package v1.2.3 x86_64 \
    "$TMP/no-rocksdb-bundle" > "$TMP/prebuilt-rejection.log" 2>&1; then
  fail "packager accepted a payload without RocksDB support"
fi
grep -Fq 'official accelerator release must include RocksDB support' "$TMP/prebuilt-rejection.log" \
  || fail "prebuilt rejection failed for an unrelated reason"
cp -a "$fixture_root/build/src/rocksdb/." "$TMP/rocksdb/"
sha256sum "$TMP/rocksdb/"{AUTHORS,COPYING,LICENSE.Apache,LICENSE.leveldb} \
  > "$TMP/rocksdb-before.sha256"

SOURCE_DATE_EPOCH=1700000000 RELEASE_BIN_DIR="$TMP/bin" \
  RELEASE_ROCKSDB_SOURCE_DIR="$TMP/rocksdb" \
  RELEASE_SOURCE_ROOT="$fixture_root" "$fixture_root/scripts/release.sh" package v1.2.3 x86_64 "$TMP/bundle"
sha256sum -c "$TMP/rocksdb-before.sha256" >/dev/null \
  || fail "release packaging changed foreign source material"
"$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/bundle"
# A current publication may bind its own binaries to its selected commit;
# standalone validation above does not require or query historical sources.
if SOURCE_SHA=0000000000000000000000000000000000000000 "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/bundle" > "$TMP/publication-source.log" 2>&1; then
  fail "publication accepted an owner binary built at another commit"
fi
grep -Fq 'Go payload must be built from the clean selected commit' "$TMP/publication-source.log" \
  || fail "publication source mismatch failed for an unrelated reason"

"$ROOT/scripts/test-publisher.sh" "$fixture_root/scripts/publish-release.sh" \
  "$TMP/bundle" kuasar-sandbox/accelerator v1.2.3 \
  "$fixture_project_sha" main
"$ROOT/scripts/test-publisher.sh" "$fixture_root/scripts/publish-release.sh" \
  "$TMP/bundle" kuasar-sandbox/accelerator v1.2.3 \
  "$fixture_project_sha" release/v1.2.x

archive="$TMP/bundle/assets/accelerator-v1.2.3-linux-x86_64.tar.gz"
go_toolchain="$(GOWORK=off go -C "$fixture_root" env GOVERSION)"
for path in ./bin/manifest-ctl ./bin/store-ctl ./bin/cache-ctl \
  ./test/scripts/bench_cache.sh \
  ./share/licenses/accelerator/project/LICENSE \
  ./share/licenses/accelerator/rocksdb/AUTHORS \
  ./share/licenses/accelerator/rocksdb/COPYING \
  ./share/licenses/accelerator/rocksdb/LICENSE.Apache \
  ./share/licenses/accelerator/rocksdb/LICENSE.leveldb \
  ./share/licenses/accelerator/go-toolchain/"$go_toolchain"/LICENSE; do
  tar -tzf "$archive" | grep -Fx "$path" >/dev/null || fail "archive is missing $path"
done
if tar -tzf "$archive" | grep -E '(^|/)sources/|SOURCES[.]tsv|GO-BUILD-INFO[.]tsv|GO-MODULES[.]tsv|MATERIALS[.]sha256|CLOUD-HYPERVISOR-Cargo.lock' >/dev/null; then
  fail "new archive includes historical source or build records"
fi
if tar -tzf "$archive" | grep -E '^\./(docs|test/e2e)(/|$)' >/dev/null; then
  fail "component archive contains documentation or E2E sources"
fi
if tar -tzf "$archive" | grep -E '(^|/)release\.json$|(^|/)release/[^/]+\.json$' >/dev/null; then
  fail "archive contains release metadata JSON"
fi

# Preserve the fixture-only native library adapter while moving all tools away
# from the selected product tree. A mistaken tools-root source lookup must fail.
mkdir "$TMP/external-tools"
cp -a "$fixture_root/scripts" "$TMP/external-tools/scripts"
(umask 077; SOURCE_DATE_EPOCH=1700000000 \
  RELEASE_SOURCE_ROOT="$fixture_root" "$TMP/external-tools/scripts/release.sh" package v1.2.3 x86_64 "$TMP/reproducible")
cmp -s "$archive" "$TMP/reproducible/assets/accelerator-v1.2.3-linux-x86_64.tar.gz" \
  || fail "identical inputs did not produce an identical archive"

for target in darwin/amd64 linux/arm64; do
  target_command=manifest-ctl
  [ "$target" != darwin/amd64 ] || target_command=store-ctl
  (cd "$fixture_root" && GOWORK=off CGO_ENABLED=0 GOOS="${target%/*}" GOARCH="${target#*/}" \
    go build -buildvcs=true -o "$TMP/target-${target//\//-}" "./cmd/$target_command")
done
for mutation in extra-binary extra-script extra-directory extra-source extra-source-directory duplicate no-rocksdb wrong-os wrong-arch; do
  candidate="$TMP/exact-contract-$mutation"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  case "$mutation" in
    extra-binary) install -m 0755 "$TMP/go-fixture" "$candidate/root/bin/unexpected-tool" ;;
    extra-script) install -m 0755 "$TMP/go-fixture" "$candidate/root/test/scripts/unexpected-helper" ;;
    extra-directory) mkdir "$candidate/root/unexpected-directory" ;;
    extra-source) mkdir -p "$candidate/root/share/sources/accelerator"; printf 'uncontracted metadata\n' > "$candidate/root/share/sources/accelerator/unexpected" ;;
    extra-source-directory) mkdir -p "$candidate/root/share/sources/accelerator/nested" ;;
    no-rocksdb) install -m 0755 "$TMP/no-rocksdb-bin/cache-ctl" "$candidate/root/bin/cache-ctl" ;;
    wrong-os) install -m 0755 "$TMP/target-darwin-amd64" "$candidate/root/bin/store-ctl" ;;
    wrong-arch) install -m 0755 "$TMP/target-linux-arm64" "$candidate/root/bin/manifest-ctl" ;;
  esac
  if [ "$mutation" = duplicate ]; then
    tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
      -cf "$candidate/duplicate.tar" -C "$candidate/root" .
    tar --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
      -rf "$candidate/duplicate.tar" -C "$candidate/root" ./bin/manifest-ctl
    gzip -c "$candidate/duplicate.tar" > "$candidate/assets/$(basename "$archive")"
  else
    tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
      -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  fi
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted $mutation with regenerated checksums"
  fi
  case "$mutation" in
    extra-*) expected='unexpected member' ;;
    duplicate) expected='duplicate member' ;;
    no-rocksdb) expected='must include RocksDB support' ;;
    wrong-*) expected='must target linux/amd64' ;;
  esac
  grep -Fq "$expected" "$candidate/result.log" || fail "$mutation failed for an unrelated reason"
done

for payload in manifest-ctl store-ctl cache-ctl; do
  candidate="$TMP/wrong-main-$payload"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  other=manifest-ctl
  [ "$payload" != manifest-ctl ] || other=store-ctl
  install -m 0755 "$candidate/root/bin/$other" "$candidate/root/bin/$payload"
  release_materials_require_go_revision "$candidate/root/bin/$payload" "$fixture_project_sha"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted another same-commit main package as $payload"
  fi
  grep -Fq 'must have its expected main package' "$candidate/result.log" \
    || fail "$payload identity failed for an unrelated reason"
done

for native in libstdc++.a libgcc.a; do
  candidate="$TMP/missing-native-$native"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  rm -rf "$candidate/root/share/licenses/accelerator/system/$native"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted missing native notices: $native"
  fi
  grep -Fq 'license material is missing' "$candidate/result.log" || fail "native notice absence failed for an unrelated reason"
done

for notice in AUTHORS COPYING LICENSE.Apache LICENSE.leveldb; do
  candidate="$TMP/missing-rocks-notice-$notice"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  rm "$candidate/root/share/licenses/accelerator/rocksdb/$notice"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted missing RocksDB $notice with regenerated checksums"
  fi
  grep -Fq "missing member \"./share/licenses/accelerator/rocksdb/$notice\"" "$candidate/result.log" \
    || fail "missing RocksDB notice failed for an unrelated reason"
done

cp -a "$TMP/bundle" "$TMP/tampered"
printf 'tampered\n' >> "$TMP/tampered/assets/accelerator-v1.2.3-linux-x86_64.tar.gz"
if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/tampered" >/dev/null 2>&1; then
  fail "validator accepted a tampered archive"
fi

cp -a "$TMP/bundle" "$TMP/extra"
touch "$TMP/extra/assets/release.json"
if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/extra" >/dev/null 2>&1; then
  fail "validator accepted an extra asset"
fi

mkdir -p "$TMP/material-stage"
tar -xzf "$archive" -C "$TMP/material-stage"
: > "$TMP/material-stage/share/licenses/accelerator/rocksdb/COPYING"
cp -a "$TMP/bundle" "$TMP/material-tampered"
tar --sort=name --owner=0 --group=0 --numeric-owner --mtime='@1700000000' \
  --pax-option=delete=atime,delete=ctime -czf \
  "$TMP/material-tampered/assets/accelerator-v1.2.3-linux-x86_64.tar.gz" \
  -C "$TMP/material-stage" .
if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 \
  "$TMP/material-tampered" >/dev/null 2>&1; then
  fail "validator accepted empty license material"
fi

if RELEASE_BIN_DIR="$TMP/bin" RELEASE_SOURCE_ROOT="$fixture_root" "$fixture_root/scripts/release.sh" package 01.2.3 x86_64 \
  "$TMP/invalid-version" >/dev/null 2>&1; then
  fail "packager accepted an invalid version"
fi
if RELEASE_BIN_DIR="$TMP/bin" RELEASE_SOURCE_ROOT="$fixture_root" "$fixture_root/scripts/release.sh" package v1.2.3 aarch64 \
  "$TMP/invalid-arch" >/dev/null 2>&1; then
  fail "packager accepted x86_64 payloads as aarch64"
fi

for mutation in setuid setgid writable-directory writable-binary; do
  candidate="$TMP/unsafe-mode-$mutation"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  case "$mutation" in
    setuid) chmod 4755 "$candidate/root/bin/manifest-ctl" ;;
    setgid) chmod 2755 "$candidate/root/bin/manifest-ctl" ;;
    writable-directory) chmod 0777 "$candidate/root/bin" ;;
    writable-binary) chmod 0777 "$candidate/root/bin/manifest-ctl" ;;
  esac
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted $mutation with regenerated checksums"
  fi
  grep -Fq 'unsafe type, mode or ownership' "$candidate/result.log" \
    || fail "$mutation was rejected for an unrelated reason"
done

cp -a "$TMP/bundle" "$TMP/nonroot-owner"
mkdir "$TMP/nonroot-owner/root"
tar -xzf "$archive" -C "$TMP/nonroot-owner/root"
tar --sort=name --owner=1234 --group=0 --numeric-owner --mtime=@1700000000 \
  -czf "$TMP/nonroot-owner/assets/$(basename "$archive")" -C "$TMP/nonroot-owner/root" .
if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/nonroot-owner" >/dev/null 2>&1; then
  fail "validator accepted non-root numeric ownership with regenerated checksums"
fi

# Standalone validation must not need source checkouts, module downloads or a build.
mkdir -p "$TMP/standalone-tools" "$TMP/standalone-bin"
cp -a "$fixture_root/scripts" "$TMP/standalone-tools/scripts"
for command in git curl wget cargo make gcc; do
  printf '#!/bin/sh\nexit 97\n' > "$TMP/standalone-bin/$command"
  chmod 0755 "$TMP/standalone-bin/$command"
done
env PATH="$TMP/standalone-bin:$PATH" GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local \
  SOURCE_SHA="$fixture_project_sha" "$TMP/standalone-tools/scripts/release.sh" validate \
  v1.2.3 x86_64 "$TMP/bundle"
echo "test-release: standalone validation without checkouts/downloads/build PASS"

echo "test-release: PASS"
