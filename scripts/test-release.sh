#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "test-release: $*" >&2
  exit 1
}

bash "$ROOT/scripts/test-preview-line.sh"
bash "$ROOT/scripts/test-delete-preview.sh"

mkdir -p "$TMP/source-bin"
cat > "$TMP/source-bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[ "${1:-}" = api ] || exit 2
[ "${2:-}" = "repos/$GITHUB_REPOSITORY/git/ref/heads/${FAKE_SOURCE_REF:?}" ] || exit 2
printf '%s\n' "${FAKE_SOURCE_SHA:?}"
EOF
chmod +x "$TMP/source-bin/gh"
env PATH="$TMP/source-bin:$PATH" GITHUB_REPOSITORY=kuasar-sandbox/accelerator FAKE_SOURCE_REF=release/v1.2.x FAKE_SOURCE_SHA=1111111111111111111111111111111111111111 bash "$ROOT/scripts/validate-release-source.sh" release/v1.2.x 1111111111111111111111111111111111111111 v1.2.3 accelerator >/dev/null
if env PATH="$TMP/source-bin:$PATH" GITHUB_REPOSITORY=kuasar-sandbox/accelerator FAKE_SOURCE_REF=release/v1.2.x FAKE_SOURCE_SHA=1111111111111111111111111111111111111111 bash "$ROOT/scripts/validate-release-source.sh" release/v1.2.x 1111111111111111111111111111111111111111 v1.3.0 accelerator >/dev/null 2>&1; then
  fail "release source validator accepted a tag from another version line"
fi
bash -n "$ROOT/scripts/delete-preview.sh" "$ROOT/scripts/validate-release-source.sh"
grep -Fqx 'run-name: Release ${{ inputs.version }} @${{ inputs.source_sha }}' \
  "$ROOT/.github/workflows/release.yml" \
  || fail "release run identity does not pin source_sha"
grep -Fq 'kuasar-preview-binding' "$ROOT/scripts/publish-release.sh" \
  || fail "Preview publisher does not record its build binding"
grep -Fq "&& 'stable-main' || inputs.version" \
  "$ROOT/.github/workflows/release.yml" \
  || fail "main Stable publication does not serialize Latest updates"
grep -Fq 'group: component-delete-${{ github.repository }}-${{ inputs.version }}' \
  "$ROOT/.github/workflows/delete-preview.yml" \
  || fail "cleanup does not use its version-scoped mutation group"
if grep -R -Fq 'queue: max' "$ROOT/.github/workflows"; then
  fail "workflows use the unsupported concurrency queue key"
fi

for entrypoint in test/e2e/e2e_manifest.sh test/e2e/e2e_obs.sh \
  test/e2e/run_all.sh; do
  [ "$(git -C "$ROOT" ls-files -s -- "$entrypoint" | awk '{print $1}')" = 100755 ] \
    || fail "$entrypoint is not executable in the Git index"
done

mkdir -p "$TMP/bin" "$TMP/src"
printf 'package main\nfunc main() {}\n' > "$TMP/src/main.go"
GO111MODULE=off go build -o "$TMP/go-fixture" "$TMP/src/main.go"
for binary in manifest-ctl store-ctl cache-ctl; do
  install -m 0755 "$TMP/go-fixture" "$TMP/bin/$binary"
done

SOURCE_DATE_EPOCH=1700000000 RELEASE_BIN_DIR="$TMP/bin" \
  "$ROOT/scripts/release.sh" package v1.2.3 x86_64 "$TMP/bundle"
"$ROOT/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/bundle"
"$ROOT/scripts/test-publisher.sh" "$ROOT/scripts/publish-release.sh" \
  "$TMP/bundle" kuasar-sandbox/accelerator v1.2.3 \
  1111111111111111111111111111111111111111 main
"$ROOT/scripts/test-publisher.sh" "$ROOT/scripts/publish-release.sh" \
  "$TMP/bundle" kuasar-sandbox/accelerator v1.2.3 \
  1111111111111111111111111111111111111111 release/v1.2.x

archive="$TMP/bundle/assets/accelerator-v1.2.3-linux-x86_64.tar.gz"
for path in ./bin/manifest-ctl ./bin/store-ctl ./bin/cache-ctl \
  ./test/scripts/bench_cache.sh; do
  tar -tzf "$archive" | grep -Fx "$path" >/dev/null || fail "archive is missing $path"
done
if tar -tzf "$archive" | grep -E '^\./(docs|test/e2e)(/|$)' >/dev/null; then
  fail "component archive contains documentation or E2E sources"
fi
if tar -tzf "$archive" | grep -E '(^|/)release\.json$|(^|/)release/[^/]+\.json$' >/dev/null; then
  fail "archive contains release metadata JSON"
fi

SOURCE_DATE_EPOCH=1700000000 RELEASE_BIN_DIR="$TMP/bin" \
  "$ROOT/scripts/release.sh" package v1.2.3 x86_64 "$TMP/reproducible"
cmp -s "$archive" "$TMP/reproducible/assets/accelerator-v1.2.3-linux-x86_64.tar.gz" \
  || fail "identical inputs did not produce an identical archive"

cp -a "$TMP/bundle" "$TMP/tampered"
printf 'tampered\n' >> "$TMP/tampered/assets/accelerator-v1.2.3-linux-x86_64.tar.gz"
if "$ROOT/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/tampered" >/dev/null 2>&1; then
  fail "validator accepted a tampered archive"
fi

cp -a "$TMP/bundle" "$TMP/extra"
touch "$TMP/extra/assets/release.json"
if "$ROOT/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/extra" >/dev/null 2>&1; then
  fail "validator accepted an extra asset"
fi

if RELEASE_BIN_DIR="$TMP/bin" "$ROOT/scripts/release.sh" package 01.2.3 x86_64 \
  "$TMP/invalid-version" >/dev/null 2>&1; then
  fail "packager accepted an invalid version"
fi
if RELEASE_BIN_DIR="$TMP/bin" "$ROOT/scripts/release.sh" package v1.2.3 aarch64 \
  "$TMP/invalid-arch" >/dev/null 2>&1; then
  fail "packager accepted an unvalidated release architecture"
fi

echo "test-release: PASS"
