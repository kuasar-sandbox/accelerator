#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
fail() { echo "test-native-materials: $*" >&2; exit 1; }
# shellcheck source=scripts/release-materials.sh
source "$ROOT/scripts/release-materials.sh"
# shellcheck source=scripts/release-native-materials.sh
source "$ROOT/scripts/release-native-materials.sh"

# Link selection tests use private synthetic files, not real compiler inputs.
mkdir -p "$test_root/link/system" "$test_root/link/go-temp"
for library in librocksdb.a libstdc++.a libgcc.a unowned.a; do
  printf 'fixture %s\n' "$library" > "$test_root/link/system/$library"
done
release_native_system_input() {
  case "$(basename "$1")" in libstdc++.a|libgcc.a) ;; *) fail "unowned native fixture input" ;; esac
  printf 'bin/cache-ctl\tsystem:%s\tfixture\tfixture\tfixture\tfixture\n' "$(basename "$1")" \
    >> "$RELEASE_MATERIALS_WORK/sources"
}
for mutation in valid missing-map empty-map missing-rocks missing-stdlib missing-gcc unknown relative outside-go-temp; do
  release_materials_init "$test_root/map-stage-$mutation" "$test_root/map-work-$mutation" fixture
  map="$test_root/link/$mutation.map"
  if [ "$mutation" != missing-map ]; then
    : > "$map"
    if [ "$mutation" != empty-map ]; then
      for library in librocksdb.a libstdc++.a libgcc.a; do
        case "$mutation:$library" in missing-rocks:librocksdb.a|missing-stdlib:libstdc++.a|missing-gcc:libgcc.a) continue ;; esac
        printf 'LOAD %s/link/system/%s\n' "$test_root" "$library" >> "$map"
      done
      # The compiler has already removed its own temporary CGO objects.
      printf 'LOAD %s/link/go-temp/go-link-123/go.o\n' "$test_root" >> "$map"
      case "$mutation" in
        unknown) printf 'LOAD %s/link/system/unowned.a\n' "$test_root" >> "$map" ;;
        relative) printf 'LOAD unowned.a\n' >> "$map" ;;
        outside-go-temp) printf 'LOAD %s/not-owned/go-link-123/go.o\n' "$test_root" >> "$map" ;;
      esac
    fi
  fi
  if (release_native_cache_inputs "$map" "$test_root/link/system/librocksdb.a" \
    "$test_root/link/go-temp" > "$test_root/map-$mutation.log" 2>&1); then
    [ "$mutation" = valid ] || fail "link input selection accepted $mutation"
  else
    [ "$mutation" != valid ] || fail "valid native link input selection failed"
  fi
done
printf 'test-native-materials: exact cache link-input selection PASS\n'

mkdir -p "$test_root/link/other"
printf 'distinct same-name archive\n' > "$test_root/link/other/libstdc++.a"
release_materials_init "$test_root/collision-stage" "$test_root/collision-work" fixture
printf 'LOAD %s\n' "$test_root/link/system/librocksdb.a" \
  "$test_root/link/system/libstdc++.a" "$test_root/link/other/libstdc++.a" \
  > "$test_root/link/collision.map"
if (release_native_cache_inputs "$test_root/link/collision.map" "$test_root/link/system/librocksdb.a" \
    "$test_root/link/go-temp" > "$test_root/collision.log" 2>&1); then
  fail "accepted colliding native input material names"
fi
grep -Fq 'distinct native link inputs share a material name' "$test_root/collision.log" \
  || fail "native collision failed for an unrelated reason"
[ "$(wc -l < "$RELEASE_MATERIALS_WORK/sources")" -eq 1 ] \
  || fail "second colliding native input reached the material collector"
printf 'test-native-materials: colliding input rejected before overwriting materials PASS\n'
