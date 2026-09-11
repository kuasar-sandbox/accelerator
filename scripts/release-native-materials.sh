#!/usr/bin/env bash
# Copyright/source material for actual RocksDB/CGO release link inputs.
# Sourced by release.sh; uses its existing material layout and fail().
release_native_copy_file() {
  local source="$1" label="$2" name="$3"
  release_materials_safe_relative "$label/$name" || fail "unsafe native material name"
  [ -s "$source" ] || fail "native license material is missing: $source"
  local destination="$RELEASE_MATERIALS_STAGE/share/licenses/$RELEASE_MATERIALS_UNIT/$label/$name"
  mkdir -p "$(dirname "$destination")"
  install -m 0644 "$source" "$destination"
}

release_native_system_input() {
  local input="$1" payload="$2" query owner source_name version label copyright common
  local source_id rpm_source sibling file count=0
  input="$(realpath -e "$input")" || fail "native link input is missing"
  label="system/$(basename "$input")"
  if command -v dpkg-query >/dev/null 2>&1 \
    && query="$(dpkg-query -S "$input" 2>/dev/null)"; then
    owner="${query%%: /*}"
    [[ "$owner" != *$'\n'* && "$owner" != *,* ]] || fail "ambiguous native package owner"
    query="$(dpkg-query -W -f '${source:Package}\t${source:Version}\n' "$owner")"
    IFS=$'\t' read -r source_name version <<< "$query"
    if [ -z "$source_name" ] || [ -z "$version" ]; then
      fail "native source package identity is missing"
    fi
    source_id="deb-source:$source_name@$version"
    copyright="/usr/share/doc/${owner%%:*}/copyright"
    release_native_copy_file "$copyright" "$label" copyright
    # Debian copyright files refer to common license texts outside the package.
    while IFS= read -r common; do
      [ -n "$common" ] || continue
      if [ ! -e "$common" ] && [[ "$common" == *. ]]; then
        common="${common%.}"
      fi
      release_native_copy_file "$common" "$label" "common-licenses/$(basename "$common")"
    done < <(grep -Eo '/usr/share/common-licenses/[A-Za-z0-9.+-]+' "$copyright" | LC_ALL=C sort -u)
  elif command -v rpm >/dev/null 2>&1 \
    && query="$(rpm -qf --qf '%{NAME}\t%{VERSION}-%{RELEASE}\t%{SOURCERPM}\n' "$input" 2>/dev/null)"; then
    IFS=$'\t' read -r owner version rpm_source <<< "$query"
    if [ -z "$rpm_source" ] || [ "$rpm_source" = '(none)' ]; then
      fail "native RPM source identity is missing: $input"
    fi
    source_name="$owner"
    source_id="rpm-source:$rpm_source"
    # Static/devel subpackages may keep notices in a sibling from the SAME SRPM.
    # Capture each status before consuming output; partial listings are not a
    # complete license inventory even if another sibling supplied valid files.
    local packages siblings files
    packages="$(rpm -qa --qf '%{NAME}.%{ARCH}\t%{SOURCERPM}\n')" \
      || fail "cannot enumerate installed RPM packages for license collection"
    siblings="$(awk -F '\t' -v source="$rpm_source" '$2 == source {print $1}' <<< "$packages")" \
      || fail "cannot select same-source RPM license packages"
    while IFS= read -r sibling; do
      [ -n "$sibling" ] || continue
      files="$(rpm -ql "$sibling")" || fail "cannot enumerate RPM license files: $sibling"
      while IFS= read -r file; do
        case "$(basename "$file")" in
          LICENSE*|COPYING*|NOTICE*|COPYRIGHT*|copyright|AUTHORS*|CREDITS*) ;;
          *) [[ "$file" == /usr/share/licenses/* ]] || continue ;;
        esac
        [ ! -d "$file" ] || continue
        release_native_copy_file "$file" "$label" "${file#/}"
        count=$((count + 1))
      done <<< "$files"
    done <<< "$siblings"
    [ "$count" -gt 0 ] || fail "native RPM license material is missing: $rpm_source"
  else
    fail "native input has no package/source material: $input"
  fi
  release_materials_record_source "$payload" "system:$(basename "$input")" "$version" \
    "$source_id" "sha256:$(sha256sum "$input" | awk '{print $1}');package:$source_name" "$label"
}

release_native_cache_inputs() {
  local map="$1" rocks_library="$2" generated_root="$3" input canonical name count=0 rocks_seen=0
  local -A selected_inputs=()
  [ -s "$map" ] || fail "cache-ctl linker map is missing"
  rocks_library="$(realpath -e "$rocks_library")" || fail "RocksDB archive is missing"
  generated_root="$(realpath -e "$generated_root")" || fail "owned Go build directory is missing"
  while IFS= read -r input; do
    [ -n "$input" ] || continue
    [[ "$input" == /* ]] || fail "cache linker map contains an unresolved native input"
    case "$input" in
      "$generated_root"/go-link-*/*.o) continue ;; # Go's run-owned, temporary CGO objects.
    esac
    canonical="$(realpath -e "$input")" || fail "cache linker map input no longer exists"
    if [ "$canonical" = "$rocks_library" ]; then
      rocks_seen=$((rocks_seen + 1))
      continue
    fi
    name="$(basename "$canonical")"
    if [ -n "${selected_inputs[$name]:-}" ] && [ "${selected_inputs[$name]}" != "$canonical" ]; then
      fail "distinct native link inputs share a material name: $name"
    fi
    selected_inputs[$name]="$canonical"
    release_native_system_input "$canonical" bin/cache-ctl
    count=$((count + 1))
  done < <(awk '$1 == "LOAD" && $2 ~ /\.(a|o)$/ {print $2}' "$map" | LC_ALL=C sort -u)
  [ "$rocks_seen" -eq 1 ] || fail "cache-ctl did not link the selected RocksDB archive"
  [ "$count" -gt 0 ] || fail "cache linker map contains no system inputs"
  for input in libstdc++.a libgcc.a; do
    awk -F '\t' -v name="system:$input" '$2 == name {found=1} END {exit !found}' \
      "$RELEASE_MATERIALS_WORK/sources" || fail "cache source material is missing $input"
  done
}
