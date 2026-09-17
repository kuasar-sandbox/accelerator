#!/usr/bin/env python3
"""CI-only guard for the platform's Linux x86_64 / glibc 2.38 release baseline.

Use the guest-runtime readelf idiom without executing the selected binaries.
The existing packager still verifies RocksDB, static link inputs and materials.
"""
import argparse
from pathlib import Path
import re
import subprocess


def validate(path, static=False):
    output = subprocess.run(
        ["readelf", "--wide", "--program-headers", "--dynamic", "--version-info", str(path)],
        check=True, capture_output=True, text=True,
    ).stdout
    interpreter = re.search(r"\bINTERP\b", output)
    needed = re.findall(r"\(NEEDED\).*?\[([^\]]+)\]", output)
    versions = re.findall(r"\bGLIBC_(\d+)\.(\d+)(?:\.(\d+))?\b", output)
    if static:
        if interpreter or "(NEEDED)" in output or versions:
            raise ValueError(f"{path}: requires a fully static executable")
    else:
        if not interpreter or "libc.so.6" not in needed or not versions:
            raise ValueError(f"{path}: cache-ctl must retain its normal CGO/glibc link mode")
        if any(re.match(r"lib(?:rocksdb|stdc\+\+|gcc_s)(?:[.-]|$)", name) for name in needed):
            raise ValueError(f"{path}: RocksDB, libstdc++ and libgcc must remain statically linked")
    if "GLIBC_PRIVATE" in output or any(
        tuple(int(part or 0) for part in version) > (2, 38, 0) for version in versions
    ):
        raise ValueError(f"{path}: exceeds the released glibc 2.38 baseline")
    print(f"Accelerator ABI: {path}: {'static' if static else 'glibc <= 2.38; static native libraries'}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary_directory", type=Path)
    args = parser.parse_args()
    for binary in ("manifest-ctl", "store-ctl"):
        validate(args.binary_directory / binary, static=True)
    validate(args.binary_directory / "cache-ctl")
