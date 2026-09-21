#!/usr/bin/env python3
"""Write reproducible, data-only Docker archives without Docker or downloads."""

import hashlib
import argparse
import io
import json
from pathlib import Path
import platform
import sys
import tarfile


def json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def add_file(archive, name, data, *, uid=0, gid=0, mode=0o644):
    entry = tarfile.TarInfo(name)
    entry.size = len(data)
    entry.uid, entry.gid, entry.mode = uid, gid, mode
    entry.mtime = 0
    archive.addfile(entry, io.BytesIO(data))


def layer(variant=None):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        if variant is None:
            directory = tarfile.TarInfo("fixture/")
            directory.type = tarfile.DIRTYPE
            directory.mode = 0o755
            archive.addfile(directory)
            # SHAKE yields stable, non-repeating bytes, independent of host RNG,
            # Python random versions and wall clock. Four MiB exceeds several
            # default CDC max chunks (1 MiB) and fixed chunks (512 KiB).
            shared = hashlib.shake_256(b"accelerator manifest shared v1").digest(4 * 1024 * 1024)
            add_file(archive, "fixture/shared.bin", shared, uid=1000, gid=1000, mode=0o640)
            add_file(archive, "fixture/changed.bin", b"replaced by the second layer\n", uid=1000, gid=1000)
        else:
            changed = hashlib.shake_256(f"accelerator manifest {variant} v1".encode()).digest(1024 * 1024)
            add_file(archive, "fixture/changed.bin", changed, uid=1000, gid=1000)
    return output.getvalue()


def write_archive(path, variant, layers, architecture="amd64"):
    if architecture not in ("amd64", "arm64"):
        raise ValueError("unsupported fixture architecture")
    digests = [hashlib.sha256(data).hexdigest() for data in layers]
    config = json_bytes({
        "architecture": architecture,
        "os": "linux",
        "created": "1970-01-01T00:00:00Z",
        # A scratch/data image: no executable or container launch is needed.
        "config": {
            "User": "1000:1000",
            "Env": ["MANIFEST_E2E=1"],
            "WorkingDir": "/fixture",
            "StopSignal": "SIGTERM",
            "Labels": {"org.kuasar.manifest-fixture": variant},
        },
        "rootfs": {"type": "layers", "diff_ids": [f"sha256:{digest}" for digest in digests]},
        "history": [
            {"created": "1970-01-01T00:00:00Z", "created_by": "shared fixture"},
            {"created": "1970-01-01T00:00:00Z", "created_by": f"variant {variant}"},
        ],
    })
    config_name = hashlib.sha256(config).hexdigest() + ".json"
    layer_names = [f"{digest}/layer.tar" for digest in digests]
    manifest = [{
        "Config": config_name,
        "RepoTags": [f"accelerator-manifest-fixture:{variant}"],
        "Layers": layer_names,
    }]
    with tarfile.open(path, "w", format=tarfile.USTAR_FORMAT) as archive:
        add_file(archive, "manifest.json", json_bytes(manifest))
        add_file(archive, config_name, config)
        for name, data in zip(layer_names, layers):
            add_file(archive, name, data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--architecture", choices=("amd64", "arm64"),
                        default={"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine()))
    args = parser.parse_args()
    if args.architecture is None:
        parser.error("--architecture is required on this host")
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    base = layer()
    for variant in ("a", "b"):
        write_archive(output / f"image-{variant}.tar", variant, [base, layer(variant)], args.architecture)


if __name__ == "__main__":
    main()
