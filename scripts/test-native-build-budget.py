#!/usr/bin/env python3
"""Exercise the RocksDB recipe's actual CMake invocation without compiling RocksDB."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class NativeBuildBudget(unittest.TestCase):
    def run_recipe(self, budget):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "input"
            (source / "include/rocksdb").mkdir(parents=True)
            (source / "include/rocksdb/db.h").write_text("fixture header\n")
            digest = subprocess.check_output(
                ["bash", "-c", 'source "$1"; source_tree_sha256 "$2"', "digest",
                 str(ROOT / "deps/common.sh"), str(source)], text=True).strip()
            archive = root / "rocksdb.tar.gz"
            with tarfile.open(archive, "w:gz") as stream:
                stream.add(source, arcname="rocksdb")
            tools = root / "tools"
            tools.mkdir()
            cmake = tools / "cmake"
            cmake.write_text(f"#!{sys.executable}\n" + '''import json, os, pathlib, sys
with open(os.environ['CMAKE_LOG'], 'a') as log:
    print(json.dumps(sys.argv[1:]), file=log)
if '--build' in sys.argv:
    (pathlib.Path(sys.argv[sys.argv.index('--build') + 1]) / 'librocksdb.a').write_bytes(b'fixture archive')
''')
            cmake.chmod(0o755)
            for tool, body in (("nproc", "echo 160"), ("g++", ":")):
                path = tools / tool
                path.write_text("#!/bin/sh\n" + body + "\n")
                path.chmod(0o755)
            log = root / "cmake.jsonl"
            env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ["PATH"],
                       BUILD_DIR=str(root / "build/x86_64"), TARGET_ARCH="x86_64",
                       CROSS_PREFIX="", ROCKSDB_TARBALL=str(archive),
                       ROCKSDB_SOURCE_SHA256=digest, CMAKE_LOG=str(log))
            env.pop("KUASAR_BUILD_JOBS", None)
            if budget is not None:
                env["KUASAR_BUILD_JOBS"] = budget
            result = subprocess.run(["bash", str(ROOT / "deps/build-rocksdb.sh")],
                                    env=env, text=True, capture_output=True, timeout=30)
            calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            return result, calls

    def test_budget_overrides_visible_host_cpu_count(self):
        result, calls = self.run_recipe("4")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1][2:], ["-j4", "--target", "rocksdb"])

    def test_local_default_keeps_nproc(self):
        result, calls = self.run_recipe(None)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("-j160", calls[1])

    def test_invalid_budget_fails_before_configure_or_build(self):
        for budget in ("", "0", "-1", "1.5", "4 8", "unlimited"):
            with self.subTest(budget=budget):
                result, calls = self.run_recipe(budget)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("KUASAR_BUILD_JOBS must be a positive integer", result.stderr)
                self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
