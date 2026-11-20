#!/usr/bin/env python3
"""Activation fixture boundaries using real port leases, without claiming E2E."""

import ast
import contextlib
import errno
import io
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
CASE = ROOT / "test/e2e/cases/basic.accelerator-activation.sh"
HELPER = ROOT / "test/e2e/lib/port_lease.sh"


def definitions(source):
    program = source.split("<<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
    tree = ast.parse(program)
    setup = ast.Module(body=[node for node in tree.body if isinstance(
        node, (ast.Import, ast.ImportFrom, ast.Assign, ast.Assert, ast.FunctionDef, ast.ClassDef))], type_ignores=[])
    return tree, setup


class ActivationFixtureTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="activation-regression-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.lib = self.root / "lib"
        (self.lib / "accelerator").mkdir(parents=True)
        (self.lib / "accelerator/port_lease.sh").write_bytes(HELPER.read_bytes())
        (self.lib / "common.sh").write_text('''require_binary() { test -x "$BIN/$1"; }
require_command() { command -v "$1" >/dev/null; }
''')
        for name in ("cache-ctl", "store-ctl", "manifest-ctl", "flatten-ctl", "systemctl", "redis-server"):
            self.executable(name, "#!/bin/sh\nexit 97\n")
        self.executable("sudo", '#!/bin/sh\n[ "$1" = -n ] && [ "$2" = true ]\n')
        self.env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ["PATH"],
                        BIN=str(self.bin), E2E_LIB=str(self.lib), TMPDIR=str(self.root),
                        ACTIVATION_TRACE=str(self.root))

    def executable(self, name, text):
        path = self.bin / name
        path.write_text(text)
        path.chmod(0o755)

    def namespace(self):
        _, setup = definitions(CASE.read_text())
        namespace = {}
        with mock.patch.object(sys, "argv", ["-", str(self.bin), str(HELPER), str(self.root / "ports")]):
            exec(compile(setup, str(CASE), "exec"), namespace)
        namespace["privilege"] = []
        return namespace

    def test_later_scenarios_do_not_preallocate_ports(self):
        # Execute the real shell entry and real helper. Only the Python product
        # body is replaced by barriers after its actual endpoint configuration.
        # No stub can report product/E2E success or touch host systemd units.
        driver = Path(__file__).resolve()
        wrapper = f'''#!{sys.executable}
import ast, importlib.util, json, os, pathlib, sys, time
trace = pathlib.Path(os.environ["ACTIVATION_TRACE"])
if len(sys.argv) < 3 or sys.argv[2] != os.environ["BIN"]:
    if len(sys.argv) == 3 and sys.argv[1] == "-":
        (trace / "lease-path").write_text(sys.argv[2])
    os.execv({sys.executable!r}, [{sys.executable!r}, *sys.argv[1:]])
source = sys.stdin.read()
spec = importlib.util.spec_from_file_location("activation_fixture_test", {str(driver)!r})
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
tree, setup = module.definitions("<<'PY'\\n" + source + "\\nPY")
def leases():
    marker = trace / "lease-path"
    return pathlib.Path(marker.read_text()).read_text().splitlines() if marker.exists() else []
(trace / "entered.json").write_text(json.dumps(leases()))
sys.argv = sys.argv[1:]
namespace = {{}}
exec(compile(setup, "fixture-setup", "exec"), namespace)
outer = next(node for node in tree.body if isinstance(node, ast.For) and isinstance(node.target, ast.Name) and node.target.id == "backend")
scenario = outer.body[0].body[0]
boundary = next(i for i, node in enumerate(scenario.body) if isinstance(node, ast.FunctionDef) and node.name == "spawn")
scenario.body = scenario.body[:boundary] + ast.parse("scenario_probe(backend, network, entrances)").body
index = 0
def probe(backend, network, entrances):
    global index
    number = index
    index += 1
    record = {{"backend": backend, "network": network, "leases": leases(), "tcp": [v for v in entrances.values() if not v.startswith("/")]}}
    (trace / f"step-{{number}}.json").write_text(json.dumps(record))
    deadline = time.monotonic() + 10
    while not (trace / f"release-{{number}}").exists():
        if time.monotonic() > deadline:
            raise TimeoutError("regression did not release scenario")
        time.sleep(.01)
namespace["scenario_probe"] = probe
exec(compile(ast.fix_missing_locations(ast.Module(body=[outer], type_ignores=[])), "fixture-scenarios", "exec"), namespace)
'''
        self.executable("python3", wrapper)
        proc = subprocess.Popen(["bash", str(CASE)], env=self.env, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, start_new_session=True)
        try:
            self.wait_file(self.root / "entered.json", proc)
            self.assertEqual(json.loads((self.root / "entered.json").read_text()), [],
                             "ports were selected before any scenario started")
            allocated = []
            for index, expected in enumerate((backend, network) for backend in ("embedded", "redis", "store")
                                             for network in ("tcp", "unix")):
                path = self.root / f"step-{index}.json"
                self.wait_file(path, proc)
                record = json.loads(path.read_text())
                self.assertEqual((record["backend"], record["network"]), expected)
                allocated.extend(address.rsplit(":", 1)[1] for address in record["tcp"])
                self.assertEqual(record["leases"], allocated)
                self.assertEqual(len(set(allocated)), len(allocated))
                # The current scenario is blocked: a later mode must not have
                # allocated ports while the current one still owns execution.
                time.sleep(.04)
                lease_path = Path((self.root / "lease-path").read_text())
                self.assertEqual(lease_path.read_text().splitlines(), allocated)
                self.assertFalse((self.root / f"step-{index + 1}.json").exists())
                (self.root / f"release-{index}").touch()
            output, _ = proc.communicate(timeout=5)
            self.assertEqual(proc.returncode, 0, output)
            self.assertEqual(len(allocated), 10)
            self.assertFalse(lease_path.exists(), "real shell EXIT trap did not remove its lease")
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
                proc.communicate(timeout=3)

    def wait_file(self, path, proc):
        deadline = time.monotonic() + 5
        while not path.exists():
            if proc.poll() is not None or time.monotonic() >= deadline:
                self.fail(f"fixture boundary was not reached: {path.name}; exit={proc.poll()}")
            time.sleep(.01)

    def test_prior_client_timewait_does_not_consume_next_scenario_port(self):
        endpoint = self.namespace()["endpoint"]
        address = endpoint(self.root, "old", "tcp")
        port = int(address.rsplit(":", 1)[1])
        with socket.socket() as client, socket.socket() as server:
            client.bind(("127.0.0.1", port))
            server.bind(("127.0.0.1", 0))
            server.listen(1)
            client.connect(server.getsockname())
            peer, _ = server.accept()
            with peer:
                client.shutdown(socket.SHUT_WR)
                self.assertEqual(peer.recv(1), b"")
            self.assertEqual(client.recv(1), b"")
        with socket.socket() as old_listener:
            old_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            with self.assertRaises(OSError) as failed:
                old_listener.bind(("127.0.0.1", port))
            self.assertEqual(failed.exception.errno, errno.EADDRINUSE)
        fresh = int(endpoint(self.root, "next", "tcp").rsplit(":", 1)[1])
        self.assertNotEqual(fresh, port)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", fresh))
            listener.listen(1)

    def test_ctl_failure_keeps_diagnostic_and_nonzero_result(self):
        count = self.root / "calls"
        self.executable("systemctl", f'''#!{sys.executable}
from pathlib import Path
import sys
with Path({str(count)!r}).open("a") as output: output.write("call\\n")
print("owned socket: Address already in use", file=sys.stderr)
raise SystemExit(17)
''')
        ctl = self.namespace()["ctl"]
        diagnostic = io.StringIO()
        with mock.patch.dict(os.environ, self.env), contextlib.redirect_stderr(diagnostic):
            with self.assertRaises(subprocess.CalledProcessError) as failed:
                ctl("start", "owned-test.socket")
        self.assertEqual(failed.exception.returncode, 17)
        self.assertIn("Address already in use", diagnostic.getvalue())
        self.assertEqual(count.read_text(), "call\n", "failed socket startup was retried")


if __name__ == "__main__":
    unittest.main(verbosity=2)
