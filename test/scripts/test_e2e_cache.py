#!/usr/bin/env python3
"""Source regression tests for cache E2E assertions, not product E2E."""
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
CASES = ROOT / "test/e2e/cases"


def section(source, first, last):
    return source.split(first, 1)[1].split(last, 1)[0]


class CacheAssertionTests(unittest.TestCase):
    def run_assertion(self, shell, output="", status=0, *, hang=False, limit=8):
        with tempfile.TemporaryDirectory(prefix="cache-assertion-") as directory:
            binary = Path(directory) / "cache-ctl"
            binary.write_text(
                '#!/bin/bash\n'
                'printf "%s\\n" "$PROBE_OUTPUT"\n'
                'if [ "$PROBE_HANG" = 1 ]; then exec /bin/sleep 60; fi\n'
                'exit "$PROBE_STATUS"\n'
            )
            binary.chmod(0o755)
            environment = dict(os.environ, BIN=directory, TMPDIR=directory,
                               TIERED_PORT="1", TEST_HASH="00",
                               PROBE_OUTPUT=output, PROBE_STATUS=str(status),
                               PROBE_HANG="1" if hang else "0")
            # Readiness loops keep their production iteration count but need not
            # sleep in an assertion-only source test.
            program = ('set -euo pipefail\n'
                       'ok() { :; }\n'
                       'fail() { echo "$*" >&2; exit 1; }\n'
                       'sleep() { :; }\n' + shell)
            return subprocess.run(["bash", "-c", program], env=environment,
                                  capture_output=True, text=True, timeout=limit)

    def test_readiness_requires_success_and_exact_serving(self):
        for filename in ("storage.cache.sh", "storage.tiered-cache.sh"):
            source = (CASES / filename).read_text()
            function = "wait_ready() {" + section(source, "wait_ready() {", "\n}\n") + "\n}\n"
            for output, status, success in (("SERVING", 0, True),
                                            ("NOT_SERVING", 0, False),
                                            ("SERVING", 1, False)):
                with self.subTest(case=filename, output=output, status=status):
                    result = self.run_assertion(function + 'wait_ready "127.0.0.1:1"\n', output, status)
                    self.assertEqual(result.returncode == 0, success, result.stderr)

    def test_readonly_write_requires_specific_failed_rejection(self):
        source = (CASES / "storage.cache.sh").read_text()
        shell = section(source,
                        'echo "=== Test 7: tiered mode — object put returns error (writes not supported) ==="\n',
                        'kill "$TIERED_PID"')
        for output, status, success in (("server error: writes not supported", 1, True),
                                        ("server error: backend unavailable", 1, False),
                                        ("writes not supported", 0, False)):
            with self.subTest(output=output, status=status):
                result = self.run_assertion(shell, output, status)
                self.assertEqual(result.returncode == 0, success, result.stderr)

    def startup_assertion(self):
        source = (CASES / "storage.tiered-cache.sh").read_text()
        # The config is not executed by this assertion-only harness.
        return section(source, 'cat > "$TMPDIR/upstream-bad.yaml" <<EOF',
                       '\n# ============================================================\necho ""\necho "=== Test 12:').split("\nEOF\n", 1)[1]

    def test_startup_rejects_success_unrelated_errors_and_timeouts(self):
        for output, status, success in (("dial reader: connection refused", 1, True),
                                        ("bad unrelated config", 1, False),
                                        ("connection refused", 0, False),
                                        ("connection refused", 124, False)):
            with self.subTest(output=output, status=status):
                result = self.run_assertion(self.startup_assertion(), output, status)
                self.assertEqual(result.returncode == 0, success, result.stderr)

    def test_unexpectedly_running_server_is_bounded_failure(self):
        start = time.monotonic()
        result = self.run_assertion(self.startup_assertion(), "connection refused", hang=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("exit=124", result.stderr)
        self.assertLess(time.monotonic() - start, 8)


if __name__ == "__main__":
    unittest.main()
