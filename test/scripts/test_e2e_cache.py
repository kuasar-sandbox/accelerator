#!/usr/bin/env python3
"""Source regression tests for cache E2E assertions, not product E2E."""
import hashlib
import json
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


class MembershipLoadTests(unittest.TestCase):
    def exercise(self, mode, calls=1, existing_prefix=False, filename="storage.cache-membership.sh"):
        source = (CASES / filename).read_text()
        helper = "load_hash_via_tiered" if filename == "storage.cache-membership.sh" else "wait_manifest_load"
        functions = "\n".join(name + "() {" + section(source, name + "() {", "\n}\n") + "\n}"
                              for name in ("payload_hash", helper))
        with tempfile.TemporaryDirectory(prefix="membership-load-") as directory:
            root = Path(directory)
            binary = root / "manifest-ctl"
            # Model only the CLI's exclusive-create contract and failure modes.
            # The actual shell helper and tar payload hashing run unchanged.
            binary.write_text("""#!/usr/bin/env python3
import io, json, os, pathlib, sys, tarfile
out = pathlib.Path(sys.argv[sys.argv.index('--output') + 1])
record = pathlib.Path(os.environ['LOAD_CALLS'])
first = not record.exists()
with record.open('a') as log:
    log.write(json.dumps(str(out)) + '\\n')
try:
    stream = out.open('xb')
except FileExistsError:
    print('output already exists', file=sys.stderr)
    sys.exit(17)
with stream:
    mode = os.environ['LOAD_MODE']
    if mode == 'fail' or (mode == 'partial-first' and first):
        stream.write(b'partial')
        print('partial load failed', file=sys.stderr)
        sys.exit(23)
    if mode == 'corrupt' or (mode == 'corrupt-first' and first):
        stream.write(b'not a tar archive')
    else:
        with tarfile.open(fileobj=stream, mode='w') as archive:
            info = tarfile.TarInfo('image')
            payload = b'wrong' if mode == 'wrong-first' and first else b'payload'
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
""")
            binary.chmod(0o755)
            prefix = root / "output"
            if existing_prefix:
                prefix.write_bytes(b'previous unrelated data')
            environment = dict(os.environ, BIN=directory, TIERED_PORT="1",
                               LOAD_CALLS=str(root / "calls"), LOAD_MODE=mode,
                               LOAD_PREFIX=str(prefix), EXPECTED_HASH=hashlib.sha256(b"payload").hexdigest())
            invocation = ('load_hash_via_tiered key \"$LOAD_PREFIX\" fixture\n' if helper == 'load_hash_via_tiered' else
                          'wait_manifest_load fixture \"$LOAD_PREFIX\" key \"$EXPECTED_HASH\"\n')
            program = ('set -euo pipefail\nsleep() { :; }\n'
                       'accel_cfg_for_cache() { echo fixture; }\n' + functions + '\n' +
                       invocation * calls)
            result = subprocess.run(['bash', '-c', program], env=environment,
                                    capture_output=True, text=True, timeout=10)
            outputs = [Path(json.loads(line)) for line in (root / 'calls').read_text().splitlines()]
            self.assertEqual(len(outputs), len(set(outputs)), 'attempts reused an exclusive output path')
            if existing_prefix:
                self.assertEqual(prefix.read_bytes(), b'previous unrelated data')
            self.assertTrue(all(path.exists() for path in outputs), 'attempt outputs were overwritten or removed')
            return result, len(outputs)

    def test_repeated_successful_probes_have_distinct_outputs(self):
        result, count = self.exercise('success', calls=2)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(count, 2)
        self.assertEqual(result.stdout.splitlines(), [hashlib.sha256(b'payload').hexdigest()] * 2)

    def test_existing_prefix_is_preserved(self):
        result, count = self.exercise('success', existing_prefix=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(count, 1)

    def test_partial_business_read_is_not_retried_green(self):
        result, count = self.exercise('partial-first')
        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertEqual(count, 1)
        self.assertEqual(result.stdout, '')
        self.assertIn('partial load failed', result.stderr)

    def test_failed_or_corrupt_loads_cannot_pass(self):
        for mode in ('fail', 'corrupt'):
            with self.subTest(mode=mode):
                result, count = self.exercise(mode)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertEqual(count, 1)
                self.assertEqual(result.stdout, '')
                self.assertIn('did not load/decode through tiered cache on its single attempt', result.stderr)
                self.assertIn('read.', result.stderr)


class MembershipCommitTests(unittest.TestCase):
    def exercise(self, log, peers, *, epoch=2, alive=True, info_status=0):
        source = (CASES / "storage.cache-membership.sh").read_text()
        function = "wait_membership() {" + section(source, "wait_membership() {", "\n}\n") + "\n}\n"
        with tempfile.TemporaryDirectory(prefix="membership-commit-") as directory:
            root = Path(directory)
            (root / "tiered.log").write_text(log)
            binary = root / "cache-ctl"
            binary.write_text('#!/bin/bash\n'
                              'printf "%s\\n" "$*" >> "$INFO_CALLS"\n'
                              'printf "%s\\n" "$INFO_JSON"\n'
                              'exit "$INFO_STATUS"\n')
            binary.chmod(0o755)
            environment = dict(os.environ, BIN=directory, TMPDIR=directory,
                               TIERED_HEALTH_PORT="1234", INFO_CALLS=str(root / "calls"),
                               INFO_JSON=json.dumps({"tiered": {"tiers": [{"peers": [{"id": peer} for peer in peers]}]}}),
                               INFO_STATUS=str(info_status), DAEMON_ALIVE="1" if alive else "0")
            # Check actual Bash process liveness. The exited child belongs to
            # this test, while the live case uses the current shell itself.
            program = ('set -euo pipefail\nsleep() { :; }\n'
                       'if [ "$DAEMON_ALIVE" = 1 ]; then TIERED_PID=$$; '
                       'else (exit 0) & TIERED_PID=$!; wait "$TIERED_PID"; fi\n' +
                       function + f'wait_membership {epoch} "s2,s3,s4,s5,s6"\n')
            result = subprocess.run(['bash', '-c', program], env=environment,
                                    capture_output=True, text=True, timeout=10)
            calls = (root / "calls").read_text().splitlines() if (root / "calls").exists() else []
            self.assertTrue(all(call == 'info --endpoint 127.0.0.1:1234 --json' for call in calls))
            return result, calls

    def test_committed_epoch_and_exact_peers_are_required_together(self):
        for epoch in (2, 3):
            with self.subTest(epoch=epoch):
                result, calls = self.exercise(f'ec: membership epoch {epoch - 1} → {epoch} committed\n',
                                             ['s6', 's2', 's5', 's3', 's4'], epoch=epoch)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(len(calls), 1)

    def test_missing_or_wrong_epoch_cannot_use_a_ready_peer_list(self):
        for log in ('', 'ec: membership epoch 0 → 1 committed\n', 'ec: membership epoch 2 → 3 committed\n'):
            with self.subTest(log=log):
                result, calls = self.exercise(log, ['s2', 's3', 's4', 's5', 's6'])
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(calls, [])

    def test_committed_log_cannot_accept_old_peers(self):
        result, calls = self.exercise('ec: membership epoch 1 → 2 committed\n', ['s1', 's2', 's3', 's4', 's5'])
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(calls), 100)
        self.assertIn('s1', result.stderr)

    def test_dead_daemon_cannot_reuse_committed_state(self):
        result, calls = self.exercise('ec: membership epoch 1 → 2 committed\n', ['s2', 's3', 's4', 's5', 's6'], alive=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(calls, [])

    def test_failed_info_command_cannot_pass_with_expected_json(self):
        result, calls = self.exercise('ec: membership epoch 1 → 2 committed\n', ['s2', 's3', 's4', 's5', 's6'], info_status=1)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(calls), 100)


class FillReadinessTests(unittest.TestCase):
    # Exercise the real helper bodies with a CLI that implements exclusive
    # creation and controlled partial/corrupt output, without claiming E2E.
    exercise = MembershipLoadTests.exercise

    def test_partial_fill_uses_a_fresh_probe_and_preserves_existing_output(self):
        for filename in ("storage.cache.sh", "storage.tiered-cache.sh"):
            with self.subTest(case=filename):
                result, count = self.exercise('partial-first', existing_prefix=True, filename=filename)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(count, 2)

    def test_repeated_observations_have_distinct_outputs(self):
        for filename in ("storage.cache.sh", "storage.tiered-cache.sh"):
            with self.subTest(case=filename):
                result, count = self.exercise('success', calls=2, filename=filename)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(count, 2)

    def test_completed_corrupt_or_wrong_payload_fails_without_retry(self):
        for filename in ("storage.cache.sh", "storage.tiered-cache.sh"):
            for mode in ('corrupt-first', 'wrong-first'):
                with self.subTest(case=filename, mode=mode):
                    result, count = self.exercise(mode, filename=filename)
                    self.assertNotEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(count, 1)
                    self.assertIn('completed fill probe returned invalid or mismatched payload', result.stderr)

    def test_exhausted_observation_retains_first_and_last_failure(self):
        for filename in ("storage.cache.sh", "storage.tiered-cache.sh"):
            with self.subTest(case=filename):
                result, count = self.exercise('fail', filename=filename)
                self.assertNotEqual(result.returncode, 0, result.stderr)
                self.assertEqual(count, 100)
                self.assertIn('attempt-1.stderr', result.stderr)
                self.assertIn('attempt-100.stderr', result.stderr)
                self.assertIn('partial load failed', result.stderr)


if __name__ == "__main__":
    unittest.main()
