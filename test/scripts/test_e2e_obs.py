#!/usr/bin/env python3
"""OBS cleanup boundaries only; no remote credentials, service or E2E claims."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
CASE = ROOT / 'test/e2e/cases/storage.obs.sh'


class ObsCleanupTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='obs-cleanup-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.lib = self.root / 'lib'
        self.lib.mkdir()
        (self.lib / 'common.sh').write_text('''require_command() { command -v "$1" >/dev/null; }
require_binary() { test -x "$BIN/$1"; }
''')
        for name in ('manifest-ctl', 'flatten-ctl'):
            path = self.bin / name
            path.write_text('#!/bin/sh\nexit 97\n')
            path.chmod(0o755)
        path = self.bin / 'store-ctl'
        path.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
config = json.loads(pathlib.Path(sys.argv[sys.argv.index('--config') + 1]).read_text())
prefix = config["obs"]["prefix"]
with pathlib.Path(os.environ['OBS_TEST_CALLS']).open('a') as out:
    out.write(json.dumps({'operation': sys.argv[1], 'prefix': prefix}) + '\\n')
if sys.argv[1] == 'init':
    print('injected partial initialization failure', file=sys.stderr)
    sys.exit(23)
if sys.argv[1] == 'purge':
    print('owned-prefix cleanup diagnostic', file=sys.stderr)
    sys.exit(int(os.environ['OBS_TEST_PURGE_STATUS']))
sys.exit(98)
''')
        path.chmod(0o755)
        self.calls = self.root / 'calls.jsonl'

    def run_boundary(self, status=0, purge_status=0, *, before_init=False, fail_init=False,
                     parent_prefix='parent/owned-tests/'):
        source = CASE.read_text()
        if not fail_init:
            marker = ('echo "==> store-ctl init --generation G1"' if before_init else
                      'OBS_AK="$OBS_AK" OBS_SK="$OBS_SK" \\\n    "$BIN/store-ctl" init')
            self.assertIn(marker, source)
            source = source.split(marker, 1)[0] + '\nexit "$OBS_TEST_STATUS"\n'
        script = self.root / 'case.sh'
        script.write_text(source)
        env = dict(os.environ, BIN=str(self.bin), E2E_LIB=str(self.lib), TMPDIR=str(self.root),
                   OBS_BUCKET='fixture', OBS_ENDPOINT='https://unused.invalid',
                   OBS_AK='fixture-only', OBS_SK='fixture-only', OBS_PREFIX=parent_prefix,
                   OBS_TEST_STATUS=str(status), OBS_TEST_PURGE_STATUS=str(purge_status),
                   OBS_TEST_CALLS=str(self.calls))
        env.pop('E2E_KEEP', None)
        result = subprocess.run(['bash', str(script)], env=env, capture_output=True, text=True, timeout=5)
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()] if self.calls.exists() else []
        return result, calls

    def test_failure_before_remote_init_does_not_purge(self):
        result, calls = self.run_boundary(status=19, before_init=True)
        self.assertEqual(result.returncode, 19, result.stderr)
        self.assertEqual(calls, [])

    def test_success_and_business_failure_both_purge_owned_prefix(self):
        for status in (0, 23):
            self.calls.unlink(missing_ok=True)
            with self.subTest(status=status):
                result, calls = self.run_boundary(status=status)
                self.assertEqual(result.returncode, status, result.stderr)
                self.assertEqual([row['operation'] for row in calls], ['purge'])
                self.assertTrue(calls[0]['prefix'].startswith('parent/owned-tests/e2e-obs-'))
                self.assertTrue(calls[0]['prefix'].endswith('/'))
                self.assertEqual(list(self.root.glob('e2e-obs-*')), [])

    def test_partial_init_failure_still_attempts_cleanup(self):
        result, calls = self.run_boundary(fail_init=True)
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertEqual([row['operation'] for row in calls], ['init', 'purge'])
        self.assertEqual(calls[0]['prefix'], calls[1]['prefix'])

    def test_purge_failure_is_not_green_and_preserves_original_failure(self):
        for status, expected in ((0, 1), (23, 23)):
            self.calls.unlink(missing_ok=True)
            with self.subTest(status=status):
                result, calls = self.run_boundary(status=status, purge_status=22)
                self.assertEqual(result.returncode, expected, result.stderr)
                self.assertIn('remote cleanup failed; residual prefix:', result.stderr)
                self.assertIn('owned-prefix cleanup diagnostic', result.stderr)
                self.assertIn('kept work dir:', result.stdout)
                self.assertEqual([row['operation'] for row in calls], ['purge'])

    def test_explicit_parent_never_reuses_another_runs_prefix(self):
        _, first = self.run_boundary()
        self.calls.unlink()
        _, second = self.run_boundary()
        self.assertNotEqual(first[0]['prefix'], second[0]['prefix'])
        self.assertNotEqual(first[0]['prefix'], 'parent/owned-tests/')

    def test_special_characters_cannot_remove_owned_suffix_from_configuration(self):
        for parent in ('parent # literal/', 'parent: "quoted"/', 'parent\\literal/', 'parent\nline/'):
            self.calls.unlink(missing_ok=True)
            with self.subTest(parent=parent):
                result, calls = self.run_boundary(fail_init=True, parent_prefix=parent)
                self.assertEqual(result.returncode, 23, result.stderr)
                self.assertEqual([row['operation'] for row in calls], ['init', 'purge'])
                prefix = calls[0]['prefix']
                self.assertEqual(prefix, calls[1]['prefix'])
                self.assertTrue(prefix.startswith(parent.rstrip('/') + '/e2e-obs-'), prefix)
                self.assertTrue(prefix.endswith('/'), prefix)


if __name__ == '__main__':
    unittest.main(verbosity=2)
