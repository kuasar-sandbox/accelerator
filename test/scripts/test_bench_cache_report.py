"""Exercise generated bilingual reports against the current bench text schema."""
from pathlib import Path
import os
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("bench_cache_remote.sh")
BENCH = """mode=get concurrency=4 duration=1s value_size=262144
  ops:        100
  throughput: 100 ops/sec
  bandwidth:  25.0 MiB/sec
  latency:
    p50:   20 us
    p99:   80 us
    p99.9: 100 us
  errors:    2
  allocs:
    allocs/op: 7
    bytes/op: 512
"""
COUNTERS = """  -- bench-window counters --
  layer        type             hits     misses      fills end-flight     errors     hit%
  tier-0       ec                 75         25          9          1          3   75.00%
  origin       local              20          5          -          -          2   80.00%
  ec peers:
    s1         192.0.2.1:17070 hits=75 misses=25 errors=3 cancelled=0 fills=9 hit%=75.00
"""

class ReportTest(unittest.TestCase):
    def render(self, counters):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        (root / "bench-c4.log").write_text(BENCH + counters)
        env = {**os.environ, "RESULTS_DIR": str(root), "CONCS": "4",
               "SHARDS": "192.0.2.1 192.0.2.2 192.0.2.3 192.0.2.4 192.0.2.5",
               "ORIGIN_HOST": "192.0.2.6", "TIERED_HOST": "192.0.2.7",
               "BENCH_HOST": "192.0.2.8", "DURATION": "1s", "VALUE_SIZE": "262144"}
        # report only reads local logs; no deploy/start/bench/SSH action is invoked.
        result = subprocess.run(["bash", str(SCRIPT), "report"], env=env,
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        return root, [(root / name).read_text()
                      for name in ("README.md", "README_zh.md")]

    def test_complete_language_pair_and_current_counter_columns(self):
        root, (en, zh) = self.render(COUNTERS)
        selector = "[English](README.md) | [简体中文](README_zh.md)"
        for text in (en, zh):
            self.assertTrue(text.startswith(selector + "\n"))
            self.assertEqual(text.count("\n## "), 5)
            self.assertIn("| 4 | 100 | 100 ops/s | 25.0 MiB/s | 20 us | 80 us | 100 us | 2 | 7 | 512 |", text)
            # Column 7 is errors=3; hit% is column 8 after end-flight.
            self.assertIn("| 4 | 75.00% | 9 | 20 | 20.000% |", text)
            self.assertIn("hits=75 misses=25 errors=3 cancelled=0 fills=9", text)
            self.assertIn("--key-salt", text)
            self.assertIn("go tool pprof -top -cum", text)
            self.assertIn("bench-c4.cpu.pprof", text)
        self.assertIn("excluding errors", en)
        self.assertIn("Origin misses/errors are not included", en)
        self.assertIn("未计 origin misses/errors", zh)
        self.assertEqual(
            [line for line in en.splitlines() if line.startswith("| 4 |")],
            [line for line in zh.splitlines() if line.startswith("| 4 |")])

    def test_unavailable_cache_counters_are_not_reported_as_zero(self):
        _, texts = self.render("")
        for text in texts:
            self.assertIn("| 4 | ? | ? | ? | - |", text)

if __name__ == "__main__":
    unittest.main()
