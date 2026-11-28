//go:build compat

// Comparative performance tests against the xdelta3 reference. Both tools are
// invoked as subprocesses on identical files, so timings are apples-to-apples
// (each pays process startup + file I/O). The headline metric is delta size
// (compression quality), which is exact; wall-clock is reported as the best of
// several runs and is indicative. Run with:
//
//	go test -tags=compat -v -run Perf ./internal/vcdiff/
package vcdiff

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// perfData builds an 8 MiB source and a target derived from it with scattered
// edits, so COPY matching dominates (a realistic delta workload).
func perfData() (source, target []byte) {
	const size = 8 << 20
	source = make([]byte, size)
	// Deterministic xorshift fill — incompressible, forcing real matching work.
	x := uint64(0x9e3779b97f4a7c15)
	for i := range source {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		source[i] = byte(x)
	}
	target = append([]byte(nil), source...)
	// 32 scattered overwrites + a couple of inserts.
	for k := 0; k < 32; k++ {
		off := (k*251 + 1000) * (size / 9000)
		if off+16 < len(target) {
			copy(target[off:off+16], []byte("EDIT-BLOCK-XXXXX"))
		}
	}
	target = append(target[:size/2], append([]byte("--inserted-region--"), target[size/2:]...)...)
	return
}

func timeBest(t *testing.T, runs int, name string, args ...string) time.Duration {
	t.Helper()
	best := time.Duration(1) << 62
	for i := 0; i < runs; i++ {
		start := time.Now()
		if out, err := exec.Command(name, args...).CombinedOutput(); err != nil {
			t.Fatalf("%s %v: %v\n%s", name, args, err, out)
		}
		if d := time.Since(start); d < best {
			best = d
		}
	}
	return best
}

func TestPerfVsXdelta3(t *testing.T) {
	requireXdelta3(t)
	dir := t.TempDir()

	// Build our CLI so both implementations run as subprocesses (fair timing).
	bin := filepath.Join(dir, "vcdiff")
	if out, err := exec.Command("go", "build", "-o", bin, "github.com/go-deltasync/vcdiff/cmd/vcdiff").CombinedOutput(); err != nil {
		t.Skipf("go build failed: %v\n%s", err, out)
	}

	source, target := perfData()
	srcPath := filepath.Join(dir, "src")
	tgtPath := filepath.Join(dir, "tgt")
	if err := os.WriteFile(srcPath, source, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tgtPath, target, 0o644); err != nil {
		t.Fatal(err)
	}

	const runs = 3
	goDelta := filepath.Join(dir, "go.vcdiff")
	xdDelta := filepath.Join(dir, "xd.vcdiff")
	goOut := filepath.Join(dir, "go.out")
	xdOut := filepath.Join(dir, "xd.out")

	goEnc := timeBest(t, runs, bin, "encode", "-s", srcPath, tgtPath, goDelta)
	xdEnc := timeBest(t, runs, "xdelta3", "-e", "-S", "-f", "-s", srcPath, tgtPath, xdDelta)
	goDec := timeBest(t, runs, bin, "decode", "-s", srcPath, goDelta, goOut)
	xdDec := timeBest(t, runs, "xdelta3", "-d", "-f", "-s", srcPath, xdDelta, xdOut)

	// Both reconstructions must be correct.
	for _, p := range []string{goOut, xdOut} {
		got, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(target) {
			t.Fatalf("%s reconstruction mismatch", p)
		}
	}

	goSize := fileSize(t, goDelta)
	xdSize := fileSize(t, xdDelta)
	mb := float64(len(target)) / (1 << 20)

	t.Logf("\n"+
		"comparative performance (target %.1f MiB, best of %d runs, subprocess wall-clock)\n"+
		"  %-10s %12s %12s %14s\n"+
		"  %-10s %12s %12s %14d\n"+
		"  %-10s %12s %12s %14d\n",
		mb, runs,
		"impl", "encode", "decode", "delta bytes",
		"go-vcdiff", fmtRate(goEnc, mb), fmtRate(goDec, mb), goSize,
		"xdelta3 -S", fmtRate(xdEnc, mb), fmtRate(xdDec, mb), xdSize,
	)
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

func fmtRate(d time.Duration, mb float64) string {
	return fmt.Sprintf("%.0fms %.0fMB/s", float64(d.Milliseconds()), mb/d.Seconds())
}
