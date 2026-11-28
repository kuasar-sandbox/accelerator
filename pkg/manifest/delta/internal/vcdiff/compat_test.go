//go:build compat

// Cross-implementation interoperability tests against the xdelta3 CLI
// (https://github.com/jmacd/xdelta), the de-facto reference VCDIFF (RFC 3284)
// implementation. Run with: go test -tags=compat ./internal/vcdiff/...
// The whole file is skipped if `xdelta3` is not on PATH.
//
// xdelta3 is invoked with -S (secondary compression disabled) so it emits the
// plain RFC 3284 layout this package targets, rather than its djw/lzma/fgk
// secondary-compressed streams (which set the VCD_DECOMPRESS bit). xdelta3's
// default application header and adler32 window checksum are handled by our
// decoder (skipped / verified respectively).
//
// We use xdelta3 rather than Google's open-vcdiff because open-vcdiff is
// archived and unmaintained, whereas xdelta3 is the actively maintained
// reference and packaged by major distributions.
package vcdiff

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func requireXdelta3(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("xdelta3"); err != nil {
		t.Skip("`xdelta3` not found on PATH; skipping cross-impl compat")
	}
}

func runXdelta3(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("xdelta3", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("xdelta3 %v failed: %v\n%s", args, err, out)
	}
}

// makeSourceAndTarget writes a dictionary (source) and a derived, edited target
// to dir and returns their paths and contents.
func makeSourceAndTarget(t *testing.T, dir string) (srcPath, tgtPath string, source, target []byte) {
	t.Helper()
	source = bytes.Repeat([]byte("The quick brown fox jumps over the lazy dog.\n"), 600)
	target = append([]byte{}, source[:12_000]...)
	target = append(target, []byte("--cross-impl-inserted-bytes--")...)
	target = append(target, source[15_000:]...)

	srcPath = filepath.Join(dir, "source.bin")
	tgtPath = filepath.Join(dir, "target.bin")
	if err := os.WriteFile(srcPath, source, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tgtPath, target, 0o644); err != nil {
		t.Fatal(err)
	}
	return
}

// TestGoEncodeXdeltaDecode: Go computes the delta, xdelta3 applies it. Exercises
// the reference decoder reading a delta our encoder produced.
func TestGoEncodeXdeltaDecode(t *testing.T) {
	requireXdelta3(t)
	dir := t.TempDir()
	srcPath, _, source, target := makeSourceAndTarget(t, dir)

	deltaPath := filepath.Join(dir, "go.vcdiff")
	df, err := os.Create(deltaPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := Encode(source, target, df); err != nil {
		t.Fatal(err)
	}
	df.Close()

	outPath := filepath.Join(dir, "xd.out")
	runXdelta3(t, "-d", "-f", "-s", srcPath, deltaPath, outPath)

	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, target) {
		t.Fatal("Go-encode / xdelta3-decode reconstruction mismatch")
	}
}

// TestXdeltaEncodeGoDecode: xdelta3 computes the delta, Go applies it. Exercises
// our decoder reading xdelta3's standard (no-secondary) output.
func TestXdeltaEncodeGoDecode(t *testing.T) {
	requireXdelta3(t)
	dir := t.TempDir()
	srcPath, tgtPath, source, target := makeSourceAndTarget(t, dir)

	deltaPath := filepath.Join(dir, "xd.vcdiff")
	// -S disables secondary compression so the delta is plain RFC 3284.
	runXdelta3(t, "-e", "-S", "-f", "-s", srcPath, tgtPath, deltaPath)

	delta, err := os.Open(deltaPath)
	if err != nil {
		t.Fatal(err)
	}
	defer delta.Close()

	var out bytes.Buffer
	if _, err := Decode(source, delta, &out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(out.Bytes(), target) {
		t.Fatal("xdelta3-encode / Go-decode reconstruction mismatch")
	}
}
