package tarstream

import (
	stdtar "archive/tar"
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestHolesToExtents(t *testing.T) {
	ext := func(pairs ...int64) []extent {
		var out []extent
		for i := 0; i < len(pairs); i += 2 {
			out = append(out, extent{pairs[i], pairs[i+1]})
		}
		return out
	}
	cases := []struct {
		size   int64
		holes  []Hole
		want   []extent
		sparse bool
		err    bool
	}{
		{size: 100, holes: nil, sparse: false},
		{size: 100, holes: []Hole{{10, 0}}, sparse: false},
		{size: 100, holes: []Hole{{0, 100}}, want: nil, sparse: true},
		{size: 100, holes: []Hole{{0, 40}}, want: ext(40, 60), sparse: true},
		{size: 100, holes: []Hole{{60, 40}}, want: ext(0, 60), sparse: true},
		{size: 100, holes: []Hole{{40, 20}}, want: ext(0, 40, 60, 40), sparse: true},
		{size: 100, holes: []Hole{{60, 20}, {20, 20}}, want: ext(0, 20, 40, 20, 80, 20), sparse: true},
		{size: 100, holes: []Hole{{20, 20}, {40, 20}}, want: ext(0, 20, 60, 40), sparse: true},
		{size: 100, holes: []Hole{{20, 30}, {40, 20}}, err: true},
		{size: 100, holes: []Hole{{90, 20}}, err: true},
		{size: 100, holes: []Hole{{-1, 5}}, err: true},
	}
	for i, c := range cases {
		got, sparse, err := holesToExtents(c.size, c.holes)
		if c.err {
			if err == nil {
				t.Errorf("case %d: want error", i)
			}
			continue
		}
		if err != nil {
			t.Errorf("case %d: %v", i, err)
			continue
		}
		if sparse != c.sparse || len(got) != len(c.want) {
			t.Errorf("case %d: got %v/%v, want %v/%v", i, got, sparse, c.want, c.sparse)
			continue
		}
		for j := range got {
			if got[j] != c.want[j] {
				t.Errorf("case %d: extents %v, want %v", i, got, c.want)
				break
			}
		}
	}
}

// fixture: logical 3 MiB — "A"*8K at 0, hole, "B"*4K at 1M, trailing hole.
func fixture() ([]byte, []Hole) {
	const size = 3 << 20
	buf := make([]byte, size)
	for i := range 8192 {
		buf[i] = 'A'
	}
	for i := 1 << 20; i < (1<<20)+4096; i++ {
		buf[i] = 'B'
	}
	return buf, []Hole{
		{8192, (1 << 20) - 8192},
		{(1 << 20) + 4096, (3 << 20) - ((1 << 20) + 4096)},
	}
}

func mustWrite(t *testing.T, name string, logical []byte, holes []Hole) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteTo(&buf, name, bytes.NewReader(logical), holes); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// readerOnly hides Seek from a bytes.Reader, forcing the sequential path.
type readerOnly struct{ io.Reader }

func TestRoundTripSeek(t *testing.T) {
	logical, holes := fixture()
	archive := mustWrite(t, "disk/img.raw", logical, holes)
	if len(archive) > 64<<10 {
		t.Errorf("archive too large: %d", len(archive))
	}

	ts, err := ReadSeekFrom(bytes.NewReader(archive), "disk/img.raw")
	if err != nil {
		t.Fatal(err)
	}
	if ts.Name() != "disk/img.raw" || ts.Size() != int64(len(logical)) {
		t.Errorf("meta = %q/%d", ts.Name(), ts.Size())
	}
	gotHoles := ts.Holes()
	if len(gotHoles) != len(holes) {
		t.Fatalf("holes = %v, want %v", gotHoles, holes)
	}
	for i := range holes {
		if gotHoles[i] != holes[i] {
			t.Fatalf("holes = %v, want %v", gotHoles, holes)
		}
	}
	got, err := io.ReadAll(ts)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, logical) {
		t.Error("content mismatch (seek view)")
	}
}

func TestRoundTripSequential(t *testing.T) {
	logical, holes := fixture()
	archive := mustWrite(t, "disk/img.raw", logical, holes)

	ts, err := ReadFrom(readerOnly{bytes.NewReader(archive)}, "")
	if err != nil {
		t.Fatal(err)
	}
	if ts.Name() != "disk/img.raw" {
		t.Errorf("first-entry name = %q", ts.Name())
	}
	if len(ts.Holes()) != 2 {
		t.Errorf("holes = %v", ts.Holes())
	}
	got, err := io.ReadAll(ts)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, logical) {
		t.Error("content mismatch (sequential view)")
	}
}

func TestRoundTripPipe(t *testing.T) {
	logical, holes := fixture()
	archive := mustWrite(t, "x", logical, holes)
	pr, pw := io.Pipe()
	go func() {
		pw.Write(archive)
		pw.Close()
	}()
	ts, err := ReadFrom(pr, "x")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(ts)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, logical) {
		t.Error("content mismatch via pipe")
	}
}

func TestStdlibOracle(t *testing.T) {
	logical, holes := fixture()
	archive := mustWrite(t, "disk/img.raw", logical, holes)

	tr := stdtar.NewReader(bytes.NewReader(archive))
	hdr, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Name != "disk/img.raw" || hdr.Size != int64(len(logical)) {
		t.Errorf("stdlib sees %q/%d", hdr.Name, hdr.Size)
	}
	got, err := io.ReadAll(tr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, logical) {
		t.Error("stdlib logical content mismatch")
	}
	if _, err := tr.Next(); err != io.EOF {
		t.Errorf("expected single-entry archive, next = %v", err)
	}
}

func sparseSupported(t *testing.T, dir string) bool {
	t.Helper()
	p := filepath.Join(dir, "probe")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	f.Truncate(1 << 20)
	f.Close()
	var st syscall.Stat_t
	if err := syscall.Stat(p, &st); err != nil {
		t.Fatal(err)
	}
	os.Remove(p)
	return st.Blocks*512 < 1<<20
}

func gnuTar(t *testing.T) string {
	t.Helper()
	p, err := exec.LookPath("tar")
	if err != nil {
		t.Skip("no system tar")
	}
	out, err := exec.Command(p, "--version").Output()
	if err != nil || !strings.Contains(string(out), "GNU tar") {
		t.Skip("system tar is not GNU tar")
	}
	return p
}

func TestGNUExtractOracle(t *testing.T) {
	tarBin := gnuTar(t)
	dir := t.TempDir()
	if !sparseSupported(t, dir) {
		t.Skip("filesystem does not keep holes")
	}
	logical, holes := fixture()
	archive := filepath.Join(dir, "a.tar")
	if err := os.WriteFile(archive, mustWrite(t, "img.raw", logical, holes), 0o644); err != nil {
		t.Fatal(err)
	}
	xdir := filepath.Join(dir, "x")
	os.Mkdir(xdir, 0o755)
	if out, err := exec.Command(tarBin, "-xf", archive, "-C", xdir).CombinedOutput(); err != nil {
		t.Fatalf("gnu tar -x: %v: %s", err, out)
	}
	got, err := os.ReadFile(filepath.Join(xdir, "img.raw"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, logical) {
		t.Error("GNU-extracted content mismatch")
	}
	var st syscall.Stat_t
	if err := syscall.Stat(filepath.Join(xdir, "img.raw"), &st); err != nil {
		t.Fatal(err)
	}
	if st.Blocks*512 >= int64(len(logical)) {
		t.Errorf("GNU extracted dense: %d blocks", st.Blocks)
	}
}

// TestGNUCreateOracle: archives produced by `tar --format=posix
// --sparse` load with the exact hole map the filesystem reports.
func TestGNUCreateOracle(t *testing.T) {
	tarBin := gnuTar(t)
	dir := t.TempDir()
	if !sparseSupported(t, dir) {
		t.Skip("filesystem does not keep holes")
	}
	logical, holes := fixture()
	src := filepath.Join(dir, "img.raw")
	f, err := os.Create(src)
	if err != nil {
		t.Fatal(err)
	}
	// Materialize sparsely: write only the data runs.
	if _, err := f.WriteAt(logical[:8192], 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(logical[1<<20:(1<<20)+4096], 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(int64(len(logical))); err != nil {
		t.Fatal(err)
	}
	f.Close()

	archive := filepath.Join(dir, "g.tar")
	if out, err := exec.Command(tarBin, "--format=posix", "--sparse", "-cf", archive, "-C", dir, "img.raw").CombinedOutput(); err != nil {
		t.Fatalf("gnu tar -c: %v: %s", err, out)
	}
	af, err := os.Open(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer af.Close()
	ts, err := ReadSeekFrom(af, "img.raw")
	if err != nil {
		t.Fatal(err)
	}
	if ts.Size() != int64(len(logical)) {
		t.Errorf("size = %d", ts.Size())
	}
	got, err := io.ReadAll(ts)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, logical) {
		t.Error("content mismatch from GNU-created archive")
	}
	gotHoles := ts.Holes()
	if len(gotHoles) != len(holes) {
		t.Fatalf("holes = %v, want %v", gotHoles, holes)
	}
	for i := range holes {
		if gotHoles[i] != holes[i] {
			t.Fatalf("holes = %v, want %v", gotHoles, holes)
		}
	}
}

func TestLegacyGNUSparseRejected(t *testing.T) {
	tarBin := gnuTar(t)
	dir := t.TempDir()
	if !sparseSupported(t, dir) {
		t.Skip("filesystem does not keep holes")
	}
	src := filepath.Join(dir, "img.raw")
	f, _ := os.Create(src)
	f.WriteAt([]byte("data"), 0)
	f.Truncate(1 << 20)
	f.Close()
	archive := filepath.Join(dir, "old.tar")
	// Default GNU format: legacy binary sparse encoding ('S').
	if out, err := exec.Command(tarBin, "--format=gnu", "--sparse", "-cf", archive, "-C", dir, "img.raw").CombinedOutput(); err != nil {
		t.Fatalf("gnu tar -c: %v: %s", err, out)
	}
	af, err := os.Open(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer af.Close()
	if _, err := ReadSeekFrom(af, "img.raw"); !errors.Is(err, ErrUnsupportedEncoding) {
		t.Errorf("want ErrUnsupportedEncoding, got %v", err)
	}
}

func TestSeekMatrix(t *testing.T) {
	logical, holes := fixture()
	ts, err := ReadSeekFrom(bytes.NewReader(mustWrite(t, "x", logical, holes)), "x")
	if err != nil {
		t.Fatal(err)
	}
	readAt := func(off int64, n int) []byte {
		t.Helper()
		if _, err := ts.Seek(off, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(ts, buf); err != nil {
			t.Fatalf("read at %d: %v", off, err)
		}
		return buf
	}
	for _, c := range []struct{ off, n int64 }{
		{0, 16},                    // data head
		{8192 - 8, 16},             // data→hole boundary
		{1 << 19, 32},              // mid-hole
		{(1 << 20) - 8, 16},        // hole→data boundary
		{(1 << 20) + 4096 - 8, 16}, // data→trailing-hole boundary
		{3<<20 - 16, 16},           // tail
	} {
		got := readAt(c.off, int(c.n))
		if !bytes.Equal(got, logical[c.off:c.off+c.n]) {
			t.Errorf("read at %d mismatch", c.off)
		}
	}
	// SeekEnd / SeekCurrent.
	if pos, _ := ts.Seek(-8, io.SeekEnd); pos != 3<<20-8 {
		t.Errorf("SeekEnd pos = %d", pos)
	}
	if pos, _ := ts.Seek(4, io.SeekCurrent); pos != 3<<20-4 {
		t.Errorf("SeekCurrent pos = %d", pos)
	}
	// Past EOF reads EOF; negative errors.
	ts.Seek(10<<20, io.SeekStart)
	if _, err := ts.Read(make([]byte, 1)); err != io.EOF {
		t.Errorf("past-EOF read = %v", err)
	}
	if _, err := ts.Seek(-1, io.SeekStart); err == nil {
		t.Error("negative seek must fail")
	}
}

func TestDenseEmptyAllHole(t *testing.T) {
	// Dense.
	data := []byte("hello dense world")
	ts, err := ReadSeekFrom(bytes.NewReader(mustWrite(t, "d", data, nil)), "d")
	if err != nil {
		t.Fatal(err)
	}
	if ts.Holes() != nil {
		t.Errorf("dense holes = %v", ts.Holes())
	}
	if got, _ := io.ReadAll(ts); !bytes.Equal(got, data) {
		t.Error("dense mismatch")
	}
	// Empty.
	ts, err = ReadSeekFrom(bytes.NewReader(mustWrite(t, "e", nil, nil)), "e")
	if err != nil {
		t.Fatal(err)
	}
	if ts.Size() != 0 {
		t.Errorf("empty size = %d", ts.Size())
	}
	if got, _ := io.ReadAll(ts); len(got) != 0 {
		t.Error("empty not empty")
	}
	// All-hole.
	all := make([]byte, 1<<20)
	ts, err = ReadSeekFrom(bytes.NewReader(mustWrite(t, "h", all, []Hole{{0, 1 << 20}})), "h")
	if err != nil {
		t.Fatal(err)
	}
	if len(ts.Holes()) != 1 || ts.Holes()[0] != (Hole{0, 1 << 20}) {
		t.Errorf("all-hole map = %v", ts.Holes())
	}
	got, err := io.ReadAll(ts)
	if err != nil || !bytes.Equal(got, all) {
		t.Errorf("all-hole content mismatch: %v", err)
	}
}

func TestLookupAndNotFound(t *testing.T) {
	// Multi-entry archive built with the stdlib writer (dense entries).
	var buf bytes.Buffer
	tw := stdtar.NewWriter(&buf)
	for _, e := range []struct{ name, body string }{{"first", "111"}, {"second", "22222"}} {
		tw.WriteHeader(&stdtar.Header{Name: e.name, Mode: 0o644, Size: int64(len(e.body))})
		tw.Write([]byte(e.body))
	}
	tw.Close()

	ts, err := ReadSeekFrom(bytes.NewReader(buf.Bytes()), "second")
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := io.ReadAll(ts); string(got) != "22222" {
		t.Errorf("second = %q", got)
	}
	if _, err := ReadFrom(readerOnly{bytes.NewReader(buf.Bytes())}, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("want ErrNotFound, got %v", err)
	}
}

func TestWriteToValidation(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteTo(&buf, "x", bytes.NewReader(make([]byte, 100)), []Hole{{50, 100}}); err == nil {
		t.Error("out-of-bounds hole must fail")
	}
	if err := WriteTo(&buf, "", bytes.NewReader(nil), nil); err == nil {
		t.Error("empty name must fail")
	}
}

func TestWriteToDeterministic(t *testing.T) {
	logical, holes := fixture()
	a := mustWrite(t, "x", logical, holes)
	b := mustWrite(t, "x", logical, holes)
	if !bytes.Equal(a, b) {
		t.Error("two emissions differ")
	}
}
