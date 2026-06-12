package tarstream

import (
	stdtar "archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/sparse"
)

// fixture: logical 3 MiB — "A"*8K at 0, hole, "B"*4K at 1M, trailing hole.
func fixture() ([]byte, []sparse.Extent) {
	const size = 3 << 20
	buf := make([]byte, size)
	for i := range 8192 {
		buf[i] = 'A'
	}
	for i := 1 << 20; i < (1<<20)+4096; i++ {
		buf[i] = 'B'
	}
	return buf, []sparse.Extent{
		{Offset: 8192, Size: (1 << 20) - 8192},
		{Offset: (1 << 20) + 4096, Size: (3 << 20) - ((1 << 20) + 4096)},
	}
}

func mustWrite(t *testing.T, name string, logical []byte, holes []sparse.Extent) []byte {
	t.Helper()
	src, err := sparse.NewSource(bytes.NewReader(logical), uint64(len(logical)), holes)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := WriteTo(context.Background(), &buf, name, src); err != nil {
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
	ts, err = ReadSeekFrom(bytes.NewReader(mustWrite(t, "h", all, []sparse.Extent{{Offset: 0, Size: 1 << 20}})), "h")
	if err != nil {
		t.Fatal(err)
	}
	if len(ts.Holes()) != 1 || ts.Holes()[0] != (sparse.Extent{Offset: 0, Size: 1 << 20}) {
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
	if err := WriteTo(context.Background(), &buf, "", sparse.Dense(bytes.NewReader(nil), 0)); err == nil {
		t.Error("empty name must fail")
	}
}

// zeroRunSource fakes a manifest-backed source: [0,4K) Data 'A',
// [4K,1M) Zero, [1M,2M) Hole. ReadAt outside the data run is an error
// — Zero runs must be synthesized by the writer, never read.
type zeroRunSource struct{}

func (zeroRunSource) Size() uint64 { return 2 << 20 }

func (zeroRunSource) RunAt(off, limit uint64) (sparse.RunKind, uint64, error) {
	const size = 2 << 20
	if off >= size {
		return 0, 0, io.EOF
	}
	limEnd := min(off+limit, uint64(size))
	switch {
	case off < 4096:
		return sparse.Data, min(4096, limEnd), nil
	case off < 1<<20:
		return sparse.Zero, min(1<<20, limEnd), nil
	default:
		return sparse.Hole, limEnd, nil
	}
}

func (zeroRunSource) ReadAt(_ context.Context, buf []byte, off uint64) (int, error) {
	if off+uint64(len(buf)) > 4096 {
		return 0, fmt.Errorf("unexpected ReadAt [%d,+%d): zero/hole runs must not be read", off, len(buf))
	}
	for i := range buf {
		buf[i] = 'A'
	}
	return len(buf), nil
}

// TestWriteToZeroRuns: Zero runs cross the boundary as data (literal
// zero bytes on the wire), never as holes; only Hole runs land in the
// sparse map.
func TestWriteToZeroRuns(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteTo(context.Background(), &buf, "z", zeroRunSource{}); err != nil {
		t.Fatal(err)
	}

	ts, err := ReadSeekFrom(bytes.NewReader(buf.Bytes()), "z")
	if err != nil {
		t.Fatal(err)
	}
	holes := ts.Holes()
	if len(holes) != 1 || holes[0] != (sparse.Extent{Offset: 1 << 20, Size: 1 << 20}) {
		t.Fatalf("holes = %v, want exactly the [1M,2M) hole", holes)
	}
	got, err := io.ReadAll(ts)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 2<<20)
	for i := range 4096 {
		want[i] = 'A'
	}
	if !bytes.Equal(got, want) {
		t.Error("logical content mismatch (zeros must read back as data)")
	}
	// The zero run is stored on the wire: the packed entry must be at
	// least 1 MiB (4K data + ~1M zeros), proving zeros were not
	// encoded as holes.
	if len(buf.Bytes()) < 1<<20 {
		t.Errorf("archive only %d bytes: zero run was dropped from the wire", buf.Len())
	}
}

func TestSourceFrom(t *testing.T) {
	logical, holes := fixture()
	archive := mustWrite(t, "img", logical, holes)
	ctx := context.Background()

	check := func(t *testing.T, src sparse.Source, name string, random bool) {
		t.Helper()
		if name != "img" {
			t.Fatalf("name = %q", name)
		}
		if src.Size() != uint64(len(logical)) {
			t.Fatalf("size = %d", src.Size())
		}
		// Run sweep mirrors the hole map; Zero never appears.
		type run struct {
			kind sparse.RunKind
			end  uint64
		}
		var runs []run
		for off := uint64(0); ; {
			kind, end, err := src.RunAt(off, src.Size())
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("RunAt(%d): %v", off, err)
			}
			if kind == sparse.Zero {
				t.Fatalf("tar source returned Zero at %d", off)
			}
			runs = append(runs, run{kind, end})
			off = end
		}
		want := []run{
			{sparse.Data, 8192},
			{sparse.Hole, 1 << 20},
			{sparse.Data, (1 << 20) + 4096},
			{sparse.Hole, 3 << 20},
		}
		if len(runs) != len(want) {
			t.Fatalf("runs = %v, want %v", runs, want)
		}
		for i := range want {
			if runs[i] != want[i] {
				t.Fatalf("run[%d] = %v, want %v", i, runs[i], want[i])
			}
		}

		// Monotone reads across the data runs.
		head := make([]byte, 8192)
		if _, err := src.ReadAt(ctx, head, 0); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(head, logical[:8192]) {
			t.Fatal("head mismatch")
		}
		mid := make([]byte, 4096)
		if _, err := src.ReadAt(ctx, mid, 1<<20); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(mid, logical[1<<20:(1<<20)+4096]) {
			t.Fatal("mid mismatch")
		}
		// Backward read: random sources serve it, one-pass ones refuse.
		_, err := src.ReadAt(ctx, head, 0)
		if random && err != nil {
			t.Fatalf("random source backward read: %v", err)
		}
		if !random && err == nil {
			t.Fatal("one-pass source accepted a backward read")
		}
	}

	src, name, err := SourceFrom(readerOnly{bytes.NewReader(archive)}, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Run("one-pass", func(t *testing.T) { check(t, src, name, false) })

	src, name, err = SourceFrom(bytes.NewReader(archive), "img")
	if err != nil {
		t.Fatal(err)
	}
	t.Run("random", func(t *testing.T) { check(t, src, name, true) })
}

// TestReadFromUpgrade: ReadFrom over a seekable source returns a view
// that also implements ReadSeeker; over a plain reader it does not.
func TestReadFromUpgrade(t *testing.T) {
	logical, holes := fixture()
	archive := mustWrite(t, "x", logical, holes)

	ts, err := ReadFrom(bytes.NewReader(archive), "x")
	if err != nil {
		t.Fatal(err)
	}
	rs, ok := ts.(ReadSeeker)
	if !ok {
		t.Fatal("seekable source must yield a ReadSeeker view")
	}
	if _, err := rs.Seek(1<<20, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(rs, buf); err != nil || !bytes.Equal(buf, []byte("BBBB")) {
		t.Errorf("seek-read = %q, %v", buf, err)
	}

	ts, err = ReadFrom(readerOnly{bytes.NewReader(archive)}, "x")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ts.(io.Seeker); ok {
		t.Error("plain reader source must not yield a Seeker view")
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

// TestFileRoundTrip: ProbeHoles + NewSource + WriteTo over a real
// sparse file round-trips both content and hole map.
func TestFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if !sparseSupported(t, dir) {
		t.Skip("filesystem does not keep holes")
	}
	logical, holes := fixture()
	p := filepath.Join(dir, "img.raw")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	f.WriteAt(logical[:8192], 0)
	f.WriteAt(logical[1<<20:(1<<20)+4096], 1<<20)
	f.Truncate(int64(len(logical)))

	probed, err := sparse.ProbeHoles(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(probed) != len(holes) {
		t.Fatalf("probed holes = %v, want %v", probed, holes)
	}
	for i := range holes {
		if probed[i] != holes[i] {
			t.Fatalf("probed holes = %v, want %v", probed, holes)
		}
	}
	src, err := sparse.NewSource(f, uint64(len(logical)), probed)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := WriteTo(context.Background(), &buf, "img.raw", src); err != nil {
		t.Fatal(err)
	}
	ts, err := ReadSeekFrom(bytes.NewReader(buf.Bytes()), "")
	if err != nil {
		t.Fatal(err)
	}
	all, err := io.ReadAll(ts)
	if err != nil || !bytes.Equal(all, logical) {
		t.Errorf("probe→write→read mismatch: %v", err)
	}
	gotHoles := ts.Holes()
	for i := range holes {
		if gotHoles[i] != holes[i] {
			t.Fatalf("round-trip holes = %v, want %v", gotHoles, holes)
		}
	}
}
