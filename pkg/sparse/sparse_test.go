package sparse

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// image builds a dense byte image of size n with data bytes 'A' in the
// given extents and zeros elsewhere (what an *os.File over a sparse
// file would read).
func image(n uint64, data ...Extent) []byte {
	buf := make([]byte, n)
	for _, e := range data {
		for i := e.Offset; i < e.Offset+e.Size; i++ {
			buf[i] = 'A'
		}
	}
	return buf
}

func TestNewSourceNormalization(t *testing.T) {
	const size = 1 << 20
	ra := bytes.NewReader(image(size))

	// Unsorted + adjacent holes merge; zero-size dropped.
	src, err := NewSource(ra, size, []Extent{
		{Offset: 8192, Size: 4096},
		{Offset: 4096, Size: 4096},
		{Offset: 100, Size: 0},
	})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	kind, end, err := src.RunAt(4096, size)
	if err != nil || kind != Hole || end != 12288 {
		t.Fatalf("merged hole run = (%v, %d, %v), want (Hole, 12288, nil)", kind, end, err)
	}

	if _, err := NewSource(ra, size, []Extent{{0, 4096}, {2048, 4096}}); err == nil {
		t.Fatal("overlapping holes accepted")
	}
	if _, err := NewSource(ra, size, []Extent{{size - 1024, 2048}}); err == nil {
		t.Fatal("out-of-bounds hole accepted")
	}
}

func TestStaticSourceRuns(t *testing.T) {
	// [0,8K) data, [8K,1M) hole, [1M,1M+4K) data, [1M+4K,3M) hole
	const size = 3 << 20
	holes := []Extent{{8192, (1 << 20) - 8192}, {(1 << 20) + 4096, (2 << 20) - 4096}}
	src, err := NewSource(bytes.NewReader(image(size, Extent{0, 8192}, Extent{1 << 20, 4096})), size, holes)
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}

	type run struct {
		kind RunKind
		end  uint64
	}
	var got []run
	for off := uint64(0); ; {
		kind, end, err := src.RunAt(off, size)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("RunAt(%d): %v", off, err)
		}
		got = append(got, run{kind, end})
		off = end
	}
	want := []run{
		{Data, 8192},
		{Hole, 1 << 20},
		{Data, (1 << 20) + 4096},
		{Hole, 3 << 20},
	}
	if len(got) != len(want) {
		t.Fatalf("runs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("run[%d] = %v, want %v", i, got[i], want[i])
		}
	}

	// limit clamps the run end.
	if _, end, _ := src.RunAt(8192, 100); end != 8292 {
		t.Fatalf("limited hole run end = %d, want 8292", end)
	}

	// ReadAt spans data, clips at EOF.
	buf := make([]byte, 8192)
	if n, err := src.ReadAt(context.Background(), buf, 0); n != 8192 || err != nil {
		t.Fatalf("ReadAt data = (%d, %v)", n, err)
	}
	if buf[0] != 'A' || buf[8191] != 'A' {
		t.Fatal("data bytes wrong")
	}
	if n, err := src.ReadAt(context.Background(), buf, size-100); n != 100 || err != io.EOF {
		t.Fatalf("clipped ReadAt = (%d, %v), want (100, EOF)", n, err)
	}
	if n, err := src.ReadAt(context.Background(), buf, size); n != 0 || err != io.EOF {
		t.Fatalf("past-EOF ReadAt = (%d, %v), want (0, EOF)", n, err)
	}
}

func TestDenseSource(t *testing.T) {
	payload := []byte(strings.Repeat("x", 1000))
	src := Dense(bytes.NewReader(payload), 1000)

	if kind, end, err := src.RunAt(0, 1<<20); kind != Data || end != 1000 || err != nil {
		t.Fatalf("RunAt = (%v, %d, %v), want (Data, 1000, nil)", kind, end, err)
	}
	if kind, end, err := src.RunAt(10, 20); kind != Data || end != 30 || err != nil {
		t.Fatalf("limited RunAt = (%v, %d, %v)", kind, end, err)
	}

	ctx := context.Background()
	buf := make([]byte, 100)
	if n, err := src.ReadAt(ctx, buf, 0); n != 100 || err != nil {
		t.Fatalf("first read = (%d, %v)", n, err)
	}
	// Forward gap: skipped by discarding.
	if n, err := src.ReadAt(ctx, buf, 300); n != 100 || err != nil {
		t.Fatalf("forward read = (%d, %v)", n, err)
	}
	// Backward read: error.
	if _, err := src.ReadAt(ctx, buf, 100); err == nil {
		t.Fatal("backward read accepted")
	}
	// Clip at size.
	if n, err := src.ReadAt(ctx, buf, 950); n != 50 || err != io.EOF {
		t.Fatalf("clipped read = (%d, %v), want (50, EOF)", n, err)
	}
}

func TestDenseSourceShortInput(t *testing.T) {
	src := Dense(bytes.NewReader([]byte("short")), 100)
	buf := make([]byte, 100)
	_, err := src.ReadAt(context.Background(), buf, 0)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short input error = %v, want ErrUnexpectedEOF", err)
	}
}

func TestProbeHoles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sparse.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// [0,8K) data, [8K,1M) hole, [1M,1M+4K) data, [1M+4K,3M) hole
	if _, err := f.WriteAt(bytes.Repeat([]byte{'A'}, 8192), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytes.Repeat([]byte{'B'}, 4096), 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(3 << 20); err != nil {
		t.Fatal(err)
	}

	holes, err := ProbeHoles(f)
	if err != nil {
		t.Fatalf("ProbeHoles: %v", err)
	}
	if len(holes) == 0 {
		t.Skip("filesystem does not expose holes (SEEK_HOLE returned dense)")
	}
	// Filesystems may round hole boundaries to block size; verify the
	// declared holes are truly inside the zero regions and cover the
	// bulk of them.
	var holeBytes uint64
	for _, h := range holes {
		if h.Offset < 8192 && h.Offset+h.Size > 8192 {
			t.Fatalf("hole %+v overlaps data [0,8192)", h)
		}
		if h.Offset < (1<<20)+4096 && h.Offset+h.Size > 1<<20 {
			t.Fatalf("hole %+v overlaps data [1M,1M+4K)", h)
		}
		holeBytes += h.Size
	}
	if want := uint64(3<<20 - 8192 - 4096); holeBytes < want/2 {
		t.Fatalf("holes cover %d bytes, want at least %d", holeBytes, want/2)
	}
	// Offset restored.
	if pos, _ := f.Seek(0, io.SeekCurrent); pos != 0 {
		t.Fatalf("file offset = %d after probe, want 0", pos)
	}

	// Dense file probes to nil.
	dense := filepath.Join(dir, "dense.bin")
	if err := os.WriteFile(dense, bytes.Repeat([]byte{'C'}, 65536), 0o644); err != nil {
		t.Fatal(err)
	}
	df, err := os.Open(dense)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	holes, err = ProbeHoles(df)
	if err != nil {
		t.Fatalf("ProbeHoles(dense): %v", err)
	}
	if len(holes) != 0 {
		t.Fatalf("dense file probed holes: %+v", holes)
	}
}

func TestSourceContractZeroNeverFromStatic(t *testing.T) {
	// Static and dense sources must never classify anything as Zero —
	// Zero is a manifest-domain hint only.
	const size = 1 << 16
	src, err := NewSource(bytes.NewReader(image(size)), size, []Extent{{0, 4096}})
	if err != nil {
		t.Fatal(err)
	}
	for off := uint64(0); ; {
		kind, end, err := src.RunAt(off, size)
		if err == io.EOF {
			break
		}
		if kind == Zero {
			t.Fatalf("static source returned Zero at %d", off)
		}
		off = end
	}
}
