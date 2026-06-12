package tar

import (
	stdtar "archive/tar"
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
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
		{size: 100, holes: []Hole{{10, 0}}, sparse: false}, // zero-length ignored
		{size: 100, holes: []Hole{{0, 100}}, want: nil, sparse: true},
		{size: 100, holes: []Hole{{0, 40}}, want: ext(40, 60), sparse: true},
		{size: 100, holes: []Hole{{60, 40}}, want: ext(0, 60), sparse: true},
		{size: 100, holes: []Hole{{40, 20}}, want: ext(0, 40, 60, 40), sparse: true},
		{size: 100, holes: []Hole{{60, 20}, {20, 20}}, want: ext(0, 20, 40, 20, 80, 20), sparse: true}, // unsorted
		{size: 100, holes: []Hole{{20, 20}, {40, 20}}, want: ext(0, 20, 60, 40), sparse: true},         // adjacent merge
		{size: 100, holes: []Hole{{20, 30}, {40, 20}}, err: true},                                      // overlap
		{size: 100, holes: []Hole{{90, 20}}, err: true},                                                // out of bounds
		{size: 100, holes: []Hole{{-1, 5}}, err: true},
	}
	for i, c := range cases {
		got, sparse, err := holesToExtents(c.size, c.holes)
		if c.err {
			if err == nil {
				t.Errorf("case %d: want error, got %v sparse=%v", i, got, sparse)
			}
			continue
		}
		if err != nil {
			t.Errorf("case %d: %v", i, err)
			continue
		}
		if sparse != c.sparse {
			t.Errorf("case %d: sparse = %v, want %v", i, sparse, c.sparse)
		}
		if len(got) != len(c.want) {
			t.Errorf("case %d: extents = %v, want %v", i, got, c.want)
			continue
		}
		for j := range got {
			if got[j] != c.want[j] {
				t.Errorf("case %d: extents = %v, want %v", i, got, c.want)
				break
			}
		}
	}
}

// logicalSparse builds the logical bytes and the hole map of a test
// file: data "A.." at 0..8K, hole, data "B.." at 1M..1M+4K, trailing
// hole to 3M.
func logicalSparse() ([]byte, []Hole) {
	const size = 3 << 20
	buf := make([]byte, size)
	for i := 0; i < 8192; i++ {
		buf[i] = 'A'
	}
	for i := 1 << 20; i < (1<<20)+4096; i++ {
		buf[i] = 'B'
	}
	holes := []Hole{
		{8192, (1 << 20) - 8192},
		{(1 << 20) + 4096, (3 << 20) - ((1 << 20) + 4096)},
	}
	return buf, holes
}

func writeSparseArchive(t *testing.T, extra func(sw *StreamWriter)) []byte {
	t.Helper()
	logical, holes := logicalSparse()
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)
	hdr := &stdtar.Header{
		Name:    "disk/img.raw",
		Mode:    0o640,
		Uid:     12,
		Gid:     34,
		Size:    int64(len(logical)),
		ModTime: time.Date(2022, 5, 6, 7, 8, 9, 0, time.UTC),
	}
	if err := sw.WriteSparse(hdr, bytes.NewReader(logical), holes); err != nil {
		t.Fatal(err)
	}
	if extra != nil {
		extra(sw)
	}
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestStreamWriterStdlibOracle(t *testing.T) {
	archive := writeSparseArchive(t, nil)
	logical, _ := logicalSparse()

	// Encoded size ≈ data only.
	if len(archive) > 64<<10 {
		t.Errorf("archive too large: %d bytes", len(archive))
	}

	tr := stdtar.NewReader(bytes.NewReader(archive))
	hdr, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Name != "disk/img.raw" {
		t.Errorf("name = %q", hdr.Name)
	}
	if hdr.Size != int64(len(logical)) {
		t.Errorf("logical size = %d", hdr.Size)
	}
	if hdr.Uid != 12 || hdr.Gid != 34 {
		t.Errorf("owner = %d:%d", hdr.Uid, hdr.Gid)
	}
	if !hdr.ModTime.Equal(time.Date(2022, 5, 6, 7, 8, 9, 0, time.UTC)) {
		t.Errorf("mtime = %v", hdr.ModTime)
	}
	got, err := io.ReadAll(tr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, logical) {
		t.Error("logical content mismatch via stdlib reader")
	}
}

func TestStreamWriterExtractOracle(t *testing.T) {
	dir := t.TempDir()
	if !sparseSupported(t, dir) {
		t.Skip("filesystem does not keep holes")
	}
	archive := writeSparseArchive(t, nil)
	logical, _ := logicalSparse()

	out := filepath.Join(dir, "img.raw")
	if err := Extract(bytes.NewReader(archive), []Rule{{Tar: "disk/img.raw", FS: out}}, Options{}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, logical) {
		t.Error("content mismatch via our Extract")
	}
	var st syscall.Stat_t
	if err := syscall.Stat(out, &st); err != nil {
		t.Fatal(err)
	}
	if st.Blocks*512 >= int64(len(logical)) {
		t.Errorf("extracted dense: %d blocks", st.Blocks)
	}
}

func TestStreamWriterGNUOracle(t *testing.T) {
	tarBin, err := exec.LookPath("tar")
	if err != nil {
		t.Skip("no system tar")
	}
	dir := t.TempDir()
	if !sparseSupported(t, dir) {
		t.Skip("filesystem does not keep holes")
	}
	archive := filepath.Join(dir, "a.tar")
	if err := os.WriteFile(archive, writeSparseArchive(t, nil), 0o644); err != nil {
		t.Fatal(err)
	}
	xdir := filepath.Join(dir, "x")
	if err := os.Mkdir(xdir, 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(tarBin, "-xf", archive, "-C", xdir).CombinedOutput(); err != nil {
		t.Fatalf("system tar -x: %v: %s", err, out)
	}
	logical, _ := logicalSparse()
	got, err := os.ReadFile(filepath.Join(xdir, "disk/img.raw"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, logical) {
		t.Error("content mismatch via GNU tar")
	}
	var st syscall.Stat_t
	if err := syscall.Stat(filepath.Join(xdir, "disk/img.raw"), &st); err != nil {
		t.Fatal(err)
	}
	if st.Blocks*512 >= int64(len(logical)) {
		t.Errorf("GNU extracted dense: %d blocks", st.Blocks)
	}
}

func TestStreamWriterMixedEntries(t *testing.T) {
	archive := writeSparseArchive(t, func(sw *StreamWriter) {
		hdr := &stdtar.Header{Name: "etc/conf", Mode: 0o644, Size: 5}
		if err := sw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := sw.Write([]byte("k=v\n!")); err != nil {
			t.Fatal(err)
		}
		// A second sparse member after a plain one (all-hole).
		h2 := &stdtar.Header{Name: "disk/empty.raw", Mode: 0o644, Size: 1 << 20}
		if err := sw.WriteSparse(h2, bytes.NewReader(make([]byte, 1<<20)), []Hole{{0, 1 << 20}}); err != nil {
			t.Fatal(err)
		}
	})

	var names []string
	var sizes []int64
	tr := stdtar.NewReader(bytes.NewReader(archive))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, hdr.Name)
		sizes = append(sizes, hdr.Size)
		if _, err := io.Copy(io.Discard, tr); err != nil {
			t.Fatalf("%s: %v", hdr.Name, err)
		}
	}
	if strings.Join(names, ",") != "disk/img.raw,etc/conf,disk/empty.raw" {
		t.Errorf("names = %v", names)
	}
	if sizes[2] != 1<<20 {
		t.Errorf("all-hole logical size = %d", sizes[2])
	}
}

func TestStreamWriterDenseFallback(t *testing.T) {
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)
	hdr := &stdtar.Header{Name: "plain.bin", Mode: 0o644, Size: 6}
	if err := sw.WriteSparse(hdr, bytes.NewReader([]byte("abcdef")), nil); err != nil {
		t.Fatal(err)
	}
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}
	tr := stdtar.NewReader(bytes.NewReader(buf.Bytes()))
	h, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if h.Name != "plain.bin" || h.Size != 6 {
		t.Errorf("hdr = %q/%d", h.Name, h.Size)
	}
	got, _ := io.ReadAll(tr)
	if string(got) != "abcdef" {
		t.Errorf("content = %q", got)
	}
}

func TestStreamWriterDeterministic(t *testing.T) {
	a := writeSparseArchive(t, nil)
	b := writeSparseArchive(t, nil)
	if !bytes.Equal(a, b) {
		t.Error("two emissions differ")
	}
}

func TestStreamWriterShortSource(t *testing.T) {
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)
	hdr := &stdtar.Header{Name: "x", Size: 1 << 20}
	err := sw.WriteSparse(hdr, bytes.NewReader(make([]byte, 100)), []Hole{{0, 4096}})
	if err == nil || !strings.Contains(err.Error(), "ended early") {
		t.Errorf("want short-source error, got %v", err)
	}
}
