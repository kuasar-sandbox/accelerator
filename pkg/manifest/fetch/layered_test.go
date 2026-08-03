package fetch

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

// newTestStream builds a single-manifest Stream whose data chunks all decrypt
// to `fill` bytes (via passthroughEncryptor + staticGetter).
func newTestStream(m *codec.Manifest, fill byte) Stream {
	var maxSz uint32
	for _, e := range m.Entries {
		if e.Size > maxSz {
			maxSz = e.Size
		}
	}
	plain := bytes.Repeat([]byte{fill}, int(maxSz))
	stampHashes(m, plain)
	return newTestManifestStream(m, make([][32]byte, len(m.Entries)), &staticGetter{plain: plain}, &passthroughEncryptor{plain: plain})
}

func readAll(t *testing.T, s Stream) []byte {
	t.Helper()
	buf := make([]byte, s.Size())
	n, err := s.ReadAt(context.Background(), buf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if uint64(n) != s.Size() {
		t.Fatalf("short read: %d/%d", n, s.Size())
	}
	return buf
}

// TestLayered_HoleFallsThrough — a hole in the top layer is served by the data
// of the layer beneath it.
func TestLayered_HoleFallsThrough(t *testing.T) {
	top := newTestStream(&codec.Manifest{
		Version: codec.Version1, ImageSize: 8192,
		Entries: []codec.ChunkEntry{{Offset: 0, Size: 4096, CiphertextHash: store.ContentKey{0x1}}},
		Holes:   []sparse.Extent{{Offset: 4096, Size: 4096}},
	}, 'A')
	bottom := newTestStream(&codec.Manifest{
		Version: codec.Version1, ImageSize: 8192,
		Entries: []codec.ChunkEntry{{Offset: 0, Size: 8192, CiphertextHash: store.ContentKey{0x2}}},
	}, 'B')
	ls := NewLayered(top, bottom)
	defer ls.Close()

	if k, _, err := ls.RunAt(0, ls.Size()); err != nil || k != sparse.Data {
		t.Errorf("RunAt(0) = (%v, %v), want Data", k, err)
	}
	if k, _, err := ls.RunAt(4096, ls.Size()); err != nil || k != sparse.Data {
		t.Errorf("RunAt(4096) = (%v, %v), want Data (fall-through to bottom)", k, err)
	}
	out := readAll(t, ls)
	assertRegion(t, out[:4096], 'A', "top data")
	assertRegion(t, out[4096:], 'B', "fall-through to bottom")
}

// TestLayered_ZeroChunkDoesNotFallThrough — an IsZero chunk in the top layer is
// real zero data; it is NOT replaced by the lower layer's data.
func TestLayered_ZeroChunkDoesNotFallThrough(t *testing.T) {
	top := newTestStream(&codec.Manifest{
		Version: codec.Version1, ImageSize: 8192,
		Entries: []codec.ChunkEntry{
			{Offset: 0, Size: 4096, IsZero: true},
			{Offset: 4096, Size: 4096, CiphertextHash: store.ContentKey{0x1}},
		},
	}, 'A')
	bottom := newTestStream(&codec.Manifest{
		Version: codec.Version1, ImageSize: 8192,
		Entries: []codec.ChunkEntry{{Offset: 0, Size: 8192, CiphertextHash: store.ContentKey{0x2}}},
	}, 'B')
	ls := NewLayered(top, bottom)
	defer ls.Close()

	if k, _, err := ls.RunAt(0, ls.Size()); err != nil || k != sparse.Zero {
		t.Errorf("RunAt(0) = (%v, %v), want Zero (no fall-through)", k, err)
	}
	out := readAll(t, ls)
	assertRegion(t, out[:4096], 0x00, "top zero chunk (no fall-through)")
	assertRegion(t, out[4096:], 'A', "top data")
}

// TestLayered_MergedHole — a hole present in every layer is a merged hole.
func TestLayered_MergedHole(t *testing.T) {
	mk := func(fill byte, h store.ContentKey) Stream {
		return newTestStream(&codec.Manifest{
			Version: codec.Version1, ImageSize: 8192,
			Entries: []codec.ChunkEntry{{Offset: 4096, Size: 4096, CiphertextHash: h}},
			Holes:   []sparse.Extent{{Offset: 0, Size: 4096}},
		}, fill)
	}
	ls := NewLayered(mk('A', store.ContentKey{0x1}), mk('B', store.ContentKey{0x2}))
	defer ls.Close()

	if k, end, err := ls.RunAt(0, ls.Size()); err != nil || k != sparse.Hole || end != 4096 {
		t.Errorf("RunAt(0) = (%v, %d, %v), want (Hole, 4096, nil)", k, end, err)
	}
	out := readAll(t, ls)
	assertRegion(t, out[:4096], 0x00, "merged hole")
	assertRegion(t, out[4096:], 'A', "top data")
}

// TestLayered_ImageSizeMaxAndOOB — Size is the max over layers; an offset past
// the top layer's bound falls through to a larger lower layer.
func TestLayered_ImageSizeMaxAndOOB(t *testing.T) {
	top := newTestStream(&codec.Manifest{
		Version: codec.Version1, ImageSize: 4096,
		Entries: []codec.ChunkEntry{{Offset: 0, Size: 4096, CiphertextHash: store.ContentKey{0x1}}},
	}, 'A')
	bottom := newTestStream(&codec.Manifest{
		Version: codec.Version1, ImageSize: 8192,
		Entries: []codec.ChunkEntry{{Offset: 0, Size: 8192, CiphertextHash: store.ContentKey{0x2}}},
	}, 'B')
	ls := NewLayered(top, bottom)
	defer ls.Close()

	if ls.Size() != 8192 {
		t.Fatalf("Size = %d, want 8192 (max over layers)", ls.Size())
	}
	out := readAll(t, ls)
	assertRegion(t, out[:4096], 'A', "top data")
	assertRegion(t, out[4096:], 'B', "out-of-bounds of top falls through to bottom")
}

// TestLayered_SingleLayerIdentity — NewLayered with one layer returns it as-is.
func TestLayered_SingleLayerIdentity(t *testing.T) {
	s := newTestStream(&codec.Manifest{
		Version: codec.Version1, ImageSize: 4096,
		Entries: []codec.ChunkEntry{{Offset: 0, Size: 4096, CiphertextHash: store.ContentKey{0x1}}},
	}, 'A')
	if got := NewLayered(s); got != s {
		t.Error("NewLayered(single) should return the layer unchanged")
	}
}

// TestLayered_MixedFileAndManifest — a manifest overlay (with a hole) over a
// sparse file base: the manifest's data wins, its hole falls through to the
// file base, and the file's own sparse hole becomes the merged hole. This is
// the layered-disk-base use case.
func TestLayered_MixedFileAndManifest(t *testing.T) {
	const size = uint64(12288) // 3 × 4096

	// Tarstream base: data in [0,8192), sparse hole in [8192,12288).
	dir := t.TempDir()
	path := filepath.Join(dir, "base.img")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	logical := append(bytes.Repeat([]byte{'B'}, 8192), make([]byte, 4096)...)
	baseSource, err := sparse.NewSource(bytes.NewReader(logical), size, []sparse.Extent{{Offset: 8192, Size: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := tarstream.WriteTo(context.Background(), f, "base", baseSource); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	base, err := OpenTarStream(path)
	if err != nil {
		t.Fatal(err)
	}

	// Manifest overlay: data in [0,4096), declared hole in [4096,12288).
	overlay := newTestStream(&codec.Manifest{
		Version: codec.Version1, ImageSize: size,
		Entries: []codec.ChunkEntry{{Offset: 0, Size: 4096, CiphertextHash: store.ContentKey{0x1}}},
		Holes:   []sparse.Extent{{Offset: 4096, Size: 8192}},
	}, 'A')

	ls := NewLayered(overlay, base)
	defer ls.Close()

	out := readAll(t, ls)
	assertRegion(t, out[:4096], 'A', "overlay data")                  // overlay wins
	assertRegion(t, out[4096:8192], 'B', "fall-through to file base") // overlay hole → file data
	assertRegion(t, out[8192:], 0x00, "merged hole (file sparse + overlay hole)")
}
