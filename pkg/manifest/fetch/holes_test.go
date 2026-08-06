package fetch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// buildHoleManifest builds a manifest with one entry [0, dataSize), one hole
// [dataSize, dataSize+holeSize), one entry [dataSize+holeSize, end).
func buildHoleManifest(dataSize, holeSize uint64) *codec.Manifest {
	return &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: dataSize*2 + holeSize,
		Entries: []codec.ChunkEntry{
			{Offset: 0, Size: uint32(dataSize), CiphertextHash: store.ContentKey{0xA1}},
			{Offset: dataSize + holeSize, Size: uint32(dataSize), CiphertextHash: store.ContentKey{0xA2}},
		},
		Holes: []sparse.Extent{
			{Offset: dataSize, Size: holeSize},
		},
	}
}

// staticGetter returns a fixed plaintext for any hash.
type staticGetter struct{ plain []byte }

func (g *staticGetter) Get(_ context.Context, _ store.Partition, _ store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	return cache.CacheHit, cache.NewMemBlob(g.plain), nil
}

// assertRegion fails if any byte in b differs from want.
func assertRegion(t *testing.T, b []byte, want byte, name string) {
	t.Helper()
	for i, got := range b {
		if got != want {
			t.Fatalf("%s byte %d = %#x, want %#x", name, i, got, want)
		}
	}
}

// TestRunAt_DataHoleData classifies the three regions and checks the length
// bound and io.EOF contract.
func TestRunAt_DataHoleData(t *testing.T) {
	const dataSize, holeSize = uint64(4096), uint64(4096)
	m := buildHoleManifest(dataSize, holeSize)
	f := newTestManifestStream(m, make([][32]byte, 2), nil, nil)
	defer f.Close()
	full := m.ImageSize

	if run, err := f.RunAt(0, full); err != nil || run.Kind() != sparse.Data || run.Offset() != 0 || run.End() != dataSize {
		t.Errorf("RunAt(0) = (%v, %v), want (Data, %d, nil)", run, err, dataSize)
	} else if _, ok := run.(ChunkRun); !ok {
		t.Errorf("manifest Data run type = %T, want ChunkRun", run)
	}
	if run, err := f.RunAt(dataSize, full); err != nil || run.Kind() != sparse.Hole || run.Offset() != dataSize || run.End() != dataSize+holeSize {
		t.Errorf("RunAt(hole) = (%v, %v), want (Hole, %d, nil)", run, err, dataSize+holeSize)
	} else if _, ok := run.(ChunkRun); ok {
		t.Errorf("manifest Hole run type = %T, must not implement ChunkRun", run)
	}
	if run, err := f.RunAt(dataSize+holeSize, full); err != nil || run.Kind() != sparse.Data || run.End() != full {
		t.Errorf("RunAt(data2) = (%v, %v), want (Data, %d, nil)", run, err, full)
	}
	// limit is a length: end <= offset+limit.
	if run, err := f.RunAt(0, 1024); err != nil || run.Kind() != sparse.Data || run.End() != 1024 {
		t.Errorf("RunAt(0, 1024) = (%v, %v), want (Data, 1024, nil)", run, err)
	}
	// offset >= Size → io.EOF.
	if _, err := f.RunAt(full, 1); !errors.Is(err, io.EOF) {
		t.Errorf("RunAt(Size) err = %v, want io.EOF", err)
	}
	if _, err := f.RunAt(0, 0); err == nil {
		t.Error("RunAt with zero limit succeeded")
	}
}

// TestReadAt_ZeroFillsHole — a single ReadAt spanning data/hole/data zero-fills
// the hole inline (POSIX semantics, no error).
func TestReadAt_ZeroFillsHole(t *testing.T) {
	const dataSize, holeSize = uint64(4096), uint64(4096)
	m := buildHoleManifest(dataSize, holeSize)
	plain := bytes.Repeat([]byte{0xEE}, int(dataSize))
	stampHashes(m, plain)
	f := newTestManifestStream(m, make([][32]byte, 2), &staticGetter{plain}, &passthroughEncryptor{plain: plain})
	defer f.Close()

	buf := make([]byte, m.ImageSize)
	n, err := f.ReadAt(context.Background(), buf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if uint64(n) != m.ImageSize {
		t.Fatalf("n: got %d, want %d", n, m.ImageSize)
	}
	assertRegion(t, buf[:dataSize], 0xEE, "first data")
	assertRegion(t, buf[dataSize:dataSize+holeSize], 0x00, "hole")
	assertRegion(t, buf[dataSize+holeSize:], 0xEE, "second data")
}

// TestReadAt_OffsetInsideHole — offset inside the hole; POSIX ReadAt zero-fills.
func TestReadAt_OffsetInsideHole(t *testing.T) {
	const dataSize, holeSize = uint64(4096), uint64(8192)
	m := buildHoleManifest(dataSize, holeSize)
	plain := bytes.Repeat([]byte{0xEE}, int(dataSize))
	f := newTestManifestStream(m, make([][32]byte, 2), &staticGetter{plain}, &passthroughEncryptor{plain: plain})
	defer f.Close()

	buf := bytes.Repeat([]byte{0xFF}, 1024)
	n, err := f.ReadAt(context.Background(), buf, dataSize+100)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 1024 {
		t.Fatalf("n: got %d, want 1024", n)
	}
	assertRegion(t, buf, 0x00, "hole zero-fill")
}

// TestReadAt_PartialEOF — len(buf) past Size → (n, io.EOF); past-EOF → (0, io.EOF).
func TestReadAt_PartialEOF(t *testing.T) {
	const dataSize = uint64(4096)
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: dataSize,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(dataSize), CiphertextHash: store.ContentKey{0xCC}}},
	}
	plain := bytes.Repeat([]byte{0xCC}, int(dataSize))
	stampHashes(m, plain)
	f := newTestManifestStream(m, make([][32]byte, 1), &staticGetter{plain}, &passthroughEncryptor{plain: plain})
	defer f.Close()

	buf := make([]byte, 1024)
	n, err := f.ReadAt(context.Background(), buf, 4000)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err: got %v, want io.EOF", err)
	}
	if n != 96 {
		t.Fatalf("n: got %d, want 96", n)
	}
	n2, err2 := f.ReadAt(context.Background(), buf, dataSize)
	if n2 != 0 || !errors.Is(err2, io.EOF) {
		t.Errorf("(n, err) at EOF = (%d, %v), want (0, io.EOF)", n2, err2)
	}
}

// TestReadAt_FillsCompleteBuf — offset+len within a single data region.
func TestReadAt_FillsCompleteBuf(t *testing.T) {
	const dataSize = uint64(8192)
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: dataSize,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(dataSize), CiphertextHash: store.ContentKey{0xDD}}},
	}
	plain := bytes.Repeat([]byte{0xDD}, int(dataSize))
	stampHashes(m, plain)
	f := newTestManifestStream(m, make([][32]byte, 1), &staticGetter{plain}, &passthroughEncryptor{plain: plain})
	defer f.Close()

	buf := make([]byte, 1024)
	n, err := f.ReadAt(context.Background(), buf, 1000)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != len(buf) {
		t.Errorf("n: got %d, want %d", n, len(buf))
	}
	assertRegion(t, buf, 0xDD, "data")
}

// TestReadAt_ZeroLenBuf — empty buffer is a no-op.
func TestReadAt_ZeroLenBuf(t *testing.T) {
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: 4096,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: 4096}},
	}
	f := newTestManifestStream(m, make([][32]byte, 1), nil, nil)
	defer f.Close()
	n, err := f.ReadAt(context.Background(), nil, 100)
	if n != 0 || err != nil {
		t.Errorf("(n, err) = (%d, %v), want (0, nil)", n, err)
	}
}

// TestRunAt_ChainLoop drives the whole image through executable Runs.
func TestRunAt_ChainLoop(t *testing.T) {
	const dataSize, holeSize = uint64(4096), uint64(4096)
	m := buildHoleManifest(dataSize, holeSize)
	plain := bytes.Repeat([]byte{0xEE}, int(dataSize))
	stampHashes(m, plain)
	s := newTestManifestStream(m, make([][32]byte, 2), &staticGetter{plain}, &passthroughEncryptor{plain: plain})
	defer s.Close()
	out := make([]byte, m.ImageSize)
	for off := uint64(0); off < m.ImageSize; {
		run, err := s.RunAt(off, m.ImageSize-off)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("RunAt @ %d: %v", off, err)
		}
		if run.Kind() == sparse.Data {
			if _, rerr := run.ReadAt(context.Background(), out[off:run.End()], 0); rerr != nil {
				t.Fatalf("Run.ReadAt @ %d: %v", off, rerr)
			}
		} // Hole/Zero: leave zeros
		off = run.End()
	}
	assertRegion(t, out[:dataSize], 0xEE, "first data")
	assertRegion(t, out[dataSize:dataSize+holeSize], 0x00, "hole")
	assertRegion(t, out[dataSize+holeSize:], 0xEE, "second data")
}
