package fetch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/fullof-work/mass-sandbox/pkg/cache"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/codec"
	"github.com/fullof-work/mass-sandbox/pkg/store"
)

// helper: build a manifest with one entry [0, dataSize), one hole
// [dataSize, dataSize+holeSize), one entry [dataSize+holeSize, end).
func buildHoleManifest(dataSize, holeSize uint64) *codec.Manifest {
	return &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: dataSize*2 + holeSize,
		Entries: []codec.ChunkEntry{
			{Offset: 0, Size: uint32(dataSize), CiphertextHash: store.ContentKey{0xA1}},
			{Offset: dataSize + holeSize, Size: uint32(dataSize), CiphertextHash: store.ContentKey{0xA2}},
		},
		Holes: []codec.HoleExtent{
			{Offset: dataSize, Size: holeSize},
		},
	}
}

// staticGetter returns a fixed plaintext for any hash.
type staticGetter struct {
	plain []byte
}

func (g *staticGetter) Get(_ context.Context, _ store.Partition, _ store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	return cache.CacheHit, cache.NewMemBlob(g.plain), nil
}

// TestWriteTo_OnHoleNil_ReturnsErrHitHole — default policy: a hole
// in the requested range surfaces ErrHitHole.
func TestWriteTo_OnHoleNil_ReturnsErrHitHole(t *testing.T) {
	const dataSize, holeSize = uint64(4096), uint64(4096)
	m := buildHoleManifest(dataSize, holeSize)
	plain := bytes.Repeat([]byte{0xAB}, int(dataSize))
	f := NewStream(m, make([][32]byte, 2), &staticGetter{plain}, &passthroughEncryptor{plain: plain})

	var buf bytes.Buffer
	err := f.WriteTo(context.Background(), &buf, 0, m.ImageSize, ReadOptions{})
	if !errors.Is(err, ErrHitHole) {
		t.Fatalf("expected ErrHitHole, got %v", err)
	}
}

// TestWriteTo_OnHoleCallback_InvokedExactlyOnce — non-nil OnHole
// receives the hole's image-offset and size.
func TestWriteTo_OnHoleCallback_InvokedExactlyOnce(t *testing.T) {
	const dataSize, holeSize = uint64(4096), uint64(2048)
	m := buildHoleManifest(dataSize, holeSize)
	plain := bytes.Repeat([]byte{0xAB}, int(dataSize))
	f := NewStream(m, make([][32]byte, 2), &staticGetter{plain}, &passthroughEncryptor{plain: plain})

	var calls atomic.Int32
	var gotOff, gotSize uint64
	holeFn := func(w io.Writer, off, sz uint64) error {
		calls.Add(1)
		gotOff = off
		gotSize = sz
		// Write a zero-fill so output buf has the right length.
		_, err := io.CopyN(w, zeroR{}, int64(sz))
		return err
	}

	var buf bytes.Buffer
	if err := f.WriteTo(context.Background(), &buf, 0, m.ImageSize, ReadOptions{OnHole: holeFn}); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("OnHole calls: got %d, want 1", got)
	}
	if gotOff != dataSize || gotSize != holeSize {
		t.Errorf("OnHole called with (%d, %d), want (%d, %d)", gotOff, gotSize, dataSize, holeSize)
	}
	if uint64(buf.Len()) != m.ImageSize {
		t.Errorf("output bytes: got %d, want %d", buf.Len(), m.ImageSize)
	}
	// Middle holeSize bytes should be zeros.
	for i := dataSize; i < dataSize+holeSize; i++ {
		if buf.Bytes()[i] != 0 {
			t.Fatalf("byte %d = %x, want 0 (hole zero-fill)", i, buf.Bytes()[i])
		}
	}
}

type zeroR struct{}

func (zeroR) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// TestReadAt_OffsetInHole_ReturnsErrHitHole — offset starts inside
// the hole; expect (0, ErrHitHole).
func TestReadAt_OffsetInHole_ReturnsErrHitHole(t *testing.T) {
	const dataSize, holeSize = uint64(4096), uint64(4096)
	m := buildHoleManifest(dataSize, holeSize)
	plain := bytes.Repeat([]byte{0xAB}, int(dataSize))
	f := NewStream(m, make([][32]byte, 2), &staticGetter{plain}, &passthroughEncryptor{plain: plain})

	buf := make([]byte, 1024)
	n, err := f.ReadAt(context.Background(), buf, dataSize+100)
	if n != 0 {
		t.Errorf("n: got %d, want 0", n)
	}
	if !errors.Is(err, ErrHitHole) {
		t.Errorf("err: got %v, want ErrHitHole", err)
	}
}

// TestReadAt_ClipsAtHoleBoundary — offset+len(buf) would cross into
// the hole; ReadAt returns just the bytes up to hole.Offset with nil
// error. Caller's next ReadAt at the hole boundary gets ErrHitHole.
func TestReadAt_ClipsAtHoleBoundary(t *testing.T) {
	const dataSize, holeSize = uint64(4096), uint64(4096)
	m := buildHoleManifest(dataSize, holeSize)
	plain := bytes.Repeat([]byte{0xAB}, int(dataSize))
	f := NewStream(m, make([][32]byte, 2), &staticGetter{plain}, &passthroughEncryptor{plain: plain})

	// Read at offset dataSize-100 with buf size 1024: should fill 100
	// bytes (up to dataSize) then stop at the hole boundary.
	buf := make([]byte, 1024)
	n, err := f.ReadAt(context.Background(), buf, dataSize-100)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != 100 {
		t.Errorf("n: got %d, want 100 (clipped at hole start)", n)
	}
	// Verify those 100 bytes are the tail of the first chunk.
	for i := 0; i < n; i++ {
		if buf[i] != 0xAB {
			t.Fatalf("buf[%d] = %x, want 0xAB", i, buf[i])
		}
	}
}

// TestReadAt_ClipsAtImageEnd — offset+len(buf) past ImageSize: short
// read with no error; subsequent ReadAt at ImageSize returns (0, io.EOF).
func TestReadAt_ClipsAtImageEnd(t *testing.T) {
	const dataSize, holeSize = uint64(4096), uint64(0) // no hole, simple image
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: dataSize,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(dataSize), CiphertextHash: store.ContentKey{0xCC}}},
	}
	_ = holeSize
	plain := bytes.Repeat([]byte{0xCC}, int(dataSize))
	f := NewStream(m, make([][32]byte, 1), &staticGetter{plain}, &passthroughEncryptor{plain: plain})

	buf := make([]byte, 1024)
	// Offset 4000 with 1024-byte buf: only 96 bytes are available before EOF.
	n, err := f.ReadAt(context.Background(), buf, 4000)
	if err != nil {
		t.Fatalf("first ReadAt: unexpected err %v", err)
	}
	if n != 96 {
		t.Errorf("first ReadAt n: got %d, want 96", n)
	}

	// Next call at offset=ImageSize must return (0, io.EOF).
	n2, err := f.ReadAt(context.Background(), buf, dataSize)
	if n2 != 0 {
		t.Errorf("EOF ReadAt n: got %d, want 0", n2)
	}
	if !errors.Is(err, io.EOF) {
		t.Errorf("EOF ReadAt err: got %v, want io.EOF", err)
	}
}

// TestReadAt_FillsCompleteBuf_NoBoundary — offset+len(buf) within
// a single contiguous data region; read fills buf completely with nil err.
func TestReadAt_FillsCompleteBuf_NoBoundary(t *testing.T) {
	const dataSize = uint64(8192)
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: dataSize,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(dataSize), CiphertextHash: store.ContentKey{0xDD}}},
	}
	plain := bytes.Repeat([]byte{0xDD}, int(dataSize))
	f := NewStream(m, make([][32]byte, 1), &staticGetter{plain}, &passthroughEncryptor{plain: plain})

	buf := make([]byte, 1024)
	n, err := f.ReadAt(context.Background(), buf, 1000)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != len(buf) {
		t.Errorf("n: got %d, want %d", n, len(buf))
	}
}

// TestReadAt_LoopAcrossHole — caller-driven loop using ReadAt +
// FindHole correctly walks the entire image, including stepping
// past the hole.
func TestReadAt_LoopAcrossHole(t *testing.T) {
	const dataSize, holeSize = uint64(4096), uint64(4096)
	m := buildHoleManifest(dataSize, holeSize)
	plain := bytes.Repeat([]byte{0xEE}, int(dataSize))
	f := NewStream(m, make([][32]byte, 2), &staticGetter{plain}, &passthroughEncryptor{plain: plain})

	output := make([]byte, m.ImageSize)
	buf := make([]byte, 1024)
	offset := uint64(0)
	for offset < m.ImageSize {
		n, err := f.ReadAt(context.Background(), buf, offset)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, ErrHitHole) {
			h, ok := f.FindHole(offset)
			if !ok {
				t.Fatalf("ErrHitHole at offset %d but FindHole returned !ok", offset)
			}
			// Skip over the hole; leave output zeros.
			offset = h.Offset + h.Size
			continue
		}
		if err != nil {
			t.Fatalf("err at offset %d: %v", offset, err)
		}
		copy(output[offset:offset+uint64(n)], buf[:n])
		offset += uint64(n)
	}

	// Verify the output: data regions hold 0xEE, hole region is all zero
	// (initial slice value).
	for i := uint64(0); i < dataSize; i++ {
		if output[i] != 0xEE {
			t.Fatalf("first data region byte %d = %x, want 0xEE", i, output[i])
		}
	}
	for i := dataSize; i < dataSize+holeSize; i++ {
		if output[i] != 0 {
			t.Fatalf("hole byte %d = %x, want 0", i, output[i])
		}
	}
	for i := dataSize + holeSize; i < m.ImageSize; i++ {
		if output[i] != 0xEE {
			t.Fatalf("second data region byte %d = %x, want 0xEE", i, output[i])
		}
	}
}

// TestReadAtBlock_ZeroFillsHole — block-device semantic: hole regions
// inside the requested range are filled with zeros transparently, no
// ErrHitHole surfaces.
func TestReadAtBlock_ZeroFillsHole(t *testing.T) {
	const dataSize, holeSize = uint64(4096), uint64(4096)
	m := buildHoleManifest(dataSize, holeSize)
	plain := bytes.Repeat([]byte{0xEE}, int(dataSize))
	f := NewStream(m, make([][32]byte, 2), &staticGetter{plain}, &passthroughEncryptor{plain: plain})

	// Single ReadAtBlock spanning all three regions: data, hole, data.
	buf := make([]byte, m.ImageSize)
	n, err := f.ReadAtBlock(context.Background(), buf, 0)
	if err != nil {
		t.Fatalf("ReadAtBlock: %v", err)
	}
	if uint64(n) != m.ImageSize {
		t.Fatalf("n: got %d, want %d", n, m.ImageSize)
	}
	for i := uint64(0); i < dataSize; i++ {
		if buf[i] != 0xEE {
			t.Fatalf("first data region byte %d = %x, want 0xEE", i, buf[i])
		}
	}
	for i := dataSize; i < dataSize+holeSize; i++ {
		if buf[i] != 0 {
			t.Fatalf("hole byte %d = %x, want 0", i, buf[i])
		}
	}
	for i := dataSize + holeSize; i < m.ImageSize; i++ {
		if buf[i] != 0xEE {
			t.Fatalf("second data region byte %d = %x, want 0xEE", i, buf[i])
		}
	}
}

// TestReadAtBlock_OffsetInsideHole — starting offset itself is inside
// the hole; read is still satisfied by zero-fill (no ErrHitHole).
func TestReadAtBlock_OffsetInsideHole(t *testing.T) {
	const dataSize, holeSize = uint64(4096), uint64(8192)
	m := buildHoleManifest(dataSize, holeSize)
	plain := bytes.Repeat([]byte{0xEE}, int(dataSize))
	f := NewStream(m, make([][32]byte, 2), &staticGetter{plain}, &passthroughEncryptor{plain: plain})

	// offset 100 bytes into the hole; read 1024 bytes (entirely inside hole).
	buf := bytes.Repeat([]byte{0xFF}, 1024)
	n, err := f.ReadAtBlock(context.Background(), buf, dataSize+100)
	if err != nil {
		t.Fatalf("ReadAtBlock: %v", err)
	}
	if n != 1024 {
		t.Fatalf("n: got %d, want 1024", n)
	}
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("buf[%d] = %x, want 0 (zero-fill)", i, b)
		}
	}
}

// TestReadAtBlock_PartialEOF — len(buf) extends past ImageSize; expect
// (n, io.EOF) where n is the portion within the image.
func TestReadAtBlock_PartialEOF(t *testing.T) {
	const dataSize = uint64(4096)
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: dataSize,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(dataSize), CiphertextHash: store.ContentKey{0xCC}}},
	}
	plain := bytes.Repeat([]byte{0xCC}, int(dataSize))
	f := NewStream(m, make([][32]byte, 1), &staticGetter{plain}, &passthroughEncryptor{plain: plain})

	buf := make([]byte, 1024)
	n, err := f.ReadAtBlock(context.Background(), buf, 4000)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err: got %v, want io.EOF", err)
	}
	if n != 96 {
		t.Fatalf("n: got %d, want 96", n)
	}

	// Past-EOF call: (0, io.EOF).
	n2, err2 := f.ReadAtBlock(context.Background(), buf, dataSize)
	if n2 != 0 || !errors.Is(err2, io.EOF) {
		t.Errorf("(n, err) at EOF = (%d, %v), want (0, io.EOF)", n2, err2)
	}
}

// TestReadAt_ZeroLenBuf — empty buffer is a no-op (0, nil).
func TestReadAt_ZeroLenBuf(t *testing.T) {
	const dataSize = uint64(4096)
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: dataSize,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(dataSize)}},
	}
	f := NewStream(m, make([][32]byte, 1), nil, nil)

	n, err := f.ReadAt(context.Background(), nil, 100)
	if n != 0 || err != nil {
		t.Errorf("(n, err) = (%d, %v), want (0, nil)", n, err)
	}
}
