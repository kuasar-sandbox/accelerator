package fetch

import (
	"bytes"
	"context"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

const chunkWindowTestPage = uint64(4096)

func TestResolveChunkWindowDirectManifestIsMetadataOnly(t *testing.T) {
	const size = 4 * chunkWindowTestPage
	plain := bytes.Repeat([]byte{0x5a}, int(size))
	manifest := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: size,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(size)}},
	}
	stampHashes(manifest, plain)
	getter := &countingHitGetter{value: plain}
	stream := newTestManifestStream(
		manifest,
		make([][32]byte, 1),
		getter,
		&passthroughEncryptor{plain: plain},
	)
	defer stream.Close()

	anchor := requireChunkAnchor(t, stream, chunkWindowTestPage, chunkWindowTestPage)
	window, err := ResolveChunkWindow(stream, anchor, size)
	if err != nil {
		t.Fatal(err)
	}
	if window.Offset() != 0 || window.End() != size {
		t.Fatalf("window = [%d,%d), want [0,%d)", window.Offset(), window.End(), size)
	}
	if got := getter.calls.Load(); got != 0 {
		t.Fatalf("ResolveChunkWindow performed %d payload Gets", got)
	}

	buf := make([]byte, size)
	if n, err := window.ReadAt(context.Background(), buf, 0); err != nil || n != len(buf) {
		t.Fatalf("window.ReadAt = (%d,%v), want (%d,nil)", n, err, len(buf))
	}
	if !bytes.Equal(buf, plain) {
		t.Fatal("expanded window returned wrong physical chunk bytes")
	}
	if got := getter.calls.Load(); got != 1 {
		t.Fatalf("whole window read performed %d Gets, want 1", got)
	}
}

func TestResolveChunkWindowLayeredVisibility(t *testing.T) {
	const size = 8 * chunkWindowTestPage
	top := newTestStream(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: size,
		Entries: []codec.ChunkEntry{
			{Offset: 0, Size: uint32(2 * chunkWindowTestPage), CiphertextHash: store.ContentKey{0x11}},
			{Offset: 6 * chunkWindowTestPage, Size: uint32(2 * chunkWindowTestPage), IsZero: true},
		},
		Holes: []sparse.Extent{{Offset: 2 * chunkWindowTestPage, Size: 4 * chunkWindowTestPage}},
	}, 'T')
	lower := newTestStream(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: size,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(size), CiphertextHash: store.ContentKey{0x22}}},
	}, 'L')
	stream := NewLayered(top, lower)
	defer stream.Close()

	anchor := requireChunkAnchor(t, stream, 4*chunkWindowTestPage, chunkWindowTestPage)
	window, err := ResolveChunkWindow(stream, anchor, size)
	if err != nil {
		t.Fatal(err)
	}
	wantStart, wantEnd := 2*chunkWindowTestPage, 6*chunkWindowTestPage
	if window.Offset() != wantStart || window.End() != wantEnd {
		t.Fatalf("window = [%d,%d), want [%d,%d)", window.Offset(), window.End(), wantStart, wantEnd)
	}
	buf := make([]byte, wantEnd-wantStart)
	if n, err := window.ReadAt(context.Background(), buf, 0); err != nil || n != len(buf) {
		t.Fatalf("window.ReadAt = (%d,%v)", n, err)
	}
	assertRegion(t, buf, 'L', "visible lower chunk window")
}

func TestResolveChunkWindowSelectsAnchoredVisibleSegment(t *testing.T) {
	const size = 8 * chunkWindowTestPage
	top := newTestStream(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: size,
		Entries: []codec.ChunkEntry{{
			Offset:         3 * chunkWindowTestPage,
			Size:           uint32(2 * chunkWindowTestPage),
			CiphertextHash: store.ContentKey{0x31},
		}},
		Holes: []sparse.Extent{
			{Offset: 0, Size: 3 * chunkWindowTestPage},
			{Offset: 5 * chunkWindowTestPage, Size: 3 * chunkWindowTestPage},
		},
	}, 'T')
	lower := newTestStream(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: size,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(size), CiphertextHash: store.ContentKey{0x32}}},
	}, 'L')
	stream := NewLayered(top, lower)
	defer stream.Close()

	for _, tt := range []struct {
		name              string
		fault, start, end uint64
	}{
		{name: "before opaque middle", fault: 2 * chunkWindowTestPage, start: 0, end: 3 * chunkWindowTestPage},
		{name: "after opaque middle", fault: 6 * chunkWindowTestPage, start: 5 * chunkWindowTestPage, end: size},
	} {
		t.Run(tt.name, func(t *testing.T) {
			anchor := requireChunkAnchor(t, stream, tt.fault, chunkWindowTestPage)
			window, err := ResolveChunkWindow(stream, anchor, size)
			if err != nil {
				t.Fatal(err)
			}
			if window.Offset() != tt.start || window.End() != tt.end {
				t.Fatalf("window = [%d,%d), want [%d,%d)", window.Offset(), window.End(), tt.start, tt.end)
			}
		})
	}
}

func TestResolveChunkWindowNestedLayers(t *testing.T) {
	const size = 8 * chunkWindowTestPage
	outer := newTestStream(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: size,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(chunkWindowTestPage), CiphertextHash: store.ContentKey{0x41}}},
		Holes:     []sparse.Extent{{Offset: chunkWindowTestPage, Size: 7 * chunkWindowTestPage}},
	}, 'O')
	inner := newTestStream(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: size,
		Entries: []codec.ChunkEntry{{
			Offset:         7 * chunkWindowTestPage,
			Size:           uint32(chunkWindowTestPage),
			CiphertextHash: store.ContentKey{0x42},
		}},
		Holes: []sparse.Extent{{Offset: 0, Size: 7 * chunkWindowTestPage}},
	}, 'I')
	lower := newTestStream(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: size,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(size), CiphertextHash: store.ContentKey{0x43}}},
	}, 'L')
	stream := NewLayered(outer, NewLayered(inner, lower))
	defer stream.Close()

	anchor := requireChunkAnchor(t, stream, 4*chunkWindowTestPage, chunkWindowTestPage)
	window, err := ResolveChunkWindow(stream, anchor, size)
	if err != nil {
		t.Fatal(err)
	}
	wantStart, wantEnd := chunkWindowTestPage, 7*chunkWindowTestPage
	if window.Offset() != wantStart || window.End() != wantEnd {
		t.Fatalf("window = [%d,%d), want [%d,%d)", window.Offset(), window.End(), wantStart, wantEnd)
	}
}

func TestResolveChunkWindowHonorsRootStreamSize(t *testing.T) {
	const physicalSize = 4 * chunkWindowTestPage
	const visibleSize = 3 * chunkWindowTestPage
	leaf := newTestStream(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: physicalSize,
		Entries: []codec.ChunkEntry{{
			Offset:         0,
			Size:           uint32(physicalSize),
			CiphertextHash: store.ContentKey{0x49},
		}},
	}, 'L')
	stream := &chunkWindowPrefixStream{Stream: leaf, size: visibleSize}
	defer stream.Close()

	anchor := requireChunkAnchor(t, stream, chunkWindowTestPage, chunkWindowTestPage)
	window, err := ResolveChunkWindow(stream, anchor, physicalSize)
	if err != nil {
		t.Fatal(err)
	}
	if window.Offset() != 0 || window.End() != visibleSize {
		t.Fatalf("window = [%d,%d), want root-visible [0,%d)", window.Offset(), window.End(), visibleSize)
	}
}

func TestResolveChunkWindowFallbacksAndValidation(t *testing.T) {
	t.Run("oversized physical chunk", func(t *testing.T) {
		const size = 2 * chunkWindowTestPage
		stream := newTestStream(&codec.Manifest{
			Version:   codec.Version1,
			ImageSize: size,
			Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(size), CiphertextHash: store.ContentKey{0x51}}},
		}, 'A')
		defer stream.Close()
		anchor := requireChunkAnchor(t, stream, chunkWindowTestPage, chunkWindowTestPage)
		window, err := ResolveChunkWindow(stream, anchor, chunkWindowTestPage)
		if err != nil {
			t.Fatal(err)
		}
		if window != anchor {
			t.Fatalf("oversized window returned a different Run: %T", window)
		}
	})

	t.Run("unsupported package-private ChunkRun", func(t *testing.T) {
		stream := &readOnlyChunkLeaf{size: 2 * chunkWindowTestPage}
		anchor := requireChunkAnchor(t, stream, chunkWindowTestPage, chunkWindowTestPage)
		window, err := ResolveChunkWindow(stream, anchor, 2*chunkWindowTestPage)
		if err != nil {
			t.Fatal(err)
		}
		if window != anchor {
			t.Fatalf("unsupported window returned a different Run: %T", window)
		}
		if got := stream.runCalls.Load(); got != 1 {
			t.Fatalf("fallback performed %d RunAt calls, want only the anchor lookup", got)
		}
	})

	t.Run("anchor from another stream", func(t *testing.T) {
		const size = 2 * chunkWindowTestPage
		left := newTestStream(&codec.Manifest{
			Version:   codec.Version1,
			ImageSize: size,
			Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(size), CiphertextHash: store.ContentKey{0x61}}},
		}, 'L')
		right := newTestStream(&codec.Manifest{
			Version:   codec.Version1,
			ImageSize: size,
			Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(size), CiphertextHash: store.ContentKey{0x62}}},
		}, 'R')
		defer left.Close()
		defer right.Close()
		anchor := requireChunkAnchor(t, left, chunkWindowTestPage, chunkWindowTestPage)
		if _, err := ResolveChunkWindow(right, anchor, size); err == nil {
			t.Fatal("ResolveChunkWindow accepted an anchor from another stream")
		}
	})

	t.Run("zero limit", func(t *testing.T) {
		stream := &readOnlyChunkLeaf{size: chunkWindowTestPage}
		anchor := requireChunkAnchor(t, stream, 0, chunkWindowTestPage)
		if _, err := ResolveChunkWindow(stream, anchor, 0); err == nil {
			t.Fatal("ResolveChunkWindow accepted a zero limit")
		}
	})
}

func requireChunkAnchor(t *testing.T, stream Stream, offset, limit uint64) ChunkRun {
	t.Helper()
	run, err := stream.RunAt(offset, limit)
	if err != nil {
		t.Fatal(err)
	}
	chunk, ok := run.(ChunkRun)
	if !ok {
		t.Fatalf("run type = %T, want ChunkRun", run)
	}
	return chunk
}

// chunkWindowPrefixStream models a logical prefix such as snapshot memory
// carried in a larger manifest chunk. It preserves the underlying ChunkRun but
// makes the root-visible EOF authoritative to the resolver.
type chunkWindowPrefixStream struct {
	Stream
	size uint64
}

func (s *chunkWindowPrefixStream) Size() uint64 { return s.size }

func (s *chunkWindowPrefixStream) RunAt(offset, limit uint64) (sparse.Run, error) {
	end, err := boundedRunEnd(s.size, offset, limit)
	if err != nil {
		return nil, err
	}
	return s.Stream.RunAt(offset, end-offset)
}

func BenchmarkResolveChunkWindow(b *testing.B) {
	const size = 1 << 20
	newChunk := func(fill byte, hash byte) Stream {
		return newTestStream(&codec.Manifest{
			Version:   codec.Version1,
			ImageSize: size,
			Entries: []codec.ChunkEntry{{
				Offset:         0,
				Size:           size,
				CiphertextHash: store.ContentKey{hash},
			}},
		}, fill)
	}

	b.Run("direct", func(b *testing.B) {
		stream := newChunk('D', 0x71)
		defer stream.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			anchor, err := stream.RunAt(size/2, chunkWindowTestPage)
			if err != nil {
				b.Fatal(err)
			}
			window, err := ResolveChunkWindow(stream, anchor.(ChunkRun), size)
			if err != nil || window.Offset() != 0 || window.End() != size {
				b.Fatal(window, err)
			}
		}
	})

	b.Run("layered", func(b *testing.B) {
		top := newTestStream(&codec.Manifest{
			Version:   codec.Version1,
			ImageSize: size,
			Holes:     []sparse.Extent{{Offset: 0, Size: size}},
		}, 'T')
		stream := NewLayered(top, newChunk('L', 0x72))
		defer stream.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			anchor, err := stream.RunAt(size/2, chunkWindowTestPage)
			if err != nil {
				b.Fatal(err)
			}
			window, err := ResolveChunkWindow(stream, anchor.(ChunkRun), size)
			if err != nil || window.Offset() != 0 || window.End() != size {
				b.Fatal(window, err)
			}
		}
	})
}
