package fetch

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

func BenchmarkLayeredRunAt(b *testing.B) {
	const size = uint64(1 << 20)
	upper := newManifestStream(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: size,
		Entries:   []codec.ChunkEntry{{Offset: size / 2, Size: uint32(size / 2)}},
		Holes:     []sparse.Extent{{Offset: 0, Size: size / 2}},
	}, nil, nil, nil, nil)
	lower := newManifestStream(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: size,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(size)}},
	}, nil, nil, nil, nil)
	stream := NewLayered(upper, lower)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := uint64(i) & ((size / 2) - 1)
		run, err := stream.RunAt(offset, 4096)
		if err != nil || run.End() <= run.Offset() {
			b.Fatal(run, err)
		}
	}
}

func BenchmarkManifestReadAt(b *testing.B) {
	const chunkSize = 1 << 20
	plain := bytes.Repeat([]byte{0x5A}, chunkSize)
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: chunkSize,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: chunkSize}},
	}
	stampHashes(m, plain)
	stream := newTestManifestStream(m, make([][32]byte, 1), &staticGetter{plain: plain}, &passthroughEncryptor{plain: plain})

	for _, size := range []int{4 << 10, 1 << 20} {
		b.Run(byteSizeName(size), func(b *testing.B) {
			buf := make([]byte, size)
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if n, err := stream.ReadAt(context.Background(), buf, 0); err != nil || n != len(buf) {
					b.Fatal(n, err)
				}
			}
		})
	}
}

func BenchmarkTarStreamReadAt(b *testing.B) {
	const imageSize = 8 << 20
	stream := openBenchmarkTarStream(b, imageSize)
	defer stream.Close()

	b.Run("4KiB-random", func(b *testing.B) {
		buf := make([]byte, 4<<10)
		b.ReportAllocs()
		b.SetBytes(int64(len(buf)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			offset := (uint64(i) * 104729) % (imageSize - uint64(len(buf)))
			if n, err := stream.ReadAt(context.Background(), buf, offset); err != nil || n != len(buf) {
				b.Fatal(n, err)
			}
		}
	})

	b.Run("1MiB-continuous", func(b *testing.B) {
		buf := make([]byte, 1<<20)
		b.ReportAllocs()
		b.SetBytes(int64(len(buf)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			offset := (uint64(i) * uint64(len(buf))) % imageSize
			if n, err := stream.ReadAt(context.Background(), buf, offset); err != nil || n != len(buf) {
				b.Fatal(n, err)
			}
		}
	})
}

func BenchmarkStreamReadAtMultiChunk(b *testing.B) {
	const (
		chunkSize = 64 << 10
		chunks    = 16
	)
	plain := bytes.Repeat([]byte{0xA5}, chunkSize)
	entries := make([]codec.ChunkEntry, chunks)
	for i := range entries {
		entries[i] = codec.ChunkEntry{Offset: uint64(i * chunkSize), Size: chunkSize}
	}
	m := &codec.Manifest{Version: codec.Version1, ImageSize: chunks * chunkSize, Entries: entries}
	stampHashes(m, plain)
	stream := newTestManifestStream(m, make([][32]byte, chunks), &staticGetter{plain: plain}, &passthroughEncryptor{plain: plain})
	buf := make([]byte, m.ImageSize)

	b.ReportAllocs()
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n, err := stream.ReadAt(context.Background(), buf, 0); err != nil || n != len(buf) {
			b.Fatal(n, err)
		}
	}
}

func BenchmarkPrefetchTraversal(b *testing.B) {
	const (
		chunkSize = 4 << 10
		chunks    = 256
	)
	entries := make([]codec.ChunkEntry, chunks)
	for i := range entries {
		entries[i] = codec.ChunkEntry{
			Offset:         uint64(i * chunkSize),
			Size:           chunkSize,
			CiphertextHash: store.ContentKey{byte(i)},
		}
	}
	m := &codec.Manifest{Version: codec.Version1, ImageSize: chunks * chunkSize, Entries: entries}
	stream := newTestManifestStream(m, make([][32]byte, chunks), benchmarkPrefetchGetter{}, nil)
	prefetcher := stream.(Prefetcher)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := prefetcher.Prefetch(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

type benchmarkPrefetchGetter struct{}

func (benchmarkPrefetchGetter) Get(context.Context, store.Partition, store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	return cache.CacheHit, cache.NewMemBlob(nil), nil
}

func openBenchmarkTarStream(b *testing.B, size int) Stream {
	b.Helper()
	path := filepath.Join(b.TempDir(), "benchmark.tar")
	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0x3C}, size)
	source, err := sparse.NewSource(bytes.NewReader(payload), uint64(size), nil)
	if err != nil {
		b.Fatal(err)
	}
	if _, _, err := tarstream.WriteTo(context.Background(), f, "image", source); err != nil {
		b.Fatal(err)
	}
	if err := f.Close(); err != nil {
		b.Fatal(err)
	}
	stream, err := OpenTarStream(path)
	if err != nil {
		b.Fatal(err)
	}
	return stream
}

func byteSizeName(size int) string {
	if size == 4<<10 {
		return "4KiB"
	}
	return "1MiB"
}
