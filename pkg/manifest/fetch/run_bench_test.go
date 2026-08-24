package fetch

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
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
	defer stream.Close()

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
	for _, size := range []int{4 << 10, 1 << 20} {
		b.Run(byteSizeName(size), func(b *testing.B) {
			stream := newTestManifestStream(m, make([][32]byte, 1), &staticGetter{plain: plain}, &passthroughEncryptor{plain: plain})
			defer stream.Close()
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

func BenchmarkManifestAESPhysicalRead(b *testing.B) {
	enc, dec, err := manifestcrypto.New(manifestcrypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		b.Fatal(err)
	}
	for name, plain := range map[string][]byte{
		"raw":    fetchNoise(1 << 20),
		"snappy": bytes.Repeat([]byte("snapshot-working-set\x00"), 48<<10),
	} {
		b.Run(name, func(b *testing.B) {
			object, hash, key, err := enc.EncryptChunk(context.Background(), [32]byte{0x72}, plain)
			if err != nil {
				b.Fatal(err)
			}
			entry := codec.ChunkEntry{Size: uint32(len(plain)), CiphertextHash: hash}
			stream := newTestManifestStream(&codec.Manifest{
				Version: codec.Version1, ImageSize: uint64(len(plain)), Entries: []codec.ChunkEntry{entry},
			}, [][32]byte{key}, &chunkMapGetter{chunks: map[store.ContentKey][]byte{hash: object}}, dec).(*manifestStream)
			defer stream.Close()

			b.Run("whole", func(b *testing.B) {
				dst := make([]byte, len(plain))
				b.ReportAllocs()
				b.SetBytes(int64(len(dst)))
				b.ReportMetric(float64(len(object))/float64(len(plain)), "physical/logical")
				for range b.N {
					if n, err := stream.readChunkDirect(context.Background(), dst, 0, entry, 0); err != nil || n != len(dst) {
						b.Fatal(n, err)
					}
				}
			})
			b.Run("4KiB-range", func(b *testing.B) {
				dst := make([]byte, 4<<10)
				const offset = 12345
				b.ReportAllocs()
				b.SetBytes(int64(len(dst)))
				b.ReportMetric(float64(len(object))/float64(len(plain)), "physical/logical")
				for range b.N {
					if n, err := stream.readChunkDirect(context.Background(), dst, 0, entry, offset); err != nil || n != len(dst) {
						b.Fatal(n, err)
					}
				}
			})
			b.Run("partial-miss", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(plain)))
				b.ReportMetric(float64(len(object))/float64(len(plain)), "physical/logical")
				for range b.N {
					decoded, err := stream.loadOwnedPlainChunk(context.Background(), 0, entry)
					if err != nil {
						b.Fatal(err)
					}
					clear(decoded)
				}
			})
		})
	}
}

func BenchmarkManifestAESReadLatencyQuantiles(b *testing.B) {
	enc, dec, err := manifestcrypto.New(manifestcrypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		b.Fatal(err)
	}
	for name, plain := range map[string][]byte{
		"raw":    fetchNoise(1 << 20),
		"snappy": bytes.Repeat([]byte("snapshot-working-set\x00"), 48<<10),
	} {
		object, hash, key, err := enc.EncryptChunk(context.Background(), [32]byte{0x72}, plain)
		if err != nil {
			b.Fatal(err)
		}
		entry := codec.ChunkEntry{Size: uint32(len(plain)), CiphertextHash: hash}
		stream := newTestManifestStream(&codec.Manifest{
			Version: codec.Version1, ImageSize: uint64(len(plain)), Entries: []codec.ChunkEntry{entry},
		}, [][32]byte{key}, &chunkMapGetter{chunks: map[store.ContentKey][]byte{hash: object}}, dec).(*manifestStream)
		defer stream.Close()

		whole := make([]byte, len(plain))
		page := make([]byte, 4<<10)
		for _, operation := range []struct {
			name string
			run  func() error
		}{
			{name: "whole", run: func() error {
				_, err := stream.readChunkDirect(context.Background(), whole, 0, entry, 0)
				return err
			}},
			{name: "partial-miss", run: func() error {
				plain, err := stream.loadOwnedPlainChunk(context.Background(), 0, entry)
				clear(plain)
				return err
			}},
		} {
			b.Run(name+"/"+operation.name, func(b *testing.B) {
				samples := make([]int64, b.N)
				b.ReportAllocs()
				b.ReportMetric(float64(len(object))/float64(len(plain)), "physical/logical")
				b.ResetTimer()
				for i := range b.N {
					started := time.Now()
					if err := operation.run(); err != nil {
						b.Fatal(err)
					}
					samples[i] = time.Since(started).Nanoseconds()
				}
				b.StopTimer()
				sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
				b.ReportMetric(float64(fetchPercentileNanos(samples, 0.50)), "p50-ns")
				b.ReportMetric(float64(fetchPercentileNanos(samples, 0.95)), "p95-ns")
				b.ReportMetric(float64(fetchPercentileNanos(samples, 0.99)), "p99-ns")
			})
		}

		// Fill once, then measure the #69 plaintext-cache hit path independently
		// of object encoding. The same benchmark guards RAW and Snappy against an L1
		// regression while preserving their separate fixtures.
		if _, err := stream.ReadAt(context.Background(), page, 0); err != nil {
			b.Fatal(err)
		}
		b.Run(name+"/4KiB-cache-hit", func(b *testing.B) {
			samples := make([]int64, b.N)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				started := time.Now()
				if _, err := stream.ReadAt(context.Background(), page, 0); err != nil {
					b.Fatal(err)
				}
				samples[i] = time.Since(started).Nanoseconds()
			}
			b.StopTimer()
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			b.ReportMetric(float64(fetchPercentileNanos(samples, 0.50)), "p50-ns")
			b.ReportMetric(float64(fetchPercentileNanos(samples, 0.95)), "p95-ns")
			b.ReportMetric(float64(fetchPercentileNanos(samples, 0.99)), "p99-ns")
		})
	}
}

func fetchPercentileNanos(sorted []int64, percentile float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(percentile*float64(len(sorted)-1))]
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
	defer stream.Close()
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
	defer stream.Close()
	prefetcher := stream.(Prefetcher)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := prefetcher.Prefetch(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkManifestChunkCacheWorkingSet guards the cache's intended operating
// point: 32 interleaved default-sized chunks stay hot, while a 33rd chunk
// demonstrates the bounded LRU miss cost instead of silently growing memory.
func BenchmarkManifestChunkCacheWorkingSet(b *testing.B) {
	const chunkSize = 512 << 10
	for _, chunks := range []int{32, 33} {
		b.Run(fmt.Sprintf("%d-chunks", chunks), func(b *testing.B) {
			enc := &manifestcrypto.AESChunkEncryptor{}
			entries := make([]codec.ChunkEntry, chunks)
			keys := make([][32]byte, chunks)
			stored := make(map[store.ContentKey][]byte, chunks)
			for i := range chunks {
				plain := bytes.Repeat([]byte{byte(i + 1)}, chunkSize)
				ciphertext, hash, key, err := enc.EncryptChunk(context.Background(), [32]byte{0x55}, plain)
				if err != nil {
					b.Fatal(err)
				}
				entries[i] = codec.ChunkEntry{
					Offset:         uint64(i * chunkSize),
					Size:           chunkSize,
					CiphertextHash: hash,
				}
				keys[i] = key
				stored[hash] = ciphertext
			}
			m := &codec.Manifest{
				Version:   codec.Version1,
				ImageSize: uint64(chunks * chunkSize),
				Entries:   entries,
			}
			getter := &chunkMapGetter{chunks: stored}
			stream := newTestManifestStream(m, keys, getter, &countingChunkEncryptor{inner: enc})
			defer stream.Close()
			buf := make([]byte, 4<<10)
			for i := range chunks {
				if n, err := stream.ReadAt(context.Background(), buf, uint64(i*chunkSize)); err != nil || n != len(buf) {
					b.Fatal(n, err)
				}
			}
			getter.calls.Store(0)

			b.ReportAllocs()
			b.SetBytes(int64(len(buf)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				chunk := i % chunks
				if n, err := stream.ReadAt(context.Background(), buf, uint64(chunk*chunkSize)); err != nil || n != len(buf) {
					b.Fatal(n, err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(getter.calls.Load())/float64(b.N), "gets/op")
		})
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
