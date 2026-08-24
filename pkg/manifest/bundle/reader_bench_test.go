package bundle

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func syntheticBundle(tb testing.TB, chunkCount, chunkBytes int) []byte {
	tb.Helper()
	salt, err := store.SaltForGeneration("BENCH")
	if err != nil {
		tb.Fatal(err)
	}
	admission := store.WriteAdmission{Generation: "BENCH", Salt: salt}
	var output bytes.Buffer
	w, err := NewWriter(&output, admission, WriterOptions{Concurrency: 1})
	if err != nil {
		tb.Fatal(err)
	}
	payload := make([]byte, chunkBytes)
	if len(payload) != 0 {
		payload[0] = 1
	}
	for index := 0; index < chunkCount; index++ {
		var key store.ContentKey
		binary.LittleEndian.PutUint64(key[:8], uint64(index+1))
		if _, err := w.Put(context.Background(), admission, store.PartitionChunk, key, payload); err != nil {
			tb.Fatal(err)
		}
	}
	manifest, err := codec.Marshal(&codec.Manifest{Version: codec.Version1, ChunkMode: codec.ChunkModeFixed}, nil)
	if err != nil {
		tb.Fatal(err)
	}
	manifestKey := store.ContentKey(sha256.Sum256(manifest))
	if _, err := w.Put(context.Background(), admission, store.PartitionManifest, manifestKey, manifest); err != nil {
		tb.Fatal(err)
	}
	if err := w.Finalize(manifestKey); err != nil {
		tb.Fatal(err)
	}
	return append([]byte(nil), output.Bytes()...)
}

type byteRange struct{ start, end int64 }

type payloadCountingReaderAt struct {
	inner  io.ReaderAt
	ranges []byteRange
	reads  atomic.Int64
}

func (r *payloadCountingReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	end := offset + int64(len(dst))
	for _, candidate := range r.ranges {
		if offset < candidate.end && end > candidate.start {
			r.reads.Add(1)
			break
		}
	}
	return r.inner.ReadAt(dst, offset)
}

func TestOpenAndManifestSelectionDoNotReadChunkPayloads(t *testing.T) {
	data := syntheticBundle(t, 1_000, 256)
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	var ranges []byteRange
	for _, file := range zr.File {
		if len(file.Name) < len(chunkPrefix) || file.Name[:len(chunkPrefix)] != chunkPrefix {
			continue
		}
		offset, err := file.DataOffset()
		if err != nil {
			t.Fatal(err)
		}
		ranges = append(ranges, byteRange{start: offset, end: offset + int64(file.UncompressedSize64)})
	}
	counting := &payloadCountingReaderAt{inner: bytes.NewReader(data), ranges: ranges}
	reader, err := NewReader(counting, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if got := counting.reads.Load(); got != 0 {
		t.Fatalf("Bundle open read Chunk payload ranges %d times", got)
	}
	manifestKey := reader.ManifestKeys()[0]
	result, blob, err := reader.Getter().Get(context.Background(), store.PartitionManifest, manifestKey)
	if err != nil || blob == nil {
		t.Fatalf("Get Manifest = %v, %v", result, err)
	}
	blob.Release()
	if got := counting.reads.Load(); got != 0 {
		t.Fatalf("Manifest selection read Chunk payload ranges %d times", got)
	}
}

func BenchmarkBundleOpen4K(b *testing.B)  { benchmarkBundleOpen(b, 4_000) }
func BenchmarkBundleOpen20K(b *testing.B) { benchmarkBundleOpen(b, 20_000) }

func benchmarkBundleOpen(b *testing.B, chunks int) {
	data := syntheticBundle(b, chunks, 1)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for range b.N {
		reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			b.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
