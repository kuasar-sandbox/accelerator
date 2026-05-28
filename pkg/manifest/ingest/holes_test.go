package ingest

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/store"
)

// observingStore records each Put call's data length so tests can
// assert no hole-region bytes were ingested. Manifest blobs go to a
// separate captured slot so chunk counters stay clean.
type observingStore struct {
	mu          sync.Mutex
	chunkPuts   atomic.Int64
	chunkBytes  atomic.Int64
	manifestKey store.ContentKey
	manifest    []byte
}

func (s *observingStore) GetSalt(_ context.Context) (string, [32]byte, error) {
	return "test-gen", [32]byte{}, nil
}

func (s *observingStore) Put(_ context.Context, p store.Partition, key store.ContentKey, data []byte) (bool, error) {
	switch p {
	case store.PartitionChunk:
		s.chunkPuts.Add(1)
		s.chunkBytes.Add(int64(len(data)))
	case store.PartitionManifest:
		s.mu.Lock()
		s.manifestKey = key
		s.manifest = append([]byte(nil), data...)
		s.mu.Unlock()
	}
	return true, nil
}

// TestIngest_HolesSkipChunker — image of 12 KiB with a 4 KiB hole
// in the middle. The chunker should only see 8 KiB of data (split
// into two 4 KiB chunks); offsets recorded in the manifest must be
// image-offsets, not segment-local offsets.
func TestIngest_HolesSkipChunker(t *testing.T) {
	const (
		chunkSize = 4 * 1024
		imageSize = 12 * 1024
	)
	src := make([]byte, imageSize)
	for i := range src {
		src[i] = byte(i%127) + 1
	}
	holes := []codec.HoleExtent{
		{Offset: chunkSize, Size: chunkSize}, // hole [4K, 8K)
	}

	rec := &observingStore{}
	ing := NewIngester(testKeyFn, nil, rec, fixedChunker(t, chunkSize), fakeEncryptor())

	_, err := ing.Ingest(context.Background(), bytes.NewReader(src), imageSize, IngestOption{
		Holes: holes,
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	m, _, err := codec.Unmarshal(rec.manifest)
	if err != nil {
		t.Fatalf("codec.Unmarshal: %v", err)
	}
	// Two chunks expected (one per data segment).
	if got := len(m.Entries); got != 2 {
		t.Fatalf("Entries: got %d, want 2", got)
	}
	// Entry offsets must reflect image-offsets.
	if m.Entries[0].Offset != 0 {
		t.Errorf("entry 0 offset %d, want 0", m.Entries[0].Offset)
	}
	if m.Entries[1].Offset != 8*1024 {
		t.Errorf("entry 1 offset %d, want 8192 (post-hole)", m.Entries[1].Offset)
	}
	// Holes preserved.
	if got := len(m.Holes); got != 1 {
		t.Fatalf("Holes: got %d, want 1", got)
	}
	if m.Holes[0].Offset != chunkSize || m.Holes[0].Size != chunkSize {
		t.Errorf("hole = %+v, want {Offset:4096, Size:4096}", m.Holes[0])
	}
	// Coverage tile must validate.
	if err := m.ValidateGeometry(); err != nil {
		t.Fatalf("manifest does not tile [0, ImageSize): %v", err)
	}
	// Store saw exactly two chunk Puts (one per non-zero, non-hole chunk).
	if got := rec.chunkPuts.Load(); got != 2 {
		t.Errorf("chunk Put count: got %d, want 2", got)
	}
}

// TestIngest_RejectsHolesWithoutSeeker — Holes set but reader
// is just an io.Reader (no ReadSeeker) → error.
func TestIngest_RejectsHolesWithoutSeeker(t *testing.T) {
	rec := &observingStore{}
	ing := NewIngester(testKeyFn, nil, rec, fixedChunker(t, 4096), fakeEncryptor())

	src := bytes.NewReader(make([]byte, 8192))
	wrapper := readerWrapper{R: src}

	_, err := ing.Ingest(context.Background(), wrapper, 8192, IngestOption{
		Holes: []codec.HoleExtent{{Offset: 0, Size: 8192}},
	})
	if err == nil {
		t.Fatal("expected error when Holes is non-empty but reader is not io.ReadSeeker")
	}
}

type readerWrapper struct {
	R interface {
		Read(p []byte) (int, error)
	}
}

func (rw readerWrapper) Read(p []byte) (int, error) { return rw.R.Read(p) }

// TestIngest_NoHoles_BackwardCompat — running Ingest with no Holes
// is byte-equivalent to the pre-hole behavior.
func TestIngest_NoHoles_BackwardCompat(t *testing.T) {
	const chunkSize = 64 * 1024
	const imageSize = 4 * chunkSize
	src := make([]byte, imageSize)
	for i := range src {
		src[i] = byte(i%127) + 1
	}

	rec := &observingStore{}
	ing := NewIngester(testKeyFn, nil, rec, fixedChunker(t, chunkSize), fakeEncryptor())

	_, err := ing.Ingest(context.Background(), bytes.NewReader(src), imageSize, IngestOption{})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	m, _, err := codec.Unmarshal(rec.manifest)
	if err != nil {
		t.Fatalf("codec.Unmarshal: %v", err)
	}
	if got := len(m.Entries); got != 4 {
		t.Errorf("Entries: got %d, want 4", got)
	}
	if got := len(m.Holes); got != 0 {
		t.Errorf("Holes: got %d, want 0", got)
	}
	if got := rec.chunkPuts.Load(); got != 4 {
		t.Errorf("chunk Puts: got %d, want 4", got)
	}
}

// TestIngest_AllHoleManifest — degenerate: entire image is a single
// hole, no data segments. Should produce a manifest with 0 entries,
// 1 hole, sealed key table contains 0 keys (just AEAD overhead).
func TestIngest_AllHoleManifest(t *testing.T) {
	rec := &observingStore{}
	ing := NewIngester(testKeyFn, nil, rec, fixedChunker(t, 4096), fakeEncryptor())

	_, err := ing.Ingest(context.Background(), bytes.NewReader(nil), 1024, IngestOption{
		Holes: []codec.HoleExtent{{Offset: 0, Size: 1024}},
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	m, _, err := codec.Unmarshal(rec.manifest)
	if err != nil {
		t.Fatalf("codec.Unmarshal: %v", err)
	}
	if got := len(m.Entries); got != 0 {
		t.Errorf("Entries: got %d, want 0", got)
	}
	if got := len(m.Holes); got != 1 {
		t.Errorf("Holes: got %d, want 1", got)
	}
	if got := rec.chunkPuts.Load(); got != 0 {
		t.Errorf("chunk Puts: got %d, want 0", got)
	}
	if err := m.ValidateGeometry(); err != nil {
		t.Errorf("ValidateGeometry: %v", err)
	}
}
