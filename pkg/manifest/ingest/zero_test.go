package ingest

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// recordingStore is a StoreWriter that records every Put call and
// captures the marshaled manifest blob for inspection.
type recordingStore struct {
	mu          sync.Mutex
	chunkPuts   []store.ContentKey
	chunkBytes  int
	manifestKey store.ContentKey
	manifest    []byte
}

func (s *recordingStore) GetSalt(_ context.Context) ([32]byte, error) {
	return [32]byte{}, nil
}

func (s *recordingStore) Put(_ context.Context, p store.Partition, key store.ContentKey, data []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch p {
	case store.PartitionChunk:
		s.chunkPuts = append(s.chunkPuts, key)
		s.chunkBytes += len(data)
	case store.PartitionManifest:
		s.manifestKey = key
		s.manifest = append([]byte(nil), data...)
	}
	return true, nil
}

func testKeyFn() ([32]byte, error) {
	return [32]byte{}, nil
}

// fixedChunker pins the chunker to predictable, known-size chunks so
// tests can reason about exact chunk counts.
func fixedChunker(t *testing.T, size uint32) chunker.Chunker {
	t.Helper()
	c, err := chunker.New(chunker.Config{
		Mode:  "fixed",
		Fixed: chunker.FixedConfig{Size: sizeStr(size)},
	})
	if err != nil {
		t.Fatalf("chunker.New: %v", err)
	}
	return c
}

func sizeStr(n uint32) string {
	// chunker accepts bare decimal sizes — emit them directly.
	return uintToString(uint64(n))
}

func uintToString(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func testEncryptor() crypto.Encryptor {
	enc, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		panic(err)
	}
	return enc
}

// loadManifest re-parses the manifest blob captured by recordingStore.
// Tests use it instead of inspecting an in-memory pointer (the new
// Result only exposes ManifestKey).
func loadManifest(t *testing.T, rec *recordingStore) *codec.Manifest {
	t.Helper()
	if rec.manifest == nil {
		t.Fatal("recordingStore: no manifest blob captured")
	}
	m, _, err := codec.Unmarshal(rec.manifest)
	if err != nil {
		t.Fatalf("codec.Unmarshal: %v", err)
	}
	return m
}

// TestIngest_AllZeroNoStorePut — fully-zero input must not touch the
// chunk store (no chunk Puts, no chunk bytes). The manifest blob is
// still written.
func TestIngest_AllZeroNoStorePut(t *testing.T) {
	rec := &recordingStore{}
	ing := NewIngester(testKeyFn, nil, rec, fixedChunker(t, 64*1024), testEncryptor())

	const chunkSize = 64 * 1024
	const numChunks = 4
	input := make([]byte, chunkSize*numChunks)

	res, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(input), uint64(len(input))), IngestOption{})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	if len(rec.chunkPuts) != 0 {
		t.Errorf("recordingStore got %d chunk Puts; expected 0 for all-zero input", len(rec.chunkPuts))
	}
	if rec.chunkBytes != 0 {
		t.Errorf("recordingStore got %d chunk bytes; expected 0", rec.chunkBytes)
	}
	if res.ZeroChunks != numChunks {
		t.Errorf("ZeroChunks=%d, want %d", res.ZeroChunks, numChunks)
	}
	if res.StoredChunks != 0 || res.DedupChunks != 0 || res.StoredBytes != 0 {
		t.Errorf("unexpected non-zero counters: stored=%d dedup=%d bytes=%d", res.StoredChunks, res.DedupChunks, res.StoredBytes)
	}
	if res.ManifestKey != rec.manifestKey {
		t.Errorf("Result.ManifestKey != captured manifest key")
	}
	m := loadManifest(t, rec)
	for i, e := range m.Entries {
		if !e.IsZero {
			t.Errorf("entry %d: IsZero=false, want true", i)
		}
	}
}

// TestIngest_KeyTableCompressed — for a half-zero input the sealed
// key table must hold exactly M=non-zero-chunk-count keys (32 B each)
// after the AEAD overhead. AES-GCM's overhead is deterministic too
// (1 flag + 12 synthesized-nonce + 16 tag), so the size is exact.
func TestIngest_KeyTableCompressed(t *testing.T) {
	rec := &recordingStore{}
	ing := NewIngester(testKeyFn, nil, rec, fixedChunker(t, 64*1024), testEncryptor())

	const chunkSize = 64 * 1024
	const numChunks = 4
	input := make([]byte, chunkSize*numChunks)
	// First two chunks: non-zero data. Last two: zero.
	for i := 0; i < 2*chunkSize; i++ {
		input[i] = byte(i%255) + 1
	}

	res, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(input), uint64(len(input))), IngestOption{})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	if res.ZeroChunks != 2 {
		t.Errorf("ZeroChunks=%d, want 2", res.ZeroChunks)
	}
	if res.StoredChunks != 2 {
		t.Errorf("StoredChunks=%d, want 2", res.StoredChunks)
	}
	if len(rec.chunkPuts) != 2 {
		t.Errorf("chunk Put calls: %d, want 2 (one per non-zero chunk)", len(rec.chunkPuts))
	}

	// Reparse the captured manifest. The sealed key table is the tail
	// of the blob; we recompute its expected length.
	m, sealed, err := codec.Unmarshal(rec.manifest)
	if err != nil {
		t.Fatalf("codec.Unmarshal: %v", err)
	}
	// AES-GCM key-table seal format: [1 byte flag][12 byte nonce][N*32 byte
	// keys][16 byte tag] → expected length = 29 + (non-zero chunk count) * 32
	const gcmOverhead = 1 + 12 + 16 // flag + synthesized nonce + GCM tag
	wantKeyTableLen := gcmOverhead + 2*32
	if got := len(sealed); got != wantKeyTableLen {
		t.Errorf("sealed key table size %d, want %d (compressed: only 2 non-zero keys)", got, wantKeyTableLen)
	}
	// Manifest IsZero flags must match what ingest reported.
	zeros := 0
	for _, e := range m.Entries {
		if e.IsZero {
			zeros++
		}
	}
	if zeros != 2 {
		t.Errorf("manifest IsZero count %d, want 2", zeros)
	}
}

// TestIngest_AllZeroSealedTableTinyAndDeterministic — empty key table
// (when EVERY chunk is zero) seals to exactly the AEAD/HMAC overhead;
// no key bytes at all.
func TestIngest_AllZeroSealedTableTinyAndDeterministic(t *testing.T) {
	rec := &recordingStore{}
	ing := NewIngester(testKeyFn, nil, rec, fixedChunker(t, 64*1024), testEncryptor())

	const chunkSize = 64 * 1024
	input := make([]byte, 4*chunkSize)

	_, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(input), uint64(len(input))), IngestOption{})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	_, sealed, err := codec.Unmarshal(rec.manifest)
	if err != nil {
		t.Fatalf("codec.Unmarshal: %v", err)
	}
	const gcmOverhead = 1 + 12 + 16 // flag + synthesized nonce + GCM tag
	if len(sealed) != gcmOverhead {
		t.Errorf("sealed key table size %d, want %d (no keys, just AEAD overhead)", len(sealed), gcmOverhead)
	}
}
