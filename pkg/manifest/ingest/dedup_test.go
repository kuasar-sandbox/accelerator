package ingest

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"


	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type countingEncryptor struct {
	crypto.Encryptor
	encryptCalls atomic.Int64
}

func (c *countingEncryptor) EncryptChunk(ctx context.Context, salt [32]byte, plaintext []byte) ([]byte, [32]byte, [32]byte, error) {
	c.encryptCalls.Add(1)
	return c.Encryptor.EncryptChunk(ctx, salt, plaintext)
}

func (c *countingEncryptor) EncryptChunkWithKey(ctx context.Context, key [32]byte, plaintext []byte) ([]byte, [32]byte, error) {
	c.encryptCalls.Add(1)
	return c.Encryptor.(crypto.KeyedEncryptor).EncryptChunkWithKey(ctx, key, plaintext)
}

type countingStore struct {
	mu           sync.Mutex
	puts         atomic.Int64
	chunkPuts    atomic.Int64
	manifestPuts atomic.Int64
	seenKeys     map[store.ContentKey]int
	poolSize     int
	endpoint     string
}

func (s *countingStore) DedupIndexEndpoint() string { return s.endpoint }

func (s *countingStore) ValidateDedupIndexMarker(context.Context, store.ContentKey) (bool, error) {
	return true, nil
}

func newCountingStore(poolSize int) *countingStore {
	return &countingStore{
		seenKeys: make(map[store.ContentKey]int),
		poolSize: poolSize,
	}
}

func (s *countingStore) PoolSize() int {
	return s.poolSize
}

func (s *countingStore) AdmitWrite(ctx context.Context) (store.WriteAdmission, error) {
	return store.WriteAdmission{Generation: "test-gen", Salt: [32]byte{0x42}}, nil
}

func (s *countingStore) Put(ctx context.Context, admission store.WriteAdmission, p store.Partition, key store.ContentKey, data []byte) (bool, error) {
	s.puts.Add(1)
	if p == store.PartitionChunk {
		s.chunkPuts.Add(1)
	} else if p == store.PartitionManifest {
		s.manifestPuts.Add(1)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	count := s.seenKeys[key]
	s.seenKeys[key] = count + 1
	return count == 0, nil
}

func TestIngestSkipsRedundantEncryptionAndPut(t *testing.T) {
	const chunkSize = 4096
	enc, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	countingEnc := &countingEncryptor{Encryptor: enc}
	cStore := newCountingStore(4)
	chk := fixedChunker(t, chunkSize)

	ing := NewIngester(testKeyFn, nil, cStore, chk, countingEnc)

	// Create 10 chunks: 3 distinct chunks, repeated multiple times:
	// A, B, A, C, B, A, C, A, B, C (3 A's, 3 B's, 4 other...)
	chunkA := bytes.Repeat([]byte{0xAA}, chunkSize)
	chunkB := bytes.Repeat([]byte{0xBB}, chunkSize)
	chunkC := bytes.Repeat([]byte{0xCC}, chunkSize)

	var payload []byte
	payload = append(payload, chunkA...) // 1: A (new)
	payload = append(payload, chunkB...) // 2: B (new)
	payload = append(payload, chunkA...) // 3: A (dedup)
	payload = append(payload, chunkC...) // 4: C (new)
	payload = append(payload, chunkB...) // 5: B (dedup)
	payload = append(payload, chunkA...) // 6: A (dedup)
	payload = append(payload, chunkC...) // 7: C (dedup)
	payload = append(payload, chunkA...) // 8: A (dedup)
	payload = append(payload, chunkB...) // 9: B (dedup)
	payload = append(payload, chunkC...) // 10: C (dedup)

	src := sparse.Dense(bytes.NewReader(payload), uint64(len(payload)))
	res, err := ing.Ingest(context.Background(), src, IngestOption{})
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// 10 total chunks, 3 unique chunks (A, B, C), 7 duplicate chunks.
	if res.StoredChunks != 3 {
		t.Errorf("StoredChunks = %d, want 3", res.StoredChunks)
	}
	if res.DedupChunks != 7 {
		t.Errorf("DedupChunks = %d, want 7", res.DedupChunks)
	}
	if res.LogicalChunkBytes != uint64(10*chunkSize) {
		t.Errorf("LogicalChunkBytes = %d, want %d", res.LogicalChunkBytes, 10*chunkSize)
	}

	// EncryptChunk and store.Put for chunks should be called EXACTLY 3 times, not 10!
	if got := countingEnc.encryptCalls.Load(); got != 3 {
		t.Errorf("EncryptChunk calls = %d, want 3", got)
	}
	if got := cStore.chunkPuts.Load(); got != 3 {
		t.Errorf("chunk Put calls = %d, want 3", got)
	}
}

func TestIngestDedupAcrossConsecutiveImages(t *testing.T) {
	const chunkSize = 4096
	enc, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	countingEnc := &countingEncryptor{Encryptor: enc}
	cStore := newCountingStore(1)
	chk := fixedChunker(t, chunkSize)

	ing := NewIngester(testKeyFn, nil, cStore, chk, countingEnc)

	chunkA := bytes.Repeat([]byte{0x11}, chunkSize)
	chunkB := bytes.Repeat([]byte{0x22}, chunkSize)
	chunkC := bytes.Repeat([]byte{0x33}, chunkSize)
	chunkD := bytes.Repeat([]byte{0x44}, chunkSize)

	// Image 1 contains chunks A, B, C
	img1Data := append(append(append([]byte(nil), chunkA...), chunkB...), chunkC...)
	res1, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(img1Data), uint64(len(img1Data))), IngestOption{})
	if err != nil {
		t.Fatalf("Ingest image 1 failed: %v", err)
	}
	if res1.StoredChunks != 3 || res1.DedupChunks != 0 {
		t.Fatalf("res1 StoredChunks = %d, DedupChunks = %d, want 3/0", res1.StoredChunks, res1.DedupChunks)
	}
	if countingEnc.encryptCalls.Load() != 3 {
		t.Fatalf("Image 1 EncryptChunk calls = %d, want 3", countingEnc.encryptCalls.Load())
	}
	if cStore.chunkPuts.Load() != 3 {
		t.Fatalf("Image 1 chunk Put calls = %d, want 3", cStore.chunkPuts.Load())
	}

	// Image 2 contains chunks B, C, D (B and C are duplicate from Image 1, D is new)
	img2Data := append(append(append([]byte(nil), chunkB...), chunkC...), chunkD...)
	res2, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(img2Data), uint64(len(img2Data))), IngestOption{})
	if err != nil {
		t.Fatalf("Ingest image 2 failed: %v", err)
	}
	if res2.StoredChunks != 1 || res2.DedupChunks != 2 {
		t.Fatalf("res2 StoredChunks = %d, DedupChunks = %d, want 1/2", res2.StoredChunks, res2.DedupChunks)
	}

	// Total encryption calls should now be 4 (3 from img1 + 1 for chunk D in img2)
	if got := countingEnc.encryptCalls.Load(); got != 4 {
		t.Errorf("Total EncryptChunk calls = %d, want 4 (chunks B and C should have been skipped)", got)
	}
	if got := cStore.chunkPuts.Load(); got != 4 {
		t.Errorf("Total chunk Put calls = %d, want 4", got)
	}
}

func TestIngestDedupConcurrentWorkersInFlight(t *testing.T) {
	const chunkSize = 4096
	enc, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	countingEnc := &countingEncryptor{Encryptor: enc}
	cStore := newCountingStore(8) // 8 concurrent workers
	chk := fixedChunker(t, chunkSize)

	ing := NewIngester(testKeyFn, nil, cStore, chk, countingEnc)

	// 16 identical chunks: multiple workers will process identical chunks concurrently
	identicalChunk := bytes.Repeat([]byte{0x55}, chunkSize)
	var payload []byte
	for i := 0; i < 16; i++ {
		payload = append(payload, identicalChunk...)
	}

	src := sparse.Dense(bytes.NewReader(payload), uint64(len(payload)))
	res, err := ing.Ingest(context.Background(), src, IngestOption{})
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	if res.StoredChunks != 1 {
		t.Errorf("StoredChunks = %d, want 1", res.StoredChunks)
	}
	if res.DedupChunks != 15 {
		t.Errorf("DedupChunks = %d, want 15", res.DedupChunks)
	}

	// EncryptChunk and Put should only have been called ONCE
	if got := countingEnc.encryptCalls.Load(); got != 1 {
		t.Errorf("EncryptChunk calls = %d, want 1 (in-flight coalescing should avoid duplicate work)", got)
	}
	if got := cStore.chunkPuts.Load(); got != 1 {
		t.Errorf("chunk Put calls = %d, want 1", got)
	}
}

func TestIngestDedupCacheLRUEviction(t *testing.T) {
	const chunkSize = 4096
	enc, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	countingEnc := &countingEncryptor{Encryptor: enc}
	cStore := newCountingStore(1)
	chk := fixedChunker(t, chunkSize)

	// Limit cache to 2 entries
	ing := NewIngesterWithCacheLimit(testKeyFn, nil, cStore, chk, countingEnc, 2)

	chunkA := bytes.Repeat([]byte{0x01}, chunkSize)
	chunkB := bytes.Repeat([]byte{0x02}, chunkSize)
	chunkC := bytes.Repeat([]byte{0x03}, chunkSize)

	// 1. Ingest A, B -> cache contains [B, A]
	if _, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(append(append([]byte(nil), chunkA...), chunkB...)), 2*chunkSize), IngestOption{}); err != nil {
		t.Fatal(err)
	}
	if countingEnc.encryptCalls.Load() != 2 {
		t.Fatalf("encrypt calls = %d, want 2", countingEnc.encryptCalls.Load())
	}

	// 2. Ingest C -> cache capacity is 2, evicts oldest (A) -> cache contains [C, B]
	if _, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(chunkC), chunkSize), IngestOption{}); err != nil {
		t.Fatal(err)
	}
	if countingEnc.encryptCalls.Load() != 3 {
		t.Fatalf("encrypt calls = %d, want 3", countingEnc.encryptCalls.Load())
	}

	// 3. Ingest B -> hits cache! (B was not evicted)
	resB, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(chunkB), chunkSize), IngestOption{})
	if err != nil {
		t.Fatal(err)
	}
	if resB.DedupChunks != 1 {
		t.Errorf("chunk B DedupChunks = %d, want 1 (should hit cache)", resB.DedupChunks)
	}
	if countingEnc.encryptCalls.Load() != 3 {
		t.Errorf("encrypt calls after B = %d, want 3 (B was cached)", countingEnc.encryptCalls.Load())
	}

	// 4. Ingest A -> misses cache! (A was evicted from cache, so EncryptChunk and store.Put are called again)
	chunkPutsBeforeA := cStore.chunkPuts.Load()
	resA, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(chunkA), chunkSize), IngestOption{})
	if err != nil {
		t.Fatal(err)
	}
	if countingEnc.encryptCalls.Load() != 4 {
		t.Errorf("encrypt calls after A = %d, want 4 (A had to be re-encrypted)", countingEnc.encryptCalls.Load())
	}
	if cStore.chunkPuts.Load() != chunkPutsBeforeA+1 {
		t.Errorf("chunk Put calls after A = %d, want %d", cStore.chunkPuts.Load(), chunkPutsBeforeA+1)
	}
	// The store already had chunk A from step 1, so store.Put returned isNew=false -> DedupChunks=1
	if resA.DedupChunks != 1 {
		t.Errorf("chunk A DedupChunks = %d, want 1", resA.DedupChunks)
	}
}

type mockOrderedSkipperStore struct {
	StoreWriter
	mu          sync.Mutex
	nextOrdinal uint64
	stored      map[store.ContentKey]uint64
	skipCalls   atomic.Int64
	putCalls    atomic.Int64
}

func newMockOrderedSkipperStore(sw StoreWriter) *mockOrderedSkipperStore {
	return &mockOrderedSkipperStore{
		StoreWriter: sw,
		stored:      make(map[store.ContentKey]uint64),
	}
}

func (m *mockOrderedSkipperStore) PutChunkOrdered(ctx context.Context, admission store.WriteAdmission, key store.ContentKey, data []byte, ordinal uint64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putCalls.Add(1)
	m.stored[key] = uint64(len(data))
	m.nextOrdinal++
	return true, nil
}

func (m *mockOrderedSkipperStore) SkipChunkOrdered(ctx context.Context, admission store.WriteAdmission, key store.ContentKey, size uint64, ordinal uint64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.stored[key]; !ok {
		return false, nil
	}
	m.skipCalls.Add(1)
	m.nextOrdinal++
	return true, nil
}

func TestIngestDedupWithOrderedStoreSkipper(t *testing.T) {
	const chunkSize = 4096
	enc, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	countingEnc := &countingEncryptor{Encryptor: enc}
	baseStore := newCountingStore(4)
	mockOrdered := newMockOrderedSkipperStore(baseStore)
	chk := fixedChunker(t, chunkSize)

	ing := NewIngester(testKeyFn, nil, mockOrdered, chk, countingEnc)

	chunkA := bytes.Repeat([]byte{0x77}, chunkSize)
	chunkB := bytes.Repeat([]byte{0x88}, chunkSize)

	// A, B, A, B
	var payload []byte
	payload = append(payload, chunkA...)
	payload = append(payload, chunkB...)
	payload = append(payload, chunkA...)
	payload = append(payload, chunkB...)

	src := sparse.Dense(bytes.NewReader(payload), uint64(len(payload)))
	res, err := ing.Ingest(context.Background(), src, IngestOption{})
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	if res.StoredChunks != 2 || res.DedupChunks != 2 {
		t.Fatalf("StoredChunks = %d, DedupChunks = %d, want 2/2", res.StoredChunks, res.DedupChunks)
	}
	if mockOrdered.putCalls.Load() != 2 {
		t.Errorf("PutChunkOrdered calls = %d, want 2", mockOrdered.putCalls.Load())
	}
	if mockOrdered.skipCalls.Load() != 2 {
		t.Errorf("SkipChunkOrdered calls = %d, want 2", mockOrdered.skipCalls.Load())
	}
	if countingEnc.encryptCalls.Load() != 2 {
		t.Errorf("EncryptChunk calls = %d, want 2", countingEnc.encryptCalls.Load())
	}
}

func TestIngestDeterministicManifestOutput(t *testing.T) {
	const chunkSize = 4096
	enc, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}

	chunkA := bytes.Repeat([]byte{0x12}, chunkSize)
	chunkB := bytes.Repeat([]byte{0x34}, chunkSize)
	payload := append(append(append([]byte(nil), chunkA...), chunkB...), chunkA...)

	// Run 1: Cold (3 chunks, 1 duplicate)
	store1 := newCountingStore(1)
	ing1 := NewIngester(testKeyFn, nil, store1, fixedChunker(t, chunkSize), enc)
	res1, err := ing1.Ingest(context.Background(), sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), IngestOption{})
	if err != nil {
		t.Fatal(err)
	}

	// Run 2: Pre-warmed ingester (chunks already in cache)
	res2, err := ing1.Ingest(context.Background(), sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), IngestOption{})
	if err != nil {
		t.Fatal(err)
	}

	// ManifestKey must be 100% bit-for-bit identical!
	if res1.ManifestKey != res2.ManifestKey {
		t.Fatalf("ManifestKey changed between cold and warm ingest: %x != %x", res1.ManifestKey, res2.ManifestKey)
	}
	if res1.LogicalChunkBytes != res2.LogicalChunkBytes {
		t.Fatalf("LogicalChunkBytes differs: %d != %d", res1.LogicalChunkBytes, res2.LogicalChunkBytes)
	}
	if res1.EncodedChunkBytes != res2.EncodedChunkBytes {
		t.Fatalf("EncodedChunkBytes differs: %d != %d", res1.EncodedChunkBytes, res2.EncodedChunkBytes)
	}
	if res1.RawChunks != res2.RawChunks || res1.CompressedChunks != res2.CompressedChunks {
		t.Fatalf("Format chunk counts differ: raw %d/%d, compressed %d/%d", res1.RawChunks, res2.RawChunks, res1.CompressedChunks, res2.CompressedChunks)
	}
}

func TestIngestDedupPersistsAcrossIngesters(t *testing.T) {
	indexPath := filepath.Join(t.TempDir(), "dedup.db")
	t.Setenv(dedupIndexEnv, indexPath)
	const chunkSize = 4096
	chunkA := bytes.Repeat([]byte{0x61}, chunkSize)
	chunkB := bytes.Repeat([]byte{0x62}, chunkSize)
	payload := append(append(append([]byte(nil), chunkA...), chunkB...), chunkA...)

	enc1, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	counting1 := &countingEncryptor{Encryptor: enc1}
	store1 := newCountingStore(4)
	store1.endpoint = "unix:///test/store"
	ing1 := NewIngester(testKeyFn, nil, store1, fixedChunker(t, chunkSize), counting1)
	res1, err := ing1.Ingest(context.Background(), sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), IngestOption{})
	if err != nil {
		t.Fatal(err)
	}
	if got := counting1.encryptCalls.Load(); got != 2 {
		t.Fatalf("first ingester encrypt calls = %d, want 2", got)
	}

	enc2, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	counting2 := &countingEncryptor{Encryptor: enc2}
	store2 := newCountingStore(4)
	store2.endpoint = "unix:///test/store"
	ing2 := NewIngester(testKeyFn, nil, store2, fixedChunker(t, chunkSize), counting2)
	res2, err := ing2.Ingest(context.Background(), sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), IngestOption{})
	if err != nil {
		t.Fatal(err)
	}
	if got := counting2.encryptCalls.Load(); got != 0 {
		t.Fatalf("second ingester encrypt calls = %d, want 0", got)
	}
	if got := store2.chunkPuts.Load(); got != 0 {
		t.Fatalf("second ingester chunk Puts = %d, want 0", got)
	}
	if res2.StoredChunks != 0 || res2.DedupChunks != 3 {
		t.Fatalf("second ingester StoredChunks/DedupChunks = %d/%d, want 0/3", res2.StoredChunks, res2.DedupChunks)
	}
	if res1.ManifestKey != res2.ManifestKey {
		t.Fatalf("persistent dedup changed manifest key: %x != %x", res1.ManifestKey, res2.ManifestKey)
	}
	if res1.LogicalChunkBytes != res2.LogicalChunkBytes || res1.EncodedChunkBytes != res2.EncodedChunkBytes {
		t.Fatalf("persistent dedup changed chunk metrics: %+v vs %+v", res1, res2)
	}
}

func TestIngestPersistentDedupCanBeDisabled(t *testing.T) {
	t.Setenv(dedupIndexEnv, "off")
	const chunkSize = 4096
	payload := bytes.Repeat([]byte{0x63}, chunkSize)
	enc1, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	store1 := newCountingStore(1)
	store1.endpoint = "unix:///test/store"
	if _, err := NewIngester(testKeyFn, nil, store1, fixedChunker(t, chunkSize), enc1).Ingest(context.Background(), sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), IngestOption{}); err != nil {
		t.Fatal(err)
	}

	enc2, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	counting2 := &countingEncryptor{Encryptor: enc2}
	res, err := NewIngester(testKeyFn, nil, store1, fixedChunker(t, chunkSize), counting2).Ingest(context.Background(), sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), IngestOption{})
	if err != nil {
		t.Fatal(err)
	}
	if got := counting2.encryptCalls.Load(); got != 1 {
		t.Fatalf("disabled index encrypt calls = %d, want 1", got)
	}
	if res.StoredChunks != 0 || res.DedupChunks != 1 {
		t.Fatalf("disabled index StoredChunks/DedupChunks = %d/%d, want 0/1", res.StoredChunks, res.DedupChunks)
	}
}

type failingKeyedEncryptor struct {
	crypto.Encryptor
	err error
}

func (e failingKeyedEncryptor) EncryptChunk(context.Context, [32]byte, []byte) ([]byte, [32]byte, [32]byte, error) {
	return nil, [32]byte{}, [32]byte{}, e.err
}

func (e failingKeyedEncryptor) EncryptChunkWithKey(context.Context, [32]byte, []byte) ([]byte, [32]byte, error) {
	return nil, [32]byte{}, e.err
}

func TestIngestCleansFlightsAfterEncryptionError(t *testing.T) {
	enc, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	failing := failingKeyedEncryptor{Encryptor: enc, err: errors.New("intentional encrypt failure")}
	ing := NewIngester(testKeyFn, nil, newCountingStore(4), fixedChunker(t, 4096), failing).(*ingester)
	payload := bytes.Repeat([]byte{0x71}, 4096*8)
	if _, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), IngestOption{}); err == nil {
		t.Fatal("Ingest unexpectedly succeeded")
	}
	ing.dedupMu.Lock()
	flights := len(ing.dedupFlights)
	ing.dedupMu.Unlock()
	if flights != 0 {
		t.Fatalf("in-flight dedup flights after error = %d, want 0", flights)
	}
}

type saltCountingStore struct {
	*countingStore
	salt [32]byte
}

func (s *saltCountingStore) AdmitWrite(context.Context) (store.WriteAdmission, error) {
	return store.WriteAdmission{Generation: "test-gen", Salt: s.salt}, nil
}

func TestIngestPersistentDedupIsSaltScoped(t *testing.T) {
	t.Setenv(dedupIndexEnv, filepath.Join(t.TempDir(), "dedup.db"))
	const chunkSize = 4096
	payload := bytes.Repeat([]byte{0x64}, chunkSize)

	base1 := newCountingStore(1)
	base1.endpoint = "unix:///test/store"
	store1 := &saltCountingStore{countingStore: base1, salt: [32]byte{1}}
	enc1, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewIngester(testKeyFn, nil, store1, fixedChunker(t, chunkSize), enc1).Ingest(context.Background(), sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), IngestOption{}); err != nil {
		t.Fatal(err)
	}

	base2 := newCountingStore(1)
	base2.endpoint = "unix:///test/store"
	store2 := &saltCountingStore{countingStore: base2, salt: [32]byte{2}}
	enc2, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	counting2 := &countingEncryptor{Encryptor: enc2}
	if _, err := NewIngester(testKeyFn, nil, store2, fixedChunker(t, chunkSize), counting2).Ingest(context.Background(), sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), IngestOption{}); err != nil {
		t.Fatal(err)
	}
	if got := counting2.encryptCalls.Load(); got != 1 {
		t.Fatalf("different salt encrypt calls = %d, want 1", got)
	}
}

type markerInvalidatingStore struct {
	*countingStore
}

func (s *markerInvalidatingStore) ValidateDedupIndexMarker(context.Context, store.ContentKey) (bool, error) {
	return false, nil
}

func TestIngestPersistentDedupResetsStaleIndex(t *testing.T) {
	t.Setenv(dedupIndexEnv, filepath.Join(t.TempDir(), "dedup.db"))
	const chunkSize = 4096
	payload := bytes.Repeat([]byte{0x65}, chunkSize)

	enc1, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	base1 := newCountingStore(1)
	base1.endpoint = "unix:///test/store"
	if _, err := NewIngester(testKeyFn, nil, base1, fixedChunker(t, chunkSize), enc1).Ingest(context.Background(), sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), IngestOption{}); err != nil {
		t.Fatal(err)
	}

	base2 := newCountingStore(1)
	base2.endpoint = "unix:///test/store"
	stale := &markerInvalidatingStore{countingStore: base2}
	enc2, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	counting2 := &countingEncryptor{Encryptor: enc2}
	res2, err := NewIngester(testKeyFn, nil, stale, fixedChunker(t, chunkSize), counting2).Ingest(context.Background(), sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), IngestOption{})
	if err != nil {
		t.Fatal(err)
	}
	if got := counting2.encryptCalls.Load(); got != 1 {
		t.Fatalf("stale index encrypt calls = %d, want 1", got)
	}
	if res2.StoredChunks != 1 {
		t.Fatalf("stale index StoredChunks = %d, want 1", res2.StoredChunks)
	}

	base3 := newCountingStore(1)
	base3.endpoint = "unix:///test/store"
	enc3, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	counting3 := &countingEncryptor{Encryptor: enc3}
	res3, err := NewIngester(testKeyFn, nil, base3, fixedChunker(t, chunkSize), counting3).Ingest(context.Background(), sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), IngestOption{})
	if err != nil {
		t.Fatal(err)
	}
	if got := counting3.encryptCalls.Load(); got != 0 {
		t.Fatalf("rebuilt index encrypt calls = %d, want 0", got)
	}
	if res3.StoredChunks != 0 || res3.DedupChunks != 1 {
		t.Fatalf("rebuilt index StoredChunks/DedupChunks = %d/%d, want 0/1", res3.StoredChunks, res3.DedupChunks)
	}
}

func TestPersistentPendingIsBounded(t *testing.T) {
	indexPath := filepath.Join(t.TempDir(), "dedup.db")
	t.Setenv(dedupIndexEnv, indexPath)
	const chunkSize = 4096
	var payload []byte
	for i := 0; i < 5; i++ {
		payload = append(payload, bytes.Repeat([]byte{byte(0x80 + i)}, chunkSize)...)
	}
	enc, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	store := newCountingStore(1)
	store.endpoint = "unix:///test/store"
	ing := NewIngesterWithCacheLimit(testKeyFn, nil, store, fixedChunker(t, chunkSize), enc, 2)
	if _, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), IngestOption{}); err != nil {
		t.Fatal(err)
	}
	p, err := openPersistentChunkIndexRead(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if count := p.entryCount(); count != 2 {
		t.Fatalf("persistent pending entries = %d, want 2", count)
	}
}
