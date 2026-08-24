package fetch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestManifestChunkCacheDefaults(t *testing.T) {
	if manifestChunkCacheMaxEntries != 32 {
		t.Fatalf("entry limit = %d, want 32", manifestChunkCacheMaxEntries)
	}
	if manifestChunkCacheMaxBytes != 32<<20 {
		t.Fatalf("byte limit = %d, want 32 MiB", manifestChunkCacheMaxBytes)
	}
	if manifestChunkCacheIdleTTL != 5*time.Second {
		t.Fatalf("idle TTL = %s, want 5s", manifestChunkCacheIdleTTL)
	}
}

func TestManifestPartialReadsReuseDecryptedChunk(t *testing.T) {
	const chunkSize = 1 << 20
	plain := make([]byte, chunkSize)
	for i := range plain {
		plain[i] = byte(i % 251)
	}
	enc := &countingChunkEncryptor{inner: &manifestcrypto.AESChunkEncryptor{}}
	ciphertext, hash, key, err := enc.inner.EncryptChunk(context.Background(), [32]byte{0x31}, plain)
	if err != nil {
		t.Fatal(err)
	}
	if ciphertext[0] != manifestcrypto.ChunkFormatAESSnappy {
		t.Fatalf("fixture format = %#x, want Snappy", ciphertext[0])
	}
	originalCiphertext := append([]byte(nil), ciphertext...)
	getter := &chunkMapGetter{chunks: map[store.ContentKey][]byte{hash: ciphertext}}
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: chunkSize,
		Entries: []codec.ChunkEntry{{
			Offset:         0,
			Size:           chunkSize,
			CiphertextHash: hash,
		}},
	}
	stream := newTestManifestStream(m, [][32]byte{key}, getter, enc)
	defer stream.Close()

	for offset := 0; offset < chunkSize; offset += 4 << 10 {
		buf := make([]byte, 4<<10)
		n, err := stream.ReadAt(context.Background(), buf, uint64(offset))
		if err != nil || n != len(buf) {
			t.Fatalf("ReadAt(%d) = (%d, %v)", offset, n, err)
		}
		if !bytes.Equal(buf, plain[offset:offset+len(buf)]) {
			t.Fatalf("ReadAt(%d) returned wrong data", offset)
		}
	}
	if got := getter.calls.Load(); got != 1 {
		t.Fatalf("Get calls = %d, want 1", got)
	}
	if got := enc.decrypts.Load(); got != 1 {
		t.Fatalf("decrypt calls = %d, want 1", got)
	}
	if !bytes.Equal(ciphertext, originalCiphertext) {
		t.Fatal("partial read mutated the immutable ciphertext Blob")
	}
}

func TestManifestChunkCacheLRUWorkingSet(t *testing.T) {
	cache := newDecryptedChunkCache(8, 1<<20, time.Hour)
	defer cache.close()
	ctx := context.Background()
	var loads atomic.Int64
	load := func(value byte) func(context.Context) ([]byte, error) {
		return func(context.Context) ([]byte, error) {
			loads.Add(1)
			return bytes.Repeat([]byte{value}, 4096), nil
		}
	}

	for i := byte(0); i < 8; i++ {
		lease, err := cache.acquireOrLoad(ctx, testDecryptedChunkKey(i), load(i))
		if err != nil {
			t.Fatal(err)
		}
		lease.release()
	}
	// Refresh chunk 0 so chunk 1 becomes the least-recently-used entry.
	lease := cache.acquire(testDecryptedChunkKey(0))
	if lease == nil {
		t.Fatal("working-set entry 0 was not cached")
	}
	lease.release()

	lease, err := cache.acquireOrLoad(ctx, testDecryptedChunkKey(8), load(8))
	if err != nil {
		t.Fatal(err)
	}
	lease.release()
	if got := loads.Load(); got != 9 {
		t.Fatalf("loads after ninth key = %d, want 9", got)
	}
	if cache.acquire(testDecryptedChunkKey(1)) != nil {
		t.Fatal("least-recently-used entry 1 survived ninth insertion")
	}
	lease = cache.acquire(testDecryptedChunkKey(0))
	if lease == nil {
		t.Fatal("recently-used entry 0 was evicted")
	}
	lease.release()

	cache.mu.Lock()
	entries, residentBytes := len(cache.entries), cache.bytes
	cache.mu.Unlock()
	if entries != 8 || residentBytes != 8*4096 {
		t.Fatalf("resident entries/bytes = %d/%d, want 8/%d", entries, residentBytes, 8*4096)
	}
}

func TestManifestChunkCacheByteBudgetAndOversizeBypass(t *testing.T) {
	const fiveMiB = 5 << 20
	cache := newDecryptedChunkCache(8, 8<<20, time.Hour)
	defer cache.close()
	ctx := context.Background()

	first := bytes.Repeat([]byte{0x11}, fiveMiB)
	lease, err := cache.acquireOrLoad(ctx, testDecryptedChunkKey(1), func(context.Context) ([]byte, error) {
		return first, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease.release()
	second := bytes.Repeat([]byte{0x22}, fiveMiB)
	lease, err = cache.acquireOrLoad(ctx, testDecryptedChunkKey(2), func(context.Context) ([]byte, error) {
		return second, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease.release()
	if cache.acquire(testDecryptedChunkKey(1)) != nil {
		t.Fatal("byte budget did not evict the older 5 MiB entry")
	}
	if lease = cache.acquire(testDecryptedChunkKey(2)); lease == nil {
		t.Fatal("newer 5 MiB entry was not retained")
	} else {
		lease.release()
	}
	if !allZero(first) {
		t.Fatal("byte-budget eviction did not clear plaintext")
	}

	oversize := bytes.Repeat([]byte{0x33}, (8<<20)+1)
	lease, err = cache.acquireOrLoad(ctx, testDecryptedChunkKey(3), func(context.Context) ([]byte, error) {
		return oversize, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if cache.acquire(testDecryptedChunkKey(3)) != nil {
		t.Fatal("oversize plaintext was admitted")
	}
	lease.release()
	if !allZero(oversize) {
		t.Fatal("non-admitted oversize plaintext was not cleared after use")
	}
}

func TestManifestChunkCacheCoalescesConcurrentMiss(t *testing.T) {
	cache := newDecryptedChunkCache(8, 1<<20, time.Hour)
	defer cache.close()
	key := testDecryptedChunkKey(7)
	started := make(chan struct{})
	release := make(chan struct{})
	var loads atomic.Int64
	loader := func(context.Context) ([]byte, error) {
		if loads.Add(1) == 1 {
			close(started)
		}
		<-release
		return bytes.Repeat([]byte{0x7A}, 4096), nil
	}

	const readers = 32
	start := make(chan struct{})
	errs := make(chan error, readers)
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			lease, err := cache.acquireOrLoad(context.Background(), key, loader)
			if err != nil {
				errs <- err
				return
			}
			if got := lease.bytes()[0]; got != 0x7A {
				errs <- fmt.Errorf("cached byte = %#x, want %#x", got, byte(0x7A))
			}
			lease.release()
		}()
	}
	close(start)
	<-started
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("concurrent loads = %d, want 1", got)
	}
}

func TestManifestChunkCacheActiveIdleExpiry(t *testing.T) {
	const ttl = 25 * time.Millisecond
	cache := newDecryptedChunkCache(8, 1<<20, ttl)
	plain := bytes.Repeat([]byte{0x5C}, 4096)
	lease, err := cache.acquireOrLoad(context.Background(), testDecryptedChunkKey(1), func(context.Context) ([]byte, error) {
		return plain, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease.release()

	deadline := time.Now().Add(2 * time.Second)
	for {
		cache.mu.Lock()
		resident := len(cache.entries)
		cleared := allZero(plain)
		cache.mu.Unlock()
		if resident == 0 {
			if !cleared {
				t.Fatal("expired plaintext was not cleared")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("idle entry was not actively expired")
		}
		time.Sleep(time.Millisecond)
	}
	cache.close()
}

func TestManifestChunkCacheDefersClearForPinnedReader(t *testing.T) {
	cache := newDecryptedChunkCache(1, 1<<20, time.Hour)
	defer cache.close()
	first := bytes.Repeat([]byte{0x41}, 4096)
	leaseA, err := cache.acquireOrLoad(context.Background(), testDecryptedChunkKey(1), func(context.Context) ([]byte, error) {
		return first, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseB, err := cache.acquireOrLoad(context.Background(), testDecryptedChunkKey(2), func(context.Context) ([]byte, error) {
		return bytes.Repeat([]byte{0x42}, 4096), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseB.release()
	if allZero(first) {
		t.Fatal("eviction cleared plaintext while a reader was pinned")
	}
	if leaseA.bytes()[0] != 0x41 {
		t.Fatal("pinned reader lost its plaintext")
	}
	leaseA.release()
	if !allZero(first) {
		t.Fatal("retired plaintext was not cleared after the final reader")
	}
}

func TestManifestChunkCacheCloseClearsResidentPlaintext(t *testing.T) {
	cache := newDecryptedChunkCache(8, 1<<20, time.Hour)
	plain := bytes.Repeat([]byte{0x6D}, 4096)
	lease, err := cache.acquireOrLoad(context.Background(), testDecryptedChunkKey(1), func(context.Context) ([]byte, error) {
		return plain, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease.release()
	cache.close()
	cache.close()
	if !allZero(plain) {
		t.Fatal("Close did not clear resident plaintext")
	}
	if cache.acquire(testDecryptedChunkKey(1)) != nil {
		t.Fatal("closed cache returned an entry")
	}
}

func TestManifestWholeChunkColdReadBypassesCache(t *testing.T) {
	plain := bytes.Repeat([]byte{0x28}, 4096)
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: uint64(len(plain)),
		Entries: []codec.ChunkEntry{{
			Offset:         0,
			Size:           uint32(len(plain)),
			CiphertextHash: sha256.Sum256(plain),
		}},
	}
	getter := &countingHitGetter{value: plain}
	stream := newTestManifestStream(m, make([][32]byte, 1), getter, &passthroughEncryptor{plain: plain})
	defer stream.Close()
	for range 2 {
		if n, err := stream.ReadAt(context.Background(), make([]byte, len(plain)), 0); err != nil || n != len(plain) {
			t.Fatalf("whole ReadAt = (%d, %v)", n, err)
		}
	}
	if got := getter.calls.Load(); got != 2 {
		t.Fatalf("cold whole-chunk Gets = %d, want 2 direct loads", got)
	}

	if n, err := stream.ReadAt(context.Background(), make([]byte, 512), 0); err != nil || n != 512 {
		t.Fatalf("partial ReadAt = (%d, %v)", n, err)
	}
	if n, err := stream.ReadAt(context.Background(), make([]byte, len(plain)), 0); err != nil || n != len(plain) {
		t.Fatalf("cached whole ReadAt = (%d, %v)", n, err)
	}
	if got := getter.calls.Load(); got != 3 {
		t.Fatalf("Gets after partial fill + whole hit = %d, want 3", got)
	}
}

type countingChunkEncryptor struct {
	inner    *manifestcrypto.AESChunkEncryptor
	decrypts atomic.Int64
}

func (e *countingChunkEncryptor) DecryptChunkTo(ctx context.Context, key [32]byte, ciphertext, dst []byte) error {
	e.decrypts.Add(1)
	return e.inner.DecryptChunkTo(ctx, key, ciphertext, dst)
}

func (e *countingChunkEncryptor) UnsealKeyTable(_ [32]byte, sealed, _ []byte) ([]byte, error) {
	return sealed, nil
}

type chunkMapGetter struct {
	chunks map[store.ContentKey][]byte
	calls  atomic.Int64
}

func (g *chunkMapGetter) Get(_ context.Context, _ store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	g.calls.Add(1)
	chunk, ok := g.chunks[key]
	if !ok {
		return cache.CacheMiss, nil, nil
	}
	return cache.CacheHit, cache.NewMemBlob(chunk), nil
}

func testDecryptedChunkKey(value byte) decryptedChunkKey {
	return decryptedChunkKey{
		ciphertextHash: [32]byte{value},
		decryptKey:     [32]byte{value + 1},
		plaintextSize:  4096,
	}
}

func allZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}
