package fetch

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/fullof-work/mass-sandbox/pkg/cache"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/codec"
	"github.com/fullof-work/mass-sandbox/pkg/store"
)

// recordingGetter counts Get calls; it lets a test assert the IsZero
// short-circuit never reaches the cache.
type recordingGetter struct {
	calls atomic.Int64
}

func (g *recordingGetter) Get(_ context.Context, _ store.Partition, _ store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	g.calls.Add(1)
	return cache.CacheMiss, nil, nil
}

// TestReadAt_IsZeroNoGet — every entry is IsZero; ReadAt produces zeros without
// calling cache.Get a single time.
func TestReadAt_IsZeroNoGet(t *testing.T) {
	const chunkSize = 4096
	const numChunks = 8
	imageSize := uint64(chunkSize * numChunks)

	entries := make([]codec.ChunkEntry, numChunks)
	for i := range entries {
		entries[i] = codec.ChunkEntry{Offset: uint64(i) * chunkSize, Size: chunkSize, IsZero: true}
	}
	m := &codec.Manifest{Version: codec.Version1, ImageSize: imageSize, Entries: entries}

	getter := &recordingGetter{}
	f := NewStream(m, make([][32]byte, numChunks), getter, nil) // encryptor nil — must never be called
	defer f.Close()

	buf := make([]byte, imageSize)
	n, err := f.ReadAt(context.Background(), buf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if uint64(n) != imageSize {
		t.Errorf("n = %d, want %d", n, imageSize)
	}
	if !bytes.Equal(buf, make([]byte, imageSize)) {
		t.Errorf("output is not all zeros")
	}
	if got := getter.calls.Load(); got != 0 {
		t.Errorf("cache.Get called %d times for IsZero-only manifest; want 0", got)
	}
}

// TestReadAt_AcrossZeroBoundary — a read straddling a non-zero chunk and a zero
// chunk places each region's bytes correctly.
func TestReadAt_AcrossZeroBoundary(t *testing.T) {
	const chunkSize = 4096
	imageSize := uint64(2 * chunkSize)

	entries := []codec.ChunkEntry{
		{Offset: 0, Size: chunkSize, IsZero: false},
		{Offset: chunkSize, Size: chunkSize, IsZero: true},
	}
	m := &codec.Manifest{Version: codec.Version1, ImageSize: imageSize, Entries: entries}

	plain := make([]byte, chunkSize)
	for i := range plain {
		plain[i] = byte(i % 251)
	}
	var hashEntry store.ContentKey
	for i := range hashEntry {
		hashEntry[i] = 0xAA
	}
	entries[0].CiphertextHash = hashEntry

	getter := &fixedHitGetter{hash: hashEntry, value: plain}
	f := NewStream(m, make([][32]byte, 2), getter, &passthroughEncryptor{plain: plain})
	defer f.Close()

	// Read [1000, 7000): tail of chunk 0 then head of zero chunk 1.
	out := make([]byte, 6000)
	n, err := f.ReadAt(context.Background(), out, 1000)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 6000 {
		t.Fatalf("n = %d, want 6000", n)
	}
	want := make([]byte, 6000)
	copy(want[:3096], plain[1000:4096])
	if !bytes.Equal(out, want) {
		t.Errorf("output mismatch across IsZero boundary")
	}
}

// TestReadAt_FetchError — a failing cache.Get surfaces as (0, err).
func TestReadAt_FetchError(t *testing.T) {
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: 4096,
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: 4096, CiphertextHash: store.ContentKey{0x9}}},
	}
	f := NewStream(m, make([][32]byte, 1), errGetter{}, &passthroughEncryptor{plain: make([]byte, 4096)})
	defer f.Close()

	n, err := f.ReadAt(context.Background(), make([]byte, 4096), 0)
	if err == nil {
		t.Fatal("expected error from failing getter")
	}
	if n != 0 {
		t.Errorf("n = %d, want 0 on error", n)
	}
}

// TestReadAt_ReleasesBlobs — every fetched chunk's cache blob is released
// exactly once after a successful multi-chunk read.
func TestReadAt_ReleasesBlobs(t *testing.T) {
	const chunkSize = 4096
	const numChunks = 3
	entries := make([]codec.ChunkEntry, numChunks)
	for i := range entries {
		entries[i] = codec.ChunkEntry{Offset: uint64(i) * chunkSize, Size: chunkSize, CiphertextHash: store.ContentKey{byte(i + 1)}}
	}
	m := &codec.Manifest{Version: codec.Version1, ImageSize: numChunks * chunkSize, Entries: entries}
	plain := bytes.Repeat([]byte{0x5A}, chunkSize)

	var released atomic.Int64
	getter := blobReleaseGetter{value: plain, released: &released}
	f := NewStream(m, make([][32]byte, numChunks), getter, &passthroughEncryptor{plain: plain})
	defer f.Close()

	if _, err := f.ReadAt(context.Background(), make([]byte, numChunks*chunkSize), 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if got := released.Load(); got != numChunks {
		t.Errorf("blobs released = %d, want %d", got, numChunks)
	}
}

// passthroughEncryptor returns its captured plaintext from Decrypt regardless
// of key/ciphertext, isolating fetch logic from real crypto.
type passthroughEncryptor struct {
	plain []byte
}

func (e *passthroughEncryptor) Encrypt(_ [32]byte, plaintext []byte) ([]byte, [32]byte) {
	return plaintext, [32]byte{}
}
func (e *passthroughEncryptor) Decrypt(_ [32]byte, _ []byte) ([]byte, error) {
	return e.plain, nil
}
func (e *passthroughEncryptor) DecryptInPlace(_ [32]byte, _ []byte) ([]byte, error) {
	return e.plain, nil
}

// fixedHitGetter returns a fixed hit on a specific hash; any other key misses.
type fixedHitGetter struct {
	hash  store.ContentKey
	value []byte
}

func (g *fixedHitGetter) Get(_ context.Context, _ store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	if key == g.hash {
		return cache.CacheHit, cache.NewMemBlob(g.value), nil
	}
	return cache.CacheMiss, nil, nil
}

// errGetter fails every Get.
type errGetter struct{}

func (errGetter) Get(_ context.Context, _ store.Partition, _ store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	return cache.CacheMiss, nil, errors.New("boom")
}

// blobReleaseGetter hands out a release-counting blob on every hit.
type blobReleaseGetter struct {
	value    []byte
	released *atomic.Int64
}

func (g blobReleaseGetter) Get(_ context.Context, _ store.Partition, _ store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	return cache.CacheHit, countingBlob{data: g.value, released: g.released}, nil
}

type countingBlob struct {
	data     []byte
	released *atomic.Int64
}

func (b countingBlob) Bytes() []byte    { return b.data }
func (b countingBlob) Clone() cache.Blob { return b }
func (b countingBlob) Release()          { b.released.Add(1) }
