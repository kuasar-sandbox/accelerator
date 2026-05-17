package fetch

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/fullof-work/mass-sandbox/pkg/cache"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/codec"
	"github.com/fullof-work/mass-sandbox/pkg/store"
)

// recordingGetter is a cache.Getter that fails any Get call. Its
// presence verifies the IsZero short-circuit in fetch — if the
// short-circuit broke, this test would observe non-zero Get calls
// and fail.
type recordingGetter struct {
	calls atomic.Int64
}

func (g *recordingGetter) Get(_ context.Context, _ store.Partition, _ store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	g.calls.Add(1)
	// Return miss — the test asserts the call count is zero, so the
	// path that reads from miss never runs in the IsZero scenario.
	return cache.CacheMiss, nil, nil
}

// TestFetcher_IsZeroNoGet — every entry is IsZero; WriteTo must
// produce zeros without calling cache.Get a single time.
func TestFetcher_IsZeroNoGet(t *testing.T) {
	const chunkSize = 4096
	const numChunks = 8
	imageSize := uint64(chunkSize * numChunks)

	entries := make([]codec.ChunkEntry, numChunks)
	for i := range entries {
		entries[i] = codec.ChunkEntry{
			Offset: uint64(i) * chunkSize,
			Size:   chunkSize,
			IsZero: true,
		}
	}
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: imageSize,
		Entries:   entries,
	}
	keys := make([][32]byte, numChunks) // all zero — should be ignored

	getter := &recordingGetter{}
	f := NewStream(m, keys, getter, nil) // encryptor is nil — also should never be called

	var buf bytes.Buffer
	if err := f.WriteTo(context.Background(), &buf, 0, 0, ReadOptions{}); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	if buf.Len() != int(imageSize) {
		t.Errorf("output length %d, want %d", buf.Len(), imageSize)
	}
	expected := make([]byte, imageSize)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("output is not all zeros")
	}
	if got := getter.calls.Load(); got != 0 {
		t.Errorf("cache.Get was called %d times for IsZero-only manifest; expected 0", got)
	}
}

// TestFetcher_PartialReadAcrossZeroBoundary — read range straddles a
// non-zero chunk and a zero chunk; each chunk's slice contributes to
// the right place in the output.
func TestFetcher_PartialReadAcrossZeroBoundary(t *testing.T) {
	const chunkSize = 4096
	imageSize := uint64(2 * chunkSize)

	entries := []codec.ChunkEntry{
		{Offset: 0, Size: chunkSize, IsZero: false},
		{Offset: chunkSize, Size: chunkSize, IsZero: true},
	}
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: imageSize,
		Entries:   entries,
	}

	// Non-zero chunk's plaintext: pattern that's easy to spot.
	plain := make([]byte, chunkSize)
	for i := range plain {
		plain[i] = byte(i % 251) // 0..250 cycling
	}
	enc := &passthroughEncryptor{plain: plain}
	hashEntry := store.ContentKey{}
	for i := range hashEntry {
		hashEntry[i] = 0xAA
	}
	entries[0].CiphertextHash = hashEntry

	// Cache Getter returns the (fake) ciphertext for this hash; for
	// any other key, miss. passthroughEncryptor.Decrypt returns plain
	// so we don't depend on real crypto.
	getter := &fixedHitGetter{hash: hashEntry, value: plain}
	keys := make([][32]byte, 2) // unused
	f := NewStream(m, keys, getter, enc)

	var out bytes.Buffer
	// Read offset 1000, length 6000 — covers tail of chunk 0 (1000..4096)
	// then head of chunk 1 (0..2904 zero bytes).
	if err := f.WriteTo(context.Background(), &out, 1000, 6000, ReadOptions{}); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if out.Len() != 6000 {
		t.Fatalf("output length %d, want 6000", out.Len())
	}
	// First 3096 bytes: plain[1000:4096]; last 2904: zeros.
	want := make([]byte, 6000)
	copy(want[:3096], plain[1000:4096])
	if !bytes.Equal(out.Bytes(), want) {
		t.Errorf("output mismatch across IsZero boundary")
	}
}

// passthroughEncryptor is a chunk encryptor whose Decrypt simply
// returns its captured plaintext bytes regardless of key/ciphertext.
// Used to isolate fetch logic from real crypto in tests.
type passthroughEncryptor struct {
	plain []byte
}

func (e *passthroughEncryptor) Encrypt(_ [32]byte, plaintext []byte) ([]byte, [32]byte) {
	return plaintext, [32]byte{}
}
func (e *passthroughEncryptor) Decrypt(_ [32]byte, _ []byte) ([]byte, error) {
	return e.plain, nil
}

// DecryptInPlace mirrors Decrypt — tests exercise the no-alloc path
// the same way as the legacy method.
func (e *passthroughEncryptor) DecryptInPlace(_ [32]byte, _ []byte) ([]byte, error) {
	return e.plain, nil
}

// fixedHitGetter returns a single fixed hit on a specific hash; any
// other key misses. value is wrapped in a memBlob (caller-owned).
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
