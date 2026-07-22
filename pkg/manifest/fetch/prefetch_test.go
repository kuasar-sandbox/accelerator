package fetch

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestNewStreamPrefetchIsUnkeyedAndSkipsZero(t *testing.T) {
	dataHash := prefetchTestKey(0xD1)
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: 8,
		Entries: []codec.ChunkEntry{
			{Offset: 0, Size: 4, CiphertextHash: dataHash},
			{Offset: 4, Size: 4, IsZero: true},
		},
	}
	getter := newPrefetchRecordingGetter()
	stream := NewStream(m, make([][32]byte, len(m.Entries)), getter, nil)
	prefetcher, ok := stream.(Prefetcher)
	if !ok {
		t.Fatal("NewStream result does not implement Prefetcher")
	}

	if err := prefetcher.Prefetch(context.Background()); err != nil {
		t.Fatalf("Prefetch(all): %v", err)
	}
	if got := getter.callCount(); got != 1 {
		t.Fatalf("Get calls = %d, want 1 (Data only; Zero must be skipped)", got)
	}
	assertPrefetchBlobsUntouchedAndReleased(t, getter.blobSnapshot())

	before := getter.callCount()
	for _, key := range []store.ContentKey{prefetchTestKey(0x77), {}} {
		err := prefetcher.Prefetch(context.Background(), key)
		if !errors.Is(err, ErrUnknownPrefetchLayer) {
			t.Fatalf("Prefetch(unkeyed, %x) error = %v, want ErrUnknownPrefetchLayer", key, err)
		}
	}
	if got := getter.callCount(); got != before {
		t.Fatalf("unknown selector performed Get: calls %d -> %d", before, got)
	}
}

func TestLayeredPrefetchVisibilityAndNoDedup(t *testing.T) {
	keyA := prefetchTestKey(0xA1)
	keyB := prefetchTestKey(0xB1)
	topGetter := newPrefetchRecordingGetter()
	bottomGetter := newPrefetchRecordingGetter()

	// The top layer exposes the bottom layer through two real, disjoint Hole
	// intervals. Both intervals map to the same physical bottom chunk. That must
	// produce two Gets: Prefetch deliberately has no per-call chunk dedup.
	top := newKeyedPrefetchManifest(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: 16,
		Entries: []codec.ChunkEntry{
			{Offset: 0, Size: 4, CiphertextHash: prefetchTestKey(0xD1)},
			{Offset: 8, Size: 4, IsZero: true},
		},
		Holes: []sparse.Extent{{Offset: 4, Size: 4}, {Offset: 12, Size: 4}},
	}, keyA, topGetter)
	bottom := newKeyedPrefetchManifest(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: 16,
		Entries: []codec.ChunkEntry{
			{Offset: 0, Size: 16, CiphertextHash: prefetchTestKey(0xD2)},
		},
	}, keyB, bottomGetter)
	stream := NewLayered(top, bottom)
	prefetcher := stream.(Prefetcher)

	if err := prefetcher.Prefetch(context.Background()); err != nil {
		t.Fatalf("Prefetch(all): %v", err)
	}
	if got := topGetter.callCount(); got != 1 {
		t.Fatalf("top Gets = %d, want 1", got)
	}
	if got := bottomGetter.callCount(); got != 2 {
		t.Fatalf("bottom Gets = %d, want 2 for two visible intervals of one chunk", got)
	}
	bottomBlobs := bottomGetter.blobSnapshot()
	if len(bottomBlobs) != 2 || bottomBlobs[0] == bottomBlobs[1] {
		t.Fatalf("bottom prefetch did not receive two independent Blob handles: %#v", bottomBlobs)
	}
	assertPrefetchBlobsUntouchedAndReleased(t, append(topGetter.blobSnapshot(), bottomBlobs...))

	topBefore, bottomBefore := topGetter.callCount(), bottomGetter.callCount()
	if err := prefetcher.Prefetch(context.Background(), keyB); err != nil {
		t.Fatalf("Prefetch(B): %v", err)
	}
	if got := topGetter.callCount() - topBefore; got != 0 {
		t.Fatalf("Prefetch(B) top Get delta = %d, want 0", got)
	}
	if got := bottomGetter.callCount() - bottomBefore; got != 2 {
		t.Fatalf("Prefetch(B) bottom Get delta = %d, want 2", got)
	}

	// Duplicate selector keys normalize the selection condition; they do not
	// multiply a traversal. The two real visible intervals still issue two Gets.
	bottomBefore = bottomGetter.callCount()
	if err := prefetcher.Prefetch(context.Background(), keyB, keyB); err != nil {
		t.Fatalf("Prefetch(B, B): %v", err)
	}
	if got := bottomGetter.callCount() - bottomBefore; got != 2 {
		t.Fatalf("duplicate selector Get delta = %d, want 2", got)
	}
}

func TestLayeredPrefetchKeysBelongToFinalLeaves(t *testing.T) {
	t.Run("visible keyed leaf resolves through nested layer", func(t *testing.T) {
		outerGetter := newPrefetchRecordingGetter()
		targetGetter := newPrefetchRecordingGetter()
		lowerGetter := newPrefetchRecordingGetter()
		outerHole := newKeyedPrefetchManifest(&codec.Manifest{
			Version: codec.Version1, ImageSize: 8,
			Holes: []sparse.Extent{{Offset: 0, Size: 8}},
		}, prefetchTestKey(0xA0), outerGetter)
		targetKey := prefetchTestKey(0xB0)
		target := newKeyedPrefetchManifest(&codec.Manifest{
			Version:   codec.Version1,
			ImageSize: 8,
			Entries:   []codec.ChunkEntry{{Offset: 0, Size: 4, CiphertextHash: prefetchTestKey(0x10)}},
			Holes:     []sparse.Extent{{Offset: 4, Size: 4}},
		}, targetKey, targetGetter)
		lower := newKeyedPrefetchManifest(densePrefetchManifest(8, 0x14), prefetchTestKey(0xC0), lowerGetter)
		stream := NewLayered(outerHole, NewLayered(target, lower))

		if err := stream.(Prefetcher).Prefetch(context.Background(), targetKey); err != nil {
			t.Fatalf("Prefetch(nested target): %v", err)
		}
		if outerGetter.callCount() != 0 || targetGetter.callCount() != 1 || lowerGetter.callCount() != 0 {
			t.Fatalf("nested selected Gets = outer:%d target:%d lower:%d, want 0:1:0", outerGetter.callCount(), targetGetter.callCount(), lowerGetter.callCount())
		}
	})

	t.Run("fully hidden nested key remains valid", func(t *testing.T) {
		topGetter := newPrefetchRecordingGetter()
		hiddenGetter := newPrefetchRecordingGetter()
		otherGetter := newPrefetchRecordingGetter()
		top := newKeyedPrefetchManifest(densePrefetchManifest(8, 0x11), prefetchTestKey(0xA1), topGetter)
		hiddenKey := prefetchTestKey(0xB1)
		hidden := newKeyedPrefetchManifest(densePrefetchManifest(8, 0x12), hiddenKey, hiddenGetter)
		other := newKeyedPrefetchManifest(densePrefetchManifest(8, 0x13), prefetchTestKey(0xC1), otherGetter)
		nested := NewLayered(hidden, other)
		stream := NewLayered(top, nested)

		err := stream.(Prefetcher).Prefetch(context.Background(), hiddenKey)
		if err != nil {
			t.Fatalf("structurally present but hidden key rejected: %v", err)
		}
		if topGetter.callCount()+hiddenGetter.callCount()+otherGetter.callCount() != 0 {
			t.Fatal("fully hidden selected leaf caused a Get")
		}
	})

	t.Run("unkeyed leaf remains opaque", func(t *testing.T) {
		unkeyedGetter := newPrefetchRecordingGetter()
		keyedGetter := newPrefetchRecordingGetter()
		unkeyed := NewStream(densePrefetchManifest(8, 0x21), make([][32]byte, 1), unkeyedGetter, nil)
		keyedKey := prefetchTestKey(0xB2)
		keyed := newKeyedPrefetchManifest(densePrefetchManifest(8, 0x22), keyedKey, keyedGetter)
		stream := NewLayered(unkeyed, keyed)
		prefetcher := stream.(Prefetcher)

		if err := prefetcher.Prefetch(context.Background(), keyedKey); err != nil {
			t.Fatalf("Prefetch(keyed hidden leaf): %v", err)
		}
		if unkeyedGetter.callCount() != 0 || keyedGetter.callCount() != 0 {
			t.Fatal("key selection ignored unkeyed opacity")
		}
		if err := prefetcher.Prefetch(context.Background()); err != nil {
			t.Fatalf("Prefetch(all): %v", err)
		}
		if unkeyedGetter.callCount() != 1 || keyedGetter.callCount() != 0 {
			t.Fatalf("all-layer Gets = unkeyed:%d keyed:%d, want 1:0", unkeyedGetter.callCount(), keyedGetter.callCount())
		}
	})

	t.Run("one key selects every matching leaf", func(t *testing.T) {
		sharedKey := prefetchTestKey(0x44)
		upperGetter := newPrefetchRecordingGetter()
		lowerGetter := newPrefetchRecordingGetter()
		upper := newKeyedPrefetchManifest(&codec.Manifest{
			Version:   codec.Version1,
			ImageSize: 8,
			Entries:   []codec.ChunkEntry{{Offset: 0, Size: 4, CiphertextHash: prefetchTestKey(0x31)}},
			Holes:     []sparse.Extent{{Offset: 4, Size: 4}},
		}, sharedKey, upperGetter)
		lower := newKeyedPrefetchManifest(densePrefetchManifest(8, 0x32), sharedKey, lowerGetter)
		stream := NewLayered(upper, lower)

		if err := stream.(Prefetcher).Prefetch(context.Background(), sharedKey); err != nil {
			t.Fatalf("Prefetch(shared key): %v", err)
		}
		if upperGetter.callCount() != 1 || lowerGetter.callCount() != 1 {
			t.Fatalf("matching leaf Gets = upper:%d lower:%d, want 1:1", upperGetter.callCount(), lowerGetter.callCount())
		}
	})
}

func TestLayeredPrefetchNonPrefetchChunkLeafIsNoOp(t *testing.T) {
	top := &readOnlyChunkLeaf{size: 8}
	bottomGetter := newPrefetchRecordingGetter()
	bottomKey := prefetchTestKey(0xB3)
	bottom := newKeyedPrefetchManifest(densePrefetchManifest(8, 0x41), bottomKey, bottomGetter)
	stream := NewLayered(top, bottom)
	prefetcher := stream.(Prefetcher)

	if _, ok := any(top).(PrefetchChunkStream); ok {
		t.Fatal("test leaf unexpectedly implements PrefetchChunkStream")
	}
	if err := prefetcher.Prefetch(context.Background()); err != nil {
		t.Fatalf("Prefetch(all): %v", err)
	}
	if err := prefetcher.Prefetch(context.Background(), bottomKey); err != nil {
		t.Fatalf("Prefetch(hidden bottom): %v", err)
	}
	if bottomGetter.callCount() != 0 {
		t.Fatalf("opaque non-prefetch leaf fell through to bottom: %d Gets", bottomGetter.callCount())
	}
	if top.readCalls.Load() != 0 || top.readChunkCalls.Load() != 0 {
		t.Fatalf("Prefetch fell back to a read: ReadAt=%d ReadChunkAt=%d", top.readCalls.Load(), top.readChunkCalls.Load())
	}
	if top.runChunkCalls.Load() == 0 {
		t.Fatal("resolver did not classify the non-prefetch ChunkStream leaf")
	}
}

func TestPrefetchChunkBlobOwnershipAndGetterFailures(t *testing.T) {
	sentinel := errors.New("sentinel getter failure")
	tests := []struct {
		name         string
		result       cache.CacheResult
		withBlob     bool
		getterErr    error
		wantErr      bool
		wantSentinel bool
		wantRelease  int64
	}{
		{name: "hit", result: cache.CacheHit, withBlob: true, wantRelease: 1},
		{name: "error with blob", result: cache.CacheHit, withBlob: true, getterErr: sentinel, wantErr: true, wantSentinel: true, wantRelease: 1},
		{name: "miss with blob", result: cache.CacheMiss, withBlob: true, wantErr: true, wantRelease: 1},
		{name: "hit nil blob", result: cache.CacheHit, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var blob *prefetchObservedBlob
			var returned cache.Blob
			if tt.withBlob {
				blob = &prefetchObservedBlob{data: []byte("ciphertext")}
				returned = blob
			}
			getter := &fixedPrefetchGetter{result: tt.result, blob: returned, err: tt.getterErr}
			stream := newKeyedPrefetchManifest(densePrefetchManifest(8, 0x51), prefetchTestKey(0xA5), getter)
			err := stream.PrefetchChunkAt(context.Background(), 0)
			if (err != nil) != tt.wantErr {
				t.Fatalf("PrefetchChunkAt error = %v, wantErr=%v", err, tt.wantErr)
			}
			if tt.wantSentinel && !errors.Is(err, sentinel) {
				t.Fatalf("error = %v, want wrapped sentinel", err)
			}
			if blob != nil {
				if got := blob.releases.Load(); got != tt.wantRelease {
					t.Fatalf("Blob releases = %d, want %d", got, tt.wantRelease)
				}
				if blob.bytes.Load() != 0 || blob.clones.Load() != 0 {
					t.Fatalf("Prefetch inspected/cloned Blob: Bytes=%d Clone=%d", blob.bytes.Load(), blob.clones.Load())
				}
			}
		})
	}
}

func TestChunkOperationsValidateBeforeGet(t *testing.T) {
	entry := codec.ChunkEntry{Offset: 4, Size: 4, CiphertextHash: prefetchTestKey(0x61)}
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: 8,
		Entries:   []codec.ChunkEntry{entry},
		Holes:     []sparse.Extent{{Offset: 0, Size: 4}},
	}

	t.Run("PrefetchChunkAt index and Zero", func(t *testing.T) {
		getter := newPrefetchRecordingGetter()
		stream := newManifestStream(m, make([][32]byte, 1), getter, getter, &passthroughEncryptor{plain: make([]byte, 4)}, store.ContentKey{}, false)
		if err := stream.PrefetchChunkAt(context.Background(), 1); err == nil {
			t.Fatal("out-of-range PrefetchChunkAt succeeded")
		}
		zero := newManifestStream(&codec.Manifest{
			Version: codec.Version1, ImageSize: 4,
			Entries: []codec.ChunkEntry{{Offset: 0, Size: 4, IsZero: true}},
		}, make([][32]byte, 1), getter, getter, nil, store.ContentKey{}, false)
		if err := zero.PrefetchChunkAt(context.Background(), 0); err != nil {
			t.Fatalf("Zero PrefetchChunkAt: %v", err)
		}
		if got := getter.callCount(); got != 0 {
			t.Fatalf("invalid/Zero prefetch performed %d Gets", got)
		}
	})

	invalidReads := []struct {
		name     string
		chunkIdx uint64
		offset   uint64
		end      uint64
		dstLen   int
	}{
		{name: "chunk index", chunkIdx: 1, offset: 4, end: 8, dstLen: 4},
		{name: "offset before entry", offset: 3, end: 7, dstLen: 4},
		{name: "end after entry", offset: 4, end: 9, dstLen: 5},
		{name: "end before offset", offset: 7, end: 6, dstLen: 1},
		{name: "short destination", offset: 4, end: 8, dstLen: 3},
	}
	for _, tt := range invalidReads {
		t.Run(tt.name, func(t *testing.T) {
			getter := newPrefetchRecordingGetter()
			stream := newManifestStream(m, make([][32]byte, 1), getter, getter, &passthroughEncryptor{plain: make([]byte, 4)}, store.ContentKey{}, false)
			if _, err := stream.ReadChunkAt(context.Background(), make([]byte, tt.dstLen), tt.chunkIdx, tt.offset, tt.end); err == nil {
				t.Fatal("invalid ReadChunkAt succeeded")
			}
			if got := getter.callCount(); got != 0 {
				t.Fatalf("invalid ReadChunkAt performed %d Gets", got)
			}
		})
	}

	t.Run("empty range", func(t *testing.T) {
		getter := newPrefetchRecordingGetter()
		stream := newManifestStream(m, make([][32]byte, 1), getter, getter, &passthroughEncryptor{plain: make([]byte, 4)}, store.ContentKey{}, false)
		n, err := stream.ReadChunkAt(context.Background(), nil, 0, 4, 4)
		if n != 0 || err != nil {
			t.Fatalf("empty ReadChunkAt = (%d, %v), want (0, nil)", n, err)
		}
		if getter.callCount() != 0 {
			t.Fatal("empty ReadChunkAt performed Get")
		}
	})

	t.Run("missing decryption key", func(t *testing.T) {
		getter := newPrefetchRecordingGetter()
		stream := newManifestStream(m, nil, getter, getter, &passthroughEncryptor{plain: make([]byte, 4)}, store.ContentKey{}, false)
		if _, err := stream.ReadChunkAt(context.Background(), make([]byte, 4), 0, 4, 8); err == nil {
			t.Fatal("ReadChunkAt without key succeeded")
		}
		if getter.callCount() != 0 {
			t.Fatal("missing-key ReadChunkAt performed Get")
		}
	})

	t.Run("short plaintext", func(t *testing.T) {
		ciphertext := []byte{1, 2, 3, 4}
		shortEntry := entry
		shortEntry.CiphertextHash = sha256.Sum256(ciphertext)
		shortManifest := &codec.Manifest{Version: codec.Version1, ImageSize: 8, Entries: []codec.ChunkEntry{shortEntry}, Holes: []sparse.Extent{{Offset: 0, Size: 4}}}
		blob := &prefetchObservedBlob{data: ciphertext}
		getter := &fixedPrefetchGetter{result: cache.CacheHit, blob: blob}
		stream := newManifestStream(shortManifest, make([][32]byte, 1), getter, getter, &passthroughEncryptor{plain: make([]byte, 3)}, store.ContentKey{}, false)
		if _, err := stream.ReadChunkAt(context.Background(), make([]byte, 4), 0, 4, 8); err == nil {
			t.Fatal("ReadChunkAt accepted short plaintext")
		}
		if getter.calls.Load() != 1 {
			t.Fatalf("short-plaintext Gets = %d, want 1", getter.calls.Load())
		}
		if blob.bytes.Load() != 1 || blob.releases.Load() != 1 {
			t.Fatalf("short-plaintext Blob lifecycle: Bytes=%d Release=%d, want 1/1", blob.bytes.Load(), blob.releases.Load())
		}
	})
}

func TestConcurrentPrefetchCallsAreIndependent(t *testing.T) {
	getter := newPrefetchGateGetter()
	stream := NewStream(densePrefetchManifest(8, 0x71), make([][32]byte, 1), getter, nil)
	prefetcher := stream.(Prefetcher)

	firstStarted := make(chan struct{})
	firstDone := asyncPrefetch(prefetcher, firstStarted)
	<-firstStarted
	firstCall := receivePrefetchGateCall(t, getter.entered)

	secondStarted := make(chan struct{})
	secondDone := asyncPrefetch(prefetcher, secondStarted)
	<-secondStarted
	close(firstCall.release)
	if err := receivePrefetchError(t, firstDone); err != nil {
		t.Fatalf("first Prefetch: %v", err)
	}

	secondCall := receivePrefetchGateCall(t, getter.entered)
	if firstCall.blob == secondCall.blob {
		t.Fatal("concurrent Prefetch calls shared one Blob handle")
	}
	close(secondCall.release)
	if err := receivePrefetchError(t, secondDone); err != nil {
		t.Fatalf("second Prefetch: %v", err)
	}
	assertPrefetchBlobsUntouchedAndReleased(t, []*prefetchObservedBlob{firstCall.blob, secondCall.blob})
}

func TestFailedPrefetchDoesNotAffectLaterRead(t *testing.T) {
	plain := []byte("read-after-prefetch-error")
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: uint64(len(plain)),
		Entries: []codec.ChunkEntry{{
			Offset:         0,
			Size:           uint32(len(plain)),
			CiphertextHash: sha256.Sum256(plain),
		}},
	}
	getter := &prefetchThenHitGetter{data: plain}
	stream := NewStream(m, make([][32]byte, 1), getter, &passthroughEncryptor{plain: plain})
	if err := stream.(Prefetcher).Prefetch(context.Background()); err == nil {
		t.Fatal("Prefetch unexpectedly succeeded")
	}

	out := make([]byte, len(plain))
	n, err := stream.ReadAt(context.Background(), out, 0)
	if err != nil || n != len(out) {
		t.Fatalf("ReadAt after failed Prefetch = (%d, %v), want (%d, nil)", n, err, len(out))
	}
	if string(out) != string(plain) {
		t.Fatalf("ReadAt data = %q, want %q", out, plain)
	}
	if got := getter.calls.Load(); got != 2 {
		t.Fatalf("Get calls = %d, want failed prefetch + successful demand", got)
	}
}

func newKeyedPrefetchManifest(m *codec.Manifest, key store.ContentKey, getter cache.Getter) *manifestStream {
	return newManifestStream(m, make([][32]byte, len(m.Entries)), getter, getter, nil, key, true)
}

func densePrefetchManifest(size uint32, hashByte byte) *codec.Manifest {
	return &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: uint64(size),
		Entries: []codec.ChunkEntry{
			{Offset: 0, Size: size, CiphertextHash: prefetchTestKey(hashByte)},
		},
	}
}

func prefetchTestKey(v byte) store.ContentKey {
	var key store.ContentKey
	key[0] = v
	return key
}

type prefetchRecordingGetter struct {
	mu    sync.Mutex
	calls []store.ContentKey
	blobs []*prefetchObservedBlob
}

func newPrefetchRecordingGetter() *prefetchRecordingGetter {
	return &prefetchRecordingGetter{}
}

func (g *prefetchRecordingGetter) Get(_ context.Context, _ store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	blob := &prefetchObservedBlob{}
	g.mu.Lock()
	g.calls = append(g.calls, key)
	g.blobs = append(g.blobs, blob)
	g.mu.Unlock()
	return cache.CacheHit, blob, nil
}

func (g *prefetchRecordingGetter) callCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}

func (g *prefetchRecordingGetter) blobSnapshot() []*prefetchObservedBlob {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]*prefetchObservedBlob(nil), g.blobs...)
}

type fixedPrefetchGetter struct {
	result cache.CacheResult
	blob   cache.Blob
	err    error
	calls  atomic.Int64
}

type prefetchThenHitGetter struct {
	data  []byte
	calls atomic.Int64
}

func (g *prefetchThenHitGetter) Get(context.Context, store.Partition, store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	if g.calls.Add(1) == 1 {
		return cache.CacheMiss, nil, errors.New("prefetch failed")
	}
	return cache.CacheHit, cache.NewMemBlob(append([]byte(nil), g.data...)), nil
}

func (g *fixedPrefetchGetter) Get(context.Context, store.Partition, store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	g.calls.Add(1)
	return g.result, g.blob, g.err
}

type prefetchObservedBlob struct {
	data     []byte
	bytes    atomic.Int64
	clones   atomic.Int64
	releases atomic.Int64
}

func (b *prefetchObservedBlob) Bytes() []byte {
	b.bytes.Add(1)
	return b.data
}

func (b *prefetchObservedBlob) Clone() cache.Blob {
	b.clones.Add(1)
	return b
}

func (b *prefetchObservedBlob) Release() {
	b.releases.Add(1)
}

func assertPrefetchBlobsUntouchedAndReleased(t *testing.T, blobs []*prefetchObservedBlob) {
	t.Helper()
	for i, blob := range blobs {
		if blob.bytes.Load() != 0 || blob.clones.Load() != 0 || blob.releases.Load() != 1 {
			t.Errorf("Blob[%d] lifecycle: Bytes=%d Clone=%d Release=%d, want 0/0/1", i, blob.bytes.Load(), blob.clones.Load(), blob.releases.Load())
		}
	}
}

type readOnlyChunkLeaf struct {
	size           uint64
	runChunkCalls  atomic.Int64
	readCalls      atomic.Int64
	readChunkCalls atomic.Int64
}

func (s *readOnlyChunkLeaf) Size() uint64 { return s.size }
func (s *readOnlyChunkLeaf) Close() error { return nil }
func (s *readOnlyChunkLeaf) RunAt(offset, limit uint64) (sparse.RunKind, uint64, error) {
	kind, end, _, err := s.RunChunkAt(offset, limit)
	return kind, end, err
}
func (s *readOnlyChunkLeaf) RunChunkAt(offset, limit uint64) (sparse.RunKind, uint64, uint64, error) {
	s.runChunkCalls.Add(1)
	if offset >= s.size {
		return 0, 0, 0, fmt.Errorf("readOnlyChunkLeaf: offset out of range")
	}
	end := offset + limit
	if end < offset || end > s.size {
		end = s.size
	}
	return sparse.Data, end, 0, nil
}
func (s *readOnlyChunkLeaf) ReadAt(_ context.Context, buf []byte, _ uint64) (int, error) {
	s.readCalls.Add(1)
	return len(buf), nil
}
func (s *readOnlyChunkLeaf) ReadChunkAt(_ context.Context, buf []byte, _ uint64, _, _ uint64) (int, error) {
	s.readChunkCalls.Add(1)
	return len(buf), nil
}

type prefetchGateGetter struct {
	entered chan *prefetchGateCall
}

type prefetchGateCall struct {
	release chan struct{}
	blob    *prefetchObservedBlob
}

func newPrefetchGateGetter() *prefetchGateGetter {
	return &prefetchGateGetter{entered: make(chan *prefetchGateCall)}
}

func (g *prefetchGateGetter) Get(ctx context.Context, _ store.Partition, _ store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	call := &prefetchGateCall{release: make(chan struct{}), blob: &prefetchObservedBlob{}}
	select {
	case g.entered <- call:
	case <-ctx.Done():
		return cache.CacheMiss, nil, ctx.Err()
	}
	select {
	case <-call.release:
		return cache.CacheHit, call.blob, nil
	case <-ctx.Done():
		return cache.CacheMiss, nil, ctx.Err()
	}
}

func asyncPrefetch(prefetcher Prefetcher, started chan<- struct{}) <-chan error {
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- prefetcher.Prefetch(context.Background())
	}()
	return done
}

func receivePrefetchGateCall(t *testing.T, calls <-chan *prefetchGateCall) *prefetchGateCall {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for prefetch Get")
		return nil
	}
}

func receivePrefetchError(t *testing.T, results <-chan error) error {
	t.Helper()
	select {
	case err := <-results:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Prefetch")
		return nil
	}
}
