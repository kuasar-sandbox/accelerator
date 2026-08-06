package fetch

import (
	"bytes"
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

func TestManifestPrefetchSkipsZero(t *testing.T) {
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
	stream := newTestManifestStream(m, make([][32]byte, len(m.Entries)), getter, nil)
	prefetcher, ok := stream.(Prefetcher)
	if !ok {
		t.Fatal("manifest Stream does not implement Prefetcher")
	}

	if err := prefetcher.Prefetch(context.Background()); err != nil {
		t.Fatalf("Prefetch(all): %v", err)
	}
	if got := getter.callCount(); got != 1 {
		t.Fatalf("Get calls = %d, want 1 (Data only; Zero must be skipped)", got)
	}
	assertPrefetchBlobsUntouchedAndReleased(t, getter.blobSnapshot())
}

func TestLayeredPrefetchVisibilityAndNoDedup(t *testing.T) {
	topGetter := newPrefetchRecordingGetter()
	bottomGetter := newPrefetchRecordingGetter()

	// The top layer exposes the bottom layer through two real, disjoint Hole
	// intervals. Both intervals map to the same physical bottom chunk. That must
	// produce two Gets: Prefetch deliberately has no per-call chunk dedup.
	top := newPrefetchManifest(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: 16,
		Entries: []codec.ChunkEntry{
			{Offset: 0, Size: 4, CiphertextHash: prefetchTestKey(0xD1)},
			{Offset: 8, Size: 4, IsZero: true},
		},
		Holes: []sparse.Extent{{Offset: 4, Size: 4}, {Offset: 12, Size: 4}},
	}, topGetter)
	bottom := newPrefetchManifest(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: 16,
		Entries: []codec.ChunkEntry{
			{Offset: 0, Size: 16, CiphertextHash: prefetchTestKey(0xD2)},
		},
	}, bottomGetter)
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
}

func TestLayeredPrefetchNonPrefetchChunkLeafIsNoOp(t *testing.T) {
	top := &readOnlyChunkLeaf{size: 8}
	bottomGetter := newPrefetchRecordingGetter()
	bottom := newPrefetchManifest(densePrefetchManifest(8, 0x41), bottomGetter)
	stream := NewLayered(top, bottom)
	prefetcher := stream.(Prefetcher)

	run, err := top.RunAt(0, top.Size())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := run.(ChunkRun); !ok {
		t.Fatalf("test run type = %T, want ChunkRun", run)
	}
	if _, ok := run.(prefetchChunkRun); ok {
		t.Fatal("test run unexpectedly implements prefetchChunkRun")
	}
	if err := prefetcher.Prefetch(context.Background()); err != nil {
		t.Fatalf("Prefetch(all): %v", err)
	}
	if bottomGetter.callCount() != 0 {
		t.Fatalf("opaque non-prefetch leaf fell through to bottom: %d Gets", bottomGetter.callCount())
	}
	if top.readCalls.Load() != 0 {
		t.Fatalf("Prefetch fell back to %d Run.ReadAt calls", top.readCalls.Load())
	}
	if top.runCalls.Load() == 0 {
		t.Fatal("Prefetch did not classify the non-prefetch ChunkRun leaf")
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
			stream := newPrefetchManifest(densePrefetchManifest(8, 0x51), getter)
			run, runErr := stream.RunAt(0, stream.Size())
			if runErr != nil {
				t.Fatal(runErr)
			}
			chunk, ok := run.(prefetchChunkRun)
			if !ok {
				t.Fatalf("run type = %T, want prefetchChunkRun", run)
			}
			err := chunk.prefetch(context.Background())
			if (err != nil) != tt.wantErr {
				t.Fatalf("ChunkRun prefetch error = %v, wantErr=%v", err, tt.wantErr)
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

func TestChunkRunOperationsValidateBeforeGet(t *testing.T) {
	entry := codec.ChunkEntry{Offset: 4, Size: 4, CiphertextHash: prefetchTestKey(0x61)}
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: 8,
		Entries:   []codec.ChunkEntry{entry},
		Holes:     []sparse.Extent{{Offset: 0, Size: 4}},
	}

	t.Run("RunAt metadata and Zero", func(t *testing.T) {
		getter := newPrefetchRecordingGetter()
		stream := newManifestStream(m, make([][32]byte, 1), getter, getter, &passthroughEncryptor{plain: make([]byte, 4)})
		run, err := stream.RunAt(4, 4)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := run.(ChunkRun); !ok {
			t.Fatalf("Data run type = %T, want ChunkRun", run)
		}
		zero := newManifestStream(&codec.Manifest{
			Version: codec.Version1, ImageSize: 4,
			Entries: []codec.ChunkEntry{{Offset: 0, Size: 4, IsZero: true}},
		}, make([][32]byte, 1), getter, getter, nil)
		zeroRun, err := zero.RunAt(0, 4)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := zeroRun.(ChunkRun); ok {
			t.Fatalf("Zero run type = %T, must not implement ChunkRun", zeroRun)
		}
		buf := []byte{1, 2, 3, 4}
		if n, err := zeroRun.ReadAt(context.Background(), buf, 0); n != 4 || err != nil {
			t.Fatalf("Zero Run.ReadAt = (%d,%v)", n, err)
		}
		if !bytes.Equal(buf, make([]byte, 4)) {
			t.Fatalf("Zero Run.ReadAt = %v", buf)
		}
		if got := getter.callCount(); got != 0 {
			t.Fatalf("RunAt/Zero read performed %d Gets", got)
		}
	})

	invalidReads := []struct {
		name string
		run  manifestDataRun
		buf  int
	}{
		{name: "chunk index", run: manifestDataRun{chunkIndex: 1, offset: 4, end: 8}, buf: 4},
		{name: "offset before entry", run: manifestDataRun{chunkIndex: 0, offset: 3, end: 7}, buf: 4},
		{name: "end after entry", run: manifestDataRun{chunkIndex: 0, offset: 4, end: 9}, buf: 5},
	}
	for _, tt := range invalidReads {
		t.Run(tt.name, func(t *testing.T) {
			getter := newPrefetchRecordingGetter()
			stream := newManifestStream(m, make([][32]byte, 1), getter, getter, &passthroughEncryptor{plain: make([]byte, 4)})
			run := tt.run
			run.stream = stream
			if _, err := run.ReadAt(context.Background(), make([]byte, tt.buf), 0); err == nil {
				t.Fatal("invalid ChunkRun.ReadAt succeeded")
			}
			if got := getter.callCount(); got != 0 {
				t.Fatalf("invalid ChunkRun.ReadAt performed %d Gets", got)
			}
		})
	}

	t.Run("invalid prefetch index", func(t *testing.T) {
		getter := newPrefetchRecordingGetter()
		stream := newManifestStream(m, make([][32]byte, 1), getter, getter, &passthroughEncryptor{plain: make([]byte, 4)})
		run := &manifestDataRun{stream: stream, chunkIndex: 1, offset: 4, end: 8}
		if err := run.prefetch(context.Background()); err == nil {
			t.Fatal("invalid ChunkRun prefetch succeeded")
		}
		if getter.callCount() != 0 {
			t.Fatal("invalid ChunkRun prefetch performed Get")
		}
	})

	t.Run("relative range", func(t *testing.T) {
		getter := newPrefetchRecordingGetter()
		stream := newManifestStream(m, make([][32]byte, 1), getter, getter, &passthroughEncryptor{plain: make([]byte, 4)})
		run, err := stream.RunAt(4, 4)
		if err != nil {
			t.Fatal(err)
		}
		for _, inner := range []uint64{4, ^uint64(0)} {
			if _, err := run.ReadAt(context.Background(), make([]byte, 1), inner); err == nil {
				t.Fatalf("out-of-range inner offset %d succeeded", inner)
			}
		}
		if getter.callCount() != 0 {
			t.Fatal("invalid relative read performed Get")
		}
	})

	t.Run("empty range", func(t *testing.T) {
		getter := newPrefetchRecordingGetter()
		stream := newManifestStream(m, make([][32]byte, 1), getter, getter, &passthroughEncryptor{plain: make([]byte, 4)})
		run, err := stream.RunAt(4, 4)
		if err != nil {
			t.Fatal(err)
		}
		n, err := run.ReadAt(context.Background(), nil, 4)
		if n != 0 || err != nil {
			t.Fatalf("empty ChunkRun.ReadAt = (%d, %v), want (0, nil)", n, err)
		}
		if getter.callCount() != 0 {
			t.Fatal("empty ChunkRun.ReadAt performed Get")
		}
	})

	t.Run("missing decryption key", func(t *testing.T) {
		getter := newPrefetchRecordingGetter()
		stream := newManifestStream(m, nil, getter, getter, &passthroughEncryptor{plain: make([]byte, 4)})
		run, err := stream.RunAt(4, 4)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := run.ReadAt(context.Background(), make([]byte, 4), 0); err == nil {
			t.Fatal("ChunkRun.ReadAt without key succeeded")
		}
		if getter.callCount() != 0 {
			t.Fatal("missing-key ChunkRun.ReadAt performed Get")
		}
	})

	t.Run("short plaintext", func(t *testing.T) {
		ciphertext := []byte{1, 2, 3, 4}
		shortEntry := entry
		shortEntry.CiphertextHash = sha256.Sum256(ciphertext)
		shortManifest := &codec.Manifest{Version: codec.Version1, ImageSize: 8, Entries: []codec.ChunkEntry{shortEntry}, Holes: []sparse.Extent{{Offset: 0, Size: 4}}}
		blob := &prefetchObservedBlob{data: ciphertext}
		getter := &fixedPrefetchGetter{result: cache.CacheHit, blob: blob}
		stream := newManifestStream(shortManifest, make([][32]byte, 1), getter, getter, &passthroughEncryptor{plain: make([]byte, 3)})
		run, err := stream.RunAt(4, 4)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := run.ReadAt(context.Background(), make([]byte, 4), 0); err == nil {
			t.Fatal("ChunkRun.ReadAt accepted short plaintext")
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
	stream := newTestManifestStream(densePrefetchManifest(8, 0x71), make([][32]byte, 1), getter, nil)
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
	stream := newTestManifestStream(m, make([][32]byte, 1), getter, &passthroughEncryptor{plain: plain})
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

func newPrefetchManifest(m *codec.Manifest, getter cache.Getter) *manifestStream {
	return newManifestStream(m, make([][32]byte, len(m.Entries)), getter, getter, nil)
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
	size      uint64
	runCalls  atomic.Int64
	readCalls atomic.Int64
}

func (s *readOnlyChunkLeaf) Size() uint64 { return s.size }
func (s *readOnlyChunkLeaf) Close() error { return nil }
func (s *readOnlyChunkLeaf) RunAt(offset, limit uint64) (sparse.Run, error) {
	s.runCalls.Add(1)
	if offset >= s.size {
		return nil, fmt.Errorf("readOnlyChunkLeaf: offset out of range")
	}
	if limit == 0 {
		return nil, fmt.Errorf("readOnlyChunkLeaf: zero limit")
	}
	end := offset + limit
	if end < offset || end > s.size {
		end = s.size
	}
	return readOnlyChunkRun{leaf: s, offset: offset, end: end}, nil
}
func (s *readOnlyChunkLeaf) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	return readStreamAt(ctx, s, buf, offset)
}

type readOnlyChunkRun struct {
	leaf   *readOnlyChunkLeaf
	offset uint64
	end    uint64
}

func (r readOnlyChunkRun) Offset() uint64       { return r.offset }
func (r readOnlyChunkRun) End() uint64          { return r.end }
func (r readOnlyChunkRun) Kind() sparse.RunKind { return sparse.Data }
func (r readOnlyChunkRun) chunkRun()            {}
func (r readOnlyChunkRun) ReadAt(_ context.Context, buf []byte, _ uint64) (int, error) {
	r.leaf.readCalls.Add(1)
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
