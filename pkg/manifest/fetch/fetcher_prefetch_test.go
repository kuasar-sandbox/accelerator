package fetch

import (
	"context"
	"crypto/sha256"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestFetcherReturnsPrefetcher(t *testing.T) {
	chunk := []byte("chunk-data")
	manifest := marshalFetcherTestManifest(t, chunk, 0x41)
	manifestKey := store.ContentKey(sha256.Sum256(manifest))
	getter := newFetcherRouteGetter(t, map[store.ContentKey][]byte{
		manifestKey: manifest,
	}, chunk)
	fetcher := NewFetcher([32]byte{}, getter, fetcherTestDecryptor{})

	stream, err := fetcher.OpenManifest(context.Background(), manifestKey)
	if err != nil {
		t.Fatalf("OpenManifest: %v", err)
	}
	prefetcher, ok := stream.(Prefetcher)
	if !ok {
		t.Fatal("Fetcher.OpenManifest result does not implement Prefetcher")
	}
	run, err := stream.RunAt(0, stream.Size())
	if err != nil {
		t.Fatalf("RunAt: %v", err)
	}
	if _, ok := run.(prefetchChunkRun); !ok {
		t.Fatalf("single manifest Data run type = %T, want prefetchChunkRun", run)
	}

	done := make(chan error, 1)
	go func() { done <- prefetcher.Prefetch(context.Background()) }()
	call := receiveFetcherChunkCall(t, getter.chunkCalls)
	close(call.release)
	if err := receiveFetcherError(t, done); err != nil {
		t.Fatalf("Prefetch: %v", err)
	}
	assertNoFetcherChunkCall(t, getter.chunkCalls)
}

func TestFetcherRejectsManifestPhysicalContentKeyMismatch(t *testing.T) {
	chunk := []byte("manifest-hash-check")
	manifest := marshalFetcherTestManifest(t, chunk, 0x72)
	key := store.ContentKey(sha256.Sum256(manifest))
	tampered := append([]byte(nil), manifest...)
	tampered[len(tampered)-1] ^= 0x80
	var released atomic.Int64
	getter := blobReleaseGetter{value: tampered, released: &released}

	_, err := NewFetcher([32]byte{}, getter, fetcherTestDecryptor{}).OpenManifest(context.Background(), key)
	if err == nil {
		t.Fatal("tampered manifest was accepted under its original ContentKey")
	}
	if got := released.Load(); got != 1 {
		t.Fatalf("manifest Blob releases = %d, want 1", got)
	}
}

func TestFetcherSharesAdmissionAcrossStreams(t *testing.T) {
	chunk := []byte("shared-chunk")
	manifestA := marshalFetcherTestManifest(t, chunk, 0x51)
	manifestB := marshalFetcherTestManifest(t, chunk, 0x52)
	keyA := store.ContentKey(sha256.Sum256(manifestA))
	keyB := store.ContentKey(sha256.Sum256(manifestB))
	getter := newFetcherRouteGetter(t, map[store.ContentKey][]byte{keyA: manifestA, keyB: manifestB}, chunk)
	publicFetcher := NewFetcher([32]byte{}, getter, fetcherTestDecryptor{})
	fetcher := publicFetcher.(*fetcher)

	streamA, err := fetcher.OpenManifest(context.Background(), keyA)
	if err != nil {
		t.Fatalf("Fetch(A): %v", err)
	}
	streamB, err := fetcher.OpenManifest(context.Background(), keyB)
	if err != nil {
		t.Fatalf("Fetch(B): %v", err)
	}

	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, len(chunk))
		_, err := streamA.ReadAt(context.Background(), buf, 0)
		readDone <- err
	}()
	demand := receiveFetcherChunkCall(t, getter.chunkCalls)

	prefetchDone := make(chan error, 1)
	go func() { prefetchDone <- streamB.(Prefetcher).Prefetch(context.Background()) }()
	waitForChangeChannel(t, fetcher.cache.scheduler)
	assertNoFetcherChunkCall(t, getter.chunkCalls)

	close(demand.release)
	if err := receiveFetcherError(t, readDone); err != nil {
		t.Fatalf("ReadAt(A): %v", err)
	}
	prefetch := receiveFetcherChunkCall(t, getter.chunkCalls)
	close(prefetch.release)
	if err := receiveFetcherError(t, prefetchDone); err != nil {
		t.Fatalf("Prefetch(B): %v", err)
	}
	assertSchedulerIdle(t, fetcher.cache.scheduler)
}

func TestFetcherManifestLoadBypassesActivePrefetch(t *testing.T) {
	chunk := []byte("metadata-priority")
	manifestA := marshalFetcherTestManifest(t, chunk, 0x61)
	manifestB := marshalFetcherTestManifest(t, chunk, 0x62)
	keyA := store.ContentKey(sha256.Sum256(manifestA))
	keyB := store.ContentKey(sha256.Sum256(manifestB))
	getter := newFetcherRouteGetter(t, map[store.ContentKey][]byte{keyA: manifestA, keyB: manifestB}, chunk)
	fetcher := NewFetcher([32]byte{}, getter, fetcherTestDecryptor{})

	stream, err := fetcher.OpenManifest(context.Background(), keyA)
	if err != nil {
		t.Fatalf("Fetch(A): %v", err)
	}
	prefetchDone := make(chan error, 1)
	go func() { prefetchDone <- stream.(Prefetcher).Prefetch(context.Background()) }()
	prefetch := receiveFetcherChunkCall(t, getter.chunkCalls)

	// An already-admitted prefetch must not make manifest metadata wait. The
	// second Fetch has to finish while the prefetch Get remains blocked.
	fetchDone := make(chan error, 1)
	go func() {
		_, err := fetcher.OpenManifest(context.Background(), keyB)
		fetchDone <- err
	}()
	if err := receiveFetcherError(t, fetchDone); err != nil {
		t.Fatalf("Fetch(B) while prefetch active: %v", err)
	}

	close(prefetch.release)
	if err := receiveFetcherError(t, prefetchDone); err != nil {
		t.Fatalf("Prefetch(A): %v", err)
	}
}

func TestFetcherManifestLoadBlocksNewPrefetch(t *testing.T) {
	chunk := []byte("metadata-admission")
	manifestA := marshalFetcherTestManifest(t, chunk, 0x63)
	manifestB := marshalFetcherTestManifest(t, chunk, 0x64)
	keyA := store.ContentKey(sha256.Sum256(manifestA))
	keyB := store.ContentKey(sha256.Sum256(manifestB))
	route := newFetcherRouteGetter(t, map[store.ContentKey][]byte{keyA: manifestA, keyB: manifestB}, chunk)
	getter := &fetcherMetadataGateGetter{
		inner:   route,
		gateKey: keyB,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	publicFetcher := NewFetcher([32]byte{}, getter, fetcherTestDecryptor{})
	fetcher := publicFetcher.(*fetcher)
	stream, err := fetcher.OpenManifest(context.Background(), keyA)
	if err != nil {
		t.Fatalf("Fetch(A): %v", err)
	}

	fetchDone := make(chan error, 1)
	go func() {
		_, err := fetcher.OpenManifest(context.Background(), keyB)
		fetchDone <- err
	}()
	select {
	case <-getter.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for manifest metadata Get")
	}

	prefetchDone := make(chan error, 1)
	go func() { prefetchDone <- stream.(Prefetcher).Prefetch(context.Background()) }()
	waitForChangeChannel(t, fetcher.cache.scheduler)
	assertNoFetcherChunkCall(t, route.chunkCalls)

	close(getter.release)
	if err := receiveFetcherError(t, fetchDone); err != nil {
		t.Fatalf("Fetch(B): %v", err)
	}
	prefetch := receiveFetcherChunkCall(t, route.chunkCalls)
	close(prefetch.release)
	if err := receiveFetcherError(t, prefetchDone); err != nil {
		t.Fatalf("Prefetch(A): %v", err)
	}
}

func TestNewStreamsHaveIndependentAdmission(t *testing.T) {
	chunk := []byte("independent-streams")
	getter := newFetcherRouteGetter(t, nil, chunk)
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: uint64(len(chunk)),
		Entries: []codec.ChunkEntry{{
			Offset:         0,
			Size:           uint32(len(chunk)),
			CiphertextHash: sha256.Sum256(chunk),
		}},
	}
	streamA := newTestManifestStream(m, make([][32]byte, 1), getter, &passthroughEncryptor{plain: chunk})
	streamB := newTestManifestStream(m, make([][32]byte, 1), getter, &passthroughEncryptor{plain: chunk})

	readDone := make(chan error, 1)
	go func() {
		_, err := streamA.ReadAt(context.Background(), make([]byte, len(chunk)), 0)
		readDone <- err
	}()
	demand := receiveFetcherChunkCall(t, getter.chunkCalls)

	prefetchDone := make(chan error, 1)
	go func() { prefetchDone <- streamB.(Prefetcher).Prefetch(context.Background()) }()
	// Independent test Stream schedulers admit B's prefetch even while A's
	// on-demand Get remains active.
	prefetch := receiveFetcherChunkCall(t, getter.chunkCalls)
	close(prefetch.release)
	if err := receiveFetcherError(t, prefetchDone); err != nil {
		t.Fatalf("Prefetch(B): %v", err)
	}
	close(demand.release)
	if err := receiveFetcherError(t, readDone); err != nil {
		t.Fatalf("ReadAt(A): %v", err)
	}
}

type fetcherTestDecryptor struct{}

func (fetcherTestDecryptor) DecryptChunkTo(_ context.Context, _ [32]byte, ciphertext, dst []byte) error {
	copy(dst, ciphertext)
	return nil
}

func (fetcherTestDecryptor) UnsealKeyTable(_ [32]byte, _, _ []byte) ([]byte, error) {
	return make([]byte, 32), nil
}

type fetcherRouteGetter struct {
	t          *testing.T
	manifests  map[store.ContentKey][]byte
	chunk      []byte
	chunkKey   store.ContentKey
	chunkCalls chan *fetcherChunkCall
}

type fetcherMetadataGateGetter struct {
	inner   cache.Getter
	gateKey store.ContentKey
	entered chan struct{}
	release chan struct{}
}

func (g *fetcherMetadataGateGetter) Get(ctx context.Context, partition store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	if partition == store.PartitionManifest && key == g.gateKey {
		select {
		case g.entered <- struct{}{}:
		case <-ctx.Done():
			return cache.CacheMiss, nil, ctx.Err()
		}
		select {
		case <-g.release:
		case <-ctx.Done():
			return cache.CacheMiss, nil, ctx.Err()
		}
	}
	return g.inner.Get(ctx, partition, key)
}

type fetcherChunkCall struct {
	release chan struct{}
}

func newFetcherRouteGetter(t *testing.T, manifests map[store.ContentKey][]byte, chunk []byte) *fetcherRouteGetter {
	t.Helper()
	return &fetcherRouteGetter{
		t:          t,
		manifests:  manifests,
		chunk:      append([]byte(nil), chunk...),
		chunkKey:   sha256.Sum256(chunk),
		chunkCalls: make(chan *fetcherChunkCall, 8),
	}
}

func (g *fetcherRouteGetter) Get(ctx context.Context, partition store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	switch partition {
	case store.PartitionManifest:
		manifest, ok := g.manifests[key]
		if !ok {
			return cache.CacheMiss, nil, nil
		}
		return cache.CacheHit, cache.NewMemBlob(append([]byte(nil), manifest...)), nil
	case store.PartitionChunk:
		if key != g.chunkKey {
			return cache.CacheMiss, nil, nil
		}
		call := &fetcherChunkCall{release: make(chan struct{})}
		select {
		case g.chunkCalls <- call:
		case <-ctx.Done():
			return cache.CacheMiss, nil, ctx.Err()
		}
		select {
		case <-call.release:
			return cache.CacheHit, cache.NewMemBlob(append([]byte(nil), g.chunk...)), nil
		case <-ctx.Done():
			return cache.CacheMiss, nil, ctx.Err()
		}
	default:
		g.t.Fatalf("unexpected partition %v", partition)
		return cache.CacheMiss, nil, nil
	}
}

func marshalFetcherTestManifest(t *testing.T, chunk []byte, marker byte) []byte {
	t.Helper()
	m := &codec.Manifest{
		Version:      codec.Version1,
		ImageSize:    uint64(len(chunk)),
		MinChunkSize: uint32(len(chunk)),
		MaxChunkSize: uint32(len(chunk)),
		Entries: []codec.ChunkEntry{{
			Offset:         0,
			Size:           uint32(len(chunk)),
			CiphertextHash: sha256.Sum256(chunk),
		}},
	}
	data, err := codec.Marshal(m, append([]byte("sealed"), marker))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return data
}

func receiveFetcherChunkCall(t *testing.T, calls <-chan *fetcherChunkCall) *fetcherChunkCall {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for chunk Get")
		return nil
	}
}

func receiveFetcherError(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for operation")
		return nil
	}
}

func assertNoFetcherChunkCall(t *testing.T, calls <-chan *fetcherChunkCall) {
	t.Helper()
	select {
	case <-calls:
		t.Fatal("unexpected chunk Get")
	default:
	}
}
