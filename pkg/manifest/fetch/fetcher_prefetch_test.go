package fetch

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestFetcherReturnsKeyedPrefetcher(t *testing.T) {
	manifestKey := keyForFetcherTest(0x41)
	unknownKey := keyForFetcherTest(0x42)
	chunk := []byte("chunk-data")
	getter := newFetcherRouteGetter(t, map[store.ContentKey][]byte{
		manifestKey: marshalFetcherTestManifest(t, chunk),
	}, chunk)
	fetcher := NewFetcher(fetcherTestCustomerKey, getter, fetcherTestDecryptor{})

	stream, err := fetcher.Fetch(context.Background(), manifestKey)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	prefetcher, ok := stream.(Prefetcher)
	if !ok {
		t.Fatal("Fetcher.Fetch result does not implement Prefetcher")
	}
	if _, ok := stream.(PrefetchChunkStream); !ok {
		t.Fatal("single manifest Fetch result does not implement PrefetchChunkStream")
	}

	done := make(chan error, 1)
	go func() { done <- prefetcher.Prefetch(context.Background(), manifestKey, manifestKey) }()
	call := receiveFetcherChunkCall(t, getter.chunkCalls)
	close(call.release)
	if err := receiveFetcherError(t, done); err != nil {
		t.Fatalf("Prefetch(own key): %v", err)
	}
	assertNoFetcherChunkCall(t, getter.chunkCalls)

	if err := prefetcher.Prefetch(context.Background(), manifestKey, unknownKey); !errors.Is(err, ErrUnknownPrefetchLayer) {
		t.Fatalf("Prefetch(known, unknown) error = %v, want ErrUnknownPrefetchLayer", err)
	}
	assertNoFetcherChunkCall(t, getter.chunkCalls)
}

func TestFetcherSharesAdmissionAcrossStreams(t *testing.T) {
	keyA := keyForFetcherTest(0x51)
	keyB := keyForFetcherTest(0x52)
	chunk := []byte("shared-chunk")
	manifest := marshalFetcherTestManifest(t, chunk)
	getter := newFetcherRouteGetter(t, map[store.ContentKey][]byte{keyA: manifest, keyB: manifest}, chunk)
	publicFetcher := NewFetcher(fetcherTestCustomerKey, getter, fetcherTestDecryptor{})
	fetcher := publicFetcher.(*fetcher)

	streamA, err := fetcher.Fetch(context.Background(), keyA)
	if err != nil {
		t.Fatalf("Fetch(A): %v", err)
	}
	streamB, err := fetcher.Fetch(context.Background(), keyB)
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
	go func() { prefetchDone <- streamB.(Prefetcher).Prefetch(context.Background(), keyB) }()
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
	keyA := keyForFetcherTest(0x61)
	keyB := keyForFetcherTest(0x62)
	chunk := []byte("metadata-priority")
	manifest := marshalFetcherTestManifest(t, chunk)
	getter := newFetcherRouteGetter(t, map[store.ContentKey][]byte{keyA: manifest, keyB: manifest}, chunk)
	fetcher := NewFetcher(fetcherTestCustomerKey, getter, fetcherTestDecryptor{})

	stream, err := fetcher.Fetch(context.Background(), keyA)
	if err != nil {
		t.Fatalf("Fetch(A): %v", err)
	}
	prefetchDone := make(chan error, 1)
	go func() { prefetchDone <- stream.(Prefetcher).Prefetch(context.Background(), keyA) }()
	prefetch := receiveFetcherChunkCall(t, getter.chunkCalls)

	// An already-admitted prefetch must not make manifest metadata wait. The
	// second Fetch has to finish while the prefetch Get remains blocked.
	fetchDone := make(chan error, 1)
	go func() {
		_, err := fetcher.Fetch(context.Background(), keyB)
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
	keyA := keyForFetcherTest(0x63)
	keyB := keyForFetcherTest(0x64)
	chunk := []byte("metadata-admission")
	manifest := marshalFetcherTestManifest(t, chunk)
	route := newFetcherRouteGetter(t, map[store.ContentKey][]byte{keyA: manifest, keyB: manifest}, chunk)
	getter := &fetcherMetadataGateGetter{
		inner:   route,
		gateKey: keyB,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	publicFetcher := NewFetcher(fetcherTestCustomerKey, getter, fetcherTestDecryptor{})
	fetcher := publicFetcher.(*fetcher)
	stream, err := fetcher.Fetch(context.Background(), keyA)
	if err != nil {
		t.Fatalf("Fetch(A): %v", err)
	}

	fetchDone := make(chan error, 1)
	go func() {
		_, err := fetcher.Fetch(context.Background(), keyB)
		fetchDone <- err
	}()
	select {
	case <-getter.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for manifest metadata Get")
	}

	prefetchDone := make(chan error, 1)
	go func() { prefetchDone <- stream.(Prefetcher).Prefetch(context.Background(), keyA) }()
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
	streamA := NewStream(m, make([][32]byte, 1), getter, &passthroughEncryptor{plain: chunk})
	streamB := NewStream(m, make([][32]byte, 1), getter, &passthroughEncryptor{plain: chunk})

	readDone := make(chan error, 1)
	go func() {
		_, err := streamA.ReadAt(context.Background(), make([]byte, len(chunk)), 0)
		readDone <- err
	}()
	demand := receiveFetcherChunkCall(t, getter.chunkCalls)

	prefetchDone := make(chan error, 1)
	go func() { prefetchDone <- streamB.(Prefetcher).Prefetch(context.Background()) }()
	// Independent NewStream schedulers admit B's prefetch even while A's
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

func (fetcherTestDecryptor) DecryptChunk(_ [32]byte, ciphertext []byte) ([]byte, error) {
	return append([]byte(nil), ciphertext...), nil
}

func (fetcherTestDecryptor) DecryptChunkInPlace(_ [32]byte, ciphertext []byte) ([]byte, error) {
	return ciphertext, nil
}

func (fetcherTestDecryptor) UnsealKeyTable(_ [32]byte, _, _ []byte) ([]byte, error) {
	return make([]byte, 32), nil
}

func fetcherTestCustomerKey() ([32]byte, error) { return [32]byte{}, nil }

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

func marshalFetcherTestManifest(t *testing.T, chunk []byte) []byte {
	t.Helper()
	m := &codec.Manifest{
		Version:   codec.Version1,
		ImageSize: uint64(len(chunk)),
		Entries: []codec.ChunkEntry{{
			Offset:         0,
			Size:           uint32(len(chunk)),
			CiphertextHash: sha256.Sum256(chunk),
		}},
	}
	data, err := codec.Marshal(m, []byte("sealed"))
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

func keyForFetcherTest(value byte) store.ContentKey {
	var key store.ContentKey
	key[0] = value
	return key
}
