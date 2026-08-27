package bundle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type recordingResolver struct {
	mu      sync.Mutex
	sources map[string]ManifestSource
	errors  map[string]error
	calls   []string
}

func (r *recordingResolver) ResolveBundle(_ context.Context, ref string) (ManifestSource, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, ref)
	if err := r.errors[ref]; err != nil {
		return ManifestSource{}, err
	}
	return r.sources[ref], nil
}

func (r *recordingResolver) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

type recordingFetcher struct {
	mu      sync.Mutex
	inner   fetch.Fetcher
	err     error
	calls   int
	lastKey store.ContentKey
}

func (f *recordingFetcher) OpenManifest(ctx context.Context, key store.ContentKey) (fetch.Stream, error) {
	f.mu.Lock()
	f.calls++
	f.lastKey = key
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return f.inner.OpenManifest(ctx, key)
}

func (f *recordingFetcher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func subsetBundle(t *testing.T, fixture testFixture, manifests []store.ContentKey, refs []string, complete bool) *Reader {
	t.Helper()
	var output bytes.Buffer
	w, err := NewWriter(&output, fixture.admission, WriterOptions{Refs: refs})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range manifests {
		if complete {
			if err := w.CopyManifestFromBundle(context.Background(), key, fixture.reader); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, err := w.Put(context.Background(), fixture.admission, store.PartitionManifest, key, objectBytes(t, fixture.reader, store.PartitionManifest, key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finalize(manifests[0]); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	return reader
}

func sourceFor(fixture testFixture, reader *Reader) ManifestSource {
	return ManifestSource{
		Reader:  reader,
		Fetcher: fetch.NewFetcher(fixture.customer, reader.Getter(), fixture.decryptor),
	}
}

func TestManifestFetcherOrderedSourcesAndCurrentPriority(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	ref0 := "file://miss.bundle"
	ref1 := "file://hit.bundle@location:B"

	t.Run("current hit", func(t *testing.T) {
		current := subsetBundle(t, fixture, []store.ContentKey{fixture.root}, []string{ref0}, true)
		resolver := &recordingResolver{}
		remote := &recordingFetcher{inner: sourceFor(fixture, fixture.reader).Fetcher}
		stream, err := NewManifestFetcherWithResolver(current, sourceFor(fixture, current).Fetcher, resolver, remote).OpenManifest(context.Background(), fixture.root)
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.Close()
		if calls := resolver.snapshot(); len(calls) != 0 {
			t.Fatalf("resolver calls = %v", calls)
		}
		if remote.count() != 0 {
			t.Fatal("remote consulted after current hit")
		}
	})

	t.Run("first ref miss second hit", func(t *testing.T) {
		current := subsetBundle(t, fixture, []store.ContentKey{fixture.other}, []string{ref0, ref1}, true)
		miss := subsetBundle(t, fixture, []store.ContentKey{fixture.other}, nil, true)
		hitFetcher := &recordingFetcher{inner: sourceFor(fixture, fixture.reader).Fetcher}
		resolver := &recordingResolver{sources: map[string]ManifestSource{
			ref0: sourceFor(fixture, miss),
			ref1: {Reader: fixture.reader, Fetcher: hitFetcher},
		}}
		remote := &recordingFetcher{inner: sourceFor(fixture, fixture.reader).Fetcher}
		stream, err := NewManifestFetcherWithResolver(current, sourceFor(fixture, current).Fetcher, resolver, remote).OpenManifest(context.Background(), fixture.root)
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.Close()
		if got := resolver.snapshot(); !reflect.DeepEqual(got, []string{ref0, ref1}) {
			t.Fatalf("resolver calls = %v", got)
		}
		if hitFetcher.count() != 1 || remote.count() != 0 {
			t.Fatalf("hit/remote calls = %d/%d", hitFetcher.count(), remote.count())
		}
	})

	t.Run("first of two hits wins", func(t *testing.T) {
		current := subsetBundle(t, fixture, []store.ContentKey{fixture.other}, []string{ref0, ref1}, true)
		first := &recordingFetcher{inner: sourceFor(fixture, fixture.reader).Fetcher}
		second := &recordingFetcher{inner: sourceFor(fixture, fixture.reader).Fetcher}
		resolver := &recordingResolver{sources: map[string]ManifestSource{
			ref0: {Reader: fixture.reader, Fetcher: first},
			ref1: {Reader: fixture.reader, Fetcher: second},
		}}
		stream, err := NewManifestFetcherWithResolver(current, sourceFor(fixture, current).Fetcher, resolver, nil).OpenManifest(context.Background(), fixture.root)
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.Close()
		if first.count() != 1 || second.count() != 0 {
			t.Fatalf("first/second calls = %d/%d", first.count(), second.count())
		}
		if got := resolver.snapshot(); !reflect.DeepEqual(got, []string{ref0}) {
			t.Fatalf("resolver calls = %v", got)
		}
	})

	t.Run("all Bundle miss then remote", func(t *testing.T) {
		current := subsetBundle(t, fixture, []store.ContentKey{fixture.other}, []string{ref0}, true)
		miss := subsetBundle(t, fixture, []store.ContentKey{fixture.other}, nil, true)
		resolver := &recordingResolver{sources: map[string]ManifestSource{ref0: sourceFor(fixture, miss)}}
		remote := &recordingFetcher{inner: sourceFor(fixture, fixture.reader).Fetcher}
		stream, err := NewManifestFetcherWithResolver(current, sourceFor(fixture, current).Fetcher, resolver, remote).OpenManifest(context.Background(), fixture.root)
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.Close()
		if remote.count() != 1 {
			t.Fatalf("remote calls = %d", remote.count())
		}
	})
}

func TestRemoteManifestNeverUsesReferencedBundleChunk(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	ref := "file://chunks.bundle"
	current := subsetBundle(t, fixture, []store.ContentKey{fixture.other}, []string{ref}, true)
	chunkBundle := subsetBundle(t, fixture, []store.ContentKey{fixture.other}, nil, true)
	rootChunks := manifestChunkKeys(t, fixture.reader, fixture.root)
	remote := &routeGetter{objects: map[store.Partition]map[store.ContentKey][]byte{
		store.PartitionManifest: {fixture.root: objectBytes(t, fixture.reader, store.PartitionManifest, fixture.root)},
		store.PartitionChunk:    {},
	}}
	for _, key := range rootChunks[1:] {
		remote.objects[store.PartitionChunk][key] = objectBytes(t, fixture.reader, store.PartitionChunk, key)
	}
	resolver := &recordingResolver{sources: map[string]ManifestSource{ref: sourceFor(fixture, chunkBundle)}}
	remoteFetcher := fetch.NewFetcher(fixture.customer, remote, fixture.decryptor)
	stream, err := NewManifestFetcherWithResolver(current, sourceFor(fixture, current).Fetcher, resolver, remoteFetcher).OpenManifest(context.Background(), fixture.root)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if _, err := stream.ReadAt(context.Background(), make([]byte, len(fixture.rootPlain)), 0); err == nil {
		t.Fatal("remote Manifest read used a Chunk from a referenced Bundle")
	}
	if got := remote.callCount(store.PartitionChunk); got == 0 {
		t.Fatal("remote Chunk Getter was not used")
	}
}

func TestManifestFetcherSelectedBundleFailureNeverFallsThrough(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	ref0 := "file://incomplete.bundle"
	ref1 := "file://complete.bundle"
	current := subsetBundle(t, fixture, []store.ContentKey{fixture.other}, []string{ref0, ref1}, true)
	incomplete := subsetBundle(t, fixture, []store.ContentKey{fixture.root}, nil, false)
	resolver := &recordingResolver{sources: map[string]ManifestSource{
		ref0: sourceFor(fixture, incomplete),
		ref1: sourceFor(fixture, fixture.reader),
	}}
	remote := &recordingFetcher{inner: sourceFor(fixture, fixture.reader).Fetcher}
	stream, err := NewManifestFetcherWithResolver(current, sourceFor(fixture, current).Fetcher, resolver, remote).OpenManifest(context.Background(), fixture.root)
	if err != nil {
		t.Fatalf("OpenManifest with an unvisited missing Chunk: %v", err)
	}
	defer stream.Close()
	if _, err := stream.ReadAt(context.Background(), make([]byte, len(fixture.rootPlain)), 0); err == nil {
		t.Fatal("selected incomplete Bundle read unexpectedly succeeded")
	}
	if got := resolver.snapshot(); !reflect.DeepEqual(got, []string{ref0}) {
		t.Fatalf("resolver calls = %v, want selected ref only", got)
	}
	if remote.count() != 0 {
		t.Fatal("remote consulted after selected Bundle closure failure")
	}
}

func TestManifestFetcherDoesNotRecurseReferencedRefs(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	parentRef := "file://parent.bundle"
	nestedRef := "file://nested.bundle"
	current := subsetBundle(t, fixture, []store.ContentKey{fixture.other}, []string{parentRef}, true)
	parent := subsetBundle(t, fixture, []store.ContentKey{fixture.other}, []string{nestedRef}, true)
	resolver := &recordingResolver{sources: map[string]ManifestSource{
		parentRef: sourceFor(fixture, parent),
		nestedRef: sourceFor(fixture, fixture.reader),
	}}
	_, err := NewManifestFetcherWithResolver(current, sourceFor(fixture, current).Fetcher, resolver, nil).OpenManifest(context.Background(), fixture.root)
	if err == nil {
		t.Fatal("nested referenced refs unexpectedly resolved the Manifest")
	}
	if got := resolver.snapshot(); !reflect.DeepEqual(got, []string{parentRef}) {
		t.Fatalf("resolver calls = %v, want flat parent only", got)
	}
}

func TestManifestFetcherUnavailableContinuesButMalformedFailsClosed(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	ref0 := "file://first.bundle"
	ref1 := "file://second.bundle"
	current := subsetBundle(t, fixture, []store.ContentKey{fixture.other}, []string{ref0, ref1}, true)

	t.Run("unavailable continues", func(t *testing.T) {
		resolver := &recordingResolver{
			errors:  map[string]error{ref0: fmt.Errorf("%w: missing sibling", ErrSourceUnavailable)},
			sources: map[string]ManifestSource{ref1: sourceFor(fixture, fixture.reader)},
		}
		stream, err := NewManifestFetcherWithResolver(current, sourceFor(fixture, current).Fetcher, resolver, nil).OpenManifest(context.Background(), fixture.root)
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.Close()
		if got := resolver.snapshot(); !reflect.DeepEqual(got, []string{ref0, ref1}) {
			t.Fatalf("resolver calls = %v", got)
		}
	})

	t.Run("malformed fails closed", func(t *testing.T) {
		resolver := &recordingResolver{
			errors:  map[string]error{ref0: errors.New("invalid ZIP profile")},
			sources: map[string]ManifestSource{ref1: sourceFor(fixture, fixture.reader)},
		}
		remote := &recordingFetcher{inner: sourceFor(fixture, fixture.reader).Fetcher}
		_, err := NewManifestFetcherWithResolver(current, sourceFor(fixture, current).Fetcher, resolver, remote).OpenManifest(context.Background(), fixture.root)
		if err == nil || !strings.Contains(err.Error(), "invalid ZIP profile") {
			t.Fatalf("OpenManifest error = %v", err)
		}
		if got := resolver.snapshot(); !reflect.DeepEqual(got, []string{ref0}) {
			t.Fatalf("resolver calls = %v", got)
		}
		if remote.count() != 0 {
			t.Fatal("remote masked malformed existing Bundle")
		}
	})
}

func TestManifestFetcherRootMustBeCurrentAndFinalErrorIsDiagnostic(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	ref := "file://missing.bundle"
	current := subsetBundle(t, fixture, []store.ContentKey{fixture.other}, []string{ref}, true)
	resolver := &recordingResolver{errors: map[string]error{ref: fmt.Errorf("%w: not found", ErrSourceUnavailable)}}
	remoteErr := errors.New("remote not found")
	remote := &recordingFetcher{err: remoteErr}
	fetcher := NewManifestFetcherWithResolver(current, sourceFor(fixture, current).Fetcher, resolver, remote)
	if _, err := fetcher.OpenRootManifest(context.Background(), fixture.root); err == nil {
		t.Fatal("root was obtained indirectly")
	}
	_, err := fetcher.OpenManifest(context.Background(), fixture.root)
	if !errors.Is(err, remoteErr) || !strings.Contains(err.Error(), ref) || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("diagnostic error = %v", err)
	}
}

func TestManifestFetcherConcurrentSelection(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	ref := "file://parent.bundle"
	current := subsetBundle(t, fixture, []store.ContentKey{fixture.other}, []string{ref}, true)
	resolver := &recordingResolver{sources: map[string]ManifestSource{ref: sourceFor(fixture, fixture.reader)}}
	fetcher := NewManifestFetcherWithResolver(current, sourceFor(fixture, current).Fetcher, resolver, nil)

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := fetcher.OpenManifest(context.Background(), fixture.root)
			if err != nil {
				t.Errorf("OpenManifest: %v", err)
				return
			}
			_ = stream.Close()
		}()
	}
	wg.Wait()
}
