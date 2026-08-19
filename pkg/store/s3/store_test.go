package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func newStoreT(t *testing.T, fake *fakeS3) *Store {
	t.Helper()
	backend, err := New(context.Background(), fake, Config{
		Bucket: "test-bucket",
		Prefix: "test/",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return backend
}

func keyFromBytes(data []byte) store.ContentKey {
	return store.ContentKey(sha256.Sum256(data))
}

func sizePointer(value int64) *int64 { return &value }

func writeS3Object(t *testing.T, backend *Store, generation store.Generation, key store.ContentKey, data []byte, expected *int64, verify bool) bool {
	t.Helper()
	handle, err := backend.OpenPut(generation, store.PartitionChunk, key, expected)
	if err != nil {
		t.Fatalf("OpenPut: %v", err)
	}
	if contextual, ok := handle.(interface{ SetContext(context.Context) }); ok {
		contextual.SetContext(context.Background())
	}
	if _, err := handle.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	digest := key
	if verify {
		digest = keyFromBytes(data)
	}
	isNew, err := handle.Commit(key, digest)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return isNew
}

func TestExplicitGenerationRoundTrip(t *testing.T) {
	fake := newFakeS3()
	backend := newStoreT(t, fake)
	data := []byte("s3 payload")
	key := keyFromBytes(data)
	size := int64(len(data))
	if !writeS3Object(t, backend, "G1", key, data, &size, true) {
		t.Fatal("first write was not new")
	}
	if found, _, err := backend.Get(context.Background(), "G2", store.PartitionChunk, key); err != nil || found {
		t.Fatalf("Get G2 = %v, %v; want miss", found, err)
	}
	found, got, err := backend.Get(context.Background(), "G1", store.PartitionChunk, key)
	if err != nil || !found || !bytes.Equal(got, data) {
		t.Fatalf("Get G1 = %v, %q, %v", found, got, err)
	}
	wantKey := "test/chunk/G1/" + hexKey(key)[:2] + "/" + hexKey(key)[2:4] + "/" + hexKey(key)
	if _, ok := fake.objects[wantKey]; !ok {
		t.Fatalf("object key %q not written", wantKey)
	}
}

func hexKey(key store.ContentKey) string {
	const digits = "0123456789abcdef"
	out := make([]byte, len(key)*2)
	for i, value := range key {
		out[i*2] = digits[value>>4]
		out[i*2+1] = digits[value&15]
	}
	return string(out)
}

func TestExistsOptionalSizeAndErrors(t *testing.T) {
	fake := newFakeS3()
	backend := newStoreT(t, fake)
	data := []byte("head payload")
	key := keyFromBytes(data)
	writeS3Object(t, backend, "G1", key, data, sizePointer(int64(len(data))), true)

	for _, test := range []struct {
		name     string
		expected *int64
		want     bool
	}{
		{name: "absent size", want: true},
		{name: "zero mismatch", expected: sizePointer(0), want: false},
		{name: "matching", expected: sizePointer(int64(len(data))), want: true},
		{name: "different", expected: sizePointer(int64(len(data) + 1)), want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			exists, err := backend.Exists(context.Background(), "G1", store.PartitionChunk, key, test.expected)
			if err != nil || exists != test.want {
				t.Fatalf("Exists = %v, %v; want %v", exists, err, test.want)
			}
		})
	}
	missing := keyFromBytes([]byte("missing"))
	if exists, err := backend.Exists(context.Background(), "G1", store.PartitionChunk, missing, nil); err != nil || exists {
		t.Fatalf("missing Exists = %v, %v", exists, err)
	}
	injected := errors.New("permission denied")
	fake.failNextHead = injected
	if exists, err := backend.Exists(context.Background(), "G1", store.PartitionChunk, key, nil); exists || !errors.Is(err, injected) {
		t.Fatalf("error Exists = %v, %v", exists, err)
	}
}

func TestExistsUsesCallerContext(t *testing.T) {
	fake := newFakeS3()
	backend := newStoreT(t, fake)
	type contextKey struct{}
	marker := &struct{}{}
	seen := false
	fake.headHook = func(ctx context.Context) { seen = ctx.Value(contextKey{}) == marker }
	ctx := context.WithValue(context.Background(), contextKey{}, marker)
	_, _ = backend.Exists(ctx, "G1", store.PartitionChunk, keyFromBytes([]byte("x")), nil)
	if !seen {
		t.Fatal("HEAD did not receive the caller context")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := backend.Exists(cancelled, "G1", store.PartitionChunk, keyFromBytes([]byte("y")), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Exists error = %v", err)
	}

	seen = false
	data := []byte("commit context")
	key := keyFromBytes(data)
	handle, err := backend.OpenPut("G1", store.PartitionChunk, key, sizePointer(int64(len(data))))
	if err != nil {
		t.Fatal(err)
	}
	handle.(interface{ SetContext(context.Context) }).SetContext(ctx)
	if _, err := handle.Write(data); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Commit(key, key); err != nil {
		t.Fatal(err)
	}
	if !seen {
		t.Fatal("PutHandle last-chance HEAD did not receive the stream context")
	}
}

func TestOpenPutCopiesBindingsAndValidatesSize(t *testing.T) {
	fake := newFakeS3()
	backend := newStoreT(t, fake)
	data := []byte("bound")
	key := keyFromBytes(data)
	expected := int64(len(data))
	handle, err := backend.OpenPut("G1", store.PartitionChunk, key, &expected)
	if err != nil {
		t.Fatal(err)
	}
	expected = 99
	_, _ = handle.Write(data)
	other := keyFromBytes([]byte("other"))
	if _, err := handle.Commit(other, key); !errors.Is(err, ErrCommitKeyMismatch) {
		t.Fatalf("Commit error = %v", err)
	}
	if len(fake.objects) != 0 {
		t.Fatal("key-mismatch Commit uploaded data")
	}

	handle, _ = backend.OpenPut("G1", store.PartitionChunk, key, sizePointer(int64(len(data)+1)))
	_, _ = handle.Write(data)
	if _, err := handle.Commit(key, key); err == nil {
		t.Fatal("short Commit succeeded")
	}
	handle, _ = backend.OpenPut("G1", store.PartitionChunk, key, sizePointer(1))
	if _, err := handle.Write(data); err == nil {
		t.Fatal("oversized Write succeeded")
	}
}

func TestDigestAndVerifyFalseSizeOnly(t *testing.T) {
	fake := newFakeS3()
	backend := newStoreT(t, fake)
	data := []byte("wrong")
	claimed := keyFromBytes([]byte("right"))
	handle, _ := backend.OpenPut("G1", store.PartitionChunk, claimed, sizePointer(int64(len(data))))
	_, _ = handle.Write(data)
	if _, err := handle.Commit(claimed, keyFromBytes(data)); !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("digest error = %v", err)
	}
	if !writeS3Object(t, backend, "G1", claimed, data, sizePointer(int64(len(data))), false) {
		t.Fatal("verify=false write was not new")
	}
	if writeS3Object(t, backend, "G1", claimed, []byte("other"), sizePointer(int64(len(data))), false) {
		t.Fatal("same-size existing object should dedup")
	}
}

func TestOpenPutLastChanceDedup(t *testing.T) {
	fake := newFakeS3()
	backend := newStoreT(t, fake)
	data := []byte("dedup")
	key := keyFromBytes(data)
	size := int64(len(data))
	writeS3Object(t, backend, "G1", key, data, &size, true)
	puts := fake.hits.put.Load()
	if writeS3Object(t, backend, "G1", key, data, &size, true) {
		t.Fatal("second write was new")
	}
	if fake.hits.put.Load() != puts {
		t.Fatal("dedup issued PutObject")
	}
}

func TestExplicitGenerationAdminHelpers(t *testing.T) {
	fake := newFakeS3()
	backend := newStoreT(t, fake)
	for _, generation := range []store.Generation{"G1", "G2"} {
		data := []byte(generation)
		writeS3Object(t, backend, generation, keyFromBytes(data), data, sizePointer(int64(len(data))), true)
	}
	fake.objects["test/__meta/generations"] = fakeObject{body: []byte("G1\nG2\n"), etag: "meta"}
	stats, err := backend.GenerationStats(context.Background(), "G1")
	if err != nil || stats[store.PartitionChunk] != 1 {
		t.Fatalf("GenerationStats = %v, %v", stats, err)
	}
	if err := backend.DropGeneration(context.Background(), "G1"); err != nil {
		t.Fatal(err)
	}
	if found, _, err := backend.Get(context.Background(), "G2", store.PartitionChunk, keyFromBytes([]byte("G2"))); err != nil || !found {
		t.Fatalf("G2 removed with G1: %v, %v", found, err)
	}
	if err := backend.Wipe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := fake.objects["test/__meta/generations"]; !ok {
		t.Fatal("Wipe removed independent generation metadata")
	}
}
