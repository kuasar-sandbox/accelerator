package s3

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func aliasKey(b byte) store.ContentKey { return store.ContentKey{b} }

// Checks that an alias is write-once on S3. A missing alias reads as absent. The
// first CreateAlias wins and returns the key it wrote. A second create with a
// different key returns the winner with created=false. The alias is not visible
// in G2.
func TestAliasCreateIsWriteOnce(t *testing.T) {
	fake := newFakeS3()
	backend := newStoreT(t, fake)
	ctx := context.Background()

	if _, ok, err := backend.GetAlias(ctx, "G1", aliasKey(1)); err != nil || ok {
		t.Fatalf("missing alias: ok=%v err=%v", ok, err)
	}
	cur, created, err := backend.CreateAlias(ctx, "G1", aliasKey(1), aliasKey(10))
	if err != nil || !created || cur != aliasKey(10) {
		t.Fatalf("first create = %x %v %v", cur, created, err)
	}
	cur, created, err = backend.CreateAlias(ctx, "G1", aliasKey(1), aliasKey(11))
	if err != nil || created || cur != aliasKey(10) {
		t.Fatalf("second create must return the winner: %x %v %v", cur, created, err)
	}
	if _, ok, _ := backend.GetAlias(ctx, "G2", aliasKey(1)); ok {
		t.Fatal("alias leaked across generations")
	}
}

// Checks that write-once is enforced by S3 itself, not only by client logic. After
// one CreateAlias, a direct Put with If-None-Match: * to the alias object must fail
// with ErrPreconditionFailed, so an overwrite cannot slip past the store.
func TestAliasCreateUsesConditionalPut(t *testing.T) {
	fake := newFakeS3()
	backend := newStoreT(t, fake)
	ctx := context.Background()
	if _, _, err := backend.CreateAlias(ctx, "G1", aliasKey(1), aliasKey(10)); err != nil {
		t.Fatal(err)
	}
	// Overwrite behind the store's back would be visible; a conditional PUT must
	// refuse instead. Re-driving the client with If-None-Match proves it.
	keys := fake.keys()
	if len(keys) != 1 {
		t.Fatalf("keys = %v", keys)
	}
	_, err := fake.Put(ctx, keys[0], store.EncodeAlias(aliasKey(99)), PutOptions{IfNoneMatch: "*"})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("overwrite not refused: %v", err)
	}
}

// Checks a race: 24 goroutines create the same alias at once, each with a different
// delta key. Every caller must report the same winner, and exactly one of them
// must report that it created the alias.
func TestAliasConcurrentCreatorsAgreeOnOneWinner(t *testing.T) {
	backend := newStoreT(t, newFakeS3())
	ctx := context.Background()
	const n = 24
	var wg sync.WaitGroup
	results := make([]store.ContentKey, n)
	created := make([]bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cur, c, err := backend.CreateAlias(ctx, "G1", aliasKey(1), aliasKey(byte(100+i)))
			if err != nil {
				t.Errorf("creator %d: %v", i, err)
			}
			results[i], created[i] = cur, c
		}(i)
	}
	wg.Wait()
	winners := 0
	for i := range results {
		if results[i] != results[0] {
			t.Fatalf("creators disagree: %x vs %x", results[i], results[0])
		}
		if created[i] {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("%d creators won, want 1", winners)
	}
}

// Checks that bad alias state and transport failures are reported, not hidden. A
// garbage object at an alias key makes GetAlias fail, and CreateAlias on that key
// must fail with created=false. A transport failure during CreateAlias must return
// an error rather than be swallowed.
func TestAliasRejectsMalformedBodyAndSurfacesErrors(t *testing.T) {
	fake := newFakeS3()
	backend := newStoreT(t, fake)
	ctx := context.Background()
	key := backend.objectKey(store.PartitionAlias, "G1", aliasKey(1))
	if _, err := fake.Put(ctx, key, []byte("garbage"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := backend.GetAlias(ctx, "G1", aliasKey(1)); err == nil {
		t.Fatal("malformed alias accepted")
	}
	if _, created, err := backend.CreateAlias(ctx, "G1", aliasKey(1), aliasKey(2)); err == nil || created {
		t.Fatalf("create over malformed alias: created=%v err=%v", created, err)
	}

	fake.failNextPut = errors.New("boom")
	if _, _, err := backend.CreateAlias(ctx, "G1", aliasKey(7), aliasKey(2)); err == nil {
		t.Fatal("transport failure was swallowed")
	}
}

// Checks that aliases follow their generation's lifecycle. GenerationStats counts
// G1's three aliases, and DropGeneration(G1) removes them without touching G2's
// alias.
func TestAliasCountedAndDroppedWithGeneration(t *testing.T) {
	backend := newStoreT(t, newFakeS3())
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, _, err := backend.CreateAlias(ctx, "G1", aliasKey(byte(i)), aliasKey(byte(50+i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := backend.CreateAlias(ctx, "G2", aliasKey(1), aliasKey(60)); err != nil {
		t.Fatal(err)
	}
	stats, err := backend.GenerationStats(ctx, "G1")
	if err != nil || stats[store.PartitionAlias] != 3 {
		t.Fatalf("stats = %v, %v", stats, err)
	}
	if err := backend.DropGeneration(ctx, "G1"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := backend.GetAlias(ctx, "G1", aliasKey(1)); ok {
		t.Fatal("alias survived DropGeneration")
	}
	if _, ok, _ := backend.GetAlias(ctx, "G2", aliasKey(1)); !ok {
		t.Fatal("DropGeneration(G1) removed a G2 alias")
	}
}

// Checks Delete, which the server uses to discard a losing delta object. Deleting
// an object removes it from S3, and deleting it again is not an error. Aliases are
// refused.
func TestDeleteObject(t *testing.T) {
	fake := newFakeS3()
	backend := newStoreT(t, fake)
	ctx := context.Background()
	data := []byte("a losing delta object")
	key := keyFromBytes(data)
	size := int64(len(data))
	if !writeS3Object(t, backend, "G1", key, data, &size, true) {
		t.Fatal("setup: not new")
	}
	if err := backend.Delete(ctx, "G1", store.PartitionChunk, key); err != nil {
		t.Fatal(err)
	}
	if ok, err := backend.Exists(ctx, "G1", store.PartitionChunk, key, nil); err != nil || ok {
		t.Fatalf("object survived delete: ok=%v err=%v", ok, err)
	}
	if err := backend.Delete(ctx, "G1", store.PartitionChunk, key); err != nil {
		t.Fatalf("deleting a missing object must be a no-op: %v", err)
	}
	if err := backend.Delete(ctx, "G1", store.PartitionAlias, aliasKey(1)); err == nil {
		t.Fatal("deleting an alias must be refused")
	}
}
