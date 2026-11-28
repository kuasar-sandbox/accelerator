package fs

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func aliasKey(b byte) store.ContentKey { return store.ContentKey{b} }

// Checks that an alias is write-once. A missing alias reads as absent. The first
// CreateAlias wins and returns the key it wrote. A second create with a different
// key returns the winner with created=false, and re-creating the same key is also
// not a creation. GetAlias then returns the winner.
func TestAliasCreateIsWriteOnce(t *testing.T) {
	backend, _ := newTestStore(t)
	ctx := context.Background()
	h := aliasKey(1)

	if _, ok, err := backend.GetAlias(ctx, testGeneration, h); err != nil || ok {
		t.Fatalf("missing alias: ok=%v err=%v", ok, err)
	}
	cur, created, err := backend.CreateAlias(ctx, testGeneration, h, aliasKey(10))
	if err != nil || !created || cur != aliasKey(10) {
		t.Fatalf("first create = %x %v %v", cur, created, err)
	}
	cur, created, err = backend.CreateAlias(ctx, testGeneration, h, aliasKey(11))
	if err != nil || created || cur != aliasKey(10) {
		t.Fatalf("second create must return the winner: %x %v %v", cur, created, err)
	}
	// Re-creating the same value is not a creation either.
	if cur, created, err = backend.CreateAlias(ctx, testGeneration, h, aliasKey(10)); err != nil || created || cur != aliasKey(10) {
		t.Fatalf("idempotent create = %x %v %v", cur, created, err)
	}
	got, ok, err := backend.GetAlias(ctx, testGeneration, h)
	if err != nil || !ok || got != aliasKey(10) {
		t.Fatalf("GetAlias = %x %v %v", got, ok, err)
	}
}

// Checks that an alias is visible only in the generation that created it. An alias
// created in G1 is absent in G2, and G2 can then create its own alias for the same
// H_ind with a different delta key.
func TestAliasIsScopedToGeneration(t *testing.T) {
	backend, _ := newTestStore(t)
	ctx := context.Background()
	if _, _, err := backend.CreateAlias(ctx, "G1", aliasKey(1), aliasKey(10)); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := backend.GetAlias(ctx, "G2", aliasKey(1)); err != nil || ok {
		t.Fatalf("alias leaked across generations: ok=%v err=%v", ok, err)
	}
	cur, created, err := backend.CreateAlias(ctx, "G2", aliasKey(1), aliasKey(20))
	if err != nil || !created || cur != aliasKey(20) {
		t.Fatalf("G2 create = %x %v %v", cur, created, err)
	}
}

// Checks a race: 32 goroutines create the same alias at once, each with a different
// delta key. Every caller must report the same winner, and exactly one of them
// must report that it created the alias, with the winner's own key.
func TestAliasConcurrentCreatorsAgreeOnOneWinner(t *testing.T) {
	backend, _ := newTestStore(t)
	ctx := context.Background()
	const n = 32
	var wg sync.WaitGroup
	results := make([]store.ContentKey, n)
	created := make([]bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cur, c, err := backend.CreateAlias(ctx, testGeneration, aliasKey(1), aliasKey(byte(100+i)))
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
			if results[i] != aliasKey(byte(100+i)) {
				t.Errorf("creator %d won but reports another value", i)
			}
		}
	}
	if winners != 1 {
		t.Fatalf("%d creators reported creating the alias, want exactly 1", winners)
	}
}

// Checks that a malformed alias file is never trusted or silently replaced. A file
// of garbage at an alias path makes GetAlias fail, and CreateAlias on that path
// must fail with created=false.
func TestAliasRejectsMalformedBody(t *testing.T) {
	backend, root := newTestStore(t)
	ctx := context.Background()
	path := backend.objectPath(store.PartitionAlias, testGeneration, aliasKey(1))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := backend.GetAlias(ctx, testGeneration, aliasKey(1)); err == nil {
		t.Fatal("malformed alias was accepted")
	}
	// A create must not silently replace it either.
	if _, created, err := backend.CreateAlias(ctx, testGeneration, aliasKey(1), aliasKey(2)); err == nil || created {
		t.Fatalf("create over malformed alias: created=%v err=%v", created, err)
	}
	_ = root
}

// Checks that staging files do not accumulate. Five creates, including repeats of
// the same H_ind, must leave the staging directory {root}/alias/.tmp empty.
func TestAliasLeavesNoStagingFiles(t *testing.T) {
	backend, root := newTestStore(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, _, err := backend.CreateAlias(ctx, testGeneration, aliasKey(byte(i%2)), aliasKey(byte(50+i))); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "alias", aliasTmpDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("%d staging files left behind", len(entries))
	}
}

// Checks that aliases follow their generation's lifecycle. GenerationStats counts
// G1's three aliases, and DropGeneration(G1) removes them without touching G2's
// alias. Wipe then removes G2's alias too.
func TestAliasCountedAndDroppedWithGeneration(t *testing.T) {
	backend, _ := newTestStore(t)
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
	if err := backend.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := backend.GetAlias(ctx, "G2", aliasKey(1)); ok {
		t.Fatal("alias survived Wipe")
	}
}

// Checks Delete, which the server uses to discard a losing delta object. Deleting
// an object removes it, and deleting it again is not an error. A delete in one
// generation leaves the same key in another generation. Aliases are refused, and
// a malformed generation is rejected.
func TestDeleteObject(t *testing.T) {
	backend, _ := newTestStore(t)
	ctx := context.Background()
	data := []byte("a losing delta object")
	key := contentKey(data)
	size := int64(len(data))
	if !writeObject(t, backend, testGeneration, store.PartitionChunk, key, data, &size, true) {
		t.Fatal("setup: not new")
	}
	if err := backend.Delete(ctx, testGeneration, store.PartitionChunk, key); err != nil {
		t.Fatal(err)
	}
	if ok, err := backend.Exists(ctx, testGeneration, store.PartitionChunk, key, nil); err != nil || ok {
		t.Fatalf("object survived delete: ok=%v err=%v", ok, err)
	}
	// Deleting again (or a never-stored object) is not an error.
	if err := backend.Delete(ctx, testGeneration, store.PartitionChunk, key); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	// Other generations are untouched.
	if !writeObject(t, backend, "G2", store.PartitionChunk, key, data, &size, true) {
		t.Fatal("setup G2: not new")
	}
	if err := backend.Delete(ctx, testGeneration, store.PartitionChunk, key); err != nil {
		t.Fatal(err)
	}
	if ok, _ := backend.Exists(ctx, "G2", store.PartitionChunk, key, nil); !ok {
		t.Fatal("delete crossed generations")
	}
	// Aliases are write-once and cannot be deleted one at a time.
	if _, _, err := backend.CreateAlias(ctx, testGeneration, aliasKey(1), aliasKey(2)); err != nil {
		t.Fatal(err)
	}
	if err := backend.Delete(ctx, testGeneration, store.PartitionAlias, aliasKey(1)); err == nil {
		t.Fatal("deleting an alias must be refused")
	}
	if err := backend.Delete(ctx, "../x", store.PartitionChunk, key); err == nil {
		t.Fatal("bad generation accepted")
	}
}
