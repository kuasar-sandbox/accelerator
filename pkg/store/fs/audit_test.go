package fs

import (
	"context"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// Checks that AuditDeltas classifies a generation's objects correctly. Of four
// chunks, two are delta objects. One of those is named by two aliases and is not
// an orphan. The other is unnamed and is reported as the sole orphan with its size
// and modification time. A third alias names a missing object and is counted as
// dangling. A delta object in another generation is excluded. An empty generation
// audits clean, and deleting the orphan leaves no orphans.
func TestAuditDeltas(t *testing.T) {
	backend, _ := newTestStore(t)
	ctx := context.Background()
	put := func(data []byte) store.ContentKey {
		t.Helper()
		key := contentKey(data)
		size := int64(len(data))
		if !writeObject(t, backend, testGeneration, store.PartitionChunk, key, data, &size, true) {
			t.Fatal("setup: not new")
		}
		return key
	}
	indep := put([]byte{0x01, 'i', 'n', 'd', 'e', 'p'})
	named := put([]byte{store.ChunkFormatDelta, 'n', 'a', 'm', 'e', 'd'})
	orphan := put([]byte{store.ChunkFormatDelta, 'o', 'r', 'p', 'h', 'a', 'n'})
	empty := put(nil)
	if _, _, err := backend.CreateAlias(ctx, testGeneration, aliasKey(1), named); err != nil {
		t.Fatal(err)
	}
	if _, _, err := backend.CreateAlias(ctx, testGeneration, aliasKey(2), named); err != nil {
		t.Fatal(err) // two chunks represented by the same delta object
	}
	if _, _, err := backend.CreateAlias(ctx, testGeneration, aliasKey(3), aliasKey(0x77)); err != nil {
		t.Fatal(err) // dangling: names an object that does not exist
	}
	// Another generation's objects are not part of this audit.
	other := []byte{store.ChunkFormatDelta, 'o', 't', 'h', 'e', 'r'}
	size := int64(len(other))
	writeObject(t, backend, "G2", store.PartitionChunk, contentKey(other), other, &size, true)

	a, err := backend.AuditDeltas(ctx, testGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if a.Chunks != 4 || a.DeltaObjects != 2 || a.DeltaBytes != 6+7 {
		t.Fatalf("chunks %d deltas %d bytes %d", a.Chunks, a.DeltaObjects, a.DeltaBytes)
	}
	if a.Aliases != 3 || a.Named != 2 || a.Dangling != 1 {
		t.Fatalf("aliases %d named %d dangling %d", a.Aliases, a.Named, a.Dangling)
	}
	if len(a.Orphans) != 1 || a.Orphans[0].Key != orphan || a.Orphans[0].Size != 7 || a.Orphans[0].ModTime.IsZero() || a.OrphanBytes != 7 {
		t.Fatalf("orphans %+v bytes %d", a.Orphans, a.OrphanBytes)
	}
	_ = indep
	_ = empty

	// An empty generation audits clean.
	if e, err := backend.AuditDeltas(ctx, "G9"); err != nil || e.Chunks != 0 || e.Aliases != 0 {
		t.Fatalf("empty generation: %+v %v", e, err)
	}
	// After deleting the orphan the audit is clean.
	if err := backend.Delete(ctx, testGeneration, store.PartitionChunk, orphan); err != nil {
		t.Fatal(err)
	}
	if a, err = backend.AuditDeltas(ctx, testGeneration); err != nil || len(a.Orphans) != 0 || a.DeltaObjects != 1 {
		t.Fatalf("after delete: %+v %v", a, err)
	}
}
