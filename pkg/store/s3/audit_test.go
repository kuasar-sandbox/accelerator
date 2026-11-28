package s3

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// prefixFakeS3 adds the optional ranged read to the fake so the audit's
// cheap path is exercised; the plain fake exercises the full-GET fallback.
type prefixFakeS3 struct {
	*fakeS3
	prefixGets atomic.Int64
	modTime    time.Time
}

func (f *prefixFakeS3) GetPrefix(ctx context.Context, key string, n int64) ([]byte, *ObjectMeta, error) {
	f.prefixGets.Add(1)
	body, meta, err := f.fakeS3.Get(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	if int64(len(body)) > n {
		body = body[:n]
	}
	meta.LastModified = f.modTime
	return body, meta, nil
}

func auditFixture(t *testing.T, fake s3Client) (*Store, store.ContentKey) {
	t.Helper()
	backend, err := New(context.Background(), fake, Config{Bucket: "test-bucket", Prefix: "test/"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	put := func(data []byte) store.ContentKey {
		key := keyFromBytes(data)
		size := int64(len(data))
		if !writeS3Object(t, backend, "G1", key, data, &size, true) {
			t.Fatal("setup: not new")
		}
		return key
	}
	put([]byte{0x01, 'i', 'n', 'd', 'e', 'p'})
	named := put([]byte{store.ChunkFormatDelta, 'n', 'a', 'm', 'e', 'd'})
	orphan := put([]byte{store.ChunkFormatDelta, 'o', 'r', 'p', 'h', 'a', 'n'})
	if _, _, err := backend.CreateAlias(ctx, "G1", aliasKey(1), named); err != nil {
		t.Fatal(err)
	}
	if _, _, err := backend.CreateAlias(ctx, "G1", aliasKey(3), aliasKey(0x77)); err != nil {
		t.Fatal(err) // dangling
	}
	other := []byte{store.ChunkFormatDelta, 'o', 't', 'h', 'e', 'r'}
	size := int64(len(other))
	writeS3Object(t, backend, "G2", keyFromBytes(other), other, &size, true)
	return backend, orphan
}

func checkAudit(t *testing.T, a store.DeltaAudit, orphan store.ContentKey) {
	t.Helper()
	if a.Chunks != 3 || a.DeltaObjects != 2 || a.DeltaBytes != 13 {
		t.Fatalf("chunks %d deltas %d bytes %d", a.Chunks, a.DeltaObjects, a.DeltaBytes)
	}
	if a.Aliases != 2 || a.Named != 2 || a.Dangling != 1 {
		t.Fatalf("aliases %d named %d dangling %d", a.Aliases, a.Named, a.Dangling)
	}
	if len(a.Orphans) != 1 || a.Orphans[0].Key != orphan || a.Orphans[0].Size != 7 || a.OrphanBytes != 7 {
		t.Fatalf("orphans %+v", a.Orphans)
	}
}

// Checks AuditDeltas against a store with no ranged read. The audit falls back to
// full GETs and must reach the same classification: three chunks, two delta objects,
// one named, one orphan, one dangling alias. The orphan's size is 7 bytes. With no
// modification time available, the orphan's age must be reported as unknown (zero).
func TestAuditDeltasFullGetFallback(t *testing.T) {
	fake := newFakeS3()
	backend, orphan := auditFixture(t, fake)
	a, err := backend.AuditDeltas(context.Background(), "G1")
	if err != nil {
		t.Fatal(err)
	}
	checkAudit(t, a, orphan)
	if !a.Orphans[0].ModTime.IsZero() {
		t.Fatal("the plain fake reports no modification time; the orphan's age must be unknown")
	}
}

// Checks AuditDeltas against a store that supports ranged reads. The audit must
// read one prefix per chunk object, which is three in this fixture, and report the
// orphan's modification time from the store. Deleting the orphan then leaves an
// audit with no orphans.
func TestAuditDeltasUsesPrefixReads(t *testing.T) {
	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	fake := &prefixFakeS3{fakeS3: newFakeS3(), modTime: when}
	backend, orphan := auditFixture(t, fake)
	a, err := backend.AuditDeltas(context.Background(), "G1")
	if err != nil {
		t.Fatal(err)
	}
	checkAudit(t, a, orphan)
	if got := fake.prefixGets.Load(); got != 3 {
		t.Fatalf("prefix reads = %d, want one per chunk object", got)
	}
	if !a.Orphans[0].ModTime.Equal(when) {
		t.Fatalf("orphan ModTime = %v, want %v", a.Orphans[0].ModTime, when)
	}
	if err := backend.Delete(context.Background(), "G1", store.PartitionChunk, orphan); err != nil {
		t.Fatal(err)
	}
	if a, err = backend.AuditDeltas(context.Background(), "G1"); err != nil || len(a.Orphans) != 0 {
		t.Fatalf("after delete: %+v %v", a, err)
	}
}
