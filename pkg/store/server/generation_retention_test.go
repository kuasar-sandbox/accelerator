package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
)

// Finishing a write and rolling out a new default does not retire old references.
// The list is both a write-admission input and the authoritative read search path.
func TestCompletedWriteStillNeedsItsGenerationForReads(t *testing.T) {
	var current atomic.Value
	current.Store([]store.Generation{"G1"})
	backend := newFSStore(t, "G1")
	cli, srv := startBufconnServer(t, Options{
		Backend: backend,
		Generations: func() []store.Generation {
			return current.Load().([]store.Generation)
		},
		VerifyKey: true,
	})
	ctx := context.Background()
	data := []byte("an object referenced by a retained parent artifact")
	key := store.ContentKey(sha256.Sum256(data))
	if response, err := streamPut(t, cli, pb.Partition_PARTITION_CHUNK, key, data); err != nil || !response.GetIsNew() {
		t.Fatalf("completed G1 Put = %v, %v", response, err)
	}

	current.Store([]store.Generation{"G1", "G2"})
	admission, err := cli.AdmitWrite(ctx, &pb.AdmitWriteRequest{})
	if err != nil || admission.GetGeneration() != "G2" {
		t.Fatalf("new admission = %v, %v", admission, err)
	}
	found, got, err := srv.GetObject(ctx, store.PartitionChunk, key)
	if err != nil || !found || !bytes.Equal(got, data) {
		t.Fatalf("retained G1 read = %v, %q, %v", found, got, err)
	}

	// Nothing deletes or changes the object: list removal alone hides it.
	current.Store([]store.Generation{"G2"})
	found, _, err = srv.GetObject(ctx, store.PartitionChunk, key)
	if err != nil || found {
		t.Fatalf("removed-generation read = %v, %v; want a miss", found, err)
	}
	found, got, err = backend.Get(ctx, "G1", store.PartitionChunk, key)
	if err != nil || !found || !bytes.Equal(got, data) {
		t.Fatalf("G1 physical object changed = %v, %q, %v", found, got, err)
	}

	current.Store([]store.Generation{"G1", "G2"})
	found, got, err = srv.GetObject(ctx, store.PartitionChunk, key)
	if err != nil || !found || !bytes.Equal(got, data) {
		t.Fatalf("restored read list = %v, %q, %v", found, got, err)
	}
}
