package server

import (
	"context"
	"crypto/sha256"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
)

// TestStatsCounters drives a representative request mix through the gRPC server
// and asserts the per-request counters that feed the periodic stats line.
func TestStatsCounters(t *testing.T) {
	cli, srv := startBufconnServer(t, Options{Backend: newFSStore(t, "gen-1"), VerifyKey: true})
	base := srv.Stats()

	if _, err := cli.AdmitWrite(context.Background(), &pb.AdmitWriteRequest{}); err != nil {
		t.Fatalf("AdmitWrite: %v", err)
	}

	data := []byte("the quick brown fox jumps over the lazy dog")
	key := sha256.Sum256(data)

	if _, err := streamPut(t, cli, pb.Partition_PARTITION_CHUNK, key, data); err != nil {
		t.Fatalf("put new: %v", err)
	}
	if _, err := streamPut(t, cli, pb.Partition_PARTITION_CHUNK, key, data); err != nil {
		t.Fatalf("put dedup: %v", err)
	}
	if got, err := streamGet(t, cli, pb.Partition_PARTITION_CHUNK, key); err != nil || len(got) != len(data) {
		t.Fatalf("get hit: got %d bytes, err %v", len(got), err)
	}
	// A miss surfaces as gRPC NotFound — a normal outcome that must NOT bump
	// the error counter (errN tracks real failures, not misses).
	missKey := sha256.Sum256([]byte("absent"))
	if _, err := streamGet(t, cli, pb.Partition_PARTITION_CHUNK, missKey); status.Code(err) != codes.NotFound {
		t.Fatalf("get miss: want NotFound, got %v", err)
	}

	s := srv.Stats()
	eq := func(name string, delta, want uint64) {
		if delta != want {
			t.Errorf("%s delta = %d, want %d", name, delta, want)
		}
	}
	eq("AdmitN", s.AdmitN-base.AdmitN, 1)
	eq("GetN", s.GetN-base.GetN, 2)
	eq("GetHits", s.GetHits-base.GetHits, 1)
	eq("GetBytes", s.GetBytes-base.GetBytes, uint64(len(data)))
	eq("PutN", s.PutN-base.PutN, 2)
	eq("PutDedup", s.PutDedup-base.PutDedup, 1)
	eq("PutBytes", s.PutBytes-base.PutBytes, uint64(len(data))) // dedup stores no bytes
	eq("ErrN", s.ErrN-base.ErrN, 0)
	eq("GetHist", s.GetHist.Count-base.GetHist.Count, 2)
	eq("PutHist", s.PutHist.Count-base.PutHist.Count, 2)
	if s.Inflight != 0 {
		t.Errorf("inflight gauge = %d, want 0 after all requests drained", s.Inflight)
	}
}
