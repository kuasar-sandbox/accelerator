package client

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
)

func key(b []byte) store.ContentKey { return store.ContentKey(sha256.Sum256(b)) }

// Checks PutRepresenting and Resolve against a real server pair. An empty store
// resolves as absent. The first PutRepresenting wins, and a second for the same
// chunk loses, names the winner, and its uploaded object is discarded. Resolve
// then reports the winner's delta key. After the independent object is stored,
// Resolve reports independent, and a later delta still loses to the existing alias.
func TestPutRepresentingAndResolve(t *testing.T) {
	c := startBufconnPair(t, 1, true)
	ctx := context.Background()
	adm := testAdmission(t, c)
	h := key([]byte("independent"))

	r, err := c.Resolve(ctx, adm.Generation, h)
	if err != nil || r.Resolution != store.ResolutionAbsent {
		t.Fatalf("empty store: %+v, %v", r, err)
	}

	a, b := []byte("patch A"), []byte("patch B")
	res, err := c.PutRepresenting(ctx, adm, key(a), a, h)
	if err != nil || !res.IsNew || res.Represented != store.RepresentedWon {
		t.Fatalf("first = %+v, %v", res, err)
	}
	res, err = c.PutRepresenting(ctx, adm, key(b), b, h)
	if err != nil || !res.IsNew || res.Represented != store.RepresentedLost || res.Key != key(a) || !res.Discarded {
		t.Fatalf("second = %+v, %v", res, err)
	}
	if found, _, err := c.Get(ctx, store.PartitionChunk, key(b)); found || err != nil {
		t.Fatalf("the discarded loser is still readable: %v, %v", found, err)
	}
	r, err = c.Resolve(ctx, adm.Generation, h)
	if err != nil || r.Resolution != store.ResolutionDelta || r.DeltaKey != key(a) {
		t.Fatalf("resolve = %+v, %v", r, err)
	}

	// Once the independent object exists it wins for Resolve; a later delta
	// still only sees the alias (LOST to A here) and is discarded.
	indep := []byte("independent")
	if _, err := c.Put(ctx, adm, store.PartitionChunk, h, indep); err != nil {
		t.Fatal(err)
	}
	r, err = c.Resolve(ctx, adm.Generation, h)
	if err != nil || r.Resolution != store.ResolutionIndependent {
		t.Fatalf("after independent put: %+v, %v", r, err)
	}
	res, err = c.PutRepresenting(ctx, adm, key([]byte("patch C")), []byte("patch C"), h)
	if err != nil || res.Represented != store.RepresentedLost || res.Key != key(a) || !res.Discarded {
		t.Fatalf("delta after independent = %+v, %v", res, err)
	}
}

// Checks the sequence Bundle upload uses for a delta object: a plain Put, then
// PutRepresenting of the same object. The second call records the alias even
// though the object already exists, and an alias conflict never deletes the
// object, because this request did not create it.
func TestPutRepresentingAfterPutRecordsAliasWithoutDiscard(t *testing.T) {
	c := startBufconnPair(t, 1, true)
	ctx := context.Background()
	adm := testAdmission(t, c)

	obj, h := []byte("bundled patch"), key([]byte("chunk"))
	if _, err := c.Put(ctx, adm, store.PartitionChunk, key(obj), obj); err != nil {
		t.Fatal(err)
	}
	res, err := c.PutRepresenting(ctx, adm, key(obj), obj, h)
	if err != nil || res.IsNew || res.Represented != store.RepresentedWon || res.Discarded {
		t.Fatalf("representing Put of a stored object = %+v, %v", res, err)
	}
	if r, err := c.Resolve(ctx, adm.Generation, h); err != nil || r.Resolution != store.ResolutionDelta || r.DeltaKey != key(obj) {
		t.Fatalf("resolve = %+v, %v", r, err)
	}

	other := []byte("another bundled patch")
	if _, err := c.Put(ctx, adm, store.PartitionChunk, key(other), other); err != nil {
		t.Fatal(err)
	}
	res, err = c.PutRepresenting(ctx, adm, key(other), other, h)
	if err != nil || res.Represented != store.RepresentedLost || res.Key != key(obj) || res.Discarded {
		t.Fatalf("conflicting representing Put = %+v, %v", res, err)
	}
	if found, _, err := c.Get(ctx, store.PartitionChunk, key(other)); !found || err != nil {
		t.Fatalf("an object this request did not create was deleted: %v, %v", found, err)
	}
}

// Checks that Resolve rejects a stale generation with FailedPrecondition rather
// than ErrUnsupported, preserving the distinction between an outdated client
// token and a server that lacks alias support.
func TestResolveRejectsStaleGeneration(t *testing.T) {
	c := startBufconnPair(t, 1, true)
	_, err := c.Resolve(context.Background(), "G9", store.ContentKey{1})
	if status.Code(errors.Unwrap(err)) != codes.FailedPrecondition && status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v", err)
	}
	if errors.Is(err, store.ErrUnsupported) {
		t.Fatal("a stale generation is not 'unsupported'")
	}
}

// oldServer predates Resolve and PutHeader.represents: it embeds the
// Unimplemented server and answers Put with a bare is_new.
type oldServer struct {
	pb.UnimplementedStoreServer
	sawRepresents bool
}

func (s *oldServer) Put(stream pb.Store_PutServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	s.sawRepresents = len(first.GetHeader().GetRepresents()) > 0
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	return stream.SendAndClose(&pb.PutResponse{IsNew: true})
}

// Checks compatibility with a server that predates the alias feature. Resolve
// returns store.ErrUnsupported on the Unimplemented status, and PutRepresenting
// still stores the object with RepresentedNone while carrying the represents
// header on the wire.
func TestResolveAgainstOldServerIsUnsupported(t *testing.T) {
	srv := &oldServer{}
	c := startCaptureClient(t, srv)
	_, err := c.Resolve(context.Background(), "G1", store.ContentKey{1})
	if !errors.Is(err, store.ErrUnsupported) {
		t.Fatalf("err = %v, want store.ErrUnsupported", err)
	}
	// And a representing Put against it still stores, reporting nothing recorded.
	obj := []byte("patch")
	res, err := c.PutRepresenting(context.Background(), store.WriteAdmission{Generation: "G1"}, key(obj), obj, store.ContentKey{1})
	if err != nil || !res.IsNew || res.Represented != store.RepresentedNone {
		t.Fatalf("put = %+v, %v", res, err)
	}
	if !srv.sawRepresents {
		t.Fatal("the header did not carry represents")
	}
}

type badLostServer struct{ pb.UnimplementedStoreServer }

func (badLostServer) Put(stream pb.Store_PutServer) error {
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	return stream.SendAndClose(&pb.PutResponse{IsNew: true, Represented: pb.Represented_REPRESENTED_LOST, RepresentedKey: []byte{1, 2, 3}})
}

func (badLostServer) Resolve(context.Context, *pb.ResolveRequest) (*pb.ResolveResponse, error) {
	return &pb.ResolveResponse{Resolution: pb.Resolution_RESOLUTION_DELTA, DeltaKey: []byte{1}}, nil
}

// Checks that the client rejects server replies with wrong-length content keys:
// a PutRepresenting LOST reply with a short key, and a Resolve DELTA reply with
// a short key, are both refused with errors.
func TestClientRejectsMalformedAliasReplies(t *testing.T) {
	c := startCaptureClient(t, badLostServer{})
	obj := []byte("patch")
	if _, err := c.PutRepresenting(context.Background(), store.WriteAdmission{Generation: "G1"}, key(obj), obj, store.ContentKey{1}); err == nil {
		t.Fatal("a LOST reply with a short key was accepted")
	}
	if _, err := c.Resolve(context.Background(), "G1", store.ContentKey{1}); err == nil {
		t.Fatal("a DELTA reply with a short key was accepted")
	}
}
