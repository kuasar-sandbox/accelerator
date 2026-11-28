package server

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
)

// plainBackend hides any optional capability of the wrapped Backend, so it
// behaves like a backend that cannot hold aliases.
type plainBackend struct{ Backend }

func objectKey(data []byte) store.ContentKey { return store.ContentKey(sha256.Sum256(data)) }

// putRep stores data as chunk object sha256(data), asking to represent hInd.
func putRep(t *testing.T, cli pb.StoreClient, data []byte, hInd store.ContentKey) (*pb.PutResponse, error) {
	t.Helper()
	key := objectKey(data)
	size := uint64(len(data))
	return streamPutHeader(t, cli, &pb.PutHeader{
		Partition:  pb.Partition_PARTITION_CHUNK,
		Key:        key[:],
		Generation: "G1",
		Size:       &size,
		Represents: hInd[:],
	}, data)
}

func resolve(t *testing.T, cli pb.StoreClient, hInd store.ContentKey) (*pb.ResolveResponse, error) {
	t.Helper()
	return cli.Resolve(context.Background(), &pb.ResolveRequest{Generation: "G1", Key: hInd[:]})
}

// Checks the first-writer-wins rule for one chunk. The first delta patch wins the
// alias. A second, different patch loses, is told the winner's key, and is deleted
// because this call created it. The winner's object stays, and retrying the
// winner's Put is a win that reports dedup. Stats must show two wins, one loss,
// and one discarded object.
func TestPutRepresentsWonThenLost(t *testing.T) {
	cli, srv := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})
	h := objectKey([]byte("independent form of the chunk"))
	first, second := []byte("delta object A"), []byte("delta object B")

	resp, err := putRep(t, cli, first, h)
	if err != nil || !resp.GetIsNew() || resp.GetRepresented() != pb.Represented_REPRESENTED_WON {
		t.Fatalf("first = %v, %v", resp, err)
	}
	resp, err = putRep(t, cli, second, h)
	if err != nil || !resp.GetIsNew() || resp.GetRepresented() != pb.Represented_REPRESENTED_LOST {
		t.Fatalf("second = %v, %v", resp, err)
	}
	if want := objectKey(first); string(resp.GetRepresentedKey()) != string(want[:]) {
		t.Fatalf("loser told %x, want %x", resp.GetRepresentedKey(), want)
	}
	// The loser's object was new and lost, so the server deleted it again:
	// nothing can reference an object the alias does not name.
	if !resp.GetDiscarded() {
		t.Fatal("a new object that lost must be reported discarded")
	}
	if found, err := streamGet(t, cli, pb.Partition_PARTITION_CHUNK, objectKey(second)); found != nil || status.Code(err) != codes.NotFound {
		t.Fatalf("the losing delta object survived: %v, %v", found, err)
	}
	// The winner is untouched.
	if found, _ := streamGet(t, cli, pb.Partition_PARTITION_CHUNK, objectKey(first)); found == nil {
		t.Fatal("the winning delta object is gone")
	}
	// A retry of the winner's own Put is a win, not a loss, and reports dedup.
	resp, err = putRep(t, cli, first, h)
	if err != nil || resp.GetIsNew() || resp.GetRepresented() != pb.Represented_REPRESENTED_WON || resp.GetDiscarded() {
		t.Fatalf("retry = %v, %v", resp, err)
	}

	s := srv.Stats()
	if s.AliasWon != 2 || s.AliasLost != 1 || s.AliasIndep != 0 || s.AliasErr != 0 {
		t.Fatalf("alias stats = %+v", s)
	}
	if s.Discarded != 1 || s.DiscardedBytes != uint64(len(second)) || s.DiscardErr != 0 {
		t.Fatalf("discard stats = %+v", s)
	}
}

// Checks that a losing patch which already existed is not deleted. The loser was
// stored earlier as an ordinary object, so this Put did not create it. The Put
// must report lost, not discarded, and leave the object in place, because the
// request that created it is the one responsible for deleting it.
func TestPutRepresentsLostOnDedupPathKeepsObject(t *testing.T) {
	cli, srv := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})
	h := objectKey([]byte("indep"))
	winner, loser := []byte("winner patch"), []byte("loser patch")
	if _, err := putRep(t, cli, winner, h); err != nil {
		t.Fatal(err)
	}
	// The loser exists without an alias of its own (stored as a plain object).
	if _, err := streamPut(t, cli, pb.Partition_PARTITION_CHUNK, objectKey(loser), loser); err != nil {
		t.Fatal(err)
	}
	resp, err := putRep(t, cli, loser, h)
	if err != nil || resp.GetIsNew() || resp.GetRepresented() != pb.Represented_REPRESENTED_LOST || resp.GetDiscarded() {
		t.Fatalf("dedup+lost = %v, %v", resp, err)
	}
	if found, _ := streamGet(t, cli, pb.Partition_PARTITION_CHUNK, objectKey(loser)); found == nil {
		t.Fatal("an object this Put did not create was deleted")
	}
	if s := srv.Stats(); s.Discarded != 0 {
		t.Fatalf("discard stats = %+v", s)
	}
}

// Checks a race: eight distinct patches are put for one chunk at the same time.
// Exactly one must win and remain. Each of the other seven must be reported as
// lost, marked discarded, and removed from the store.
func TestPutRepresentsConcurrentLosersAreDiscarded(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})
	h := objectKey([]byte("the chunk"))
	const n = 8
	type outcome struct {
		resp *pb.PutResponse
		err  error
	}
	results := make([]outcome, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := putRep(t, cli, []byte(fmt.Sprintf("patch %d", i)), h)
			results[i] = outcome{resp, err}
		}(i)
	}
	wg.Wait()
	won, lost := 0, 0
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("put %d: %v", i, r.err)
		}
		switch r.resp.GetRepresented() {
		case pb.Represented_REPRESENTED_WON:
			won++
			if found, _ := streamGet(t, cli, pb.Partition_PARTITION_CHUNK, objectKey([]byte(fmt.Sprintf("patch %d", i)))); found == nil {
				t.Fatalf("winner %d was deleted", i)
			}
		case pb.Represented_REPRESENTED_LOST:
			lost++
			if !r.resp.GetDiscarded() {
				t.Fatalf("loser %d was not discarded", i)
			}
			if found, _ := streamGet(t, cli, pb.Partition_PARTITION_CHUNK, objectKey([]byte(fmt.Sprintf("patch %d", i)))); found != nil {
				t.Fatalf("loser %d survived", i)
			}
		default:
			t.Fatalf("put %d: unexpected outcome %v", i, r.resp.GetRepresented())
		}
	}
	if won != 1 || lost != n-1 {
		t.Fatalf("won %d lost %d", won, lost)
	}
}

// Checks that the alias step runs even when the delta object is already stored.
// An earlier Put that stopped before the alias step, or an identical concurrent
// patch, leaves the object present with no alias. A Put of those bytes with
// represents must still win and create the alias, so Resolve returns the delta.
func TestPutRepresentsOnDedupPath(t *testing.T) {
	// The delta object is already stored (an earlier Put that died before the
	// alias step, or a concurrent identical patch): the dedup short-circuit must
	// still attempt the alias.
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})
	h := objectKey([]byte("indep"))
	obj := []byte("a delta object stored without an alias")
	if _, err := streamPut(t, cli, pb.Partition_PARTITION_CHUNK, objectKey(obj), obj); err != nil {
		t.Fatal(err)
	}
	resp, err := putRep(t, cli, obj, h)
	if err != nil || resp.GetIsNew() || resp.GetRepresented() != pb.Represented_REPRESENTED_WON {
		t.Fatalf("dedup+alias = %v, %v", resp, err)
	}
	r, err := resolve(t, cli, h)
	if err != nil || r.GetResolution() != pb.Resolution_RESOLUTION_DELTA {
		t.Fatalf("resolve = %v, %v", r, err)
	}
}

// Checks a delta that arrives after the independent object exists. The writer
// resolved before the whole object landed. The alias is still created and the
// delta wins it, so the writer may record its object, which is therefore not an
// orphan. The winning object is kept. Resolve still answers independent, steering
// later writers to the whole object.
func TestPutRepresentsBesideIndependentObject(t *testing.T) {
	cli, srv := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})
	indep := []byte("the independent object")
	h := objectKey(indep)
	if _, err := streamPut(t, cli, pb.Partition_PARTITION_CHUNK, h, indep); err != nil {
		t.Fatal(err)
	}
	patch := []byte("a delta of it")
	resp, err := putRep(t, cli, patch, h)
	if err != nil || resp.GetRepresented() != pb.Represented_REPRESENTED_WON || resp.GetDiscarded() {
		t.Fatalf("put = %v, %v", resp, err)
	}
	if len(resp.GetRepresentedKey()) != 0 {
		t.Fatal("key set for a non-LOST outcome")
	}
	if found, _ := streamGet(t, cli, pb.Partition_PARTITION_CHUNK, objectKey(patch)); found == nil {
		t.Fatal("the winning delta object must be kept")
	}
	r, err := resolve(t, cli, h)
	if err != nil || r.GetResolution() != pb.Resolution_RESOLUTION_INDEPENDENT {
		t.Fatalf("resolve = %v, %v", r, err)
	}
	if s := srv.Stats(); s.AliasIndep != 0 || s.AliasWon != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

// Checks a backend that cannot hold aliases. The Put still stores the object and
// replies REPRESENTED_NONE, so the caller keeps its own object. The server counts
// an alias error. Resolve on that backend returns Unimplemented.
func TestPutRepresentsNoneWhenBackendHasNoAliases(t *testing.T) {
	cli, srv := startBufconnServer(t, Options{Backend: plainBackend{newFSStore(t, "G1")}, VerifyKey: true})
	obj := []byte("delta object")
	resp, err := putRep(t, cli, obj, objectKey([]byte("indep")))
	if err != nil || !resp.GetIsNew() || resp.GetRepresented() != pb.Represented_REPRESENTED_NONE {
		t.Fatalf("put = %v, %v", resp, err)
	}
	if got := srv.Stats().AliasErr; got != 1 {
		t.Fatalf("AliasErr = %d", got)
	}
	if _, err := resolve(t, cli, objectKey(obj)); status.Code(err) != codes.Unimplemented {
		t.Fatalf("resolve on a backend without aliases: %v", err)
	}
}

// Checks that Put rejects invalid represents requests with InvalidArgument: a
// represents on the manifest or blob partition, a short represents key, and a
// chunk that represents itself. A rejected request must not store its object.
func TestPutRepresentsValidation(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})
	obj := []byte("object")
	key := objectKey(obj)
	size := uint64(len(obj))
	h := objectKey([]byte("h"))
	for name, header := range map[string]*pb.PutHeader{
		"manifest partition": {Partition: pb.Partition_PARTITION_MANIFEST, Key: key[:], Generation: "G1", Size: &size, Represents: h[:]},
		"blob partition":     {Partition: pb.Partition_PARTITION_BLOB, Key: key[:], Generation: "G1", Size: &size, Represents: h[:]},
		"short key":          {Partition: pb.Partition_PARTITION_CHUNK, Key: key[:], Generation: "G1", Size: &size, Represents: h[:5]},
		"represents itself":  {Partition: pb.Partition_PARTITION_CHUNK, Key: key[:], Generation: "G1", Size: &size, Represents: key[:]},
	} {
		if _, err := streamPutHeader(t, cli, header, obj); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: code = %v, want InvalidArgument", name, status.Code(err))
		}
	}
	// None of the rejected Puts may have stored anything.
	if got, _ := streamGet(t, cli, pb.Partition_PARTITION_CHUNK, key); got != nil {
		t.Error("a rejected Put stored its object")
	}
}

// Checks the two answers Resolve gives before and after a delta is put. Before any
// write it answers absent with no key. After a delta with represents, it answers
// delta with the winner's key. Both calls are counted in ResolveN.
func TestResolveStates(t *testing.T) {
	cli, srv := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})
	h := objectKey([]byte("chunk"))

	r, err := resolve(t, cli, h)
	if err != nil || r.GetResolution() != pb.Resolution_RESOLUTION_ABSENT || len(r.GetDeltaKey()) != 0 {
		t.Fatalf("absent = %v, %v", r, err)
	}
	obj := []byte("patch")
	if _, err := putRep(t, cli, obj, h); err != nil {
		t.Fatal(err)
	}
	r, err = resolve(t, cli, h)
	want := objectKey(obj)
	if err != nil || r.GetResolution() != pb.Resolution_RESOLUTION_DELTA || string(r.GetDeltaKey()) != string(want[:]) {
		t.Fatalf("delta = %v, %v", r, err)
	}
	if got := srv.Stats().ResolveN; got != 2 {
		t.Fatalf("ResolveN = %d", got)
	}
}

// Checks that when both an alias and the independent object exist, Resolve answers
// independent rather than delta.
func TestResolvePrefersIndependentOverAlias(t *testing.T) {
	// Both can exist after an independent-vs-delta race. Readers address objects
	// by manifest, so either is valid; Resolve must give one stable answer.
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})
	indep := []byte("independent")
	h := objectKey(indep)
	if _, err := putRep(t, cli, []byte("patch"), h); err != nil {
		t.Fatal(err)
	}
	if _, err := streamPut(t, cli, pb.Partition_PARTITION_CHUNK, h, indep); err != nil {
		t.Fatal(err)
	}
	r, err := resolve(t, cli, h)
	if err != nil || r.GetResolution() != pb.Resolution_RESOLUTION_INDEPENDENT {
		t.Fatalf("resolve = %v, %v", r, err)
	}
}

// Checks that Resolve rejects bad requests with the right status code: a
// path-like generation and a missing generation are InvalidArgument, a generation
// that is not current is FailedPrecondition, and a key shorter than 32 bytes is
// InvalidArgument.
func TestResolveValidation(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})
	h := objectKey([]byte("h"))
	for name, tc := range map[string]struct {
		req  *pb.ResolveRequest
		code codes.Code
	}{
		"bad generation": {&pb.ResolveRequest{Generation: "../x", Key: h[:]}, codes.InvalidArgument},
		"not current":    {&pb.ResolveRequest{Generation: "G9", Key: h[:]}, codes.FailedPrecondition},
		"short key":      {&pb.ResolveRequest{Generation: "G1", Key: h[:3]}, codes.InvalidArgument},
		"no generation":  {&pb.ResolveRequest{Key: h[:]}, codes.InvalidArgument},
	} {
		if _, err := cli.Resolve(context.Background(), tc.req); status.Code(err) != tc.code {
			t.Errorf("%s: code = %v, want %v", name, status.Code(err), tc.code)
		}
	}
}

// Checks that an alias is reachable only through Resolve, not as a partition. After
// a delta with represents, a Get on the chunk partition at H_ind must return
// NotFound. Also checks that an unknown partition number is rejected.
func TestAliasesAreNotAddressableAsAPartition(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})
	h := objectKey([]byte("chunk"))
	if _, err := putRep(t, cli, []byte("patch"), h); err != nil {
		t.Fatal(err)
	}
	// Alias bodies are named by H_ind in their own partition; there is no way to
	// ask Get or Put for that partition, and the chunk partition does not show it.
	if got, err := streamGet(t, cli, pb.Partition_PARTITION_CHUNK, h); status.Code(err) != codes.NotFound || got != nil {
		t.Fatalf("an alias leaked into the chunk partition: %v, %v", got, err)
	}
	if _, err := protoPartitionToStore(pb.Partition(99)); err == nil {
		t.Fatal("unknown partition accepted")
	}
}
