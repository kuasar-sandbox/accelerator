package server

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
)

// Server implements pb.StoreServer. The generation callback returns one
// immutable oldest-to-newest list. verifyKey controls whether the server
// re-hashes data during Put to enforce the client-claimed ContentKey.
type Server struct {
	pb.UnimplementedStoreServer

	backend     Backend
	generations func() []store.Generation
	verifyKey   bool

	stats Stats
}

// Options configures a new Server.
type Options struct {
	// Backend is the storage implementation. Must be non-nil.
	Backend Backend

	// Generations returns the current immutable oldest-to-newest list.
	// Each request calls it exactly once.
	Generations func() []store.Generation

	// VerifyKey toggles per-Put SHA256 verification. Default true;
	// set false in trusted-loader contexts where the extra hash pass
	// on the server side is redundant (e.g. batch backfill from a
	// known-good mirror).
	VerifyKey bool
}

// New constructs a Server.
func New(opts Options) (*Server, error) {
	if opts.Backend == nil {
		return nil, errors.New("server: backend is required")
	}
	if opts.Generations == nil {
		return nil, errors.New("server: generations callback is required")
	}
	if err := store.ValidateGenerations(opts.Generations()); err != nil {
		return nil, fmt.Errorf("server: initial generations: %w", err)
	}
	return &Server{
		backend:     opts.Backend,
		generations: opts.Generations,
		verifyKey:   opts.VerifyKey,
	}, nil
}

// AdmitWrite fixes one ingest to the requested current generation, or to the
// newest current generation when the request leaves generation empty.
func (s *Server) AdmitWrite(_ context.Context, req *pb.AdmitWriteRequest) (*pb.AdmitWriteResponse, error) {
	s.stats.admitN.Add(1)
	gens := s.generations()
	if len(gens) == 0 {
		s.stats.errN.Add(1)
		return nil, status.Error(codes.Unavailable, "generation list is empty")
	}
	gen := store.Generation(req.GetGeneration())
	if gen == "" {
		gen = gens[len(gens)-1]
	} else {
		if err := store.ValidateGeneration(gen); err != nil {
			s.stats.errN.Add(1)
			return nil, status.Errorf(codes.InvalidArgument, "admit write: generation: %v", err)
		}
		if !containsGeneration(gens, gen) {
			s.stats.errN.Add(1)
			return nil, status.Errorf(codes.FailedPrecondition, "admit write: generation %q is not current", gen)
		}
	}
	salt, err := store.SaltForGeneration(gen)
	if err != nil {
		s.stats.errN.Add(1)
		return nil, status.Errorf(codes.Internal, "admit write: canonical salt: %v", err)
	}
	return &pb.AdmitWriteResponse{Generation: string(gen), Salt: salt[:]}, nil
}

// GetObject performs the shared newest-to-oldest lookup used by both the
// store gRPC Get and store-ctl's embedded cache wire listener.
func (s *Server) GetObject(ctx context.Context, partition store.Partition, key store.ContentKey) (bool, []byte, error) {
	gens := s.generations()
	return getAcrossGenerations(ctx, s.backend, gens, partition, key)
}

func getAcrossGenerations(ctx context.Context, backend Backend, generations []store.Generation, partition store.Partition, key store.ContentKey) (bool, []byte, error) {
	for i := len(generations) - 1; i >= 0; i-- {
		found, data, err := backend.Get(ctx, generations[i], partition, key)
		if err != nil || found {
			return found, data, err
		}
	}
	return false, nil, nil
}

// Get streams a stored object back to the caller. Miss → NotFound,
// hit → one or more GetResponse messages with byte chunks.
func (s *Server) Get(req *pb.GetRequest, stream pb.Store_GetServer) error {
	s.stats.inflight.Add(1)
	start := time.Now()
	defer func() {
		s.stats.inflight.Add(-1)
		s.stats.getN.Add(1)
		s.stats.getHist.Record(time.Since(start))
	}()

	partition, err := protoPartitionToStore(req.GetPartition())
	if err != nil {
		s.stats.errN.Add(1)
		return status.Error(codes.InvalidArgument, err.Error())
	}
	var key store.ContentKey
	if len(req.GetKey()) != len(key) {
		s.stats.errN.Add(1)
		return status.Errorf(codes.InvalidArgument, "key must be %d bytes, got %d", len(key), len(req.GetKey()))
	}
	copy(key[:], req.GetKey())

	found, data, err := s.GetObject(stream.Context(), partition, key)
	if err != nil {
		s.stats.errN.Add(1)
		return status.Errorf(codes.Internal, "backend get: %v", err)
	}
	if !found {
		return status.Error(codes.NotFound, "")
	}
	s.stats.getHits.Add(1)
	s.stats.getBytes.Add(uint64(len(data)))

	// Stream the bytes out in 256 KiB frames.
	for off := 0; off < len(data); off += sendFrameSize {
		end := off + sendFrameSize
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&pb.GetResponse{Data: data[off:end]}); err != nil {
			return err
		}
	}
	return nil
}

// Put ingests a streaming object. The first message must be a
// PutHeader (partition + client-computed key); subsequent messages
// carry raw byte chunks. On a dedup hit (key already exists in the
// admitted generation) the server SendAndCloses immediately after the
// header, saving the full data payload's worth of bandwidth.
// Otherwise bytes stream directly into the Backend's generation-bound
// PutHandle and are optionally re-hashed for verification.
func (s *Server) Put(stream pb.Store_PutServer) error {
	s.stats.inflight.Add(1)
	start := time.Now()
	defer func() {
		s.stats.inflight.Add(-1)
		s.stats.putN.Add(1)
		s.stats.putHist.Record(time.Since(start))
	}()

	// 1. Receive header.
	first, err := stream.Recv()
	if err != nil {
		s.stats.errN.Add(1)
		return status.Errorf(codes.InvalidArgument, "put: recv header: %v", err)
	}
	header := first.GetHeader()
	if header == nil {
		s.stats.errN.Add(1)
		return status.Error(codes.InvalidArgument, "put: first message must be a header")
	}
	partition, err := protoPartitionToStore(header.GetPartition())
	if err != nil {
		s.stats.errN.Add(1)
		return status.Error(codes.InvalidArgument, err.Error())
	}
	var key store.ContentKey
	if len(header.GetKey()) != len(key) {
		s.stats.errN.Add(1)
		return status.Errorf(codes.InvalidArgument, "key must be %d bytes, got %d", len(key), len(header.GetKey()))
	}
	copy(key[:], header.GetKey())
	generation := store.Generation(header.GetGeneration())
	if err := store.ValidateGeneration(generation); err != nil {
		s.stats.errN.Add(1)
		return status.Errorf(codes.InvalidArgument, "put: generation: %v", err)
	}
	generations := s.generations()
	if !containsGeneration(generations, generation) {
		s.stats.errN.Add(1)
		return status.Errorf(codes.FailedPrecondition, "put: generation %q is not current", generation)
	}
	var represents *store.ContentKey
	if r := header.GetRepresents(); len(r) != 0 {
		if partition != store.PartitionChunk {
			s.stats.errN.Add(1)
			return status.Error(codes.InvalidArgument, "put: represents is only valid for the chunk partition")
		}
		var h store.ContentKey
		if len(r) != len(h) {
			s.stats.errN.Add(1)
			return status.Errorf(codes.InvalidArgument, "put: represents must be %d bytes, got %d", len(h), len(r))
		}
		copy(h[:], r)
		if h == key {
			s.stats.errN.Add(1)
			return status.Error(codes.InvalidArgument, "put: an object cannot represent itself")
		}
		represents = &h
	}
	var expectedSize *int64
	if header.Size != nil {
		if *header.Size > math.MaxInt64 {
			s.stats.errN.Add(1)
			return status.Errorf(codes.InvalidArgument, "put: size %d overflows int64", *header.Size)
		}
		n := int64(*header.Size)
		expectedSize = &n
	}

	// 2. Dedup short-circuit — if the key already exists in the
	// admitted generation, tell the client "not new" and close the
	// stream. The client will see io.EOF on its next Send (or it
	// may have Sent everything already; CloseAndRecv still returns
	// the response we emit here).
	exists, err := s.backend.Exists(stream.Context(), generation, partition, key, expectedSize)
	if err != nil {
		s.stats.errN.Add(1)
		return status.Errorf(codes.Internal, "put: exists: %v", err)
	}
	if exists {
		s.stats.putDedup.Add(1)
		return stream.SendAndClose(s.putResponse(stream.Context(), generation, partition, false, 0, key, represents))
	}

	// 3. Open a streaming-Put handle; bytes land directly on disk.
	handle, err := s.backend.OpenPut(generation, partition, key, expectedSize)
	if err != nil {
		s.stats.errN.Add(1)
		return status.Errorf(codes.Internal, "open put: %v", err)
	}
	if contextual, ok := handle.(interface{ SetContext(context.Context) }); ok {
		contextual.SetContext(stream.Context())
	}
	var hasher hash.Hash
	if s.verifyKey {
		hasher = sha256.New()
	}

	// 4. Drain data frames until stream end.
	var nbytes int64
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.stats.errN.Add(1)
			_ = handle.Abort()
			return status.Errorf(codes.Internal, "put: recv data: %v", err)
		}
		data := msg.GetData()
		if len(data) == 0 {
			// An empty data frame is allowed (noop) but we do NOT
			// accept a second PutHeader mid-stream.
			if msg.GetHeader() != nil {
				s.stats.errN.Add(1)
				_ = handle.Abort()
				return status.Error(codes.InvalidArgument, "put: header after data")
			}
			continue
		}
		if int64(len(data)) > math.MaxInt64-nbytes {
			s.stats.errN.Add(1)
			_ = handle.Abort()
			return status.Error(codes.InvalidArgument, "put: payload size overflows int64")
		}
		if expectedSize != nil && int64(len(data)) > *expectedSize-nbytes {
			s.stats.errN.Add(1)
			_ = handle.Abort()
			return status.Errorf(codes.InvalidArgument, "put: payload exceeds expected size %d", *expectedSize)
		}
		n, err := handle.Write(data)
		if err != nil {
			s.stats.errN.Add(1)
			_ = handle.Abort()
			return status.Errorf(codes.Internal, "put: write: %v", err)
		}
		if n != len(data) {
			s.stats.errN.Add(1)
			_ = handle.Abort()
			return status.Errorf(codes.Internal, "put: write: %v", io.ErrShortWrite)
		}
		nbytes += int64(n)
		if hasher != nil {
			hasher.Write(data)
		}
	}
	if expectedSize != nil && nbytes != *expectedSize {
		s.stats.errN.Add(1)
		_ = handle.Abort()
		return status.Errorf(codes.InvalidArgument, "put: payload size %d, expected %d", nbytes, *expectedSize)
	}

	// 5. Commit (with optional verify).
	verifyDigest := key
	if hasher != nil {
		copy(verifyDigest[:], hasher.Sum(nil))
	}
	isNew, err := handle.Commit(key, verifyDigest)
	if err != nil {
		s.stats.errN.Add(1)
		return status.Errorf(codes.InvalidArgument, "put: commit: %v", err)
	}
	if isNew {
		s.stats.putBytes.Add(uint64(nbytes))
	} else {
		s.stats.putDedup.Add(1)
	}
	return stream.SendAndClose(s.putResponse(stream.Context(), generation, partition, isNew, uint64(nbytes), key, represents))
}

// putResponse builds the Put reply. When the client asked for the object to be
// recorded as the representation of another chunk, it attempts that after the
// object is durably stored (new or already present) and reports the outcome.
//
// The alias is the only serialisation point between writers, so it is created
// first and decides everything: whoever's object it names has WON; everyone
// else has LOST. No separate "does the independent object exist" check is
// made here. Such a check could only produce an outcome the client cannot act
// on safely (an object nobody is allowed to reference but nobody may delete,
// because another writer may have just been told it WON with that very
// object). If an independent object has landed in the meantime, Resolve
// prefers it, later writers converge on it, and the delta the winner recorded
// stays referenced by the winner's manifest: not an orphan.
//
// A LOST object that this Put itself created is deleted again. That is safe
// precisely because the alias is write-once: within this generation the alias
// will never name this object, a client records a delta object only when the
// alias names it, so no manifest can ever point at it. A LOST object that
// already existed (dedup hit) is left to the request that created it: that
// request sees the same alias, so it also LOST, and it deletes its own object.
//
// Failing to record the alias is not failing the Put: the object is stored and
// valid, and the client treats REPRESENTED_NONE as "store the chunk
// independently"; the object stays as the only kind of orphan this design
// still produces, and only on an error path.
func (s *Server) putResponse(ctx context.Context, generation store.Generation, partition store.Partition, isNew bool, nbytes uint64, key store.ContentKey, represents *store.ContentKey) *pb.PutResponse {
	resp := &pb.PutResponse{IsNew: isNew}
	if represents == nil {
		return resp
	}
	ab, ok := s.backend.(AliasBackend)
	if !ok {
		s.stats.aliasErr.Add(1)
		return resp
	}
	current, created, err := ab.CreateAlias(ctx, generation, *represents, key)
	switch {
	case err != nil:
		s.stats.aliasErr.Add(1)
	case created || current == key:
		s.stats.aliasWon.Add(1)
		resp.Represented = pb.Represented_REPRESENTED_WON
	default:
		s.stats.aliasLost.Add(1)
		resp.Represented = pb.Represented_REPRESENTED_LOST
		resp.RepresentedKey = current[:]
		if isNew {
			if err := ab.Delete(ctx, generation, partition, key); err != nil {
				s.stats.discardErr.Add(1)
			} else {
				s.stats.discarded.Add(1)
				s.stats.discardedBytes.Add(nbytes)
				resp.Discarded = true
			}
		}
	}
	return resp
}

// Resolve reports how one generation stores the chunk whose independent
// address is the request key. It looks only at the named generation: aliases
// are scoped to the dedup domain they were created in, and an object in an
// older generation was encrypted under a different salt.
func (s *Server) Resolve(ctx context.Context, req *pb.ResolveRequest) (*pb.ResolveResponse, error) {
	s.stats.resolveN.Add(1)
	ab, ok := s.backend.(AliasBackend)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "resolve: backend does not support aliases")
	}
	generation := store.Generation(req.GetGeneration())
	if err := store.ValidateGeneration(generation); err != nil {
		s.stats.errN.Add(1)
		return nil, status.Errorf(codes.InvalidArgument, "resolve: generation: %v", err)
	}
	if !containsGeneration(s.generations(), generation) {
		s.stats.errN.Add(1)
		return nil, status.Errorf(codes.FailedPrecondition, "resolve: generation %q is not current", generation)
	}
	var key store.ContentKey
	if len(req.GetKey()) != len(key) {
		s.stats.errN.Add(1)
		return nil, status.Errorf(codes.InvalidArgument, "resolve: key must be %d bytes, got %d", len(key), len(req.GetKey()))
	}
	copy(key[:], req.GetKey())

	indep, err := s.backend.Exists(ctx, generation, store.PartitionChunk, key, nil)
	if err != nil {
		s.stats.errN.Add(1)
		return nil, status.Errorf(codes.Internal, "resolve: exists: %v", err)
	}
	if indep {
		return &pb.ResolveResponse{Resolution: pb.Resolution_RESOLUTION_INDEPENDENT}, nil
	}
	delta, found, err := ab.GetAlias(ctx, generation, key)
	if err != nil {
		s.stats.errN.Add(1)
		return nil, status.Errorf(codes.Internal, "resolve: alias: %v", err)
	}
	if !found {
		return &pb.ResolveResponse{Resolution: pb.Resolution_RESOLUTION_ABSENT}, nil
	}
	return &pb.ResolveResponse{Resolution: pb.Resolution_RESOLUTION_DELTA, DeltaKey: delta[:]}, nil
}

func containsGeneration(generations []store.Generation, generation store.Generation) bool {
	for _, g := range generations {
		if g == generation {
			return true
		}
	}
	return false
}

// sendFrameSize is the target byte size for a single Get response
// chunk. 256 KiB fits comfortably under the 4 MiB gRPC default
// max_recv_msg_size and keeps the pipeline efficient.
const sendFrameSize = 256 * 1024

func protoPartitionToStore(p pb.Partition) (store.Partition, error) {
	switch p {
	case pb.Partition_PARTITION_CHUNK:
		return store.PartitionChunk, nil
	case pb.Partition_PARTITION_MANIFEST:
		return store.PartitionManifest, nil
	case pb.Partition_PARTITION_BLOB:
		return store.PartitionBlob, nil
	default:
		return "", errors.New("unknown partition")
	}
}
