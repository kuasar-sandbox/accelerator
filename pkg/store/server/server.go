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

func deriveSalt(generation store.Generation) [32]byte {
	return sha256.Sum256(append([]byte("accelerator-salt-v1"), []byte(generation)...))
}

// AdmitWrite fixes one ingest to the newest current generation.
func (s *Server) AdmitWrite(_ context.Context, _ *pb.AdmitWriteRequest) (*pb.AdmitWriteResponse, error) {
	s.stats.admitN.Add(1)
	gens := s.generations()
	if len(gens) == 0 {
		s.stats.errN.Add(1)
		return nil, status.Error(codes.Unavailable, "generation list is empty")
	}
	gen := gens[len(gens)-1]
	salt := deriveSalt(gen)
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
		return stream.SendAndClose(&pb.PutResponse{IsNew: false})
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
	return stream.SendAndClose(&pb.PutResponse{IsNew: isNew})
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
