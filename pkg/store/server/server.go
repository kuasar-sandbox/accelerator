package server

import (
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/store"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/store/pb"
)

// Server implements pb.StoreServer. The streaming Put path writes
// bytes directly into a temp file owned by the Backend's PutHandle,
// so the server never buffers the full payload in memory. verifyKey
// controls whether the server re-hashes data during Put to enforce
// the client-claimed ContentKey.
type Server struct {
	pb.UnimplementedStoreServer

	backend   Backend
	salt      [32]byte
	gen       string
	verifyKey bool

	stats Stats
}

// Options configures a new Server.
type Options struct {
	// Backend is the storage implementation. Must be non-nil.
	Backend Backend

	// VerifyKey toggles per-Put SHA256 verification. Default true;
	// set false in trusted-loader contexts where the extra hash pass
	// on the server side is redundant (e.g. batch backfill from a
	// known-good mirror).
	VerifyKey bool
}

// New constructs a Server. Salt is derived at construction time from
// the backend's active generation and surfaced via GetSalt.
func New(opts Options) (*Server, error) {
	if opts.Backend == nil {
		return nil, errors.New("server: backend is required")
	}
	gen := opts.Backend.ActiveGeneration()
	return &Server{
		backend:   opts.Backend,
		salt:      crypto.DeriveSalt(gen),
		gen:       gen,
		verifyKey: opts.VerifyKey,
	}, nil
}

// GetSalt returns the active generation name and its derived salt.
// The salt derivation algorithm is a server-side concern; clients
// combine this salt with any extra salt they control.
func (s *Server) GetSalt(ctx context.Context, _ *pb.GetSaltRequest) (*pb.GetSaltResponse, error) {
	s.stats.saltN.Add(1)
	return &pb.GetSaltResponse{
		Generation: s.gen,
		Salt:       s.salt[:],
	}, nil
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

	found, data, err := s.backend.Get(stream.Context(), partition, key)
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
// active generation) the server SendAndCloses immediately after the
// header, saving the full data payload's worth of bandwidth.
// Otherwise bytes stream directly into a temp file via the
// Backend's PutHandle, optionally re-hashed for verification, and
// atomic-renamed at the end.
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

	// 2. Dedup short-circuit — if the key already exists in the
	// active generation, tell the client "not new" and close the
	// stream. The client will see io.EOF on its next Send (or it
	// may have Sent everything already; CloseAndRecv still returns
	// the response we emit here).
	if s.backend.Exists(partition, key) {
		s.stats.putDedup.Add(1)
		return stream.SendAndClose(&pb.PutResponse{IsNew: false})
	}

	// 3. Open a streaming-Put handle; bytes land directly on disk.
	handle, err := s.backend.OpenPut(partition)
	if err != nil {
		s.stats.errN.Add(1)
		return status.Errorf(codes.Internal, "open put: %v", err)
	}
	var hasher hash.Hash
	if s.verifyKey {
		hasher = sha256.New()
	}

	// 4. Drain data frames until stream end.
	var nbytes uint64
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
		if _, err := handle.Write(data); err != nil {
			s.stats.errN.Add(1)
			_ = handle.Abort()
			return status.Errorf(codes.Internal, "put: write: %v", err)
		}
		nbytes += uint64(len(data))
		if hasher != nil {
			hasher.Write(data)
		}
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
	s.stats.putBytes.Add(nbytes)
	return stream.SendAndClose(&pb.PutResponse{IsNew: isNew})
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
	default:
		return "", errors.New("unknown partition")
	}
}
