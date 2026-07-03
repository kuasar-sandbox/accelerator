// Package client implements a gRPC client for the store-ctl service.
//
// The client exposes byte-level Get / Put against the remote store.
// Get buffers streamed frames into a single byte slice; Put streams
// the caller's []byte to the server in 256 KiB frames after sending
// a header carrying the client-computed ContentKey.
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
)

// frameSize is the per-message payload cap on the wire for
// streaming Put. 256 KiB is well under gRPC's 4 MiB default
// max_recv_msg_size and matches the server's send chunk size.
const frameSize = 256 * 1024

// Client wraps a pool of gRPC ClientConn instances against a single
// store-ctl endpoint. Round-robin RPC dispatch across the pool gives
// HTTP/2 head-of-line-blocking relief for concurrent large transfers
// while keeping the per-conn multiplexing benefits of gRPC.
type Client struct {
	conns   []*grpc.ClientConn
	stubs   []pb.StoreClient
	next    atomic.Uint32
	timeout time.Duration
}

// New dials `pool` independent gRPC ClientConns against endpoint
// (insecure transport — store-ctl is an internal service). pool=0
// defaults to 4. timeout is the per-RPC wall-clock budget; 0 means
// no client-side deadline (server-side enforcement still applies).
func New(endpoint string, pool int, timeout time.Duration) (*Client, error) {
	if endpoint == "" {
		return nil, errors.New("client: endpoint is required")
	}
	if pool <= 0 {
		pool = 4
	}
	target := normalizeTarget(endpoint)
	conns := make([]*grpc.ClientConn, 0, pool)
	stubs := make([]pb.StoreClient, 0, pool)
	for i := 0; i < pool; i++ {
		cc, err := grpc.NewClient(
			target,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			// Rollback partial pool on failure.
			for _, c := range conns {
				_ = c.Close()
			}
			return nil, fmt.Errorf("client: dial %s: %w", endpoint, err)
		}
		conns = append(conns, cc)
		stubs = append(stubs, pb.NewStoreClient(cc))
	}
	return &Client{
		conns:   conns,
		stubs:   stubs,
		timeout: timeout,
	}, nil
}

// Close closes every ClientConn in the pool. Errors from individual
// closes are joined into a single returned error.
func (c *Client) Close() error {
	var errs []error
	for _, cc := range c.conns {
		if err := cc.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// PoolSize reports the number of independent gRPC connections (the
// configured store.pool). It is the real client-side parallelism limit
// — round-robin RPC dispatch can only have this many Puts in genuinely
// concurrent flight — so callers (e.g. the ingest worker pool) size
// their concurrency to it. Race-free: stubs is fixed at New().
func (c *Client) PoolSize() int { return len(c.stubs) }

// pickStub returns the next gRPC stub via atomic round-robin. With
// pool=1 every call returns the same stub; with larger pools the
// counter wraps modulo len(stubs).
func (c *Client) pickStub() pb.StoreClient {
	idx := c.next.Add(1) - 1
	return c.stubs[int(idx%uint32(len(c.stubs)))]
}

// withTimeout wraps ctx in a deadline if c.timeout > 0. Returns the
// derived context and a cancel func; callers MUST defer cancel.
func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.timeout)
}

// GetSalt returns the server's active generation name and the salt
// bytes derived from it. Callers combine this with their own
// extra-salt to produce the convergent-encryption seed.
func (c *Client) GetSalt(ctx context.Context) (generation string, salt [32]byte, err error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	resp, err := c.pickStub().GetSalt(ctx, &pb.GetSaltRequest{})
	if err != nil {
		return "", [32]byte{}, fmt.Errorf("store: GetSalt: %w", err)
	}
	if len(resp.GetSalt()) != 32 {
		return "", [32]byte{}, fmt.Errorf("store: GetSalt: salt length %d, want 32", len(resp.GetSalt()))
	}
	copy(salt[:], resp.GetSalt())
	return resp.GetGeneration(), salt, nil
}

// Get fetches a stored object as raw bytes. The server-streamed
// frames are buffered into a single slice on the client side —
// callers materialise anyway. Miss is translated from gRPC NotFound
// to (false, nil, nil).
func (c *Client) Get(ctx context.Context, partition store.Partition, key store.ContentKey) (bool, []byte, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	pp, err := storePartitionToProto(partition)
	if err != nil {
		return false, nil, err
	}
	stream, err := c.pickStub().Get(ctx, &pb.GetRequest{Partition: pp, Key: key[:]})
	if err != nil {
		return false, nil, fmt.Errorf("store: Get open: %w", err)
	}

	var buf bytes.Buffer
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return false, nil, nil
			}
			return false, nil, fmt.Errorf("store: Get recv: %w", err)
		}
		buf.Write(msg.GetData())
	}
	return true, buf.Bytes(), nil
}

// Put streams a byte payload to the store. The caller-supplied key is
// sent in the header; data is then framed into 256 KiB chunks and
// streamed. The server may SendAndClose early on dedup hits — the
// client handles the resulting io.EOF on its next Send by breaking
// out of the loop; CloseAndRecv still returns the response.
func (c *Client) Put(ctx context.Context, partition store.Partition, key store.ContentKey, data []byte) (bool, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	pp, err := storePartitionToProto(partition)
	if err != nil {
		return false, err
	}
	stream, err := c.pickStub().Put(ctx)
	if err != nil {
		return false, fmt.Errorf("store: Put open: %w", err)
	}

	// 1. Header.
	if err := stream.Send(&pb.PutRequest{
		Body: &pb.PutRequest_Header{
			Header: &pb.PutHeader{Partition: pp, Key: key[:]},
		},
	}); err != nil {
		return false, fmt.Errorf("store: Put send header: %w", err)
	}

	// 2. Data frames.
	for off := 0; off < len(data); off += frameSize {
		end := off + frameSize
		if end > len(data) {
			end = len(data)
		}
		err := stream.Send(&pb.PutRequest{Body: &pb.PutRequest_Data{Data: data[off:end]}})
		if err != nil {
			if err == io.EOF {
				// Server closed the stream early, e.g. dedup
				// short-circuit. CloseAndRecv below still pulls the
				// response; do NOT return here.
				break
			}
			return false, fmt.Errorf("store: Put send data: %w", err)
		}
	}

	// 3. Close + recv.
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return false, fmt.Errorf("store: Put close: %w", err)
	}
	return resp.GetIsNew(), nil
}

// normalizeTarget rewrites a Unix-socket endpoint into gRPC's canonical
// "unix:///abs" target so a bare path ("/run/store.sock") or the cache client's
// "unix://path" form dials the same way; an explicit "unix:"/"unix-abstract:"
// scheme and any "host:port" pass through unchanged for gRPC's own resolver.
func normalizeTarget(endpoint string) string {
	switch {
	case strings.HasPrefix(endpoint, "unix:"):
		return endpoint // gRPC resolves unix:/unix:///unix-abstract: natively
	case strings.HasPrefix(endpoint, "/"):
		return "unix://" + endpoint // absolute path -> unix:///abs
	default:
		return endpoint
	}
}

func storePartitionToProto(p store.Partition) (pb.Partition, error) {
	switch p {
	case store.PartitionChunk:
		return pb.Partition_PARTITION_CHUNK, nil
	case store.PartitionManifest:
		return pb.Partition_PARTITION_MANIFEST, nil
	case store.PartitionBlob:
		return pb.Partition_PARTITION_BLOB, nil
	default:
		return pb.Partition_PARTITION_UNSPECIFIED, fmt.Errorf("store: unknown partition %q", p)
	}
}
