package client

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/fullof-work/mass-sandbox/pkg/cache"
	"github.com/fullof-work/mass-sandbox/pkg/cache/wire"
	"github.com/fullof-work/mass-sandbox/pkg/store"
)

// ShardCloser = cache.ShardTier + io.Closer. The client package's
// exported shard-side contract. Constructors return ShardCloser so
// callers can close the underlying wire resources explicitly.
type ShardCloser interface {
	cache.ShardTier
	io.Closer
}

// shardImpl is the private wire-client implementation of ShardCloser.
type shardImpl struct {
	getPool  *ConnPool
	fillPool *ConnPool
	timeout  time.Duration
	blobPool cache.BlobPool

	endpoint  string
	hits      atomic.Uint64
	misses    atomic.Uint64 // wire.StatusMiss only
	errors    atomic.Uint64 // pool/wire errors + wire.StatusError
	cancelled atomic.Uint64 // wire.StatusCancelled
	fills     atomic.Uint64
}

// NewShard dials endpoint and returns a shard-level read+write handle.
// Internally creates two connection pools: getPool (pre-established,
// for GetShard) and fillPool (on-demand, for FillShard). This prevents
// fire-and-forget backfill writes from starving read operations.
func NewShard(endpoint string, opts Options) (ShardCloser, error) {
	if opts.Pool <= 0 {
		opts.Pool = 4
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 2 * time.Second
	}
	if opts.BlobPool == nil {
		opts.BlobPool = cache.DefaultPool
	}
	gp, err := DialConnPool(endpoint, ConnPoolConfig{
		PoolSize: opts.Pool,
		MaxSize:  opts.Pool,
		Timeout:  opts.Timeout,
	})
	if err != nil {
		return nil, err
	}
	fp, err := DialConnPool(endpoint, ConnPoolConfig{
		PoolSize: 0,
		MaxSize:  1,
		Timeout:  opts.Timeout,
		IdleMax:  30 * time.Second,
	})
	if err != nil {
		gp.Close()
		return nil, err
	}
	return &shardImpl{
		getPool:  gp,
		fillPool: fp,
		timeout:  opts.Timeout,
		blobPool: opts.BlobPool,
		endpoint: endpoint,
	}, nil
}

// GetShard implements cache.ShardGetter. Fetches the shard (if any)
// that this peer holds for key — the peer may hold any one of the
// N RS shard slots; the shard's (idx, total) is encoded in the first
// cache.ShardPrefixSize bytes of the returned Blob. Callers unpack
// and aggregate shards by idx for RS decode.
//
// Error semantics (symmetric with client.impl.Get in object.go):
//   - StatusHit      → (CacheHit, blob, nil)       — blob begins with [idx][total]
//   - StatusMiss     → (CacheMiss, nil, nil)       — peer-confirmed absence
//   - StatusCancelled→ (CacheMiss, nil, ErrCancelled) — server honored cancel
//   - StatusError    → (CacheMiss, nil, error)      — server-side error
//   - wire/pool err  → (CacheMiss, nil, error)      — transport failure
//
// Callers distinguish "peer confirmed absent" (nil err) from "transport
// error / server error / cancellation" (non-nil err). EC.Get uses this
// to decide whether reconstruction backfill is safe — a transport err
// means we don't know whether the peer has the data, so repairing it
// would be wrong.
func (c *shardImpl) GetShard(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	pc, err := c.getPool.Acquire(ctx)
	if err != nil {
		c.errors.Add(1)
		return cache.CacheMiss, nil, fmt.Errorf("cache: GetShard acquire: %w", err)
	}

	req := &wire.Request{
		Opcode:    wire.OpcodeShardGet,
		Namespace: partitionToWireNS(p),
		Hash:      key,
	}

	pc.SetOpDeadline(ctx, c.timeout)
	if err := pc.WriteRequest(req); err != nil {
		c.getPool.Release(pc, false)
		c.errors.Add(1)
		return cache.CacheMiss, nil, fmt.Errorf("cache: GetShard write: %w", err)
	}

	// Cancel watcher — started after write completes (no concurrent writes).
	watchDone := make(chan struct{})
	watchExited := make(chan struct{})
	go func() {
		defer close(watchExited)
		select {
		case <-ctx.Done():
			_ = pc.SendCancel()
		case <-watchDone:
		}
	}()

	resp, err := pc.ReadResponse(c.blobPool)
	close(watchDone)
	<-watchExited // ensure watcher exited before Release

	if err != nil {
		c.getPool.Release(pc, false)
		c.errors.Add(1)
		return cache.CacheMiss, nil, fmt.Errorf("cache: GetShard read: %w", err)
	}
	c.getPool.Release(pc, true)

	switch resp.Status {
	case wire.StatusHit:
		c.hits.Add(1)
		return cache.CacheHit, resp.Value, nil
	case wire.StatusMiss:
		c.misses.Add(1)
		return cache.CacheMiss, nil, nil
	case wire.StatusCancelled:
		c.cancelled.Add(1)
		if resp.Value != nil {
			resp.Value.Release()
		}
		return cache.CacheMiss, nil, ErrCancelled
	case wire.StatusError:
		c.errors.Add(1)
		if resp.Value != nil {
			resp.Value.Release()
		}
		return cache.CacheMiss, nil, fmt.Errorf("cache: GetShard server error: %s", resp.ErrMsg)
	default:
		c.errors.Add(1)
		if resp.Value != nil {
			resp.Value.Release()
		}
		return cache.CacheMiss, nil, fmt.Errorf("cache: GetShard unexpected status %d", resp.Status)
	}
}

// FillShard implements cache.ShardFiller. Writes a single RS shard.
//
// value must already carry the [idx][total] prefix (see
// cache.EncodeShardPrefix). We pass it through verbatim so callers
// that allocate prefix-padded buffers upfront (EC Fill path) avoid any
// secondary copy on the hot path.
func (c *shardImpl) FillShard(ctx context.Context, p store.Partition, key store.ContentKey, value []byte) error {
	pc, err := c.fillPool.Acquire(ctx)
	if err != nil {
		return err
	}

	req := &wire.Request{
		Opcode:    wire.OpcodeShardPut,
		Namespace: partitionToWireNS(p),
		Hash:      key,
		Value:     value,
	}

	pc.SetOpDeadline(ctx, c.timeout)
	if err := pc.WriteRequest(req); err != nil {
		c.fillPool.Release(pc, false)
		return err
	}

	resp, err := pc.ReadResponse(c.blobPool)
	if err != nil {
		c.fillPool.Release(pc, false)
		return err
	}
	if resp.Value != nil {
		resp.Value.Release()
	}
	c.fillPool.Release(pc, true)

	if resp.Status == wire.StatusError {
		return fmt.Errorf("cache: shard fill: %s", resp.ErrMsg)
	}
	c.fills.Add(1)
	return nil
}

// Close closes both underlying connection pools.
func (c *shardImpl) Close() error {
	err1 := c.getPool.Close()
	err2 := c.fillPool.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

var _ cache.ShardTier = (*shardImpl)(nil)
var _ ShardCloser = (*shardImpl)(nil)
