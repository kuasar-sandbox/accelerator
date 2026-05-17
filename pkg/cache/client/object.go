package client

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/fullof-work/mass-sandbox/pkg/cache"
	"github.com/fullof-work/mass-sandbox/pkg/cache/wire"
	"github.com/fullof-work/mass-sandbox/pkg/store"
)

// Options configures a cache-ctl wire client.
type Options struct {
	Pool     int             // number of parallel connections (default 4)
	Timeout  time.Duration   // per-op timeout (default 2s)
	BlobPool cache.BlobPool  // payload allocator; nil → cache.DefaultPool
}

// GetCloser = cache.Getter + io.Closer. The client package's exported
// read-side contract — Close is a first-class part of the contract so
// callers never need to runtime-assert io.Closer on the returned value.
type GetCloser interface {
	cache.Getter
	io.Closer
}

// TierCloser = cache.Tier + io.Closer. The client package's exported
// read+write contract.
type TierCloser interface {
	cache.Tier
	io.Closer
}

// impl is the private wire-client that underlies both NewGetter and
// New. It maintains two connection pools to isolate Get and Fill
// traffic: getPool is pre-established for low-latency reads; fillPool
// dials on-demand (capped at 1) so backfill writes can never starve
// reads.
type impl struct {
	getPool  *ConnPool
	fillPool *ConnPool
	timeout  time.Duration
	blobPool cache.BlobPool
}

func newImpl(endpoint string, opts Options) (*impl, error) {
	if opts.Pool <= 0 {
		opts.Pool = 4
	}
	// opts.Timeout <= 0 stays 0 = no per-call deadline: bounded only by
	// the caller's context, never an arbitrary number.
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
	return &impl{getPool: gp, fillPool: fp, timeout: opts.Timeout, blobPool: opts.BlobPool}, nil
}

// NewGetter dials endpoint and returns a read-only handle.
func NewGetter(endpoint string, opts Options) (GetCloser, error) {
	return newImpl(endpoint, opts)
}

// New dials endpoint and returns a full read+write tier.
func New(endpoint string, opts Options) (TierCloser, error) {
	return newImpl(endpoint, opts)
}

// Get implements cache.Getter.
//
// Wire-level failures (connection, timeout, EOF) are reported as errors
// rather than silently converted to CacheMiss. Returning MISS on a
// transport failure would be semantically wrong: MISS means "the peer
// confirmed it does not have this key", whereas a transport error means
// "we don't know". In a TieredCache chain, a false MISS at origin is
// indistinguishable from a genuine absence — the cascade terminates and
// the caller is told the key doesn't exist, even though it may.
func (c *impl) Get(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	pc, err := c.getPool.Acquire(ctx)
	if err != nil {
		return cache.CacheMiss, nil, fmt.Errorf("cache: client.Get acquire: %w", err)
	}

	req := &wire.Request{
		Opcode:    wire.OpcodeObjectGet,
		Namespace: partitionToWireNS(p),
		Hash:      key,
	}

	pc.SetOpDeadline(ctx, c.timeout)
	if err := pc.WriteRequest(req); err != nil {
		c.getPool.Release(pc, false)
		return cache.CacheMiss, nil, fmt.Errorf("cache: client.Get write: %w", err)
	}

	resp, err := pc.ReadResponse(c.blobPool)
	if err != nil {
		c.getPool.Release(pc, false)
		return cache.CacheMiss, nil, fmt.Errorf("cache: client.Get read: %w", err)
	}
	c.getPool.Release(pc, true)

	switch resp.Status {
	case wire.StatusHit:
		return cache.CacheHit, resp.Value, nil
	case wire.StatusMiss:
		return cache.CacheMiss, nil, nil
	case wire.StatusError:
		return cache.CacheMiss, nil, fmt.Errorf("cache: server error: %s", resp.ErrMsg)
	default:
		return cache.CacheMiss, nil, fmt.Errorf("cache: unexpected status %d", resp.Status)
	}
}

// Fill implements cache.Filler.
func (c *impl) Fill(ctx context.Context, p store.Partition, key store.ContentKey, value []byte) error {
	pc, err := c.fillPool.Acquire(ctx)
	if err != nil {
		return err
	}

	req := &wire.Request{
		Opcode:    wire.OpcodeObjectPut,
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
		return fmt.Errorf("cache: fill: %s", resp.ErrMsg)
	}
	return nil
}

// Close closes both underlying connection pools.
func (c *impl) Close() error {
	err1 := c.getPool.Close()
	err2 := c.fillPool.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

var _ cache.Getter = (*impl)(nil)
var _ cache.Tier = (*impl)(nil)
var _ GetCloser = (*impl)(nil)
var _ TierCloser = (*impl)(nil)

// ── helpers ──

func partitionToWireNS(p store.Partition) byte {
	switch p {
	case store.PartitionChunk:
		return wire.NSChunk
	case store.PartitionManifest:
		return wire.NSManifest
	default:
		return wire.NSChunk
	}
}
