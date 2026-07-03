package ec

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// timingEnabled, when true (CACHE_CTL_TIMING=1 at process start), makes
// EC.Get log phase breakdowns at a 1% sampling rate. Off by default so
// the log has zero cost on the hot path.
var timingEnabled = os.Getenv("CACHE_CTL_TIMING") == "1"
var timingCounter atomic.Uint64

// timingShouldSample returns true roughly every 100th call. Atomic
// counter incremented regardless so the 1% property holds across
// concurrent Get callers.
func timingShouldSample() bool {
	return timingEnabled && timingCounter.Add(1)%100 == 0
}

// logECGetTiming emits a single breakdown line for a sampled Get. All
// durations are printed in µs; shard arrival offsets show how staggered
// the fan-out returned (biggest gap = hedging tail).
func logECGetTiming(key store.ContentKey, t0, tLocate, tData, tDecode time.Time,
	hits, misses int, decoded bool, arrivals []time.Duration) {
	us := func(d time.Duration) int64 { return d.Microseconds() }
	locate := us(tLocate.Sub(t0))
	fanOut := us(tData.Sub(tLocate))
	var decode int64
	if decoded {
		decode = us(tDecode.Sub(tData))
	}
	total := us(tDecode.Sub(t0))
	var firstArr, lastArr int64
	if len(arrivals) > 0 {
		firstArr = us(arrivals[0])
		lastArr = us(arrivals[len(arrivals)-1])
	}
	log.Printf("DBG EC.Get key=%x locateN=%dµs fan_out=%dµs (first=%dµs last=%dµs) decode=%dµs total=%dµs hits=%d misses=%d decoded=%v",
		key[:6], locate, fanOut, firstArr, lastArr, decode, total, hits, misses, decoded)
}

// impl implements Interface via Reed-Solomon 4/5 erasure coding across
// a cluster of cache-ctl shard nodes.
//
// Tier-level hit/miss counts live in the enclosing TieredCache, not
// here: TieredCache.Get observes the CacheResult of each tier.Get and
// increments its own parallel counters. Per-peer counts live in the
// individual shardImpl instances, surfaced via PeersStats() below.
type impl struct {
	enc    *encoder
	router *router
	pool   *peerPool

	// baseCtx parents the detached repair-backfill goroutine; baseCancel
	// (called by Close) reaps it on shutdown so a fill blocked on a
	// wedged peer can't leak when fillTimeout is 0 (= no deadline).
	baseCtx     context.Context
	baseCancel  context.CancelFunc
	fillTimeout time.Duration

	// shardScratchPool holds per-shard scratch buffers large enough
	// for a single shard at realistic value sizes (seed 256 KiB covers
	// values up to ~1 MiB). Pre-filled into the `shards` slice before
	// Reconstruct so reedsolomon reuses the buffer instead of going
	// through its own AllocAligned. Also retained by the segmented
	// blob's release closure after Get returns: as long as the caller
	// holds the blob, the scratch buffer is borrowed; Release returns
	// it to the pool.
	shardScratchPool sync.Pool

	// repairRunner, when non-nil, schedules a reconstructed-shard
	// backfill after a successful Get that required reconstruction.
	// nil disables repair (default). Set via EnableRepair so the caller
	// (typically TieredCache) can attach lifecycle tracking.
	repairRunner func(fn func())
}

// EnableRepair implements cache.Repairable. Set a non-nil runner to
// turn on reconstructed-shard backfill. Setting nil disables repair.
// Not safe to call concurrently with Get; intended for wiring time.
func (t *impl) EnableRepair(runner func(fn func())) {
	t.repairRunner = runner
}

// Config holds settings for New. BlobPool is threaded into every peer
// shard-client for coordinated payload pooling; pass nil to fall back
// to cache.DefaultPool (matches the pre-refactor NewTier default).
type Config struct {
	Cluster     ClusterConfig
	FillTimeout time.Duration // default 30s
	BlobPool    cache.BlobPool
}

// New creates an EC tier from configuration.
func New(cfg Config) (Interface, error) {
	data := cfg.Cluster.DataShards
	parity := cfg.Cluster.ParityShards
	if data <= 0 {
		data = 4
	}
	if parity <= 0 {
		parity = 1
	}
	// Fit the scheme to the peer count. data+parity > len(peers) would make
	// every Get fail at routing ("need N nodes but only M"); clamp to fit.
	if nd, np, clamped := clampShardsToPeers(data, parity, len(cfg.Cluster.Peers)); clamped {
		log.Printf("ec: data+parity (%d+%d) exceeds %d peers; clamped to %d+%d",
			data, parity, len(cfg.Cluster.Peers), nd, np)
		data, parity = nd, np
	}

	enc, err := newEncoder(data, parity)
	if err != nil {
		return nil, err
	}

	rt, err := newRouter(cfg.Cluster.Peers)
	if err != nil {
		return nil, err
	}

	poolSize := cfg.Cluster.Pool
	if poolSize <= 0 {
		poolSize = 2
	}
	timeout, _ := time.ParseDuration(cfg.Cluster.Timeout)
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	// cfg.FillTimeout <= 0 stays 0 = no fill deadline: a fill is
	// bounded only by baseCtx (Close), never an arbitrary number.
	baseCtx, baseCancel := context.WithCancel(context.Background())
	t := &impl{
		enc:         enc,
		router:      rt,
		pool:        newPeerPool(poolSize, timeout, cfg.BlobPool),
		baseCtx:     baseCtx,
		baseCancel:  baseCancel,
		fillTimeout: cfg.FillTimeout,
	}
	// Seed: 256 KiB covers a single shard for values up to ~1 MiB.
	t.shardScratchPool.New = func() any { return make([]byte, 0, 256<<10) }
	return t, nil
}

// Get implements cache.Getter. Fetches shards in parallel with
// hedging: decodes as soon as `data` distinct-idx shards arrive,
// cancels outstanding requests, and returns without waiting for the
// rest.
//
// Each peer returns whichever shard it holds (idx + total in the first
// two bytes of the blob); the EC client aggregates by peer-reported
// idx, not by fan-out position. This is what makes membership changes
// non-disruptive: surviving peers keep serving their existing shards
// under their own idx, and the decoder accepts any K of N distinct idx.
func (t *impl) Get(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	sample := timingShouldSample()
	var tStart, tLocateEnd, tDataReady, tDecodeEnd time.Time
	if sample {
		tStart = time.Now()
	}

	total := t.enc.TotalShards()
	data := t.enc.data
	parity := t.enc.parity

	peerIDs, err := t.router.LocateN(key, total)
	if err != nil {
		return cache.CacheMiss, nil, err
	}
	if sample {
		tLocateEnd = time.Now()
	}

	// shardResult carries both the peer position (peerPos, 0..total-1
	// within peerIDs) and the peer-reported shard idx (shardIdx, parsed
	// from blob prefix). They are independent: any peer may return any
	// idx. peerPos is used to target repair back to a specific peer;
	// shardIdx is used to index into the shards[] array for RS decode.
	type shardResult struct {
		peerPos       int
		shardIdx      int        // -1 on miss/error
		blob          cache.Blob // non-nil = hit; Bytes() begins with [idx][total]
		missConfirmed bool       // true only when peer replied StatusMiss
		invalid       bool       // true if blob parsed but failed validation (wrong total, bad prefix)
		arrivedAt     time.Time
	}

	results := make(chan shardResult, total)
	shardCtx, shardCancel := context.WithCancel(ctx)
	defer shardCancel()

	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(peerPos int) {
			defer wg.Done()
			endpoint := t.router.Endpoint(peerIDs[peerPos])
			sc, poolErr := t.pool.Get(endpoint)
			if poolErr != nil {
				res := shardResult{peerPos: peerPos, shardIdx: -1}
				if sample {
					res.arrivedAt = time.Now()
				}
				results <- res
				return
			}
			cr, blob, err := sc.GetShard(shardCtx, p, key)
			if cr != cache.CacheHit {
				res := shardResult{peerPos: peerPos, shardIdx: -1, missConfirmed: err == nil}
				if sample {
					res.arrivedAt = time.Now()
				}
				results <- res
				return
			}
			idx, peerTotal, _, ok := cache.ParseShardPrefix(blob.Bytes())
			if !ok || int(peerTotal) != total || int(idx) >= total {
				// Stale shard from a different RS scheme, corrupt prefix,
				// or idx out of range. Release the blob and treat as an
				// invalid response (not a confirmed miss — we don't want
				// to overwrite whatever the peer has).
				blob.Release()
				res := shardResult{peerPos: peerPos, shardIdx: -1, invalid: true}
				if sample {
					res.arrivedAt = time.Now()
				}
				results <- res
				return
			}
			res := shardResult{peerPos: peerPos, shardIdx: int(idx), blob: blob}
			if sample {
				res.arrivedAt = time.Now()
			}
			results <- res
		}(i)
	}

	// Aggregator: SOLE consumer of `results`. Reading at most `total`
	// times from an unclosed channel — fan-out goroutines always send
	// (channel cap=total, non-blocking) so every iteration gets a real
	// shardResult. A separate cleanup goroutine, launched AFTER the
	// aggregator decides, is the unique reader of the tail — never
	// races with the aggregator.
	shards := make([][]byte, total)      // indexed by shardIdx
	blobs := make([]cache.Blob, total)   // indexed by shardIdx
	seenIdx := make([]bool, total)       // idx returned by some peer on this Get
	missPeerPos := make([]int, 0, total) // peer positions that confirmed-miss (for repair)
	hits, misses := 0, 0
	var shardArrivals [5]time.Duration
	shardArrivalsN := 0

	for i := 0; i < total; i++ {
		r := <-results
		if sample && shardArrivalsN < len(shardArrivals) {
			shardArrivals[shardArrivalsN] = r.arrivedAt.Sub(tStart)
			shardArrivalsN++
		}
		if r.blob != nil {
			if seenIdx[r.shardIdx] {
				// Duplicate idx (two peers served the same shard_idx).
				// Shouldn't normally happen under the one-peer-one-shard
				// invariant; keep the first and drop the duplicate.
				r.blob.Release()
				misses++
			} else {
				seenIdx[r.shardIdx] = true
				// Strip the 2-byte [idx][total] prefix; RS decode
				// operates on the raw shard bytes.
				shards[r.shardIdx] = r.blob.Bytes()[cache.ShardPrefixSize:]
				blobs[r.shardIdx] = r.blob
				hits++
			}
		} else {
			if r.missConfirmed {
				missPeerPos = append(missPeerPos, r.peerPos)
			}
			misses++
		}
		if hits >= data || misses > parity {
			break
		}
	}
	if sample {
		tDataReady = time.Now()
	}

	// Cleanup: wait for all fan-outs, close channel, drain+release the
	// tail the aggregator didn't consume. Starts only after the
	// aggregator has finished reading — no concurrent consumers.
	go func() {
		wg.Wait()
		close(results)
		for r := range results {
			if r.blob != nil {
				r.blob.Release()
			}
		}
	}()

	if hits < data {
		releaseAll(blobs)
		if sample {
			logECGetTiming(key, tStart, tLocateEnd, tDataReady, tDataReady, hits, misses, false,
				shardArrivals[:shardArrivalsN])
		}
		return cache.CacheMiss, nil, nil
	}

	// Count how many of the `data` (indices 0..data-1) shards we have
	// in hand. If all data is present, Reconstruct is unnecessary — we
	// can skip RS entirely and hand the shard buffers straight to the
	// segmented blob. 1-in-5 case for 4+1 hedging (the miss was the
	// parity shard).
	dataHits := 0
	for i := 0; i < data; i++ {
		if shards[i] != nil {
			dataHits++
		}
	}

	// Pre-fill nil shard slots with pooled scratch so reedsolomon's
	// Reconstruct reuses our buffers instead of allocating via its
	// internal AllocAligned. Check: the library tests
	// `cap(shards[i]) >= shardSize` and reuses the slot if so.
	//
	// Only needed when reconstruction will run (dataHits < data).
	var scratchSlots []int
	if dataHits < data {
		var shardSize int
		for _, s := range shards {
			if s != nil {
				shardSize = len(s)
				break
			}
		}
		for i := 0; i < total; i++ {
			if shards[i] != nil {
				continue
			}
			buf := t.shardScratchPool.Get().([]byte)
			if cap(buf) < shardSize {
				// Undersized: return and let library allocate. Rare
				// (seed covers up to 1 MiB values).
				t.shardScratchPool.Put(buf)
				continue
			}
			shards[i] = buf[:0]
			scratchSlots = append(scratchSlots, i)
		}

		// Reconstruct only the data shards we need; parity slots can
		// remain untouched. Pairs with the "skip if dataHits==data"
		// gate above: RS runs only when it has actual work to do, and
		// the work is bounded to data-shard recovery.
		if err := t.enc.enc.ReconstructData(shards); err != nil {
			// Return scratch and blobs cleanly.
			for _, idx := range scratchSlots {
				t.shardScratchPool.Put(shards[idx][:0])
			}
			releaseAll(blobs)
			return cache.CacheMiss, nil, fmt.Errorf("ec: reconstruct: %w", err)
		}
	}
	if sample {
		tDecodeEnd = time.Now()
		logECGetTiming(key, tStart, tLocateEnd, tDataReady, tDecodeEnd, hits, misses, true,
			shardArrivals[:shardArrivalsN])
	}

	// Original value length is the first 4 bytes of shards[0]. Must be
	// decoded before any trimming — any malformed length aborts.
	if len(shards[0]) < 4 {
		for _, idx := range scratchSlots {
			t.shardScratchPool.Put(shards[idx][:0])
		}
		releaseAll(blobs)
		return cache.CacheMiss, nil, fmt.Errorf("ec: shard 0 too short for length prefix")
	}
	origLen := int(binary.LittleEndian.Uint32(shards[0][:4]))
	if origLen < 0 || origLen > len(shards[0])*data-4 {
		for _, idx := range scratchSlots {
			t.shardScratchPool.Put(shards[idx][:0])
		}
		releaseAll(blobs)
		return cache.CacheMiss, nil, fmt.Errorf("ec: invalid length prefix %d", origLen)
	}

	// Repair: fire-and-forget writes to peers that confirmed-miss,
	// carrying the reconstructed shard bytes for the indices no
	// survivor returned. WrapShard allocates a fresh buffer per repair
	// so the async goroutines don't depend on the scratch pool's
	// lifetime — scratch can be returned to the pool on Get's release
	// path without coordination.
	t.scheduleRepair(shards, seenIdx, missPeerPos, p, key)

	// Build the segmented blob's release closure. It:
	//   - Releases each wire-served blob (pool-backed, from peer wire
	//     ReadResponse). blobs[shardIdx] is non-nil for hit slots.
	//   - Returns every scratch buffer to shardScratchPool. Repair, if
	//     scheduled, owns its own WrapShard'd buffer, so the scratch
	//     is safe to recycle as soon as the synchronous path finishes.
	ownedAllShards := make([][]byte, total)
	copy(ownedAllShards, shards)
	ownedBlobs := make([]cache.Blob, total)
	copy(ownedBlobs, blobs)
	ownedScratchSlots := scratchSlots
	dataOnly := ownedAllShards[:data]
	release := func() {
		for i, b := range ownedBlobs {
			if b != nil {
				b.Release()
				ownedBlobs[i] = nil
			}
		}
		for _, idx := range ownedScratchSlots {
			t.shardScratchPool.Put(ownedAllShards[idx][:0])
		}
	}
	return cache.CacheHit, newSegmentedBlob(dataOnly, origLen, release), nil
}

// scheduleRepair wraps each missing-idx shard with [idx][total]
// prefix (single alloc per shard) and fires one async FillShard per
// (missPeer, missingIdx) pair.
func (t *impl) scheduleRepair(shards [][]byte, seenIdx []bool, missPeerPos []int, p store.Partition, key store.ContentKey) {
	runner := t.repairRunner
	if runner == nil || len(missPeerPos) == 0 {
		return
	}
	total := t.enc.TotalShards()
	parity := t.enc.parity

	// missingIdx = {0..total-1} \ seenIdx
	missingIdx := make([]int, 0, parity)
	for i := 0; i < total; i++ {
		if !seenIdx[i] {
			missingIdx = append(missingIdx, i)
		}
	}
	if len(missingIdx) == 0 {
		return
	}

	// Pair up: one peer ↔ one missing idx. If counts mismatch (peers
	// missed for both confirmed-miss and transport-error reasons), we
	// only cover the peers we know about.
	n := len(missingIdx)
	if len(missPeerPos) < n {
		n = len(missPeerPos)
	}

	// Pre-wrap each repair payload synchronously so the async
	// goroutines don't race each other on shards[idx] reads vs the
	// Get path's release.
	type repairJob struct {
		peerPos int
		value   []byte
	}
	jobs := make([]repairJob, n)
	for i := 0; i < n; i++ {
		idx := missingIdx[i]
		jobs[i] = repairJob{
			peerPos: missPeerPos[i],
			value:   WrapShard(shards[idx], idx, total),
		}
	}

	// Snapshot router/peer state for the goroutine.
	peerIDs, err := t.router.LocateN(key, total)
	if err != nil {
		return
	}

	runner(func() {
		for _, j := range jobs {
			endpoint := t.router.Endpoint(peerIDs[j.peerPos])
			sc, err := t.pool.Get(endpoint)
			if err != nil {
				continue
			}
			ctx, cancel := t.fillCtxFrom(t.baseCtx)
			_ = sc.FillShard(ctx, p, key, j.value)
			cancel()
		}
	})
}

func releaseAll(blobs []cache.Blob) {
	for _, b := range blobs {
		if b != nil {
			b.Release()
		}
	}
}

// Fill implements cache.Filler synchronously. Encodes the value into
// RS shards, distributes them to shard nodes in parallel, and waits
// for all shards before returning.
//
// The per-shard FillShard payload carries the [idx][total] prefix the
// peer will persist in its value; EncodePrefixed writes directly into
// caller-allocated prefix-padded buffers, so there is no secondary
// copy between the RS encoder's output and the wire.
//
// Short-circuit on first failure: the first FillShard error cancels a
// shared child context, so any in-flight sibling FillShards abort their
// wire ops immediately. Synchronous semantics match rocks.Fill — when
// Fill returns nil, all `total` shards have been acknowledged by their
// peers.
func (t *impl) Fill(ctx context.Context, p store.Partition, key store.ContentKey, data []byte) error {
	total := t.enc.TotalShards()

	// Allocate one prefix-padded buffer per shard; EncodePrefixed
	// writes directly into bufs[i][cache.ShardPrefixSize:]. Single
	// alloc per shard (no separate Split-buffer + prefix-buffer copy).
	bufSize := ShardBufferSize(len(data), t.enc.data)
	bufs := make([][]byte, total)
	for i := 0; i < total; i++ {
		bufs[i] = make([]byte, bufSize)
	}
	if err := t.enc.EncodePrefixed(data, bufs); err != nil {
		return err
	}

	peerIDs, err := t.router.LocateN(key, total)
	if err != nil {
		return err
	}

	fillCtx, cancel := t.fillCtxFrom(ctx)
	defer cancel()

	var (
		errMu    sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)

	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(pos int) {
			defer wg.Done()
			endpoint := t.router.Endpoint(peerIDs[pos])
			sc, err := t.pool.Get(endpoint)
			if err == nil {
				err = sc.FillShard(fillCtx, p, key, bufs[pos])
				// One fast retry on transient transport failure. Skip
				// when the error signals cancel-cascade (a sibling has
				// already failed and triggered cancel()) — retrying is
				// pointless once the result is unusable anyway.
				if err != nil && !errors.Is(err, context.Canceled) && fillCtx.Err() == nil {
					select {
					case <-time.After(100 * time.Millisecond):
					case <-fillCtx.Done():
					}
					if fillCtx.Err() == nil {
						err = sc.FillShard(fillCtx, p, key, bufs[pos])
					}
				}
			}
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				cancel()
			}
		}(i)
	}
	wg.Wait()

	if firstErr != nil {
		return fmt.Errorf("ec: fill: %w", firstErr)
	}
	return nil
}

// fillCtxFrom derives a fill context from parent, applying the
// fillTimeout deadline only when > 0 (0 = no deadline; bounded solely
// by parent / Close).
func (t *impl) fillCtxFrom(parent context.Context) (context.Context, context.CancelFunc) {
	if t.fillTimeout > 0 {
		return context.WithTimeout(parent, t.fillTimeout)
	}
	return context.WithCancel(parent)
}

// Close cancels the detached repair backfill and closes the peer pool.
func (t *impl) Close() error {
	if t.baseCancel != nil {
		t.baseCancel()
	}
	return t.pool.Close()
}

// Epoch returns the current membership epoch. See Interface.Epoch.
func (t *impl) Epoch() int64 {
	return t.router.Epoch()
}
