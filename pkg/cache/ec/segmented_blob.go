package ec

import (
	"sync"
	"sync/atomic"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/cache"
)

// segmentedBlob is the cache.Blob returned by EC.Get when the data
// shards can be delivered without a contiguous-buffer join. It holds:
//
//   - `shards`: borrowed references to the `data` underlying shard
//     buffers (each 131 KiB-ish). `Segments()` returns views of these
//     trimmed to the original value's byte range.
//   - `release`: fired on the last Release() to return the shard
//     buffers to their respective pools (wire read-pool for peer-
//     served shards; the EC tier's shardScratchPool for reconstructed
//     shards).
//   - `trimFirst`/`trimLastLen`: byte offsets applied to shards[0] and
//     the last populated data shard to strip the 4-byte length prefix
//     and any padding.
//
// The point of segmentedBlob is to skip the ~500 KiB join memcpy that
// ec.encoder.DecodeInto performs to produce a single value slice.
// wire.WriteResponse detects cache.Segmenter and emits the shards
// directly via net.Buffers (writev), so the response body reaches the
// kernel as four concatenated iovec entries without ever being joined
// in Go userspace.
//
// Bytes() falls back to a lazy join for callers that need contiguous
// access (e.g., tests). The hot wire-response path never calls Bytes.
type segmentedBlob struct {
	shards      [][]byte
	trimFirst   int // bytes to skip from shards[0] (length prefix = 4)
	trimLastLen int // effective length of the final non-empty segment

	// release is run on the final Release()/clone balance. It hands
	// the underlying shard buffers back to their pools / pool-backed
	// blobs. Set to a no-op if ownership was not handed in.
	release func()

	refs *atomic.Int32

	// joinedCache memoises Bytes()'s lazy join so repeated calls do
	// not redo the memcpy. nil until first Bytes() call.
	joinedOnce sync.Once
	joined     []byte
}

// newSegmentedBlob wraps the given shards (indices 0..data-1) as a
// Blob. trimFirst is subtracted from the first segment (length prefix)
// and the total payload length is origLen. release is invoked when
// the final reference is dropped.
//
// shards must have at least `data` non-nil entries holding the
// reconstructed data shard bytes. All segments have equal length
// (shardSize) except that shards[0] is effectively skipped by
// trimFirst and the last populated segment is truncated to match
// origLen.
func newSegmentedBlob(dataShards [][]byte, origLen int, release func()) cache.Blob {
	refs := new(atomic.Int32)
	refs.Store(1)
	b := &segmentedBlob{
		shards:    dataShards,
		trimFirst: 4,
		release:   release,
		refs:      refs,
	}
	// Figure out how much of the last segment is payload. Total raw =
	// sum(len(s)) - trimFirst; origLen <= raw. Trim from the last
	// segment.
	raw := 0
	for _, s := range dataShards {
		raw += len(s)
	}
	raw -= b.trimFirst
	// tailTrim: bytes to cut from last segment.
	tailTrim := raw - origLen
	if n := len(dataShards); n > 0 {
		b.trimLastLen = len(dataShards[n-1]) - tailTrim
		if b.trimLastLen < 0 {
			b.trimLastLen = 0
		}
	}
	return b
}

func (b *segmentedBlob) Segments() [][]byte {
	if len(b.shards) == 0 {
		return nil
	}
	// Build the segment list fresh each call. Cheap: O(data) slice
	// header ops, no data copy.
	n := len(b.shards)
	out := make([][]byte, 0, n)
	if len(b.shards[0]) > b.trimFirst {
		if n == 1 {
			// Single-segment path (unlikely with data>=2): trimFirst + trimLastLen combined.
			end := b.trimFirst + b.trimLastLen
			if end > len(b.shards[0]) {
				end = len(b.shards[0])
			}
			out = append(out, b.shards[0][b.trimFirst:end])
			return out
		}
		out = append(out, b.shards[0][b.trimFirst:])
	}
	for i := 1; i < n-1; i++ {
		if len(b.shards[i]) > 0 {
			out = append(out, b.shards[i])
		}
	}
	last := b.shards[n-1]
	if b.trimLastLen > 0 && b.trimLastLen <= len(last) {
		out = append(out, last[:b.trimLastLen])
	} else if b.trimLastLen == len(last) {
		out = append(out, last)
	}
	return out
}

func (b *segmentedBlob) Bytes() []byte {
	b.joinedOnce.Do(func() {
		segs := b.Segments()
		total := 0
		for _, s := range segs {
			total += len(s)
		}
		buf := make([]byte, 0, total)
		for _, s := range segs {
			buf = append(buf, s...)
		}
		b.joined = buf
	})
	return b.joined
}

func (b *segmentedBlob) Clone() cache.Blob {
	b.refs.Add(1)
	return &segmentedBlob{
		shards:      b.shards,
		trimFirst:   b.trimFirst,
		trimLastLen: b.trimLastLen,
		release:     b.release,
		refs:        b.refs,
		// joinedOnce/joined are per-handle; the clone does its own
		// lazy join if needed (rare; segments are idempotent).
	}
}

func (b *segmentedBlob) Release() {
	if b.shards == nil {
		return
	}
	b.shards = nil
	if b.refs.Add(-1) == 0 && b.release != nil {
		b.release()
	}
}

var (
	_ cache.Blob      = (*segmentedBlob)(nil)
	_ cache.Segmenter = (*segmentedBlob)(nil)
)
