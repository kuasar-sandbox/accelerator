// Package ec implements a Reed-Solomon 4/5 erasure-coded cache tier.
package ec

import (
	"encoding/binary"
	"fmt"

	"github.com/klauspost/reedsolomon"
	"github.com/kuasar-sandbox/accelerator/pkg/cache"
)

// encoder wraps a Reed-Solomon encoder with data/parity shard counts.
type encoder struct {
	enc    reedsolomon.Encoder
	data   int
	parity int
}

// ShardDataSize returns the per-shard body size (bytes of RS-encoded
// data, not including the [idx][total] prefix) for the given value
// length and data-shard count. The returned size is the one the RS
// encoder will produce after padding `4 + valueLen` to a multiple of
// data-shard count.
func ShardDataSize(valueLen, data int) int {
	prefixed := 4 + valueLen
	return (prefixed + data - 1) / data
}

// ShardBufferSize returns the total size of each buffer to pass to
// EncodePrefixed: ShardPrefixSize + ShardDataSize.
func ShardBufferSize(valueLen, data int) int {
	return cache.ShardPrefixSize + ShardDataSize(valueLen, data)
}

// newEncoder creates an RS encoder with the given data and parity shard counts.
func newEncoder(data, parity int) (*encoder, error) {
	enc, err := reedsolomon.New(data, parity)
	if err != nil {
		return nil, fmt.Errorf("ec: create encoder: %w", err)
	}
	return &encoder{enc: enc, data: data, parity: parity}, nil
}

// TotalShards returns data + parity.
func (e *encoder) TotalShards() int { return e.data + e.parity }

// Encode splits the value into data+parity shards.
// A 4-byte little-endian length prefix is prepended before splitting
// so that Decode can trim padding.
//
// The returned shards are raw RS bytes — not prefixed with [idx][total].
// Callers that need wire-ready buffers should use EncodePrefixed.
func (e *encoder) Encode(value []byte) ([][]byte, error) {
	// Prepend 4-byte length prefix.
	prefixed := make([]byte, 4+len(value))
	binary.LittleEndian.PutUint32(prefixed[:4], uint32(len(value)))
	copy(prefixed[4:], value)

	// Pad to multiple of data shards.
	remainder := len(prefixed) % e.data
	if remainder != 0 {
		prefixed = append(prefixed, make([]byte, e.data-remainder)...)
	}

	shards, err := e.enc.Split(prefixed)
	if err != nil {
		return nil, fmt.Errorf("ec: split: %w", err)
	}
	if err := e.enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("ec: encode: %w", err)
	}
	return shards, nil
}

// EncodePrefixed computes data+parity shards directly into the
// caller-provided prefix-padded buffers. On return:
//
//	bufs[i][0]                    = byte(i)           -- shard idx
//	bufs[i][1]                    = byte(data+parity) -- total shard count
//	bufs[i][ShardPrefixSize:]     = RS-encoded shard bytes (size == ShardDataSize)
//
// Each bufs[i] must have length exactly ShardBufferSize(len(value), e.data).
//
// Allocates one temporary `prefixed` buffer (same as the plain Encode
// path) and copies one stripe per data shard into bufs[i][2:]; the
// parity slots are computed in place by reedsolomon.Encode operating
// on shard-body sub-slices of bufs[i]. The striping logic handles the
// general case where shardSize can be smaller than the 4-byte length
// prefix (very small values) — the length bytes straddle bufs[0] and
// bufs[1] naturally because they're just the first 4 bytes of the
// logical `prefixed` layout.
func (e *encoder) EncodePrefixed(value []byte, bufs [][]byte) error {
	total := e.data + e.parity
	if len(bufs) != total {
		return fmt.Errorf("ec: expected %d bufs, got %d", total, len(bufs))
	}
	shardSize := ShardDataSize(len(value), e.data)
	expectedLen := cache.ShardPrefixSize + shardSize
	for i := 0; i < total; i++ {
		if len(bufs[i]) != expectedLen {
			return fmt.Errorf("ec: bufs[%d] size %d, expected %d", i, len(bufs[i]), expectedLen)
		}
		cache.EncodeShardPrefix(bufs[i], byte(i), byte(total))
	}

	// Build the conceptual "prefixed" buffer: [4-byte len][value][zero pad
	// to shardSize*data]. make() zero-initialises the tail, so we only
	// need to write the header and value bytes.
	prefixed := make([]byte, shardSize*e.data)
	binary.LittleEndian.PutUint32(prefixed[:4], uint32(len(value)))
	copy(prefixed[4:4+len(value)], value)

	// Stripe the prefixed buffer into bufs[0..data-1][ShardPrefixSize:]
	// in shardSize-aligned chunks.
	for i := 0; i < e.data; i++ {
		copy(bufs[i][cache.ShardPrefixSize:], prefixed[i*shardSize:(i+1)*shardSize])
	}

	// Parity bytes are overwritten by reedsolomon.Encode; no need to
	// pre-zero them.

	shards := make([][]byte, total)
	for i := 0; i < total; i++ {
		shards[i] = bufs[i][cache.ShardPrefixSize:]
	}
	return e.enc.Encode(shards)
}

// WrapShard returns a newly-allocated [idx][total][shard] buffer
// suitable as a FillShard value. Used on the repair path where the
// reconstructed shard bytes don't live in a prefix-padded buffer.
// Allocates once; acceptable because repair is async and rate-limited.
func WrapShard(shard []byte, idx, total int) []byte {
	buf := make([]byte, cache.ShardPrefixSize+len(shard))
	cache.EncodeShardPrefix(buf, byte(idx), byte(total))
	copy(buf[cache.ShardPrefixSize:], shard)
	return buf
}

// Decode reconstructs the original value from shards.
// Missing shards should be nil. At least `data` shards must be present.
//
// Note: EC.Get does NOT call Decode on the hot path. It uses
// ec.segmentedBlob to deliver shards directly to the wire response via
// writev, avoiding the joined-buffer memcpy. Decode remains available
// for callers that need a contiguous []byte (tests, benchmarks, any
// non-wire consumer).
func (e *encoder) Decode(shards [][]byte) ([]byte, error) {
	if err := e.enc.Reconstruct(shards); err != nil {
		return nil, fmt.Errorf("ec: reconstruct: %w", err)
	}

	shardSize := len(shards[0])
	joined := make([]byte, 0, shardSize*e.data)
	for i := 0; i < e.data; i++ {
		joined = append(joined, shards[i]...)
	}

	if len(joined) < 4 {
		return nil, fmt.Errorf("ec: joined data too short (%d bytes)", len(joined))
	}
	origLen := binary.LittleEndian.Uint32(joined[:4])
	if int(origLen) > len(joined)-4 {
		return nil, fmt.Errorf("ec: invalid length prefix %d (max %d)", origLen, len(joined)-4)
	}
	return joined[4 : 4+origLen], nil
}
