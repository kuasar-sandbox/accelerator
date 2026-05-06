package cache

// Shard value format for EC-coded shards stored on peer nodes.
//
// A shard is persisted (and transferred on the wire) as:
//
//	[1 byte shard_idx][1 byte total][N bytes shard_data]
//
// Putting idx and total into the value — rather than into the rocksdb
// key or the wire header — decouples shard identity from cluster
// membership. A peer can answer "what shard do you have for key X?"
// without the client needing to know beforehand which idx this peer
// holds; the peer's stored value self-describes its own (idx, total).
//
// This is load-bearing for non-disruptive membership changes: when a
// peer joins/leaves, surviving peers keep serving their existing shards
// by their self-described idx, and the EC decoder accepts any K of N
// distinct idx it receives.
//
// total lets the decoder reject stale shards written under a different
// RS (K+P) scheme, or shards that were accidentally served from a
// different cluster — treat "total != expected_total" as a miss.
const (
	// ShardPrefixSize is the byte length of the [idx][total] header
	// that prefixes every on-disk / on-wire shard payload.
	ShardPrefixSize = 2
)

// EncodeShardPrefix writes the (idx, total) header bytes into the first
// ShardPrefixSize bytes of buf. The caller is responsible for sizing
// buf to ShardPrefixSize + shardDataSize and for filling buf[ShardPrefixSize:]
// with the RS shard bytes before handing to Fill/network.
func EncodeShardPrefix(buf []byte, idx, total uint8) {
	_ = buf[1] // bounds check elision
	buf[0] = idx
	buf[1] = total
}

// ParseShardPrefix extracts idx and total from the head of a stored /
// received shard value. Returns ok=false if the buffer is too short
// to contain the prefix.
//
// The returned data slice aliases buf; callers must not release the
// underlying buffer while data is in use.
func ParseShardPrefix(buf []byte) (idx, total uint8, data []byte, ok bool) {
	if len(buf) < ShardPrefixSize {
		return 0, 0, nil, false
	}
	return buf[0], buf[1], buf[ShardPrefixSize:], true
}
