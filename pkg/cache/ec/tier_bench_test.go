package ec

import (
	"bytes"
	"context"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/client"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// mockShard implements client.ShardCloser synchronously from a single
// in-memory shard value (already prefix-padded: [idx][total][data]).
// Under the post-membership-change design each peer holds exactly one
// shard per key; the returned blob carries the peer-owned idx in its
// first byte. FillShard and Close are no-ops. This strips out TCP /
// bufio / sync.Pool so the benchmark measures the EC layer's
// goroutine+channel+decode overhead in isolation.
type mockShard struct {
	shardValue []byte // full [idx][total][data] buffer, or nil for miss
}

func (m *mockShard) GetShard(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	if m.shardValue == nil {
		return cache.CacheMiss, nil, nil
	}
	// Copy so the caller can treat the blob as owned.
	buf := make([]byte, len(m.shardValue))
	copy(buf, m.shardValue)
	return cache.CacheHit, cache.NewMemBlob(buf), nil
}

func (m *mockShard) FillShard(ctx context.Context, p store.Partition, key store.ContentKey, value []byte) error {
	return nil
}

func (m *mockShard) Close() error { return nil }

// newMockTier builds an EC tier whose peerPool is pre-populated with
// the provided mockShards — no TCP, no dialing.
func newMockTier(b *testing.B, value []byte, nPeers, data, parity int) (*impl, store.ContentKey) {
	b.Helper()
	enc, err := newEncoder(data, parity)
	if err != nil {
		b.Fatal(err)
	}
	total := data + parity
	bufs := make([][]byte, total)
	bufSize := ShardBufferSize(len(value), data)
	for i := 0; i < total; i++ {
		bufs[i] = make([]byte, bufSize)
	}
	if err := enc.EncodePrefixed(value, bufs); err != nil {
		b.Fatal(err)
	}

	peers := make([]Peer, nPeers)
	for i := range peers {
		peers[i] = Peer{ID: string(rune('A' + i)), Endpoint: "mock-" + string(rune('A'+i))}
	}
	rt, err := newRouter(peers)
	if err != nil {
		b.Fatal(err)
	}

	t := &impl{
		enc:    enc,
		router: rt,
		pool:   newPeerPool(2, 0, cache.DefaultPool),
	}
	t.shardScratchPool.New = func() any { return make([]byte, 0, 256<<10) }

	// Each peer stores one shard value (full [idx][total][data]); we
	// pre-populate from Fill's assignment order (peer at LocateN
	// position i gets bufs[i]).
	var key store.ContentKey
	copy(key[:], bytes.Repeat([]byte{0xAB}, 32))
	peerIDs, err := rt.LocateN(key, nPeers)
	if err != nil {
		b.Fatal(err)
	}
	for pos, id := range peerIDs {
		ep := rt.Endpoint(id)
		t.pool.clients[ep] = &mockShardCloser{m: &mockShard{shardValue: bufs[pos]}}
	}
	return t, key
}

// mockShardCloser adapts mockShard to client.ShardCloser.
type mockShardCloser struct {
	m *mockShard
}

func (c *mockShardCloser) GetShard(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	return c.m.GetShard(ctx, p, key)
}
func (c *mockShardCloser) FillShard(ctx context.Context, p store.Partition, key store.ContentKey, value []byte) error {
	return c.m.FillShard(ctx, p, key, value)
}
func (c *mockShardCloser) Close() error { return c.m.Close() }

var _ client.ShardCloser = (*mockShardCloser)(nil)

// BenchmarkECGet_AllHit_InProcess measures the EC tier's per-Get
// overhead at 512KB with mock in-process peers. This isolates the
// cost attributable to EC itself (fan-out goroutines, channel ops,
// aggregator, Decode + joined buffer) from TCP / wire codec / kernel.
func BenchmarkECGet_AllHit_InProcess(b *testing.B) {
	value := bytes.Repeat([]byte("X"), 512*1024)
	t, key := newMockTier(b, value, 5, 4, 1)
	ctx := context.Background()

	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, blob, err := t.Get(ctx, store.PartitionChunk, key)
		if err != nil || blob == nil {
			b.Fatalf("Get: blob=%v err=%v", blob, err)
		}
		blob.Release()
	}
}
