package ec

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type repairTestShard struct {
	mu     sync.Mutex
	result cache.CacheResult
	value  []byte
	err    error
	fills  [][]byte
}

func (s *repairTestShard) GetShard(context.Context, store.Partition, store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.result != cache.CacheHit {
		return s.result, nil, s.err
	}
	return cache.CacheHit, cache.NewMemBlob(append([]byte(nil), s.value...)), s.err
}

func (s *repairTestShard) FillShard(_ context.Context, _ store.Partition, _ store.ContentKey, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copyValue := append([]byte(nil), value...)
	s.fills = append(s.fills, copyValue)
	s.value = copyValue
	s.result = cache.CacheHit
	s.err = nil
	return nil
}

func (s *repairTestShard) Close() error { return nil }

func (s *repairTestShard) setFailure(err error) {
	s.mu.Lock()
	s.result = cache.CacheMiss
	s.value = nil
	s.err = err
	s.mu.Unlock()
}

func (s *repairTestShard) lastFill() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.fills) == 0 {
		return nil
	}
	return append([]byte(nil), s.fills[len(s.fills)-1]...)
}

func TestGetReconstructsParityBeforeRepair(t *testing.T) {
	peers := []Peer{
		{ID: "p0", Endpoint: "p0"},
		{ID: "p1", Endpoint: "p1"},
		{ID: "p2", Endpoint: "p2"},
		{ID: "p3", Endpoint: "p3"},
		{ID: "p4", Endpoint: "p4"},
	}
	tierHandle, err := New(Config{Cluster: ClusterConfig{
		DataShards: 4, ParityShards: 1, Peers: peers,
	}})
	if err != nil {
		t.Fatal(err)
	}
	tier := tierHandle.(*impl)
	defer tier.Close()

	data := bytes.Repeat([]byte("parity-repair"), 1024)
	shards := make([][]byte, 5)
	for i := range shards {
		shards[i] = make([]byte, ShardBufferSize(len(data), 4))
	}
	if err := tier.enc.EncodePrefixed(data, shards); err != nil {
		t.Fatal(err)
	}
	key := store.ContentKey{1, 2, 3}
	peerIDs, err := tier.router.LocateN(key, len(peers))
	if err != nil {
		t.Fatal(err)
	}

	byPosition := make([]*repairTestShard, len(peers))
	for pos, peerID := range peerIDs {
		shard := &repairTestShard{}
		if pos == 0 {
			shard.result = cache.CacheMiss
		} else {
			shard.result = cache.CacheHit
			shard.value = append([]byte(nil), shards[pos-1]...)
		}
		byPosition[pos] = shard
		tier.pool.clients[tier.router.Endpoint(peerID)] = shard
	}
	tier.EnableRepair(func(fn func()) { fn() })

	result, blob, err := tier.Get(context.Background(), store.PartitionManifest, key)
	if err != nil || result != cache.CacheHit || blob == nil {
		t.Fatalf("first Get result=%v blob=%v err=%v", result, blob, err)
	}
	if !bytes.Equal(blob.Bytes(), data) {
		t.Fatal("first Get returned corrupt data")
	}
	blob.Release()

	repaired := byPosition[0].lastFill()
	idx, total, payload, ok := cache.ParseShardPrefix(repaired)
	if !ok || idx != 4 || total != 5 {
		t.Fatalf("repaired parity prefix idx=%d total=%d ok=%v", idx, total, ok)
	}
	if len(payload) == 0 || !bytes.Equal(repaired, shards[4]) {
		t.Fatalf("repaired parity size=%d, want %d", len(repaired), len(shards[4]))
	}

	byPosition[1].setFailure(errors.New("peer unavailable"))
	result, blob, err = tier.Get(context.Background(), store.PartitionManifest, key)
	if err != nil || result != cache.CacheHit || blob == nil {
		t.Fatalf("4-of-5 Get result=%v blob=%v err=%v", result, blob, err)
	}
	defer blob.Release()
	if !bytes.Equal(blob.Bytes(), data) {
		t.Fatal("4-of-5 Get returned corrupt data")
	}
}
