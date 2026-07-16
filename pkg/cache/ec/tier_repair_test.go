package ec

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

type lateMissRepairShard struct {
	*repairTestShard
	calls     atomic.Int32
	cancelled chan struct{}
	once      sync.Once
}

func (s *lateMissRepairShard) GetShard(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	if s.calls.Add(1) == 1 {
		<-ctx.Done()
		s.once.Do(func() { close(s.cancelled) })
		return cache.CacheMiss, nil, ctx.Err()
	}
	return s.repairTestShard.GetShard(ctx, p, key)
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

func TestGetRepairsConfirmedMissHiddenByEarlyReturn(t *testing.T) {
	peers := []Peer{
		{ID: "p0", Endpoint: "p0"},
		{ID: "p1", Endpoint: "p1"},
		{ID: "p2", Endpoint: "p2"},
		{ID: "p3", Endpoint: "p3"},
		{ID: "p4", Endpoint: "p4"},
	}
	tierHandle, err := New(Config{Cluster: ClusterConfig{
		DataShards: 4, ParityShards: 1, Peers: peers, Timeout: "1s",
	}})
	if err != nil {
		t.Fatal(err)
	}
	tier := tierHandle.(*impl)
	defer tier.Close()
	var scratchGets atomic.Int32
	tier.shardScratchPool.New = func() any {
		scratchGets.Add(1)
		return make([]byte, 0, 256<<10)
	}

	data := bytes.Repeat([]byte("late-confirmed-miss"), 1024)
	shards := make([][]byte, len(peers))
	for i := range shards {
		shards[i] = make([]byte, ShardBufferSize(len(data), 4))
	}
	if err := tier.enc.EncodePrefixed(data, shards); err != nil {
		t.Fatal(err)
	}
	key := store.ContentKey{9, 8, 7}
	peerIDs, err := tier.router.LocateN(key, len(peers))
	if err != nil {
		t.Fatal(err)
	}

	byPosition := make([]*repairTestShard, len(peers))
	late := &lateMissRepairShard{
		repairTestShard: &repairTestShard{result: cache.CacheMiss},
		cancelled:       make(chan struct{}),
	}
	byPosition[0] = late.repairTestShard
	tier.pool.clients[tier.router.Endpoint(peerIDs[0])] = late
	for pos := 1; pos < len(peers); pos++ {
		shard := &repairTestShard{
			result: cache.CacheHit,
			value:  append([]byte(nil), shards[pos-1]...),
		}
		byPosition[pos] = shard
		tier.pool.clients[tier.router.Endpoint(peerIDs[pos])] = shard
	}

	repairFn := make(chan func(), 1)
	tier.EnableRepair(func(fn func()) {
		repairFn <- fn
	})

	result, blob, err := tier.Get(context.Background(), store.PartitionManifest, key)
	if err != nil || result != cache.CacheHit || blob == nil {
		t.Fatalf("Get result=%v blob=%v err=%v", result, blob, err)
	}
	if !bytes.Equal(blob.Bytes(), data) {
		t.Fatal("Get returned corrupt data")
	}
	blob.Release()
	if got := scratchGets.Load(); got != 0 {
		t.Fatalf("foreground Get reconstructed unconfirmed parity: scratch gets=%d", got)
	}
	if fill := late.lastFill(); fill != nil {
		t.Fatalf("repair ran before the deferred probe: %x", fill)
	}

	select {
	case <-late.cancelled:
	case <-time.After(time.Second):
		t.Fatal("foreground late peer did not observe cancellation")
	}
	repairDone := make(chan struct{})
	select {
	case fn := <-repairFn:
		go func() {
			fn()
			close(repairDone)
		}()
	case <-time.After(time.Second):
		t.Fatal("background repair probe was not scheduled")
	}
	select {
	case <-repairDone:
	case <-time.After(2 * time.Second):
		t.Fatal("background repair probe did not finish")
	}
	repaired := late.lastFill()
	if !bytes.Equal(repaired, shards[4]) {
		t.Fatalf("repaired parity size=%d, want %d", len(repaired), len(shards[4]))
	}

	// The repaired peer must now supply the fourth shard when another peer is
	// unavailable. Disable further repair so this assertion covers only the
	// first probe's result.
	tier.EnableRepair(nil)
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

func TestRepairProbeReservationIsBounded(t *testing.T) {
	tierHandle, err := New(Config{Cluster: ClusterConfig{
		DataShards:   1,
		ParityShards: 1,
		Peers:        []Peer{{ID: "p0", Endpoint: "p0"}, {ID: "p1", Endpoint: "p1"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tier := tierHandle.(*impl)
	defer tier.Close()
	if !tier.tryReserveRepairProbe() {
		t.Fatal("first repair probe reservation was rejected")
	}
	if tier.tryReserveRepairProbe() {
		t.Fatal("second concurrent repair probe reservation was accepted")
	}
	tier.releaseRepairProbe()
	if !tier.tryReserveRepairProbe() {
		t.Fatal("repair probe slot was not reusable")
	}
	tier.releaseRepairProbe()
}

func TestRepairProbeDoesNotWriteOnTransportError(t *testing.T) {
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

	key := store.ContentKey{4, 5, 6}
	peerIDs, err := tier.router.LocateN(key, len(peers))
	if err != nil {
		t.Fatal(err)
	}
	failed := &repairTestShard{result: cache.CacheMiss, err: errors.New("connection reset")}
	tier.pool.clients[tier.router.Endpoint(peerIDs[0])] = failed

	shards := make([][]byte, len(peers))
	for i := range shards {
		shards[i] = []byte{byte(i + 1)}
	}
	blobs := make([]cache.Blob, len(peers))
	seen := []bool{true, true, true, true, false}
	tier.EnableRepair(func(fn func()) { fn() })
	if !tier.tryReserveRepairProbe() {
		t.Fatal("repair probe reservation was rejected")
	}
	routedPeers, err := tier.router.LocatePeers(key, len(peers))
	if err != nil {
		t.Fatal(err)
	}
	if !tier.scheduleProbeRepair(shards, blobs, seen, nil, []int{0}, routedPeers, store.PartitionManifest, key) {
		t.Fatal("repair probe was not scheduled")
	}
	if fill := failed.lastFill(); fill != nil {
		t.Fatalf("transport error triggered a repair write: %x", fill)
	}
	if !tier.tryReserveRepairProbe() {
		t.Fatal("transport-error probe did not release its reservation")
	}
	tier.releaseRepairProbe()
}

func TestRepairProbeCombinesConfirmedAndLateMisses(t *testing.T) {
	peers := []Peer{
		{ID: "p0", Endpoint: "p0"},
		{ID: "p1", Endpoint: "p1"},
		{ID: "p2", Endpoint: "p2"},
		{ID: "p3", Endpoint: "p3"},
		{ID: "p4", Endpoint: "p4"},
		{ID: "p5", Endpoint: "p5"},
	}
	tierHandle, err := New(Config{Cluster: ClusterConfig{
		DataShards: 4, ParityShards: 2, Peers: peers,
	}})
	if err != nil {
		t.Fatal(err)
	}
	tier := tierHandle.(*impl)
	defer tier.Close()

	key := store.ContentKey{7, 7, 7}
	peerIDs, err := tier.router.LocateN(key, len(peers))
	if err != nil {
		t.Fatal(err)
	}
	confirmed := &repairTestShard{result: cache.CacheMiss}
	late := &repairTestShard{result: cache.CacheMiss}
	tier.pool.clients[tier.router.Endpoint(peerIDs[0])] = confirmed
	tier.pool.clients[tier.router.Endpoint(peerIDs[1])] = late

	shards := make([][]byte, len(peers))
	for i := range shards {
		shards[i] = []byte{byte(i + 1)}
	}
	blobs := make([]cache.Blob, len(peers))
	seen := []bool{true, true, true, true, false, false}
	tier.EnableRepair(func(fn func()) { fn() })
	if !tier.tryReserveRepairProbe() {
		t.Fatal("repair probe reservation was rejected")
	}
	routedPeers, err := tier.router.LocatePeers(key, len(peers))
	if err != nil {
		t.Fatal(err)
	}
	if !tier.scheduleProbeRepair(shards, blobs, seen, []int{0}, []int{1}, routedPeers, store.PartitionManifest, key) {
		t.Fatal("repair probe was not scheduled")
	}

	idx0, total0, _, ok0 := cache.ParseShardPrefix(confirmed.lastFill())
	idx1, total1, _, ok1 := cache.ParseShardPrefix(late.lastFill())
	if !ok0 || !ok1 || total0 != 6 || total1 != 6 {
		t.Fatalf("invalid repair prefixes: confirmed=(%d,%d,%v) late=(%d,%d,%v)", idx0, total0, ok0, idx1, total1, ok1)
	}
	if idx0 == idx1 || (idx0 != 4 && idx0 != 5) || (idx1 != 4 && idx1 != 5) {
		t.Fatalf("repair indices=(%d,%d), want distinct {4,5}", idx0, idx1)
	}
	if !tier.tryReserveRepairProbe() {
		t.Fatal("combined repair probe did not release its reservation")
	}
	tier.releaseRepairProbe()
}

func TestRepairProbeUsesForegroundPeerSnapshot(t *testing.T) {
	peers := []Peer{
		{ID: "p0", Endpoint: "old-p0"},
		{ID: "p1", Endpoint: "old-p1"},
		{ID: "p2", Endpoint: "old-p2"},
		{ID: "p3", Endpoint: "old-p3"},
		{ID: "p4", Endpoint: "old-p4"},
	}
	tierHandle, err := New(Config{Cluster: ClusterConfig{
		DataShards: 4, ParityShards: 1, Peers: peers,
	}})
	if err != nil {
		t.Fatal(err)
	}
	tier := tierHandle.(*impl)
	defer tier.Close()

	key := store.ContentKey{3, 1, 4}
	foregroundPeers, err := tier.router.LocatePeers(key, len(peers))
	if err != nil {
		t.Fatal(err)
	}
	probePos := 0
	oldPeer := &repairTestShard{result: cache.CacheMiss}
	newPeer := &repairTestShard{result: cache.CacheMiss}
	tier.pool.clients[foregroundPeers[probePos].Endpoint] = oldPeer

	updated := make([]Peer, len(peers))
	for i, peer := range peers {
		updated[i] = Peer{ID: peer.ID, Endpoint: "new-" + peer.ID}
		tier.pool.clients[updated[i].Endpoint] = newPeer
	}

	var repair func()
	tier.EnableRepair(func(fn func()) { repair = fn })
	if !tier.tryReserveRepairProbe() {
		t.Fatal("repair probe reservation was rejected")
	}
	shards := [][]byte{{1}, {2}, {3}, {4}, nil}
	blobs := make([]cache.Blob, len(peers))
	seen := []bool{true, true, true, true, false}
	if !tier.scheduleProbeRepair(shards, blobs, seen, nil, []int{probePos}, foregroundPeers, store.PartitionChunk, key) {
		t.Fatal("repair probe was not scheduled")
	}
	if repair == nil {
		t.Fatal("repair runner did not capture the probe")
	}
	if _, _, err := tier.router.ApplyMembership(2, updated); err != nil {
		t.Fatal(err)
	}

	repair()
	if fill := oldPeer.lastFill(); fill == nil {
		t.Fatal("foreground peer did not receive the repair")
	}
	if fill := newPeer.lastFill(); fill != nil {
		t.Fatalf("replacement peer received repair from stale positions: %x", fill)
	}
}

func TestValidShardIndexRejectsPrefixOnlyValue(t *testing.T) {
	value := make([]byte, cache.ShardPrefixSize)
	cache.EncodeShardPrefix(value, 4, 5)
	if _, ok := validShardIndex(value, 5); ok {
		t.Fatal("prefix-only shard counted as a usable hit")
	}
	value = append(value, 0x01)
	if idx, ok := validShardIndex(value, 5); !ok || idx != 4 {
		t.Fatalf("non-empty shard idx=%d ok=%v, want 4/true", idx, ok)
	}
}
