//go:build !no_rocksdb

package rocks

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/util"
	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/freq"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	grocksdb "github.com/linxGnu/grocksdb"
)

// ErrReadOnly is returned by Put on a impl opened via OpenReadOnly.
var ErrReadOnly = errors.New("rocks: store is read-only")

// roPool is a sync.Pool for reusing grocksdb.ReadOptions. Shared across
// all Get calls to avoid the CGO allocation cost of a fresh options
// object on every read.
var roPool = sync.Pool{
	New: func() any { return grocksdb.NewDefaultReadOptions() },
}

// impl wraps a RocksDB instance with column-family separation.
//
// Sketch ownership is internal: Open constructs a freq.Sketch from the
// provided FreqConfig, restores persisted state from CF_DEFAULT, and
// cascades sketch.Close during impl.Close. Callers never construct or
// close the sketch directly.
type impl struct {
	db       *grocksdb.DB
	cfh      map[string]*grocksdb.ColumnFamilyHandle
	sketch   *freq.Sketch
	filter   *sketchCompactionFilter // nil for OpenReadOnly stores
	ro       *grocksdb.ReadOptions
	wo       *grocksdb.WriteOptions
	readOnly bool
}

// Compile-time assertion that impl satisfies Interface.
var _ Interface = (*impl)(nil)

// Open creates or opens a RocksDB store at the given path in read-write
// mode. The sketch is constructed internally from freqCfg and persisted
// state is restored from CF_DEFAULT if present. Legacy sketch formats are
// discarded with a log message — the sketch starts empty in that case.
//
// Construction order (resolves two chicken-and-egg problems):
//  1. Build the CompactionFilter before opening the DB — grocksdb's
//     COWList captures it, so any startup/recovery compaction triggered
//     inside OpenDbColumnFamilies sees armed=false and unconditionally
//     preserves every key.
//  2. Populate the impl struct (db/cfh/ro/wo fields) before building
//     the sketch. The sketch's persistLoop closure captures the impl
//     pointer, and Go's memory model guarantees that writes preceding
//     the `go` statement inside freq.New are visible to the new
//     goroutine — so the first persist tick observes a fully-populated
//     impl.
//  3. Install the sketch on the filter and Arm it only after the sketch
//     has been restored from CF_DEFAULT. From that moment compaction
//     threads consult real frequency data.
func Open(cfg runtime.RocksConfig, freqCfg runtime.FreqConfig) (Interface, error) {
	return openImpl(cfg, freqCfg)
}

// openImpl is the internal constructor that returns the concrete
// *impl type. Used by tests that need access to unexported fields
// (db / cfh / sketch / compactAll / put / get). External callers go
// through Open which returns Interface.
func openImpl(cfg runtime.RocksConfig, freqCfg runtime.FreqConfig) (*impl, error) {
	opts := BuildOptions(cfg)

	// (1) Attach the (unarmed, sketch-less) compaction filter to the
	// chunk and manifest CFs. grocksdb retains the Go object via its
	// COWList. CF_DEFAULT holds ReservedSketchKey and other control
	// metadata — it must NOT run through this filter.
	filter := newSketchCompactionFilter()
	opts.ChunkCF.SetCompactionFilter(filter)
	opts.ManifestCF.SetCompactionFilter(filter)
	opts.BlobCF.SetCompactionFilter(filter)

	cfNames := []string{cfDefault, cfChunk, cfManifest, cfBlob}
	cfOpts := []*grocksdb.Options{opts.DBOpts, opts.ChunkCF, opts.ManifestCF, opts.BlobCF}

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(opts.DBOpts, cfg.Path, cfNames, cfOpts)
	if err != nil {
		return nil, fmt.Errorf("rocks: open %s: %w", cfg.Path, err)
	}

	cfh := make(map[string]*grocksdb.ColumnFamilyHandle, len(cfNames))
	for i, name := range cfNames {
		cfh[name] = cfHandles[i]
	}

	// (2) Populate impl fields BEFORE the sketch is built. The closure
	// below captures `store`, and freq.New's persistLoop goroutine only
	// runs after this point — Go's memory model guarantees the captured
	// pointer sees initialised fields.
	inst := &impl{
		db:       db,
		cfh:      cfh,
		filter:   filter,
		ro:       grocksdb.NewDefaultReadOptions(),
		wo:       grocksdb.NewDefaultWriteOptions(),
		readOnly: false,
	}

	sketch := buildSketch(freqCfg, func(data []byte) {
		// Tick-path background persister. Mirrors the sync path in
		// persistSketch but is driven by freq.Sketch's persistLoop.
		if inst.readOnly || inst.wo == nil {
			return
		}
		if err := inst.db.PutCF(inst.wo, inst.cfh[cfDefault], []byte(freq.ReservedSketchKey), data); err != nil {
			log.Printf("rocks: background sketch persist failed: %v", err)
		}
	})
	inst.sketch = sketch
	filter.SetSketch(sketch)

	// (3) Restore persisted sketch from CF_DEFAULT. No concurrent users
	// exist yet — the filter is still unarmed and Get/Put aren't exposed.
	ro := grocksdb.NewDefaultReadOptions()
	data, getErr := db.GetCF(ro, cfh[cfDefault], []byte(freq.ReservedSketchKey))
	ro.Destroy()
	if getErr == nil && data != nil && data.Size() > 0 {
		if err := sketch.Unmarshal(data.Data()); err != nil {
			log.Printf("rocks: discarding persisted sketch: %v", err)
		}
	}
	if data != nil {
		data.Free()
	}

	// (4) Open the gate — unless the caller has asked to disable
	// eviction entirely (freq.disable_eviction in the YAML config).
	// When the local daemon plays the "origin" role for a downstream
	// tiered instance, writes land once and must stay put: arming the
	// filter in that context would silently drop once-touched keys on
	// the first compaction pass.
	if !freqCfg.DisableEviction {
		filter.Arm()
	}

	return inst, nil
}

// OpenReadOnly opens a RocksDB store in read-only mode using RocksDB's
// secondary-instance semantics. Intended for inspection tools (e.g.
// `cache-ctl info`) that must not perturb a live database: no WAL is
// written, no MANIFEST update occurs, no recovery replay runs, and no
// sketch is constructed or persisted. Put on such a store returns
// ErrReadOnly unconditionally.
func OpenReadOnly(cfg runtime.RocksConfig) (Interface, error) {
	return openReadOnlyImpl(cfg)
}

// openReadOnlyImpl is the internal read-only constructor exposing the
// concrete *impl type. See openImpl for rationale.
func openReadOnlyImpl(cfg runtime.RocksConfig) (*impl, error) {
	opts := BuildOptions(cfg)

	cfNames := []string{cfDefault, cfChunk, cfManifest, cfBlob}
	cfOpts := []*grocksdb.Options{opts.DBOpts, opts.ChunkCF, opts.ManifestCF, opts.BlobCF}

	// errorIfWalFileExists=false tolerates a crashed-but-not-recovered WAL.
	db, cfHandles, err := grocksdb.OpenDbForReadOnlyColumnFamilies(
		opts.DBOpts, cfg.Path, cfNames, cfOpts, false,
	)
	if err != nil {
		return nil, fmt.Errorf("rocks: open read-only %s: %w", cfg.Path, err)
	}

	cfh := make(map[string]*grocksdb.ColumnFamilyHandle, len(cfNames))
	for i, name := range cfNames {
		cfh[name] = cfHandles[i]
	}

	return &impl{
		db:       db,
		cfh:      cfh,
		sketch:   nil, // intentional: inspect tooling never touches frequency state
		ro:       grocksdb.NewDefaultReadOptions(),
		wo:       nil, // intentional: any Put is rejected at the Go layer
		readOnly: true,
	}, nil
}

// sketch returns the store's frequency sketch. nil if the store was opened
// via OpenReadOnly. Intended for CompactionFilter integration — callers
// should NOT invoke Touch on it; Touch is handled internally by get/put.
func (s *impl) Sketch() *freq.Sketch { return s.sketch }

// get retrieves a value by namespace and key and wraps it in a
// refcounted cache.Blob. The blob is backed by a buffer borrowed from
// the chunk or manifest value pool (selected by ns); the buffer
// returns to the pool when the last Clone of the returned blob is
// Released.
//
// The ctx parameter is accepted for interface parity with other
// cache layers but is NOT honoured: grocksdb's CGO GetCF is a
// synchronous blocking call and cannot be cancelled mid-operation.
// Callers that need a hard wall-clock bound should wrap the get in
// a goroutine + select on ctx.Done.
func (s *impl) get(ctx context.Context, ns string, key []byte) (cache.Blob, bool, error) {
	// Touch before the actual read so that a Get call — regardless of
	// hit/miss — registers frequency. This matches the pre-refactor
	// "Touch before backend.Get" ordering in wire_handler.
	if s.sketch != nil {
		s.sketch.Touch(key)
	}

	cf, ok := s.cfh[ns]
	if !ok {
		cf = s.cfh[cfChunk]
	}

	// Use per-call ReadOptions to avoid CGO contention on shared s.ro.
	ro := roPool.Get().(*grocksdb.ReadOptions)
	slice, err := s.db.GetCF(ro, cf, key)
	roPool.Put(ro)
	if err != nil {
		return nil, false, fmt.Errorf("rocks: get: %w", err)
	}
	defer slice.Free()

	if slice.Size() == 0 {
		return nil, false, nil
	}

	// Pick the right-sized pool (chunk vs manifest) and borrow a
	// buffer, copying the grocksdb slice contents into it before
	// the slice.Free() (deferred above) runs.
	pool := poolForNS(ns)
	buf, blob := pool.Alloc(slice.Size())
	copy(buf, slice.Data())
	return blob, true, nil
}

// put writes a value by namespace and key.
//
// Read-only stores return ErrReadOnly without touching the DB. On success,
// the key is registered in the frequency sketch — failed puts do not
// occupy frequency budget (deliberate correction over the pre-refactor
// handler-level Touch, which counted failed writes).
//
// The ctx parameter is accepted for interface parity but not honoured;
// see the note on get.
func (s *impl) put(ctx context.Context, ns string, key, value []byte) error {
	if s.readOnly {
		return ErrReadOnly
	}

	cf, ok := s.cfh[ns]
	if !ok {
		cf = s.cfh[cfChunk]
	}

	if err := s.db.PutCF(s.wo, cf, key, value); err != nil {
		return fmt.Errorf("rocks: put: %w", err)
	}
	if s.sketch != nil {
		s.sketch.Touch(key)
	}
	return nil
}

// PersistSketch saves the current sketch state to the reserved key.
// Noop for read-only stores and for stores without a sketch.
func (s *impl) PersistSketch() {
	if s.readOnly || s.sketch == nil || s.wo == nil {
		return
	}
	data := s.sketch.Marshal()
	if len(data) > 0 {
		_ = s.db.PutCF(s.wo, s.cfh[cfDefault], []byte(freq.ReservedSketchKey), data)
	}
}

// Close closes the store and all column family handles. Order:
//  1. PersistSketch (read-write only) — flush external state first.
//  2. sketch.Close — stop background goroutines.
//  3. grocksdb resource release — last, so nothing above touches a freed DB.
func (s *impl) Close() error {
	if !s.readOnly {
		s.PersistSketch()
	}
	if s.sketch != nil {
		_ = s.sketch.Close()
	}

	if s.ro != nil {
		s.ro.Destroy()
	}
	if s.wo != nil {
		s.wo.Destroy()
	}
	for _, h := range s.cfh {
		h.Destroy()
	}
	s.db.Close()
	return nil
}

// compactAll triggers manual compaction on all column families.
// Internal — exposed only to package-internal tests.
func (s *impl) compactAll() {
	for _, cf := range s.cfh {
		s.db.CompactRangeCF(cf, grocksdb.Range{Start: nil, Limit: nil})
	}
}

// ---------------------------------------------------------------------
// Interface methods: cache.Tier (object-level) and cache.ShardTier
// (shard-level with hash||idx key encoding).
// ---------------------------------------------------------------------

// Get implements cache.Getter. Object-level read routed to the CF
// matching store.Partition.
func (s *impl) Get(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	blob, found, err := s.get(ctx, partitionToCF(p), key[:])
	if err != nil {
		return cache.CacheMiss, nil, err
	}
	if !found {
		return cache.CacheMiss, nil, nil
	}
	return cache.CacheHit, blob, nil
}

// Fill implements cache.Filler. Object-level write. Synchronous —
// `data` is safe to release after Fill returns.
func (s *impl) Fill(ctx context.Context, p store.Partition, key store.ContentKey, data []byte) error {
	return s.put(ctx, partitionToCF(p), key[:], data)
}

// GetShard implements cache.ShardGetter. Shard-level read keyed by
// content hash only; the returned blob's first ShardPrefixSize bytes
// carry the shard (idx, total) metadata as stored by FillShard.
//
// The on-disk rocksdb key is `content_key[:32] || shardKeySuffix` —
// a fixed-suffix byte differentiates shard entries from object
// entries that share the same CF, so object and shard ops never
// collide even when the same cache-ctl daemon serves both (e.g. the
// unified-handler mode used by e2e tests).
func (s *impl) GetShard(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	blob, found, err := s.get(ctx, partitionToCF(p), shardKey(key))
	if err != nil {
		return cache.CacheMiss, nil, err
	}
	if !found {
		return cache.CacheMiss, nil, nil
	}
	return cache.CacheHit, blob, nil
}

// FillShard implements cache.ShardFiller. The value bytes are written
// verbatim; the caller is responsible for prefixing value with the
// [idx][total] header (see cache.EncodeShardPrefix). A peer stores
// exactly one shard per content_key — repeated FillShard for the same
// key overwrites, consistent with the invariant that under normal
// operation a peer holds at most one (idx, total, data) per content.
func (s *impl) FillShard(ctx context.Context, p store.Partition, key store.ContentKey, value []byte) error {
	return s.put(ctx, partitionToCF(p), shardKey(key), value)
}

// shardKey produces the rocksdb key for a shard: the 32-byte content
// key plus a fixed marker byte. The marker discriminates shards from
// 32-byte object keys in the same CF; its value (shardKeySuffix) is
// implementation detail — any fixed byte works.
const shardKeySuffix byte = 0x00

func shardKey(key store.ContentKey) []byte {
	k := make([]byte, 33)
	copy(k[:32], key[:])
	k[32] = shardKeySuffix
	return k
}

// buildSketch constructs a freq.Sketch from runtime.FreqConfig, applying
// defaults for unset fields. A zero FreqConfig yields a Sketch with all
// defaults — this is what cache-ctl bench and the test suite rely on.
//
// persistFn is forwarded directly to freq.Config.PersistFn. freq.New's
// existing gate (PersistInterval > 0 && PersistFn != nil) decides
// whether to start the background persistLoop goroutine — so passing
// nil here (e.g. when persistence is disabled) is safe and keeps
// behaviour identical to the pre-refactor "Close-only persist" path.
func buildSketch(cfg runtime.FreqConfig, persistFn func([]byte)) *freq.Sketch {
	counters := parseCountWithDefault(cfg.Counters, freq.DefaultCounters)
	resetAfter := parseCountWithDefault(cfg.ResetAfter, freq.DefaultResetAfter)
	resetInterval, _ := time.ParseDuration(cfg.ResetInterval)
	persistInterval, _ := time.ParseDuration(cfg.PersistInterval)
	threshold := uint8(cfg.EvictThreshold)
	if threshold == 0 {
		threshold = freq.DefaultEvictThreshold
	}
	return freq.New(freq.Config{
		Counters:        counters,
		ResetAfter:      resetAfter,
		ResetInterval:   resetInterval,
		EvictThreshold:  threshold,
		PersistInterval: persistInterval,
		PersistFn:       persistFn,
	})
}

func parseCountWithDefault(s string, def uint64) uint64 {
	if s == "" {
		return def
	}
	v, err := util.ParseSize(s)
	if err != nil || v == 0 {
		return def
	}
	return v
}
