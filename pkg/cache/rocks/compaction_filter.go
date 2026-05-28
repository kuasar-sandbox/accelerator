package rocks

import (
	"sync/atomic"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/cache/freq"
)

// sketchCompactionFilter drives RocksDB compaction on the chunk and
// manifest CFs based on the frequency sketch owned by the Store. Keys
// whose sketch estimate is at or below the configured evict threshold
// are removed; everything else is preserved.
//
// Two-phase initialisation:
//  1. newSketchCompactionFilter() — sketch=nil, armed=false. Attach to
//     CFs and open the DB; any startup/recovery compaction sees
//     armed=false and preserves everything unconditionally.
//  2. SetSketch(s) after the sketch is constructed, Arm() after the
//     sketch is restored from CF_DEFAULT. Filter() now consults the
//     sketch.
//
// Thread safety: grocksdb invokes Filter from multiple compaction
// threads concurrently. sketch is an atomic.Pointer and the underlying
// freq.Sketch is lock-free; armed is an atomic.Bool. No additional
// synchronisation is required.
//
// The filter never calls sketch.Touch — compaction happens independently
// of user access, so treating it as an access event would make the
// sketch self-reinforce (every compaction would refresh every key).
// Filter is strictly read-only against the sketch.
type sketchCompactionFilter struct {
	sketch atomic.Pointer[freq.Sketch]
	armed  atomic.Bool
}

func newSketchCompactionFilter() *sketchCompactionFilter {
	return &sketchCompactionFilter{}
}

// Filter is invoked by grocksdb for every key encountered during
// compaction. Returning (true, nil) removes the key; (false, nil)
// preserves it.
func (f *sketchCompactionFilter) Filter(level int, key, val []byte) (bool, []byte) {
	if !f.armed.Load() {
		return false, nil
	}
	s := f.sketch.Load()
	if s == nil {
		return false, nil
	}
	if s.ShouldEvict(key) {
		return true, nil
	}
	return false, nil
}

// SetSketch installs the sketch pointer. Must be called exactly once
// from rocks.Open, after the Store is fully constructed.
func (f *sketchCompactionFilter) SetSketch(s *freq.Sketch) { f.sketch.Store(s) }

// Arm opens the gate. After Arm, Filter consults the sketch instead of
// unconditionally preserving. Must be called exactly once from
// rocks.Open, after SetSketch and after the persisted sketch has been
// restored from CF_DEFAULT.
func (f *sketchCompactionFilter) Arm() { f.armed.Store(true) }

// Name implements grocksdb.CompactionFilter.
func (f *sketchCompactionFilter) Name() string { return "cache/rocks/sketchFilter" }

// SetIgnoreSnapshots implements grocksdb.CompactionFilter. We are
// frequency-driven and snapshot-agnostic.
func (f *sketchCompactionFilter) SetIgnoreSnapshots(b bool) {}

// Destroy implements grocksdb.CompactionFilter. grocksdb's COWList
// retains the filter for the lifetime of the DB, so there is nothing
// to release here.
func (f *sketchCompactionFilter) Destroy() {}
