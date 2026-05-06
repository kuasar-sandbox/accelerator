// Package freq provides a Count-Min Sketch for tracking key access frequency.
//
// The sketch is used by RocksDB CompactionFilter to evict cold keys during
// background compaction. It is NOT used for admission control — all Put
// operations succeed unconditionally.
package freq

// DefaultCounters is the default number of 4-bit counters (8M ≈ 4 MiB).
const DefaultCounters = 8 << 20

// DefaultResetAfter is the default number of Touch calls before halving.
const DefaultResetAfter = 1 << 20 // 1M

// DefaultEvictThreshold is the minimum frequency below which a key is
// considered cold and eligible for removal by the CompactionFilter.
const DefaultEvictThreshold = 1

// ReservedSketchKey is the RocksDB key used to persist the sketch state.
// It lives in the default column family (not chunk/manifest).
const ReservedSketchKey = "__freq_sketch__"
