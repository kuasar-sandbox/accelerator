// Package rocks provides a RocksDB-backed key-value store for the cache layer.
//
// The store uses separate column families for chunk and manifest data,
// and integrates with freq.Sketch via CompactionFilter for cold-key eviction.
package rocks

import "github.com/fullof-work/mass-sandbox/pkg/store"

// Column family names (internal — callers address data via store.Partition).
const (
	cfChunk    = "chunk"
	cfManifest = "manifest"
	cfDefault  = "default" // RocksDB requires a "default" CF
)

// partitionToCF maps a store.Partition to its rocks column family.
// Unknown partitions fall back to cfChunk (legacy behaviour).
func partitionToCF(p store.Partition) string {
	switch p {
	case store.PartitionChunk:
		return cfChunk
	case store.PartitionManifest:
		return cfManifest
	default:
		return cfChunk
	}
}
