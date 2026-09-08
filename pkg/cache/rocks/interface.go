package rocks

import (
	"io"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
)

// Interface is the single exported surface of the rocks package.
// A rocks instance is simultaneously a cache.Tier (object-level
// Get/Fill), a cache.ShardTier (shard-level GetShard/FillShard), and
// an io.Closer. AllStats returns per-CF rocksdb properties for the
// Info gRPC service.
//
// Obtained via Open (read-write) or OpenReadOnly (read-only — Fill /
// FillShard return ErrReadOnly on such instances). Under the no_rocksdb
// build tag both constructors return ErrNotCompiled and the interface
// exists only to keep the public API shape stable.
type Interface interface {
	cache.Tier
	cache.ShardTier
	io.Closer

	// AllStats snapshots per-CF properties (estimate-num-keys,
	// disk-usage, mem-usage, ...) for each user column family.
	AllStats() []CFStats
}
