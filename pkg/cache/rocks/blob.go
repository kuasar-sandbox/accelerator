package rocks

import (
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/cache"
)

// Two independent value buffer pools, sized to the typical payload of
// their respective column families. A single shared pool would cycle
// 256 KiB buffers through 16 KiB manifest reads and vice versa —
// either wasting capacity or forcing grow-and-realloc on every fetch.
// With two pools each Alloc produces a right-sized buffer that stays
// right-sized across reuse.
//
// chunkPool: typical FastCDC block is 256 KiB (default avg).
// manifestPool: typical manifest header + sealed key table lands
// under 16 KiB for images up to ~10 GiB; larger images will cause
// the pool to grow on demand.
var (
	chunkPool    = cache.NewPool(256 << 10)
	manifestPool = cache.NewPool(16 << 10)
)

// poolForNS routes a column family name to the matching value pool.
// Unknown namespaces fall through to chunkPool — this matches the
// cfChunk default elsewhere in the store.
func poolForNS(ns string) cache.BlobPool {
	switch ns {
	case cfManifest:
		return manifestPool
	default:
		return chunkPool
	}
}
