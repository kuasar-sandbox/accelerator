package cache

import (
	"context"
	"fmt"

	"github.com/fullof-work/container-accelerator-research/pkg/store"
)

// TieredCache orchestrates lookup across cache layers: L1 local → L3 origin store.
type TieredCache struct {
	l1     *LocalCache
	origin store.ContentStore
}

// NewTieredCache creates a tiered cache with L1 local cache and L3 origin store.
func NewTieredCache(l1 *LocalCache, origin store.ContentStore) *TieredCache {
	return &TieredCache{
		l1:     l1,
		origin: origin,
	}
}

// Get attempts to read from L1, then origin store.
// On L1 miss + origin hit, promotes to L1.
func (tc *TieredCache) Get(ctx context.Context, hash [32]byte) ([]byte, error) {
	// L1 lookup.
	if data, ok := tc.l1.Get(hash); ok {
		return data, nil
	}

	// L3 origin lookup.
	data, err := tc.origin.Get(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("tiered cache: origin: %w", err)
	}

	// Promote to L1.
	tc.l1.Put(hash, data)
	return data, nil
}

// L1Stats returns the L1 cache statistics.
func (tc *TieredCache) L1Stats() CacheStats {
	return tc.l1.Stats()
}
