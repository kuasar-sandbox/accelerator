package cache

import (
	"context"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// storeBackend is the private adapter target for NewStoreOrigin.
// *pkg/store/client.Client satisfies it structurally. Kept unexported
// because NewStoreOrigin is the only user — callers don't need to name
// or mock this interface directly.
type storeBackend interface {
	Get(ctx context.Context, p store.Partition, key store.ContentKey) (bool, []byte, error)
}

// NewStoreOrigin wraps a byte-returning store client as a cache.Getter
// suitable for use as the origin tier of a TieredCache. Hits are
// wrapped in a GC-managed memBlob; origin is the cold path so the
// one-time allocation has no measurable cost versus a pooled blob.
func NewStoreOrigin(g storeBackend) Getter {
	return &storeOrigin{g: g}
}

type storeOrigin struct{ g storeBackend }

func (s *storeOrigin) Get(ctx context.Context, p store.Partition, key store.ContentKey) (CacheResult, Blob, error) {
	found, data, err := s.g.Get(ctx, p, key)
	if err != nil {
		return CacheMiss, nil, err
	}
	if !found {
		return CacheMiss, nil, nil
	}
	return CacheHit, NewMemBlob(data), nil
}
