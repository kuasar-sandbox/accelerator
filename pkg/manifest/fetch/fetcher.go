package fetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// Fetcher opens one manifest-backed Stream at a time. Streams opened by the
// same Fetcher share the underlying Getter and its on-demand/prefetch
// admission state, but otherwise have independent data and lifetimes.
type Fetcher interface {
	OpenManifest(ctx context.Context, key store.ContentKey) (Stream, error)
}

// NewFetcher returns a Fetcher bound to the given cache.Getter (which
// internally fans out to cache-ctl, or falls back to a store-as-cache
// adapter; that policy lives outside this package) and decryptor.
func NewFetcher(customerKey [32]byte, cg cache.Getter, dec crypto.Decryptor) Fetcher {
	return &fetcher{customerKey: customerKey, cache: newScheduledCacheClient(cg), dec: dec}
}

type fetcher struct {
	customerKey [32]byte
	cache       *scheduledCacheClient
	dec         crypto.Decryptor
}

// OpenManifest loads, parses, and key-table-unseals one manifest, then returns
// a lazy chunk-backed Stream. Layer composition is explicitly handled by
// NewLayered rather than encoded into a manifest reference.
func (f *fetcher) OpenManifest(ctx context.Context, manifestKey store.ContentKey) (Stream, error) {
	result, blob, err := f.cache.OnDemandGetter().Get(ctx, store.PartitionManifest, manifestKey)
	if err != nil {
		if blob != nil {
			blob.Release()
		}
		return nil, fmt.Errorf("fetch: manifest blob: %w", err)
	}
	if result != cache.CacheHit {
		if blob != nil {
			blob.Release()
		}
		return nil, fmt.Errorf("fetch: manifest not found: %s", hex.EncodeToString(manifestKey[:]))
	}
	if blob == nil {
		return nil, fmt.Errorf("fetch: manifest cache hit returned nil blob")
	}
	mData := blob.Bytes()
	if sha256.Sum256(mData) != manifestKey {
		blob.Release()
		return nil, fmt.Errorf("fetch: manifest content key mismatch (corrupt or tampered store/cache)")
	}
	m, sealedKT, err := codec.Unmarshal(mData)
	blob.Release()
	if err != nil {
		return nil, fmt.Errorf("fetch: unmarshal manifest: %w", err)
	}
	keys, err := codec.UnsealKeys(m, sealedKT, f.customerKey, f.dec)
	if err != nil {
		return nil, fmt.Errorf("fetch: unseal keys: %w", err)
	}
	return newManifestStream(
		m,
		keys,
		f.cache.OnDemandGetter(),
		f.cache.PrefetchGetter(),
		f.dec,
	), nil
}
