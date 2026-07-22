package fetch

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// CustomerKeyFunc supplies the 32-byte convergent-encryption customer
// key on demand. Function form (not value) so callers can defer key
// material resolution to env / vault / etc.; for the simplest case
// pass a closure that returns a literal.
type CustomerKeyFunc func() ([32]byte, error)

// Fetcher is the read-side factory: open a Stream over one or more content
// keys (several keys overlay as layers — see Fetch). One Fetcher per
// (process, customer-key, cache, decryptor) combination; many Streams per
// Fetcher (one per read).
type Fetcher interface {
	Fetch(ctx context.Context, keys ...store.ContentKey) (Stream, error)
}

// NewFetcher returns a Fetcher bound to the given cache.Getter (which
// internally fans out to cache-ctl, or falls back to a store-as-cache
// adapter; that policy lives outside this package) and decryptor.
//
// The customer key is fetched lazily (per Fetch call) via keyFn so the
// caller controls the key's lifetime; pass a closure if you want it
// resolved once per process.
func NewFetcher(keyFn CustomerKeyFunc, cg cache.Getter, dec crypto.Decryptor) Fetcher {
	return &fetcher{keyFn: keyFn, cache: newScheduledCacheClient(cg), dec: dec}
}

type fetcher struct {
	keyFn CustomerKeyFunc
	cache *scheduledCacheClient
	dec   crypto.Decryptor
}

// Fetch opens a Stream over one or more manifests. A single key reads that
// manifest directly; several keys overlay as layers (manifest://k1:k2:k3):
// the topmost layer holding data at an offset serves it, declared holes fall
// through to lower layers, and Size is the maximum over all layers. All layers
// are unsealed with this Fetcher's customer key.
//
// Stream data and lifetime remain independent across calls. Streams returned by
// the same Fetcher share only the underlying Getter and its on-demand/prefetch
// admission state; they may otherwise be used concurrently.
func (f *fetcher) Fetch(ctx context.Context, keys ...store.ContentKey) (Stream, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("fetch: no manifest key")
	}
	subs := make([]Stream, len(keys))
	for i, k := range keys {
		s, err := f.fetchOne(ctx, k)
		if err != nil {
			return nil, err
		}
		subs[i] = s
	}
	return NewLayered(subs...), nil
}

// fetchOne loads, parses, and key-table-unseals a single manifest and builds a
// single-layer stream over its content.
func (f *fetcher) fetchOne(ctx context.Context, manifestKey store.ContentKey) (Stream, error) {
	customerKey, err := f.keyFn()
	if err != nil {
		return nil, fmt.Errorf("fetch: customer key: %w", err)
	}

	result, blob, err := f.cache.OnDemandGetter().Get(ctx, store.PartitionManifest, manifestKey)
	if err != nil {
		return nil, fmt.Errorf("fetch: manifest blob: %w", err)
	}
	if result != cache.CacheHit {
		return nil, fmt.Errorf("fetch: manifest not found: %s", hex.EncodeToString(manifestKey[:]))
	}
	mData := append([]byte(nil), blob.Bytes()...)
	blob.Release()

	m, sealedKT, err := codec.Unmarshal(mData)
	if err != nil {
		return nil, fmt.Errorf("fetch: unmarshal manifest: %w", err)
	}
	keys, err := codec.UnsealKeys(m, sealedKT, customerKey, f.dec)
	if err != nil {
		return nil, fmt.Errorf("fetch: unseal keys: %w", err)
	}
	// manifestStream takes a ChunkEncryptor (legacy interface); the same codec
	// impl satisfies both it and Decryptor. Adapt so the call site doesn't reach
	// into pkg/manifest/crypto internals.
	return newManifestStream(
		m,
		keys,
		f.cache.OnDemandGetter(),
		f.cache.PrefetchGetter(),
		decryptorAsChunkEncryptor{dec: f.dec},
		manifestKey,
		true,
	), nil
}

// decryptorAsChunkEncryptor bridges crypto.Decryptor (DecryptChunk /
// DecryptChunkInPlace) to crypto.ChunkEncryptor (Decrypt /
// DecryptInPlace). Read-only — Encrypt panics if called, signalling a
// mis-use (the stream never encrypts).
type decryptorAsChunkEncryptor struct{ dec crypto.Decryptor }

func (a decryptorAsChunkEncryptor) Encrypt(salt [32]byte, plain []byte) ([]byte, [32]byte, [32]byte) {
	panic("fetch: Encrypt called on read-only stream adapter")
}

func (a decryptorAsChunkEncryptor) Decrypt(key [32]byte, cipher []byte) ([]byte, error) {
	return a.dec.DecryptChunk(key, cipher)
}

func (a decryptorAsChunkEncryptor) DecryptInPlace(key [32]byte, buf []byte) ([]byte, error) {
	return a.dec.DecryptChunkInPlace(key, buf)
}
