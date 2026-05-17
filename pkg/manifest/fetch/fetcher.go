package fetch

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/fullof-work/mass-sandbox/pkg/cache"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/codec"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/crypto"
	"github.com/fullof-work/mass-sandbox/pkg/store"
)

// CustomerKeyFunc supplies the 32-byte convergent-encryption customer
// key on demand. Function form (not value) so callers can defer key
// material resolution to env / vault / etc.; for the simplest case
// pass a closure that returns a literal.
type CustomerKeyFunc func() ([32]byte, error)

// Fetcher is the read-side factory: open a Stream for a given content
// key. One Fetcher per (process, customer-key, cache, decryptor)
// combination; many Streams per Fetcher (one per manifest read).
type Fetcher interface {
	Fetch(ctx context.Context, manifestKey store.ContentKey) (Stream, error)
}

// NewFetcher returns a Fetcher bound to the given cache.Getter (which
// internally fans out to cache-ctl, or falls back to a store-as-cache
// adapter; that policy lives outside this package) and decryptor.
//
// The customer key is fetched lazily (per Fetch call) via keyFn so the
// caller controls the key's lifetime; pass a closure if you want it
// resolved once per process.
func NewFetcher(keyFn CustomerKeyFunc, cg cache.Getter, dec crypto.Decryptor) Fetcher {
	return &fetcher{keyFn: keyFn, cache: cg, dec: dec}
}

type fetcher struct {
	keyFn CustomerKeyFunc
	cache cache.Getter
	dec   crypto.Decryptor
}

// Fetch loads + parses + key-table-unseals the manifest identified by
// manifestKey, then builds a Stream over its content. Each call is
// independent — Streams returned by repeated Fetch calls do not share
// state and may be used concurrently.
func (f *fetcher) Fetch(ctx context.Context, manifestKey store.ContentKey) (Stream, error) {
	customerKey, err := f.keyFn()
	if err != nil {
		return nil, fmt.Errorf("fetch: customer key: %w", err)
	}

	result, blob, err := f.cache.Get(ctx, store.PartitionManifest, manifestKey)
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
	// stream takes a ChunkEncryptor today (legacy interface). Decryptor
	// is the new narrow surface; in practice both are satisfied by the
	// same underlying codec impl. Adapt via decryptorAsChunkEncryptor
	// so the call site doesn't reach into pkg/manifest/crypto internals.
	return &stream{
		m:         m,
		cache:     f.cache,
		encryptor: decryptorAsChunkEncryptor{dec: f.dec},
		keys:      keys,
	}, nil
}

// decryptorAsChunkEncryptor bridges crypto.Decryptor (DecryptChunk /
// DecryptChunkInPlace) to crypto.ChunkEncryptor (Decrypt /
// DecryptInPlace). Read-only — Encrypt panics if called, signalling a
// mis-use (the stream never encrypts).
type decryptorAsChunkEncryptor struct{ dec crypto.Decryptor }

func (a decryptorAsChunkEncryptor) Encrypt(key [32]byte, plain []byte) ([]byte, [32]byte) {
	panic("fetch: Encrypt called on read-only stream adapter")
}

func (a decryptorAsChunkEncryptor) Decrypt(key [32]byte, cipher []byte) ([]byte, error) {
	return a.dec.DecryptChunk(key, cipher)
}

func (a decryptorAsChunkEncryptor) DecryptInPlace(key [32]byte, buf []byte) ([]byte, error) {
	return a.dec.DecryptChunkInPlace(key, buf)
}
