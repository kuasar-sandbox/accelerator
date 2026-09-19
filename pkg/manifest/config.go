// Package manifest is the orchestration facade for the read and write
// pipelines of the container accelerator.
//
// The package's subpackages cover the layered concerns:
//
//	codec    — on-disk binary format (Marshal / Unmarshal / AAD / UnsealKeys)
//	chunker  — FastCDC / fixed-size chunking
//	crypto   — convergent chunk encryption + customer-key-table sealing
//	ingest   — write pipeline (Reader → Chunk → Encrypt → Store + Manifest)
//	fetch    — read pipeline (Manifest + Cache → decrypted bytes)
//
// pkg/manifest itself owns the cross-subpackage Config (YAML schema)
// and the two factory methods (Config.NewIngester / Config.NewFetcher)
// that wire the collaborators together. Callers that already hold
// concrete chunker / crypto / store / cache instances can skip this
// layer and call the subpackage factories directly.
package manifest

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	cacheclient "github.com/kuasar-sandbox/accelerator/pkg/cache/client"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/ingest"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	storeclient "github.com/kuasar-sandbox/accelerator/pkg/store/client"
)

// Default store/cache timeouts when StoreConfig.Timeout /
// CacheConfig.Timeout are empty. Empty = no per-call deadline: a
// store/cache RPC is bounded only by the caller's context, never an
// arbitrary number. Operators opt into a finite budget by setting
// store.timeout / cache.timeout explicitly.
const (
	defaultStoreTimeout = ""
	defaultCacheTimeout = ""
)

// Config aggregates the five sub-schemas that together describe a
// fully-wired ingest or fetch pipeline. The YAML form mirrors the
// subpackages 1:1:
//
//	manifest: {key}                # 32-byte hex customer key
//	store:    {endpoint, ...}
//	cache:    {endpoint, ...}      # optional; absent → store-as-origin
//	chunker:  {mode, cdc:{...}}
//	crypto:   {chunk, manifest}
//
// Read and write paths use the same Config; ingest needs store +
// chunker + crypto, fetch needs cache (or store fallback) + crypto.
// Both methods validate just the fields they consume.
type Config struct {
	Manifest ManifestSubConfig `yaml:"manifest"`
	Store    StoreConfig       `yaml:"store"`
	Cache    CacheConfig       `yaml:"cache"`
	Chunker  chunker.Config    `yaml:"chunker"`
	Crypto   crypto.Config     `yaml:"crypto"`
}

// ManifestSubConfig carries the per-config customer-key material. Kept
// in a sub-struct so the YAML node name remains `manifest:` and the
// field is grouped with future manifest-wide knobs.
type ManifestSubConfig struct {
	// Key is the 32-byte customer key, hex-encoded (64 chars). Required
	// for any ingest or fetch, but may be left empty here and supplied via
	// the $MANIFEST_KEY environment variable instead — which also overrides
	// a non-empty value here (see CustomerKeyEnv), so the secret can stay
	// out of this shared file. CustomerKey() resolves + validates lazily,
	// so an absent key only trips a call site that actually needs to
	// seal/unseal.
	Key string `yaml:"key"`

	// VerifyContent controls SHA-256 verification of physical Manifest and
	// Chunk objects during ordinary reads. Nil defaults to true.
	VerifyContent *bool `yaml:"verify_content,omitempty"`

	// WriteGeneration optionally pins locally produced objects to one
	// generation. An online Store must admit it; offline writers derive the
	// canonical salt locally. Empty offline defaults to the ordinary name NONE.
	WriteGeneration string `yaml:"write_generation,omitempty"`
}

var warnVerificationDisabled sync.Once

func (c *Config) fetchOptions() fetch.Options {
	verify := c.Manifest.VerifyContent == nil || *c.Manifest.VerifyContent
	if !verify {
		warnVerificationDisabled.Do(func() {
			log.Printf("WARNING: manifest.verify_content=false; ordinary Manifest/Chunk SHA-256 verification is disabled")
		})
	}
	return fetch.Options{VerifyContent: verify}
}

// StoreConfig is the wire-level connection schema for the store-ctl
// daemon. Endpoint is the only required field; Pool and Timeout fall
// back to client defaults when zero/empty. Timeout is a Go duration
// string (e.g. "5s", "1m") so YAML stays human-readable.
type StoreConfig struct {
	Endpoint string `yaml:"endpoint"`
	Pool     int    `yaml:"pool"`
	Timeout  string `yaml:"timeout"`
}

// CacheConfig is the wire-level connection schema for the cache-ctl
// daemon. When Endpoint is empty, the read path falls back to a
// store-as-cache adapter (cache.NewStoreOrigin) and the manifest
// facade dials only the store. The fallback exists so the same
// Config can drive both daemon-backed and daemon-less deployments.
type CacheConfig struct {
	Endpoint string `yaml:"endpoint"`
	Pool     int    `yaml:"pool"`
	Timeout  string `yaml:"timeout"`
}

// parseTimeout parses a Go duration string and applies fallback when
// empty. Used by NewIngester / NewFetcher.
func parseTimeout(s, fallback string) (time.Duration, error) {
	if s == "" {
		s = fallback
	}
	if s == "" {
		return 0, nil // 0 = no deadline; bounded only by caller ctx
	}
	return time.ParseDuration(s)
}

// IngesterCloser bundles ingest.Ingester with the Close method that
// releases the underlying store client. Callers always close so the
// connection pool drains; the embedded Closer is documented by name
// so the caller never needs a runtime type assertion.
type IngesterCloser interface {
	ingest.Ingester
	io.Closer
}

// FetcherCloser bundles fetch.Fetcher with the Close method that
// releases the underlying cache and/or store client.
type FetcherCloser interface {
	fetch.Fetcher
	io.Closer
}

// NewIngester wires Store + Chunker + Crypto into an Ingester. The
// caller supplies the customer-key resolver and (optional) extra-salt
// resolver — both are invoked once per Ingest call so secrets and
// per-image salts can be sourced lazily.
//
// Close must be called on the returned IngesterCloser to drain the
// store client's connection pool.
func (c *Config) NewIngester(keyFn ingest.CustomerKeyFunc, extraSaltFn ingest.ExtraSaltFunc) (IngesterCloser, error) {
	if c.Store.Endpoint == "" {
		return nil, fmt.Errorf("manifest: store.endpoint required for ingest")
	}
	storeTO, err := parseTimeout(c.Store.Timeout, defaultStoreTimeout)
	if err != nil {
		return nil, fmt.Errorf("manifest: store.timeout: %w", err)
	}
	sc, err := storeclient.New(c.Store.Endpoint, c.Store.Pool, storeTO)
	if err != nil {
		return nil, fmt.Errorf("manifest: dial store: %w", err)
	}
	var writer ingest.StoreWriter = sc
	if c.Manifest.WriteGeneration != "" {
		generation := store.Generation(c.Manifest.WriteGeneration)
		if err := store.ValidateGeneration(generation); err != nil {
			_ = sc.Close()
			return nil, fmt.Errorf("manifest: write_generation: %w", err)
		}
		writer = &generationStoreWriter{client: sc, generation: generation}
	}
	ing, err := c.NewIngesterWithWriter(keyFn, extraSaltFn, writer)
	if err != nil {
		_ = sc.Close()
		return nil, err
	}
	return &ingesterCloser{Ingester: ing, store: sc}, nil
}

type generationStoreWriter struct {
	client     *storeclient.Client
	generation store.Generation
}

func (w *generationStoreWriter) AdmitWrite(ctx context.Context) (store.WriteAdmission, error) {
	return w.client.AdmitWriteFor(ctx, w.generation)
}

func (w *generationStoreWriter) Put(ctx context.Context, admission store.WriteAdmission, partition store.Partition, key store.ContentKey, data []byte) (bool, error) {
	return w.client.Put(ctx, admission, partition, key, data)
}

func (w *generationStoreWriter) PoolSize() int { return w.client.PoolSize() }

// NewIngesterWithWriter wires Chunker + Crypto to a caller-owned writer. It is
// used by local containers such as Manifest Bundles; writer lifetime and
// admission acquisition remain the caller's responsibility.
func (c *Config) NewIngesterWithWriter(keyFn ingest.CustomerKeyFunc, extraSaltFn ingest.ExtraSaltFunc, writer ingest.StoreWriter) (ingest.Ingester, error) {
	if writer == nil {
		return nil, fmt.Errorf("manifest: store writer is required")
	}
	chk, err := chunker.New(c.Chunker)
	if err != nil {
		return nil, err
	}
	if maxSize := chk.Info().MaxSize; uint64(maxSize) > uint64(codec.MaxChunkDecodedSize) {
		return nil, fmt.Errorf(
			"manifest: chunker maximum size %d exceeds canonical decoded chunk limit %d",
			maxSize,
			codec.MaxChunkDecodedSize,
		)
	}
	enc, _, err := crypto.New(c.Crypto)
	if err != nil {
		return nil, err
	}
	return ingest.NewIngester(keyFn, extraSaltFn, writer, chk, enc), nil
}

// NewBundleIngester wires a caller-owned multi-Manifest writer with the
// configured customer key and no extra salt. It preserves the original API for
// callers that do not need a narrower derivation domain.
func (c *Config) NewBundleIngester(writer ingest.StoreWriter) (ingest.Ingester, error) {
	return c.NewBundleIngesterWithExtraSalt(writer, nil)
}

// NewBundleIngesterWithExtraSalt wires a caller-owned Bundle writer to the
// same ingest path and extra-salt derivation used by Store output. A Bundle
// records the base WriteAdmission while the authenticated Manifest key table
// records the actual per-Chunk keys, so readers and exact upload do not need
// the extra salt again.
func (c *Config) NewBundleIngesterWithExtraSalt(writer ingest.StoreWriter, extraSaltFn ingest.ExtraSaltFunc) (ingest.Ingester, error) {
	return c.NewIngesterWithWriter(c.IngestKeyFunc(), extraSaltFn, writer)
}

// WriteAdmission resolves the one admission a local multi-object writer must
// reuse. Store RPC failures never fall back to offline derivation.
func (c *Config) WriteAdmission(ctx context.Context) (store.WriteAdmission, error) {
	generation := store.Generation(c.Manifest.WriteGeneration)
	if c.Store.Endpoint == "" {
		if generation == "" {
			generation = "NONE"
		}
		salt, err := store.SaltForGeneration(generation)
		if err != nil {
			return store.WriteAdmission{}, fmt.Errorf("manifest: write_generation: %w", err)
		}
		return store.WriteAdmission{Generation: generation, Salt: salt}, nil
	}
	storeTO, err := parseTimeout(c.Store.Timeout, defaultStoreTimeout)
	if err != nil {
		return store.WriteAdmission{}, fmt.Errorf("manifest: store.timeout: %w", err)
	}
	sc, err := storeclient.New(c.Store.Endpoint, c.Store.Pool, storeTO)
	if err != nil {
		return store.WriteAdmission{}, fmt.Errorf("manifest: dial store: %w", err)
	}
	defer sc.Close()
	if generation == "" {
		return sc.AdmitWrite(ctx)
	}
	return sc.AdmitWriteFor(ctx, generation)
}

// NewFetcher wires Cache (or store fallback) + Crypto into a Fetcher.
// When CacheConfig.Endpoint is non-empty, dial cache-ctl; otherwise
// dial only the store and wrap it via cache.NewStoreOrigin so the
// daemon-less topology still works through the same code path.
//
// Close must be called on the returned FetcherCloser to drain the
// underlying client pools.

func (c *Config) NewFetcher() (FetcherCloser, error) {
	return c.NewFetcherWithOptions(c.fetchOptions())
}

// NewFetcherWithOptions wires the configured remote Getter with an explicit
// verification policy. Explicit verification commands use this to force true.
func (c *Config) NewFetcherWithOptions(opts fetch.Options) (FetcherCloser, error) {
	customerKey, err := c.CustomerKey()
	if err != nil {
		return nil, err
	}
	_, dec, err := crypto.New(c.Crypto)
	if err != nil {
		return nil, err
	}

	if c.Cache.Endpoint != "" {
		cacheTO, err := parseTimeout(c.Cache.Timeout, defaultCacheTimeout)
		if err != nil {
			return nil, fmt.Errorf("manifest: cache.timeout: %w", err)
		}
		cc, err := cacheclient.NewGetter(c.Cache.Endpoint, cacheclient.Options{
			Pool:    c.Cache.Pool,
			Timeout: cacheTO,
		})
		if err != nil {
			return nil, fmt.Errorf("manifest: dial cache: %w", err)
		}
		f := fetch.NewFetcherWithOptions(customerKey, cc, dec, opts)
		return &fetcherCloser{Fetcher: f, closers: []io.Closer{cc}}, nil
	}

	if c.Store.Endpoint == "" {
		return nil, fmt.Errorf("manifest: cache.endpoint or store.endpoint required for fetch")
	}
	storeTO, err := parseTimeout(c.Store.Timeout, defaultStoreTimeout)
	if err != nil {
		return nil, fmt.Errorf("manifest: store.timeout: %w", err)
	}
	sc, err := storeclient.New(c.Store.Endpoint, c.Store.Pool, storeTO)
	if err != nil {
		return nil, fmt.Errorf("manifest: dial store: %w", err)
	}
	getter := cache.NewStoreOrigin(sc)
	f := fetch.NewFetcherWithOptions(customerKey, getter, dec, opts)
	return &fetcherCloser{Fetcher: f, closers: []io.Closer{sc}}, nil
}

// NewFetcherWithGetter wires a caller-owned Getter using the configured
// ordinary-read verification policy. The returned Fetcher does not close the
// Getter.
func (c *Config) NewFetcherWithGetter(getter cache.Getter) (fetch.Fetcher, error) {
	return c.NewFetcherWithGetterOptions(getter, c.fetchOptions())
}

// NewFetcherWithGetterOptions is the explicit-policy form used by local
// containers and mandatory verification operations.
func (c *Config) NewFetcherWithGetterOptions(getter cache.Getter, opts fetch.Options) (fetch.Fetcher, error) {
	if getter == nil {
		return nil, fmt.Errorf("manifest: cache getter is required")
	}
	customerKey, err := c.CustomerKey()
	if err != nil {
		return nil, err
	}
	_, dec, err := crypto.New(c.Crypto)
	if err != nil {
		return nil, err
	}
	return fetch.NewFetcherWithOptions(customerKey, getter, dec, opts), nil
}

type ingesterCloser struct {
	ingest.Ingester
	store io.Closer
}

func (i *ingesterCloser) Close() error { return i.store.Close() }

type fetcherCloser struct {
	fetch.Fetcher
	closers []io.Closer
}

func (f *fetcherCloser) Close() error {
	var firstErr error
	for _, c := range f.closers {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// GetManifestBlob returns the raw bytes of the manifest stored under
// key. Dials the store (or cache, when configured) on each call —
// suitable for one-shot CLI use; callers doing repeated lookups
// should build a Fetcher instead and Close it manually.
//
// The returned bytes are byte-equivalent to what manifest-ctl store
// wrote (Marshal is deterministic for any (manifest, sealedKT) pair),
// so they can be piped into `manifest-ctl info` or saved verbatim.
func (c *Config) GetManifestBlob(ctx context.Context, key store.ContentKey) ([]byte, error) {
	return c.GetManifestBlobWithOptions(ctx, key, c.fetchOptions())
}

// GetManifestBlobWithOptions is the explicit-policy form used by mandatory
// verification operations.
func (c *Config) GetManifestBlobWithOptions(ctx context.Context, key store.ContentKey, opts fetch.Options) ([]byte, error) {
	if c.Cache.Endpoint != "" {
		cacheTO, err := parseTimeout(c.Cache.Timeout, defaultCacheTimeout)
		if err != nil {
			return nil, fmt.Errorf("manifest: cache.timeout: %w", err)
		}
		cc, err := cacheclient.NewGetter(c.Cache.Endpoint, cacheclient.Options{
			Pool:    c.Cache.Pool,
			Timeout: cacheTO,
		})
		if err != nil {
			return nil, fmt.Errorf("manifest: dial cache: %w", err)
		}
		defer cc.Close()
		result, blob, err := cc.Get(ctx, store.PartitionManifest, key)
		if err != nil {
			if blob != nil {
				blob.Release()
			}
			return nil, err
		}
		if result != cache.CacheHit {
			if blob != nil {
				blob.Release()
			}
			return nil, fmt.Errorf("manifest: not found: %s", HexKey(key))
		}
		if blob == nil {
			return nil, fmt.Errorf("manifest: cache hit returned nil blob")
		}
		borrowed := blob.Bytes()
		if opts.VerifyContent {
			if err := verifyManifestContentKey(borrowed, key, "cache"); err != nil {
				blob.Release()
				return nil, err
			}
		}
		data := append([]byte(nil), borrowed...)
		blob.Release()
		return data, nil
	}
	if c.Store.Endpoint == "" {
		return nil, fmt.Errorf("manifest: cache.endpoint or store.endpoint required")
	}
	storeTO, err := parseTimeout(c.Store.Timeout, defaultStoreTimeout)
	if err != nil {
		return nil, fmt.Errorf("manifest: store.timeout: %w", err)
	}
	sc, err := storeclient.New(c.Store.Endpoint, c.Store.Pool, storeTO)
	if err != nil {
		return nil, fmt.Errorf("manifest: dial store: %w", err)
	}
	defer sc.Close()
	found, data, err := sc.Get(ctx, store.PartitionManifest, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("manifest: not found: %s", HexKey(key))
	}
	if opts.VerifyContent {
		if err := verifyManifestContentKey(data, key, "store"); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func verifyManifestContentKey(data []byte, key store.ContentKey, source string) error {
	if sha256.Sum256(data) != key {
		return fmt.Errorf("manifest: content key mismatch (corrupt or tampered %s)", source)
	}
	return nil
}

// CheckManifest verifies a manifest layer is PRESENT and sealed under the
// CURRENT customer key (MANIFEST_KEY) — WITHOUT fetching any chunks. It gets the
// manifest blob (one cache/store round-trip), unmarshals it, and unseals the
// per-chunk key table under CustomerKey(): the AES-GCM unseal authenticates the
// key, so a wrong/inconsistent customer key fails here deterministically.
//
// Returns nil when the layer exists and the key is consistent; a "not found"
// error when the blob is absent; an unseal error when it was sealed under a
// different key. It does NOT read chunks; use a full Fetcher read (the
// manifest-ctl verify path) when chunk presence must also be proven.
func (c *Config) CheckManifest(ctx context.Context, key store.ContentKey) error {
	blob, err := c.GetManifestBlob(ctx, key)
	if err != nil {
		return err
	}
	m, sealedKT, err := codec.Unmarshal(blob)
	if err != nil {
		return fmt.Errorf("manifest %s: parse: %w", HexKey(key), err)
	}
	ck, err := c.CustomerKey()
	if err != nil {
		return err
	}
	_, dec, err := crypto.New(c.Crypto)
	if err != nil {
		return fmt.Errorf("manifest: crypto: %w", err)
	}
	if _, err := codec.UnsealKeys(m, sealedKT, ck, dec); err != nil {
		return fmt.Errorf("manifest %s: key inconsistent (unseal failed under current MANIFEST_KEY): %w", HexKey(key), err)
	}
	return nil
}

// Ensure compile-time that the package's store/codec types are still
// reachable; the alias file (alias.go) re-exports them for callers
// that want a single import.
var (
	_ store.Partition  = store.PartitionChunk
	_ codec.ChunkEntry = codec.ChunkEntry{}
	_ context.Context  = context.TODO()
)
