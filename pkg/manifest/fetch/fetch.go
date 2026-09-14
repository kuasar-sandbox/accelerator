// Package fetch implements the unified read layer for the container accelerator.
//
// A Stream is the read-base abstraction: it reconstructs a virtual image from
// either a chunked + encrypted manifest, a local tarstream artifact, or an
// overlay of several such streams. Stream is sparse.Source plus lifetime — it strengthens the
// Source baseline contract (monotone single-goroutine reads) to full concurrent
// random access, which the runtime consumers (vhost, uffd) rely on. Three
// implementations satisfy it — manifestStream,
// the file stream returned by OpenTarStream, and layeredStream — and a sandbox
// disk, snapshot bundle, or manifest-ctl read all sit on the same abstraction.
//
// Run classification uses sparse.RunKind. Only manifest-backed streams ever
// return sparse.Zero (an IsZero chunk in the serving layer — explicit zero
// data that needs no fetch but does NOT fall through an overlay); file streams
// classify allocated zeros as Data and filesystem holes as Hole.
//
// Manifest Data runs additionally implement ChunkRun, preserving their physical
// chunk identity through layered visibility resolution without exposing chunk
// indices or manifest internals.
//
// Prefetcher is a separate optional enhancement for backend-specific warm-up.
// Keeping prefetch out of Stream and ChunkRun lets each backend expose the
// capability only when it has a meaningful implementation.
//
// RunAt, Run.ReadAt, ReadAt, and Prefetch are safe for concurrent use. Close is
// a lifetime boundary: callers cancel and wait for active operations before
// closing.
package fetch

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// Stream is a sparse.Source with a lifetime, and a stronger contract: RunAt
// and ReadAt are safe for concurrent use at arbitrary offsets, every returned
// Run supports concurrent random ReadAt calls, RunAt requires a non-zero limit,
// and Stream.ReadAt MAY fetch the data chunks in a range concurrently.
type Stream interface {
	sparse.Source

	// Close releases resources owned by this stream (e.g. a file descriptor or
	// decrypted-chunk cache); a layered stream closes its layers.
	// It must not run concurrently with RunAt, ReadAt, or Prefetch.
	Close() error
}

// ChunkRun marks a Data Run whose serving leaf is a physical manifest chunk.
// Partial reads can reuse the stream's bounded decrypted-chunk cache; a cold
// whole-chunk read bypasses it. The unexported method reserves implementations
// to this package.
type ChunkRun interface {
	sparse.Run
	chunkRun()
}

// Prefetcher optionally warms the backend read path for an entire Stream.
// Completion means the backend accepted or completed its own best-effort
// operation; it is not a residency or readiness guarantee.
type Prefetcher interface {
	Prefetch(ctx context.Context) error
}

// prefetchChunkRun is the internal physical-chunk prefetch capability. It only
// waits for the existing cache Get/fill path and does not verify or decrypt.
type prefetchChunkRun interface {
	ChunkRun
	prefetch(ctx context.Context) error
}

var errInvalidRun = errors.New("fetch: invalid run")

// manifestStream is the single-manifest implementation (Stream + Prefetcher).
type manifestStream struct {
	m              *codec.Manifest
	onDemandGetter cache.Getter
	prefetchGetter cache.Getter
	decryptor      crypto.Decryptor
	verifyContent  bool
	keys           [][32]byte // decrypted per-chunk keys, parallel to m.Entries
	chunkCache     *decryptedChunkCache
	// Tests may replace this metadata-only lookup to verify a Run reuses its
	// captured index. Production leaves it nil for the direct codec fast path.
	chunkIndexLookup func([]codec.ChunkEntry, uint64) int
}

// chunkRangeDecryptor is the exceptional capability needed only when an
// oversized chunk bypasses the plaintext cache. Keeping it private avoids
// widening the canonical crypto.Decryptor interface beyond DecryptChunkTo.
type chunkRangeDecryptor interface {
	DecryptChunkRangeTo(ctx context.Context, key [32]byte, ciphertext []byte, plaintextSize, plaintextOffset int, dst []byte) error
}

func newManifestStream(
	m *codec.Manifest,
	keys [][32]byte,
	onDemand, prefetch cache.Getter,
	dec crypto.Decryptor,
) *manifestStream {
	return newManifestStreamWithOptions(m, keys, onDemand, prefetch, dec, Options{VerifyContent: true})
}

func newManifestStreamWithOptions(
	m *codec.Manifest,
	keys [][32]byte,
	onDemand, prefetch cache.Getter,
	dec crypto.Decryptor,
	opts Options,
) *manifestStream {
	return &manifestStream{
		m:              m,
		onDemandGetter: onDemand,
		prefetchGetter: prefetch,
		decryptor:      dec,
		verifyContent:  opts.VerifyContent,
		keys:           keys,
		chunkCache: newDecryptedChunkCache(
			manifestChunkCacheMaxEntries,
			manifestChunkCacheMaxBytes,
			manifestChunkCacheIdleTTL,
		),
	}
}

func (s *manifestStream) Size() uint64 { return s.m.ImageSize }
func (s *manifestStream) Close() error {
	s.chunkCache.close()
	return nil
}

// RunAt resolves one visible forward run. Data remains bounded to one physical
// chunk and captures its index; adjacent Hole or Zero metadata may be merged.
// No payload getter, verification, decrypt, or prefetch work occurs here.
func (s *manifestStream) RunAt(offset, limit uint64) (sparse.Run, error) {
	limEnd, err := boundedRunEnd(s.m.ImageSize, offset, limit)
	if err != nil {
		return nil, err
	}
	if h, in := findHoleAt(s.m.Holes, offset); in {
		end := h.Offset + h.Size
		if end <= offset {
			return nil, fmt.Errorf("%w: hole at %d ends at %d", errInvalidRun, offset, end)
		}
		if end > limEnd {
			end = limEnd
		}
		return newHoleRun(offset, end), nil
	}
	i := s.chunkIndexForOffset(offset)
	if i < 0 {
		return newHoleRun(offset, limEnd), nil // invalid tiling: degrade safely
	}
	e := s.m.Entries[i]
	end := e.Offset + uint64(e.Size)
	if end <= offset {
		return nil, fmt.Errorf("%w: entry %d at %d ends at %d", errInvalidRun, i, offset, end)
	}
	if end > limEnd {
		end = limEnd
	}
	if !e.IsZero {
		return newManifestDataRun(s, uint64(i), offset, end), nil
	}

	// Zero runs carry no physical read capability. Merge only immediately
	// adjacent IsZero entries; a hole or Data entry remains a hard boundary.
	next := i + 1
	for end < limEnd {
		if next >= len(s.m.Entries) {
			break
		}
		e2 := s.m.Entries[next]
		if !e2.IsZero || e2.Offset != end {
			break
		}
		nextEnd := e2.Offset + uint64(e2.Size)
		if nextEnd <= end {
			return nil, fmt.Errorf("%w: entry %d at %d ends at %d", errInvalidRun, next, end, nextEnd)
		}
		end = nextEnd
		if end > limEnd {
			end = limEnd
		}
		next++
	}
	return newZeroRun(offset, end), nil
}

func (s *manifestStream) chunkIndexForOffset(offset uint64) int {
	if s.chunkIndexLookup != nil {
		return s.chunkIndexLookup(s.m.Entries, offset)
	}
	return codec.ChunkIndexForOffset(s.m.Entries, offset)
}

type manifestDataRun struct {
	stream     *manifestStream
	chunkIndex uint64
	offset     uint64
	end        uint64
}

func newManifestDataRun(stream *manifestStream, chunkIndex, offset, end uint64) *manifestDataRun {
	r := dataRunPool.Get().(*manifestDataRun)
	*r = manifestDataRun{stream: stream, chunkIndex: chunkIndex, offset: offset, end: end}
	return r
}

func (r *manifestDataRun) Offset() uint64       { return r.offset }
func (r *manifestDataRun) End() uint64          { return r.end }
func (r *manifestDataRun) Kind() sparse.RunKind { return sparse.Data }
func (r *manifestDataRun) chunkRun()            {}

func (r *manifestDataRun) ReadAt(ctx context.Context, buf []byte, innerOffset uint64) (int, error) {
	if err := validateRunRead(r, innerOffset, len(buf)); err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return r.stream.readChunkAt(ctx, buf, r.chunkIndex, r.offset+innerOffset)
}

func (r *manifestDataRun) prefetch(ctx context.Context) error {
	blob, err := r.stream.loadChunkAt(ctx, r.chunkIndex, loadPrefetch)
	if err != nil {
		return err
	}
	if blob != nil {
		blob.Release()
	}
	return nil
}

func (r *manifestDataRun) release() {
	*r = manifestDataRun{}
	dataRunPool.Put(r)
}

var _ ChunkRun = (*manifestDataRun)(nil)
var _ prefetchChunkRun = (*manifestDataRun)(nil)

type loadKind uint8

const (
	loadOnDemand loadKind = iota
	loadPrefetch
)

// loadChunkAt owns all cache-result validation. A successful non-nil Blob is
// transferred to the caller, which must Release it exactly once. Every Blob
// returned on an error or non-hit path is released here.
func (s *manifestStream) loadChunkAt(ctx context.Context, chunkIdx uint64, kind loadKind) (cache.Blob, error) {
	if chunkIdx >= uint64(len(s.m.Entries)) {
		return nil, fmt.Errorf("fetch: chunk index %d out of range", chunkIdx)
	}
	e := s.m.Entries[chunkIdx]
	if e.IsZero {
		return nil, nil
	}
	var getter cache.Getter
	switch kind {
	case loadOnDemand:
		getter = s.onDemandGetter
	case loadPrefetch:
		getter = s.prefetchGetter
	default:
		return nil, fmt.Errorf("fetch: chunk %d: invalid load kind %d", chunkIdx, kind)
	}
	if getter == nil {
		return nil, fmt.Errorf("fetch: chunk %d: cache getter is nil", chunkIdx)
	}
	result, blob, err := getter.Get(ctx, store.PartitionChunk, store.ContentKey(e.CiphertextHash))
	if err != nil {
		if blob != nil {
			blob.Release()
		}
		return nil, fmt.Errorf("chunk %d: %w", chunkIdx, err)
	}
	if result != cache.CacheHit {
		if blob != nil {
			blob.Release()
		}
		return nil, fmt.Errorf("chunk %d: not found", chunkIdx)
	}
	if blob == nil {
		return nil, fmt.Errorf("chunk %d: cache hit returned nil blob", chunkIdx)
	}
	return blob, nil
}

// readChunkAt copies one sub-range from the already-resolved chunk index.
// Partial reads share a bounded plaintext cache; cold whole-chunk reads and
// chunks larger than the byte budget retain the direct path.
func (s *manifestStream) readChunkAt(ctx context.Context, buf []byte, chunkIdx, offset uint64) (int, error) {
	if chunkIdx >= uint64(len(s.m.Entries)) {
		return 0, fmt.Errorf("fetch: chunk index %d out of range", chunkIdx)
	}
	e := s.m.Entries[chunkIdx]
	entryEnd := e.Offset + uint64(e.Size)
	if entryEnd < e.Offset {
		return 0, fmt.Errorf("fetch: chunk %d range overflows", chunkIdx)
	}
	if uint64(len(buf)) > ^uint64(0)-offset {
		return 0, fmt.Errorf("fetch: chunk %d: read offset %d length %d overflows", chunkIdx, offset, len(buf))
	}
	end := offset + uint64(len(buf))
	if offset < e.Offset || end > entryEnd {
		return 0, fmt.Errorf("fetch: chunk %d: range [%d,%d) outside entry [%d,%d)", chunkIdx, offset, end, e.Offset, entryEnd)
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if uint64(e.Size) > uint64(crypto.MaxChunkDecodedSize) {
		return 0, fmt.Errorf("fetch: chunk %d: plaintext size %d exceeds hard limit %d", chunkIdx, e.Size, crypto.MaxChunkDecodedSize)
	}
	if e.IsZero {
		clearSlice(buf)
		return len(buf), nil
	}
	if chunkIdx >= uint64(len(s.keys)) {
		return 0, fmt.Errorf("fetch: chunk %d: missing decryption key", chunkIdx)
	}
	if s.decryptor == nil {
		return 0, fmt.Errorf("fetch: chunk %d: decryptor is nil", chunkIdx)
	}
	key := decryptedChunkKey{
		ciphertextHash: e.CiphertextHash,
		decryptKey:     s.keys[chunkIdx],
		plaintextSize:  e.Size,
	}
	if lease := s.chunkCache.acquire(key); lease != nil {
		defer lease.release()
		return copyChunkRange(buf, lease.bytes(), chunkIdx, e, offset, end)
	}
	if (offset == e.Offset && uint64(len(buf)) == uint64(e.Size)) || uint64(e.Size) > manifestChunkCacheMaxBytes {
		return s.readChunkDirect(ctx, buf, chunkIdx, e, offset)
	}

	lease, err := s.chunkCache.acquireOrLoad(ctx, key, func(ctx context.Context) ([]byte, error) {
		return s.loadOwnedPlainChunk(ctx, chunkIdx, e)
	})
	if err != nil {
		return 0, err
	}
	defer lease.release()
	return copyChunkRange(buf, lease.bytes(), chunkIdx, e, offset, end)
}

func (s *manifestStream) readChunkDirect(
	ctx context.Context,
	buf []byte,
	chunkIdx uint64,
	e codec.ChunkEntry,
	offset uint64,
) (int, error) {
	blob, err := s.loadChunkAt(ctx, chunkIdx, loadOnDemand)
	if err != nil {
		return 0, err
	}
	defer blob.Release()
	ciphertext := blob.Bytes()

	if s.decryptor.Mode() == "aes" {
		// The content key is SHA256(ciphertext) and is authenticated by the key
		// table's AAD, so verifying the returned bytes against it rejects a corrupt
		// or tampered chunk before the unauthenticated AES-CTR decrypt would turn
		// attacker-chosen ciphertext into attacker-chosen plaintext. Hash the bytes
		// as received and keep the borrowed Blob bytes immutable throughout decode.
		if s.verifyContent && sha256.Sum256(ciphertext) != e.CiphertextHash {
			return 0, fmt.Errorf("chunk %d: ciphertext hash mismatch (corrupt or tampered store/cache)", chunkIdx)
		}
	}

	if offset == e.Offset && uint64(len(buf)) == uint64(e.Size) {
		if err := s.decryptor.DecryptChunkTo(ctx, s.keys[chunkIdx], ciphertext, buf); err != nil {
			return 0, fmt.Errorf("chunk %d: decrypt: %w", chunkIdx, err)
		}
		return len(buf), nil
	}

	innerOffset := offset - e.Offset
	if s.decryptor.Mode() == "aes" {
		rangeDecryptor, ok := s.decryptor.(chunkRangeDecryptor)
		if !ok {
			return 0, fmt.Errorf("chunk %d: decryptor cannot serve an oversized partial range", chunkIdx)
		}
		if innerOffset > uint64(^uint(0)>>1) || uint64(e.Size) > uint64(^uint(0)>>1) {
			return 0, fmt.Errorf("chunk %d: plaintext offset %d overflows int", chunkIdx, innerOffset)
		}
		if err := rangeDecryptor.DecryptChunkRangeTo(ctx, s.keys[chunkIdx], ciphertext, int(e.Size), int(innerOffset), buf); err != nil {
			return 0, fmt.Errorf("chunk %d: range decrypt: %w", chunkIdx, err)
		}
		return len(buf), nil
	}

	// AES-GCM cannot independently authenticate an arbitrary ciphertext range.
	// Authenticate and decrypt the complete chunk first, then copy the requested
	// plaintext range into the caller's buffer.
	if s.decryptor.Mode() == "aes-gcm" {
		plain := make([]byte, e.Size)
		if err := s.decryptor.DecryptChunkTo(ctx, s.keys[chunkIdx], ciphertext, plain); err != nil {
			return 0, fmt.Errorf("chunk %d: decrypt: %w", chunkIdx, err)
		}
		copy(buf, plain[int(innerOffset):int(innerOffset)+len(buf)])
		return len(buf), nil
	}

	return 0, fmt.Errorf("chunk %d: unknown decryptor mode %s", chunkIdx, s.decryptor.Mode())
}

// loadOwnedPlainChunk allocates exactly one plaintext-sized cache buffer and
// keeps the immutable Blob alive while the decryptor writes directly into it.
func (s *manifestStream) loadOwnedPlainChunk(ctx context.Context, chunkIdx uint64, e codec.ChunkEntry) ([]byte, error) {
	blob, err := s.loadChunkAt(ctx, chunkIdx, loadOnDemand)
	if err != nil {
		return nil, err
	}
	ciphertext := blob.Bytes()
	if s.verifyContent && sha256.Sum256(ciphertext) != e.CiphertextHash {
		blob.Release()
		return nil, fmt.Errorf("chunk %d: ciphertext hash mismatch (corrupt or tampered store/cache)", chunkIdx)
	}
	plain := make([]byte, e.Size)
	err = s.decryptor.DecryptChunkTo(ctx, s.keys[chunkIdx], ciphertext, plain)
	blob.Release()
	if err != nil {
		clearSlice(plain)
		return nil, fmt.Errorf("chunk %d: decrypt: %w", chunkIdx, err)
	}
	return plain, nil
}

func copyChunkRange(
	buf, plain []byte,
	chunkIdx uint64,
	e codec.ChunkEntry,
	offset, end uint64,
) (int, error) {
	if uint64(len(plain)) < uint64(e.Size) {
		return 0, fmt.Errorf("chunk %d: decrypted data has %d bytes, need %d", chunkIdx, len(plain), e.Size)
	}
	lo := int(offset - e.Offset)
	hi := int(end - e.Offset)
	copy(buf, plain[lo:hi])
	return len(buf), nil
}

func (s *manifestStream) Prefetch(ctx context.Context) error {
	return prefetchStream(ctx, s)
}

// ReadAt walks the runs and fetches data chunks concurrently — one goroutine
// per Data run, each writing directly into its buf slice; Hole/Zero are
// zero-filled in place. ctx cancel on early return releases in-flight blobs.
func (s *manifestStream) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	return readStreamAt(ctx, s, buf, offset)
}

func boundedRunEnd(size, offset, limit uint64) (uint64, error) {
	if offset >= size {
		return 0, io.EOF
	}
	if limit == 0 {
		return 0, fmt.Errorf("%w: zero limit at offset %d", errInvalidRun, offset)
	}
	if limit >= size-offset {
		return size, nil
	}
	return offset + limit, nil
}

// findHoleAt returns the hole extent containing offset, or ok=false. The hole
// slice is sorted by Offset and disjoint. O(log M). (manifestStream owns its
// lookup — the manifest's holes live next to chunk entries, not behind a
// sparse source.)
func findHoleAt(holes []sparse.Extent, offset uint64) (sparse.Extent, bool) {
	lo, hi := 0, len(holes)-1
	idx := -1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		if holes[mid].Offset <= offset {
			idx = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if idx < 0 {
		return sparse.Extent{}, false
	}
	h := holes[idx]
	if offset < h.Offset+h.Size {
		return h, true
	}
	return sparse.Extent{}, false
}

// clearSlice zeros every byte in b (the compiler lowers this to memclr).
func clearSlice(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
