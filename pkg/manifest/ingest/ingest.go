// Package ingest implements the unified write pipeline for the
// container accelerator.
//
// Pipeline: Reader → Chunk → Encrypt → Store.Put (dedup) → Marshal
// Manifest → Store.Put (manifest partition).
//
// Construct an Ingester via NewIngester; call Ingest per image. The
// returned Result reports the manifest content key already written to
// the store — callers never see the marshaled manifest bytes.
package ingest

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"sync"

	"github.com/fullof-work/mass-sandbox/pkg/manifest/chunker"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/codec"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/crypto"
	"github.com/fullof-work/mass-sandbox/pkg/store"
)

// CustomerKeyFunc supplies the 32-byte customer key used to seal the
// manifest's key table. Function form (not value) so callers can
// resolve from env / vault / etc. and so the value can be discarded
// after sealing.
type CustomerKeyFunc func() ([32]byte, error)

// ExtraSaltFunc supplies optional extra salt bytes that get mixed into
// the convergent salt for this ingest. Returning nil / empty leaves
// the store-supplied generation salt unmodified — the typical case for
// the platform's shared dedup domain. Non-empty bytes scope the dedup
// domain narrower (per-tenant, per-image, etc.).
type ExtraSaltFunc func() ([]byte, error)

// StoreWriter is the narrow write surface the ingester needs from the
// store layer. Two methods:
//
//   - GetSalt returns the current generation ID + 32-byte base salt;
//     called once per Ingest before chunking starts.
//   - Put writes a single chunk or manifest blob keyed by content
//     hash. Returns isNew=true on first write, isNew=false when the
//     content was already present (server-side dedup).
//
// The interface is deliberately narrow so tests can stub it without
// pulling in pkg/store's RPC surface.
type StoreWriter interface {
	GetSalt(ctx context.Context) (generation string, salt [32]byte, err error)
	Put(ctx context.Context, p store.Partition, key store.ContentKey, data []byte) (isNew bool, err error)
}

// IngestOption holds the per-call knobs for Ingest. Zero value is
// valid (no holes, no progress callback).
type IngestOption struct {
	// Holes describes byte ranges of the original image that hold no
	// data — filesystem holes from a sparse file, qcow2 unallocated
	// clusters, TRIM ranges, etc. The chunker never sees these
	// regions; they appear in the manifest as HoleExtent records and
	// are reconstructed by the read path according to caller policy
	// (see fetch.ErrHitHole).
	//
	// When non-empty, r must be an io.ReadSeeker — Ingest seeks past
	// holes to the next data segment instead of reading & discarding.
	// Stdin/pipe input must be wrapped in a temp file by the caller.
	//
	// Holes need not be sorted; Ingest sorts and validates the layout
	// against the image size before consuming.
	Holes []codec.HoleExtent

	// OnProgress, if non-nil, is invoked after each chunk write with
	// the running (processed, total) byte count over the data segments
	// (hole regions do not participate).
	OnProgress func(processed, total uint64)
}

// Result reports the outcome of an Ingest call.
type Result struct {
	// ManifestKey is the content key of the marshaled manifest blob in
	// the store's manifest partition. Callers persist this key as the
	// handle to the ingested image.
	ManifestKey store.ContentKey

	// Generation is the store generation ID that owned the salt used
	// to derive chunk keys. Cross-generation reads must agree on this
	// value; the read path doesn't need it (the manifest already
	// carries every key it needs), but ingest exposes it for tooling.
	Generation string

	// Stats on the work performed.
	StoredChunks uint32 // chunks newly written to the store
	DedupChunks  uint32 // chunks already present (Put returned isNew=false)
	ZeroChunks   uint32 // chunks whose plaintext was all-zero (not stored)
	StoredBytes  uint64 // bytes newly written to the store (ciphertext)
}

// Ingester is the write-side handle. One Ingester per (process,
// customer-key-func, salt-func, store-writer, chunker, encryptor)
// combination; many Ingest calls per Ingester (each independent).
type Ingester interface {
	Ingest(ctx context.Context, r io.Reader, size uint64, opt IngestOption) (*Result, error)
}

// NewIngester wires the four collaborators into an Ingester. None is
// validated here — pass a working chunker.Chunker (built via
// chunker.New) and a working crypto.Encryptor (built via crypto.New).
//
// The customer key is fetched lazily (per Ingest call) via keyFn; the
// caller controls its lifetime. extraSaltFn may be nil — that is the
// "no extra salt" case (platform-wide dedup domain).
func NewIngester(keyFn CustomerKeyFunc, extraSaltFn ExtraSaltFunc, sw StoreWriter, chk chunker.Chunker, enc crypto.Encryptor) Ingester {
	return &ingester{
		keyFn:       keyFn,
		extraSaltFn: extraSaltFn,
		store:       sw,
		chunker:     chk,
		enc:         enc,
	}
}

type ingester struct {
	keyFn       CustomerKeyFunc
	extraSaltFn ExtraSaltFunc
	store       StoreWriter
	chunker     chunker.Chunker
	enc         crypto.Encryptor
}

// Ingest reads the [0, size) image from r, chunks the data segments
// (= [0, size) minus opt.Holes), encrypts each chunk, writes them to
// the store with content-addressed dedup, seals the key table with
// the customer key, marshals the manifest, writes the manifest blob,
// and returns Result.ManifestKey.
//
// When opt.Holes is non-empty, r must be an io.ReadSeeker.
func (i *ingester) Ingest(ctx context.Context, r io.Reader, size uint64, opt IngestOption) (*Result, error) {
	customerKey, err := i.keyFn()
	if err != nil {
		return nil, fmt.Errorf("ingest: customer key: %w", err)
	}

	generation, baseSalt, err := i.store.GetSalt(ctx)
	if err != nil {
		return nil, fmt.Errorf("ingest: get salt: %w", err)
	}
	salt, err := i.mixSalt(baseSalt)
	if err != nil {
		return nil, err
	}

	// Resolve the chunker's mode/bounds for the manifest header. The
	// read path's fast-bounds check (fetch) consults these.
	info := i.chunker.Info()
	var chunkMode codec.ChunkMode
	switch info.Mode {
	case "fixed":
		chunkMode = codec.ChunkModeFixed
	default:
		chunkMode = codec.ChunkModeFastCDC
	}

	m := &codec.Manifest{
		Version:      codec.Version1,
		ChunkMode:    chunkMode,
		ImageSize:    size,
		MinChunkSize: info.MinSize,
		MaxChunkSize: info.MaxSize,
	}

	holes, err := normaliseHoles(opt.Holes, size)
	if err != nil {
		return nil, err
	}
	m.Holes = holes
	dataSegs := dataSegmentsFromHoles(size, holes)
	if len(holes) > 0 {
		if _, ok := r.(io.ReadSeeker); !ok {
			return nil, fmt.Errorf("ingest: opt.Holes is non-empty but reader is not io.ReadSeeker")
		}
	}

	// chunkOutcome is one chunk's record. The (sequential) chunker
	// callback appends these in strict file order; workers fill the
	// store-side fields of their *own* element, so the manifest stays
	// deterministic regardless of store.Put completion order and no
	// lock guards the slice elements (distinct indices).
	type chunkOutcome struct {
		offset uint64
		size   uint32
		isZero bool
		key    [32]byte
		ck     store.ContentKey
		ctLen  uint64
		isNew  bool
	}
	var (
		outcomes []*chunkOutcome
		res      Result
	)

	// Concurrency = the store client's connection-pool size: that is the
	// real parallelism limit (round-robin can only have that many Puts
	// genuinely in flight). Detected via an optional interface so the
	// StoreWriter contract is unchanged; fakes/tests fall back to 1.
	workers := 1
	if pc, ok := i.store.(interface{ PoolSize() int }); ok {
		if n := pc.PoolSize(); n > workers {
			workers = n
		}
	}

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type job struct {
		oc   *chunkOutcome
		data []byte
	}
	// cap == workers: at most `workers` queued + `workers` executing, so
	// the chunker blocks (upstream backpressure, bounded memory) instead
	// of racing ahead and piling up a task queue.
	jobs := make(chan job, workers)
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		putErr   error
		progMu   sync.Mutex
		progDone uint64
	)
	fail := func(e error) { errOnce.Do(func() { putErr = e; cancel() }) }
	emitProgress := func(n uint32) {
		if opt.OnProgress == nil {
			return
		}
		progMu.Lock()
		progDone += uint64(n)
		opt.OnProgress(progDone, size)
		progMu.Unlock()
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if wctx.Err() != nil {
					continue // drain fast; error already recorded
				}
				key := crypto.DeriveKey(salt, j.data)
				ciphertext, _ := i.enc.EncryptChunk(key, j.data)
				ck := store.ContentKey(sha256.Sum256(ciphertext))
				isNew, err := i.store.Put(wctx, store.PartitionChunk, ck, ciphertext)
				if err != nil {
					fail(fmt.Errorf("ingest: store put chunk: %w", err))
					continue
				}
				j.oc.key = key
				j.oc.ck = ck
				j.oc.ctLen = uint64(len(ciphertext))
				j.oc.isNew = isNew
				emitProgress(j.oc.size)
			}
		}()
	}

	var chunkErr error
	for _, seg := range dataSegs {
		if rs, ok := r.(io.ReadSeeker); ok && (len(holes) > 0 || seg.Offset != 0) {
			if _, err := rs.Seek(int64(seg.Offset), io.SeekStart); err != nil {
				chunkErr = fmt.Errorf("ingest: seek to %d: %w", seg.Offset, err)
				break
			}
		}

		// Bound the chunker to this segment; otherwise CDC's read
		// buffer would spill into the next segment's bytes.
		segReader := io.LimitReader(r, int64(seg.Size))
		segOffset := seg.Offset

		err := i.chunker.Chunk(segReader, func(cr chunker.ChunkResult) error {
			if wctx.Err() != nil {
				return wctx.Err() // a worker failed; stop chunking
			}
			oc := &chunkOutcome{
				offset: cr.Offset + segOffset, // chunker emits 0-based offsets per segment
				size:   cr.Size,
			}
			outcomes = append(outcomes, oc)
			if cr.IsZero {
				oc.isZero = true
				emitProgress(cr.Size)
				return nil
			}
			select {
			case jobs <- job{oc: oc, data: cr.Data}:
				return nil
			case <-wctx.Done():
				return wctx.Err()
			}
		})
		if err != nil {
			chunkErr = fmt.Errorf("ingest: chunking segment [%d, %d): %w", seg.Offset, seg.Offset+seg.Size, err)
			break
		}
	}
	close(jobs)
	wg.Wait()
	if putErr != nil {
		return nil, putErr
	}
	if chunkErr != nil {
		return nil, chunkErr
	}

	// Assemble the manifest strictly in file order from outcomes. This
	// is what preserves manifest determinism (same input -> same key)
	// despite out-of-order store.Put completion.
	sparseKeys := make([][32]byte, 0, len(outcomes))
	nonZeroKeys := make([][32]byte, 0, len(outcomes))
	for _, oc := range outcomes {
		var entry codec.ChunkEntry
		entry.Offset = oc.offset
		entry.Size = oc.size
		if oc.isZero {
			entry.IsZero = true
			res.ZeroChunks++
		} else {
			entry.CiphertextHash = oc.ck
			if oc.isNew {
				res.StoredChunks++
				res.StoredBytes += oc.ctLen
			} else {
				res.DedupChunks++
			}
			nonZeroKeys = append(nonZeroKeys, oc.key)
		}
		m.Entries = append(m.Entries, entry)
		sparseKeys = append(sparseKeys, oc.key)
	}

	// Sanity: catch coverage drift before sealing — if it ever
	// triggers, the AAD computed below would not match what Marshal
	// emits.
	if err := m.ValidateGeometry(); err != nil {
		return nil, fmt.Errorf("ingest: %w", err)
	}

	// Flatten only the non-zero keys; zero entries get no slot in the
	// sealed table. UnsealKeys reconstitutes the sparse layout from
	// the entries' IsZero flags.
	flatKeys := make([]byte, len(nonZeroKeys)*32)
	for idx, k := range nonZeroKeys {
		copy(flatKeys[idx*32:(idx+1)*32], k[:])
	}

	sealed, err := i.enc.SealKeyTable(customerKey, flatKeys, codec.BuildAAD(m))
	if err != nil {
		return nil, fmt.Errorf("ingest: seal key table: %w", err)
	}
	m.Keys = sparseKeys

	blob, err := codec.Marshal(m, sealed)
	if err != nil {
		return nil, fmt.Errorf("ingest: marshal manifest: %w", err)
	}
	manifestKey := store.ContentKey(sha256.Sum256(blob))
	if _, err := i.store.Put(ctx, store.PartitionManifest, manifestKey, blob); err != nil {
		return nil, fmt.Errorf("ingest: store put manifest: %w", err)
	}

	res.ManifestKey = manifestKey
	res.Generation = generation
	return &res, nil
}

// mixSalt folds optional extra-salt bytes into the base salt. The
// derivation is salt = SHA256("accelerator-extra-salt-v1" || base ||
// extra); empty extra returns base unchanged so the no-mix path is
// allocation-free.
func (i *ingester) mixSalt(base [32]byte) ([32]byte, error) {
	if i.extraSaltFn == nil {
		return base, nil
	}
	extra, err := i.extraSaltFn()
	if err != nil {
		return [32]byte{}, fmt.Errorf("ingest: extra salt: %w", err)
	}
	if len(extra) == 0 {
		return base, nil
	}
	h := sha256.New()
	h.Write([]byte("accelerator-extra-salt-v1"))
	h.Write(base[:])
	h.Write(extra)
	var mixed [32]byte
	copy(mixed[:], h.Sum(nil))
	return mixed, nil
}

// normaliseHoles returns a sorted, validated copy of the caller's
// holes. Errors out on overlapping holes, holes past ImageSize, or
// zero-size holes. The caller's slice is not mutated.
func normaliseHoles(holes []codec.HoleExtent, size uint64) ([]codec.HoleExtent, error) {
	if len(holes) == 0 {
		return nil, nil
	}
	out := make([]codec.HoleExtent, len(holes))
	copy(out, holes)
	sortHoles(out)
	prevEnd := uint64(0)
	for i, h := range out {
		if h.Size == 0 {
			return nil, fmt.Errorf("ingest: hole at offset %d has zero size", h.Offset)
		}
		if h.Offset < prevEnd {
			return nil, fmt.Errorf("ingest: holes[%d] overlaps with previous (offset %d < %d)", i, h.Offset, prevEnd)
		}
		end := h.Offset + h.Size
		if end > size {
			return nil, fmt.Errorf("ingest: hole [%d, %d) extends past ImageSize %d", h.Offset, end, size)
		}
		prevEnd = end
	}
	return out, nil
}

// sortHoles — local insertion sort to avoid the sort.Slice / reflect
// cost. N is small in practice (a few extents per sparse image).
func sortHoles(s []codec.HoleExtent) {
	for i := 1; i < len(s); i++ {
		x := s[i]
		j := i - 1
		for j >= 0 && s[j].Offset > x.Offset {
			s[j+1] = s[j]
			j--
		}
		s[j+1] = x
	}
}

// dataSegmentsFromHoles produces the inverse of holes within
// [0, size): the contiguous runs that hold actual chunked data. With
// no holes returns a single {0, size} segment, preserving the
// pre-hole code path verbatim.
func dataSegmentsFromHoles(size uint64, holes []codec.HoleExtent) []codec.HoleExtent {
	if len(holes) == 0 {
		if size == 0 {
			return nil
		}
		return []codec.HoleExtent{{Offset: 0, Size: size}}
	}
	var segs []codec.HoleExtent
	cursor := uint64(0)
	for _, h := range holes {
		if h.Offset > cursor {
			segs = append(segs, codec.HoleExtent{Offset: cursor, Size: h.Offset - cursor})
		}
		cursor = h.Offset + h.Size
	}
	if cursor < size {
		segs = append(segs, codec.HoleExtent{Offset: cursor, Size: size - cursor})
	}
	return segs
}
