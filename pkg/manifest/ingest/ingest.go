// Package ingest implements the unified write pipeline for the
// container accelerator.
//
// Pipeline: sparse.Source → Chunk → Encrypt → Store.Put (dedup) →
// Marshal Manifest → Store.Put (manifest partition).
//
// The input is a sparse.Source: its Hole runs become the manifest's
// hole extents (never chunked, never read), its Zero runs are fed to
// the chunker as synthesized zero bytes without calling ReadAt (the
// fetch is free, but the bytes are data — chunk boundaries and IsZero
// classification are identical to reading literal zeros, so the
// manifest key is a pure function of content + holes, independent of
// the source kind). Data is consumed in one strictly-forward pass, so
// one-pass sources (sparse.Dense over a pipe, tarstream.SourceFrom)
// ingest without temp files.
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

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/delta"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/internal/objectformat"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// CustomerKeyFunc supplies the 32-byte customer key used to seal the
// manifest's key table. Function form (not value) so callers can
// resolve from env / vault / etc. and so the value can be discarded
// after sealing.
type CustomerKeyFunc func() ([32]byte, error)

// ExtraSaltFunc supplies optional extra salt bytes that get mixed into
// the convergent salt for this ingest. Returning nil / empty leaves
// the store-supplied salt unmodified — the typical case for
// the platform's shared dedup domain. Non-empty bytes scope the dedup
// domain narrower (per-tenant, per-image, etc.).
type ExtraSaltFunc func() ([]byte, error)

// StoreWriter is the narrow write surface the ingester needs from the
// store layer. Two methods:
//
//   - AdmitWrite returns one generation-bound salt; called once per Ingest.
//   - Put writes a single chunk or manifest blob keyed by content
//     hash. Returns isNew=true on first write, isNew=false when the
//     content was already present (server-side dedup).
//
// The interface is deliberately narrow so tests can stub it without
// pulling in pkg/store's RPC surface.
type StoreWriter interface {
	AdmitWrite(ctx context.Context) (store.WriteAdmission, error)
	Put(ctx context.Context, admission store.WriteAdmission, p store.Partition, key store.ContentKey, data []byte) (isNew bool, err error)
}

// OrderedChunkStoreWriter is an optional local-container capability. Ingest
// still encrypts chunks concurrently, but supplies each non-zero chunk's
// logical ordinal so a serial writer can append physical objects in image
// order with bounded backpressure. Remote Store writers need not implement it.
type OrderedChunkStoreWriter interface {
	StoreWriter
	PutChunkOrdered(ctx context.Context, admission store.WriteAdmission, key store.ContentKey, data []byte, ordinal uint64) (isNew bool, err error)
}

// IngestOption holds the per-call knobs for Ingest. Zero value is
// valid.
type IngestOption struct {
	// OnProgress, if non-nil, is invoked after each chunk with the
	// running (processed, total) byte count. Zero runs count when
	// their synthesized chunks complete; hole regions do not
	// participate (total is the full image size, so a sparse image
	// finishes below 100% of total — callers wanting an effective
	// denominator subtract the hole bytes).
	OnProgress func(processed, total uint64)
}

// Result reports the outcome of an Ingest call.
type Result struct {
	// ManifestKey is the content key of the marshaled manifest blob in
	// the store's manifest partition. Callers persist this key as the
	// handle to the ingested image.
	ManifestKey store.ContentKey

	// Stats on the work performed.
	StoredChunks uint32 // chunks newly written to the store
	DedupChunks  uint32 // chunks already present (Put returned isNew=false)
	ZeroChunks   uint32 // chunks whose plaintext was all-zero (not stored)
	StoredBytes  uint64 // bytes newly written to the store (physical chunks + manifest)

	// RawChunks and CompressedChunks count every non-hole, non-zero chunk,
	// including chunks whose physical object is already present in the store.
	RawChunks        uint32
	CompressedChunks uint32
	// LogicalChunkBytes is original plaintext; EncodedChunkBytes is the selected
	// payload before the format byte and encryption. Both include dedup hits.
	LogicalChunkBytes     uint64
	EncodedChunkBytes     uint64
	CompressionSavedBytes uint64 // LogicalChunkBytes - EncodedChunkBytes

	// Delta compression (all zero unless delta is enabled). A chunk stored as a
	// delta is still counted under RawChunks/CompressedChunks by the independent
	// encoding it would otherwise have had, so Raw+Compressed still equals the
	// number of non-zero chunks; DeltaChunks says how many of those were stored
	// as deltas instead. StoredChunks/DedupChunks/StoredBytes describe the
	// object actually written, delta or not.
	DeltaChunks       uint32 // chunks stored as delta objects, newly written or already present
	DeltaLogicalBytes uint64 // plaintext bytes of those chunks
	DeltaSavedBytes   uint64 // independent object bytes minus delta object bytes, summed
	DeltaRepHits      uint32 // of DeltaChunks, resolved from the representation index
	DeltaAttempts     uint32 // chunks for which at least one candidate base was delta-encoded
	DeltaRejected     uint32 // attempts where no delta met the acceptance rule
	DeltaNoCandidate  uint32 // eligible chunks with no similar base available
	DeltaNotResident  uint32 // similar bases found but whose plaintext was not in the pool
	DeltaKnownSkips   uint32 // chunks skipped because already stored independently
	DeltaTimeouts     uint32 // attempts cut short by the per-chunk deadline
	DeltaErrors       uint32 // unexpected failures that fell back to independent objects
	DeltaReadbacks    uint32 // bases fetched from the store because they were not in the pool

	// Cross-node coordination (see store.PartitionAlias). The store keeps one
	// write-once alias per delta'd chunk so replicas that never exchange state
	// still record the same representation.
	DeltaResolves         uint32 // Resolve calls made
	DeltaAdopted          uint32 // chunks that took another writer's verified delta object
	DeltaAdoptFailed      uint32 // aliases that could not be adopted (unusable, or could not be checked)
	DeltaAliasWon         uint32 // delta objects uploaded that became the chunk's representation
	DeltaAliasLost        uint32 // delta objects uploaded that lost to another writer's
	DeltaAliasIndependent uint32 // delta objects uploaded for a chunk that turned out to exist whole (older servers only)
	DeltaOrphanBytes      uint64 // bytes of uploaded delta objects that nothing references and that remain in the store
	DeltaDiscardedBytes   uint64 // bytes of uploaded delta objects that lost the alias race and the store deleted again

	ManifestLogicalBytes uint64 // decoded logical manifest, including magic
	ManifestStoredBytes  uint64 // physical envelope size, including magic + encoding
	ManifestCompressed   bool
}

// Ingester is the write-side handle. One Ingester per (process,
// customer-key-func, salt-func, store-writer, chunker, encryptor)
// combination; many Ingest calls per Ingester (each independent).
type Ingester interface {
	Ingest(ctx context.Context, src sparse.Source, opt IngestOption) (*Result, error)
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
	delta       *deltaRuntime // nil = delta compression off
}

// Ingest consumes src in one forward pass: Hole runs are recorded as
// manifest hole extents and skipped; Zero and Data runs form the
// hole-bounded data segments that are chunked, encrypted and written
// to the store with content-addressed dedup. The key table is sealed
// with the customer key, the manifest marshaled and written, and
// Result.ManifestKey returned.
func (i *ingester) Ingest(ctx context.Context, src sparse.Source, opt IngestOption) (*Result, error) {
	customerKey, err := i.keyFn()
	if err != nil {
		return nil, fmt.Errorf("ingest: customer key: %w", err)
	}

	admission, err := i.store.AdmitWrite(ctx)
	if err != nil {
		return nil, fmt.Errorf("ingest: admit write: %w", err)
	}
	salt, err := i.mixSalt(admission.Salt)
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

	size := src.Size()
	m := &codec.Manifest{
		Version:      codec.Version1,
		ChunkMode:    chunkMode,
		ImageSize:    size,
		MinChunkSize: info.MinSize,
		MaxChunkSize: info.MaxSize,
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
		encLen uint64
		format byte
		isNew  bool

		// Delta: isDelta marks a chunk whose stored object is a delta object;
		// repHit that it was already stored (no Put); deltaSaved is independent
		// object size minus delta object size; baseKey/baseSize name the base
		// for the manifest's delta table.
		isDelta    bool
		repHit     bool
		deltaSaved uint64
		baseKey    [32]byte
		baseSize   uint32
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

	var (
		dom      *delta.Domain
		counters deltaCounters
	)
	if i.delta != nil {
		dom = i.delta.eng.Domain(salt)
	}

	type job struct {
		oc      *chunkOutcome
		data    []byte
		ordinal uint64
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
				ciphertext, hash, key, err := i.enc.EncryptChunk(wctx, salt, j.data)
				if err != nil {
					fail(fmt.Errorf("ingest: encrypt chunk: %w", err))
					continue
				}
				if err := validateEncryptedChunkObject(ciphertext, j.oc.size); err != nil {
					fail(fmt.Errorf("ingest: encrypt chunk: %w", err))
					continue
				}
				// ciphertext/hash/key describe the independent object. When delta
				// applies, a different object is stored and recorded instead.
				stored, storedHash, storedKey := ciphertext, hash, key
				var dout deltaOutcome
				if dom != nil {
					dout = i.delta.tryDelta(wctx, dom, admission.Generation, salt, j.data, ciphertext, hash, &counters)
					if wctx.Err() != nil {
						fail(wctx.Err())
						continue
					}
				}

				var (
					isNew bool
					dp    deltaPut
				)
				// putIndependent stores the independent object through whichever
				// path this writer wants; the delta fallback uses it too.
				putIndependent := func(ctx context.Context) (bool, error) {
					ck := store.ContentKey(hash)
					if ordered, ok := i.store.(OrderedChunkStoreWriter); ok {
						return ordered.PutChunkOrdered(ctx, admission, ck, ciphertext, j.ordinal)
					}
					return i.store.Put(ctx, admission, store.PartitionChunk, ck, ciphertext)
				}
				switch {
				case dout.repHit:
					// Already stored as a delta: nothing to write.
					storedHash, storedKey = dout.rep.DeltaObjectKey, dout.key
					j.oc.ctLen = uint64(dout.rep.ObjectSize)
					j.oc.isDelta, j.oc.repHit = true, true
					j.oc.deltaSaved = savedBytes(len(ciphertext), uint64(dout.rep.ObjectSize))
					j.oc.baseKey, j.oc.baseSize = dout.rep.BaseObjectKey, dout.rep.BaseSize
				case dout.object != nil:
					// A new delta object. The store decides whether it becomes the
					// chunk's representation; the manifest records what it decided.
					dp, err = i.delta.putDelta(wctx, dom, admission, salt, &dout, j.data, ciphertext, hash, key, putIndependent, &counters)
					if err != nil {
						fail(fmt.Errorf("ingest: store put chunk: %w", err))
						continue
					}
					storedHash, storedKey = dp.hash, dp.key
					j.oc.isDelta = dp.isDelta
					if dp.isDelta {
						j.oc.deltaSaved = savedBytes(len(ciphertext), uint64(dp.objectSize))
						j.oc.baseKey, j.oc.baseSize = dp.rep.BaseObjectKey, dp.rep.BaseSize
					}
					isNew = dp.isNew
					j.oc.ctLen = dp.written
				default:
					isNew, err = putIndependent(wctx)
					if err != nil {
						fail(fmt.Errorf("ingest: store put chunk: %w", err))
						continue
					}
					j.oc.ctLen = uint64(len(stored))
				}
				j.oc.key = storedKey
				j.oc.ck = store.ContentKey(storedHash)
				j.oc.encLen = uint64(len(ciphertext) - 1) // independent payload, so compression stats keep their meaning
				j.oc.format = ciphertext[0]
				j.oc.isNew = isNew
				emitProgress(j.oc.size)

				// Only after the object is safely stored, feed the soft state.
				// Registering a base hands j.data to the pool, so it comes last.
				if dom != nil && !dout.repHit {
					if dout.object == nil || dp.independent {
						i.delta.afterIndependentPut(dom, dout, hash, key, j.data)
					}
					// A stored delta object's representation was recorded by putDelta.
				}
			}
		}()
	}

	// One forward pass over src's runs: holes are recorded and
	// skipped; contiguous Zero|Data runs form one segment, chunked as
	// a unit so chunk boundaries match a literal byte stream bounded
	// only by holes.
	var chunkErr error
	cursor := uint64(0)
	var chunkOrdinal uint64
	for cursor < size && chunkErr == nil {
		run, err := src.RunAt(cursor, size-cursor)
		if err != nil {
			chunkErr = fmt.Errorf("ingest: classify @ %d: %w", cursor, err)
			break
		}
		kind, end := run.Kind(), run.End()
		if kind == sparse.Hole {
			if n := len(m.Holes); n > 0 && m.Holes[n-1].Offset+m.Holes[n-1].Size == cursor {
				m.Holes[n-1].Size += end - cursor
			} else {
				m.Holes = append(m.Holes, sparse.Extent{Offset: cursor, Size: end - cursor})
			}
			cursor = end
			continue
		}

		// Extend the segment through every contiguous non-hole run
		// (pure metadata queries — no data is consumed).
		segStart := cursor
		for cursor < size {
			run, err := src.RunAt(cursor, size-cursor)
			if err != nil {
				chunkErr = fmt.Errorf("ingest: classify @ %d: %w", cursor, err)
				break
			}
			if run.Kind() == sparse.Hole {
				break
			}
			cursor = run.End()
		}
		if chunkErr != nil {
			break
		}

		segOffset := segStart
		err = i.chunker.Chunk(&segReader{ctx: wctx, src: src, cur: segStart, end: cursor}, func(cr chunker.ChunkResult) error {
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
			case jobs <- job{oc: oc, data: cr.Data, ordinal: chunkOrdinal}:
				chunkOrdinal++
				return nil
			case <-wctx.Done():
				return wctx.Err()
			}
		})
		if err != nil {
			chunkErr = fmt.Errorf("ingest: chunking segment [%d, %d): %w", segStart, cursor, err)
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
			res.LogicalChunkBytes += uint64(oc.size)
			res.EncodedChunkBytes += oc.encLen
			switch oc.format {
			case crypto.ChunkFormatAESRaw:
				res.RawChunks++
			case crypto.ChunkFormatAESSnappy:
				res.CompressedChunks++
			}
			if oc.isDelta {
				res.DeltaChunks++
				res.DeltaLogicalBytes += uint64(oc.size)
				res.DeltaSavedBytes += oc.deltaSaved
				// A delta entry makes this a Version2 manifest: the entry is
				// flagged and the delta table names its base, so prefetch and
				// dependency tooling need not open the object to learn it.
				entry.IsDelta = true
				m.Version = codec.Version2
				m.Deltas = append(m.Deltas, codec.DeltaRef{
					Entry:    uint32(len(m.Entries)),
					BaseSize: oc.baseSize,
					Base:     oc.baseKey,
				})
			}
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
	res.ManifestLogicalBytes = uint64(codec.HeaderSize) + uint64(len(m.Entries))*codec.EntrySize + uint64(len(m.Holes))*codec.HoleSize + uint64(len(m.Deltas))*codec.DeltaRefSize + uint64(len(sealed))
	res.ManifestStoredBytes = uint64(len(blob))
	res.ManifestCompressed = len(blob) >= 5 && blob[4] == codec.ManifestEncodingSnappy
	res.CompressionSavedBytes = res.LogicalChunkBytes - res.EncodedChunkBytes

	manifestKey := store.ContentKey(sha256.Sum256(blob))
	manifestIsNew, err := i.store.Put(ctx, admission, store.PartitionManifest, manifestKey, blob)
	if err != nil {
		return nil, fmt.Errorf("ingest: store put manifest: %w", err)
	}
	if manifestIsNew {
		res.StoredBytes += uint64(len(blob))
	}

	res.ManifestKey = manifestKey

	if i.delta != nil {
		res.DeltaRepHits = counters.repHits.Load()
		res.DeltaAttempts = counters.attempts.Load()
		res.DeltaRejected = counters.rejected.Load()
		res.DeltaNoCandidate = counters.noCandidate.Load()
		res.DeltaNotResident = counters.notResident.Load()
		res.DeltaKnownSkips = counters.knownSkips.Load()
		res.DeltaTimeouts = counters.timeouts.Load()
		res.DeltaErrors = counters.encodeErrors.Load() + counters.repStale.Load()
		res.DeltaReadbacks = counters.readbacks.Load()
		res.DeltaResolves = counters.resolves.Load()
		res.DeltaAdopted = counters.adopted.Load()
		res.DeltaAdoptFailed = counters.adoptFailed.Load()
		res.DeltaAliasWon = counters.aliasWon.Load()
		res.DeltaAliasLost = counters.aliasLost.Load()
		res.DeltaAliasIndependent = counters.aliasIndep.Load()
		res.DeltaOrphanBytes = counters.orphanBytes.Load()
		res.DeltaDiscardedBytes = counters.discardedBytes.Load()
		i.delta.flush()
	}
	return &res, nil
}

func validateEncryptedChunkObject(object []byte, plaintextSize uint32) error {
	if len(object) == 0 {
		return fmt.Errorf("empty physical object")
	}
	encodedSize := uint64(len(object) - 1)
	switch object[0] {
	case crypto.ChunkFormatAESRaw:
		if encodedSize != uint64(plaintextSize) {
			return fmt.Errorf("RAW payload size %d, want %d", encodedSize, plaintextSize)
		}
	case crypto.ChunkFormatAESSnappy:
		if !objectformat.CompressionBeneficial(uint64(plaintextSize), encodedSize) {
			return fmt.Errorf("Snappy payload size %d does not satisfy canonical benefit rule for %d plaintext bytes", encodedSize, plaintextSize)
		}
	case crypto.ChunkFormatAESDelta:
		return validateDeltaObject(object, plaintextSize)
	default:
		return fmt.Errorf("unknown physical format 0x%02x", object[0])
	}
	return nil
}

// savedBytes is independent minus delta object size, floored at zero.
func savedBytes(independent int, deltaObject uint64) uint64 {
	if uint64(independent) <= deltaObject {
		return 0
	}
	return uint64(independent) - deltaObject
}

// segReader feeds one hole-bounded segment [cur, end) of a
// sparse.Source to the chunker as a plain io.Reader: Data runs are
// read from the source (strictly forward — one-pass sources work),
// Zero runs are synthesized without touching it. The chunker sees the
// exact byte stream it would see reading literal zeros, so chunk
// boundaries and IsZero classification are source-independent.
type segReader struct {
	ctx      context.Context
	src      sparse.Source
	cur, end uint64
	run      sparse.Run
}

func (r *segReader) Read(p []byte) (int, error) {
	if r.cur >= r.end {
		return 0, io.EOF
	}
	if r.run == nil || r.cur >= r.run.End() {
		run, err := r.src.RunAt(r.cur, r.end-r.cur)
		if err != nil {
			return 0, err
		}
		if run.Kind() == sparse.Hole {
			return 0, fmt.Errorf("ingest: hole @ %d inside a data segment (inconsistent RunAt)", r.cur)
		}
		r.run = run
	}
	n := len(p)
	if rest := r.run.End() - r.cur; rest < uint64(n) {
		n = int(rest)
	}
	if r.run.Kind() == sparse.Zero {
		clear(p[:n])
	} else {
		m, err := r.run.ReadAt(r.ctx, p[:n], r.cur-r.run.Offset())
		if err != nil {
			return 0, err
		}
		if m < n {
			return 0, fmt.Errorf("ingest: source ended early (%d of %d bytes @ %d)", m, n, r.cur)
		}
	}
	r.cur += uint64(n)
	return n, nil
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
