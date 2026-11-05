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
	"log"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
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

// OrderedChunkRequirement is an optional capability for writers that implement
// OrderedChunkStoreWriter but do not always require ordinal sequencing.
// Returning false lets ingest use StoreWriter.Put and coalesce more aggressively.
type OrderedChunkRequirement interface {
	RequiresOrderedChunks() bool
}

// DedupIndexValidator optionally confirms that a persistent dedup index marker
// still names a live store object. Used to discard an index after a store wipe
// or reinitialization.
type DedupIndexValidator interface {
	ValidateDedupIndexMarker(ctx context.Context, marker store.ContentKey) (bool, error)
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
	return NewIngesterWithCacheLimit(keyFn, extraSaltFn, sw, chk, enc, defaultChunkDedupCacheLimit)
}

// NewIngesterWithCacheLimit wires collaborators into an Ingester with an explicit
// bounded chunk deduplication index limit.
func NewIngesterWithCacheLimit(keyFn CustomerKeyFunc, extraSaltFn ExtraSaltFunc, sw StoreWriter, chk chunker.Chunker, enc crypto.Encryptor, cacheLimit int) Ingester {
	return &ingester{
		keyFn:        keyFn,
		extraSaltFn:  extraSaltFn,
		store:        sw,
		chunker:      chk,
		enc:          enc,
		dedupCache:   newChunkDedupCache(cacheLimit),
		dedupFlights: make(map[[32]byte]*chunkFlight),
		dedupPath:    persistentDedupPathForWriter(sw),
	}
}

type ingester struct {
	keyFn       CustomerKeyFunc
	extraSaltFn ExtraSaltFunc
	store       StoreWriter
	chunker     chunker.Chunker
	enc         crypto.Encryptor

	dedupMu      sync.Mutex
	dedupCache   *chunkDedupCache
	dedupFlights map[[32]byte]*chunkFlight
	dedupPath    string
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
	_, implementsOrderedWriter := i.store.(OrderedChunkStoreWriter)
	isOrderedWriter := implementsOrderedWriter
	if ordering, ok := i.store.(OrderedChunkRequirement); ok && !ordering.RequiresOrderedChunks() {
		isOrderedWriter = false
	}
	orderedSkipper, canSkipOrdered := i.store.(OrderedChunkSkipper)

	var persistent *persistentChunkIndex
	persistReset := false
	if i.dedupPath != "" {
		var err error
		persistent, err = openPersistentChunkIndexRead(i.dedupPath)
		if err != nil {
			log.Printf("ingest: persistent dedup index unavailable: %v", err)
			persistReset = true
		}
		if persistent != nil {
			marker, found, err := persistent.marker()
			if err != nil {
				log.Printf("ingest: persistent dedup index marker unavailable: %v", err)
				_ = persistent.Close()
				persistent = nil
				persistReset = true
			} else if !found {
				_ = persistent.Close()
				persistent = nil
				persistReset = true
			} else if validator, ok := i.store.(DedupIndexValidator); ok {
				valid, err := validator.ValidateDedupIndexMarker(ctx, store.ContentKey(marker))
				if err != nil || !valid {
					if err != nil {
						log.Printf("ingest: persistent dedup index marker validation failed: %v", err)
					}
					_ = persistent.Close()
					persistent = nil
					persistReset = true
				}
			}
		}
	}
	var pending map[[32]byte]chunkMeta
	if i.dedupPath != "" {
		pending = make(map[[32]byte]chunkMeta)
	}
	var pendingMu sync.Mutex
	pendingLimit := i.dedupCache.limit // bound metadata retained for index persistence
	if pendingLimit <= 0 || pendingLimit > defaultChunkDedupCacheLimit {
		pendingLimit = defaultChunkDedupCacheLimit
	}
	var persistentLog sync.Once
	logPersistentErr := func(err error) {
		persistentLog.Do(func() {
			log.Printf("ingest: persistent dedup index lookup unavailable: %v", err)
		})
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if wctx.Err() != nil {
					continue // drain fast; error already recorded
				}

				fp := crypto.DeriveKey(salt, j.data)

				var (
					cachedMeta chunkMeta
					hit        bool
					flight     *chunkFlight
					isLeader   bool
				)
				releaseFlight := func() {
					if !isLeader {
						return
					}
					i.dedupMu.Lock()
					if i.dedupFlights[fp] == flight {
						delete(i.dedupFlights, fp)
					}
					close(flight.done)
					i.dedupMu.Unlock()
					isLeader = false
				}

				for {
					if wctx.Err() != nil {
						break
					}
					i.dedupMu.Lock()
					if meta, ok := i.dedupCache.get(fp); ok {
						i.dedupMu.Unlock()
						cachedMeta = meta
						hit = true
						break
					}
					f, active := i.dedupFlights[fp]
					if !active {
						flight = &chunkFlight{
							done:    make(chan struct{}),
							ordinal: j.ordinal,
						}
						i.dedupFlights[fp] = flight
						isLeader = true
						i.dedupMu.Unlock()
						break
					}
					// A flight is already active for this chunk fingerprint.
					// If this store enforces ordered Chunk ordinal sequencing,
					// a lower ordinal cannot wait on a higher ordinal.
					if isOrderedWriter && f.ordinal >= j.ordinal {
						i.dedupMu.Unlock()
						break // proceed independently
					}
					done := f.done
					i.dedupMu.Unlock()
					select {
					case <-done:
						continue
					case <-wctx.Done():
						break
					}
				}

				if wctx.Err() != nil {
					releaseFlight()
					continue
				}

				if !hit && isLeader && persistent != nil {
					meta, found, err := persistent.lookup(fp)
					if err != nil {
						logPersistentErr(err)
					}
					if found {
						i.dedupMu.Lock()
						i.dedupCache.put(fp, meta)
						i.dedupMu.Unlock()
						releaseFlight()
						cachedMeta = meta
						hit = true
					}
				}
				if wctx.Err() != nil {
					releaseFlight()
					continue
				}

				if hit {
					// Duplicate chunk found in dedup index.
					if isOrderedWriter {
						if canSkipOrdered {
							skipped, err := orderedSkipper.SkipChunkOrdered(wctx, admission, cachedMeta.ck, cachedMeta.ctLen, j.ordinal)
							if err != nil {
								fail(fmt.Errorf("ingest: skip chunk ordered: %w", err))
								continue
							}
							if !skipped {
								// Underlying ordered writer does not have the chunk yet (e.g. fresh bundle).
								goto performEncryptAndStore
							}
						} else {
							goto performEncryptAndStore
						}
					}

					// Successfully deduplicated! Completely skip EncryptChunk and store Put.
					j.oc.key = cachedMeta.key
					j.oc.ck = cachedMeta.ck
					j.oc.ctLen = cachedMeta.ctLen
					j.oc.encLen = cachedMeta.encLen
					j.oc.format = cachedMeta.format
					j.oc.isNew = false
					emitProgress(j.oc.size)
					continue
				}

			performEncryptAndStore:
				var (
					ciphertext []byte
					hash       [32]byte
					key        [32]byte
					err        error
				)
				if keyed, ok := i.enc.(crypto.KeyedEncryptor); ok {
					ciphertext, hash, err = keyed.EncryptChunkWithKey(wctx, fp, j.data)
					key = fp
				} else {
					ciphertext, hash, key, err = i.enc.EncryptChunk(wctx, salt, j.data)
				}
				if err != nil {
					fail(fmt.Errorf("ingest: encrypt chunk: %w", err))
					releaseFlight()
					continue
				}
				if err := validateEncryptedChunkObject(ciphertext, j.oc.size); err != nil {
					fail(fmt.Errorf("ingest: encrypt chunk: %w", err))
					releaseFlight()
					continue
				}
				ck := store.ContentKey(hash)
				var isNew bool
				if isOrderedWriter {
					ordered := i.store.(OrderedChunkStoreWriter)
					isNew, err = ordered.PutChunkOrdered(wctx, admission, ck, ciphertext, j.ordinal)
				} else {
					isNew, err = i.store.Put(wctx, admission, store.PartitionChunk, ck, ciphertext)
				}
				if err != nil {
					fail(fmt.Errorf("ingest: store put chunk: %w", err))
					releaseFlight()
					continue
				}

				meta := chunkMeta{
					key:    key,
					ck:     ck,
					ctLen:  uint64(len(ciphertext)),
					encLen: uint64(len(ciphertext) - 1),
					format: ciphertext[0],
				}

				i.dedupMu.Lock()
				i.dedupCache.put(fp, meta)
				i.dedupMu.Unlock()
				releaseFlight()
				if pending != nil {
					pendingMu.Lock()
					if len(pending) < pendingLimit {
						pending[fp] = meta
					}
					pendingMu.Unlock()
				}

				j.oc.key = key
				j.oc.ck = ck
				j.oc.ctLen = meta.ctLen
				j.oc.encLen = meta.encLen
				j.oc.format = meta.format
				j.oc.isNew = isNew
				emitProgress(j.oc.size)
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
	if persistent != nil {
		if err := persistent.Close(); err != nil {
			log.Printf("ingest: close persistent dedup index: %v", err)
		}
	}
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
	res.ManifestLogicalBytes = uint64(codec.HeaderSize) + uint64(len(m.Entries))*codec.EntrySize + uint64(len(m.Holes))*codec.HoleSize + uint64(len(sealed))
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
	if i.dedupPath != "" && (len(pending) > 0 || persistReset) {
		if err := persistChunkMetas(i.dedupPath, pending, manifestKey, persistReset); err != nil {
			log.Printf("ingest: persist chunk dedup index: %v", err)
		}
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
	default:
		return fmt.Errorf("unknown physical format 0x%02x", object[0])
	}
	return nil
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
