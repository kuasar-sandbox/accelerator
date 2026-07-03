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
				ciphertext, _, key := i.enc.EncryptChunk(salt, j.data)
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

	// One forward pass over src's runs: holes are recorded and
	// skipped; contiguous Zero|Data runs form one segment, chunked as
	// a unit so chunk boundaries match a literal byte stream bounded
	// only by holes.
	var chunkErr error
	cursor := uint64(0)
	for cursor < size && chunkErr == nil {
		kind, end, err := src.RunAt(cursor, size-cursor)
		if err != nil {
			chunkErr = fmt.Errorf("ingest: classify @ %d: %w", cursor, err)
			break
		}
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
			k2, e2, err := src.RunAt(cursor, size-cursor)
			if err != nil {
				chunkErr = fmt.Errorf("ingest: classify @ %d: %w", cursor, err)
				break
			}
			if k2 == sparse.Hole {
				break
			}
			cursor = e2
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
			case jobs <- job{oc: oc, data: cr.Data}:
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
	runKind  sparse.RunKind
	runEnd   uint64
}

func (r *segReader) Read(p []byte) (int, error) {
	if r.cur >= r.end {
		return 0, io.EOF
	}
	if r.cur >= r.runEnd {
		kind, end, err := r.src.RunAt(r.cur, r.end-r.cur)
		if err != nil {
			return 0, err
		}
		if kind == sparse.Hole {
			return 0, fmt.Errorf("ingest: hole @ %d inside a data segment (inconsistent RunAt)", r.cur)
		}
		r.runKind, r.runEnd = kind, end
	}
	n := len(p)
	if rest := r.runEnd - r.cur; rest < uint64(n) {
		n = int(rest)
	}
	if r.runKind == sparse.Zero {
		clear(p[:n])
	} else {
		m, err := r.src.ReadAt(r.ctx, p[:n], r.cur)
		if err != nil && err != io.EOF {
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
