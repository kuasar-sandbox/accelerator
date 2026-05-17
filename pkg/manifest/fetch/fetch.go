// Package fetch implements the unified read layer for the container accelerator.
//
// It reconstructs a virtual image from a Manifest, decrypted keys, and a tiered
// cache, serving arbitrary byte-range reads via ReadAt and streamed output via WriteTo.
package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/fullof-work/mass-sandbox/pkg/cache"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/crypto"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/codec"
	"github.com/fullof-work/mass-sandbox/pkg/store"
)

// ErrHitHole signals that a read range intersects a manifest hole —
// the position has no associated data. WriteTo returns this error
// when its OnHole callback is nil; ReadAt returns it when offset
// itself lies inside a hole. Callers wanting policy-based handling
// (zero-fill / sparse output / fallocate punch / propagate) should
// branch on errors.Is(err, ErrHitHole).
var ErrHitHole = errors.New("fetch: range intersects a hole")

// ReadOptions bundles the optional callbacks for WriteTo. Zero value
// is the strictest configuration: no progress reporting, and any
// hole intersected by the requested range causes WriteTo to return
// ErrHitHole. Each callback is independent — populate only what you need.
type ReadOptions struct {
	// OnProgress, if non-nil, is invoked after each chunk write with
	// the running (done, total) chunk count. Hole regions do not
	// participate in this counter.
	OnProgress func(done, total int)

	// OnHole, if non-nil, is invoked when WriteTo's range intersects
	// a manifest hole. The callback receives the writer, the image
	// offset where the (possibly clipped) hole starts, and the size
	// of the intersected portion. It decides the policy:
	//   - skip / write nothing       → simple sparse output
	//   - zero-fill via io.CopyN(...) → POSIX read semantics
	//   - fallocate(PUNCH_HOLE)       → block-device sparse output
	//   - return error                → reject manifests with holes
	// Nil callback ≡ "return ErrHitHole on first hole intersection".
	OnHole func(w io.Writer, offset, size uint64) error
}

// Stream serves byte-range reads from a chunked and encrypted image.
// One Stream per Manifest; constructed via NewStream (when the caller
// already has decrypted keys, e.g. inside Fetcher.Fetch) or returned
// from Fetcher.Fetch (factory-style, hex-key entry).
//
// All methods are safe for concurrent use.
type Stream interface {
	ImageSize() uint64
	Manifest() *codec.Manifest
	Holes() []codec.HoleExtent
	FindHole(offset uint64) (codec.HoleExtent, bool)
	WriteTo(ctx context.Context, w io.Writer, offset, length uint64, opts ReadOptions) error
	ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error)
	ReadAtBlock(ctx context.Context, buf []byte, offset uint64) (int, error)
}

// stream is the concrete implementation of Stream.
type stream struct {
	m         *codec.Manifest
	cache     cache.Getter
	encryptor crypto.ChunkEncryptor
	keys      [][32]byte // decrypted per-chunk keys
}

// NewStream constructs a per-Manifest Stream. Callers that hold a
// hex content key but not yet a Manifest should use a Fetcher (via
// NewFetcher) which loads + unseals + builds the Stream internally.
//
// keys must contain one decrypted convergent key per chunk entry, in
// the same order as m.Entries. Zero entries should hold the zero key
// (never read by the fetch path).
func NewStream(m *codec.Manifest, keys [][32]byte, c cache.Getter, enc crypto.ChunkEncryptor) Stream {
	return &stream{
		m:         m,
		cache:     c,
		encryptor: enc,
		keys:      keys,
	}
}

// ImageSize returns the total virtual image size, including hole regions.
func (f *stream) ImageSize() uint64 { return f.m.ImageSize }

// Holes returns a snapshot of the manifest's hole list. The returned
// slice aliases the underlying manifest — callers must not mutate.
func (f *stream) Holes() []codec.HoleExtent { return f.m.Holes }

// Manifest returns the underlying manifest. Callers must treat the
// returned pointer as read-only — its slices alias the Fetcher's
// state and are mutated by no-one after construction.
func (f *stream) Manifest() *codec.Manifest { return f.m }

// FindHole returns the hole extent that contains offset, or ok=false
// if offset is inside a data region (or past ImageSize). O(log M).
func (f *stream) FindHole(offset uint64) (codec.HoleExtent, bool) {
	holes := f.m.Holes
	// Binary search for the rightmost hole with Offset <= offset.
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
		return codec.HoleExtent{}, false
	}
	h := holes[idx]
	if offset < h.Offset+h.Size {
		return h, true
	}
	return codec.HoleExtent{}, false
}

// firstHoleAfter returns the index of the first hole at or after
// offset. Returns len(holes) if no such hole exists.
func (f *stream) firstHoleAfter(offset uint64) int {
	holes := f.m.Holes
	return sort.Search(len(holes), func(i int) bool {
		return holes[i].Offset >= offset
	})
}

// WriteTo writes [offset, offset+length) of the virtual image to w.
//
// Data chunks are fetched concurrently and written in image-offset
// order; hole regions intersecting the requested range are passed to
// opts.OnHole (or trigger ErrHitHole when OnHole is nil).
//
// length=0 means "until end of image". Concurrency is governed by
// the cache tier semaphores; this layer does not impose its own limit.
func (f *stream) WriteTo(ctx context.Context, w io.Writer, offset, length uint64, opts ReadOptions) error {
	if offset >= f.m.ImageSize {
		return nil
	}
	if length == 0 || offset+length > f.m.ImageSize {
		length = f.m.ImageSize - offset
	}
	end := offset + length

	// Find the entry index range that intersects [offset, end). Same
	// search as the legacy WriteTo — entries are still the unit of
	// concurrent fetch.
	startIdx := codec.ChunkIndexForOffset(f.m.Entries, offset)
	if startIdx < 0 {
		// offset is not within any entry — it must be inside a hole
		// or beyond ImageSize (already handled). Walk holes only.
		startIdx = sort.Search(len(f.m.Entries), func(i int) bool {
			return f.m.Entries[i].Offset >= offset
		})
	}
	endIdx := startIdx
	for endIdx < len(f.m.Entries) {
		e := f.m.Entries[endIdx]
		if e.Offset >= end {
			break
		}
		endIdx++
	}

	// Concurrently fetch all entries that may produce output bytes.
	results, errCh := f.fetchChunks(ctx, startIdx, endIdx)

	// Walk the segment timeline in offset order: alternate between
	// reading the next entry's plaintext from results and invoking
	// OnHole for hole regions that fall in the gaps.
	hi := f.firstHoleAfter(offset)
	cursor := offset
	totalSegs := endIdx - startIdx // for progress; holes don't bump it
	doneSegs := 0

	for ei := startIdx; ei < endIdx; ei++ {
		// Emit any holes strictly before the next entry.
		for hi < len(f.m.Holes) && f.m.Holes[hi].Offset < f.m.Entries[ei].Offset {
			h := f.m.Holes[hi]
			holeStart := h.Offset
			if holeStart < cursor {
				holeStart = cursor
			}
			holeEnd := h.Offset + h.Size
			if holeEnd > end {
				holeEnd = end
			}
			if holeEnd > holeStart {
				if err := emitHole(w, holeStart, holeEnd-holeStart, opts.OnHole); err != nil {
					return err
				}
			}
			cursor = holeEnd
			hi++
		}

		// Now write the entry's contribution.
		var cr chunkResult
		select {
		case cr = <-results[ei-startIdx]:
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
		entry := f.m.Entries[ei]
		var lo, eHi uint64
		if entry.Offset < offset {
			lo = offset - entry.Offset
		}
		eHi = uint64(entry.Size)
		if entry.Offset+eHi > end {
			eHi = end - entry.Offset
		}
		_, werr := w.Write(cr.plain[lo:eHi])
		cr.release()
		if werr != nil {
			return fmt.Errorf("fetch: write: %w", werr)
		}
		cursor = entry.Offset + eHi
		doneSegs++
		if opts.OnProgress != nil {
			opts.OnProgress(doneSegs, totalSegs)
		}
	}

	// Trailing holes within [cursor, end).
	for hi < len(f.m.Holes) && f.m.Holes[hi].Offset < end {
		h := f.m.Holes[hi]
		holeStart := h.Offset
		if holeStart < cursor {
			holeStart = cursor
		}
		holeEnd := h.Offset + h.Size
		if holeEnd > end {
			holeEnd = end
		}
		if holeEnd > holeStart {
			if err := emitHole(w, holeStart, holeEnd-holeStart, opts.OnHole); err != nil {
				return err
			}
		}
		cursor = holeEnd
		hi++
	}

	return nil
}

// emitHole dispatches to onHole if set, otherwise returns ErrHitHole.
func emitHole(w io.Writer, offset, size uint64, onHole func(w io.Writer, offset, size uint64) error) error {
	if onHole == nil {
		return ErrHitHole
	}
	return onHole(w, offset, size)
}

// ReadAt reads up to len(buf) bytes starting at image offset offset
// into buf. The read never crosses a hole or end-of-image boundary;
// callers either get a successful short read (and detect the
// boundary by comparing offset+n against ImageSize / FindHole) or a
// positional error.
//
// Return contract:
//
//	(n, nil)            n ∈ [1, len(buf)] bytes valid in buf. If
//	                    n < len(buf), the read was clipped at a
//	                    hole start or end-of-image; the caller can
//	                    advance offset and re-issue ReadAt to handle
//	                    the boundary.
//	(0, io.EOF)         offset >= ImageSize.
//	(0, ErrHitHole)     offset is inside a hole. Use FindHole to
//	                    locate the extent and continue reading at
//	                    hole.Offset + hole.Size.
//	(0, transport err)  cache / decrypt / I/O failure.
//
// Invariant: err != nil ⇒ n == 0. n > 0 ⇒ err == nil.
//
// Buffer state on error: bytes in buf[0:k) where k is the number of
// chunks successfully copied before the failing one MAY have been
// written. Callers MUST NOT inspect buf after an error return — only
// use buf[:n] when err == nil. (This matches the no-alloc fast path:
// data is written directly into the caller's slice rather than via
// an intermediate scratch buffer.)
//
// Note this differs from io.ReaderAt's POSIX-ish "may return n>0
// with non-nil error" contract; the strict err⇒n=0 separation
// simplifies error handling at the cost of the buffer-state caveat
// above.
func (f *stream) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	if offset >= f.m.ImageSize {
		return 0, io.EOF
	}
	if _, inHole := f.FindHole(offset); inHole {
		return 0, ErrHitHole
	}
	if len(buf) == 0 {
		return 0, nil
	}

	// Compute the upper bound of this read: clipped by ImageSize, by
	// the next hole, and by len(buf).
	end := offset + uint64(len(buf))
	if end > f.m.ImageSize {
		end = f.m.ImageSize
	}
	hi := f.firstHoleAfter(offset)
	if hi < len(f.m.Holes) && f.m.Holes[hi].Offset < end {
		end = f.m.Holes[hi].Offset
	}
	clipped := buf[:end-offset]

	// Existing concurrent-fetch logic over Entries within [offset, end).
	// By invariant entries fully cover [offset, end) since end is
	// clipped at the next hole boundary.
	startIdx := codec.ChunkIndexForOffset(f.m.Entries, offset)
	if startIdx < 0 {
		// Should be impossible after the in-hole and EOF guards above.
		return 0, fmt.Errorf("fetch: no chunk for offset %d", offset)
	}
	endIdx := startIdx
	for endIdx < len(f.m.Entries) {
		e := f.m.Entries[endIdx]
		if e.Offset >= end {
			break
		}
		endIdx++
	}

	results, errCh := f.fetchChunks(ctx, startIdx, endIdx)

	// Drain results in entry order, copying directly into the caller's
	// buf. On error we still return n=0; the buffer may be partially
	// modified, but the caller's contract says n bytes are valid only
	// when err == nil. Eliminating the scratch buffer saves an alloc
	// the size of the read plus one memcpy per call.
	pos := 0
	for ei := startIdx; ei < endIdx; ei++ {
		var cr chunkResult
		select {
		case cr = <-results[ei-startIdx]:
		case err := <-errCh:
			return 0, err
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		entry := f.m.Entries[ei]
		var lo, eHi uint64
		if entry.Offset < offset {
			lo = offset - entry.Offset
		}
		eHi = uint64(entry.Size)
		if entry.Offset+eHi > end {
			eHi = end - entry.Offset
		}
		copied := copy(clipped[pos:], cr.plain[lo:eHi])
		cr.release()
		pos += copied
	}
	return pos, nil
}

// ReadAtBlock reads len(buf) bytes starting at offset with block-device
// semantics: holes are transparently zero-filled and a single read may
// span hole / data boundaries until either the buffer is full or
// end-of-image is reached. This is the entry point used by virtual
// block backends (vhost-user-blk) where the guest expects the original
// sparse image to look like a flat zero-filled device.
//
// Return contract (POSIX-like, matches io.ReaderAt):
//
//	(len(buf), nil)  full read; buf is entirely valid.
//	(n, io.EOF)      n < len(buf): the read was clipped at ImageSize.
//	                 buf[:n] is valid, buf[n:] is untouched.
//	(0, io.EOF)      offset >= ImageSize.
//	(0, transport)   cache / decrypt / context error.
//
// Safe for concurrent use. Concurrency comes "for free" because the
// underlying ReadAt and fetchChunks read only immutable fields of
// Fetcher and create per-call channels.
func (f *stream) ReadAtBlock(ctx context.Context, buf []byte, offset uint64) (int, error) {
	if offset >= f.m.ImageSize {
		return 0, io.EOF
	}
	if len(buf) == 0 {
		return 0, nil
	}
	end := offset + uint64(len(buf))
	var eofErr error
	if end > f.m.ImageSize {
		end = f.m.ImageSize
		eofErr = io.EOF
	}
	want := int(end - offset) // safe: bounded by len(buf)

	n := 0
	for n < want {
		cur := offset + uint64(n)
		if h, inHole := f.FindHole(cur); inHole {
			// Zero-fill the slice of the hole that overlaps [cur, end).
			holeEnd := h.Offset + h.Size
			if holeEnd > end {
				holeEnd = end
			}
			zeros := int(holeEnd - cur)
			clearSlice(buf[n : n+zeros])
			n += zeros
			continue
		}
		// ReadAt returns a short read clipped at the next hole boundary
		// or end-of-image; either way the for-loop keeps stepping until
		// we've filled `want` bytes.
		m, err := f.ReadAt(ctx, buf[n:want], cur)
		if err != nil {
			return 0, err
		}
		if m == 0 {
			// Defensive: ReadAt should always make progress when not at
			// a hole / EOF (both are guarded above). Break to avoid an
			// infinite loop if the invariant is ever violated.
			return 0, fmt.Errorf("fetch: ReadAt returned no progress at offset %d", cur)
		}
		n += m
	}
	return n, eofErr
}

// clearSlice zeros every byte in b. The compiler recognises this loop
// pattern and lowers it to memclr — no need for a manual unroll or
// reflection trick.
func clearSlice(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// chunkResult carries one chunk's plaintext from the fetcher goroutine
// to the receiver. plain aliases the underlying ciphertext buffer
// (DecryptInPlace) for non-zero chunks; the receiver MUST call
// release() exactly once after consuming plain so the cache's blob
// pool can recycle the storage. For IsZero chunks the storage is a
// fresh GC-managed slice with a no-op release.
type chunkResult struct {
	plain   []byte
	release func()
}

// fetchChunks launches a goroutine for each chunk in [startIdx, endIdx).
// Returns one result channel per chunk (in order) and a single shared
// error channel. Concurrency is naturally bounded by tier semaphores
// inside TieredCache.
//
// Plaintext aliasing: each non-zero chunk's plaintext slice is an
// alias into the cache.Blob's buffer (AES-CTR XORs in place, Fake
// just reslices off the HMAC tag). The blob is released by the
// receiver via chunkResult.release(), not by this goroutine — so the
// buffer stays valid for the entire copy at the read site without an
// intermediate plaintext allocation.
func (f *stream) fetchChunks(ctx context.Context, startIdx, endIdx int) ([]chan chunkResult, chan error) {
	n := endIdx - startIdx
	results := make([]chan chunkResult, n)
	for i := range results {
		results[i] = make(chan chunkResult, 1)
	}
	errCh := make(chan error, n)
	var firstErr sync.Once

	for i := 0; i < n; i++ {
		go func(i int) {
			idx := startIdx + i
			entry := f.m.Entries[idx]

			if entry.IsZero {
				results[i] <- chunkResult{
					plain:   make([]byte, entry.Size),
					release: func() {},
				}
				return
			}

			result, blob, err := f.cache.Get(ctx, store.PartitionChunk, store.ContentKey(entry.CiphertextHash))
			if err != nil {
				firstErr.Do(func() { errCh <- fmt.Errorf("chunk %d: %w", idx, err) })
				return
			}
			if result != cache.CacheHit {
				firstErr.Do(func() { errCh <- fmt.Errorf("chunk %d: not found", idx) })
				return
			}

			// In-place decrypt: plain aliases the blob's storage. The
			// blob is handed off to the receiver through release; we
			// must NOT release it here.
			plain, err := f.encryptor.DecryptInPlace(f.keys[idx], blob.Bytes())
			if err != nil {
				blob.Release()
				firstErr.Do(func() { errCh <- fmt.Errorf("chunk %d: decrypt: %w", idx, err) })
				return
			}
			results[i] <- chunkResult{plain: plain, release: blob.Release}
		}(i)
	}
	return results, errCh
}
