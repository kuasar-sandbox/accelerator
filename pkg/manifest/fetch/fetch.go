// Package fetch implements the unified read layer for the container accelerator.
//
// A Stream is the read-base abstraction: it reconstructs a virtual image from
// either a chunked + encrypted manifest, a local file, or an overlay of several
// such streams. Three implementations satisfy it — manifestStream (also
// implements ChunkStream), fileStream, and layeredStream — and a sandbox disk,
// snapshot bundle, or manifest-ctl read all sit on the same abstraction.
//
// ChunkStream is an optional enhancement a Stream may implement so a caller can
// thread a chunk index between classification (RunChunkAt) and read
// (ReadChunkAt), skipping a re-lookup. Its only in-tree users are manifestStream
// itself (RunAt/ReadAt are built on the chunk primitives) and layeredStream
// (which threads a serving layer's chunk index from resolve to read). External
// consumers use the plain Stream methods.
//
// All methods are safe for concurrent use.
package fetch

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/cache"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/store"
)

// RunKind classifies the effective (overlay-resolved) content at an image
// offset.
type RunKind uint8

const (
	// Hole: a declared hole / out-of-bounds in every layer — no data anywhere.
	// Consumers zero-fill or punch a sparse extent; no fetch.
	Hole RunKind = iota
	// Zero: an IsZero chunk in the serving layer — explicit zero data. Needs no
	// fetch, but it is content the layer owns, so it does NOT fall through.
	Zero
	// Data: a non-zero chunk in the serving layer — must be fetched.
	Data
)

// Stream serves byte-range reads from a chunked+encrypted manifest, a local
// file, or an overlay of several. limit in RunAt is a length: the returned run
// satisfies offset < end <= min(offset+limit, Size()).
type Stream interface {
	// Size is the total virtual image size, holes included. For a multi-layer
	// stream it is the maximum Size over all layers.
	Size() uint64

	// RunAt classifies the effective content at offset and returns the kind plus
	// the end of a same-kind run. err == io.EOF iff offset >= Size(); otherwise
	// err == nil and offset < end <= min(offset+limit, Size()). Hole/Zero runs
	// may extend through contiguous same-kind regions; a Data run is at most one
	// chunk. Callers iterate RunAt until io.EOF.
	RunAt(offset, limit uint64) (kind RunKind, end uint64, err error)

	// ReadAt reads len(buf) bytes from offset with POSIX io.ReaderAt semantics:
	// holes and IsZero chunks are zero-filled, a single call spans hole/data
	// boundaries until buf is full or end-of-image.
	//
	//	(len(buf), nil)  full read
	//	(n, io.EOF)      n < len(buf): clipped at Size(); buf[:n] valid
	//	(0, io.EOF)      offset >= Size()
	//	(0, transport)   cache / decrypt / context error (n == 0 on error)
	//
	// ReadAt MAY fetch the data chunks in the range concurrently (internal
	// goroutines); see ChunkStream.ReadChunkAt for the synchronous counterpart.
	ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error)

	// Close releases resources owned by this stream (e.g. a file descriptor).
	// manifest streams own none (no-op); a layered stream closes its layers.
	Close() error
}

// ChunkStream is the optional enhancement contract: it lets a caller reuse a
// chunk index between classification and read, avoiding a second offset lookup.
//
// Concurrency convention: ReadChunkAt is ALWAYS synchronous and runs in the
// calling goroutine — it spawns no goroutines. Batch parallelism is Stream.ReadAt's
// job. A caller wanting concurrency invokes ReadChunkAt from its own goroutines.
type ChunkStream interface {
	// RunChunkAt is RunAt that additionally returns the serving chunk index
	// (valid for Data and Zero; meaningless for Hole). It returns a single
	// region (no Hole/Zero extension).
	RunChunkAt(offset, limit uint64) (kind RunKind, end, chunkIdx uint64, err error)
	// ReadChunkAt synchronously fetches+decrypts chunk chunkIdx and copies its
	// [offset, end) sub-range into buf. Used for Data (and Zero, which yields
	// zeros); never for Hole.
	ReadChunkAt(ctx context.Context, buf []byte, chunkIdx, offset, end uint64) (int, error)
}

// manifestStream is the single-manifest implementation (Stream + ChunkStream).
type manifestStream struct {
	m         *codec.Manifest
	cache     cache.Getter
	encryptor crypto.ChunkEncryptor
	keys      [][32]byte // decrypted per-chunk keys, parallel to m.Entries
}

// NewStream constructs a single-manifest Stream. keys holds one decrypted
// convergent key per chunk entry, in m.Entries order (zero entries hold the
// zero key, never read). Callers holding a hex key but not a Manifest should use
// a Fetcher (NewFetcher).
func NewStream(m *codec.Manifest, keys [][32]byte, c cache.Getter, enc crypto.ChunkEncryptor) Stream {
	return &manifestStream{m: m, cache: c, encryptor: enc, keys: keys}
}

func (s *manifestStream) Size() uint64 { return s.m.ImageSize }
func (s *manifestStream) Close() error { return nil }

// RunChunkAt classifies one region starting at offset. By the tiling invariant
// (entries + holes cover [0, ImageSize) with no overlap or gap) a data/zero
// chunk ends exactly where the next hole begins.
func (s *manifestStream) RunChunkAt(offset, limit uint64) (RunKind, uint64, uint64, error) {
	if offset >= s.m.ImageSize {
		return 0, 0, 0, io.EOF
	}
	limEnd := offset + limit
	if limEnd < offset || limEnd > s.m.ImageSize { // overflow or past EOF
		limEnd = s.m.ImageSize
	}
	if h, in := findHoleAt(s.m.Holes, offset); in {
		end := h.Offset + h.Size
		if end > limEnd {
			end = limEnd
		}
		return Hole, end, 0, nil
	}
	i := codec.ChunkIndexForOffset(s.m.Entries, offset)
	if i < 0 {
		return Hole, limEnd, 0, nil // unreachable in bounds (tiling); degrade safely
	}
	e := s.m.Entries[i]
	end := e.Offset + uint64(e.Size)
	if end > limEnd {
		end = limEnd
	}
	if e.IsZero {
		return Zero, end, uint64(i), nil
	}
	return Data, end, uint64(i), nil
}

// RunAt extends Hole/Zero runs through contiguous same-kind regions; a Data run
// stays a single chunk.
func (s *manifestStream) RunAt(offset, limit uint64) (RunKind, uint64, error) {
	kind, end, _, err := s.RunChunkAt(offset, limit)
	if err != nil || kind == Data {
		return kind, end, err
	}
	limEnd := offset + limit
	if limEnd < offset || limEnd > s.m.ImageSize {
		limEnd = s.m.ImageSize
	}
	for end < limEnd {
		k2, e2, _, err2 := s.RunChunkAt(end, limEnd-end)
		if err2 != nil || k2 != kind {
			break
		}
		end = e2
	}
	return kind, end, nil
}

// readChunkInto is the single synchronous fetch path: fetch chunk chunkIdx,
// decrypt in place, copy its [offset, end) sub-range into dst. It spawns no
// goroutines; ReadChunkAt calls it directly, ReadAt calls it from workers.
func (s *manifestStream) readChunkInto(ctx context.Context, dst []byte, chunkIdx, offset, end uint64) error {
	e := s.m.Entries[chunkIdx]
	if e.IsZero {
		clearSlice(dst)
		return nil
	}
	result, blob, err := s.cache.Get(ctx, store.PartitionChunk, store.ContentKey(e.CiphertextHash))
	if err != nil {
		return fmt.Errorf("chunk %d: %w", chunkIdx, err)
	}
	if result != cache.CacheHit {
		return fmt.Errorf("chunk %d: not found", chunkIdx)
	}
	plain, err := s.encryptor.DecryptInPlace(s.keys[chunkIdx], blob.Bytes())
	if err != nil {
		blob.Release()
		return fmt.Errorf("chunk %d: decrypt: %w", chunkIdx, err)
	}
	copy(dst, plain[offset-e.Offset:end-e.Offset])
	blob.Release()
	return nil
}

// ReadChunkAt implements ChunkStream — synchronous single-chunk read.
func (s *manifestStream) ReadChunkAt(ctx context.Context, buf []byte, chunkIdx, offset, end uint64) (int, error) {
	if err := s.readChunkInto(ctx, buf, chunkIdx, offset, end); err != nil {
		return 0, err
	}
	return int(end - offset), nil
}

// ReadAt walks the runs and fetches data chunks concurrently — one goroutine
// per Data run, each writing directly into its buf slice; Hole/Zero are
// zero-filled in place. ctx cancel on early return releases in-flight blobs.
func (s *manifestStream) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	if offset >= s.m.ImageSize {
		return 0, io.EOF
	}
	if len(buf) == 0 {
		return 0, nil
	}
	end := offset + uint64(len(buf))
	var eof error
	if end > s.m.ImageSize {
		end = s.m.ImageSize
		eof = io.EOF
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	var once sync.Once

	for cur := offset; cur < end; {
		kind, runEnd, idx, _ := s.RunChunkAt(cur, end-cur)
		dst := buf[cur-offset : runEnd-offset]
		if kind == Data {
			wg.Add(1)
			go func(idx, lo, hi uint64, dst []byte) {
				defer wg.Done()
				if err := s.readChunkInto(cctx, dst, idx, lo, hi); err != nil {
					once.Do(func() { errCh <- err; cancel() })
				}
			}(idx, cur, runEnd, dst)
		} else { // Hole | Zero
			clearSlice(dst)
		}
		cur = runEnd
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return 0, err
	default:
	}
	return int(end - offset), eof
}

// findHoleAt returns the hole extent containing offset, or ok=false. The hole
// slice is sorted by Offset and disjoint. O(log M).
func findHoleAt(holes []codec.HoleExtent, offset uint64) (codec.HoleExtent, bool) {
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

// nextHoleStart returns the start of the first hole at or after offset (caller
// must be in data at offset), clamped to limEnd — used to bound a data run.
func nextHoleStart(holes []codec.HoleExtent, offset, limEnd uint64) uint64 {
	lo, hi := 0, len(holes)
	for lo < hi {
		mid := (lo + hi) / 2
		if holes[mid].Offset < offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(holes) && holes[lo].Offset < limEnd {
		return holes[lo].Offset
	}
	return limEnd
}

// clearSlice zeros every byte in b (the compiler lowers this to memclr).
func clearSlice(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
