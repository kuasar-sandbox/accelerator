package cache

import (
	"sync"
	"sync/atomic"
)

// Blob is an immutable, refcounted byte container shared across cache
// layers. It is intentionally decoupled from pkg/store — when the
// store package is re-implemented, no cache code needs to change.
//
// The interface deliberately exposes only three operations: Bytes,
// Clone and Release. It does NOT expose NewReader — since Bytes is
// zero-copy, any caller that wants an io.Reader can trivially wrap
// it with bytes.NewReader(blob.Bytes()) locally, so adding NewReader
// here would just duplicate surface.
//
// Lifecycle rules:
//
//   - Every Blob handle starts with refcount = 1 and MUST be Released
//     exactly once by its current owner.
//   - Clone produces an independent handle sharing the same underlying
//     bytes with an incremented refcount. The caller of Clone owns
//     the returned handle and must Release it exactly once.
//   - Bytes returns a borrowed view that is valid until the handle it
//     was called on is Released. Cloning before Release extends the
//     underlying buffer's lifetime until every clone is also Released.
//   - Double-Release on a single handle is a programmer error.
//
// Typical hand-off pattern (TieredCache fill-aside):
//
//	cloned := blob.Clone()                // caller owns `cloned`
//	go func() {
//	    defer cloned.Release()
//	    // do work that may outlive the original blob's Release
//	    useBytes(cloned.Bytes())
//	}()
//	// original `blob` continues being used synchronously; the
//	// backing buffer survives until both the original and the
//	// cloned handles have been Released.
type Blob interface {
	// Bytes returns a borrowed zero-copy view of the underlying
	// bytes. Callers must not retain the slice past this handle's
	// Release.
	Bytes() []byte

	// Clone returns a new Blob handle that shares the same backing
	// bytes with an incremented refcount. The caller owns the
	// returned handle and must Release it exactly once.
	Clone() Blob

	// Release drops one reference to the underlying buffer. When the
	// last reference is released, any pool-backed buffer is returned
	// to its pool. Safe to call exactly once per handle.
	Release()
}

// Segmenter is an OPTIONAL interface a Blob may implement when its
// bytes are not stored as a single contiguous slice. Wire-level
// consumers (e.g., wire.WriteResponse) check for this interface and,
// when present, send the segments via net.Buffers (writev) in a
// single syscall instead of first asking for Bytes() — which would
// force a join/copy.
//
// The concatenation of Segments() MUST equal Bytes(). Implementations
// are free to make Bytes() perform a lazy copy+join into a single
// buffer for callers that can only consume a contiguous slice.
//
// All returned segments are borrowed: they're valid only until the
// host Blob is Released, and callers MUST NOT retain them past that
// point. Refcounting is delegated to the Blob's Clone/Release.
type Segmenter interface {
	Segments() [][]byte
}

// NewMemBlob wraps a GC-managed byte slice as a Blob. Release is a
// no-op; Clone returns a new handle that shares the same slice
// (refcounting is unnecessary because the GC handles reclamation).
//
// Use NewMemBlob when the bytes come from a source that doesn't
// benefit from pool reuse: wire-client buffers, io.ReadAll results,
// small constants in tests.
func NewMemBlob(data []byte) Blob {
	return memBlob{data: data}
}

// memBlob is the GC-backed default Blob implementation. A value
// receiver is sufficient — the struct holds a slice header only, and
// a value-receiver Blob can still be returned via the interface
// because the struct is small enough to stay as a single pointer on
// the interface side.
type memBlob struct {
	data []byte
}

func (b memBlob) Bytes() []byte { return b.data }
func (b memBlob) Clone() Blob   { return memBlob{data: b.data} }
func (b memBlob) Release()      {}

// ─────────────────────────────── BlobPool ───────────────────────────────

// BlobPool allocates writeable buffers wrapped in Blob handles. The
// wire reader and other hot-path consumers use it to avoid per-request
// GC allocations.
//
// Alloc returns a (buf, blob) pair: buf is a writeable slice of exactly
// `size` bytes to be filled by the caller (e.g. via io.ReadFull); the
// returned Blob shares that same buffer as its Bytes() view. On
// Release, the buffer returns to the pool.
//
// Implementations must guarantee that buf and blob.Bytes() refer to
// the same underlying storage — the caller writes into buf, then the
// downstream consumer reads via blob.Bytes() without any copy.
type BlobPool interface {
	Alloc(size int) (buf []byte, blob Blob)
}

// NewPool creates a size-tiered BlobPool. initialSize seeds the
// underlying sync.Pool's New func; requests larger than initialSize
// cause the pool to grow on demand (the usual sync.Pool idiom).
// Requests that can't be satisfied by a pooled buffer fall back to
// a fresh make() wrapped in a GC-managed memBlob — those allocations
// are not pooled but still return a valid Blob to the caller.
func NewPool(initialSize int) BlobPool {
	return &syncPool{
		inner: &sync.Pool{
			New: func() any { return make([]byte, 0, initialSize) },
		},
	}
}

// DefaultPool is a zero-cost pool that always allocates fresh via
// make() and returns a GC-managed memBlob. Use it when callers don't
// need pool reuse — behaviour matches the pre-pool baseline.
var DefaultPool BlobPool = defaultPool{}

// syncPool is the production BlobPool backed by a sync.Pool.
type syncPool struct {
	inner *sync.Pool
}

func (sp *syncPool) Alloc(size int) ([]byte, Blob) {
	buf := sp.inner.Get().([]byte)
	if cap(buf) < size {
		// Oversized: return the stale buffer to the pool unchanged
		// so it stays in rotation, and serve the request via a
		// fresh GC-managed allocation.
		sp.inner.Put(buf)
		fresh := make([]byte, size)
		return fresh, memBlob{data: fresh}
	}
	buf = buf[:size]
	refs := new(atomic.Int32)
	refs.Store(1)
	return buf, &pooledBlob{data: buf, pool: sp.inner, refs: refs}
}

// defaultPool is the no-op BlobPool used as fallback.
type defaultPool struct{}

func (defaultPool) Alloc(size int) ([]byte, Blob) {
	data := make([]byte, size)
	return data, memBlob{data: data}
}

// ─────────────────────────────── pooledBlob ───────────────────────────────

// pooledBlob is a refcounted Blob backed by a sync.Pool byte buffer.
// Moved here from pkg/cache/rocks/blob.go so the wire/client packages
// can use the same mechanism without importing rocks.
//
// Clones share the same buffer and refcount; Release drops one
// reference, and the last Release returns the buffer to its owning
// pool.
type pooledBlob struct {
	data []byte        // nil after this handle's Release
	pool *sync.Pool    // where to return the buffer when refs hits 0
	refs *atomic.Int32 // shared across all clones of the same buffer
}

func (b *pooledBlob) Bytes() []byte { return b.data }

func (b *pooledBlob) Clone() Blob {
	b.refs.Add(1)
	return &pooledBlob{data: b.data, pool: b.pool, refs: b.refs}
}

func (b *pooledBlob) Release() {
	if b.data == nil {
		return
	}
	data := b.data
	b.data = nil
	if b.refs.Add(-1) == 0 {
		b.pool.Put(data[:0])
	}
}
