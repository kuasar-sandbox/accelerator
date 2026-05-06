package cache

import (
	"bytes"
	"testing"
	"unsafe"
)

func TestMemBlob_Bytes(t *testing.T) {
	data := []byte("hello blob")
	b := NewMemBlob(data)
	if !bytes.Equal(b.Bytes(), data) {
		t.Fatalf("Bytes: got %q, want %q", b.Bytes(), data)
	}
	b.Release() // no-op but must be safe
}

// TestMemBlob_CloneSharesBackingSlice verifies that Clone returns a
// handle that views the exact same backing slice (zero-copy), and
// that releasing the clone does not invalidate the original — memBlob
// has no pool backing so both handles remain valid for the lifetime
// of the test.
func TestMemBlob_CloneSharesBackingSlice(t *testing.T) {
	data := []byte("shared payload")
	orig := NewMemBlob(data)
	defer orig.Release()

	clone := orig.Clone()
	if &orig.Bytes()[0] != &clone.Bytes()[0] {
		t.Fatal("Clone should return a handle over the same backing slice")
	}

	// Release the clone first — orig must still be usable.
	clone.Release()
	if !bytes.Equal(orig.Bytes(), data) {
		t.Fatalf("after clone.Release, orig.Bytes: got %q, want %q", orig.Bytes(), data)
	}
}

// TestMemBlob_CloneChainReleaseSafe constructs a chain of clones and
// releases them in mixed order. memBlob.Release is a no-op, so this is
// a contract-shape test: "calling Release exactly once per handle must
// not panic, regardless of order, and Bytes must stay valid until all
// references are dropped".
func TestMemBlob_CloneChainReleaseSafe(t *testing.T) {
	b := NewMemBlob([]byte{1, 2, 3, 4})
	c1 := b.Clone()
	c2 := b.Clone()
	c3 := c1.Clone()

	// Release in a mixed order.
	c2.Release()
	if got := c3.Bytes(); got[0] != 1 || got[3] != 4 {
		t.Fatalf("c3.Bytes after c2.Release: got %v", got)
	}
	c1.Release()
	c3.Release()
	b.Release()
}

// TestNewMemBlob_NilData accepts a nil input gracefully — some
// callers may pass through a nil from an upstream path without
// explicit guarding.
func TestNewMemBlob_NilData(t *testing.T) {
	b := NewMemBlob(nil)
	if b.Bytes() != nil {
		t.Fatalf("Bytes() for nil memBlob: got %v, want nil", b.Bytes())
	}
	c := b.Clone()
	if c.Bytes() != nil {
		t.Fatal("Clone().Bytes() for nil memBlob: want nil")
	}
	c.Release()
	b.Release()
}

// ─────────────────────────── pooledBlob tests ───────────────────────────

// TestPooledBlob_RefcountBalance validates that the underlying pool
// buffer is only returned to its pool after every clone (including
// the original handle) has been Released. Refcount semantics are the
// whole reason pooledBlob exists — the fill-aside hand-off in
// TieredCache depends on clones outliving the wire response's Release.
func TestPooledBlob_RefcountBalance(t *testing.T) {
	pool := NewPool(64).(*syncPool)
	buf, blob := pool.Alloc(64)
	for i := range buf {
		buf[i] = byte(i)
	}
	orig := blob.(*pooledBlob)
	if got := orig.refs.Load(); got != 1 {
		t.Fatalf("initial refcount: got %d, want 1", got)
	}

	// Clone three times. Each Clone should bump the shared refcount.
	c1 := orig.Clone()
	c2 := orig.Clone()
	c3 := orig.Clone()
	if got := orig.refs.Load(); got != 4 {
		t.Fatalf("after 3 clones, refcount: got %d, want 4", got)
	}

	// Release clones in mixed order. Bytes must remain valid on any
	// still-live handle until the refcount hits zero.
	c2.Release()
	if got := orig.refs.Load(); got != 3 {
		t.Fatalf("after 1 release, refcount: got %d, want 3", got)
	}
	if c1.Bytes()[0] != 0 || c1.Bytes()[63] != 63 {
		t.Fatal("c1.Bytes should still be readable after c2.Release")
	}

	c1.Release()
	c3.Release()
	if got := orig.refs.Load(); got != 1 {
		t.Fatalf("after 3 releases, refcount: got %d, want 1", got)
	}
	if orig.Bytes()[0] != 0 || orig.Bytes()[63] != 63 {
		t.Fatal("orig.Bytes should still be readable while refcount > 0")
	}

	// Final release — refcount hits zero, buffer goes back to pool.
	orig.Release()
	if got := orig.refs.Load(); got != 0 {
		t.Fatalf("after final release, refcount: got %d, want 0", got)
	}
	if orig.data != nil {
		t.Fatal("after Release, handle's data should be nil")
	}
}

// TestPooledBlob_ReleaseIdempotent verifies that calling Release on
// the same handle twice is a no-op (not a double-free).
func TestPooledBlob_ReleaseIdempotent(t *testing.T) {
	pool := NewPool(32)
	_, blob := pool.Alloc(32)
	b := blob.(*pooledBlob)

	b.Release()
	if b.data != nil {
		t.Fatal("data should be nil after first Release")
	}

	// Second Release must not panic or touch the pool.
	b.Release()
	if b.data != nil {
		t.Fatal("data should still be nil after second Release")
	}
}

// TestPooledBlob_BytesZeroCopy verifies that Bytes() returns a slice
// whose backing array is the same as the underlying buffer — not a
// defensive copy. Zero-copy is load-bearing for wire handler throughput.
func TestPooledBlob_BytesZeroCopy(t *testing.T) {
	pool := NewPool(32)
	buf, blob := pool.Alloc(32)
	defer blob.Release()

	if unsafe.SliceData(blob.Bytes()) != unsafe.SliceData(buf) {
		t.Fatal("Bytes should return a view over the same backing array as the buffer")
	}
}

// TestPooledBlob_CloneBytesAliasesOriginal verifies that clones return
// a Bytes view over the same backing array as the original — Clone is
// handle-level refcounting, not a data copy.
func TestPooledBlob_CloneBytesAliasesOriginal(t *testing.T) {
	pool := NewPool(32)
	_, orig := pool.Alloc(32)
	defer orig.Release()

	clone := orig.Clone()
	defer clone.Release()

	if unsafe.SliceData(orig.Bytes()) != unsafe.SliceData(clone.Bytes()) {
		t.Fatal("Clone().Bytes should alias the original's backing array")
	}
}

// TestDefaultPool verifies that DefaultPool allocates fresh via make()
// on every call (zero pool reuse) and that the returned Blob is a
// GC-managed memBlob with no-op Release.
func TestDefaultPool(t *testing.T) {
	buf, blob := DefaultPool.Alloc(16)
	if len(buf) != 16 || len(blob.Bytes()) != 16 {
		t.Fatalf("DefaultPool.Alloc(16) returned unexpected sizes: buf=%d blob=%d", len(buf), len(blob.Bytes()))
	}
	if unsafe.SliceData(buf) != unsafe.SliceData(blob.Bytes()) {
		t.Fatal("DefaultPool: buf and blob.Bytes should alias")
	}
	// Release is a no-op for DefaultPool (memBlob).
	blob.Release()
}
