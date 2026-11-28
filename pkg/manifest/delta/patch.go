package delta

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/delta/internal/vcdiff"
)

var encoders = sync.Pool{New: func() any { return vcdiff.NewEncoder() }}

// Encode returns the VCDIFF (RFC 3284) stream that rebuilds target from base.
// Deterministic: the same inputs always give the same bytes, which is what lets
// a delta object be re-derived and what keeps its address stable. Safe for
// concurrent use; encoders (which are not) come from a pool.
func Encode(base, target []byte) []byte {
	e := encoders.Get().(*vcdiff.Encoder)
	defer encoders.Put(e)
	return e.EncodeBytes(base, target)
}

// ErrBadDelta marks a stream that is malformed, oversize, or does not decode to
// the size the object header declared.
var ErrBadDelta = errors.New("delta: invalid VCDIFF stream")

// Apply rebuilds the target from base and a VCDIFF stream. targetSize is the
// plaintext size the delta object's header declared; the decode is bounded by
// it (total output, per-section lengths, per-instruction length) and must
// produce exactly that many bytes, so a hostile stream cannot make the reader
// allocate or loop beyond what the header already promised.
func Apply(base, stream []byte, targetSize int) ([]byte, error) {
	if targetSize < 0 {
		return nil, fmt.Errorf("%w: negative target size", ErrBadDelta)
	}
	out := make([]byte, targetSize)
	if err := ApplyTo(base, stream, out); err != nil {
		return nil, err
	}
	return out, nil
}

// ApplyTo is Apply writing into a caller-owned buffer of exactly the declared
// target size. The reader uses it so a delta chunk lands directly in the
// destination instead of being decoded twice.
func ApplyTo(base, stream, dst []byte) error {
	out := &exactWriter{buf: dst}
	n, err := vcdiff.DecodeLimited(base, byteReader(stream), out, vcdiff.Limits{
		MaxTarget:  int64(len(dst)),
		MaxSection: int64(len(stream)),
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBadDelta, err)
	}
	if n != int64(len(dst)) || out.n != len(dst) {
		return fmt.Errorf("%w: decoded %d bytes, want %d", ErrBadDelta, n, len(dst))
	}
	return nil
}

// exactWriter fills a preallocated slice and refuses to grow it.
type exactWriter struct {
	buf []byte
	n   int
}

func (w *exactWriter) Write(p []byte) (int, error) {
	if len(p) > len(w.buf)-w.n {
		return 0, io.ErrShortBuffer
	}
	copy(w.buf[w.n:], p)
	w.n += len(p)
	return len(p), nil
}

type sliceReader struct {
	b []byte
	i int
}

func byteReader(b []byte) *sliceReader { return &sliceReader{b: b} }

func (r *sliceReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}
