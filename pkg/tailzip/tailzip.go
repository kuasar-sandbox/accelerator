// Package tailzip locates, validates, encodes, and composes ZIP archives that
// are appended to another logical byte stream.  It deliberately knows nothing
// about the entries' application schema.
package tailzip

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

const (
	maxEOCDSearch = 22 + 1<<16
	defaultLimit  = 64 << 20
)

var ErrNotFound = errors.New("tailzip: suffix ZIP not found")

// Entry is one deterministic STORED entry. Entry order is preserved.
type Entry struct {
	Name string
	Body []byte
}

// Options controls structural validation. Zero value is the permissive image
// profile and remains compatible with ordinary archive/zip output.
type Options struct {
	MaxSize       int64
	MaxEntries    int
	KnownEntries  []string
	RequireOrder  bool
	RequireStored bool
}

// Tail identifies the ZIP suffix and its payload boundary.
type Tail struct{ Offset, Size int64 }

func (t Tail) PayloadSize() int64 { return t.Offset }

// Locate finds a ZIP whose EOCD ends exactly at size. Random reads are bounded
// by the ZIP format's 64 KiB comment window plus central-directory metadata.
func Locate(r io.ReaderAt, size int64, opt Options) (found Tail, retErr error) {
	observed := &checkedReaderAt{ReaderAt: r}
	defer func() {
		if err := observed.failure(); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()
	return locate(observed, size, opt)
}
func locate(r io.ReaderAt, size int64, opt Options) (Tail, error) {
	if r == nil {
		return Tail{}, fmt.Errorf("tailzip: reader is required")
	}
	if opt.MaxSize < 0 || opt.MaxEntries < 0 {
		return Tail{}, fmt.Errorf("tailzip: negative validation limit")
	}
	if size < 0 {
		return Tail{}, fmt.Errorf("tailzip: negative size")
	}
	n := int64(maxEOCDSearch)
	if n > size {
		n = size
	}
	buf := make([]byte, n)
	if n > 0 {
		count, err := r.ReadAt(buf, size-n)
		if err != nil && err != io.EOF {
			return Tail{}, fmt.Errorf("tailzip: read suffix: %w", err)
		}
		if count != len(buf) {
			return Tail{}, io.ErrUnexpectedEOF
		}
	}
	for i := len(buf) - 22; i >= 0; i-- {
		if binary.LittleEndian.Uint32(buf[i:]) != 0x06054b50 {
			continue
		}
		comment := int(binary.LittleEndian.Uint16(buf[i+20 : i+22]))
		if i+22+comment != len(buf) {
			continue
		}
		footer, err := parseEndRecord(buf[i:i+22], uint64(size-n+int64(i)))
		if err != nil {
			return Tail{}, err
		}
		t := Tail{Offset: int64(footer.Base), Size: size - int64(footer.Base)}

		limit := opt.MaxSize
		if limit == 0 {
			limit = defaultLimit
		}
		if limit > 0 && t.Size > limit {
			return Tail{}, fmt.Errorf("tailzip: suffix size %d exceeds limit %d", t.Size, limit)
		}
		if opt.MaxEntries > 0 && footer.Count > opt.MaxEntries {
			return Tail{}, fmt.Errorf("tailzip: %d entries exceed limit %d", footer.Count, opt.MaxEntries)
		}
		if err := checkDirectory(r, footer); err != nil {
			return Tail{}, err
		}
		if err := validateAt(r, t, opt); err != nil {
			return Tail{}, err
		}
		return t, nil
	}
	if bytes.Contains(buf, []byte{0x50, 0x4b, 0x05, 0x06}) {
		return Tail{}, fmt.Errorf("tailzip: recognizable EOCD does not end at logical EOF")
	}
	return Tail{}, ErrNotFound
}

func validateAt(r io.ReaderAt, t Tail, opt Options) error {
	zr, err := zip.NewReader(io.NewSectionReader(r, t.Offset, t.Size), t.Size)
	if err != nil {
		return fmt.Errorf("tailzip: open suffix: %w", err)
	}
	if opt.MaxEntries > 0 && len(zr.File) > opt.MaxEntries {
		return fmt.Errorf("tailzip: %d entries exceed limit %d", len(zr.File), opt.MaxEntries)
	}
	known := make(map[string]int, len(opt.KnownEntries))
	for i, n := range opt.KnownEntries {
		known[n] = i
	}
	remaining := opt.MaxSize
	if remaining == 0 {
		remaining = defaultLimit
	}
	for i, f := range zr.File {
		if f.UncompressedSize64 > uint64(remaining) {
			return fmt.Errorf("tailzip: decoded metadata exceeds size limit")
		}
		remaining -= int64(f.UncompressedSize64)
		if len(known) > 0 {
			want, ok := known[f.Name]
			if !ok {
				return fmt.Errorf("tailzip: unknown entry %q", f.Name)
			}
			if opt.RequireOrder && want != i {
				return fmt.Errorf("tailzip: entry %q is out of order", f.Name)
			}
		}
		if opt.RequireStored && f.Method != zip.Store {
			return fmt.Errorf("tailzip: entry %q is not stored", f.Name)
		}
		rc, err := f.Open()
		if err != nil {
			return fmt.Errorf("tailzip: open %q: %w", f.Name, err)
		}
		_, copyErr := io.Copy(io.Discard, rc)
		closeErr := rc.Close()
		if copyErr != nil {
			return fmt.Errorf("tailzip: verify %q: %w", f.Name, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("tailzip: close %q: %w", f.Name, closeErr)
		}
	}
	return nil
}

// Open validates and returns an archive/zip reader scoped to the suffix.
func Open(r io.ReaderAt, size int64, opt Options) (reader *zip.Reader, found Tail, retErr error) {
	observed := &checkedReaderAt{ReaderAt: r}
	defer func() {
		if err := observed.failure(); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()
	return open(observed, size, opt)
}
func open(r io.ReaderAt, size int64, opt Options) (*zip.Reader, Tail, error) {
	t, err := Locate(r, size, opt)
	if err != nil {
		return nil, Tail{}, err
	}
	zr, err := zip.NewReader(io.NewSectionReader(r, t.Offset, t.Size), t.Size)
	if err != nil {
		return nil, Tail{}, fmt.Errorf("tailzip: open suffix: %w", err)
	}
	return zr, t, nil
}

// Read returns a bounded copy of the complete suffix.
func Read(r io.ReaderAt, size int64, opt Options) ([]byte, Tail, error) {
	t, err := Locate(r, size, opt)
	if err != nil {
		return nil, Tail{}, err
	}
	b := make([]byte, t.Size)
	n, err := r.ReadAt(b, t.Offset)
	if err != nil && err != io.EOF {
		return nil, Tail{}, fmt.Errorf("tailzip: read archive: %w", err)
	}
	if n != len(b) {
		return nil, Tail{}, io.ErrUnexpectedEOF
	}
	return b, t, nil
}

// Encode emits deterministic STORED ZIP bytes.
func Encode(entries []Entry) ([]byte, error) {
	if len(entries) >= math.MaxUint16 {
		return nil, fmt.Errorf("tailzip: too many entries")
	}
	encodedSize := uint64(22)
	for _, e := range entries {
		if len(e.Name) > math.MaxUint16 {
			return nil, fmt.Errorf("tailzip: entry name too long")
		}
		encodedSize += uint64(30+46+18+16+len(e.Name)*2) + uint64(len(e.Body))
		if encodedSize > defaultLimit {
			return nil, fmt.Errorf("tailzip: encoded tail exceeds size limit")
		}
	}
	var out bytes.Buffer
	zw := zip.NewWriter(&out)
	epoch := time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
	seen := map[string]bool{}
	for _, e := range entries {
		if e.Name == "" || seen[e.Name] {
			return nil, fmt.Errorf("tailzip: invalid or duplicate entry %q", e.Name)
		}
		seen[e.Name] = true
		h := &zip.FileHeader{Name: e.Name, Method: zip.Store, Modified: epoch}
		w, err := zw.CreateHeader(h)
		if err != nil {
			return nil, err
		}
		if _, err = w.Write(e.Body); err != nil {
			return nil, err
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// Validate validates standalone suffix bytes.
func Validate(data []byte, opt Options) error {
	t, err := Locate(bytes.NewReader(data), int64(len(data)), opt)
	if err != nil {
		return err
	}
	if t.Offset != 0 {
		return fmt.Errorf("tailzip: archive has a %d-byte prefix", t.Offset)
	}
	return nil
}

// Prefix exposes the logical payload before a located tail without reading it.
func Prefix(src sparse.Source, size uint64) (sparse.Source, error) {
	section := Section{Source: src, Length: size}
	if err := section.valid(); err != nil {
		return nil, err
	}
	if size == src.Size() {
		return src, nil
	}
	return section, nil
}

// Append composes a validated standalone suffix after src. Payload runs are
// forwarded unchanged; the suffix is one dense Data run backed by bounded
// memory. The caller retains ownership of src and tail.
func Append(src sparse.Source, tail []byte, opt Options) (sparse.Source, error) {
	if src == nil {
		return nil, fmt.Errorf("tailzip: source is required")
	}
	if err := Validate(tail, opt); err != nil {
		return nil, err
	}
	if uint64(len(tail)) > math.MaxUint64-src.Size() {
		return nil, fmt.Errorf("tailzip: composed size overflows uint64")
	}
	return &appendSource{s: src, tail: tail, size: src.Size() + uint64(len(tail))}, nil
}

type appendSource struct {
	s    sparse.Source
	tail []byte
	size uint64
}

func (a *appendSource) Size() uint64 { return a.size }
func (a *appendSource) RunAt(off, limit uint64) (sparse.Run, error) {
	if off >= a.size {
		return nil, io.EOF
	}
	if limit == 0 {
		return nil, fmt.Errorf("tailzip: zero run limit")
	}
	if off < a.s.Size() {
		if limit > a.s.Size()-off {
			limit = a.s.Size() - off
		}
		run, err := a.s.RunAt(off, limit)
		if err != nil {
			return nil, err
		}
		if run == nil || run.Offset() != off || run.End() <= off || run.End() > off+limit {
			return nil, fmt.Errorf("tailzip: payload returned invalid run")
		}
		return run, nil
	}
	end := off + limit
	if end < off || end > a.size {
		end = a.size
	}
	return &tailRun{start: off, end: end, base: a.s.Size(), data: a.tail}, nil
}
func (a *appendSource) ReadAt(ctx context.Context, b []byte, off uint64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if off >= a.size {
		return 0, io.EOF
	}
	length := len(b)
	var eof error
	if uint64(length) > a.size-off {
		length = int(a.size - off)
		eof = io.EOF
	}
	read := 0
	if off < a.s.Size() && length > 0 {
		count := min(uint64(length), a.s.Size()-off)
		n, err := a.s.ReadAt(ctx, b[:count], off)
		if err != nil && err != io.EOF {
			return n, err
		}
		if uint64(n) != count {
			return n, io.ErrUnexpectedEOF
		}
		read = n
		off += uint64(n)
	}
	if read < length {
		read += copy(b[read:length], a.tail[off-a.s.Size():])
	}
	return read, eof
}

func (a *appendSource) PayloadCommitment() (uint64, [32]byte, bool) {
	if p, ok := a.s.(tarstream.IdentityProvider); ok {
		size, digest, valid := p.PayloadCommitment()
		if valid && size == a.s.Size() {
			return size, digest, true
		}
	}
	return a.s.Size(), [32]byte{}, false
}
func (a *appendSource) TarStreamDigest(name string) ([32]byte, bool) {
	size, digest, ok := a.PayloadCommitment()
	if !ok {
		return [32]byte{}, false
	}
	out, err := tarstream.ComposeDigest(name, a.size, size, digest, a.tail)
	return out, err == nil
}

type tailRun struct {
	start, end, base uint64
	data             []byte
}

func (r *tailRun) Offset() uint64       { return r.start }
func (r *tailRun) End() uint64          { return r.end }
func (r *tailRun) Kind() sparse.RunKind { return sparse.Data }
func (r *tailRun) ReadAt(ctx context.Context, b []byte, inner uint64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if inner > r.end-r.start || uint64(len(b)) > r.end-r.start-inner {
		return 0, fmt.Errorf("tailzip: run read outside bounds")
	}
	copy(b, r.data[r.start-r.base+inner:])
	return len(b), nil
}

// IsNotFound reports absence rather than malformed ZIP data.
func IsNotFound(err error) bool { return errors.Is(err, ErrNotFound) }

// Preserve source errors accompanying full buffers across ZIP metadata reads.
type checkedReaderAt struct {
	io.ReaderAt
	mu  sync.Mutex
	err error
}

func (r *checkedReaderAt) failure() error { r.mu.Lock(); defer r.mu.Unlock(); return r.err }
func (r *checkedReaderAt) ReadAt(b []byte, off int64) (int, error) {
	if err := r.failure(); err != nil {
		return 0, err
	}
	if r.ReaderAt == nil {
		return 0, errors.New("tailzip: reader is required")
	}
	n, err := r.ReaderAt.ReadAt(b, off)
	// Ordinary EOF with a complete buffer is handled by the ZIP reader.
	// Only actual source failures remain sticky across metadata reads.
	if err != nil && err != io.EOF {
		r.mu.Lock()
		if r.err == nil {
			r.err = err
		}
		r.mu.Unlock()
	}
	return n, err
}

// Check the declared count before archive/zip allocates its File objects and
// require archive-relative local offsets. Absolute-prefix archives are rejected
// instead of accidentally classifying their payload as part of the ZIP tail.
func checkDirectory(r io.ReaderAt, footer Footer) error {
	pos, end := footer.CentralStart, footer.CentralStart+footer.CentralSize
	var header [46]byte
	for i := 0; i < footer.Count; i++ {
		if pos > end || end-pos < 46 {
			return errors.New("tailzip: truncated central directory")
		}
		n, err := r.ReadAt(header[:], int64(pos))
		if err != nil && err != io.EOF {
			return err
		}
		if n != len(header) {
			return io.ErrUnexpectedEOF
		}
		if binary.LittleEndian.Uint32(header[:4]) != 0x02014b50 {
			return errors.New("tailzip: invalid central header")
		}
		offset := binary.LittleEndian.Uint32(header[42:46])
		if i == 0 && offset != 0 {
			return errors.New("tailzip: local ZIP offsets must be relative to the suffix")
		}
		if offset == math.MaxUint32 {
			return errors.New("tailzip: ZIP64 local offset is unsupported")
		}
		variable := uint64(binary.LittleEndian.Uint16(header[28:30])) + uint64(binary.LittleEndian.Uint16(header[30:32])) + uint64(binary.LittleEndian.Uint16(header[32:34]))
		if variable > end-pos-46 {
			return errors.New("tailzip: central fields exceed directory")
		}
		pos += 46 + variable
	}
	if pos != end {
		return errors.New("tailzip: EOCD entry count disagrees with directory")
	}
	if footer.Count == 0 && footer.CentralStart != footer.Base {
		return errors.New("tailzip: empty suffix contains a prefix")
	}
	return nil
}
