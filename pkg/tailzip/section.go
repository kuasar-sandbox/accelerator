package tailzip

import (
	"context"
	"errors"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"io"
)

// Section borrows a bounded logical range without owning or buffering Source.
// Unshifted runs preserve the underlying optional ChunkRun capability.
type Section struct {
	Source       sparse.Source
	Base, Length uint64
}

func (s Section) Size() uint64 { return s.Length }
func (s Section) valid() error {
	if s.Source == nil || s.Base > s.Source.Size() || s.Length > s.Source.Size()-s.Base {
		return errors.New("tailzip: section outside source")
	}
	return nil
}
func (s Section) RunAt(off, limit uint64) (sparse.Run, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	if off >= s.Length {
		return nil, io.EOF
	}
	if limit == 0 {
		return nil, errors.New("tailzip: zero run limit")
	}
	limit = min(limit, s.Length-off)
	run, err := s.Source.RunAt(s.Base+off, limit)
	if err != nil {
		return nil, err
	}
	if run == nil || run.Offset() != s.Base+off || run.End() <= s.Base+off || run.End() > s.Base+off+limit {
		return nil, errors.New("tailzip: source returned invalid run")
	}
	if s.Base == 0 {
		return run, nil
	}
	return sectionRun{Run: run, offset: off, end: run.End() - s.Base}, nil
}
func (s Section) ReadAt(ctx context.Context, b []byte, off uint64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := s.valid(); err != nil {
		return 0, err
	}
	if off >= s.Length {
		return 0, io.EOF
	}
	var eof error
	if uint64(len(b)) > s.Length-off {
		b = b[:s.Length-off]
		eof = io.EOF
	}
	n, err := s.Source.ReadAt(ctx, b, s.Base+off)
	if err != nil && err != io.EOF {
		return n, err
	}
	if n != len(b) {
		return n, io.ErrUnexpectedEOF
	}
	return n, eof
}
func (s Section) PayloadCommitment() (uint64, [32]byte, bool) {
	if s.Base == 0 && s.Source != nil {
		if p, ok := s.Source.(tarstream.IdentityProvider); ok {
			size, digest, valid := p.PayloadCommitment()
			if s.Length == s.Source.Size() {
				return size, digest, valid
			}
			if size == s.Length {
				return size, digest, valid
			}
		}
	}
	return s.Length, [32]byte{}, false
}
func (s Section) TarStreamDigest(name string) ([32]byte, bool) {
	if s.Base == 0 && s.Source != nil {
		if p, ok := s.Source.(tarstream.IdentityProvider); ok {
			if s.Length == s.Source.Size() {
				return p.TarStreamDigest(name)
			}
			size, digest, valid := s.PayloadCommitment()
			if valid && size == s.Length {
				v, err := tarstream.ComposeDigest(name, size, size, digest, nil)
				return v, err == nil
			}
		}
	}
	return [32]byte{}, false
}

type sectionRun struct {
	sparse.Run
	offset, end uint64
}

func (r sectionRun) Offset() uint64 { return r.offset }
func (r sectionRun) End() uint64    { return r.end }
func (r sectionRun) ReadAt(ctx context.Context, b []byte, inner uint64) (int, error) {
	if inner > r.end-r.offset || uint64(len(b)) > r.end-r.offset-inner {
		return 0, errors.New("tailzip: run read outside section")
	}
	return r.Run.ReadAt(ctx, b, inner)
}
