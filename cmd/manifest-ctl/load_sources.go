package main

import (
	"context"
	"errors"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"io"
)

// capturedInput saves only a declared, bounded dense tail. Payload bytes are
// consumed monotonically and never retained. Finish drains any unread suffix.
type capturedInput struct {
	sparse.Source
	boundary, pos uint64
	tail          []byte
}

func newCapturedInput(src sparse.Source, capture bool) (*capturedInput, error) {
	boundary := src.Size()
	if p, ok := src.(tarstream.IdentityProvider); ok {
		boundary, _, _ = p.PayloadCommitment()
	}
	if boundary > src.Size() {
		return nil, errors.New("invalid declared payload boundary")
	}
	if src.Size()-boundary > maxTransferTail {
		return nil, errors.New("declared tail exceeds memory budget")
	}
	c := &capturedInput{Source: src, boundary: boundary}
	if capture {
		c.tail = make([]byte, src.Size()-boundary)
	}
	return c, nil
}
func (c *capturedInput) PayloadCommitment() (uint64, [32]byte, bool) {
	return c.boundary, [32]byte{}, false
}
func (c *capturedInput) TarStreamDigest(string) ([32]byte, bool) { return [32]byte{}, false }
func (c *capturedInput) RunAt(off, limit uint64) (sparse.Run, error) {
	run, err := c.Source.RunAt(off, limit)
	if err != nil {
		return nil, err
	}
	return capturedRun{Run: run, owner: c}, nil
}

type capturedRun struct {
	sparse.Run
	owner *capturedInput
}

func (r capturedRun) ReadAt(ctx context.Context, b []byte, inner uint64) (int, error) {
	if inner > r.End()-r.Offset() || uint64(len(b)) > r.End()-r.Offset()-inner {
		return 0, errors.New("run read exceeds bounds")
	}
	if r.Kind() != sparse.Data {
		clear(b)
		return len(b), ctx.Err()
	}
	return r.owner.ReadAt(ctx, b, r.Offset()+inner)
}
func (c *capturedInput) ReadAt(ctx context.Context, b []byte, off uint64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if off < c.pos {
		return 0, errors.New("sequential input cannot be replayed")
	}
	// A window can start inside the tail: capture skipped tail bytes first.
	if len(c.tail) > 0 && off > c.boundary && c.pos < off {
		start := max(c.pos, c.boundary)
		end := min(off, c.Size())
		if start < end {
			n, err := c.Source.ReadAt(ctx, c.tail[start-c.boundary:end-c.boundary], start)
			if err != nil {
				return n, err
			}
			if uint64(n) != end-start {
				return n, io.ErrUnexpectedEOF
			}
			c.pos = end
		}
	}
	n, err := c.Source.ReadAt(ctx, b, off)
	if n > 0 {
		end := off + uint64(n)
		if len(c.tail) > 0 && end > c.boundary {
			start := max(off, c.boundary)
			copy(c.tail[start-c.boundary:end-c.boundary], b[start-off:uint64(n)])
		}
		c.pos = end
	}
	return n, err
}
func (c *capturedInput) Finish(ctx context.Context) error {
	buffer := make([]byte, 64<<10)
	for c.pos < c.Size() {
		off := c.pos
		run, err := c.Source.RunAt(off, c.Size()-off)
		if err != nil {
			return err
		}
		if run == nil || run.End() <= off || run.End() > c.Size() {
			return errors.New("invalid sequential source run")
		}
		if run.Kind() == sparse.Hole || run.Kind() == sparse.Zero {
			c.pos = run.End()
			continue
		}
		size := min(uint64(len(buffer)), run.End()-off)
		n, err := c.ReadAt(ctx, buffer[:size], off)
		if err != nil {
			return err
		}
		if uint64(n) != size {
			return io.ErrUnexpectedEOF
		}
	}
	return ctx.Err()
}
