package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

type holeRun struct {
	offset uint64
	end    uint64
}

func (r *holeRun) Offset() uint64       { return r.offset }
func (r *holeRun) End() uint64          { return r.end }
func (r *holeRun) Kind() sparse.RunKind { return sparse.Hole }
func (r *holeRun) ReadAt(ctx context.Context, buf []byte, innerOffset uint64) (int, error) {
	return readZeroRun(ctx, r.offset, r.end, buf, innerOffset)
}
func (r *holeRun) release() {
	*r = holeRun{}
	holeRunPool.Put(r)
}

type zeroRun struct {
	offset uint64
	end    uint64
}

func (r *zeroRun) Offset() uint64       { return r.offset }
func (r *zeroRun) End() uint64          { return r.end }
func (r *zeroRun) Kind() sparse.RunKind { return sparse.Zero }
func (r *zeroRun) ReadAt(ctx context.Context, buf []byte, innerOffset uint64) (int, error) {
	return readZeroRun(ctx, r.offset, r.end, buf, innerOffset)
}
func (r *zeroRun) release() {
	*r = zeroRun{}
	zeroRunPool.Put(r)
}

var (
	holeRunPool = sync.Pool{New: func() any { return new(holeRun) }}
	zeroRunPool = sync.Pool{New: func() any { return new(zeroRun) }}
	dataRunPool = sync.Pool{New: func() any { return new(manifestDataRun) }}
)

func newHoleRun(offset, end uint64) *holeRun {
	r := holeRunPool.Get().(*holeRun)
	*r = holeRun{offset: offset, end: end}
	return r
}

func newZeroRun(offset, end uint64) *zeroRun {
	r := zeroRunPool.Get().(*zeroRun)
	*r = zeroRun{offset: offset, end: end}
	return r
}

type recyclableRun interface {
	sparse.Run
	release()
}

// releaseRun is used only after a package-internal operation has relinquished
// every reference to a Run. Runs returned to external callers are never
// released here, so they remain immutable for their documented Stream
// lifetime. sync.Pool reuse is opportunistic and does not grow with image size.
func releaseRun(run sparse.Run) {
	if recyclable, ok := run.(recyclableRun); ok {
		recyclable.release()
	}
}

func releaseRuns(runs []sparse.Run) {
	for _, run := range runs {
		releaseRun(run)
	}
}

func readZeroRun(ctx context.Context, offset, end uint64, buf []byte, innerOffset uint64) (int, error) {
	if end <= offset {
		return 0, fmt.Errorf("%w: invalid run [%d,%d)", errInvalidRun, offset, end)
	}
	runLength := end - offset
	if innerOffset > runLength || uint64(len(buf)) > runLength-innerOffset {
		return 0, fmt.Errorf("%w: read offset %d length %d outside [0,%d)", errInvalidRun, innerOffset, len(buf), runLength)
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	clearSlice(buf)
	return len(buf), nil
}

func validateRun(run sparse.Run, offset, requestedEnd uint64) error {
	if run == nil {
		return fmt.Errorf("%w: nil run at %d", errInvalidRun, offset)
	}
	switch run.Kind() {
	case sparse.Hole, sparse.Zero, sparse.Data:
	default:
		return fmt.Errorf("%w: unknown kind %d at %d", errInvalidRun, run.Kind(), offset)
	}
	if run.Offset() != offset {
		return fmt.Errorf("%w: run offset %d, want %d", errInvalidRun, run.Offset(), offset)
	}
	if run.End() <= offset || run.End() > requestedEnd {
		return fmt.Errorf("%w: run at %d ends at %d, bound %d", errInvalidRun, offset, run.End(), requestedEnd)
	}
	return nil
}

func validateRunRead(run sparse.Run, innerOffset uint64, length int) error {
	if run == nil || run.End() <= run.Offset() {
		return fmt.Errorf("%w: invalid run bounds", errInvalidRun)
	}
	runLength := run.End() - run.Offset()
	if innerOffset > runLength || uint64(length) > runLength-innerOffset {
		return fmt.Errorf("%w: read offset %d length %d outside [0,%d)", errInvalidRun, innerOffset, length, runLength)
	}
	return nil
}

func planRuns(ctx context.Context, stream Stream, offset, end uint64) ([]sparse.Run, error) {
	plan := make([]sparse.Run, 0)
	for current := offset; current < end; {
		if err := ctx.Err(); err != nil {
			releaseRuns(plan)
			return nil, err
		}
		run, err := stream.RunAt(current, end-current)
		if err != nil {
			releaseRun(run)
			releaseRuns(plan)
			return nil, err
		}
		if err := validateRun(run, current, end); err != nil {
			releaseRun(run)
			releaseRuns(plan)
			return nil, err
		}
		plan = append(plan, run)
		current = run.End()
	}
	return plan, nil
}

// readStreamAt resolves the complete range before performing payload I/O or
// modifying buf. Data runs then execute concurrently; the first failure
// cancels peers while every started operation is joined before return.
func readStreamAt(ctx context.Context, stream Stream, buf []byte, offset uint64) (int, error) {
	if offset >= stream.Size() {
		return 0, io.EOF
	}
	if len(buf) == 0 {
		return 0, nil
	}

	available := stream.Size() - offset
	readLen := uint64(len(buf))
	var eof error
	if readLen > available {
		readLen = available
		eof = io.EOF
	}
	end := offset + readLen
	plan, err := planRuns(ctx, stream, offset, end)
	if err != nil {
		return 0, err
	}
	defer releaseRuns(plan)
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var errorsMu sync.Mutex
	var readErrors []error
	report := func(err error) {
		errorsMu.Lock()
		// A peer's derived cancellation adds no cause. Keep the first actual
		// error and every later error that may contain a permanent cause.
		if len(readErrors) == 0 || !errors.Is(err, context.Canceled) || readerr.IsPermanent(err) {
			readErrors = append(readErrors, err)
		}
		cancel()
		errorsMu.Unlock()
	}

	for _, run := range plan {
		lo, hi := run.Offset(), run.End()
		dst := buf[lo-offset : hi-offset]
		if run.Kind() != sparse.Data {
			clearSlice(dst)
			continue
		}

		wg.Add(1)
		go func(run sparse.Run, dst []byte) {
			defer wg.Done()
			n, err := run.ReadAt(cctx, dst, 0)
			if n == len(dst) && !readerr.IsPermanent(err) && err == io.EOF {
				err = nil
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				err = readerr.Mark(err, false)
			}
			if err != nil {
				report(fmt.Errorf("fetch: read [%d,%d): %w", run.Offset(), run.End(), err))
				return
			}
			if n != len(dst) {
				report(readerr.Mark(fmt.Errorf("fetch: short read [%d,%d): got %d of %d bytes", run.Offset(), run.End(), n, len(dst)), false))
			}
		}(run, dst)
	}

	wg.Wait()
	if err := errors.Join(readErrors...); err != nil {
		return 0, err
	}
	return int(readLen), eof
}

func prefetchStream(ctx context.Context, stream Stream) error {
	for offset, end := uint64(0), stream.Size(); offset < end; {
		if err := ctx.Err(); err != nil {
			return err
		}
		run, err := stream.RunAt(offset, end-offset)
		if err != nil {
			releaseRun(run)
			return err
		}
		if err := validateRun(run, offset, end); err != nil {
			releaseRun(run)
			return err
		}
		next := run.End()
		if run.Kind() == sparse.Data {
			if chunk, ok := run.(prefetchChunkRun); ok {
				if err := chunk.prefetch(ctx); err != nil {
					releaseRun(run)
					return err
				}
			}
		}
		releaseRun(run)
		offset = next
	}
	return nil
}
