package fetch

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// resolvedRun is the package-private visibility result shared by RunAt,
// ReadAt, and Prefetch. leaf is always the final non-layered serving Stream.
type resolvedRun struct {
	leaf       Stream
	kind       sparse.RunKind
	end        uint64
	chunkIndex uint64
	hasChunk   bool
}

// resolveStream resolves one visible run in [offset, requestedEnd). Nested
// layered streams are traversed recursively so key and chunk ownership remain
// attached to the final leaf. A child that is already out of bounds is a Hole
// through requestedEnd and is not called.
func resolveStream(stream Stream, offset, requestedEnd uint64) (resolvedRun, error) {
	if stream == nil {
		return resolvedRun{}, fmt.Errorf("%w: nil stream", errInvalidRun)
	}
	if requestedEnd <= offset {
		return resolvedRun{}, fmt.Errorf("%w: [%d,%d)", errInvalidRun, offset, requestedEnd)
	}
	if offset >= stream.Size() {
		return resolvedRun{kind: sparse.Hole, end: requestedEnd}, nil
	}

	localEnd := requestedEnd
	if localEnd > stream.Size() {
		localEnd = stream.Size()
	}

	var (
		run resolvedRun
		err error
	)
	if layered, ok := stream.(*layeredStream); ok {
		run, err = layered.resolveRun(offset, localEnd)
	} else {
		run.leaf = stream
		if chunked, ok := stream.(chunkStream); ok {
			run.kind, run.end, run.chunkIndex, err = chunked.RunChunkAt(offset, localEnd-offset)
			run.hasChunk = err == nil && run.kind != sparse.Hole
		} else {
			run.kind, run.end, err = stream.RunAt(offset, localEnd-offset)
		}
	}
	if err != nil {
		return resolvedRun{}, err
	}
	if err := validateResolvedRun(run, offset, localEnd); err != nil {
		return resolvedRun{}, err
	}

	if run.kind == sparse.Hole {
		run.leaf = nil
		run.chunkIndex = 0
		run.hasChunk = false
		// Once a child Hole reaches that child's EOF, the child remains
		// transparent. Extend the Hole through the caller's bound instead of
		// manufacturing a boundary that can repeat the same lower chunk Get.
		if run.end == localEnd && localEnd < requestedEnd {
			run.end = requestedEnd
		}
	}
	return run, nil
}

func validateResolvedRun(run resolvedRun, offset, requestedEnd uint64) error {
	switch run.kind {
	case sparse.Hole, sparse.Zero, sparse.Data:
	default:
		return fmt.Errorf("%w: unknown kind %d at %d", errInvalidRun, run.kind, offset)
	}
	if run.end <= offset || run.end > requestedEnd {
		return fmt.Errorf("%w: run at %d ends at %d, bound %d", errInvalidRun, offset, run.end, requestedEnd)
	}
	if run.kind != sparse.Hole && run.leaf == nil {
		return fmt.Errorf("%w: kind %d at %d has no serving leaf", errInvalidRun, run.kind, offset)
	}
	return nil
}

// resolveRun overlays direct children top to bottom. Every upper Hole tightens
// bound before a lower (possibly nested) child is queried, so a lower Data run
// can never cross the next point where an upper child starts serving again.
func (ls *layeredStream) resolveRun(offset, requestedEnd uint64) (resolvedRun, error) {
	if offset >= ls.size {
		return resolvedRun{}, io.EOF
	}
	if requestedEnd <= offset || requestedEnd > ls.size {
		return resolvedRun{}, fmt.Errorf("%w: layered range [%d,%d), size %d", errInvalidRun, offset, requestedEnd, ls.size)
	}

	bound := requestedEnd
	for _, layer := range ls.layers {
		run, err := resolveStream(layer, offset, bound)
		if err != nil {
			return resolvedRun{}, err
		}
		if run.kind == sparse.Hole {
			bound = run.end
			continue
		}
		return run, nil
	}
	return resolvedRun{kind: sparse.Hole, end: bound}, nil
}

type plannedRun struct {
	offset uint64
	run    resolvedRun
}

func planResolvedRuns(ctx context.Context, stream Stream, offset, end uint64) ([]plannedRun, error) {
	plan := make([]plannedRun, 0)
	for current := offset; current < end; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		run, err := resolveStream(stream, current, end)
		if err != nil {
			return nil, err
		}
		plan = append(plan, plannedRun{offset: current, run: run})
		current = run.end
	}
	return plan, nil
}

// readResolvedAt first plans every run, then performs any buffer mutation or
// data I/O. This makes a later resolver error atomic with respect to reads.
func readResolvedAt(ctx context.Context, stream Stream, buf []byte, offset uint64) (int, error) {
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
	plan, err := planResolvedRuns(ctx, stream, offset, end)
	if err != nil {
		return 0, err
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	var once sync.Once
	report := func(err error) {
		once.Do(func() {
			errCh <- err
			cancel()
		})
	}

	for _, item := range plan {
		lo, hi := item.offset, item.run.end
		dst := buf[lo-offset : hi-offset]
		if item.run.kind != sparse.Data {
			clearSlice(dst)
			continue
		}

		wg.Add(1)
		go func(item plannedRun, dst []byte) {
			defer wg.Done()
			var (
				n   int
				err error
			)
			if item.run.hasChunk {
				chunked, ok := item.run.leaf.(chunkStream)
				if !ok {
					report(fmt.Errorf("%w: serving leaf lost chunkStream", errInvalidRun))
					return
				}
				n, err = chunked.ReadChunkAt(cctx, dst, item.run.chunkIndex, item.offset, item.run.end)
			} else {
				n, err = item.run.leaf.ReadAt(cctx, dst, item.offset)
			}
			if err != nil {
				report(fmt.Errorf("fetch: read [%d,%d): %w", item.offset, item.run.end, err))
				return
			}
			if n != len(dst) {
				report(fmt.Errorf("fetch: short read [%d,%d): got %d of %d bytes", item.offset, item.run.end, n, len(dst)))
			}
		}(item, dst)
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return 0, err
	default:
	}
	return int(readLen), eof
}

func prefetchStream(ctx context.Context, stream Stream) error {
	for offset, end := uint64(0), stream.Size(); offset < end; {
		if err := ctx.Err(); err != nil {
			return err
		}
		run, err := resolveStream(stream, offset, end)
		if err != nil {
			return err
		}
		if run.kind == sparse.Data {
			if chunked, ok := run.leaf.(prefetchChunkStream); ok && run.hasChunk {
				if err := chunked.PrefetchChunkAt(ctx, run.chunkIndex); err != nil {
					return err
				}
			}
		}
		offset = run.end
	}
	return nil
}
