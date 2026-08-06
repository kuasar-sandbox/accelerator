package fetch

import (
	"context"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// layeredStream overlays several Streams (top → bottom) into one Stream: the
// topmost layer that holds data at an offset serves it; a declared hole (or
// out-of-bounds) falls through to lower layers; an IsZero chunk is opaque and
// does not fall through. Size is the maximum over layers.
//
// RunAt returns the final serving child's Run directly, preserving optional
// capabilities such as ChunkRun through arbitrary nested overlays.
type layeredStream struct {
	layers []Stream // top → bottom
	size   uint64   // max Size over layers
}

// NewLayered overlays layers (top → bottom) into one Stream. A single layer is
// returned as-is. Callers must pass at least one layer.
func NewLayered(layers ...Stream) Stream {
	if len(layers) == 1 {
		return layers[0]
	}
	var max uint64
	for _, l := range layers {
		if sz := l.Size(); sz > max {
			max = sz
		}
	}
	return &layeredStream{layers: layers, size: max}
}

func (ls *layeredStream) Size() uint64 { return ls.size }

func (ls *layeredStream) Close() error {
	var firstErr error
	for _, l := range ls.layers {
		if err := l.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (ls *layeredStream) RunAt(offset, limit uint64) (sparse.Run, error) {
	requestedEnd, err := boundedRunEnd(ls.size, offset, limit)
	if err != nil {
		return nil, err
	}

	bound := requestedEnd
	for _, layer := range ls.layers {
		if offset >= layer.Size() {
			continue
		}
		localEnd := bound
		if localEnd > layer.Size() {
			localEnd = layer.Size()
		}
		run, err := layer.RunAt(offset, localEnd-offset)
		if err != nil {
			releaseRun(run)
			return nil, err
		}
		if err := validateRun(run, offset, localEnd); err != nil {
			releaseRun(run)
			return nil, err
		}
		if run.Kind() != sparse.Hole {
			return run, nil
		}
		runEnd := run.End()
		releaseRun(run)

		// A child that remains Hole through its own EOF is transparent
		// through the caller's bound; otherwise its next visible boundary
		// tightens every lower-layer query.
		if runEnd == localEnd && localEnd < bound {
			continue
		}
		bound = runEnd
	}
	return newHoleRun(offset, bound), nil
}

// ReadAt walks final visible runs and reads Data runs concurrently; Hole and
// Zero runs are zero-filled in place.
func (ls *layeredStream) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	return readStreamAt(ctx, ls, buf, offset)
}

func (ls *layeredStream) Prefetch(ctx context.Context) error {
	return prefetchStream(ctx, ls)
}
