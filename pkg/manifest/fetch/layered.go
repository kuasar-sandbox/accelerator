package fetch

import (
	"context"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// layeredStream overlays several Streams (top → bottom) into one Stream: the
// topmost layer that holds data at an offset serves it; a declared hole (or
// out-of-bounds) falls through to lower layers; an IsZero chunk is opaque and
// does not fall through. Size is the maximum over layers.
//
// It exposes Stream and Prefetcher, but not ChunkStream: a layer's chunk index
// is layer-local. It consumes ChunkStream and PrefetchChunkStream internally,
// threading the final serving leaf's chunk index from resolve to read/prefetch.
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

func (ls *layeredStream) RunAt(offset, limit uint64) (sparse.RunKind, uint64, error) {
	requestedEnd, err := boundedRunEnd(ls.size, offset, limit)
	if err != nil {
		return 0, 0, err
	}
	run, err := ls.resolveRun(offset, requestedEnd)
	if err != nil {
		return 0, 0, err
	}
	return run.kind, run.end, nil
}

// ReadAt walks the resolved runs and delegates each Data run to its serving
// layer (ReadChunkAt for ChunkStream layers, ReadAt otherwise) concurrently;
// merged holes and serving zeros are zero-filled in place.
func (ls *layeredStream) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	return readResolvedAt(ctx, ls, buf, offset)
}

func (ls *layeredStream) Prefetch(ctx context.Context, keys ...store.ContentKey) error {
	return prefetchStream(ctx, ls, keys)
}
