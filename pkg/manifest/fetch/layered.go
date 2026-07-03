package fetch

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// layeredStream overlays several Streams (top → bottom) into one Stream: the
// topmost layer that holds data at an offset serves it; a declared hole (or
// out-of-bounds) falls through to lower layers; an IsZero chunk is opaque and
// does not fall through. Size is the maximum over layers.
//
// It is Stream-only: a layer's chunk index is layer-local, so layeredStream does
// not expose ChunkStream. It does, however, consume a layer's ChunkStream
// internally (threading the serving chunk index from resolve to read).
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

// resolve walks layers top → bottom: an out-of-bounds layer is a permanent hole
// (skip); a layer with a declared hole at offset falls through but bounds the
// run at its hole end; the first layer serving a chunk wins, its run end further
// bounding the run. No serving layer ⇒ a merged hole. For a serving ChunkStream
// layer it returns the chunk index (hasChunk=true) so the read can skip a
// re-lookup.
func (ls *layeredStream) resolve(offset, limit uint64) (li int, kind sparse.RunKind, end, chunkIdx uint64, hasChunk bool) {
	end = offset + limit
	if end < offset || end > ls.size {
		end = ls.size
	}
	for i, layer := range ls.layers {
		if offset >= layer.Size() {
			continue // out-of-bounds = hole for this layer
		}
		var k sparse.RunKind
		var e, idx uint64
		var hc bool
		var err error
		if cs, ok := layer.(ChunkStream); ok {
			k, e, idx, err = cs.RunChunkAt(offset, limit)
			hc = true
		} else {
			k, e, err = layer.RunAt(offset, limit)
		}
		if err != nil { // io.EOF (out-of-bounds) — defensive, offset < Size already
			continue
		}
		if k == sparse.Hole {
			if e < end {
				end = e // this layer resumes serving at its hole end
			}
			continue
		}
		if e < end {
			end = e
		}
		return i, k, end, idx, hc
	}
	return -1, sparse.Hole, end, 0, false
}

func (ls *layeredStream) RunAt(offset, limit uint64) (sparse.RunKind, uint64, error) {
	if offset >= ls.size {
		return 0, 0, io.EOF
	}
	_, kind, end, _, _ := ls.resolve(offset, limit)
	return kind, end, nil
}

// ReadAt walks the resolved runs and delegates each Data run to its serving
// layer (ReadChunkAt for ChunkStream layers, ReadAt otherwise) concurrently;
// merged holes and serving zeros are zero-filled in place.
func (ls *layeredStream) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	if offset >= ls.size {
		return 0, io.EOF
	}
	if len(buf) == 0 {
		return 0, nil
	}
	end := offset + uint64(len(buf))
	var eof error
	if end > ls.size {
		end = ls.size
		eof = io.EOF
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	var once sync.Once

	for cur := offset; cur < end; {
		li, kind, runEnd, idx, hasChunk := ls.resolve(cur, end-cur)
		dst := buf[cur-offset : runEnd-offset]
		if kind == sparse.Data {
			layer := ls.layers[li]
			wg.Add(1)
			go func(layer Stream, idx, lo, hi uint64, hasChunk bool, dst []byte) {
				defer wg.Done()
				var err error
				if hasChunk {
					_, err = layer.(ChunkStream).ReadChunkAt(cctx, dst, idx, lo, hi)
				} else {
					_, err = layer.ReadAt(cctx, dst, lo)
				}
				if err != nil && !errors.Is(err, io.EOF) {
					once.Do(func() { errCh <- err; cancel() })
				}
			}(layer, idx, cur, runEnd, hasChunk, dst)
		} else { // Hole | Zero
			clearSlice(dst)
		}
		cur = runEnd
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return 0, err
	default:
	}
	return int(end - offset), eof
}
