package fetch

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestResolverCarriesAncestorBoundThroughNestedLayers(t *testing.T) {
	const size = uint64(8)
	outerUpper := &resolverTestStream{
		size: size,
		run: func(offset, limit uint64) (sparse.RunKind, uint64, error) {
			if offset < 4 {
				return sparse.Hole, 4, nil
			}
			return sparse.Data, offset + limit, nil
		},
	}
	nestedUpper := resolverHoleStream(size)
	bottom := &resolverTestStream{
		size: size,
		run: func(offset, limit uint64) (sparse.RunKind, uint64, error) {
			return sparse.Data, offset + limit, nil
		},
	}
	nested := NewLayered(nestedUpper, bottom)
	stream := NewLayered(outerUpper, nested)

	run, err := resolveStream(stream, 0, size)
	if err != nil {
		t.Fatalf("resolveStream: %v", err)
	}
	if run.kind != sparse.Data || run.end != 4 || run.leaf != bottom {
		t.Fatalf("resolved run = {kind:%v end:%d leaf:%T}, want bottom Data through 4", run.kind, run.end, run.leaf)
	}
	if got := bottom.lastLimit.Load(); got != 4 {
		t.Fatalf("deepest leaf limit = %d, want ancestor bound 4", got)
	}

	kind, end, err := stream.RunAt(0, size)
	if err != nil || kind != sparse.Data || end != 4 {
		t.Fatalf("RunAt = (%v, %d, %v), want (Data, 4, nil)", kind, end, err)
	}
}

func TestResolverRejectsLeafErrorsAndInvalidRuns(t *testing.T) {
	sentinel := errors.New("sentinel resolver failure")
	tests := []struct {
		name        string
		run         func(offset, limit uint64) (sparse.RunKind, uint64, error)
		want        error
		wantInvalid bool
	}{
		{
			name: "error",
			run: func(uint64, uint64) (sparse.RunKind, uint64, error) {
				return 0, 0, sentinel
			},
			want: sentinel,
		},
		{
			name: "EOF below Size",
			run: func(uint64, uint64) (sparse.RunKind, uint64, error) {
				return 0, 0, io.EOF
			},
			want: io.EOF,
		},
		{
			name: "unknown kind",
			run: func(offset, _ uint64) (sparse.RunKind, uint64, error) {
				return sparse.RunKind(0xFF), offset + 1, nil
			},
			wantInvalid: true,
		},
		{
			name: "non advancing",
			run: func(offset, _ uint64) (sparse.RunKind, uint64, error) {
				return sparse.Data, offset, nil
			},
			wantInvalid: true,
		},
		{
			name: "end before offset",
			run: func(offset, _ uint64) (sparse.RunKind, uint64, error) {
				return sparse.Data, offset - 1, nil
			},
			wantInvalid: true,
		},
		{
			name: "end after requested bound",
			run: func(offset, limit uint64) (sparse.RunKind, uint64, error) {
				return sparse.Data, offset + limit + 1, nil
			},
			wantInvalid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leaf := &resolverTestStream{size: 8, run: tt.run}
			_, err := resolveStream(leaf, 2, 6)
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if tt.wantInvalid && !errors.Is(err, errInvalidRun) {
				t.Fatalf("error = %v, want errInvalidRun", err)
			}
			if err == nil {
				t.Fatal("invalid resolver result was accepted")
			}
		})
	}
}

func TestResolverUsesChunkStreamErrors(t *testing.T) {
	sentinel := errors.New("chunk classifier failed")
	base := &resolverTestStream{
		size: 8,
		run: func(offset, limit uint64) (sparse.RunKind, uint64, error) {
			return sparse.Data, offset + limit, nil
		},
	}
	chunked := &resolverTestChunkStream{
		resolverTestStream: base,
		runChunk: func(uint64, uint64) (sparse.RunKind, uint64, uint64, error) {
			return 0, 0, 0, sentinel
		},
	}

	if _, err := resolveStream(chunked, 0, 8); !errors.Is(err, sentinel) {
		t.Fatalf("resolveStream error = %v, want chunk sentinel", err)
	}
	if base.runCalls.Load() != 0 || chunked.runChunkCalls.Load() != 1 {
		t.Fatalf("classifier calls = RunAt:%d RunChunkAt:%d, want 0/1", base.runCalls.Load(), chunked.runChunkCalls.Load())
	}
}

func TestResolverFailuresPropagateThroughEveryOperation(t *testing.T) {
	sentinel := errors.New("leaf metadata failed")
	tests := []struct {
		name string
		run  func(offset, limit uint64) (sparse.RunKind, uint64, error)
		want error
	}{
		{
			name: "sentinel",
			run: func(uint64, uint64) (sparse.RunKind, uint64, error) {
				return 0, 0, sentinel
			},
			want: sentinel,
		},
		{
			name: "unexpected EOF",
			run: func(uint64, uint64) (sparse.RunKind, uint64, error) {
				return 0, 0, io.EOF
			},
			want: io.EOF,
		},
		{
			name: "invalid kind",
			run: func(offset, _ uint64) (sparse.RunKind, uint64, error) {
				return sparse.RunKind(99), offset + 1, nil
			},
			want: errInvalidRun,
		},
		{
			name: "invalid end",
			run: func(offset, _ uint64) (sparse.RunKind, uint64, error) {
				return sparse.Data, offset, nil
			},
			want: errInvalidRun,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newStream := func() (*resolverTestStream, Stream) {
				bad := &resolverTestStream{size: 8, run: tt.run}
				return bad, NewLayered(resolverHoleStream(8), bad)
			}

			_, stream := newStream()
			if _, _, err := stream.RunAt(0, 8); !errors.Is(err, tt.want) {
				t.Fatalf("RunAt error = %v, want %v", err, tt.want)
			}

			bad, stream := newStream()
			buf := []byte{0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5}
			if n, err := stream.ReadAt(context.Background(), buf, 0); n != 0 || !errors.Is(err, tt.want) {
				t.Fatalf("ReadAt = (%d, %v), want (0, %v)", n, err, tt.want)
			}
			if bad.readCalls.Load() != 0 {
				t.Fatalf("ReadAt performed %d data reads after resolver failure", bad.readCalls.Load())
			}

			bad, stream = newStream()
			prefetcher, ok := stream.(Prefetcher)
			if !ok {
				t.Fatal("layered stream does not implement Prefetcher")
			}
			if err := prefetcher.Prefetch(context.Background()); !errors.Is(err, tt.want) {
				t.Fatalf("Prefetch error = %v, want %v", err, tt.want)
			}
			if bad.readCalls.Load() != 0 {
				t.Fatalf("Prefetch performed %d data reads", bad.readCalls.Load())
			}
		})
	}
}

func TestReadAtPlansAllRunsBeforeStartingIO(t *testing.T) {
	sentinel := errors.New("second run metadata failure")
	leaf := &resolverTestStream{
		size: 8,
		run: func(offset, _ uint64) (sparse.RunKind, uint64, error) {
			if offset == 0 {
				return sparse.Data, 4, nil
			}
			return 0, 0, sentinel
		},
		read: func(_ context.Context, buf []byte, _ uint64) (int, error) {
			for i := range buf {
				buf[i] = 0xFF
			}
			return len(buf), nil
		},
	}
	stream := NewLayered(leaf, resolverHoleStream(8))
	buf := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	want := append([]byte(nil), buf...)

	n, err := stream.ReadAt(context.Background(), buf, 0)
	if n != 0 || !errors.Is(err, sentinel) {
		t.Fatalf("ReadAt = (%d, %v), want (0, sentinel)", n, err)
	}
	if got := leaf.readCalls.Load(); got != 0 {
		t.Fatalf("phase-two I/O started %d times before phase-one resolution completed", got)
	}
	for i := range buf {
		if buf[i] != want[i] {
			t.Fatalf("buffer mutated before planning completed: byte %d = %#x, want %#x", i, buf[i], want[i])
		}
	}
}

func TestReadAtRejectsNonChunkLeafShortReadAndFullEOF(t *testing.T) {
	tests := []struct {
		name string
		read func(context.Context, []byte, uint64) (int, error)
		want error
	}{
		{
			name: "short read",
			read: func(_ context.Context, buf []byte, _ uint64) (int, error) {
				return len(buf) - 1, nil
			},
		},
		{
			name: "full read with EOF",
			read: func(_ context.Context, buf []byte, _ uint64) (int, error) {
				return len(buf), io.EOF
			},
			want: io.EOF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leaf := &resolverTestStream{
				size: 8,
				run: func(offset, limit uint64) (sparse.RunKind, uint64, error) {
					return sparse.Data, offset + limit, nil
				},
				read: tt.read,
			}
			stream := NewLayered(leaf, resolverHoleStream(8))
			n, err := stream.ReadAt(context.Background(), make([]byte, 8), 0)
			if n != 0 || err == nil {
				t.Fatalf("ReadAt = (%d, %v), want (0, error)", n, err)
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("ReadAt error = %v, want %v", err, tt.want)
			}
			if tt.want == nil && !strings.Contains(err.Error(), "short read") {
				t.Fatalf("ReadAt error = %v, want short-read diagnostic", err)
			}
			if got := leaf.readCalls.Load(); got != 1 {
				t.Fatalf("leaf ReadAt calls = %d, want 1", got)
			}
		})
	}
}

type resolverTestStream struct {
	size      uint64
	run       func(offset, limit uint64) (sparse.RunKind, uint64, error)
	read      func(context.Context, []byte, uint64) (int, error)
	runCalls  atomic.Int64
	readCalls atomic.Int64
	lastLimit atomic.Uint64
}

func (s *resolverTestStream) Size() uint64 { return s.size }
func (s *resolverTestStream) Close() error { return nil }
func (s *resolverTestStream) RunAt(offset, limit uint64) (sparse.RunKind, uint64, error) {
	s.runCalls.Add(1)
	s.lastLimit.Store(limit)
	if s.run != nil {
		return s.run(offset, limit)
	}
	return sparse.Data, offset + limit, nil
}
func (s *resolverTestStream) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	s.readCalls.Add(1)
	if s.read != nil {
		return s.read(ctx, buf, offset)
	}
	return len(buf), nil
}

type resolverTestChunkStream struct {
	*resolverTestStream
	runChunk       func(offset, limit uint64) (sparse.RunKind, uint64, uint64, error)
	readChunk      func(context.Context, []byte, uint64, uint64, uint64) (int, error)
	runChunkCalls  atomic.Int64
	readChunkCalls atomic.Int64
}

func (s *resolverTestChunkStream) RunChunkAt(offset, limit uint64) (sparse.RunKind, uint64, uint64, error) {
	s.runChunkCalls.Add(1)
	if s.runChunk != nil {
		return s.runChunk(offset, limit)
	}
	return sparse.Data, offset + limit, 0, nil
}
func (s *resolverTestChunkStream) ReadChunkAt(ctx context.Context, buf []byte, chunkIdx, offset, end uint64) (int, error) {
	s.readChunkCalls.Add(1)
	if s.readChunk != nil {
		return s.readChunk(ctx, buf, chunkIdx, offset, end)
	}
	return len(buf), nil
}

func resolverHoleStream(size uint64) *resolverTestStream {
	return &resolverTestStream{
		size: size,
		run: func(offset, limit uint64) (sparse.RunKind, uint64, error) {
			return sparse.Hole, offset + limit, nil
		},
	}
}
