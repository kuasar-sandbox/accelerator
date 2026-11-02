package fetch

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestLayeredCarriesAncestorBoundThroughNestedLayers(t *testing.T) {
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
	bottom := &resolverTestStream{size: size}
	nested := NewLayered(nestedUpper, bottom)
	stream := NewLayered(outerUpper, nested)

	run, err := stream.RunAt(0, size)
	if err != nil {
		t.Fatalf("RunAt: %v", err)
	}
	leaf, ok := run.(resolverTestRun)
	if !ok || leaf.stream != bottom || run.Kind() != sparse.Data || run.Offset() != 0 || run.End() != 4 {
		t.Fatalf("resolved run = %T [%d,%d) kind %v, want bottom Data [0,4)", run, run.Offset(), run.End(), run.Kind())
	}
	if got := bottom.lastLimit.Load(); got != 4 {
		t.Fatalf("deepest leaf limit = %d, want ancestor bound 4", got)
	}
}

func TestLayeredRejectsLeafErrorsAndInvalidRuns(t *testing.T) {
	sentinel := errors.New("sentinel resolver failure")
	tests := []struct {
		name        string
		run         func(offset, limit uint64) (sparse.RunKind, uint64, error)
		want        error
		wantInvalid bool
	}{
		{name: "error", run: func(uint64, uint64) (sparse.RunKind, uint64, error) { return 0, 0, sentinel }, want: sentinel},
		{name: "EOF below Size", run: func(uint64, uint64) (sparse.RunKind, uint64, error) { return 0, 0, io.EOF }, want: io.EOF},
		{name: "unknown kind", run: func(offset, _ uint64) (sparse.RunKind, uint64, error) { return sparse.RunKind(0xFF), offset + 1, nil }, wantInvalid: true},
		{name: "non advancing", run: func(offset, _ uint64) (sparse.RunKind, uint64, error) { return sparse.Data, offset, nil }, wantInvalid: true},
		{name: "end before offset", run: func(offset, _ uint64) (sparse.RunKind, uint64, error) { return sparse.Data, offset - 1, nil }, wantInvalid: true},
		{name: "end after requested bound", run: func(offset, limit uint64) (sparse.RunKind, uint64, error) {
			return sparse.Data, offset + limit + 1, nil
		}, wantInvalid: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leaf := &resolverTestStream{size: 8, run: tt.run}
			stream := NewLayered(resolverHoleStream(8), leaf)
			_, err := stream.RunAt(2, 4)
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

func TestResolverFailuresPropagateThroughEveryOperation(t *testing.T) {
	sentinel := errors.New("leaf metadata failed")
	tests := []struct {
		name string
		run  func(offset, limit uint64) (sparse.RunKind, uint64, error)
		want error
	}{
		{name: "sentinel", run: func(uint64, uint64) (sparse.RunKind, uint64, error) { return 0, 0, sentinel }, want: sentinel},
		{name: "unexpected EOF", run: func(uint64, uint64) (sparse.RunKind, uint64, error) { return 0, 0, io.EOF }, want: io.EOF},
		{name: "invalid kind", run: func(offset, _ uint64) (sparse.RunKind, uint64, error) { return sparse.RunKind(99), offset + 1, nil }, want: errInvalidRun},
		{name: "invalid end", run: func(offset, _ uint64) (sparse.RunKind, uint64, error) { return sparse.Data, offset, nil }, want: errInvalidRun},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newStream := func() (*resolverTestStream, Stream) {
				bad := &resolverTestStream{size: 8, run: tt.run}
				return bad, NewLayered(resolverHoleStream(8), bad)
			}

			_, stream := newStream()
			if _, err := stream.RunAt(0, 8); !errors.Is(err, tt.want) {
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
			if err := stream.(Prefetcher).Prefetch(context.Background()); !errors.Is(err, tt.want) {
				t.Fatalf("Prefetch error = %v, want %v", err, tt.want)
			}
			if bad.readCalls.Load() != 0 {
				t.Fatalf("Prefetch performed %d data reads", bad.readCalls.Load())
			}
		})
	}
}

func TestPrefetchCancelsJobsWhenWalkFails(t *testing.T) {
	sentinel := errors.New("second run metadata failure")
	tests := []struct {
		name string
		run  func(offset, limit uint64) (sparse.RunKind, uint64, error)
		want error
	}{
		{
			name: "RunAt",
			run: func(offset, _ uint64) (sparse.RunKind, uint64, error) {
				if offset == 0 {
					return sparse.Data, 4, nil
				}
				return 0, 0, sentinel
			},
			want: sentinel,
		},
		{
			name: "validateRun",
			run: func(offset, _ uint64) (sparse.RunKind, uint64, error) {
				if offset == 0 {
					return sparse.Data, 4, nil
				}
				return sparse.Data, offset, nil
			},
			want: errInvalidRun,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			started := make(chan struct{})
			stream := &prefetchWalkStream{
				size: 8,
				run:  tt.run,
				prefetch: func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					return ctx.Err()
				},
			}
			done := make(chan error, 1)
			go func() { done <- prefetchStream(context.Background(), stream) }()

			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("first chunk prefetch did not start")
			}
			select {
			case err := <-done:
				if !errors.Is(err, tt.want) {
					t.Fatalf("Prefetch error = %v, want %v", err, tt.want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Prefetch did not return after walk failure; dispatched Get was not canceled")
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
		t.Fatalf("phase-two I/O started %d times before planning completed", got)
	}
	for i := range buf {
		if buf[i] != want[i] {
			t.Fatalf("buffer mutated before planning completed: byte %d = %#x, want %#x", i, buf[i], want[i])
		}
	}
}

func TestReadAtRangeContract(t *testing.T) {
	tests := []struct {
		name               string
		n                  int
		cause              error
		permanent, success bool
	}{
		{"short nil", 7, nil, true, false},
		{"short EOF", 7, io.EOF, true, false},
		{"short unexpected EOF", 7, io.ErrUnexpectedEOF, true, false},
		{"full EOF", 8, io.EOF, false, true},
		{"full unexpected EOF", 8, io.ErrUnexpectedEOF, true, false},
		{"full retryable EOF", 8, readerr.Mark(io.EOF, true), false, false},
		{"full permanent EOF", 8, readerr.Mark(io.EOF, false), true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leaf := &resolverTestStream{size: 8, read: func(context.Context, []byte, uint64) (int, error) { return tt.n, tt.cause }}
			stream := NewLayered(leaf, resolverHoleStream(8))
			n, err := stream.ReadAt(context.Background(), make([]byte, 8), 0)
			if tt.success {
				if n != 8 || err != nil {
					t.Fatalf("success = %d, %v", n, err)
				}
				return
			}
			if n != 0 || err == nil || readerr.IsPermanent(err) != tt.permanent {
				t.Fatalf("ReadAt = %d, %v, permanent=%t", n, err, readerr.IsPermanent(err))
			}
			if tt.cause != nil && !errors.Is(err, tt.cause) {
				t.Fatalf("lost cause %v: %v", tt.cause, err)
			}
		})
	}
}

func TestReadAtRunsConcurrentlyAndCancelsPeers(t *testing.T) {
	sentinel := errors.New("run failed")
	entered := make(chan uint64, 2)
	fail := make(chan struct{})
	canceled := make(chan struct{})
	leaf := &resolverTestStream{
		size: 8,
		run:  func(offset, _ uint64) (sparse.RunKind, uint64, error) { return sparse.Data, offset + 4, nil },
		read: func(ctx context.Context, buf []byte, offset uint64) (int, error) {
			entered <- offset
			if offset == 0 {
				<-fail
				return 0, sentinel
			}
			<-ctx.Done()
			close(canceled)
			return 0, ctx.Err()
		},
	}
	done := make(chan error, 1)
	go func() {
		_, err := leaf.ReadAt(context.Background(), make([]byte, 8), 0)
		done <- err
	}()

	seen := map[uint64]bool{}
	for len(seen) < 2 {
		select {
		case off := <-entered:
			seen[off] = true
		case <-time.After(5 * time.Second):
			t.Fatal("data runs did not start concurrently")
		}
	}
	close(fail)
	select {
	case err := <-done:
		if !errors.Is(err, sentinel) {
			t.Fatalf("ReadAt error = %v, want sentinel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReadAt did not return")
	}
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("peer run did not observe cancellation")
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
func (s *resolverTestStream) RunAt(offset, limit uint64) (sparse.Run, error) {
	s.runCalls.Add(1)
	s.lastLimit.Store(limit)
	kind, end := sparse.Data, offset+limit
	var err error
	if s.run != nil {
		kind, end, err = s.run(offset, limit)
	}
	if err != nil {
		return nil, err
	}
	return resolverTestRun{stream: s, offset: offset, end: end, kind: kind}, nil
}
func (s *resolverTestStream) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	return readStreamAt(ctx, s, buf, offset)
}

type resolverTestRun struct {
	stream *resolverTestStream
	offset uint64
	end    uint64
	kind   sparse.RunKind
}

func (r resolverTestRun) Offset() uint64       { return r.offset }
func (r resolverTestRun) End() uint64          { return r.end }
func (r resolverTestRun) Kind() sparse.RunKind { return r.kind }
func (r resolverTestRun) ReadAt(ctx context.Context, buf []byte, inner uint64) (int, error) {
	r.stream.readCalls.Add(1)
	if r.kind != sparse.Data {
		clear(buf)
		return len(buf), nil
	}
	if r.stream.read != nil {
		return r.stream.read(ctx, buf, r.offset+inner)
	}
	return len(buf), nil
}

type prefetchWalkStream struct {
	size     uint64
	run      func(offset, limit uint64) (sparse.RunKind, uint64, error)
	prefetch func(context.Context) error
}

func (s *prefetchWalkStream) Size() uint64 { return s.size }
func (s *prefetchWalkStream) Close() error { return nil }
func (s *prefetchWalkStream) RunAt(offset, limit uint64) (sparse.Run, error) {
	kind, end, err := s.run(offset, limit)
	if err != nil {
		return nil, err
	}
	return prefetchWalkRun{stream: s, offset: offset, end: end, kind: kind}, nil
}
func (s *prefetchWalkStream) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	return readStreamAt(ctx, s, buf, offset)
}

type prefetchWalkRun struct {
	stream *prefetchWalkStream
	offset uint64
	end    uint64
	kind   sparse.RunKind
}

func (r prefetchWalkRun) Offset() uint64       { return r.offset }
func (r prefetchWalkRun) End() uint64          { return r.end }
func (r prefetchWalkRun) Kind() sparse.RunKind { return r.kind }
func (r prefetchWalkRun) chunkRun()            {}
func (r prefetchWalkRun) ReadAt(context.Context, []byte, uint64) (int, error) {
	return 0, errors.New("prefetchWalkRun: unexpected ReadAt")
}
func (r prefetchWalkRun) prefetch(ctx context.Context) error {
	return r.stream.prefetch(ctx)
}

func resolverHoleStream(size uint64) *resolverTestStream {
	return &resolverTestStream{
		size: size,
		run: func(offset, limit uint64) (sparse.RunKind, uint64, error) {
			return sparse.Hole, offset + limit, nil
		},
	}
}
