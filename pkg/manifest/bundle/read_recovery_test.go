package bundle

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type recoveringIndex struct {
	io.ReaderAt
	start int64
	fail  error
	calls atomic.Int32
}

func TestUnavailableRefDoesNotHideJoinedPermanentCause(t *testing.T) {
	diagnostics := searchDiagnostics{unavailable: map[string]error{"file://unreachable": io.ErrClosedPipe}}
	missing := readerr.Mark(store.ErrNotFound, false)
	if err := searchError("test", diagnostics, missing); readerr.IsPermanent(err) {
		t.Fatalf("remote miss incorrectly proves unreachable ref absent: %v", err)
	}
	corrupt := readerr.Mark(errors.New("verified corrupt payload"), false)
	err := searchError("test", diagnostics, errors.Join(missing, corrupt))
	if !readerr.IsPermanent(err) || !errors.Is(err, corrupt) {
		t.Fatalf("joined integrity cause lost: %v", err)
	}
}

func (r *recoveringIndex) ReadAt(p []byte, off int64) (int, error) {
	if off == r.start && r.calls.Add(1) == 1 {
		for i := range p {
			p[i] = 0xff
		}
		return len(p), r.fail
	}
	return r.ReaderAt.ReadAt(p, off)
}
func TestChunkIndexFirstAccessFailureIsRecoverable(t *testing.T) {
	for _, cause := range []error{io.ErrClosedPipe, context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			data := syntheticBundle(t, 4_000, 1)
			layout := inspectBundleIORanges(t, data)
			source := &recoveringIndex{ReaderAt: bytes.NewReader(data), start: layout.chunkIndex.start, fail: cause}
			reader, err := NewReader(source, int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			err = reader.prepareChunks(context.Background())
			if !errors.Is(err, cause) || readerr.IsPermanent(err) {
				t.Fatalf("first cause lost: %v", err)
			}
			if err := reader.prepareChunks(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := reader.prepareChunks(context.Background()); err != nil {
				t.Fatal(err)
			}
			if source.calls.Load() != 2 {
				t.Fatalf("index attempts=%d, want 2 then success reuse", source.calls.Load())
			}
		})
	}
}
