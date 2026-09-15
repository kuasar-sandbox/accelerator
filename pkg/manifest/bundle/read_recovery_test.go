package bundle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	for _, remote := range []error{missing, errors.Join(missing), fmt.Errorf("remote: %w", errors.Join(missing, missing))} {
		if err := searchError("test", diagnostics, remote); readerr.IsPermanent(err) || !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("remote miss incorrectly proves unreachable ref absent: %v", err)
		}
	}
	corrupt := readerr.Mark(errors.New("verified corrupt payload"), false)
	err := searchError("test", diagnostics, errors.Join(missing, corrupt))
	if !readerr.IsPermanent(err) || !errors.Is(err, corrupt) {
		t.Fatalf("joined integrity cause lost: %v", err)
	}
	remote := NewManifestFetcher(nil, nil, nil)
	outer := NewManifestFetcherWithResolver(&Reader{refs: []string{"file://unreachable"}}, nil, nil, remote)
	if _, err := outer.OpenManifest(context.Background(), store.ContentKey{}); !errors.Is(err, store.ErrNotFound) || readerr.IsPermanent(err) {
		t.Fatalf("nested remote miss incorrectly proves unreachable ref absent: %v", err)
	}
}

func TestBundleCompleteUnexpectedEOFClassification(t *testing.T) {
	for _, cause := range []error{io.ErrUnexpectedEOF, readerr.Mark(io.ErrUnexpectedEOF, true), fmt.Errorf("source: %w", io.ErrUnexpectedEOF)} {
		data := syntheticBundle(t, 4_000, 1)
		layout := inspectBundleIORanges(t, data)
		for _, offset := range []int64{int64(len(data) - 22), layout.chunkIndex.start} {
			source := &recoveringIndex{ReaderAt: bytes.NewReader(data), start: offset, fail: cause}
			reader, err := NewReader(source, int64(len(data)))
			if err == nil {
				err = reader.prepareChunks(context.Background())
				_ = reader.Close()
			}
			if !errors.Is(err, cause) || readerr.IsPermanent(err) != (cause == io.ErrUnexpectedEOF) {
				t.Fatalf("complete read at %d: cause=%v, result=%v", offset, cause, err)
			}
		}
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
