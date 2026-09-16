package sparse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"syscall"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
)

type fullErrorReaderAt struct{ err error }

func (r fullErrorReaderAt) ReadAt(p []byte, _ int64) (int, error) {
	return copy(p, "data"), r.err
}

func TestStaticRangeUnexpectedEOFClassification(t *testing.T) {
	for _, cause := range []error{io.ErrUnexpectedEOF, readerr.Mark(io.ErrUnexpectedEOF, true), fmt.Errorf("source: %w", io.ErrUnexpectedEOF), io.EOF} {
		s, err := NewSource(fullErrorReaderAt{cause}, 4, nil)
		if err != nil {
			t.Fatal(err)
		}
		run, err := s.RunAt(0, 4)
		if err != nil {
			t.Fatal(err)
		}
		p := make([]byte, 4)
		n, err := run.ReadAt(context.Background(), p, 0)
		if cause == io.EOF {
			if n != 4 || err != nil || string(p) != "data" {
				t.Fatalf("legal full EOF changed: %d %v %q", n, err, p)
			}
		} else if !errors.Is(err, cause) || readerr.IsPermanent(err) != (cause == io.ErrUnexpectedEOF) {
			t.Fatalf("static declared range cause changed: n=%d err=%v", n, err)
		}
	}
}

type consumedErrorReader struct {
	*bytes.Reader
	err error
}

func (r *consumedErrorReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if r.err != nil {
		err, r.err = r.err, nil
	}
	return n, err
}

func TestDenseFailedReadCannotReplayAdvancedBytes(t *testing.T) {
	cause := readerr.Mark(io.ErrClosedPipe, true)
	s := Dense(&consumedErrorReader{bytes.NewReader([]byte("AAAABBBB")), cause}, 8)
	if _, err := s.ReadAt(context.Background(), make([]byte, 4), 0); !errors.Is(err, cause) {
		t.Fatalf("first cause lost: %v", err)
	}
	buf := []byte("keep")
	if n, err := s.ReadAt(context.Background(), buf, 0); n != 0 || !readerr.IsPermanent(err) || string(buf) != "keep" {
		t.Fatalf("replayed advanced sequential source: n=%d buf=%q err=%v", n, buf, err)
	}
	if n, err := s.ReadAt(context.Background(), buf, 4); n != 4 || err != nil || string(buf) != "BBBB" {
		t.Fatalf("consumed position lost: n=%d buf=%q err=%v", n, buf, err)
	}
}

type fullErrorReader struct{ err error }

func (r fullErrorReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0x42
	}
	return len(p), r.err
}

func TestDenseFullReadPreservesExplicitSourceFailure(t *testing.T) {
	for _, cause := range []error{io.EOF, syscall.EAGAIN} {
		for _, retryable := range []bool{false, true} {
			marked := readerr.Mark(cause, retryable)
			s := Dense(fullErrorReader{marked}, 8)
			n, err := s.ReadAt(context.Background(), make([]byte, 8), 0)
			if n != 0 || !errors.Is(err, marked) || readerr.IsPermanent(err) == retryable {
				t.Fatalf("full explicit error=%d, %v", n, err)
			}
		}
	}
	s := Dense(fullErrorReader{io.EOF}, 8)
	if n, err := s.ReadAt(context.Background(), make([]byte, 8), 0); n != 8 || err != nil {
		t.Fatalf("legal full EOF=%d, %v", n, err)
	}
}
