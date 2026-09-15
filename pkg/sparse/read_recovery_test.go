package sparse

import (
	"context"
	"errors"
	"io"
	"syscall"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
)

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
