package fetch

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestParallelReadJoinsAndPreservesLaterPermanentCause(t *testing.T) {
	first := errors.New("first access failure")
	otherStarted := make(chan struct{})
	var joined atomic.Bool
	s := &resolverTestStream{size: 8, run: func(off, _ uint64) (sparse.RunKind, uint64, error) { return sparse.Data, off + 4, nil }, read: func(ctx context.Context, p []byte, off uint64) (int, error) {
		if off == 0 {
			<-otherStarted
			return 0, first
		}
		close(otherStarted)
		<-ctx.Done()
		for i := range p {
			p[i] = 0x42
		}
		joined.Store(true)
		return len(p), readerr.Mark(io.EOF, false)
	}}
	_, err := s.ReadAt(context.Background(), make([]byte, 8), 0)
	if !joined.Load() || !errors.Is(err, first) || !errors.Is(err, io.EOF) || !readerr.IsPermanent(err) {
		t.Fatalf("unjoined/hidden permanent cause: %v", err)
	}
}
