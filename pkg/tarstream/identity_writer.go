package tarstream

import "io"

// identityWriter overlaps the ordered payload hash with encryption and output.
// It owns two fixed buffers, so the caller can reuse its read buffer as soon as
// Write returns. Only this worker accesses the identity accumulator until Close
// has joined it. Sources and destination writers remain on the caller goroutine.
type identityWriter struct {
	identity *identityAccumulator
	free     chan []byte
	work     chan []byte
	done     chan struct{}
	failed   chan struct{}
	err      error
	closed   bool
}

func newIdentityWriter(identity *identityAccumulator) *identityWriter {
	w := &identityWriter{
		identity: identity,
		free:     make(chan []byte, 2),
		work:     make(chan []byte, 2),
		done:     make(chan struct{}),
		failed:   make(chan struct{}),
	}
	for range cap(w.free) {
		w.free <- make([]byte, copyBufSize)
	}
	go func() {
		defer close(w.done)
		for buffer := range w.work {
			if w.err == nil {
				var n int
				n, w.err = w.identity.Write(buffer)
				if w.err == nil && n != len(buffer) {
					w.err = io.ErrShortWrite
				}
				if w.err != nil {
					// Publish the first error; err is immutable after this signal.
					close(w.failed)
				}
			}
			w.free <- buffer[:cap(buffer)]
		}
	}()
	return w
}

func (w *identityWriter) Write(data []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	consumed := 0
	for len(data) > 0 {
		buffer := <-w.free
		select {
		case <-w.failed:
			w.free <- buffer
			return consumed, w.err
		default:
		}
		n := copy(buffer, data)
		w.work <- buffer[:n]
		data = data[n:]
		consumed += n
	}
	return consumed, nil
}

// Close joins the hash worker on both successful and failed output paths.
// Any hash error must be observed before the completion marker is written.
func (w *identityWriter) Close() error {
	if !w.closed {
		w.closed = true
		close(w.work)
		<-w.done
	}
	return w.err
}
