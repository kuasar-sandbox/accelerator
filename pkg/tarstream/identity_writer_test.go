package tarstream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestPipelinedIdentityMatchesSequential(t *testing.T) {
	old := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(old)
	const payload = 4 << 20
	for _, holes := range [][]sparse.Extent{nil, {{Offset: 4096, Size: 3 * 4096}, {Offset: payload - 4096, Size: 4096}}, {{Offset: 123, Size: 8765}}} {
		body := make([]byte, payload+19)
		for i := range body {
			body[i] = byte(i*31 + i/4096)
		}
		for _, hole := range holes {
			clear(body[hole.Offset : hole.Offset+hole.Size])
		}
		for _, encrypted := range []bool{false, true} {
			var reference []byte
			var expectedScheme, expectedDigest string
			for _, procs := range []int{1, 2} {
				runtime.GOMAXPROCS(procs)
				base, err := sparse.NewSource(bytes.NewReader(body), uint64(len(body)), holes)
				if err != nil {
					t.Fatal(err)
				}
				source := segmentedDenseSource{Source: base, payloadSize: payload}
				var output bytes.Buffer
				var opts []WriteOption
				var readOpts []ReadOption
				if encrypted {
					option := WithCodec(&optionTestCodec{}, true)
					opts = append(opts, option)
					readOpts = append(readOpts, option)
				}
				scheme, digest, err := WriteTo(context.Background(), &output, "snapshot", source, opts...)
				if err != nil {
					t.Fatal(err)
				}
				if procs == 1 {
					reference, expectedScheme, expectedDigest = append([]byte(nil), output.Bytes()...), scheme, digest
				} else if scheme != expectedScheme || digest != expectedDigest || (!encrypted && !bytes.Equal(reference, output.Bytes())) {
					t.Fatal("pipelining changed the canonical identity or plaintext encoding")
				}
				readOpts = append(readOpts, WithExpectedDigest(scheme, digest))
				decoded, _, err := SourceAt(bytes.NewReader(output.Bytes()), int64(output.Len()), "snapshot", readOpts...)
				if err != nil {
					t.Fatal(err)
				}
				got := make([]byte, len(body))
				if n, err := decoded.ReadAt(context.Background(), got, 0); err != nil || n != len(got) {
					t.Fatalf("read=%d,%v", n, err)
				}
				if !bytes.Equal(got, body) {
					t.Fatal("pipelining changed sparse memory or metadata tail")
				}
			}
		}
	}
}

func TestIdentityWriterOwnsBuffersAndJoinsErrors(t *testing.T) {
	const size = 3*copyBufSize + 13
	body := bytes.Repeat([]byte{0x3a}, size)
	makeIdentity := func() *identityAccumulator {
		return newIdentityAccumulator(size, []extent{{Size: size}}, size, 0, nil, []extent{{Size: size}})
	}
	sequential, pipelined := makeIdentity(), makeIdentity()
	sequential.Write(body)
	w := newIdentityWriter(pipelined)
	defer w.Close()
	for offset := 0; offset < len(body); {
		chunk := append([]byte(nil), body[offset:min(len(body), offset+copyBufSize)]...)
		if n, err := w.Write(chunk); err != nil || n != len(chunk) {
			t.Fatalf("Write=%d,%v", n, err)
		}
		clear(chunk) // caller may immediately reuse its buffer
		offset += len(chunk)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if sequential.finish("snapshot", size, size) != pipelined.finish("snapshot", size, size) {
		t.Fatal("writer retained caller buffers")
	}
	if _, err := w.Write(body[:1]); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	// An inconsistent presence stream must fail before any completion marker.
	invalid := newIdentityAccumulator(4096, nil, 0, 0, []byte{0}, nil)
	w = newIdentityWriter(invalid)
	defer w.Close()
	// Exceed the two-buffer queue so the producer must observe a returned
	// buffer after the worker has detected the inconsistent presence stream.
	data := make([]byte, 4*copyBufSize)
	if n, err := w.Write(data); !errors.Is(err, io.ErrShortWrite) || n > 2*copyBufSize {
		t.Fatalf("producer did not stop on hash failure: %d, %v", n, err)
	}
	if n, err := w.Write(data); n != 0 || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("hash failure was not sticky: %d, %v", n, err)
	}
	if err := w.Close(); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	if len(w.free) != cap(w.free) {
		t.Fatal("hash failure did not drain and return all buffers")
	}
}
