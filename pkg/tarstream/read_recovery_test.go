package tarstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"syscall"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestSequentialProbePreservesCompleteUnexpectedEOF(t *testing.T) {
	var artifact bytes.Buffer
	if _, _, err := WriteTo(context.Background(), &artifact, "image", sparse.Dense(bytes.NewReader([]byte("data")), 4)); err != nil {
		t.Fatal(err)
	}
	for _, cause := range []error{io.ErrUnexpectedEOF, readerr.Mark(io.ErrUnexpectedEOF, true), fmt.Errorf("source: %w", io.ErrUnexpectedEOF), io.EOF} {
		source, _, err := SourceFrom(&failedFieldReader{Reader: bytes.NewReader(artifact.Bytes()), at: 0, cause: cause}, "")
		var got [4]byte
		if err == nil {
			_, err = source.ReadAt(context.Background(), got[:], 0)
		}
		if cause == io.EOF {
			if err != nil || string(got[:]) != "data" {
				t.Fatalf("full bare EOF changed: %v %q", err, got)
			}
		} else if !errors.Is(err, cause) || readerr.IsPermanent(err) != (cause == io.ErrUnexpectedEOF) {
			t.Fatalf("full first probe cause lost: %v %q", err, got)
		}
	}
}

type failedFieldReader struct {
	*bytes.Reader
	at    int64
	cause error
}

func (r *failedFieldReader) Read(p []byte) (int, error) {
	off, _ := r.Reader.Seek(0, io.SeekCurrent)
	n, err := r.Reader.Read(p)
	if off == r.at {
		return n, r.cause
	}
	return n, err
}

type failedFieldReaderAt struct {
	*bytes.Reader
	at    int64
	cause error
}

func (r failedFieldReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.Reader.ReadAt(p, off)
	if off == r.at {
		return n, r.cause
	}
	return n, err
}

func TestCompleteEncryptedFieldsPreserveReadFailure(t *testing.T) {
	codec := &optionTestCodec{}
	var artifact bytes.Buffer
	if _, _, err := WriteTo(context.Background(), &artifact, "image", sparse.Dense(bytes.NewReader(bytes.Repeat([]byte{0x42}, 4096)), 4096), WithCodec(codec, true)); err != nil {
		t.Fatal(err)
	}
	for _, cause := range []error{readerr.Mark(io.EOF, false), readerr.Mark(io.EOF, true), readerr.Mark(syscall.EAGAIN, false), context.Canceled} {
		for _, offset := range []int64{0, 8, envelopePrefixSize, envelopePrefixSize + envelopeHeaderSealed} {
			t.Run(cause.Error()+"/field", func(t *testing.T) {
				source, _, err := SourceFrom(&failedFieldReader{Reader: bytes.NewReader(artifact.Bytes()), at: offset, cause: cause}, "", WithCodec(codec, true))
				if err == nil {
					_, err = source.ReadAt(context.Background(), make([]byte, 4096), 0)
				}
				if !errors.Is(err, cause) || readerr.IsPermanent(err) != readerr.IsPermanent(cause) {
					t.Fatalf("sequential offset=%d classification/cause lost: %v", offset, err)
				}
			})
		}
	}
	// A fresh whole object can still be opened and consumed after failed attempts.
	source, _, err := SourceFrom(bytes.NewReader(artifact.Bytes()), "", WithCodec(codec, true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ReadAt(context.Background(), make([]byte, 4096), 0); err != nil {
		t.Fatal(err)
	}
}
func TestRandomMetadataPreservesCompleteWrappedEOF(t *testing.T) {
	var artifact bytes.Buffer
	if _, _, err := WriteTo(context.Background(), &artifact, "image", sparse.Dense(bytes.NewReader([]byte("data")), 4)); err != nil {
		t.Fatal(err)
	}
	for _, cause := range []error{readerr.Mark(io.EOF, false), readerr.Mark(io.EOF, true), readerr.Mark(syscall.EAGAIN, false), context.Canceled} {
		_, _, err := SourceAt(failedFieldReaderAt{bytes.NewReader(artifact.Bytes()), 0, cause}, int64(artifact.Len()), "")
		if !errors.Is(err, cause) || readerr.IsPermanent(err) != readerr.IsPermanent(cause) {
			t.Fatalf("random metadata classification/cause lost: %v", err)
		}
	}
}
