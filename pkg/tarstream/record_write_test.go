package tarstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// Include the sequence and AAD in the test record so byte comparison detects
// changes in record boundaries, order and authentication context.
type writeTestCodec struct{ optionTestCodec }

func (c *writeTestCodec) Encrypt(dst, plain, aad []byte, sequence uint64) ([]byte, error) {
	start := len(dst)
	dst, _ = c.optionTestCodec.Encrypt(dst, plain, aad, sequence)
	var counter [8]byte
	binary.BigEndian.PutUint64(counter[:], sequence)
	h := sha256.New()
	h.Write(aad)
	h.Write(counter[:])
	copy(dst[start+1+len(plain):], h.Sum(nil)[:16])
	return dst, nil
}

type countingRecordOutput struct {
	bytes.Buffer
	calls, largest int
}

func (w *countingRecordOutput) Write(p []byte) (int, error) {
	w.calls++
	w.largest = max(w.largest, len(p))
	return w.Buffer.Write(p)
}

func TestRecordWriteBatchPreservesEncoding(t *testing.T) {
	for _, size := range []int{0, 1, 4095, 4096, 63 * 4096, 64 * 4096, 65*4096 + 19, 2*recordWriteBatchSize + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i*17 + i/4096)
			}
			source := sparse.Dense(bytes.NewReader(data), uint64(size))
			plan, err := makeWritePlan("snapshot", source)
			if err != nil {
				t.Fatal(err)
			}
			var plaintext bytes.Buffer
			if _, err := emitPlan(context.Background(), &plaintext, plan, source, nil); err != nil {
				t.Fatal(err)
			}
			geometry, err := makeEnvelopeHeader(plan)
			if err != nil {
				t.Fatal(err)
			}
			prefix, header := clearPrefix([32]byte{0x42}), geometry.marshal()
			codec := &writeTestCodec{}
			// Independent per-record reference for the existing v1 encoding.
			var want []byte
			aad := newRecordAAD(prefix, header)
			for index, offset := uint64(0), 0; offset < plaintext.Len(); index++ {
				n := recordSize
				if index == 0 {
					n = int(geometry.firstSize)
				}
				n = min(n, plaintext.Len()-offset)
				setRecordAAD(aad, index, uint32(n))
				want, err = codec.Encrypt(want, plaintext.Bytes()[offset:offset+n], aad, index+1)
				if err != nil {
					t.Fatal(err)
				}
				offset += n
			}
			for _, inputChunk := range []int{1, 4093, copyBufSize} {
				var output countingRecordOutput
				writer := newRecordWriter(&output, codec, prefix, header, geometry)
				for offset := 0; offset < plaintext.Len(); {
					n := min(inputChunk, plaintext.Len()-offset)
					if wrote, err := writer.Write(plaintext.Bytes()[offset : offset+n]); err != nil || wrote != n {
						t.Fatalf("Write=%d,%v", wrote, err)
					}
					offset += n
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(output.Bytes(), want) {
					t.Fatalf("encoding changed with input chunk %d", inputChunk)
				}
				if output.largest > recordWriteBatchSize {
					t.Fatal("unbounded output batch")
				}
				if output.calls > len(want)/(recordWriteBatchSize-recordSize-recordOverhead)+1 {
					t.Fatalf("too many output writes: %d", output.calls)
				}
			}
		})
	}
}

type limitedRecordOutput struct {
	bytes.Buffer
	remaining int
	chunk     int
	err       error
}

func (w *limitedRecordOutput) Write(p []byte) (int, error) {
	if w.chunk > 0 {
		p = p[:min(len(p), w.chunk)]
	}
	n := min(len(p), w.remaining)
	w.Buffer.Write(p[:n])
	w.remaining -= n
	if n < len(p) || n == 0 {
		return n, w.err
	}
	return n, nil
}

func TestRecordWriteBatchOutputFailures(t *testing.T) {
	source, err := sparse.NewSource(bytes.NewReader(bytes.Repeat([]byte{0x7b}, 5*recordWriteBatchSize)), 5*recordWriteBatchSize, nil)
	if err != nil {
		t.Fatal(err)
	}
	var complete bytes.Buffer
	option := WithCodec(&optionTestCodec{}, true)
	if _, _, err := WriteTo(context.Background(), &complete, "snapshot", source, option); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("output failed")
	// Header, first batch, middle batch and final Close flush failures must all
	// fail the operation without returning a publishable identity.
	for _, limit := range []int{0, envelopePrefixSize + envelopeHeaderSealed, recordWriteBatchSize - 1, 2*recordWriteBatchSize + 7, complete.Len() - 1} {
		for _, outputErr := range []error{wantErr, nil} {
			w := &limitedRecordOutput{remaining: limit, err: outputErr}
			scheme, digest, err := WriteTo(context.Background(), w, "snapshot", source, option)
			want := outputErr
			if want == nil {
				want = io.ErrShortWrite
			}
			if !errors.Is(err, want) || scheme != "" || digest != "" {
				t.Fatalf("limit=%d: %q %q %v", limit, scheme, digest, err)
			}
		}
	}
	// Legal partial writes must be retried through the final batch.
	w := &limitedRecordOutput{remaining: complete.Len(), chunk: 701, err: wantErr}
	if _, _, err := WriteTo(context.Background(), w, "snapshot", source, option); err != nil {
		t.Fatal(err)
	}
	// Salts vary, so compare the canonical stream using the reader.
	decoded, _, err := SourceAt(bytes.NewReader(w.Bytes()), int64(w.Len()), "snapshot", option)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, source.Size())
	if n, err := decoded.ReadAt(context.Background(), got, 0); err != nil || n != len(got) {
		t.Fatalf("read=%d,%v", n, err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte{0x7b}, len(got))) {
		t.Fatal("short writes corrupted payload")
	}
}
