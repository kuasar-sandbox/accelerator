package tailzip

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"io"
	"testing"
)

func TestAppendReadAcrossPayloadBoundary(t *testing.T) {
	tail, err := Encode([]Entry{{Name: "a", Body: []byte("tail")}})
	if err != nil {
		t.Fatal(err)
	}
	src, err := sparse.NewSource(bytes.NewReader([]byte("data")), 4, nil)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := Append(src, tail, Options{})
	if err != nil {
		t.Fatal(err)
	}
	out := make([]byte, 8)
	n, err := joined.ReadAt(context.Background(), out, 2)
	want := append([]byte("ta"), tail[:6]...)
	if err != nil || n != len(out) || !bytes.Equal(out, want) {
		t.Fatalf("cross-boundary read = %d, %v, %x; want %x", n, err, out, want)
	}
}

type earlyEOFSource struct{ sparse.Source }

func (s earlyEOFSource) ReadAt(context.Context, []byte, uint64) (int, error) { return 1, io.EOF }
func TestPrefixPropagatesPrematureEOF(t *testing.T) {
	src, _ := sparse.NewSource(bytes.NewReader(make([]byte, 8)), 8, nil)
	prefix, err := Prefix(earlyEOFSource{src}, 4)
	if err != nil {
		t.Fatal(err)
	}
	n, err := prefix.ReadAt(context.Background(), make([]byte, 4), 0)
	if err == nil {
		t.Fatalf("premature EOF accepted: n=%d", n)
	}
}

func TestRecognizableTruncatedTailIsNotAbsence(t *testing.T) {
	tail, err := Encode([]Entry{{Name: "a", Body: []byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	tail = tail[:len(tail)-1]
	_, err = Locate(bytes.NewReader(tail), int64(len(tail)), Options{})
	if err == nil || IsNotFound(err) {
		t.Fatalf("truncated recognizable tail classified as absent: %v", err)
	}
}

// Compressed metadata still has to fit the bounded metadata budget.
func TestDecodedTailRespectsSizeBudget(t *testing.T) {
	var out bytes.Buffer
	zw := zip.NewWriter(&out)
	w, err := zw.Create("a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write(make([]byte, 1<<20)); err != nil {
		t.Fatal(err)
	}
	if err = zw.Close(); err != nil {
		t.Fatal(err)
	}
	if out.Len() > 4096 {
		t.Fatal("fixture is not compressed enough")
	}
	if _, err = Locate(bytes.NewReader(out.Bytes()), int64(out.Len()), Options{MaxSize: 4096}); err == nil {
		t.Fatal("decoded metadata exceeds allowed budget")
	}
}

type errorReaderAt struct {
	*bytes.Reader
	calls   int
	failure error
}

func (r *errorReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.Reader.ReadAt(p, off)
	r.calls++
	if r.calls == 2 {
		return n, r.failure
	}
	return n, err
}
func TestLocateRetainsUnderlyingReaderError(t *testing.T) {
	tail, err := Encode([]Entry{{Name: "a", Body: []byte("value")}})
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected object read error")
	reader := &errorReaderAt{Reader: bytes.NewReader(tail), failure: failure}
	_, err = Locate(reader, int64(len(tail)), Options{})
	if !errors.Is(err, failure) {
		t.Fatalf("underlying reader cause lost: %v", err)
	}
}
