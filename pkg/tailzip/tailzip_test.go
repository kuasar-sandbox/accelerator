package tailzip

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestLocateReadAndStrictProfile(t *testing.T) {
	tail, err := Encode([]Entry{{Name: "a", Body: []byte("A")}, {Name: "b", Body: []byte("BB")}})
	if err != nil {
		t.Fatal(err)
	}
	prefix := bytes.Repeat([]byte{0x55}, 4096)
	whole := append(append([]byte(nil), prefix...), tail...)
	got, at, err := Read(bytes.NewReader(whole), int64(len(whole)), Options{KnownEntries: []string{"a", "b"}, RequireOrder: true, RequireStored: true})
	if err != nil {
		t.Fatal(err)
	}
	if at.Offset != int64(len(prefix)) || !bytes.Equal(got, tail) {
		t.Fatalf("tail = %+v, bytes equal %v", at, bytes.Equal(got, tail))
	}
	if err := Validate(tail, Options{KnownEntries: []string{"b", "a"}, RequireOrder: true}); err == nil {
		t.Fatal("out-of-order archive accepted")
	}
}

func TestLocateAbsentTruncatedAndCorrupt(t *testing.T) {
	if _, err := Locate(bytes.NewReader([]byte("payload")), 7, Options{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("absent: %v", err)
	}
	tail, _ := Encode([]Entry{{Name: "x", Body: []byte("body")}})
	for _, cut := range []int{1, 10} {
		b := tail[:len(tail)-cut]
		if _, err := Locate(bytes.NewReader(b), int64(len(b)), Options{}); err == nil {
			t.Fatalf("truncation %d accepted", cut)
		}
	}
	bad := append([]byte(nil), tail...)
	i := bytes.Index(bad, []byte("body"))
	bad[i] ^= 1
	if _, err := Locate(bytes.NewReader(bad), int64(len(bad)), Options{}); err == nil {
		t.Fatal("bad CRC accepted")
	}
}

func TestLocateRejectsForgedBoundsAndLimit(t *testing.T) {
	tail, _ := Encode([]Entry{{Name: "x", Body: []byte("x")}})
	if _, err := Locate(bytes.NewReader(tail), int64(len(tail)), Options{MaxSize: 1}); err == nil {
		t.Fatal("oversize accepted")
	}
	e := bytes.LastIndex(tail, []byte{'P', 'K', 5, 6})
	bad := append([]byte(nil), tail...)
	binary.LittleEndian.PutUint32(bad[e+16:e+20], 0xffffffff)
	if _, err := Locate(bytes.NewReader(bad), int64(len(bad)), Options{}); err == nil {
		t.Fatal("forged directory accepted")
	}
}

func TestEncodeRetainsArchiveZipCompatibility(t *testing.T) {
	b, _ := Encode([]Entry{{Name: "config.json", Body: []byte("{}")}})
	z, err := zip.NewReader(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	r, err := z.File[0].Open()
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "{}" {
		t.Fatal(string(got))
	}
}

func TestPrefixAndAppendPreserveSparseKinds(t *testing.T) {
	base := &testSource{size: 8}
	tail, _ := Encode([]Entry{{Name: "x", Body: []byte("x")}})
	joined, err := Append(base, tail, Options{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := joined.RunAt(0, 8)
	if err != nil || r.Kind() != sparse.Hole {
		t.Fatalf("payload run: %v %v", r, err)
	}
	r, err = joined.RunAt(8, uint64(len(tail)))
	if err != nil || r.Kind() != sparse.Data {
		t.Fatalf("tail run: %v %v", r, err)
	}
	p, err := Prefix(joined, 8)
	if err != nil {
		t.Fatal(err)
	}
	if p.Size() != 8 {
		t.Fatal(p.Size())
	}
}

type testSource struct{ size uint64 }

func (s *testSource) Size() uint64 { return s.size }
func (s *testSource) RunAt(off, limit uint64) (sparse.Run, error) {
	if off >= s.size {
		return nil, io.EOF
	}
	end := off + limit
	if end > s.size {
		end = s.size
	}
	return testRun{off, end}, nil
}
func (s *testSource) ReadAt(context.Context, []byte, uint64) (int, error) { panic("hole read") }

type testRun struct{ off, end uint64 }

func (r testRun) Offset() uint64       { return r.off }
func (r testRun) End() uint64          { return r.end }
func (r testRun) Kind() sparse.RunKind { return sparse.Hole }
func (r testRun) ReadAt(_ context.Context, b []byte, off uint64) (int, error) {
	clear(b)
	return len(b), nil
}
