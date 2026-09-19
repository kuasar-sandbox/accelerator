package tailzip

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

type fullEOFReader struct{ *bytes.Reader }

func (r fullEOFReader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.Reader.ReadAt(p, off)
	if n == len(p) && off+int64(n) == r.Size() {
		return n, io.EOF
	}
	return n, err
}
func TestSuffixFullBufferAtEOF(t *testing.T) {
	tail, err := Encode([]Entry{{Name: "config.json", Body: []byte("{}")}})
	if err != nil {
		t.Fatal(err)
	}
	r := fullEOFReader{bytes.NewReader(tail)}
	_, _, err = Open(r, int64(len(tail)), Options{})
	if err != nil {
		t.Fatal(err)
	}
}
func TestSuffixRejectsAbsoluteOffsetPrefix(t *testing.T) {
	var output bytes.Buffer
	output.Write(bytes.Repeat([]byte{'X'}, 4096))
	w := zip.NewWriter(&output)
	w.SetOffset(4096)
	file, err := w.CreateHeader(&zip.FileHeader{Name: "config.json", Method: zip.Store})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte("{}")); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	tail, err := Locate(bytes.NewReader(output.Bytes()), int64(output.Len()), Options{})
	if err == nil && tail.Offset != 4096 {
		t.Fatalf("absolute-offset prefix silently treated as tail: %+v", tail)
	}
	if err := Validate(output.Bytes(), Options{}); err == nil {
		t.Fatal("prefixed archive accepted as standalone tail")
	}
}

type entryLimitProbe struct {
	*bytes.Reader
	calls int
}

func (r *entryLimitProbe) ReadAt(p []byte, off int64) (int, error) {
	r.calls++
	if r.calls > 1 {
		return 0, errors.New("directory parsed before entry limit")
	}
	return r.Reader.ReadAt(p, off)
}
func TestEntryLimitPrecedesZIPAllocation(t *testing.T) {
	entries := make([]Entry, 8)
	for i := range entries {
		entries[i] = Entry{Name: fmt.Sprintf("e%d", i), Body: []byte("x")}
	}
	tail, err := Encode(entries)
	if err != nil {
		t.Fatal(err)
	}
	probe := &entryLimitProbe{Reader: bytes.NewReader(tail)}
	_, err = Locate(probe, int64(len(tail)), Options{MaxEntries: 1})
	if err == nil || !strings.Contains(err.Error(), "entries exceed") || probe.calls != 1 {
		t.Fatalf("limit too late: calls=%d err=%v", probe.calls, err)
	}
}
