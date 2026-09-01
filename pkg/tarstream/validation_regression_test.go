package tarstream

import (
	stdtar "archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

type emptyOnceBeforeEOFReader struct {
	reader  *bytes.Reader
	emitted bool
}

func (r *emptyOnceBeforeEOFReader) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	if n == 0 && err == io.EOF && !r.emitted {
		r.emitted = true
		return 0, nil
	}
	return n, err
}

type noProgressReader struct{}

func (noProgressReader) Read([]byte) (int, error) { return 0, nil }

func TestSequentialEOFProbeRetriesEmptyReads(t *testing.T) {
	body := bytes.Repeat([]byte("data"), 2048)
	codec := &optionTestCodec{}
	for _, tc := range []struct {
		name         string
		writeOptions []WriteOption
		readOptions  []ReadOption
	}{
		{name: "plaintext"},
		{name: "encrypted", writeOptions: []WriteOption{WithCodec(codec, true)}, readOptions: []ReadOption{WithCodec(codec, true)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var artifact bytes.Buffer
			if _, _, err := WriteTo(context.Background(), &artifact, "image", sparse.Dense(bytes.NewReader(body), uint64(len(body))), tc.writeOptions...); err != nil {
				t.Fatal(err)
			}
			source, _, err := SourceFrom(&emptyOnceBeforeEOFReader{reader: bytes.NewReader(artifact.Bytes())}, "", tc.readOptions...)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(body))
			if n, err := source.ReadAt(context.Background(), got, 0); n != len(got) || err != nil {
				t.Fatalf("ReadAt() = %d, %v", n, err)
			}
			if !bytes.Equal(got, body) {
				t.Fatal("round-trip mismatch")
			}
		})
	}

	if _, err := readOneByte(noProgressReader{}, make([]byte, 1)); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("permanent empty reader error = %v", err)
	}
}

func TestValidHeaderChecksumAcceptsSignedSum(t *testing.T) {
	header := ustarBlock(rawHeader{name: "payload", mode: 0o644})
	header[0] = 0xff
	copy(header[148:156], "        ")
	var signed int64
	for _, value := range header {
		signed += int64(int8(value))
	}
	field := fmt.Sprintf("%06o\x00 ", signed)
	if len(field) != 8 {
		t.Fatalf("signed checksum field length = %d", len(field))
	}
	copy(header[148:156], field)
	if !validHeaderChecksum(header[:]) {
		t.Fatal("historical signed checksum rejected")
	}

	artifact := append(append([]byte(nil), header[:]...), zeroBlock2[:]...)
	if _, err := ReadSeekFrom(bytes.NewReader(artifact), ""); err != nil {
		t.Fatalf("plaintext reader rejected signed checksum: %v", err)
	}
}

func TestCanonicalMarkerRejectsPrecedingExtensionHeader(t *testing.T) {
	var artifact bytes.Buffer
	writer := stdtar.NewWriter(&artifact)
	if err := writer.WriteHeader(&stdtar.Header{Name: "payload", Mode: 0o644, Size: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	markerName := DigestMarkerPrefix + strings.Repeat("a", 64)
	if err := writer.WriteHeader(&stdtar.Header{
		Name:       markerName,
		Mode:       0o644,
		Format:     stdtar.FormatPAX,
		PAXRecords: map[string]string{"comment": "extension before marker"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	if _, _, err := SourceAt(bytes.NewReader(artifact.Bytes()), int64(artifact.Len()), ""); !errors.Is(err, ErrInvalidCanonicalTarstream) {
		t.Fatalf("SourceAt extension-marker error = %v", err)
	}
	source, _, err := SourceFrom(bytes.NewReader(artifact.Bytes()), "")
	if err == nil {
		_, err = source.ReadAt(context.Background(), make([]byte, 4), 0)
	}
	if !errors.Is(err, ErrInvalidCanonicalTarstream) {
		t.Fatalf("SourceFrom extension-marker error = %v", err)
	}
}
