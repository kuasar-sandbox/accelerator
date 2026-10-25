package flatten

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func layerHash(b []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(b)) }

type observedBody struct {
	io.Reader
	closed   atomic.Int32
	closeErr error
}

func (b *observedBody) Close() error { b.closed.Add(1); return b.closeErr }

func encodedLayer(t *testing.T, codec string, plain []byte) ([]byte, string) {
	t.Helper()
	var b bytes.Buffer
	switch codec {
	case "gzip":
		w := gzip.NewWriter(&b)
		if _, err := w.Write(plain); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return b.Bytes(), "application/vnd.oci.image.layer.v1.tar+gzip"
	case "zstd":
		w, err := zstd.NewWriter(&b, zstd.WithEncoderConcurrency(1))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(plain); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return b.Bytes(), "application/vnd.oci.image.layer.v1.tar+zstd"
	default:
		return bytes.Clone(plain), "application/vnd.oci.image.layer.v1.tar"
	}
}

func streamInput(blob, plain []byte, media string, body *observedBody) LayerInput {
	return LayerInput{MediaType: media, Size: int64(len(blob)), Digest: layerHash(blob), DiffID: layerHash(plain),
		Open: func(context.Context) (io.ReadCloser, error) { return body, nil }}
}

func TestLayerCompleteStream(t *testing.T) {
	// tar.Reader stops before the final padding. DiffID covers that padding,
	// not just the entries or the first tar EOF; gzip/zstd trailers follow it.
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	if err := tw.WriteHeader(&tar.Header{Name: "payload", Mode: 0644, Size: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	archive.Write(make([]byte, 8192))
	plain := archive.Bytes()
	for _, codec := range []string{"plain", "gzip", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			blob, media := encodedLayer(t, codec, plain)
			body := &observedBody{Reader: bytes.NewReader(blob)}
			s, err := openLayer(context.Background(), streamInput(blob, plain, media, body), LayerLimits{})
			if err != nil {
				t.Fatal(err)
			}
			tr := tar.NewReader(s)
			if _, err := tr.Next(); err != nil {
				t.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, tr); err != nil {
				t.Fatal(err)
			}
			if _, err := tr.Next(); err != io.EOF {
				t.Fatalf("tar EOF: %v", err)
			}
			if s.decoded.n >= int64(len(plain)) {
				t.Fatal("test did not leave unread tar padding")
			}
			if err := s.finish(); err != nil {
				t.Fatal(err)
			}
			if s.decoded.n != int64(len(plain)) {
				t.Fatal("incomplete DiffID coverage")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if body.closed.Load() != 1 {
				t.Fatalf("Close count %d", body.closed.Load())
			}
		})
	}
}

func TestLayerValidationFailures(t *testing.T) {
	plain := bytes.Repeat([]byte("payload"), 2000)
	for _, codec := range []string{"plain", "gzip", "zstd"} {
		for _, fault := range []string{"size-small", "size-large", "digest", "diffid", "decoded-limit", "close", "truncated"} {
			t.Run(codec+"/"+fault, func(t *testing.T) {
				blob, media := encodedLayer(t, codec, plain)
				body := &observedBody{Reader: bytes.NewReader(blob)}
				in := streamInput(blob, plain, media, body)
				limits := LayerLimits{}
				switch fault {
				case "size-small":
					in.Size--
				case "size-large":
					in.Size++
				case "digest":
					in.Digest = layerHash(nil)
				case "diffid":
					in.DiffID = layerHash(nil)
				case "decoded-limit":
					limits.DecodedBytes = int64(len(plain) - 1)
				case "close":
					body.closeErr = errors.New("owned Close failed")
				case "truncated":
					body.Reader = bytes.NewReader(blob[:len(blob)-1])
				}
				s, err := openLayer(context.Background(), in, limits)
				if err == nil {
					err = s.finish()
				}
				if err == nil {
					t.Fatal("accepted invalid layer")
				}
				if body.closed.Load() != 1 {
					t.Fatalf("Close count %d", body.closed.Load())
				}
			})
		}
	}
}

func TestLayerTrailerVerification(t *testing.T) {
	plain := make([]byte, 2048)
	for _, codec := range []string{"gzip", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			blob, media := encodedLayer(t, codec, plain)
			blob[len(blob)-1] ^= 1 // valid compressed digest, invalid decoder trailer
			body := &observedBody{Reader: bytes.NewReader(blob)}
			s, err := openLayer(context.Background(), streamInput(blob, plain, media, body), LayerLimits{})
			if err == nil {
				err = s.finish()
			}
			if err == nil {
				t.Fatal("accepted corrupt trailer")
			}
		})
	}
	opened := false
	in := LayerInput{MediaType: "unknown", Size: 0, Digest: layerHash(nil), DiffID: layerHash(nil), Open: func(context.Context) (io.ReadCloser, error) {
		opened = true
		return io.NopCloser(strings.NewReader("")), nil
	}}
	if _, err := openLayer(context.Background(), in, LayerLimits{}); err == nil || opened {
		t.Fatalf("unknown type: err=%v opened=%v", err, opened)
	}
}

// Descriptors whose mediaType mislabels the blob (says gzip, blob is a plain
// tar — and permutations) are accepted by content sniffing, restoring the
// semantics fixed for the registry path in #156 (issue #154): docker and ggcr
// both sniff, and digest verification passes because it hashes stored bytes.
func TestLayerSniffOverridesDescriptorMediaType(t *testing.T) {
	plain := make([]byte, 4096)
	for i := range plain {
		plain[i] = byte(i)
	}
	gzBlob, _ := encodedLayer(t, "gzip", plain)
	zsBlob, _ := encodedLayer(t, "zstd", plain)
	// A zstd stream opening with an RFC 8878 §3.1.2 skippable frame (magic
	// 0x184D2A50, empty payload) before the first data frame, and the same
	// with the range's upper magic 0x184D2A5F carrying a one-byte payload.
	skippable := append([]byte{0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0}, zsBlob...)
	skippable5f := append([]byte{0x5f, 0x2a, 0x4d, 0x18, 1, 0, 0, 0, 0xAA}, zsBlob...)

	gzipMT := "application/vnd.oci.image.layer.v1.tar+gzip"
	zstdMT := "application/vnd.oci.image.layer.v1.tar+zstd"
	plainMT := "application/vnd.oci.image.layer.v1.tar"
	for _, tc := range []struct {
		name   string
		label  string
		blob   []byte
		diffID string
	}{
		{"gzip-label-plain-blob", gzipMT, plain, layerHash(plain)},
		{"zstd-label-plain-blob", zstdMT, plain, layerHash(plain)},
		{"plain-label-gzip-blob", plainMT, gzBlob, layerHash(plain)},
		{"plain-label-zstd-blob", plainMT, zsBlob, layerHash(plain)},
		{"gzip-label-zstd-blob", gzipMT, zsBlob, layerHash(plain)},
		{"zstd-label-gzip-blob", zstdMT, gzBlob, layerHash(plain)},
		{"zstd-skippable-frame-prefix", zstdMT, skippable, layerHash(plain)},
		{"zstd-skippable-magic-5f", zstdMT, skippable5f, layerHash(plain)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &observedBody{Reader: bytes.NewReader(tc.blob)}
			in := LayerInput{MediaType: tc.label, Size: int64(len(tc.blob)),
				Digest: layerHash(tc.blob), DiffID: tc.diffID,
				Open: func(context.Context) (io.ReadCloser, error) { return body, nil }}
			s, err := openLayer(context.Background(), in, LayerLimits{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(s)
			err = errors.Join(err, s.finish())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, plain) {
				t.Fatalf("decoded %d bytes, want the plain stream", len(got))
			}
			if body.closed.Load() != 1 {
				t.Fatalf("Close count %d", body.closed.Load())
			}
		})
	}

	// Streams too short to hold any magic stay on the plain path (#156 kept
	// 0–3 byte bodies out of the zstd decoder's ErrUnexpectedEOF).
	for _, n := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprintf("short-%d", n), func(t *testing.T) {
			blob := []byte("abcdefgh")[:n]
			for _, label := range []string{plainMT, gzipMT, zstdMT} {
				body := &observedBody{Reader: bytes.NewReader(blob)}
				in := LayerInput{MediaType: label, Size: int64(n),
					Digest: layerHash(blob), DiffID: layerHash(blob),
					Open: func(context.Context) (io.ReadCloser, error) { return body, nil }}
				s, err := openLayer(context.Background(), in, LayerLimits{})
				if err != nil {
					t.Fatalf("%s: %v", label, err)
				}
				if err := s.finish(); err != nil {
					t.Fatalf("%s: %v", label, err)
				}
			}
		})
	}
}

// errBody yields data, then fails with err on every subsequent Read.
type errBody struct {
	data []byte
	err  error
	off  int
}

func (b *errBody) Read(p []byte) (int, error) {
	if b.off < len(b.data) {
		n := copy(p, b.data[b.off:])
		b.off += n
		return n, nil
	}
	return 0, b.err
}

// Real body errors must surface from Read/finish: io.ReadFull only
// synthesizes ErrUnexpectedEOF from a literal io.EOF, so the EOF family the
// sniff ignores cannot swallow transport failures — and a clean short EOF is
// still caught downstream by the decoder trailer / size / DiffID checks.
func TestLayerSniffSurfacesBodyErrors(t *testing.T) {
	sentinel := errors.New("body failed mid-sniff")
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"partial-bytes-then-error", []byte("not-a-magic-prefix")},
		{"error-at-zero-bytes", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &observedBody{Reader: &errBody{data: tc.data, err: sentinel}}
			in := LayerInput{MediaType: "application/vnd.oci.image.layer.v1.tar",
				Size: int64(len(tc.data)) + 64, Digest: layerHash(tc.data), DiffID: layerHash(tc.data),
				Open: func(context.Context) (io.ReadCloser, error) { return body, nil }}
			s, err := openLayer(context.Background(), in, LayerLimits{})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.finish(); !errors.Is(err, sentinel) {
				t.Fatalf("finish: %v, want wrapped %v", err, sentinel)
			}
			if body.closed.Load() != 1 {
				t.Fatalf("Close count %d", body.closed.Load())
			}
		})
	}

	// A stream ending cleanly right after the gzip header — size and digest
	// fully consistent with the stored bytes — must still be rejected: the
	// sniff ignores the short EOF, and the trailer check catches the cut.
	t.Run("clean-eof-truncated-gzip", func(t *testing.T) {
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		if _, err := w.Write([]byte("payload")); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		truncated := b.Bytes()[:10] // header only, no member data, no trailer
		body := &observedBody{Reader: bytes.NewReader(truncated)}
		in := streamInput(truncated, []byte("payload"), "application/vnd.oci.image.layer.v1.tar+gzip", body)
		s, err := openLayer(context.Background(), in, LayerLimits{})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.finish(); err == nil {
			t.Fatal("accepted a gzip stream truncated after its header")
		}
		if body.closed.Load() != 1 {
			t.Fatalf("Close count %d", body.closed.Load())
		}
	})
}

// chunkedBody yields at most chunk bytes per Read, exercising the sniff's
// ReadFull aggregation and the prefix replay across fragment boundaries.
type chunkedBody struct {
	data  []byte
	off   int
	chunk int
}

func (b *chunkedBody) Read(p []byte) (int, error) {
	if b.off >= len(b.data) {
		return 0, io.EOF
	}
	n := b.chunk
	if n > len(p) {
		n = len(p)
	}
	if n > len(b.data)-b.off {
		n = len(b.data) - b.off
	}
	copy(p, b.data[b.off:b.off+n])
	b.off += n
	return n, nil
}

func TestLayerSniffFragmentedReads(t *testing.T) {
	plain := make([]byte, 96)
	for i := range plain {
		plain[i] = byte(i * 7)
	}
	for _, codec := range []string{"plain", "gzip", "zstd"} {
		blob, media := encodedLayer(t, codec, plain)
		for _, chunk := range []int{1, 2, 3} {
			t.Run(fmt.Sprintf("%s/chunk-%d", codec, chunk), func(t *testing.T) {
				body := &observedBody{Reader: &chunkedBody{data: blob, chunk: chunk}}
				s, err := openLayer(context.Background(), streamInput(blob, plain, media, body), LayerLimits{})
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(s)
				err = errors.Join(err, s.finish())
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, plain) {
					t.Fatalf("decoded %d bytes, want %d", len(got), len(plain))
				}
				if body.closed.Load() != 1 {
					t.Fatalf("Close count %d", body.closed.Load())
				}
			})
		}
	}
}

type blockedLayerBody struct {
	entered, closed chan struct{}
	once            sync.Once
	closes          atomic.Int32
}

func (b *blockedLayerBody) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.closed
	return 0, io.ErrClosedPipe
}
func (b *blockedLayerBody) Close() error {
	if b.closes.Add(1) == 1 {
		close(b.closed)
	}
	return nil
}

func TestLayerCancellationClosesOwnedBody(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := &blockedLayerBody{entered: make(chan struct{}), closed: make(chan struct{})}
	in := LayerInput{MediaType: "application/vnd.oci.image.layer.v1.tar", Size: 1024, Digest: layerHash(nil), DiffID: layerHash(nil), Open: func(context.Context) (io.ReadCloser, error) { return body, nil }}
	s, err := openLayer(ctx, in, LayerLimits{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.finish() }()
	<-body.entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel left a reader running")
	}
	if body.closes.Load() != 1 {
		t.Fatal("owned body closed more than once")
	}
}
