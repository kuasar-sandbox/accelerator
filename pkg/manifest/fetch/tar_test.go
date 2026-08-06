package fetch

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"golang.org/x/sys/unix"
)

// TestOpenTarStream: a tarstream artifact serves as a Stream — hole
// map from the envelope, concurrent random reads, nothing unpacked.
func TestOpenTarStream(t *testing.T) {
	const size = 2 << 20
	logical := make([]byte, size)
	copy(logical, "HEAD")
	copy(logical[1<<20:], "TAIL")
	holes := []sparse.Extent{{Offset: 4096, Size: (1 << 20) - 4096}, {Offset: (1 << 20) + 4096, Size: (1 << 20) - 4096}}

	src, err := sparse.NewSource(bytes.NewReader(logical), size, holes)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "img.img")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	wantScheme, wantDigest, err := tarstream.WriteTo(context.Background(), f, "image", src)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	st, err := OpenTarStream(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	d, ok := st.(tarstream.Digester)
	if !ok {
		t.Fatal("OpenTarStream result does not implement tarstream.Digester")
	}
	gotScheme, gotDigest := d.Digest()
	if gotScheme != wantScheme || gotDigest != wantDigest {
		t.Fatalf("digest = %q:%q; want %q:%q", gotScheme, gotDigest, wantScheme, wantDigest)
	}
	if st.Size() != size {
		t.Fatalf("size = %d", st.Size())
	}
	if run, err := st.RunAt(4096, size); err != nil || run.Kind() != sparse.Hole || run.End() != 1<<20 {
		t.Fatalf("RunAt(4096) = (%v, %v)", run, err)
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(off uint64) {
			defer wg.Done()
			buf := make([]byte, 4)
			if _, err := st.ReadAt(ctx, buf, off); err != nil && err != io.EOF {
				errs <- err
				return
			}
			want := logical[off : off+4]
			if !bytes.Equal(buf, want) {
				errs <- io.ErrUnexpectedEOF
			}
		}(uint64(i%2) << 20)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Non-tar input fails loudly.
	raw := filepath.Join(t.TempDir(), "raw.bin")
	os.WriteFile(raw, []byte("not a tar at all"), 0o644)
	if _, err := OpenTarStream(raw); err == nil {
		t.Fatal("raw file accepted as tar artifact")
	}

	// A generic tar remains readable through tarstream.SourceAt, but platform
	// artifact consumers require the digest capability.
	plain := filepath.Join(t.TempDir(), "plain.tar")
	pf, err := os.Create(plain)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(pf)
	if err := tw.WriteHeader(&tar.Header{Name: "image", Mode: 0o644, Size: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pf.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenTarStream(plain); err == nil {
		t.Fatal("markerless tar accepted as platform artifact")
	}
}

func TestTarFileStreamPrefetch(t *testing.T) {
	const logicalSize = 8 << 20
	logical := make([]byte, logicalSize)
	copy(logical, "HEAD")
	copy(logical[logicalSize-4:], "TAIL")
	src, err := sparse.NewSource(
		bytes.NewReader(logical),
		logicalSize,
		[]sparse.Extent{{Offset: 4096, Size: logicalSize - 8192}},
	)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "snapshot.snapshot")
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := tarstream.WriteTo(context.Background(), out, "snapshot", src); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == logicalSize {
		t.Fatalf("fixture physical size = logical size = %d; want distinct ranges", logicalSize)
	}

	type adviceCall struct {
		fd             int
		offset, length int64
		advice         int
	}
	var calls []adviceCall
	originalFadvise := fadvise
	fadvise = func(fd int, offset, length int64, advice int) error {
		calls = append(calls, adviceCall{
			fd:     fd,
			offset: offset,
			length: length,
			advice: advice,
		})
		return nil
	}
	t.Cleanup(func() { fadvise = originalFadvise })

	stream, err := OpenTarStream(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 0 {
		t.Fatalf("OpenTarStream submitted %d advice call(s); want none", len(calls))
	}
	local, ok := stream.(*tarFileStream)
	if !ok {
		t.Fatalf("OpenTarStream type = %T; want *tarFileStream", stream)
	}
	prefetcher, ok := stream.(Prefetcher)
	if !ok {
		t.Fatalf("OpenTarStream type %T does not implement Prefetcher", stream)
	}

	if err := prefetcher.Prefetch(context.Background()); err != nil {
		t.Fatalf("Prefetch: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("Prefetch advice calls = %d; want 1", len(calls))
	}
	if got, want := calls[0].fd, int(local.f.Fd()); got != want {
		t.Errorf("advice fd = %d; want open stream fd %d", got, want)
	}
	if got := calls[0].offset; got != 0 {
		t.Errorf("advice offset = %d; want 0", got)
	}
	if got, want := calls[0].length, info.Size(); got != want {
		t.Errorf("advice length = %d; want physical artifact size %d", got, want)
	}
	if got := calls[0].advice; got != unix.FADV_WILLNEED {
		t.Errorf("advice = %d; want FADV_WILLNEED (%d)", got, unix.FADV_WILLNEED)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := prefetcher.Prefetch(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Prefetch(canceled) error = %v; want context.Canceled", err)
	}
	if len(calls) != 1 {
		t.Fatalf("canceled Prefetch submitted advice; calls = %d, want 1", len(calls))
	}

	wantErr := errors.New("advice unavailable")
	fadvise = func(int, int64, int64, int) error { return wantErr }
	if err := prefetcher.Prefetch(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Prefetch advice error = %v; want wrapped %v", err, wantErr)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("Close after Prefetch: %v", err)
	}
}

func TestOpenEncryptedTarStream(t *testing.T) {
	codec, err := manifestcrypto.NewTarStreamCodec([32]byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("encrypted-local-artifact"), 1024)
	path := filepath.Join(t.TempDir(), "image.image")
	output, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	wantScheme, wantDigest, err := tarstream.WriteTo(
		context.Background(), output, "image",
		sparse.Dense(bytes.NewReader(body), uint64(len(body))),
		tarstream.WithCodec(codec, false),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}

	stream, err := OpenTarStream(path, tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(wantScheme, wantDigest))
	if err != nil {
		t.Fatal(err)
	}
	scheme, digest := stream.(tarstream.Digester).Digest()
	if scheme != wantScheme || digest != wantDigest {
		t.Fatalf("Digest() = %s:%s, want %s:%s", scheme, digest, wantScheme, wantDigest)
	}
	got := make([]byte, len(body))
	if _, err := stream.ReadAt(context.Background(), got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("encrypted file stream content mismatch")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenTarStream(path); !errors.Is(err, tarstream.ErrCodecRequired) {
		t.Fatalf("OpenTarStream without codec error = %v", err)
	}
	wrong, _ := manifestcrypto.NewTarStreamCodec([32]byte{9})
	if _, err := OpenTarStream(path, tarstream.WithCodec(wrong, true)); !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("OpenTarStream wrong-key error = %v", err)
	}
}

var _ Prefetcher = (*tarFileStream)(nil)
