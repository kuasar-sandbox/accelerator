package fetch

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/tarstream"
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
	if err := tarstream.WriteTo(context.Background(), f, "image", src); err != nil {
		t.Fatal(err)
	}
	f.Close()

	st, err := OpenTarStream(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if st.Size() != size {
		t.Fatalf("size = %d", st.Size())
	}
	if kind, end, err := st.RunAt(4096, size); kind != sparse.Hole || end != 1<<20 || err != nil {
		t.Fatalf("RunAt(4096) = (%v, %d, %v)", kind, end, err)
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
}
