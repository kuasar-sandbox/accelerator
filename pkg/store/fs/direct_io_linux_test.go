//go:build linux

package fs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestDirectIOAlignmentHelpers(t *testing.T) {
	for _, test := range []struct {
		size, alignment, want int64
	}{
		{0, 512, 0},
		{1, 512, 512},
		{512, 512, 512},
		{513, 512, 1024},
		{4097, 4096, 8192},
	} {
		got, err := roundUpDirectLength(test.size, test.alignment)
		if err != nil || got != test.want {
			t.Errorf("roundUpDirectLength(%d, %d) = %d, %v; want %d", test.size, test.alignment, got, err, test.want)
		}
	}
	for _, alignment := range []int{512, 4096, 8192} {
		raw, aligned := alignedBuffer(8192, alignment)
		if uintptr(unsafe.Pointer(&aligned[0]))%uintptr(alignment) != 0 {
			t.Errorf("buffer is not aligned to %d", alignment)
		}
		if len(raw) < len(aligned) {
			t.Fatal("aligned slice escaped raw allocation")
		}
	}
}

func TestDirectIOGetEmptySmallAndUnalignedObjects(t *testing.T) {
	writer, root := newTestStore(t)
	reader, err := New(Config{Root: root, DirectIO: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, length := range []int{0, 1, 511, 512, 513, 4097, 64*1024 + 7} {
		data := make([]byte, length)
		for i := range data {
			data[i] = byte(i%251 + 1)
		}
		key := contentKey(data)
		size := int64(len(data))
		writeObject(t, writer, testGeneration, store.PartitionChunk, key, data, &size, true)
		found, got, err := reader.Get(context.Background(), testGeneration, store.PartitionChunk, key)
		if errors.Is(err, ErrDirectIOUnsupported) {
			t.Skipf("test filesystem does not support strict Direct I/O: %v", err)
		}
		if err != nil || !found || !bytes.Equal(got, data) {
			t.Fatalf("length %d: Get = %v, %d bytes, %v", length, found, len(got), err)
		}
	}
}

func TestDirectIOUnsupportedErrorsNeverFallback(t *testing.T) {
	for _, errno := range []error{unix.EINVAL, unix.EOPNOTSUPP, unix.ENOSYS} {
		if err := directIOError("test", errno); !errors.Is(err, ErrDirectIOUnsupported) {
			t.Errorf("directIOError(%v) = %v, want ErrDirectIOUnsupported", errno, err)
		}
	}
}

func TestFIFOIsRejected(t *testing.T) {
	backend, _ := newTestStore(t)
	key := contentKey([]byte("fifo"))
	path := backend.objectPath(store.PartitionChunk, testGeneration, key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if exists, err := backend.Exists(context.Background(), testGeneration, store.PartitionChunk, key, nil); err == nil || exists {
		t.Fatalf("Exists FIFO = %v, %v; want error", exists, err)
	}
}
