package bundle

import (
	"bytes"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
)

type metadataCountingReaderAt struct {
	inner     *bytes.Reader
	maxOffset atomic.Int64
}

func (r *metadataCountingReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	end := offset + int64(len(dst))
	for {
		previous := r.maxOffset.Load()
		if end <= previous || r.maxOffset.CompareAndSwap(previous, end) {
			break
		}
	}
	return r.inner.ReadAt(dst, offset)
}

func TestReadMetadataReadsOnlyPrefix(t *testing.T) {
	refs := []string{"file://parent.bundle", "file://ancestor.bundle@location:A"}
	data := syntheticBundleWithRefs(t, 20_000, 256, refs)
	source := &metadataCountingReaderAt{inner: bytes.NewReader(data)}
	metadata, err := ReadMetadata(source, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(metadata.Refs(), refs) {
		t.Fatalf("Refs = %v, want %v", metadata.Refs(), refs)
	}
	if metadata.Admission().Generation != "BENCH" {
		t.Fatalf("Admission = %#v", metadata.Admission())
	}
	if got := source.maxOffset.Load(); got > 2<<10 {
		t.Fatalf("metadata read reached offset %d, want only prefix", got)
	}
	copyRefs := metadata.Refs()
	copyRefs[0] = "file://mutated.bundle"
	if reflect.DeepEqual(copyRefs, metadata.Refs()) {
		t.Fatal("Metadata.Refs exposed mutable state")
	}
}

func TestReadMetadataWithoutRefs(t *testing.T) {
	data := syntheticBundle(t, 4_000, 1)
	metadata, err := ReadMetadata(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if refs := metadata.Refs(); len(refs) != 0 {
		t.Fatalf("Refs = %v", refs)
	}
}

func BenchmarkMetadataOpen4K(b *testing.B)  { benchmarkMetadataOpen(b, 4_000) }
func BenchmarkMetadataOpen20K(b *testing.B) { benchmarkMetadataOpen(b, 20_000) }

func benchmarkMetadataOpen(b *testing.B, chunks int) {
	data := syntheticBundle(b, chunks, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		metadata, err := ReadMetadata(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			b.Fatal(err)
		}
		if metadata.Admission().Generation != "BENCH" {
			b.Fatal(fmt.Errorf("unexpected admission %q", metadata.Admission().Generation))
		}
	}
}
