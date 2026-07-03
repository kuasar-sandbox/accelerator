package fetch

import (
	"fmt"
	"os"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

// tarFileStream is a local tarstream artifact as a Stream: the
// platform's at-rest container for images, snapshot bundles and
// overlays (single-entry sparse tars). The envelope's hole map drives
// RunAt; ReadAt is offset arithmetic over the packed region via
// *os.File.ReadAt (pread), meeting Stream's concurrent random-access
// contract. Nothing is unpacked.
type tarFileStream struct {
	sparse.Source
	f *os.File
}

// OpenTarStream opens the artifact at path (a tarstream envelope; its
// first regular entry is the payload) and returns it as a Stream. The
// caller owns the returned stream and must Close it. A non-tarstream
// file fails loudly — platform artifacts are always tar-contained.
func OpenTarStream(path string) (Stream, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("fetch: open %s: %w", path, err)
	}
	src, _, err := tarstream.SourceAt(f, "")
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("fetch: %s: not a tarstream artifact: %w", path, err)
	}
	return &tarFileStream{Source: src, f: f}, nil
}

func (s *tarFileStream) Close() error { return s.f.Close() }
