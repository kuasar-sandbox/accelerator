package fetch

import (
	"fmt"
	"os"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

// tarFileStream is a local tarstream artifact as a Stream: the
// platform's at-rest container for images, snapshot bundles and
// overlays (one sparse payload plus an empty digest marker). The envelope's hole map drives
// RunAt; ReadAt is offset arithmetic over the packed region via
// *os.File.ReadAt (pread), meeting Stream's concurrent random-access
// contract. Nothing is unpacked.
type tarFileStream struct {
	sparse.Source
	f      *os.File
	digest string
}

// OpenTarStream opens the artifact at path (a tarstream envelope whose first
// entry is the payload and whose final empty entry declares SHA256) and returns
// it as a Stream that also implements tarstream.Digester. The caller owns the
// returned stream and must Close it. A non-tarstream file or a tar without the
// marker fails loudly.
func OpenTarStream(path string) (Stream, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("fetch: open %s: %w", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("fetch: stat %s: %w", path, err)
	}
	src, _, err := tarstream.SourceAt(f, st.Size(), "")
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("fetch: %s: not a tarstream artifact: %w", path, err)
	}
	d, ok := src.(tarstream.Digester)
	if !ok {
		_ = f.Close()
		return nil, fmt.Errorf("fetch: %s: tarstream artifact missing digest marker", path)
	}
	return &tarFileStream{Source: src, f: f, digest: d.Digest()}, nil
}

func (s *tarFileStream) Digest() string { return s.digest }
func (s *tarFileStream) Close() error   { return s.f.Close() }
