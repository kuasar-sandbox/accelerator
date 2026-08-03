package fetch

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"golang.org/x/sys/unix"
)

var fadvise = unix.Fadvise

// tarFileStream is a local tarstream artifact as a Stream: the
// platform's at-rest container for images, snapshot bundles and
// overlays (one sparse payload plus an empty digest marker). The envelope's hole map drives
// RunAt; ReadAt is offset arithmetic over the packed region via
// *os.File.ReadAt (pread), meeting Stream's concurrent random-access
// contract. Nothing is unpacked.
type tarFileStream struct {
	sparse.Source
	f            *os.File
	physicalSize int64
	sourceCloser io.Closer
	digestScheme string
	digest       string
}

// OpenTarStream opens a plaintext canonical tarstream or encrypted v1 artifact
// and returns it as a Stream that also implements tarstream.Digester. Options
// carry the local codec, plaintext policy, and expected identity. The caller
// owns the returned stream and must Close it. A non-tarstream file or a tar
// without the canonical marker fails loudly.
func OpenTarStream(path string, options ...tarstream.ReadOption) (Stream, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("fetch: open %s: %w", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("fetch: stat %s: %w", path, err)
	}
	src, _, err := tarstream.SourceAt(f, st.Size(), "", options...)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("fetch: %s: not a tarstream artifact: %w", path, err)
	}
	d, ok := src.(tarstream.Digester)
	if !ok {
		_ = f.Close()
		return nil, fmt.Errorf("fetch: %s: tarstream artifact missing digest marker", path)
	}
	digestScheme, digest := d.Digest()
	stream := &tarFileStream{
		Source:       src,
		f:            f,
		physicalSize: st.Size(),
		digestScheme: digestScheme,
		digest:       digest,
	}
	if closer, ok := src.(io.Closer); ok {
		stream.sourceCloser = closer
	}
	return stream, nil
}

func (s *tarFileStream) Digest() (string, string) { return s.digestScheme, s.digest }
func (s *tarFileStream) Close() error {
	if s.sourceCloser != nil {
		if err := s.sourceCloser.Close(); err != nil {
			_ = s.f.Close()
			return err
		}
	}
	return s.f.Close()
}

// Prefetch submits a best-effort readahead hint for this artifact's complete
// physical file range. It does not wait for the range to become resident.
func (s *tarFileStream) Prefetch(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := fadvise(int(s.f.Fd()), 0, s.physicalSize, unix.FADV_WILLNEED); err != nil {
		return fmt.Errorf("fetch: fadvise WILLNEED: %w", err)
	}
	return nil
}
