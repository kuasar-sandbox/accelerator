package fetch

import (
	"fmt"
	"os"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/sparse"
)

// fileStream is a local file as a Stream: a sparse.Source built from
// the file plus its probed hole map (sparse.ProbeHoles — allocation
// metadata, so RunAt classifies filesystem holes as Hole and allocated
// zeros as Data, letting a sparse file fall through correctly as an
// overlay layer), owning the descriptor's lifetime. *os.File.ReadAt is
// concurrency-safe, so the stream meets Stream's strengthened
// contract.
type fileStream struct {
	sparse.Source
	f *os.File
}

// OpenFileStream opens path read-only and returns it as a Stream. The caller
// owns the returned stream and must Close it.
func OpenFileStream(path string) (Stream, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("fetch: open %s: %w", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("fetch: stat %s: %w", path, err)
	}
	holes, err := sparse.ProbeHoles(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("fetch: probe holes %s: %w", path, err)
	}
	src, err := sparse.NewSource(f, uint64(st.Size()), holes)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("fetch: %s: %w", path, err)
	}
	return &fileStream{Source: src, f: f}, nil
}

func (s *fileStream) Close() error { return s.f.Close() }
