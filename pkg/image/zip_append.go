package image

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/kuasar-sandbox/accelerator/pkg/tailzip"
)

// AppendConfigZip appends the deterministic runtime configuration suffix.
func AppendConfigZip(path string, cfg *RuntimeConfig) error {
	body, err := cfg.MarshalDeterministic()
	if err != nil {
		return fmt.Errorf("zip append: marshal config: %w", err)
	}
	tail, err := tailzip.Encode([]tailzip.Entry{{Name: ConfigFileName, Body: body}})
	if err != nil {
		return fmt.Errorf("zip append: encode: %w", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return fmt.Errorf("zip append: open %s: %w", path, err)
	}
	if _, err = f.Write(tail); err != nil {
		_ = f.Close()
		return fmt.Errorf("zip append: write: %w", err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("zip append: close: %w", err)
	}
	return nil
}

// ReadConfig locates the trailing ZIP via archive/zip's EOCD scan
// (which tolerates arbitrary prefix data — here the EROFS image) on
// the supplied io.ReaderAt of given size and decodes the config.json
// entry into a RuntimeConfig.
//
// Returns fs.ErrNotExist if the data has no recognisable ZIP trailer
// or no config.json entry inside.
//
// The reader is read concurrently by archive/zip during the EOCD
// scan and entry decode; r must be safe for concurrent ReadAt calls.
func ReadConfig(r io.ReaderAt, size int64) (*RuntimeConfig, error) {
	zr, _, err := tailzip.Open(r, size, tailzip.Options{MaxEntries: 1024})
	if err != nil {
		if tailzip.IsNotFound(err) {
			return nil, fmt.Errorf("read config: no zip trailer: %w", os.ErrNotExist)
		}
		return nil, fmt.Errorf("read config: open zip: %w", err)
	}
	for _, ze := range zr.File {
		if ze.Name != ConfigFileName {
			continue
		}
		rc, err := ze.Open()
		if err != nil {
			return nil, fmt.Errorf("read config: open entry: %w", err)
		}
		body, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, fmt.Errorf("read config: read entry: %w", err)
		}
		var cfg RuntimeConfig
		if err := json.Unmarshal(body, &cfg); err != nil {
			return nil, fmt.Errorf("read config: parse json: %w", err)
		}
		return &cfg, nil
	}
	return nil, fmt.Errorf("read config: no %s entry in zip: %w", ConfigFileName, os.ErrNotExist)
}

// ReadConfigFromFile opens path and delegates to ReadConfig. The
// file-path-vs-reader split lets manifest:// callers inject a
// fetch.Fetcher-backed io.ReaderAt without re-implementing the
// ZIP trailer scan.
func ReadConfigFromFile(path string) (*RuntimeConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	return ReadConfig(f, st.Size())
}
