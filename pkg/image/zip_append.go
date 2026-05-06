package flatten

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"
)

// zipEpoch is the fixed mtime stamped onto every ZIP entry. Pinning
// it (rather than using time.Now()) is what makes the appended ZIP
// byte-identical across rebuilds — a prerequisite for cross-image
// dedup at the chunker level.
var zipEpoch = time.Date(1980, time.January, 1, 0, 0, 0, 0, time.UTC)

// AppendConfigZip appends a ZIP archive to the file at path. The
// archive contains a single STORED-mode entry, ConfigFileName,
// holding the deterministic JSON of cfg.
//
// EROFS readers (and `mount -t erofs`) ignore the trailing bytes
// because the EROFS image self-describes its own size in the
// superblock; ZIP readers locate the End of Central Directory by
// scanning backwards from EOF and tolerate arbitrary prefix data.
// The two formats coexist in one file without modification to either.
func AppendConfigZip(path string, cfg *RuntimeConfig) error {
	body, err := cfg.MarshalDeterministic()
	if err != nil {
		return fmt.Errorf("zip append: marshal config: %w", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return fmt.Errorf("zip append: open %s: %w", path, err)
	}
	defer f.Close()
	zw := zip.NewWriter(f)
	hdr := &zip.FileHeader{
		Name:     ConfigFileName,
		Method:   zip.Store, // no compression — keeps content directly inspectable
		Modified: zipEpoch,
	}
	w, err := zw.CreateHeader(hdr)
	if err != nil {
		return fmt.Errorf("zip append: create entry: %w", err)
	}
	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("zip append: write entry: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("zip append: close zip: %w", err)
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
	zr, err := zip.NewReader(r, size)
	if err != nil {
		// archive/zip returns ErrFormat when EOCD isn't found, which
		// is exactly the "no ZIP appended" case for our purposes.
		if errors.Is(err, zip.ErrFormat) {
			return nil, fmt.Errorf("read config: no zip trailer: %w", fs.ErrNotExist)
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
	return nil, fmt.Errorf("read config: no %s entry in zip: %w", ConfigFileName, fs.ErrNotExist)
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
