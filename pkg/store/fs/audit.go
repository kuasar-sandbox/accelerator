package fs

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// AuditDeltas scans one generation for delta objects and the aliases that
// name them (see store.DeltaAudit). It reads one byte of every chunk object to
// learn its format, so it is an admin-time operation, not a hot path.
//
// The scan makes two passes. First it collects every alias in the generation and
// the delta key each one names. Then it walks the chunk objects. A delta object
// that no alias names is an orphan: nothing references it, so it is a candidate
// for deletion, subject to the grace period described on store.DeltaAudit. A
// named key with no delta object behind it is counted as dangling. The audit
// only reports; it deletes nothing.
func (s *Store) AuditDeltas(ctx context.Context, generation store.Generation) (store.DeltaAudit, error) {
	var out store.DeltaAudit
	if err := validateGeneration(generation); err != nil {
		return out, err
	}

	// 1. Every alias and the delta object it names.
	named := make(map[store.ContentKey]struct{})
	aliasRoot := filepath.Join(s.root, string(store.PartitionAlias), string(generation))
	err := s.walkObjects(ctx, aliasRoot, func(path string, key store.ContentKey, info fs.FileInfo) error {
		body, err := readBufferedFile(path)
		if err != nil {
			return fmt.Errorf("fs: read alias %s: %w", path, err)
		}
		delta, err := store.DecodeAlias(body)
		if err != nil {
			return fmt.Errorf("fs: alias %s: %w", path, err)
		}
		out.Aliases++
		named[delta] = struct{}{}
		return nil
	})
	if err != nil {
		return out, err
	}
	out.Named = len(named)

	// 2. Every chunk object: format byte, size, mtime.
	present := make(map[store.ContentKey]struct{})
	chunkRoot := filepath.Join(s.root, string(store.PartitionChunk), string(generation))
	err = s.walkObjects(ctx, chunkRoot, func(path string, key store.ContentKey, info fs.FileInfo) error {
		out.Chunks++
		format, err := readFormatByte(path)
		if err != nil {
			return err
		}
		if format != store.ChunkFormatDelta {
			return nil
		}
		present[key] = struct{}{}
		out.DeltaObjects++
		out.DeltaBytes += uint64(info.Size())
		if _, ok := named[key]; ok {
			return nil
		}
		out.Orphans = append(out.Orphans, store.OrphanDelta{Key: key, Size: uint64(info.Size()), ModTime: info.ModTime()})
		out.OrphanBytes += uint64(info.Size())
		return nil
	})
	if err != nil {
		return out, err
	}
	for key := range named {
		if _, ok := present[key]; !ok {
			out.Dangling++
		}
	}
	return out, nil
}

// walkObjects visits every regular file under root whose name is a content
// key. A missing root is an empty generation, not an error. Staging files,
// directories, symlinks, and any other non-object name are skipped. The walk
// stops early if the context is cancelled.
func (s *Store) walkObjects(ctx context.Context, root string, visit func(path string, key store.ContentKey, info fs.FileInfo) error) error {
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			if os.IsNotExist(err) {
				return fs.SkipDir
			}
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		raw, derr := hex.DecodeString(entry.Name())
		if derr != nil || len(raw) != len(store.ContentKey{}) {
			return nil // staging files and anything else that is not an object
		}
		info, ierr := entry.Info()
		if ierr != nil {
			return ierr
		}
		return visit(path, store.ContentKey(raw), info)
	})
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("fs: walk %s: %w", root, err)
	}
	return nil
}

// readFormatByte returns the first byte of an object; an empty object has no
// format and is reported as 0. The object is opened without following symlinks,
// so the audit reads only the object itself.
func readFormatByte(path string) (byte, error) {
	f, err := openReadNoFollow(path)
	if err != nil {
		return 0, fmt.Errorf("fs: open %s: %w", path, err)
	}
	defer f.Close()
	var b [1]byte
	n, err := io.ReadFull(f, b[:])
	if err != nil {
		if n == 0 && (err == io.EOF || err == io.ErrUnexpectedEOF) {
			return 0, nil
		}
		return 0, fmt.Errorf("fs: read %s: %w", path, err)
	}
	return b[0], nil
}
