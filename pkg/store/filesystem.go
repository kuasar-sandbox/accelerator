package store

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

// FilesystemStore implements ContentStore using the local filesystem.
// Chunks are stored at: {root}/{hash[0:2]}/{hash[2:4]}/{hash}
type FilesystemStore struct {
	root string
}

// NewFilesystemStore creates a new filesystem-backed content store.
func NewFilesystemStore(root string) (*FilesystemStore, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, fmt.Errorf("store: create root: %w", err)
	}
	return &FilesystemStore{root: root}, nil
}

func (fs *FilesystemStore) chunkPath(hash [32]byte) string {
	h := hex.EncodeToString(hash[:])
	return filepath.Join(fs.root, h[:2], h[2:4], h)
}

func (fs *FilesystemStore) Put(_ context.Context, hash [32]byte, data []byte) error {
	path := fs.chunkPath(hash)

	// Put-if-not-exists: skip if already stored.
	if _, err := os.Stat(path); err == nil {
		return nil
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("store: mkdir: %w", err)
	}

	// Write to temp file + atomic rename for crash safety.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("store: write tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("store: rename: %w", err)
	}
	return nil
}

func (fs *FilesystemStore) Get(_ context.Context, hash [32]byte) ([]byte, error) {
	data, err := os.ReadFile(fs.chunkPath(hash))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("store: read: %w", err)
	}
	return data, nil
}

func (fs *FilesystemStore) Exists(_ context.Context, hash [32]byte) (bool, error) {
	_, err := os.Stat(fs.chunkPath(hash))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("store: stat: %w", err)
	}
	return true, nil
}

func (fs *FilesystemStore) Delete(_ context.Context, hash [32]byte) error {
	err := os.Remove(fs.chunkPath(hash))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("store: delete: %w", err)
	}
	return nil
}
