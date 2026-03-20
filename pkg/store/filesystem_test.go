package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"os"
	"testing"
)

func TestFilesystemStorePutGet(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFilesystemStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 512*1024)
	rand.Read(data)
	hash := sha256.Sum256(data)
	ctx := context.Background()

	if err := s.Put(ctx, hash, data); err != nil {
		t.Fatal(err)
	}

	got, err := s.Get(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(data) {
		t.Fatalf("data length mismatch: got %d, want %d", len(got), len(data))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("data mismatch at byte %d", i)
		}
	}
}

func TestFilesystemStoreDedup(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFilesystemStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("hello world chunk")
	hash := sha256.Sum256(data)
	ctx := context.Background()

	// Put twice should succeed (put-if-not-exists).
	if err := s.Put(ctx, hash, data); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, hash, data); err != nil {
		t.Fatal(err)
	}
}

func TestFilesystemStoreNotFound(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFilesystemStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	var hash [32]byte
	rand.Read(hash[:])
	ctx := context.Background()

	_, err = s.Get(ctx, hash)
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestFilesystemStoreExists(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFilesystemStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("test data")
	hash := sha256.Sum256(data)
	ctx := context.Background()

	exists, err := s.Exists(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("should not exist before Put")
	}

	if err := s.Put(ctx, hash, data); err != nil {
		t.Fatal(err)
	}

	exists, err = s.Exists(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("should exist after Put")
	}
}

func TestFilesystemStoreDelete(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFilesystemStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("to be deleted")
	hash := sha256.Sum256(data)
	ctx := context.Background()

	if err := s.Put(ctx, hash, data); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, hash); err != nil {
		t.Fatal(err)
	}

	exists, err := s.Exists(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("should not exist after Delete")
	}
}

func TestFilesystemStoreDirectoryLayout(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFilesystemStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("layout test")
	hash := sha256.Sum256(data)
	ctx := context.Background()

	if err := s.Put(ctx, hash, data); err != nil {
		t.Fatal(err)
	}

	// Verify the two-level directory structure exists.
	path := s.chunkPath(hash)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("chunk file not found at expected path: %s", path)
	}
}
