//go:build !no_rocksdb

package rocks

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/rocks/internal/native"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestDiskUsageBytes(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), bytes.Repeat([]byte{1}, 1024), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nested", "b"), bytes.Repeat([]byte{2}, 2048), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := diskUsageBytes(root)
	if err != nil {
		t.Fatal(err)
	}
	if got != 3072 {
		t.Fatalf("disk usage = %d, want 3072", got)
	}

	linkRoot := filepath.Join(t.TempDir(), "rocks-link")
	if err := os.Symlink(root, linkRoot); err != nil {
		t.Fatal(err)
	}
	gotLink, err := diskUsageBytes(linkRoot)
	if err != nil {
		t.Fatal(err)
	}
	if gotLink != 3072 {
		t.Fatalf("symlinked disk usage = %d, want 3072", gotLink)
	}
}

func TestDiskPressureCompactionEvictsColdKeys(t *testing.T) {
	cfg := tempRocksConfigT(t)
	cfg.DiskBytes = "1MiB"
	cfg.DiskHighWatermark = 0.0001
	cfg.DiskMaintenanceInterval = "1h"
	fc := runtime.FreqConfig{
		Counters:       "1K",
		ResetAfter:     "1G",
		EvictThreshold: 1,
	}
	s, err := openImpl(cfg, fc)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	coldKey := store.ContentKey(sha256.Sum256([]byte("cold-disk-pressure")))
	hotKey := store.ContentKey(sha256.Sum256([]byte("hot-disk-pressure")))
	value := bytes.Repeat([]byte{0x42}, 16<<10)
	if err := s.Fill(ctx, store.PartitionChunk, coldKey, value); err != nil {
		t.Fatal(err)
	}
	if err := s.Fill(ctx, store.PartitionChunk, hotKey, value); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		res, blob, err := s.Get(ctx, store.PartitionChunk, hotKey)
		if err != nil || res != cache.CacheHit || blob == nil {
			t.Fatalf("hot Get = %v, %v, %v", res, blob, err)
		}
		blob.Release()
	}
	if err := s.db.Flush(native.Chunk); err != nil {
		t.Fatal(err)
	}

	p := resolveDiskPressureConfig(cfg)
	usage, err := diskUsageBytes(cfg.Path)
	if err != nil {
		t.Fatal(err)
	}
	if usage < p.high {
		t.Fatalf("disk usage %d below high watermark %d", usage, p.high)
	}
	s.runDiskPressure(p, time.Now())

	if rawKeyExists(t, s, native.Chunk, coldKey[:]) {
		t.Fatal("cold key survived disk-pressure compaction")
	}
	if !rawKeyExists(t, s, native.Chunk, hotKey[:]) {
		t.Fatal("hot key was evicted by disk-pressure compaction")
	}
}

func TestDiskPressureCooldownStartsAtCompletion(t *testing.T) {
	cfg := tempRocksConfigT(t)
	cfg.DiskBytes = "1MiB"
	cfg.DiskHighWatermark = 0.0001
	cfg.DiskLowWatermark = 0.00005
	cfg.DiskMaintenanceInterval = "1h"
	fc := runtime.FreqConfig{
		Counters:       "1K",
		ResetAfter:     "1G",
		EvictThreshold: 1,
	}
	s, err := openImpl(cfg, fc)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	hotKey := store.ContentKey(sha256.Sum256([]byte("hot-cooldown-completion")))
	value := bytes.Repeat([]byte{0x42}, 16<<10)
	if err := s.Fill(ctx, store.PartitionChunk, hotKey, value); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		res, blob, err := s.Get(ctx, store.PartitionChunk, hotKey)
		if err != nil || res != cache.CacheHit || blob == nil {
			t.Fatalf("hot Get = %v, %v, %v", res, blob, err)
		}
		blob.Release()
	}
	if err := s.db.Flush(native.Chunk); err != nil {
		t.Fatal(err)
	}

	t0 := time.Now()
	finishedAt := t0.Add(6 * time.Minute)
	p := resolveDiskPressureConfig(cfg)
	p.now = func() time.Time { return finishedAt }

	s.runDiskPressure(p, t0)

	wantNext := finishedAt.Add(diskPressureCompactionCooloff)
	s.pressureMu.Lock()
	gotNext := s.pressureNext
	s.pressureMu.Unlock()
	if !gotNext.Equal(wantNext) {
		t.Fatalf("pressureNext = %v, want %v (5m after completion, not start)", gotNext, wantNext)
	}
}

func rawKeyExists(t *testing.T, s *impl, cf int, key []byte) bool {
	t.Helper()
	found, err := s.db.Read(cf, key, func(n int) []byte { return make([]byte, n) })
	if err != nil {
		t.Fatal(err)
	}
	return found
}
