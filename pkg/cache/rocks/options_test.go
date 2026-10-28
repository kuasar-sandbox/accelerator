//go:build !no_rocksdb

package rocks

import (
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

func TestBuildConfigBlobDBTuning(t *testing.T) {
	cfg := runtime.RocksConfig{
		DiskBytes:         "64MiB",
		WriteBufferBytes:  "32MiB",
		MaxBackgroundJobs: 3,
	}
	got := buildConfig(cfg)
	if got.WriteBufferBytes != 32<<20 {
		t.Fatalf("write buffer = %d, want 32MiB", got.WriteBufferBytes)
	}
	if got.MaxBytesForLevelBase != blobCFMaxBytesForLevelBase {
		t.Fatalf("max bytes for level base = %d, want %d", got.MaxBytesForLevelBase, blobCFMaxBytesForLevelBase)
	}
	if got.TargetFileSizeBase != blobCFTargetFileSizeBase {
		t.Fatalf("target file size base = %d, want %d", got.TargetFileSizeBase, blobCFTargetFileSizeBase)
	}
	if got.BlobGCForceThreshold != blobGCForceThreshold {
		t.Fatalf("blob GC force threshold = %v, want %v", got.BlobGCForceThreshold, blobGCForceThreshold)
	}
	if got.BlobFileSize != 4<<20 {
		t.Fatalf("blob file size = %d, want 4MiB", got.BlobFileSize)
	}

	large := buildConfig(runtime.RocksConfig{DiskBytes: "1TiB"})
	if large.BlobFileSize != 256<<20 {
		t.Fatalf("large blob file size = %d, want 256MiB", large.BlobFileSize)
	}
	tiny := buildConfig(runtime.RocksConfig{DiskBytes: "8MiB"})
	if tiny.BlobFileSize != 1<<20 {
		t.Fatalf("tiny blob file size = %d, want 1MiB", tiny.BlobFileSize)
	}
}

func TestResolveDiskPressureConfig(t *testing.T) {
	defaults := resolveDiskPressureConfig(runtime.RocksConfig{})
	if !defaults.enabled {
		t.Fatal("default pressure handling disabled")
	}
	if defaults.limit != 1<<40 {
		t.Fatalf("limit = %d, want 1TiB", defaults.limit)
	}
	if defaults.high != uint64(float64(defaults.limit)*defaultDiskHighWatermark) {
		t.Fatalf("high = %d", defaults.high)
	}
	if defaults.low != uint64(float64(defaults.limit)*defaultDiskLowWatermark) {
		t.Fatalf("low = %d", defaults.low)
	}
	if defaults.interval != defaultDiskMaintenancePeriod {
		t.Fatalf("interval = %s, want %s", defaults.interval, defaultDiskMaintenancePeriod)
	}

	custom := resolveDiskPressureConfig(runtime.RocksConfig{
		DiskBytes:               "100MiB",
		DiskHighWatermark:       0.5,
		DiskLowWatermark:        0.35,
		DiskMaintenanceInterval: "5s",
	})
	if custom.high != 50<<20 {
		t.Fatalf("custom high = %d, want 50MiB", custom.high)
	}
	if custom.low != 35<<20 {
		t.Fatalf("custom low = %d, want 35MiB", custom.low)
	}
	if custom.interval != 5*time.Second {
		t.Fatalf("custom interval = %s", custom.interval)
	}

	for _, disabled := range []string{"0", "off", "OFF"} {
		p := resolveDiskPressureConfig(runtime.RocksConfig{DiskMaintenanceInterval: disabled})
		if p.enabled {
			t.Fatalf("interval %q enabled pressure handling", disabled)
		}
	}
}
