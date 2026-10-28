//go:build !no_rocksdb

package rocks

import (
	"github.com/kuasar-sandbox/accelerator/internal/util"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/rocks/internal/native"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

const (
	blobCFMaxBytesForLevelBase = 32 << 20
	blobCFTargetFileSizeBase   = 8 << 20
	blobGCForceThreshold       = 0.5
)

// buildConfig resolves only the settings used by this store.
func buildConfig(cfg runtime.RocksConfig) native.Config {
	diskBytes := parseDiskBytes(cfg)
	// BlockCache size = disk_bytes * mem_ratio.
	memRatio := cfg.MemRatio
	if memRatio <= 0 {
		memRatio = 0.01
	}
	blockCacheBytes := uint64(float64(diskBytes) * memRatio)
	if blockCacheBytes < 64<<20 {
		blockCacheBytes = 64 << 20 // min 64 MiB
	}

	// Block size
	blockSize, _ := util.ParseSize(cfg.BlockSize)
	if blockSize == 0 {
		blockSize = 64 << 10 // 64 KiB
	}

	// Bloom filter
	bloomBits := cfg.BloomBits
	if bloomBits <= 0 {
		bloomBits = 15
	}
	// Write buffer — default 256 MiB to reduce flush frequency under concurrent writes.
	writeBufferBytes, _ := util.ParseSize(cfg.WriteBufferBytes)
	if writeBufferBytes == 0 {
		writeBufferBytes = 256 << 20 // 256 MiB default (was 64 MiB Go/RocksDB default)
	}

	// Background jobs
	maxJobs := cfg.MaxBackgroundJobs
	if maxJobs <= 0 {
		maxJobs = 8 // default 8 (was 4)
	}
	return native.Config{
		CacheBytes:            blockCacheBytes,
		BlockBytes:            blockSize,
		WriteBufferBytes:      writeBufferBytes,
		BloomBits:             bloomBits,
		BackgroundCompactions: maxJobs,
		DirectReads:           runtime.BoolDefault(cfg.DirectReads, true),
		BlobFileSize:          blobFileSize(diskBytes),
		BlobGCForceThreshold:  blobGCForceThreshold,
		MaxBytesForLevelBase:  blobCFMaxBytesForLevelBase,
		TargetFileSizeBase:    blobCFTargetFileSizeBase,
	}
}

func blobFileSize(diskBytes uint64) uint64 {
	const (
		minBlobFileSize = 1 << 20
		maxBlobFileSize = 256 << 20
	)
	size := diskBytes / 16
	if size < minBlobFileSize {
		return minBlobFileSize
	}
	if size > maxBlobFileSize {
		return maxBlobFileSize
	}
	return size
}
