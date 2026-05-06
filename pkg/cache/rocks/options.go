package rocks

import (
	"github.com/fullof-work/mass-sandbox/pkg/config"
	"github.com/fullof-work/mass-sandbox/pkg/cache/runtime"
	grocksdb "github.com/linxGnu/grocksdb"
)

// OpenOptions holds resolved RocksDB options ready for Open.
type OpenOptions struct {
	DBOpts    *grocksdb.Options
	ChunkCF   *grocksdb.Options
	ManifestCF *grocksdb.Options
	BBTOpts   *grocksdb.BlockBasedTableOptions
}

// BuildOptions creates RocksDB options from a runtime.RocksConfig.
func BuildOptions(cfg runtime.RocksConfig) *OpenOptions {
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()

	// BlockCache size = disk_bytes * mem_ratio
	diskBytes, _ := config.ParseSize(cfg.DiskBytes)
	if diskBytes == 0 {
		diskBytes = 1 << 40 // 1 TiB default
	}
	memRatio := cfg.MemRatio
	if memRatio <= 0 {
		memRatio = 0.01
	}
	blockCacheBytes := uint64(float64(diskBytes) * memRatio)
	if blockCacheBytes < 64<<20 {
		blockCacheBytes = 64 << 20 // min 64 MiB
	}
	bbto.SetBlockCache(grocksdb.NewLRUCache(blockCacheBytes))

	// Block size
	blockSize, _ := config.ParseSize(cfg.BlockSize)
	if blockSize == 0 {
		blockSize = 64 << 10 // 64 KiB
	}
	bbto.SetBlockSize(int(blockSize))

	// Bloom filter
	bloomBits := cfg.BloomBits
	if bloomBits <= 0 {
		bloomBits = 15
	}
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(float64(bloomBits)))

	// Pin L0 filter and index
	bbto.SetPinL0FilterAndIndexBlocksInCache(true)
	bbto.SetCacheIndexAndFilterBlocks(true)

	// Database options
	dbOpts := grocksdb.NewDefaultOptions()
	dbOpts.SetBlockBasedTableFactory(bbto)
	dbOpts.SetCreateIfMissing(true)
	dbOpts.SetCreateIfMissingColumnFamilies(true)

	// No compression (encrypted ciphertext has high entropy)
	dbOpts.SetCompression(grocksdb.NoCompression)

	// Direct reads
	if runtime.BoolDefault(cfg.DirectReads, true) {
		dbOpts.SetUseDirectReads(true)
	}

	// Write buffer — default 256 MiB to reduce flush frequency under concurrent writes.
	writeBufferBytes, _ := config.ParseSize(cfg.WriteBufferBytes)
	if writeBufferBytes == 0 {
		writeBufferBytes = 256 << 20 // 256 MiB default (was 64 MiB Go/RocksDB default)
	}
	dbOpts.SetWriteBufferSize(writeBufferBytes)
	dbOpts.SetMaxWriteBufferNumber(4) // allow 4 concurrent memtables before stall

	// Background jobs
	maxJobs := cfg.MaxBackgroundJobs
	if maxJobs <= 0 {
		maxJobs = 8 // default 8 (was 4)
	}
	dbOpts.SetMaxBackgroundCompactions(maxJobs)
	dbOpts.SetMaxBackgroundFlushes(2)

	// BlobDB on both chunk and manifest CFs.
	//
	// chunk values are ~256 KiB typical (one FastCDC block), manifest
	// values range from a few KiB (metadata + sealed key table) to
	// several MiB (large images). Large values walking the full LSM
	// leveled compaction path would burn 10-30x write amplification; we
	// route them through independent .blob files so compaction only
	// touches key+ref pairs (~50 bytes/key). Write amplification drops
	// to 1-3x, and the resulting IO pattern is mostly sequential
	// append — friendly to SSD endurance and latency tails.
	//
	// Small values (< 4 KiB) remain inline in the SST: SetMinBlobSize
	// is the per-value gate, so a KiB-sized manifest metadata blob is
	// unaffected and still costs exactly one IO to fetch.
	applyBlobDB := func(o *grocksdb.Options) {
		o.EnableBlobFiles(true)
		o.SetMinBlobSize(4 << 10)   // 4 KiB inline threshold
		o.SetBlobFileSize(256 << 20) // 256 MiB per blob file
		o.EnableBlobGC(true)         // background reclamation of stale blobs
	}

	chunkOpts := grocksdb.NewDefaultOptions()
	chunkOpts.SetBlockBasedTableFactory(bbto)
	chunkOpts.SetCompression(grocksdb.NoCompression)
	applyBlobDB(chunkOpts)

	manifestOpts := grocksdb.NewDefaultOptions()
	manifestOpts.SetBlockBasedTableFactory(bbto)
	manifestOpts.SetCompression(grocksdb.NoCompression)
	applyBlobDB(manifestOpts)

	return &OpenOptions{
		DBOpts:     dbOpts,
		ChunkCF:    chunkOpts,
		ManifestCF: manifestOpts,
		BBTOpts:    bbto,
	}
}
