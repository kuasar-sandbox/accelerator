//go:build !no_rocksdb

package rocks

import (
	"github.com/kuasar-sandbox/accelerator/internal/util"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	grocksdb "github.com/linxGnu/grocksdb"
)

// OpenOptions holds resolved RocksDB options ready for Open.
type OpenOptions struct {
	DBOpts     *grocksdb.Options
	ChunkCF    *grocksdb.Options
	ManifestCF *grocksdb.Options
	BlobCF     *grocksdb.Options
	BBTOpts    *grocksdb.BlockBasedTableOptions
}

// BuildOptions creates RocksDB options from a runtime.RocksConfig.
func BuildOptions(cfg runtime.RocksConfig) *OpenOptions {
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()

	// Block+blob cache size = disk_bytes * mem_ratio. One LRU is shared:
	// SST index/filter/data blocks and BlobDB payloads. Chunk values live
	// in .blob files, not SST data blocks — a block-only cache never
	// absorbs GetCF of a flushed chunk (restore SLO ≤5ms).
	diskBytes, _ := util.ParseSize(cfg.DiskBytes)
	if diskBytes == 0 {
		diskBytes = 1 << 40 // 1 TiB default
	}
	memRatio := cfg.MemRatio
	if memRatio <= 0 {
		memRatio = 0.01
	}
	cacheBytes := uint64(float64(diskBytes) * memRatio)
	if cacheBytes < 64<<20 {
		cacheBytes = 64 << 20 // min 64 MiB
	}
	cache := grocksdb.NewLRUCache(cacheBytes)
	bbto.SetBlockCache(cache)

	// Block size
	blockSize, _ := util.ParseSize(cfg.BlockSize)
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

	// Direct I/O: reads and flush/compaction must match. Buffered flush
	// + O_DIRECT GetCF on the same .blob file forces kernel writeback
	// before the read and shows up as ≥8ms GetCF tails.
	if runtime.BoolDefault(cfg.DirectReads, true) {
		dbOpts.SetUseDirectReads(true)
		dbOpts.SetUseDirectIOForFlushAndCompaction(true)
	}

	// Write buffer — default 256 MiB to reduce flush frequency under concurrent writes.
	writeBufferBytes, _ := util.ParseSize(cfg.WriteBufferBytes)
	if writeBufferBytes == 0 {
		writeBufferBytes = 256 << 20 // 256 MiB default (was 64 MiB Go/RocksDB default)
	}
	const maxWriteBuffers = 4

	// Background jobs
	maxJobs := cfg.MaxBackgroundJobs
	if maxJobs <= 0 {
		maxJobs = 8 // default 8 (was 4)
	}
	dbOpts.SetMaxBackgroundCompactions(maxJobs)
	dbOpts.SetMaxBackgroundFlushes(2)

	// CF options built from NewDefaultOptions() do not inherit DB-level
	// write-buffer settings. write_buffer_size is per-CF; leaving chunk
	// at RocksDB's 64 MiB flushed the memtable mid-restore (Write Buffer
	// Full) and raced GetCF with a 60 MiB blob-file write.
	applyCF := func(o *grocksdb.Options) {
		o.SetBlockBasedTableFactory(bbto)
		o.SetCompression(grocksdb.NoCompression)
		o.SetWriteBufferSize(writeBufferBytes)
		o.SetMaxWriteBufferNumber(maxWriteBuffers)
	}
	applyCF(dbOpts)

	// BlobDB on chunk, manifest, and blob CFs.
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
		o.SetMinBlobSize(4 << 10)    // 4 KiB inline threshold
		o.SetBlobFileSize(256 << 20) // 256 MiB per blob file
		o.EnableBlobGC(true)         // background reclamation of stale blobs
		o.SetBlobCache(cache)
		// Flush already has the blob in memory. Prepopulate so a
		// subsequent Direct-IO GetCF does not read it back from disk.
		o.SetPrepopulateBlobCache(grocksdb.PrepopulateBlobFlushOnly)
	}

	chunkOpts := grocksdb.NewDefaultOptions()
	applyCF(chunkOpts)
	applyBlobDB(chunkOpts)

	manifestOpts := grocksdb.NewDefaultOptions()
	applyCF(manifestOpts)
	applyBlobDB(manifestOpts)

	blobOpts := grocksdb.NewDefaultOptions()
	applyCF(blobOpts)
	applyBlobDB(blobOpts)

	return &OpenOptions{
		DBOpts:     dbOpts,
		ChunkCF:    chunkOpts,
		ManifestCF: manifestOpts,
		BlobCF:     blobOpts,
		BBTOpts:    bbto,
	}
}
