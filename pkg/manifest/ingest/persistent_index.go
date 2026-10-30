package ingest

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/bbolt"

	"github.com/kuasar-sandbox/accelerator/internal/util"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
)

const (
	dedupIndexBucket        = "chunk-dedup-v2"
	dedupIndexMetaBucket    = "chunk-dedup-meta-v2"
	dedupIndexLegacyBucket  = "chunk-dedup-v1"
	dedupIndexLegacyMeta    = "chunk-dedup-meta-v1"
	dedupIndexMarkerKey     = "manifest-marker"
	dedupIndexEntryCountKey = "entry-count"
	dedupIndexKeyDomain     = "accelerator-chunk-dedup-index-v2\x00"
	dedupIndexVersion       = byte(2)
	dedupIndexValueSize     = 1 + 32 + 8 + 8 + 1 + 4
	dedupIndexEnv           = "ACCELERATOR_CHUNK_DEDUP_INDEX"
	dedupIndexLimitEnv      = "ACCELERATOR_CHUNK_DEDUP_INDEX_LIMIT"
	dedupIndexMaxBytesEnv   = "ACCELERATOR_CHUNK_DEDUP_INDEX_MAX_BYTES"

	defaultPersistentIndexLimit    = 65536
	defaultPersistentIndexMaxBytes = 32 << 20 // 32 MiB
)

func chunkIndexKey(fp [32]byte) [32]byte {
	h := sha256.New()
	h.Write([]byte(dedupIndexKeyDomain))
	h.Write(fp[:])
	var key [32]byte
	copy(key[:], h.Sum(nil))
	return key
}

type persistentChunkIndex struct {
	db *bbolt.DB
}

func persistentDedupPathForWriter(sw StoreWriter) string {
	endpointer, ok := sw.(interface{ DedupIndexEndpoint() string })
	if !ok {
		return ""
	}
	if _, ok := sw.(DedupIndexValidator); !ok {
		return ""
	}
	return persistentDedupPath(endpointer.DedupIndexEndpoint())
}

func persistentDedupPath(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return ""
	}
	raw := strings.TrimSpace(os.Getenv(dedupIndexEnv))
	if raw != "" {
		if strings.EqualFold(raw, "off") {
			return ""
		}
		return raw
	}
	base, err := os.UserCacheDir()
	if err != nil || base == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(endpoint))
	return filepath.Join(base, "accelerator", "chunk-dedup", hex.EncodeToString(sum[:16])+".db")
}

func openPersistentChunkIndexRead(path string) (*persistentChunkIndex, error) {
	if path == "" {
		return nil, nil
	}
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{ReadOnly: true, Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	return &persistentChunkIndex{db: db}, nil
}

func (p *persistentChunkIndex) lookup(fp [32]byte) (chunkMeta, bool, error) {
	if p == nil || p.db == nil {
		return chunkMeta{}, false, nil
	}
	var (
		meta  chunkMeta
		found bool
	)
	err := p.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(dedupIndexBucket))
		if bucket == nil {
			return nil
		}
		indexKey := chunkIndexKey(fp)
		value := bucket.Get(indexKey[:])
		if value == nil {
			return nil
		}
		decoded, err := decodeChunkMeta(value)
		if err != nil {
			log.Printf("ingest: ignoring corrupt persistent dedup entry: %v", err)
			return nil
		}
		decoded.key = fp
		meta = decoded
		found = true
		return nil
	})
	return meta, found, err
}

func (p *persistentChunkIndex) marker() ([32]byte, bool, error) {
	var marker [32]byte
	var found bool
	if p == nil || p.db == nil {
		return marker, false, nil
	}
	err := p.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(dedupIndexMetaBucket))
		if bucket == nil {
			return nil
		}
		value := bucket.Get([]byte(dedupIndexMarkerKey))
		if len(value) != len(marker) {
			return nil
		}
		copy(marker[:], value)
		found = true
		return nil
	})
	return marker, found, err
}

func (p *persistentChunkIndex) Close() error {
	if p == nil || p.db == nil {
		return nil
	}
	err := p.db.Close()
	p.db = nil
	return err
}

func persistentIndexLimit() int {
	if raw := strings.TrimSpace(os.Getenv(dedupIndexLimitEnv)); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return defaultPersistentIndexLimit
}

func persistentIndexMaxBytes() int64 {
	if raw := strings.TrimSpace(os.Getenv(dedupIndexMaxBytesEnv)); raw != "" {
		if n, err := util.ParseSize(raw); err == nil && n > 0 {
			return int64(n)
		}
	}
	return defaultPersistentIndexMaxBytes
}

func compactPersistentIndex(path string) error {
	tmpPath := path + ".compact.tmp"
	_ = os.Remove(tmpPath)
	src, err := bbolt.Open(path, 0o600, &bbolt.Options{ReadOnly: true, Timeout: 10 * time.Second})
	if err != nil {
		return err
	}
	dst, err := bbolt.Open(tmpPath, 0o600, &bbolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		_ = src.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	compactErr := bbolt.Compact(dst, src, 0)
	srcCloseErr := src.Close()
	dstCloseErr := dst.Close()
	if compactErr != nil {
		_ = os.Remove(tmpPath)
		return compactErr
	}
	if srcCloseErr != nil || dstCloseErr != nil {
		_ = os.Remove(tmpPath)
		return errors.Join(srcCloseErr, dstCloseErr)
	}
	return os.Rename(tmpPath, path)
}

func persistChunkMetas(path string, entries map[[32]byte]chunkMeta, marker [32]byte, reset bool) error {
	if path == "" {
		return nil
	}
	maxBytes := persistentIndexMaxBytes()
	if reset {
		if fi, err := os.Stat(path); err == nil && fi.Size() > maxBytes {
			_ = os.Remove(path)
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create dedup index directory: %w", err)
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: 30 * time.Second})
	if err != nil {
		return fmt.Errorf("open dedup index: %w", err)
	}

	limit := persistentIndexLimit()
	updateErr := db.Update(func(tx *bbolt.Tx) error {
		if reset {
			for _, name := range []string{dedupIndexBucket, dedupIndexMetaBucket} {
				if err := tx.DeleteBucket([]byte(name)); err != nil && !errors.Is(err, bbolt.ErrBucketNotFound) {
					return err
				}
			}
		}
		for _, name := range []string{dedupIndexLegacyBucket, dedupIndexLegacyMeta} {
			if err := tx.DeleteBucket([]byte(name)); err != nil && !errors.Is(err, bbolt.ErrBucketNotFound) {
				return err
			}
		}
		bucket, err := tx.CreateBucketIfNotExists([]byte(dedupIndexBucket))
		if err != nil {
			return err
		}
		metaBucket, err := tx.CreateBucketIfNotExists([]byte(dedupIndexMetaBucket))
		if err != nil {
			return err
		}

		currentCount := 0
		if countRaw := metaBucket.Get([]byte(dedupIndexEntryCountKey)); len(countRaw) == 8 {
			currentCount = int(binary.LittleEndian.Uint64(countRaw))
		} else if !reset {
			currentCount = bucket.Stats().KeyN
		}

		// Count how many keys are newly introduced by entries.
		newKeys := 0
		for fp := range entries {
			indexKey := chunkIndexKey(fp)
			if bucket.Get(indexKey[:]) == nil {
				newKeys++
			}
		}

		// If currentCount + newKeys exceeds limit, evict excess older entries before inserting.
		if excess := (currentCount + newKeys) - limit; excess > 0 {
			c := bucket.Cursor()
			for k, _ := c.First(); k != nil && excess > 0; k, _ = c.Next() {
				if err := c.Delete(); err != nil {
					return err
				}
				excess--
				currentCount--
			}
		}

		for fp, meta := range entries {
			indexKey := chunkIndexKey(fp)
			if bucket.Get(indexKey[:]) == nil {
				currentCount++
			}
			if err := bucket.Put(indexKey[:], encodeChunkMeta(meta)); err != nil {
				return err
			}
		}

		if currentCount > limit {
			excess := currentCount - limit
			c := bucket.Cursor()
			for k, _ := c.First(); k != nil && excess > 0; k, _ = c.Next() {
				if err := c.Delete(); err != nil {
					return err
				}
				excess--
				currentCount--
			}
		}

		var countBuf [8]byte
		binary.LittleEndian.PutUint64(countBuf[:], uint64(currentCount))
		if err := metaBucket.Put([]byte(dedupIndexEntryCountKey), countBuf[:]); err != nil {
			return err
		}
		return metaBucket.Put([]byte(dedupIndexMarkerKey), marker[:])
	})

	closeErr := db.Close()
	if updateErr != nil {
		return updateErr
	}
	if closeErr != nil {
		return closeErr
	}

	if fi, err := os.Stat(path); err == nil && fi.Size() > maxBytes {
		if err := compactPersistentIndex(path); err != nil {
			log.Printf("ingest: compact persistent dedup index: %v", err)
		}
	}
	return nil
}

func encodeChunkMeta(meta chunkMeta) []byte {
	value := make([]byte, dedupIndexValueSize)
	value[0] = dedupIndexVersion
	copy(value[1:33], meta.ck[:])
	binary.LittleEndian.PutUint64(value[33:41], meta.ctLen)
	binary.LittleEndian.PutUint64(value[41:49], meta.encLen)
	value[49] = meta.format
	binary.LittleEndian.PutUint32(value[50:54], crc32.ChecksumIEEE(value[:50]))
	return value
}

func decodeChunkMeta(value []byte) (chunkMeta, error) {
	if len(value) != dedupIndexValueSize {
		return chunkMeta{}, fmt.Errorf("value size %d, want %d", len(value), dedupIndexValueSize)
	}
	if value[0] != dedupIndexVersion {
		return chunkMeta{}, fmt.Errorf("unsupported version %d", value[0])
	}
	if got, want := binary.LittleEndian.Uint32(value[50:54]), crc32.ChecksumIEEE(value[:50]); got != want {
		return chunkMeta{}, fmt.Errorf("checksum %08x, want %08x", got, want)
	}
	var meta chunkMeta
	copy(meta.ck[:], value[1:33])
	meta.ctLen = binary.LittleEndian.Uint64(value[33:41])
	meta.encLen = binary.LittleEndian.Uint64(value[41:49])
	meta.format = value[49]
	if meta.ctLen == 0 || meta.encLen+1 != meta.ctLen {
		return chunkMeta{}, fmt.Errorf("invalid physical/encoded lengths %d/%d", meta.ctLen, meta.encLen)
	}
	if meta.format != crypto.ChunkFormatAESRaw && meta.format != crypto.ChunkFormatAESSnappy {
		return chunkMeta{}, fmt.Errorf("unknown format 0x%02x", meta.format)
	}
	return meta, nil
}
