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
	"strings"
	"time"

	"go.etcd.io/bbolt"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
)

const (
	dedupIndexBucket     = "chunk-dedup-v1"
	dedupIndexMetaBucket = "chunk-dedup-meta-v1"
	dedupIndexMarkerKey  = "manifest-marker"
	dedupIndexVersion    = byte(1)
	dedupIndexValueSize  = 1 + 32 + 32 + 8 + 8 + 1 + 4
	dedupIndexEnv        = "ACCELERATOR_CHUNK_DEDUP_INDEX"
)

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
		value := bucket.Get(fp[:])
		if value == nil {
			return nil
		}
		decoded, err := decodeChunkMeta(value)
		if err != nil {
			log.Printf("ingest: ignoring corrupt persistent dedup entry: %v", err)
			return nil
		}
		if decoded.key != fp {
			log.Printf("ingest: ignoring persistent dedup entry with mismatched key")
			return nil
		}
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

func persistChunkMetas(path string, entries map[[32]byte]chunkMeta, marker [32]byte, reset bool) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create dedup index directory: %w", err)
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: 30 * time.Second})
	if err != nil {
		return fmt.Errorf("open dedup index: %w", err)
	}
	defer db.Close()
	return db.Update(func(tx *bbolt.Tx) error {
		if reset {
			for _, name := range []string{dedupIndexBucket, dedupIndexMetaBucket} {
				if err := tx.DeleteBucket([]byte(name)); err != nil && !errors.Is(err, bbolt.ErrBucketNotFound) {
					return err
				}
			}
		}
		bucket, err := tx.CreateBucketIfNotExists([]byte(dedupIndexBucket))
		if err != nil {
			return err
		}
		for fp, meta := range entries {
			if err := bucket.Put(fp[:], encodeChunkMeta(meta)); err != nil {
				return err
			}
		}
		metaBucket, err := tx.CreateBucketIfNotExists([]byte(dedupIndexMetaBucket))
		if err != nil {
			return err
		}
		return metaBucket.Put([]byte(dedupIndexMarkerKey), marker[:])
	})
}

func encodeChunkMeta(meta chunkMeta) []byte {
	value := make([]byte, dedupIndexValueSize)
	value[0] = dedupIndexVersion
	copy(value[1:33], meta.key[:])
	copy(value[33:65], meta.ck[:])
	binary.LittleEndian.PutUint64(value[65:73], meta.ctLen)
	binary.LittleEndian.PutUint64(value[73:81], meta.encLen)
	value[81] = meta.format
	binary.LittleEndian.PutUint32(value[82:86], crc32.ChecksumIEEE(value[:82]))
	return value
}

func decodeChunkMeta(value []byte) (chunkMeta, error) {
	if len(value) != dedupIndexValueSize {
		return chunkMeta{}, fmt.Errorf("value size %d, want %d", len(value), dedupIndexValueSize)
	}
	if value[0] != dedupIndexVersion {
		return chunkMeta{}, fmt.Errorf("unsupported version %d", value[0])
	}
	if got, want := binary.LittleEndian.Uint32(value[82:86]), crc32.ChecksumIEEE(value[:82]); got != want {
		return chunkMeta{}, fmt.Errorf("checksum %08x, want %08x", got, want)
	}
	var meta chunkMeta
	copy(meta.key[:], value[1:33])
	copy(meta.ck[:], value[33:65])
	meta.ctLen = binary.LittleEndian.Uint64(value[65:73])
	meta.encLen = binary.LittleEndian.Uint64(value[73:81])
	meta.format = value[81]
	if meta.ctLen == 0 || meta.encLen+1 != meta.ctLen {
		return chunkMeta{}, fmt.Errorf("invalid physical/encoded lengths %d/%d", meta.ctLen, meta.encLen)
	}
	if meta.format != crypto.ChunkFormatAESRaw && meta.format != crypto.ChunkFormatAESSnappy {
		return chunkMeta{}, fmt.Errorf("unknown format 0x%02x", meta.format)
	}
	return meta, nil
}
