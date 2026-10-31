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

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/accelerator/internal/util"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
)

const (
	dedupIndexMagic       = uint32(0x4b444544) // "KDED"
	dedupIndexVersion16   = uint16(2)
	dedupIndexHeaderSize  = 48
	dedupIndexRecordSize  = 32 + dedupIndexValueSize // 32 + 54 = 86
	dedupIndexKeyDomain   = "accelerator-chunk-dedup-index-v2\x00"
	dedupIndexVersion     = byte(2)
	dedupIndexValueSize   = 1 + 32 + 8 + 8 + 1 + 4 // 54
	dedupIndexEnv         = "ACCELERATOR_CHUNK_DEDUP_INDEX"
	dedupIndexLimitEnv    = "ACCELERATOR_CHUNK_DEDUP_INDEX_LIMIT"
	dedupIndexMaxBytesEnv = "ACCELERATOR_CHUNK_DEDUP_INDEX_MAX_BYTES"

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
	markerVal [32]byte
	entries   map[[32]byte]chunkMeta
	order     [][32]byte
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
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return parsePersistentChunkIndex(data)
}

func parsePersistentChunkIndex(data []byte) (*persistentChunkIndex, error) {
	if len(data) < dedupIndexHeaderSize {
		return nil, nil
	}
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != dedupIndexMagic {
		return nil, nil
	}
	ver := binary.LittleEndian.Uint16(data[4:6])
	if ver != dedupIndexVersion16 {
		return nil, nil
	}
	expectedCRC := binary.LittleEndian.Uint32(data[44:48])
	if crc32.ChecksumIEEE(data[:44]) != expectedCRC {
		return nil, nil
	}

	var marker [32]byte
	copy(marker[:], data[8:40])
	entryCount := int(binary.LittleEndian.Uint32(data[40:44]))

	entries := make(map[[32]byte]chunkMeta, entryCount)
	order := make([][32]byte, 0, entryCount)

	offset := dedupIndexHeaderSize
	for i := 0; i < entryCount && offset+dedupIndexRecordSize <= len(data); i++ {
		var key [32]byte
		copy(key[:], data[offset:offset+32])
		valBytes := data[offset+32 : offset+dedupIndexRecordSize]
		decoded, err := decodeChunkMeta(valBytes)
		if err == nil {
			entries[key] = decoded
			order = append(order, key)
		} else {
			log.Printf("ingest: ignoring corrupt persistent dedup entry: %v", err)
		}
		offset += dedupIndexRecordSize
	}

	return &persistentChunkIndex{
		markerVal: marker,
		entries:   entries,
		order:     order,
	}, nil
}

func (p *persistentChunkIndex) lookup(fp [32]byte) (chunkMeta, bool, error) {
	if p == nil || p.entries == nil {
		return chunkMeta{}, false, nil
	}
	indexKey := chunkIndexKey(fp)
	meta, found := p.entries[indexKey]
	if !found {
		return chunkMeta{}, false, nil
	}
	meta.key = fp
	return meta, true, nil
}

func (p *persistentChunkIndex) marker() ([32]byte, bool, error) {
	if p == nil {
		return [32]byte{}, false, nil
	}
	return p.markerVal, true, nil
}

func (p *persistentChunkIndex) entryCount() int {
	if p == nil {
		return 0
	}
	return len(p.entries)
}

func (p *persistentChunkIndex) Close() error {
	if p != nil {
		p.entries = nil
		p.order = nil
	}
	return nil
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

func writePersistentIndex(path string, merged map[[32]byte]chunkMeta, order [][32]byte, marker [32]byte) error {
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create temp index: %w", err)
	}

	var hdr [dedupIndexHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], dedupIndexMagic)
	binary.LittleEndian.PutUint16(hdr[4:6], dedupIndexVersion16)
	binary.LittleEndian.PutUint16(hdr[6:8], 0) // reserved
	copy(hdr[8:40], marker[:])
	binary.LittleEndian.PutUint32(hdr[40:44], uint32(len(merged)))
	binary.LittleEndian.PutUint32(hdr[44:48], crc32.ChecksumIEEE(hdr[:44]))

	if _, err := f.Write(hdr[:]); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	var rec [dedupIndexRecordSize]byte
	for _, k := range order {
		meta, ok := merged[k]
		if !ok {
			continue
		}
		copy(rec[0:32], k[:])
		metaBytes := encodeChunkMeta(meta)
		copy(rec[32:dedupIndexRecordSize], metaBytes)
		if _, err := f.Write(rec[:]); err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return err
		}
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	return os.Rename(tmpPath, path)
}

func compactPersistentIndex(path string) error {
	lockPath := path + ".lock"
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open dedup lock: %w", err)
	}
	defer lockFile.Close()
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		return fmt.Errorf("lock dedup index: %w", err)
	}
	defer func() { _ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN) }()

	existing, err := openPersistentChunkIndexRead(path)
	if err != nil || existing == nil {
		return err
	}
	defer existing.Close()
	return writePersistentIndex(path, existing.entries, existing.order, existing.markerVal)
}

func persistChunkMetas(path string, entries map[[32]byte]chunkMeta, marker [32]byte, reset bool) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create dedup index directory: %w", err)
	}

	lockPath := path + ".lock"
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open dedup lock: %w", err)
	}
	defer lockFile.Close()
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		return fmt.Errorf("lock dedup index: %w", err)
	}
	defer func() { _ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN) }()

	merged := make(map[[32]byte]chunkMeta)
	order := make([][32]byte, 0)

	if !reset {
		if existing, _ := openPersistentChunkIndexRead(path); existing != nil {
			for _, k := range existing.order {
				if meta, ok := existing.entries[k]; ok {
					merged[k] = meta
					order = append(order, k)
				}
			}
			_ = existing.Close()
		}
	}

	for fp, meta := range entries {
		k := chunkIndexKey(fp)
		if _, exists := merged[k]; !exists {
			order = append(order, k)
		}
		merged[k] = meta
	}

	limit := persistentIndexLimit()
	if len(merged) > limit {
		excess := len(merged) - limit
		newOrder := make([][32]byte, 0, limit)
		for _, k := range order {
			if excess > 0 {
				delete(merged, k)
				excess--
			} else {
				newOrder = append(newOrder, k)
			}
		}
		order = newOrder
	}

	if err := writePersistentIndex(path, merged, order, marker); err != nil {
		return err
	}

	maxBytes := persistentIndexMaxBytes()
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
