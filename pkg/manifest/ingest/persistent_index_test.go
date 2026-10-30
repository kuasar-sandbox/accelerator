package ingest

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"go.etcd.io/bbolt"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestChunkMetaEncodeDecode(t *testing.T) {
	meta := chunkMeta{
		ck:     store.ContentKey{2},
		ctLen:  123,
		encLen: 122,
		format: crypto.ChunkFormatAESSnappy,
	}
	decoded, err := decodeChunkMeta(encodeChunkMeta(meta))
	if err != nil {
		t.Fatal(err)
	}
	if decoded != meta {
		t.Fatalf("decoded = %#v, want %#v", decoded, meta)
	}
	if decoded.key != ([32]byte{}) {
		t.Fatal("persistent metadata decoded a plaintext chunk key")
	}
	fp := crypto.DeriveKey([32]byte{1}, []byte("plaintext"))
	if chunkIndexKey(fp) == fp {
		t.Fatal("persistent index key is not one-way")
	}
	corrupt := encodeChunkMeta(meta)
	corrupt[20] ^= 0xff
	if _, err := decodeChunkMeta(corrupt); err == nil {
		t.Fatal("corrupt entry was accepted")
	}
}

func TestPersistentDedupPathIsolatesEndpoints(t *testing.T) {
	t.Setenv(dedupIndexEnv, "")
	a := persistentDedupPath("unix:///tmp/store-a")
	b := persistentDedupPath("unix:///tmp/store-b")
	if a == "" || b == "" {
		t.Skip("user cache directory unavailable")
	}
	if a == b {
		t.Fatalf("different endpoints share path %q", a)
	}
	t.Setenv(dedupIndexEnv, "off")
	if got := persistentDedupPath("unix:///tmp/store-a"); got != "" {
		t.Fatalf("disabled path = %q, want empty", got)
	}
	t.Setenv(dedupIndexEnv, "/tmp/explicit-index.db")
	if got := persistentDedupPath("unix:///tmp/store-a"); got != "/tmp/explicit-index.db" {
		t.Fatalf("override path = %q", got)
	}
	if got := persistentDedupPath(""); got != "" {
		t.Fatalf("empty endpoint path = %q, want empty", got)
	}
}

func TestPersistentIndexEntryLimit(t *testing.T) {
	t.Setenv(dedupIndexLimitEnv, "5")
	indexPath := filepath.Join(t.TempDir(), "dedup.db")

	entries1 := make(map[[32]byte]chunkMeta)
	for i := 0; i < 4; i++ {
		var fp [32]byte
		fp[0] = byte(i + 1)
		entries1[fp] = chunkMeta{
			ck:     store.ContentKey{byte(i + 1)},
			ctLen:  100,
			encLen: 99,
			format: crypto.ChunkFormatAESRaw,
		}
	}
	marker1 := [32]byte{0xaa}
	if err := persistChunkMetas(indexPath, entries1, marker1, false); err != nil {
		t.Fatal(err)
	}

	p1, err := openPersistentChunkIndexRead(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	for fp := range entries1 {
		if _, found, err := p1.lookup(fp); err != nil || !found {
			t.Fatalf("entries1 key %x missing from index", fp[:4])
		}
	}
	p1.Close()

	// Ingest 4 more different entries. Total would be 8 > 5 limit.
	entries2 := make(map[[32]byte]chunkMeta)
	for i := 4; i < 8; i++ {
		var fp [32]byte
		fp[0] = byte(i + 1)
		entries2[fp] = chunkMeta{
			ck:     store.ContentKey{byte(i + 1)},
			ctLen:  100,
			encLen: 99,
			format: crypto.ChunkFormatAESRaw,
		}
	}
	marker2 := [32]byte{0xbb}
	if err := persistChunkMetas(indexPath, entries2, marker2, false); err != nil {
		t.Fatal(err)
	}

	p2, err := openPersistentChunkIndexRead(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()

	// All 4 new entries must be present.
	for fp := range entries2 {
		if _, found, err := p2.lookup(fp); err != nil || !found {
			t.Fatalf("entries2 key %x missing from index after eviction", fp[:4])
		}
	}

	// Verify the database has exactly 5 entries.
	count := 0
	if err := p2.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(dedupIndexBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(_, _ []byte) error {
			count++
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Fatalf("persistent index count = %d, want capped at 5", count)
	}
}

func TestPersistentIndexCompaction(t *testing.T) {
	t.Setenv(dedupIndexLimitEnv, "100")
	// Set max bytes threshold to 1 byte so compaction triggers after write.
	t.Setenv(dedupIndexMaxBytesEnv, "1")
	indexPath := filepath.Join(t.TempDir(), "dedup.db")

	entries := make(map[[32]byte]chunkMeta)
	for i := 0; i < 20; i++ {
		var fp [32]byte
		binary.LittleEndian.PutUint64(fp[:8], uint64(i))
		entries[fp] = chunkMeta{
			ck:     store.ContentKey(fp),
			ctLen:  256,
			encLen: 255,
			format: crypto.ChunkFormatAESRaw,
		}
	}
	marker := [32]byte{0xcc}
	if err := persistChunkMetas(indexPath, entries, marker, false); err != nil {
		t.Fatal(err)
	}

	p, err := openPersistentChunkIndexRead(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	for fp := range entries {
		if _, found, err := p.lookup(fp); err != nil || !found {
			t.Fatalf("compacted entry %x missing", fp[:4])
		}
	}
	gotMarker, found, err := p.marker()
	if err != nil || !found || gotMarker != marker {
		t.Fatalf("compacted marker = %x (found=%v), want %x", gotMarker, found, marker)
	}
}
