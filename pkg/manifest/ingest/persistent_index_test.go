package ingest

import (
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestChunkMetaEncodeDecode(t *testing.T) {
	meta := chunkMeta{
		key:    crypto.DeriveKey([32]byte{1}, []byte("plaintext")),
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
	corrupt := encodeChunkMeta(meta)
	corrupt[40] ^= 0xff
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
