package ingest_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/bundle"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/ingest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestBundleIngestDedupAcrossImages(t *testing.T) {
	salt, err := store.SaltForGeneration("G1")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	writer, err := bundle.NewWriter(&output, store.WriteAdmission{Generation: "G1", Salt: salt}, bundle.WriterOptions{Concurrency: 8})
	if err != nil {
		t.Fatal(err)
	}
	enc, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	chk, err := chunker.New(chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "4096"}})
	if err != nil {
		t.Fatal(err)
	}
	ing := ingest.NewIngester(func() ([32]byte, error) { return [32]byte{1}, nil }, nil, writer, chk, enc)

	chunkA := bytes.Repeat([]byte{0xAA}, 4096)
	chunkB := bytes.Repeat([]byte{0xBB}, 4096)
	first := append(append(append(append(append([]byte(nil), chunkA...), chunkB...), chunkA...), chunkB...), chunkA...)
	second := append(append(append([]byte(nil), chunkB...), chunkA...), chunkB...)
	for i, payload := range [][]byte{first, second} {
		res, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), ingest.IngestOption{})
		if err != nil {
			t.Fatalf("Ingest image %d: %v", i, err)
		}
		if i == 0 && (res.StoredChunks != 2 || res.DedupChunks != 3) {
			t.Fatalf("first image stored/dedup = %d/%d, want 2/3", res.StoredChunks, res.DedupChunks)
		}
		if i == 1 && (res.StoredChunks != 0 || res.DedupChunks != 3) {
			t.Fatalf("second image stored/dedup = %d/%d, want 0/3", res.StoredChunks, res.DedupChunks)
		}
	}
}
