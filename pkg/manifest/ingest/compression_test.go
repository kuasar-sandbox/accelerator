package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestIngestCompressionStatsSeparateRawSnappyAndStoredBytes(t *testing.T) {
	const chunkSize = 64 << 10
	input := append(ingestNoise(chunkSize), bytes.Repeat([]byte{0x41}, chunkSize)...)
	rec := &recordingStore{}
	ing := NewIngester(testKeyFn, nil, rec, fixedChunker(t, chunkSize), testEncryptor())

	result, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(input), uint64(len(input))), IngestOption{})
	if err != nil {
		t.Fatal(err)
	}
	if result.RawChunks != 1 || result.CompressedChunks != 1 {
		t.Fatalf("raw/compressed chunks = %d/%d, want 1/1", result.RawChunks, result.CompressedChunks)
	}
	if result.LogicalChunkBytes != uint64(len(input)) {
		t.Fatalf("logical chunk bytes = %d, want %d", result.LogicalChunkBytes, len(input))
	}
	if result.EncodedChunkBytes >= result.LogicalChunkBytes {
		t.Fatalf("encoded bytes = %d, logical = %d", result.EncodedChunkBytes, result.LogicalChunkBytes)
	}
	if result.CompressionSavedBytes != result.LogicalChunkBytes-result.EncodedChunkBytes {
		t.Fatalf("saved bytes = %d, want %d", result.CompressionSavedBytes, result.LogicalChunkBytes-result.EncodedChunkBytes)
	}
	if result.StoredBytes != uint64(rec.chunkBytes+len(rec.manifest)) {
		t.Fatalf("stored bytes = %d, physical new bytes = %d", result.StoredBytes, rec.chunkBytes+len(rec.manifest))
	}
	if result.ManifestStoredBytes != uint64(len(rec.manifest)) || result.ManifestLogicalBytes == 0 {
		t.Fatalf("manifest physical/logical bytes = %d/%d", result.ManifestStoredBytes, result.ManifestLogicalBytes)
	}
}

func TestIngestUsesEncryptChunkReturnedPhysicalHash(t *testing.T) {
	const chunkSize = 64 << 10
	rec := &recordingStore{}
	wantHash := [32]byte{0x72, 0xaa}
	ing := NewIngester(testKeyFn, nil, rec, fixedChunker(t, chunkSize), returnedHashEncryptor{hash: wantHash})
	input := bytes.Repeat([]byte{0x7f}, chunkSize)

	result, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(input), uint64(len(input))), IngestOption{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.chunkPuts) != 1 || rec.chunkPuts[0] != wantHash {
		t.Fatalf("chunk Put keys = %x, want returned hash %x", rec.chunkPuts, wantHash)
	}
	physical := append([]byte{crypto.ChunkFormatAESRaw}, input...)
	if sha256.Sum256(physical) == wantHash {
		t.Fatal("test fixture accidentally made returned hash equal recomputed hash")
	}
	if result.RawChunks != 1 || result.EncodedChunkBytes != chunkSize {
		t.Fatalf("unexpected format stats: %+v", result)
	}
}

func TestIngestRejectsInvalidEncryptedChunkObjectContract(t *testing.T) {
	const chunkSize = 64 << 10
	for _, tc := range []struct {
		name   string
		object []byte
	}{
		{name: "empty", object: nil},
		{name: "unknown format", object: []byte{0xff}},
		{name: "raw wrong size", object: []byte{crypto.ChunkFormatAESRaw, 1}},
		{name: "snappy insufficient benefit", object: append([]byte{crypto.ChunkFormatAESSnappy}, make([]byte, 60<<10)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ing := NewIngester(testKeyFn, nil, &recordingStore{}, fixedChunker(t, chunkSize), invalidObjectEncryptor{object: tc.object})
			input := bytes.Repeat([]byte{0x7f}, chunkSize)
			if _, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(input), uint64(len(input))), IngestOption{}); err == nil {
				t.Fatal("invalid encrypted object was accepted")
			}
		})
	}
}

type returnedHashEncryptor struct {
	hash [32]byte
}

func (e returnedHashEncryptor) EncryptChunk(_ context.Context, _ [32]byte, plaintext []byte) ([]byte, [32]byte, [32]byte, error) {
	object := make([]byte, 1+len(plaintext))
	object[0] = crypto.ChunkFormatAESRaw
	copy(object[1:], plaintext)
	return object, e.hash, [32]byte{0x19}, nil
}

func (returnedHashEncryptor) SealKeyTable(_ [32]byte, keys, _ []byte) ([]byte, error) {
	return append([]byte(nil), keys...), nil
}

type invalidObjectEncryptor struct {
	object []byte
}

func (e invalidObjectEncryptor) EncryptChunk(context.Context, [32]byte, []byte) ([]byte, [32]byte, [32]byte, error) {
	return append([]byte(nil), e.object...), [32]byte{}, [32]byte{}, nil
}

func (invalidObjectEncryptor) SealKeyTable(_ [32]byte, keys, _ []byte) ([]byte, error) {
	return append([]byte(nil), keys...), nil
}

func ingestNoise(size int) []byte {
	out := make([]byte, size)
	var x uint64 = 0x243f6a8885a308d3
	for i := range out {
		x ^= x << 7
		x ^= x >> 9
		x ^= x << 8
		out[i] = byte(x)
	}
	return out
}
