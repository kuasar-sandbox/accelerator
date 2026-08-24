package fetch

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type verifyBenchmarkGetter struct {
	manifestKey store.ContentKey
	manifest    []byte
	chunkKey    store.ContentKey
	chunk       []byte
}

func (g *verifyBenchmarkGetter) Get(_ context.Context, partition store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	switch {
	case partition == store.PartitionManifest && key == g.manifestKey:
		return cache.CacheHit, cache.NewMemBlob(g.manifest), nil
	case partition == store.PartitionChunk && key == g.chunkKey:
		return cache.CacheHit, cache.NewMemBlob(g.chunk), nil
	default:
		return cache.CacheMiss, nil, nil
	}
}

// BenchmarkVerifyContent isolates the ordinary-read SHA-256 policy cost for a
// physical Manifest open and a cold 1 MiB Chunk read.
func BenchmarkVerifyContent(b *testing.B) {
	enc, dec, err := manifestcrypto.New(manifestcrypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		b.Fatal(err)
	}
	plain := fetchNoise(1 << 20)
	physical, hash, decryptKey, err := enc.EncryptChunk(context.Background(), [32]byte{0x72}, plain)
	if err != nil {
		b.Fatal(err)
	}
	entry := codec.ChunkEntry{Size: uint32(len(plain)), CiphertextHash: hash}
	manifest := &codec.Manifest{
		Version:      codec.Version1,
		ChunkMode:    codec.ChunkModeFixed,
		ImageSize:    uint64(len(plain)),
		MinChunkSize: uint32(len(plain)),
		MaxChunkSize: uint32(len(plain)),
		Entries:      []codec.ChunkEntry{entry},
		Keys:         [][32]byte{decryptKey},
	}
	var customerKey [32]byte
	customerKey[0] = 0x44
	sealed, err := enc.SealKeyTable(customerKey, decryptKey[:], codec.BuildAAD(manifest))
	if err != nil {
		b.Fatal(err)
	}
	manifestBytes, err := codec.Marshal(manifest, sealed)
	if err != nil {
		b.Fatal(err)
	}
	getter := &verifyBenchmarkGetter{
		manifestKey: store.ContentKey(sha256.Sum256(manifestBytes)),
		manifest:    manifestBytes,
		chunkKey:    store.ContentKey(hash),
		chunk:       physical,
	}

	for _, setting := range []struct {
		name   string
		verify bool
	}{
		{name: "true", verify: true},
		{name: "false", verify: false},
	} {
		b.Run("Manifest/"+setting.name, func(b *testing.B) {
			fetcher := NewFetcherWithOptions(customerKey, getter, dec, Options{VerifyContent: setting.verify})
			b.ReportAllocs()
			b.SetBytes(int64(len(manifestBytes)))
			for range b.N {
				stream, err := fetcher.OpenManifest(context.Background(), getter.manifestKey)
				if err != nil {
					b.Fatal(err)
				}
				if err := stream.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("Chunk1MiB/"+setting.name, func(b *testing.B) {
			stream := newManifestStreamWithOptions(manifest, [][32]byte{decryptKey}, getter, getter, dec, Options{VerifyContent: setting.verify})
			defer stream.Close()
			dst := make([]byte, len(plain))
			b.ReportAllocs()
			b.SetBytes(int64(len(dst)))
			for range b.N {
				if n, err := stream.readChunkDirect(context.Background(), dst, 0, entry, 0); err != nil || n != len(dst) {
					b.Fatal(n, err)
				}
			}
		})
	}
}
