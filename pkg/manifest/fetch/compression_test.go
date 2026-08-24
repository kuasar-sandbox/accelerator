package fetch

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestManifestWholeChunkDecodeToCallerKeepsBlobImmutable(t *testing.T) {
	t.Parallel()
	enc, dec := realManifestCrypto(t)
	for _, tc := range []struct {
		name       string
		plain      []byte
		wantFormat byte
	}{
		{name: "raw", plain: fetchNoise(1 << 20), wantFormat: manifestcrypto.ChunkFormatAESRaw},
		{name: "snappy", plain: bytes.Repeat([]byte("snapshot-page\x00"), 64<<10), wantFormat: manifestcrypto.ChunkFormatAESSnappy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			object, hash, key, err := enc.EncryptChunk(context.Background(), [32]byte{0x72}, tc.plain)
			if err != nil {
				t.Fatal(err)
			}
			if object[0] != tc.wantFormat {
				t.Fatalf("format = %#x, want %#x", object[0], tc.wantFormat)
			}
			original := append([]byte(nil), object...)
			stream := newTestManifestStream(&codec.Manifest{
				Version:   codec.Version1,
				ImageSize: uint64(len(tc.plain)),
				Entries: []codec.ChunkEntry{{
					Offset:         0,
					Size:           uint32(len(tc.plain)),
					CiphertextHash: hash,
				}},
			}, [][32]byte{key}, &chunkMapGetter{chunks: map[store.ContentKey][]byte{hash: object}}, dec)
			defer stream.Close()

			dst := make([]byte, len(tc.plain))
			if n, err := stream.ReadAt(context.Background(), dst, 0); err != nil || n != len(dst) {
				t.Fatalf("ReadAt = (%d, %v)", n, err)
			}
			if !bytes.Equal(dst, tc.plain) {
				t.Fatal("plaintext mismatch")
			}
			if !bytes.Equal(object, original) {
				t.Fatal("whole-chunk read mutated cache.Blob bytes")
			}
		})
	}
}

func TestManifestSnappyPartialMissUsesOnePlaintextAndStaysCached(t *testing.T) {
	t.Parallel()
	enc, dec := realManifestCrypto(t)
	plain := bytes.Repeat([]byte("active-memory-page\x00"), 48<<10)
	object, hash, key, err := enc.EncryptChunk(context.Background(), [32]byte{0x33}, plain)
	if err != nil {
		t.Fatal(err)
	}
	if object[0] != manifestcrypto.ChunkFormatAESSnappy {
		t.Fatalf("fixture format = %#x, want Snappy", object[0])
	}
	original := append([]byte(nil), object...)
	getter := &chunkMapGetter{chunks: map[store.ContentKey][]byte{hash: object}}
	stream := newTestManifestStream(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: uint64(len(plain)),
		Entries: []codec.ChunkEntry{{
			Size:           uint32(len(plain)),
			CiphertextHash: hash,
		}},
	}, [][32]byte{key}, getter, dec)
	defer stream.Close()

	for _, offset := range []int{0, 4096, len(plain) - 4096} {
		dst := make([]byte, 4096)
		if n, err := stream.ReadAt(context.Background(), dst, uint64(offset)); err != nil || n != len(dst) {
			t.Fatalf("ReadAt(%d) = (%d, %v)", offset, n, err)
		}
		if !bytes.Equal(dst, plain[offset:offset+len(dst)]) {
			t.Fatalf("ReadAt(%d) mismatch", offset)
		}
	}
	if got := getter.calls.Load(); got != 1 {
		t.Fatalf("physical Gets = %d, want one partial-miss fill", got)
	}
	if !bytes.Equal(object, original) {
		t.Fatal("partial Snappy read mutated cache.Blob bytes")
	}
}

func TestManifestSnappyConcurrentPartialMissDecodesOnce(t *testing.T) {
	t.Parallel()
	enc, _ := realManifestCrypto(t)
	plain := bytes.Repeat([]byte("coalesced-snappy-page\x00"), 48<<10)
	object, hash, key, err := enc.EncryptChunk(context.Background(), [32]byte{0x39}, plain)
	if err != nil {
		t.Fatal(err)
	}
	if object[0] != manifestcrypto.ChunkFormatAESSnappy {
		t.Fatalf("fixture format = %#x, want Snappy", object[0])
	}
	counting := &countingChunkEncryptor{inner: &manifestcrypto.AESChunkEncryptor{}}
	getter := &chunkMapGetter{chunks: map[store.ContentKey][]byte{hash: object}}
	stream := newTestManifestStream(&codec.Manifest{
		Version: codec.Version1, ImageSize: uint64(len(plain)), Entries: []codec.ChunkEntry{{Size: uint32(len(plain)), CiphertextHash: hash}},
	}, [][32]byte{key}, getter, counting)
	defer stream.Close()

	const readers = 32
	start := make(chan struct{})
	errCh := make(chan error, readers)
	var ready, done sync.WaitGroup
	ready.Add(readers)
	done.Add(readers)
	for i := range readers {
		go func(i int) {
			defer done.Done()
			ready.Done()
			<-start
			offset := (i * 4096) % (len(plain) - 4096)
			dst := make([]byte, 4096)
			n, err := stream.ReadAt(context.Background(), dst, uint64(offset))
			if err != nil || n != len(dst) {
				errCh <- fmt.Errorf("ReadAt(%d) = (%d, %v)", offset, n, err)
				return
			}
			if !bytes.Equal(dst, plain[offset:offset+len(dst)]) {
				errCh <- fmt.Errorf("ReadAt(%d) mismatch", offset)
			}
		}(i)
	}
	ready.Wait()
	close(start)
	done.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if got := getter.calls.Load(); got != 1 {
		t.Fatalf("physical Gets = %d, want 1", got)
	}
	if got := counting.decrypts.Load(); got != 1 {
		t.Fatalf("Snappy decodes = %d, want 1", got)
	}
}

func TestManifestSnappyRangeDecodeMatchesPlaintext(t *testing.T) {
	t.Parallel()
	enc, dec := realManifestCrypto(t)
	plain := bytes.Repeat([]byte("oversized-block-path\x00"), 32<<10)
	object, hash, key, err := enc.EncryptChunk(context.Background(), [32]byte{0x5a}, plain)
	if err != nil {
		t.Fatal(err)
	}
	entry := codec.ChunkEntry{Size: uint32(len(plain)), CiphertextHash: hash}
	stream := newTestManifestStream(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: uint64(len(plain)),
		Entries:   []codec.ChunkEntry{entry},
	}, [][32]byte{key}, &chunkMapGetter{chunks: map[store.ContentKey][]byte{hash: object}}, dec).(*manifestStream)
	defer stream.Close()

	const offset = 12345
	dst := make([]byte, 4096)
	n, err := stream.readChunkDirect(context.Background(), dst, 0, entry, uint64(offset))
	if err != nil || n != len(dst) {
		t.Fatalf("readChunkDirect = (%d, %v)", n, err)
	}
	if !bytes.Equal(dst, plain[offset:offset+len(dst)]) {
		t.Fatal("Snappy range decode mismatch")
	}
}

func TestManifestOversizedSnappyPartialReadUsesBoundedDirectRangePath(t *testing.T) {
	enc, dec := realManifestCrypto(t)
	size := int(manifestChunkCacheMaxBytes) + 1
	pattern := []byte("oversized-snapshot-block\x00")
	plain := bytes.Repeat(pattern, size/len(pattern)+1)[:size]
	object, hash, key, err := enc.EncryptChunk(context.Background(), [32]byte{0x6a}, plain)
	if err != nil {
		t.Fatal(err)
	}
	if object[0] != manifestcrypto.ChunkFormatAESSnappy {
		t.Fatalf("fixture format = %#x, want Snappy", object[0])
	}
	getter := &chunkMapGetter{chunks: map[store.ContentKey][]byte{hash: object}}
	stream := newTestManifestStream(&codec.Manifest{
		Version:   codec.Version1,
		ImageSize: uint64(len(plain)),
		Entries:   []codec.ChunkEntry{{Size: uint32(len(plain)), CiphertextHash: hash}},
	}, [][32]byte{key}, getter, dec).(*manifestStream)
	defer stream.Close()

	const offset = 17
	dst := make([]byte, 4<<10)
	if n, err := stream.ReadAt(context.Background(), dst, offset); err != nil || n != len(dst) {
		t.Fatalf("ReadAt = (%d, %v)", n, err)
	}
	if !bytes.Equal(dst, plain[offset:offset+len(dst)]) {
		t.Fatal("oversized Snappy partial plaintext mismatch")
	}
	if got := getter.calls.Load(); got != 1 {
		t.Fatalf("physical Gets = %d, want 1", got)
	}
	cacheKey := decryptedChunkKey{ciphertextHash: hash, decryptKey: key, plaintextSize: uint32(len(plain))}
	if lease := stream.chunkCache.acquire(cacheKey); lease != nil {
		lease.release()
		t.Fatal("oversized Snappy plaintext was admitted to the bounded chunk cache")
	}
}

func realManifestCrypto(t *testing.T) (manifestcrypto.Encryptor, manifestcrypto.Decryptor) {
	t.Helper()
	enc, dec, err := manifestcrypto.New(manifestcrypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	return enc, dec
}

func fetchNoise(size int) []byte {
	out := make([]byte, size)
	var x uint64 = 0xd1b54a32d192ed03
	for i := range out {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		out[i] = byte(x)
	}
	return out
}
