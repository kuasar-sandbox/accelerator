package crypto

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"
)

func TestChunkCodecRawAndS2RoundTrip(t *testing.T) {
	t.Parallel()
	var salt [32]byte
	copy(salt[:], "issue-72-canonical-salt")

	tests := []struct {
		name   string
		plain  []byte
		format byte
	}{
		{name: "raw", plain: deterministicNoise(1 << 20), format: ChunkFormatAESRaw},
		{name: "s2", plain: bytes.Repeat([]byte("compressible snapshot page\x00"), 1<<15), format: ChunkFormatAESS2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			codec := &AESChunkEncryptor{}
			object, hash, key, err := codec.EncryptChunk(context.Background(), salt, tc.plain)
			if err != nil {
				t.Fatal(err)
			}
			if len(object) == 0 || object[0] != tc.format {
				t.Fatalf("format = %#x, want %#x", object[0], tc.format)
			}
			if hash != sha256.Sum256(object) {
				t.Fatal("returned ContentKey does not hash the physical object")
			}
			if key != DeriveKey(salt, tc.plain) {
				t.Fatal("chunk key was not derived from original plaintext")
			}

			original := append([]byte(nil), object...)
			dst := make([]byte, len(tc.plain))
			if err := codec.DecryptChunkTo(context.Background(), key, object, dst); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(dst, tc.plain) {
				t.Fatal("round trip mismatch")
			}
			if !bytes.Equal(object, original) {
				t.Fatal("DecryptChunkTo mutated immutable ciphertext")
			}

			object2, hash2, key2, err := codec.EncryptChunk(context.Background(), salt, tc.plain)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(object2, object) || hash2 != hash || key2 != key {
				t.Fatal("canonical chunk encoding is not deterministic")
			}
		})
	}
}

func TestChunkCompressionBenefitBoundary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                  string
		raw, encoded          uint64
		want                  bool
	}{
		{name: "exact threshold", raw: 16 << 10, encoded: 12 << 10, want: true},
		{name: "one byte below savings", raw: 16 << 10, encoded: (12 << 10) + 1},
		{name: "enough bytes but over ratio", raw: 32 << 10, encoded: (24 << 10) + 1},
		{name: "large safe arithmetic", raw: ^uint64(0), encoded: (^uint64(0)) / 2, want: true},
		{name: "encoded larger", raw: 4096, encoded: 8192},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := compressionBeneficial(tc.raw, tc.encoded); got != tc.want {
				t.Fatalf("compressionBeneficial(%d, %d) = %v, want %v", tc.raw, tc.encoded, got, tc.want)
			}
		})
	}
}

func TestDecryptChunkToRejectsMalformedObjects(t *testing.T) {
	t.Parallel()
	codec := &AESChunkEncryptor{}
	var key [32]byte
	for _, tc := range []struct {
		name   string
		object []byte
		dst    []byte
	}{
		{name: "empty", object: nil},
		{name: "unknown format", object: []byte{0xff}},
		{name: "raw short", object: []byte{ChunkFormatAESRaw, 1}, dst: make([]byte, 2)},
		{name: "raw long", object: []byte{ChunkFormatAESRaw, 1, 2}, dst: make([]byte, 1)},
		{name: "s2 empty", object: []byte{ChunkFormatAESS2}, dst: make([]byte, 1)},
		{name: "s2 corrupt", object: []byte{ChunkFormatAESS2, 0xff, 0xff}, dst: make([]byte, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := codec.DecryptChunkTo(context.Background(), key, tc.object, tc.dst); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestDecryptChunkToRejectsWrongDecodedLengthAndOverlap(t *testing.T) {
	t.Parallel()
	codec := &AESChunkEncryptor{}
	plain := bytes.Repeat([]byte{0x42}, 256<<10)
	object, _, key, err := codec.EncryptChunk(context.Background(), [32]byte{1}, plain)
	if err != nil {
		t.Fatal(err)
	}
	if object[0] != ChunkFormatAESS2 {
		t.Fatalf("fixture format = %#x, want S2", object[0])
	}
	if err := codec.DecryptChunkTo(context.Background(), key, object, make([]byte, len(plain)-1)); err == nil {
		t.Fatal("wrong decoded length was accepted")
	}

	raw, _, rawKey, err := codec.EncryptChunk(context.Background(), [32]byte{2}, deterministicNoise(128<<10))
	if err != nil {
		t.Fatal(err)
	}
	if raw[0] != ChunkFormatAESRaw {
		t.Fatalf("fixture format = %#x, want RAW", raw[0])
	}
	if err := codec.DecryptChunkTo(context.Background(), rawKey, raw, raw[1:]); err == nil {
		t.Fatal("overlapping ciphertext and destination was accepted")
	}
}

func TestChunkScratchWaitHonorsContext(t *testing.T) {
	manager := newScratchManager(1, 2<<20, 2<<20, 2<<20)
	held, err := manager.acquire(context.Background(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer held.release()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = manager.acquire(ctx, 1<<20)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked acquire error = %v, want deadline exceeded", err)
	}
	if got := manager.peakBytes.Load(); got > 2<<20 {
		t.Fatalf("peak scratch bytes = %d, budget = %d", got, 2<<20)
	}
}

func TestRawRangeDecryptMatchesWholeChunk(t *testing.T) {
	t.Parallel()
	codec := &AESChunkEncryptor{}
	plain := deterministicNoise(128 << 10)
	object, _, key, err := codec.EncryptChunk(context.Background(), [32]byte{9}, plain)
	if err != nil {
		t.Fatal(err)
	}
	if object[0] != ChunkFormatAESRaw {
		t.Fatalf("fixture format = %#x, want RAW", object[0])
	}
	for offset := 0; offset < 2*16; offset++ {
		for _, size := range []int{1, 15, 16, 17, 4096} {
			dst := make([]byte, size)
			if err := codec.DecryptChunkRangeTo(context.Background(), key, object, len(plain), offset, dst); err != nil {
				t.Fatalf("offset=%d size=%d: %v", offset, size, err)
			}
			if !bytes.Equal(dst, plain[offset:offset+size]) {
				t.Fatalf("offset=%d size=%d mismatch", offset, size)
			}
		}
	}
}

func deterministicNoise(size int) []byte {
	out := make([]byte, size)
	var x uint64 = 0x9e3779b97f4a7c15
	for i := range out {
		x ^= x << 7
		x ^= x >> 9
		x ^= x << 8
		out[i] = byte(x)
	}
	return out
}
