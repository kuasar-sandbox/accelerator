package crypto

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
)

func TestChunkCodecRawAndSnappyRoundTrip(t *testing.T) {
	t.Parallel()
	var salt [32]byte
	copy(salt[:], "issue-72-canonical-salt")

	tests := []struct {
		name   string
		plain  []byte
		format byte
	}{
		{name: "raw", plain: deterministicNoise(1 << 20), format: ChunkFormatAESRaw},
		{name: "snappy", plain: bytes.Repeat([]byte("compressible snapshot page\x00"), 1<<15), format: ChunkFormatAESSnappy},
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

func TestCanonicalChunkGoldenVectors(t *testing.T) {
	var salt [32]byte
	for i := range salt {
		salt[i] = byte(i)
	}
	codec := &AESChunkEncryptor{}
	for _, tc := range []struct {
		name       string
		plain      []byte
		wantObject string
		wantHash   string
		wantKey    string
	}{
		{
			name:       "raw",
			plain:      []byte("canonical raw payload"),
			wantObject: "0140d94c0e3accdc9a1010a82610e6d8713111cd4eba",
			wantHash:   "c3c223b605a52155b17d995a2bd00ab802e9c1681f77dfd57cd9481acb48df48",
			wantKey:    "77c35b305bd2147369a1bdcb499c9570b16f29e88419d5d7805a856d87039f64",
		},
		{
			name:       "snappy",
			plain:      bytes.Repeat([]byte{0x5a}, 4608),
			wantObject: "022b17b5088d36cbea767a57a891b307413b901624c28b07026a25ef11f1c23e73ddf7a468dbb923602a2e2d2fbe9a804f7ede9e1aa13ecd4e6e1a1cc4e6fcc927621917cde1469caaf54209448be74b04661040a17887a4e94347c120bd500c2dd92d2b57e357617adffb095040425b0b63141d30cc7df48ba157582a887dc3e619824e49f3d09edddb176d9d9d3aa6365ee23c5ab0da73eeb2df9d8bca30cfb2775b29e8885aa9412b04a7abaf3ce6f79752c46f394f9003f242fc593e53a876ee2f5694067342556cec65b9f09f9491d8d0a15d4b707ddc43efd831",
			wantHash:   "8341a56c0f45801626123fa9ba465fbaeb6af2790daaeb756466078e51a0d5bc",
			wantKey:    "1eca5d3527b5a461ff40dafe9be38feefdef0b684df37ba39e1ed57efcaf0905",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			object, hash, key, err := codec.EncryptChunk(context.Background(), salt, tc.plain)
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(object); got != tc.wantObject {
				t.Fatalf("object = %s, want %s", got, tc.wantObject)
			}
			if got := hex.EncodeToString(hash[:]); got != tc.wantHash {
				t.Fatalf("hash = %s, want %s", got, tc.wantHash)
			}
			if got := hex.EncodeToString(key[:]); got != tc.wantKey {
				t.Fatalf("key = %s, want %s", got, tc.wantKey)
			}
		})
	}
}

func TestChunkCompressionBenefitBoundary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		raw, encoded uint64
		want         bool
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

func TestEncryptChunkFailsOnCompressionErrorOrInvalidContract(t *testing.T) {
	sentinel := errors.New("compressor failed")
	for _, tc := range []struct {
		name   string
		encode func(dst, src []byte) ([]byte, error)
	}{
		{
			name: "error",
			encode: func([]byte, []byte) ([]byte, error) {
				return nil, sentinel
			},
		},
		{
			name: "foreign allocation",
			encode: func(_ []byte, src []byte) ([]byte, error) {
				return snappy.Encode(nil, src), nil
			},
		},
		{
			name: "wrong decoded length",
			encode: func(dst, _ []byte) ([]byte, error) {
				return snappy.Encode(dst, []byte("wrong source")), nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			codec := &AESChunkEncryptor{encodeBlock: tc.encode}
			object, _, _, err := codec.EncryptChunk(context.Background(), [32]byte{}, bytes.Repeat([]byte("snapshot"), 1024))
			if err == nil {
				t.Fatal("invalid compressor result silently fell back to RAW")
			}
			if object != nil {
				t.Fatal("compression failure returned a physical object")
			}
			if tc.name == "error" && !errors.Is(err, sentinel) {
				t.Fatalf("error = %v, want wrapped sentinel", err)
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
		{name: "snappy empty", object: []byte{ChunkFormatAESSnappy}, dst: make([]byte, 1)},
		{name: "snappy corrupt", object: []byte{ChunkFormatAESSnappy, 0xff, 0xff}, dst: make([]byte, 1)},
		{name: "snappy insufficient benefit", object: append([]byte{ChunkFormatAESSnappy}, make([]byte, 60<<10)...), dst: make([]byte, 64<<10)},
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
	if object[0] != ChunkFormatAESSnappy {
		t.Fatalf("fixture format = %#x, want Snappy", object[0])
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

func TestChunkScratchByteWaitHonorsContextWithoutLeakingSlot(t *testing.T) {
	manager := newScratchManager(2, 64<<10, 64<<10, 64<<10)
	held, err := manager.acquire(context.Background(), 64<<10)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err = manager.acquire(ctx, 64<<10)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked byte acquire error = %v, want deadline exceeded", err)
	}
	held.release()

	lease, err := manager.acquire(context.Background(), 64<<10)
	if err != nil {
		t.Fatalf("slot leaked after canceled byte wait: %v", err)
	}
	lease.release()
}

func TestEncryptAndDecryptScratchAdmissionHonorContext(t *testing.T) {
	plain := bytes.Repeat([]byte("context-cancel-snapshot\x00"), 16<<10)
	base := &AESChunkEncryptor{}
	object, _, key, err := base.EncryptChunk(context.Background(), [32]byte{0x72}, plain)
	if err != nil {
		t.Fatal(err)
	}
	if object[0] != ChunkFormatAESSnappy {
		t.Fatalf("fixture format = %#x, want Snappy", object[0])
	}

	decodeManager := newScratchManager(1, 2<<20, 2<<20, 2<<20)
	heldDecode, err := decodeManager.acquire(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = (&AESChunkEncryptor{decodeScratch: decodeManager}).DecryptChunkTo(ctx, key, object, make([]byte, len(plain)))
	cancel()
	heldDecode.release()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("decode error = %v, want deadline exceeded", err)
	}

	encodeManager := newScratchManager(1, 2<<20, 2<<20, 2<<20)
	heldEncode, err := encodeManager.acquire(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, _, _, err = (&AESChunkEncryptor{encodeScratch: encodeManager}).EncryptChunk(ctx, [32]byte{0x72}, plain)
	cancel()
	heldEncode.release()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("encode error = %v, want deadline exceeded", err)
	}
}

func TestSnappyWrongKeyFails(t *testing.T) {
	t.Parallel()
	codec := &AESChunkEncryptor{}
	plain := bytes.Repeat([]byte("wrong-key-check\x00"), 16<<10)
	object, _, key, err := codec.EncryptChunk(context.Background(), [32]byte{0x44}, plain)
	if err != nil {
		t.Fatal(err)
	}
	key[0] ^= 0xff
	if err := codec.DecryptChunkTo(context.Background(), key, object, make([]byte, len(plain))); err == nil {
		t.Fatal("Snappy object decrypted under the wrong key")
	}
}

func TestScratchConcurrentPeakAndRetentionAreBounded(t *testing.T) {
	const (
		budget         = 256 << 10
		retainedBudget = 128 << 10
	)
	manager := newScratchManager(8, budget, 64<<10, retainedBudget)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			lease, err := manager.acquire(context.Background(), 64<<10)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			lease.markTouched(len(lease.buf))
			for i := range lease.buf {
				lease.buf[i] = byte(i)
			}
			lease.release()
		}()
	}
	close(start)
	wg.Wait()
	if got := manager.peakBytes.Load(); got > budget {
		t.Fatalf("peak active bytes = %d, budget = %d", got, budget)
	}
	manager.mu.Lock()
	retained := manager.retainedBytes
	manager.mu.Unlock()
	if retained > retainedBudget {
		t.Fatalf("retained bytes = %d, budget = %d", retained, retainedBudget)
	}
}

func TestScratchWarmSizeClassReusesBuffer(t *testing.T) {
	manager := newScratchManager(1, 2<<20, 2<<20, 2<<20)
	lease, err := manager.acquire(context.Background(), 512<<10)
	if err != nil {
		t.Fatal(err)
	}
	lease.release()
	misses := manager.poolMisses.Load()
	for range 10 {
		lease, err := manager.acquire(context.Background(), 512<<10)
		if err != nil {
			t.Fatal(err)
		}
		lease.release()
	}
	if got := manager.poolMisses.Load(); got != misses {
		t.Fatalf("warm size class added %d pool misses", got-misses)
	}
}

func TestScratchReleaseClearsTouchedBytes(t *testing.T) {
	manager := newScratchManager(1, 64<<10, 64<<10, 64<<10)
	lease, err := manager.acquire(context.Background(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	for i := range lease.buf {
		lease.buf[i] = 0xa5
	}
	lease.markTouched(len(lease.buf))
	lease.release()

	reused, err := manager.acquire(context.Background(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer reused.release()
	if !bytes.Equal(reused.buf, make([]byte, len(reused.buf))) {
		t.Fatal("released scratch retained data from its previous user")
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

func TestRangeDecryptRejectsDecodedChunkAboveHardLimit(t *testing.T) {
	err := (&AESChunkEncryptor{}).DecryptChunkRangeTo(
		context.Background(),
		[32]byte{},
		[]byte{ChunkFormatAESRaw},
		MaxChunkDecodedSize+1,
		0,
		nil,
	)
	if !errors.Is(err, ErrChunkTooLarge) {
		t.Fatalf("error = %v, want ErrChunkTooLarge", err)
	}
}

func TestDecryptChunkToWarmAllocationsArePayloadIndependent(t *testing.T) {
	codec := &AESChunkEncryptor{}
	for _, tc := range []struct {
		name      string
		plain     []byte
		format    byte
		maxAllocs float64
	}{
		{name: "raw", plain: deterministicNoise(1 << 20), format: ChunkFormatAESRaw, maxAllocs: 3},
		{name: "snappy", plain: bytes.Repeat([]byte("allocation-snapshot-page\x00"), 40<<10), format: ChunkFormatAESSnappy, maxAllocs: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			object, _, key, err := codec.EncryptChunk(context.Background(), [32]byte{0x72}, tc.plain)
			if err != nil {
				t.Fatal(err)
			}
			if object[0] != tc.format {
				t.Fatalf("format = %#x, want %#x", object[0], tc.format)
			}
			dst := make([]byte, len(tc.plain))
			if err := codec.DecryptChunkTo(context.Background(), key, object, dst); err != nil {
				t.Fatal(err)
			}
			allocs := testing.AllocsPerRun(100, func() {
				if err := codec.DecryptChunkTo(context.Background(), key, object, dst); err != nil {
					panic(err)
				}
			})
			if allocs > tc.maxAllocs {
				t.Fatalf("warm decrypt allocations = %.1f, want <= %.1f (no payload allocation)", allocs, tc.maxAllocs)
			}
		})
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
