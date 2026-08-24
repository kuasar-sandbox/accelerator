package crypto

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

var benchmarkBytes []byte
var benchmarkHash [32]byte

// BenchmarkCompressionCandidates keeps the format-freeze comparison in-tree.
// Production uses only Go Snappy block Encode. S2, S2 Better, and Zstd
// SpeedFastest are benchmark controls, never runtime choices.
func BenchmarkCompressionCandidates(b *testing.B) {
	for name, corpus := range benchmarkCorpora() {
		b.Run(name, func(b *testing.B) {
			zstdEncoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
			if err != nil {
				b.Fatal(err)
			}
			defer zstdEncoder.Close()
			maxS2 := s2.MaxEncodedLen(len(corpus))
			maxSnappy := snappy.MaxEncodedLen(len(corpus))
			for _, candidate := range []struct {
				name       string
				maxEncoded int
				encode     func([]byte) []byte
			}{
				{name: "raw", maxEncoded: len(corpus), encode: func(dst []byte) []byte { return append(dst[:0], corpus...) }},
				{name: "go-snappy", maxEncoded: maxSnappy, encode: func(dst []byte) []byte { return snappy.Encode(dst[:maxSnappy], corpus) }},
				{name: "s2", maxEncoded: maxS2, encode: func(dst []byte) []byte { return s2.Encode(dst[:0], corpus) }},
				{name: "s2-better", maxEncoded: maxS2, encode: func(dst []byte) []byte { return s2.EncodeBetter(dst[:0], corpus) }},
				{name: "zstd-fastest", maxEncoded: len(corpus), encode: func(dst []byte) []byte { return zstdEncoder.EncodeAll(corpus, dst[:0]) }},
			} {
				b.Run(candidate.name+"/encode", func(b *testing.B) {
					dst := make([]byte, 0, candidate.maxEncoded)
					encoded := candidate.encode(dst)
					ratio := float64(len(encoded)) / float64(len(corpus))
					b.ReportAllocs()
					b.SetBytes(int64(len(corpus)))
					b.ResetTimer()
					for range b.N {
						benchmarkBytes = candidate.encode(dst)
					}
					b.StopTimer()
					b.ReportMetric(ratio, "physical/logical")
				})
			}

			raw := append([]byte(nil), corpus...)
			snappyBlock := snappy.Encode(nil, corpus)
			s2Block := s2.Encode(nil, corpus)
			s2Better := s2.EncodeBetter(nil, corpus)
			zstdBlock := zstdEncoder.EncodeAll(corpus, nil)
			zstdDecoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
			if err != nil {
				b.Fatal(err)
			}
			defer zstdDecoder.Close()
			for _, candidate := range []struct {
				name    string
				encoded []byte
				decode  func([]byte) ([]byte, error)
			}{
				{name: "raw", encoded: raw, decode: func(dst []byte) ([]byte, error) { return append(dst[:0], raw...), nil }},
				{name: "go-snappy", encoded: snappyBlock, decode: func(dst []byte) ([]byte, error) { return snappy.Decode(dst, snappyBlock) }},
				{name: "s2", encoded: s2Block, decode: func(dst []byte) ([]byte, error) { return s2.Decode(dst[:0:len(corpus)], s2Block) }},
				{name: "s2-better", encoded: s2Better, decode: func(dst []byte) ([]byte, error) { return s2.Decode(dst[:0:len(corpus)], s2Better) }},
				{name: "zstd-fastest", encoded: zstdBlock, decode: func(dst []byte) ([]byte, error) { return zstdDecoder.DecodeAll(zstdBlock, dst[:0]) }},
			} {
				b.Run(candidate.name+"/decode", func(b *testing.B) {
					dst := make([]byte, len(corpus))
					ratio := float64(len(candidate.encoded)) / float64(len(corpus))
					b.ReportAllocs()
					b.SetBytes(int64(len(corpus)))
					b.ResetTimer()
					for range b.N {
						decoded, err := candidate.decode(dst)
						if err != nil || len(decoded) != len(corpus) {
							b.Fatal(len(decoded), err)
						}
						benchmarkBytes = decoded
					}
					b.StopTimer()
					b.ReportMetric(ratio, "physical/logical")
				})
			}
		})
	}
}

func BenchmarkAESChunkCanonicalCodec(b *testing.B) {
	codec := &AESChunkEncryptor{}
	for name, plain := range map[string][]byte{
		"raw":    deterministicNoise(1 << 20),
		"snappy": benchmarkSnapshotLike(1 << 20),
	} {
		b.Run(name, func(b *testing.B) {
			object, _, key, err := codec.EncryptChunk(context.Background(), [32]byte{0x72}, plain)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("encrypt", func(b *testing.B) {
				beforeMisses := codec.encScratch().poolMisses.Load()
				b.ReportAllocs()
				b.SetBytes(int64(len(plain)))
				b.ReportMetric(float64(len(object))/float64(len(plain)), "physical/logical")
				b.ResetTimer()
				for range b.N {
					var err error
					benchmarkBytes, _, _, err = codec.EncryptChunk(context.Background(), [32]byte{0x72}, plain)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(codec.encScratch().poolMisses.Load()-beforeMisses)/float64(b.N), "scratch-misses/op")
			})
			b.Run("decrypt", func(b *testing.B) {
				dst := make([]byte, len(plain))
				if err := codec.DecryptChunkTo(context.Background(), key, object, dst); err != nil {
					b.Fatal(err)
				}
				beforeMisses := codec.decScratch().poolMisses.Load()
				b.ReportAllocs()
				b.SetBytes(int64(len(plain)))
				b.ResetTimer()
				for range b.N {
					if err := codec.DecryptChunkTo(context.Background(), key, object, dst); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(len(object))/float64(len(plain)), "physical/logical")
				b.ReportMetric(float64(codec.decScratch().poolMisses.Load()-beforeMisses)/float64(b.N), "scratch-misses/op")
			})
		})
	}
}

func BenchmarkAESChunkLatencyQuantiles(b *testing.B) {
	codec := &AESChunkEncryptor{}
	for name, plain := range map[string][]byte{
		"raw":    deterministicNoise(1 << 20),
		"snappy": benchmarkSnapshotLike(1 << 20),
	} {
		object, _, key, err := codec.EncryptChunk(context.Background(), [32]byte{0x72}, plain)
		if err != nil {
			b.Fatal(err)
		}
		dst := make([]byte, len(plain))
		operations := []struct {
			name string
			run  func() error
		}{
			{name: "encrypt", run: func() error {
				var err error
				benchmarkBytes, _, _, err = codec.EncryptChunk(context.Background(), [32]byte{0x72}, plain)
				return err
			}},
			{name: "decrypt", run: func() error {
				return codec.DecryptChunkTo(context.Background(), key, object, dst)
			}},
		}
		if name == "raw" {
			operations = append(operations, struct {
				name string
				run  func() error
			}{name: "baseline-raw-encrypt", run: func() error {
				benchmarkBytes, benchmarkHash = benchmarkBaselineRawEncrypt([32]byte{0x72}, plain)
				return nil
			}})
		}
		for _, operation := range operations {
			b.Run(name+"/"+operation.name, func(b *testing.B) {
				samples := make([]int64, b.N)
				b.ReportAllocs()
				b.SetBytes(int64(len(plain)))
				b.ResetTimer()
				for i := range b.N {
					started := time.Now()
					if err := operation.run(); err != nil {
						b.Fatal(err)
					}
					samples[i] = time.Since(started).Nanoseconds()
				}
				b.StopTimer()
				sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
				b.ReportMetric(float64(percentileNanos(samples, 0.50)), "p50-ns")
				b.ReportMetric(float64(percentileNanos(samples, 0.95)), "p95-ns")
				b.ReportMetric(float64(percentileNanos(samples, 0.99)), "p99-ns")
			})
		}
	}
}

// benchmarkBaselineRawEncrypt is the pre-Issue-72 chunk writer retained only
// as a benchmark control. It is not a reader fallback or production format
// compatibility path.
func benchmarkBaselineRawEncrypt(salt [32]byte, plaintext []byte) ([]byte, [32]byte) {
	key := DeriveKey(salt, plaintext)
	block, err := aes.NewCipher(key[:])
	if err != nil {
		panic(err)
	}
	object := make([]byte, 1+len(plaintext))
	object[0] = ChunkFormatAESRaw
	var iv [aes.BlockSize]byte
	cipher.NewCTR(block, iv[:]).XORKeyStream(object[1:], plaintext)
	return object, sha256.Sum256(object)
}

func percentileNanos(sorted []int64, percentile float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(percentile * float64(len(sorted)-1))
	return sorted[index]
}

func benchmarkCorpora() map[string][]byte {
	return map[string][]byte{
		"snapshot-like-pages": benchmarkSnapshotLike(1 << 20),
		"disk-text-mix":       bytes.Repeat([]byte("{\"path\":\"/var/lib/data\",\"mode\":420}\nELF\x00\x00\x00\x00"), (1<<20)/48),
		"high-entropy":        deterministicNoise(1 << 20),
	}
}

func benchmarkSnapshotLike(size int) []byte {
	out := make([]byte, size)
	for page := 0; page < size/(4<<10); page++ {
		if page%7 == 0 || page%19 == 0 {
			copy(out[page*(4<<10):], []byte(fmt.Sprintf("task=%06d heap-object kuasar snapshot", page)))
		}
	}
	return out
}
