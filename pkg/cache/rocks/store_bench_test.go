package rocks

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

func tempRocksConfig(b *testing.B) runtime.RocksConfig {
	b.Helper()
	dir, err := os.MkdirTemp("", "rocks-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return runtime.RocksConfig{
		Path:      dir,
		DiskBytes: "1GiB",
		MemRatio:  0.1,
		BloomBits: 10,
	}
}

func BenchmarkPut_256KB(b *testing.B) {
	cfg := tempRocksConfig(b)
	s, err := openImpl(cfg, runtime.FreqConfig{})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	value := make([]byte, 256*1024)
	rand.Read(value)
	b.SetBytes(int64(len(value)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("key-%d", i)))
		if err := s.put(ctx, cfChunk, key[:], value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet_64KB(b *testing.B) {
	cfg := tempRocksConfig(b)
	s, err := openImpl(cfg, runtime.FreqConfig{})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()

	// Pre-populate 100 keys.
	value := make([]byte, 64*1024)
	rand.Read(value)
	keys := make([][32]byte, 100)
	for i := range keys {
		keys[i] = sha256.Sum256([]byte(fmt.Sprintf("key-%d", i)))
		s.put(ctx, cfChunk, keys[i][:], value)
	}

	b.SetBytes(int64(len(value)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k := keys[i%len(keys)]
		blob, found, err := s.get(ctx, cfChunk, k[:])
		if err != nil {
			b.Fatal(err)
		}
		if !found || len(blob.Bytes()) != len(value) {
			b.Fatal("unexpected miss or size mismatch")
		}
		blob.Release()
	}
}

func BenchmarkGetMiss(b *testing.B) {
	cfg := tempRocksConfig(b)
	s, err := openImpl(cfg, runtime.FreqConfig{})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("miss-%d", i)))
		_, found, err := s.get(ctx, cfChunk, key[:])
		if err != nil {
			b.Fatal(err)
		}
		if found {
			b.Fatal("unexpected hit for miss key")
		}
	}
}
