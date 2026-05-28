package ec

import (
	"bytes"
	"testing"
)

func BenchmarkEncode_256KB(b *testing.B) {
	enc, _ := newEncoder(4, 1)
	data := bytes.Repeat([]byte("X"), 256*1024)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.Encode(data)
	}
}

func BenchmarkDecode_Normal(b *testing.B) {
	enc, _ := newEncoder(4, 1)
	data := bytes.Repeat([]byte("X"), 256*1024)
	shards, _ := enc.Encode(data)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Copy shards to avoid mutation across iterations.
		cp := make([][]byte, len(shards))
		for j := range shards {
			cp[j] = append([]byte{}, shards[j]...)
		}
		_, _ = enc.Decode(cp)
	}
}

func BenchmarkDecode_1Missing(b *testing.B) {
	enc, _ := newEncoder(4, 1)
	data := bytes.Repeat([]byte("X"), 256*1024)
	shards, _ := enc.Encode(data)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cp := make([][]byte, len(shards))
		for j := range shards {
			cp[j] = append([]byte{}, shards[j]...)
		}
		cp[i%5] = nil // rotate which shard is missing
		_, _ = enc.Decode(cp)
	}
}

// BenchmarkDecode_AllHit_512KB measures the Decode fast path at the
// value size used by the integration bench. No reconstruction needed
// (Reconstruct still runs internally but no shard is nil), so this
// isolates the joined-buffer alloc + 4× shard append cost — a known
// hot-path candidate from the perf inventory.
func BenchmarkDecode_AllHit_512KB(b *testing.B) {
	enc, _ := newEncoder(4, 1)
	data := bytes.Repeat([]byte("X"), 512*1024)
	shards, _ := enc.Encode(data)
	// Snapshot shard contents once; reuse per iteration via fresh
	// slice-of-slices (Decode mutates `shards` entries via Reconstruct
	// but only when some are nil; for all-hit it doesn't touch them).
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cp := make([][]byte, len(shards))
		copy(cp, shards) // slice-header copy; bytes shared
		_, _ = enc.Decode(cp)
	}
}
