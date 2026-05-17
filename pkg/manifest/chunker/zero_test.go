package chunker

import (
	"bytes"
	"testing"
)

// collectChunks runs Chunk over input and returns the produced
// ChunkResult records. Zero-chunk callers can inspect both IsZero and
// the Data payload (which must be nil for IsZero=true).
func collectChunks(t *testing.T, input []byte, cfg Config) []ChunkResult {
	t.Helper()
	chk, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var out []ChunkResult
	if err := chk.Chunk(bytes.NewReader(input), func(cr ChunkResult) error {
		// Important: Data is a sub-slice of the chunker's read buffer
		// for the non-zero path, so a deferred snapshot would alias
		// across chunks. Copy into a stable owned slice up front.
		var dataCopy []byte
		if cr.Data != nil {
			dataCopy = append([]byte(nil), cr.Data...)
		}
		out = append(out, ChunkResult{
			Offset: cr.Offset,
			Size:   cr.Size,
			Data:   dataCopy,
			IsZero: cr.IsZero,
		})
		return nil
	}); err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	return out
}

// TestFixed_AllZero — fixed chunker treats a uniformly-zero buffer as
// a sequence of IsZero chunks with Data=nil.
func TestFixed_AllZero(t *testing.T) {
	cfg := Config{Mode: "fixed", Fixed: FixedConfig{Size: "64KiB"}}
	input := make([]byte, 4*64*1024) // 4 chunks
	results := collectChunks(t, input, cfg)
	if len(results) != 4 {
		t.Fatalf("got %d chunks, want 4", len(results))
	}
	for i, cr := range results {
		if !cr.IsZero {
			t.Errorf("chunk %d: IsZero=false, want true", i)
		}
		if cr.Data != nil {
			t.Errorf("chunk %d: Data is non-nil (%d bytes), want nil", i, len(cr.Data))
		}
		if cr.Size != 64*1024 {
			t.Errorf("chunk %d: Size=%d, want %d", i, cr.Size, 64*1024)
		}
	}
}

// TestFixed_PartialZero — non-zero leading chunk + zero tail. Verify
// the IsZero flag flips correctly across the boundary and the
// non-zero chunk carries its data.
func TestFixed_PartialZero(t *testing.T) {
	cfg := Config{Mode: "fixed", Fixed: FixedConfig{Size: "64KiB"}}
	input := make([]byte, 3*64*1024)
	// First 64 KiB: random non-zero pattern.
	for i := 0; i < 64*1024; i++ {
		input[i] = byte(i%255) + 1 // never zero
	}
	results := collectChunks(t, input, cfg)
	if len(results) != 3 {
		t.Fatalf("got %d chunks, want 3", len(results))
	}
	if results[0].IsZero {
		t.Error("chunk 0: should NOT be IsZero")
	}
	if results[0].Data == nil {
		t.Error("chunk 0: Data should be non-nil")
	}
	if !bytes.Equal(results[0].Data, input[:64*1024]) {
		t.Error("chunk 0: Data does not match input")
	}
	for i := 1; i < 3; i++ {
		if !results[i].IsZero {
			t.Errorf("chunk %d: should be IsZero", i)
		}
		if results[i].Data != nil {
			t.Errorf("chunk %d: Data should be nil", i)
		}
	}
}

// TestFixed_SingleByteNonZero — even a single non-zero byte breaks the
// IsZero classification for that chunk.
func TestFixed_SingleByteNonZero(t *testing.T) {
	cfg := Config{Mode: "fixed", Fixed: FixedConfig{Size: "64KiB"}}
	input := make([]byte, 64*1024)
	input[12345] = 0x42
	results := collectChunks(t, input, cfg)
	if len(results) != 1 {
		t.Fatalf("got %d chunks, want 1", len(results))
	}
	if results[0].IsZero {
		t.Error("chunk 0: IsZero=true with one non-zero byte present")
	}
	if results[0].Data == nil {
		t.Error("chunk 0: Data should be non-nil")
	}
}

// TestCDC_AllZero — CDC chunker over a uniformly-zero stream produces
// IsZero chunks (size determined by CDC, not asserted exactly here —
// just that every chunk is IsZero with nil Data).
func TestCDC_AllZero(t *testing.T) {
	cfg := Config{} // defaults: cdc mode, 128K/512K/1M // CDC, 64K min / 512K avg / 1M max
	input := make([]byte, 4*1024*1024)
	results := collectChunks(t, input, cfg)
	if len(results) == 0 {
		t.Fatal("expected at least 1 chunk for 4MiB input")
	}
	var total uint64
	for i, cr := range results {
		if !cr.IsZero {
			t.Errorf("chunk %d: IsZero=false in all-zero stream", i)
		}
		if cr.Data != nil {
			t.Errorf("chunk %d: Data non-nil in all-zero stream", i)
		}
		total += uint64(cr.Size)
	}
	if total != uint64(len(input)) {
		t.Errorf("size sum %d != input %d", total, len(input))
	}
}

// TestCDC_HasZeroChunk — non-zero prefix + long zero tail, expect at
// least one IsZero chunk in the tail. CDC boundaries are
// content-defined so we don't pin the exact count, only the property.
func TestCDC_HasZeroChunk(t *testing.T) {
	cfg := Config{} // defaults: cdc mode, 128K/512K/1M
	input := make([]byte, 4*1024*1024)
	for i := 0; i < 256*1024; i++ {
		input[i] = byte(i%255) + 1
	}
	results := collectChunks(t, input, cfg)
	zeroCount := 0
	for _, cr := range results {
		if cr.IsZero {
			zeroCount++
			if cr.Data != nil {
				t.Error("IsZero chunk should have nil Data")
			}
		}
	}
	if zeroCount == 0 {
		t.Fatal("expected at least one IsZero chunk in 4MiB input with 3.75MiB zero tail")
	}
}

// TestIsAllZero — the helper itself, since both fixed and CDC depend on it.
func TestIsAllZero(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want bool
	}{
		{"empty", []byte{}, true},
		{"single zero", []byte{0}, true},
		{"single nonzero", []byte{1}, false},
		{"all zero", make([]byte, 1024), true},
		{"trailing nonzero", append(make([]byte, 1023), 0x01), false},
		{"leading nonzero", append([]byte{0x01}, make([]byte, 1023)...), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isAllZero(tc.in); got != tc.want {
				t.Errorf("isAllZero(%v)=%v, want %v", tc.in[:min(8, len(tc.in))], got, tc.want)
			}
		})
	}
}
