package delta

import (
	"bytes"
	"errors"
	"testing"
)

// Checks that Encode followed by Apply rebuilds the target exactly, for ten kinds
// of input: identical, a few edits, insertion, deletion, unrelated data, empty,
// one byte, all zeros, a base prefix, and a doubled base.
func TestEncodeApplyRoundTrip(t *testing.T) {
	base := randBytes(1, 256<<10)
	cases := map[string][]byte{
		"identical":    base,
		"few edits":    mutate(base, 2, 5, 100),
		"insertion":    append(append(append([]byte(nil), base[:1000]...), []byte("inserted")...), base[1000:]...),
		"deletion":     append(append([]byte(nil), base[:5000]...), base[6000:]...),
		"unrelated":    randBytes(9, 64<<10),
		"empty":        {},
		"one byte":     {7},
		"zeros":        make([]byte, 100<<10),
		"base prefix":  base[:10000],
		"base doubled": append(append([]byte(nil), base...), base...),
	}
	for name, target := range cases {
		stream := Encode(base, target)
		got, err := Apply(base, stream, len(target))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !bytes.Equal(got, target) {
			t.Fatalf("%s: round trip mismatch", name)
		}
	}
}

// Checks that encoding the same inputs twice gives identical bytes, since the
// delta object's address depends on it. Also checks that the delta of a
// near-duplicate is under 1/20 of the target size.
func TestEncodeIsDeterministicAndSmallForSimilar(t *testing.T) {
	base := randBytes(1, 256<<10)
	target := mutate(base, 2, 5, 100)
	a, b := Encode(base, target), Encode(base, target)
	if !bytes.Equal(a, b) {
		t.Fatal("encode must be deterministic: it fixes the delta object's address")
	}
	if len(a) > len(target)/20 {
		t.Fatalf("delta of a near-duplicate is %d bytes for a %d byte target", len(a), len(target))
	}
}

// Checks that Encode gives the same bytes when 16 goroutines call it at once,
// sharing the encoder pool. Each goroutine's output must match a single-threaded
// encode of the same target.
func TestEncodeConcurrent(t *testing.T) {
	base := randBytes(1, 128<<10)
	targets := [][]byte{mutate(base, 1, 3, 50), mutate(base, 2, 9, 80), mutate(base, 3, 1, 10)}
	want := make([][]byte, len(targets))
	for i, tg := range targets {
		want[i] = Encode(base, tg)
	}
	errc := make(chan error, 16)
	for g := 0; g < 16; g++ {
		go func(g int) {
			i := g % len(targets)
			if !bytes.Equal(Encode(base, targets[i]), want[i]) {
				errc <- errors.New("concurrent encode diverged")
				return
			}
			errc <- nil
		}(g)
	}
	for g := 0; g < 16; g++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
}

// Checks that Apply returns ErrBadDelta for a declared size that does not match
// the target, and for an empty, random, truncated, or bad-header stream. Also
// checks that decoding against a different base does not reproduce the target.
func TestApplyRejectsWrongSizeAndGarbage(t *testing.T) {
	base := randBytes(1, 64<<10)
	target := mutate(base, 2, 3, 40)
	stream := Encode(base, target)

	for _, size := range []int{len(target) - 1, len(target) + 1, 0, -1} {
		if _, err := Apply(base, stream, size); !errors.Is(err, ErrBadDelta) {
			t.Errorf("size %d: want ErrBadDelta, got %v", size, err)
		}
	}
	for name, bad := range map[string][]byte{
		"empty":      {},
		"garbage":    randBytes(5, 100),
		"truncated":  stream[:len(stream)/2],
		"bad header": append([]byte{0, 0, 0}, stream[3:]...),
	} {
		if _, err := Apply(base, bad, len(target)); !errors.Is(err, ErrBadDelta) {
			t.Errorf("%s: want ErrBadDelta, got %v", name, err)
		}
	}
	// The wrong base must not silently yield the target.
	if got, err := Apply(randBytes(77, 64<<10), stream, len(target)); err == nil && bytes.Equal(got, target) {
		t.Error("decoding against a different base reproduced the target")
	}
}

// Checks that a stream producing more output than the declared size is rejected
// with ErrBadDelta. The test encodes a 64 KiB target but declares 1024 bytes, so
// the failure comes from the output exceeding the declared size.
func TestApplyBoundedByDeclaredSize(t *testing.T) {
	base := randBytes(1, 64<<10)
	big := mutate(base, 2, 3, 40)
	stream := Encode(base, big)
	if _, err := Apply(base, stream, 1024); !errors.Is(err, ErrBadDelta) {
		t.Fatalf("stream larger than declared size must fail: %v", err)
	}
}

// Fuzzes the round trip: for any base and target up to 64 KiB, Apply(Encode(base,
// target)) must return the target exactly. Larger inputs are skipped.
func FuzzEncodeApply(f *testing.F) {
	f.Add([]byte("hello world hello world"), []byte("hello brave world hello"))
	f.Add([]byte{}, []byte{1, 2, 3})
	f.Add(bytes.Repeat([]byte{0}, 5000), bytes.Repeat([]byte{0}, 4000))
	f.Add(randBytes(1, 3000), mutate(randBytes(1, 3000), 2, 3, 20))
	f.Fuzz(func(t *testing.T, base, target []byte) {
		if len(base) > 1<<16 || len(target) > 1<<16 {
			t.Skip()
		}
		stream := Encode(base, target)
		got, err := Apply(base, stream, len(target))
		if err != nil {
			t.Fatalf("round trip failed: %v", err)
		}
		if !bytes.Equal(got, target) {
			t.Fatal("round trip mismatch")
		}
	})
}

// Checks that Apply never panics or over-allocates on arbitrary bytes. It fuzzes
// the stream and declared size against a fixed 2 KiB base, with sizes up to 1 MiB.
// Any success must return exactly the declared size. The seeds include a valid
// stream and a bare VCDIFF header.
func FuzzApplyUntrusted(f *testing.F) {
	base := randBytes(1, 2048)
	f.Add(Encode(base, mutate(base, 2, 2, 20)), 2048)
	f.Add([]byte{0xd6, 0xc3, 0xc4, 0x00, 0x00}, 10)
	f.Fuzz(func(t *testing.T, stream []byte, size int) {
		if size < 0 || size > 1<<20 {
			t.Skip()
		}
		out, err := Apply(base, stream, size)
		if err == nil && len(out) != size {
			t.Fatalf("success with wrong length %d != %d", len(out), size)
		}
	})
}
