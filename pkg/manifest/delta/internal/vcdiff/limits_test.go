package vcdiff

import (
	"bytes"
	"math/rand"
	"testing"
)

// These tests cover the LOCAL MODIFICATIONS (DecodeLimited and the
// per-instruction bound); they are not part of the upstream test suite.

func limitsFixture(t *testing.T) (src, tgt, stream []byte) {
	t.Helper()
	r := rand.New(rand.NewSource(1))
	src = make([]byte, 64<<10)
	r.Read(src)
	tgt = append([]byte(nil), src...)
	for i := 0; i < 20; i++ {
		tgt[r.Intn(len(tgt))] ^= 0xff
	}
	return src, tgt, EncodeBytes(src, tgt)
}

func TestDecodeLimitedRoundTrip(t *testing.T) {
	src, tgt, stream := limitsFixture(t)
	var out bytes.Buffer
	n, err := DecodeLimited(src, bytes.NewReader(stream), &out, Limits{MaxTarget: int64(len(tgt)), MaxSection: int64(len(stream))})
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(tgt)) || !bytes.Equal(out.Bytes(), tgt) {
		t.Fatalf("round trip mismatch: n=%d", n)
	}
}

func TestDecodeLimitedRejectsOversizeTarget(t *testing.T) {
	src, tgt, stream := limitsFixture(t)
	var out bytes.Buffer
	_, err := DecodeLimited(src, bytes.NewReader(stream), &out, Limits{MaxTarget: int64(len(tgt)) - 1, MaxSection: -1})
	if err == nil {
		t.Fatal("expected target limit error")
	}
	if out.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting", out.Len())
	}
}

func TestDecodeLimitedRejectsOversizeSection(t *testing.T) {
	src, _, stream := limitsFixture(t)
	var out bytes.Buffer
	_, err := DecodeLimited(src, bytes.NewReader(stream), &out, Limits{MaxTarget: -1, MaxSection: 4})
	if err == nil {
		t.Fatal("expected section limit error")
	}
}

// A hand-built window declaring a 1<<40 RUN inside a small target must fail
// without allocating anything near that size.
func TestDecodeRunOverrunsWindow(t *testing.T) {
	var s []byte
	s = append(s, 0xd6, 0xc3, 0xc4, 0x00, 0x00) // header, no secondary compressor
	s = append(s, 0x00)                         // window indicator: no source
	var body []byte
	body = append(body, varintBytes(8)...) // target window length 8
	body = append(body, 0x00)              // delta indicator
	data := []byte{0x41}
	inst := append([]byte{0x00}, varintBytes(1<<40)...) // RUN with explicit huge size
	body = append(body, varintBytes(uint64(len(data)))...)
	body = append(body, varintBytes(uint64(len(inst)))...)
	body = append(body, varintBytes(0)...)
	body = append(body, data...)
	body = append(body, inst...)
	s = append(s, varintBytes(uint64(len(body)))...)
	s = append(s, body...)

	var out bytes.Buffer
	if _, err := DecodeLimited(nil, bytes.NewReader(s), &out, Limits{MaxTarget: 8, MaxSection: int64(len(s))}); err == nil {
		t.Fatal("expected overrun error")
	}
}

func varintBytes(v uint64) []byte {
	var tmp [10]byte
	i := len(tmp)
	i--
	tmp[i] = byte(v & 0x7f)
	v >>= 7
	for v > 0 {
		i--
		tmp[i] = byte(v&0x7f) | 0x80
		v >>= 7
	}
	return tmp[i:]
}
