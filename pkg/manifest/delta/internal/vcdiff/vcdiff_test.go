package vcdiff

import (
	"bytes"
	"errors"
	"hash/adler32"
	"io"
	"math/rand"
	"testing"
)

var errBoom = errors.New("boom")

// failWriter fails on the (failAt+1)-th Write call.
type failWriter struct{ failAt, n int }

func (w *failWriter) Write(p []byte) (int, error) {
	if w.n >= w.failAt {
		return 0, errBoom
	}
	w.n++
	return len(p), nil
}

// errReader yields its payload once, then returns a non-EOF error.
type errReader struct {
	payload []byte
	done    bool
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, errBoom
	}
	r.done = true
	return copy(p, r.payload), nil
}

// windowSpec lets a test hand-build a single VCDIFF window with full control.
type windowSpec struct {
	winInd           byte
	seg              *[2]uint64 // segLen, segPos; nil omits the segment fields
	targetLen        uint64
	deltaInd         byte
	checksum         *uint32
	data, inst, addr []byte
}

func (ws windowSpec) bytes() []byte {
	body := appendVarint(nil, ws.targetLen)
	body = append(body, ws.deltaInd)
	body = appendVarint(body, uint64(len(ws.data)))
	body = appendVarint(body, uint64(len(ws.inst)))
	body = appendVarint(body, uint64(len(ws.addr)))
	if ws.checksum != nil {
		body = append(body, byte(*ws.checksum>>24), byte(*ws.checksum>>16), byte(*ws.checksum>>8), byte(*ws.checksum))
	}
	body = append(body, ws.data...)
	body = append(body, ws.inst...)
	body = append(body, ws.addr...)

	w := []byte{ws.winInd}
	if ws.seg != nil {
		w = appendVarint(w, ws.seg[0])
		w = appendVarint(w, ws.seg[1])
	}
	w = appendVarint(w, uint64(len(body)))
	return append(w, body...)
}

func fullDelta(specs ...windowSpec) []byte {
	d := appendHeader(nil)
	for _, s := range specs {
		d = append(d, s.bytes()...)
	}
	return d
}

func decodeBytes(t *testing.T, source, delta []byte) ([]byte, error) {
	t.Helper()
	var out bytes.Buffer
	_, err := Decode(source, bytes.NewReader(delta), &out)
	return out.Bytes(), err
}

// --- varint ----------------------------------------------------------------

func TestVarintRoundTrip(t *testing.T) {
	for _, v := range []uint64{0, 1, 127, 128, 16383, 16384, 123456789, 1 << 40, ^uint64(0)} {
		enc := appendVarint(nil, v)
		if len(enc) != varintLen(v) {
			t.Fatalf("varintLen(%d)=%d, encoded %d bytes", v, varintLen(v), len(enc))
		}
		got, err := readVarint(&byteCursor{b: enc})
		if err != nil || got != v {
			t.Fatalf("readVarint(%x) = (%d, %v), want %d", enc, got, err, v)
		}
	}
	// Spec example.
	if got := appendVarint(nil, 123456789); !bytes.Equal(got, []byte{0xBA, 0xEF, 0x9A, 0x15}) {
		t.Fatalf("appendVarint(123456789) = %x, want BAEF9A15", got)
	}
}

func TestReadVarintErrors(t *testing.T) {
	if _, err := readVarint(&byteCursor{}); err == nil {
		t.Fatal("expected EOF on empty input")
	}
	// Ten 0xFF groups exceed 64 bits, tripping the overflow guard. (0x80-only
	// runs keep the accumulator at zero, so they would only ever hit EOF.)
	overflow := bytes.Repeat([]byte{0xFF}, 10)
	if _, err := readVarint(&byteCursor{b: overflow}); err == nil {
		t.Fatal("expected varint overflow")
	}
}

func TestByteCursor(t *testing.T) {
	c := &byteCursor{b: []byte{1, 2}}
	if c.remaining() != 2 {
		t.Fatalf("remaining=%d", c.remaining())
	}
	_, _ = c.ReadByte()
	_, _ = c.ReadByte()
	if _, err := c.ReadByte(); err == nil {
		t.Fatal("expected EOF")
	}
}

// --- code table ------------------------------------------------------------

func TestDefaultCodeTable(t *testing.T) {
	tbl := defaultCodeTable()
	// Spot-check the canonical layout (RFC 3284 §5.6).
	check := func(i int, want codeTableEntry) {
		if tbl[i] != want {
			t.Fatalf("entry %d = %+v, want %+v", i, tbl[i], want)
		}
	}
	check(0, codeTableEntry{instRUN, 0, 0, instNOOP, 0, 0})
	check(1, codeTableEntry{instADD, 0, 0, instNOOP, 0, 0})
	check(18, codeTableEntry{instADD, 17, 0, instNOOP, 0, 0})
	check(19, codeTableEntry{instCOPY, 0, 0, instNOOP, 0, 0})
	check(20, codeTableEntry{instCOPY, 4, 0, instNOOP, 0, 0})
	check(163, codeTableEntry{instADD, 1, 0, instCOPY, 4, 0})
	check(235, codeTableEntry{instADD, 1, 0, instCOPY, 4, 6})
	check(247, codeTableEntry{instCOPY, 4, 0, instADD, 1, 0})
	check(255, codeTableEntry{instCOPY, 4, 8, instADD, 1, 0})
	if got := opcodeCOPY0(3); got != 19+16*3 {
		t.Fatalf("opcodeCOPY0(3)=%d", got)
	}
}

// --- address cache ---------------------------------------------------------

func TestAddressCacheDecodeModes(t *testing.T) {
	c := newAddressCache(defaultNearCache, defaultSameCache)
	c.reset()
	// SELF
	if a, err := c.decode(0, modeSelf, &byteCursor{b: appendVarint(nil, 42)}); err != nil || a != 42 {
		t.Fatalf("self = (%d,%v)", a, err)
	}
	// HERE: addr = here - v
	if a, err := c.decode(100, modeHere, &byteCursor{b: appendVarint(nil, 10)}); err != nil || a != 90 {
		t.Fatalf("here = (%d,%v)", a, err)
	}
	// near[0] holds 42 (first update); near mode 2 reads near[0]+delta.
	if a, err := c.decode(0, 2, &byteCursor{b: appendVarint(nil, 5)}); err != nil || a != 47 {
		t.Fatalf("near = (%d,%v)", a, err)
	}
	// same: store an address, then read it back by its bucket byte.
	c.reset()
	c.update(300) // same[300%768]=300 at bucket 300/256=1, byte 44
	if a, err := c.decode(0, byte(2+defaultNearCache+1), &byteCursor{b: []byte{300 % 256}}); err != nil || a != 300 {
		t.Fatalf("same = (%d,%v)", a, err)
	}
}

func TestAddressCacheDecodeErrors(t *testing.T) {
	c := newAddressCache(defaultNearCache, defaultSameCache)
	for _, mode := range []byte{modeSelf, modeHere, 2, byte(2 + defaultNearCache)} {
		if _, err := c.decode(0, mode, &byteCursor{}); err == nil {
			t.Fatalf("mode %d: expected read error on empty input", mode)
		}
	}
}

func TestAddressCacheEncodeModes(t *testing.T) {
	c := newAddressCache(defaultNearCache, defaultSameCache)
	c.reset()

	// SELF wins for a fresh small address.
	if e := c.encode(1000, 5000); e.mode != modeSelf || e.val != 1000 {
		t.Fatalf("self: %+v", e)
	}
	// near[0]==1000 now; the same address costs delta 0 -> near mode 2.
	if e := c.encode(1000, 5000); e.mode != 2 || e.val != 0 {
		t.Fatalf("near: %+v", e)
	}
	// HERE wins when here-addr is tiny relative to a large address.
	c.reset()
	if e := c.encode(200000, 200001); e.mode != modeHere || e.val != 1 {
		t.Fatalf("here: %+v", e)
	}
	// addr < near[j] must skip that near candidate (no panic, falls back).
	c.reset()
	c.update(1000) // near[0]=1000
	if e := c.encode(500, 600); e.mode != modeHere {
		t.Fatalf("near-skip: %+v", e)
	}
}

// --- header ----------------------------------------------------------------

func TestReadHeaderErrors(t *testing.T) {
	cases := map[string][]byte{
		"short":         {0xD6, 0xC3},
		"bad-magic":     {0x00, 0x00, 0x00, 0x00, 0x00},
		"bad-version":   {0xD6, 0xC3, 0xC4, 0x99, 0x00},
		"no-indicator":  {0xD6, 0xC3, 0xC4, 0x00},
		"decompress":    {0xD6, 0xC3, 0xC4, 0x00, hdrDecompress},
		"codetable":     {0xD6, 0xC3, 0xC4, 0x00, hdrCodeTable},
		"unknown-bits":  {0xD6, 0xC3, 0xC4, 0x00, 0x08},
		"appheader-len": {0xD6, 0xC3, 0xC4, 0x00, hdrAppHeader}, // missing length
		"appheader-skip": append([]byte{0xD6, 0xC3, 0xC4, 0x00, hdrAppHeader},
			appendVarint(nil, 10)...), // claims 10 bytes, none follow
	}
	for name, d := range cases {
		if _, err := decodeBytes(t, nil, d); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
}

func TestDecodeAppHeaderAndVersionExt(t *testing.T) {
	// Header with an app header, version 0x53, then a window producing "Hi".
	d := []byte{0xD6, 0xC3, 0xC4, versionExt, hdrAppHeader}
	d = appendVarint(d, 3)
	d = append(d, "abc"...) // app header bytes (skipped)
	d = append(d, windowSpec{
		winInd:    0x00,
		targetLen: 2,
		data:      []byte("Hi"),
		inst:      []byte{opcodeADD0, 2},
	}.bytes()...)
	out, err := decodeBytes(t, nil, d)
	if err != nil || string(out) != "Hi" {
		t.Fatalf("decode = (%q, %v)", out, err)
	}
}

// --- decode: window framing & instruction errors ---------------------------

func TestDecodeWindowErrors(t *testing.T) {
	cs := adler32.Checksum([]byte("x"))
	bad := cs ^ 0xffffffff
	cases := map[string][]byte{
		"unsupported-winind": fullDelta(windowSpec{winInd: 0x08}),
		"both-src-tgt":       fullDelta(windowSpec{winInd: winSource | winTarget}),
		"segment-oor": fullDelta(windowSpec{
			winInd: winSource, seg: &[2]uint64{10, 0}, targetLen: 0,
		}),
		"delta-comp": fullDelta(windowSpec{winInd: 0x00, targetLen: 0, deltaInd: deltaCompMask}),
		"checksum-mismatch": fullDelta(windowSpec{
			winInd: winChecksum, targetLen: 1, checksum: &bad,
			data: []byte("x"), inst: []byte{opcodeADD0, 1},
		}),
		"add-past-end": fullDelta(windowSpec{
			winInd: 0x00, targetLen: 5, data: []byte("AB"), inst: []byte{opcodeADD0, 5},
		}),
		"run-past-end": fullDelta(windowSpec{
			winInd: 0x00, targetLen: 3, inst: []byte{opcodeRUN0, 3},
		}),
		"copy-addr-negative": fullDelta(windowSpec{
			winInd: 0x00, targetLen: 1, inst: []byte{opcodeCOPY0(modeHere), 1}, addr: []byte{5},
		}),
		"copy-addr-oor": fullDelta(windowSpec{
			winInd: 0x00, targetLen: 1, inst: []byte{opcodeCOPY0(modeSelf), 1}, addr: appendVarint(nil, 1000),
		}),
		"truncated-inst": fullDelta(windowSpec{
			winInd: 0x00, targetLen: 10, data: []byte("A"), inst: []byte{2},
		}),
		"inst-size-eof": fullDelta(windowSpec{
			winInd: 0x00, targetLen: 5, inst: []byte{opcodeADD0},
		}),
		"overrun": fullDelta(windowSpec{
			winInd: 0x00, targetLen: 5, data: []byte("AB"),
			inst: []byte{2 /*ADD1*/, 247 /*COPY4+ADD1*/}, addr: appendVarint(nil, 0),
		}),
		"not-consumed": fullDelta(windowSpec{
			winInd: 0x00, targetLen: 1, data: []byte("AB"), inst: []byte{2},
		}),
	}
	for name, d := range cases {
		if _, err := decodeBytes(t, nil, d); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
}

func TestDecodeChecksumOK(t *testing.T) {
	cs := adler32.Checksum([]byte("x"))
	d := fullDelta(windowSpec{
		winInd: winChecksum, targetLen: 1, checksum: &cs,
		data: []byte("x"), inst: []byte{opcodeADD0, 1},
	})
	if out, err := decodeBytes(t, nil, d); err != nil || string(out) != "x" {
		t.Fatalf("decode = (%q, %v)", out, err)
	}
}

func TestDecodeDoubleInstructionAndSizeBaked(t *testing.T) {
	// opcode 2  = ADD size 1 (size baked); opcode 247 = COPY size4 mode0 + ADD size1.
	// Produces "A" + "AAAA" (overlap COPY from addr 0) + "B" = "AAAAAB".
	d := fullDelta(windowSpec{
		winInd:    0x00,
		targetLen: 6,
		data:      []byte("AB"),
		inst:      []byte{2, 247},
		addr:      appendVarint(nil, 0),
	})
	out, err := decodeBytes(t, nil, d)
	if err != nil || string(out) != "AAAAAB" {
		t.Fatalf("decode = (%q, %v)", out, err)
	}
}

func TestDecodeCopyFromSourceAndRun(t *testing.T) {
	src := []byte("HELLO WORLD")
	cs := adler32.Checksum([]byte("HELLO WORLD!!!"))
	d := fullDelta(windowSpec{
		winInd:    winSource | winChecksum,
		seg:       &[2]uint64{uint64(len(src)), 0},
		targetLen: 14,
		checksum:  &cs,
		data:      []byte("!"),
		inst:      []byte{opcodeCOPY0(modeSelf), 11, opcodeRUN0, 3},
		addr:      appendVarint(nil, 0),
	})
	out, err := decodeBytes(t, src, d)
	if err != nil || string(out) != "HELLO WORLD!!!" {
		t.Fatalf("decode = (%q, %v)", out, err)
	}

	// Truncation sweep: every proper prefix must fail except the bare header.
	for i := 0; i < len(d); i++ {
		_, err := decodeBytes(t, src, d[:i])
		switch {
		case i == len(appendHeader(nil)): // header only -> zero windows, empty output
			if err != nil {
				t.Fatalf("prefix %d: unexpected error %v", i, err)
			}
		default:
			if err == nil {
				t.Fatalf("prefix %d: expected error", i)
			}
		}
	}
}

func TestDecodeCrossBoundaryCopy(t *testing.T) {
	// A COPY that starts in the source window (addr 3 of "HELLO") and extends
	// past its end into the target it is producing. Our encoder never emits a
	// boundary-crossing COPY, but the decoder must still apply it byte by byte:
	// source bytes "LO" then the freshly produced "LO" => "LOLO".
	src := []byte("HELLO")
	d := fullDelta(windowSpec{
		winInd:    winSource,
		seg:       &[2]uint64{uint64(len(src)), 0},
		targetLen: 4,
		inst:      []byte{opcodeCOPY0(modeSelf), 4},
		addr:      appendVarint(nil, 3),
	})
	out, err := decodeBytes(t, src, d)
	if err != nil || string(out) != "LOLO" {
		t.Fatalf("decode = (%q, %v)", out, err)
	}
}

func TestDecodeTargetWindowAndMultiWindow(t *testing.T) {
	// Window 1 (no source): ADD "HELLO". Window 2 (VCD_TARGET): COPY the 5 bytes
	// of produced target back, yielding "HELLOHELLO".
	d := fullDelta(
		windowSpec{winInd: 0x00, targetLen: 5, data: []byte("HELLO"), inst: []byte{opcodeADD0, 5}},
		windowSpec{
			winInd: winTarget, seg: &[2]uint64{5, 0}, targetLen: 5,
			inst: []byte{opcodeCOPY0(modeSelf), 5}, addr: appendVarint(nil, 0),
		},
	)
	out, err := decodeBytes(t, nil, d)
	if err != nil || string(out) != "HELLOHELLO" {
		t.Fatalf("decode = (%q, %v)", out, err)
	}
}

func TestDecodeStreamErrors(t *testing.T) {
	// Non-EOF error while reading the window indicator.
	if _, err := Decode(nil, &errReader{payload: appendHeader(nil)}, io.Discard); err == nil {
		t.Fatal("expected window-indicator read error")
	}
	// Writer fails on the window output.
	d := fullDelta(windowSpec{winInd: 0x00, targetLen: 2, data: []byte("Hi"), inst: []byte{opcodeADD0, 2}})
	if _, err := Decode(nil, bytes.NewReader(d), &failWriter{failAt: 0}); err == nil {
		t.Fatal("expected write error")
	}
}

// --- encode round-trips -----------------------------------------------------

func roundTrip(t *testing.T, source, target []byte) []byte {
	t.Helper()
	delta := EncodeBytes(source, target)
	out, err := decodeBytes(t, source, delta)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(out, target) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(out), len(target))
	}
	return delta
}

func TestEncodeRoundTrips(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	randBytes := func(n int) []byte {
		b := make([]byte, n)
		rng.Read(b)
		return b
	}
	base := randBytes(4096)
	edited := append(append([]byte{}, base[:1000]...), append([]byte("INSERTED CHUNK"), base[1000:]...)...)

	cases := []struct {
		name           string
		source, target []byte
	}{
		{"identical", base, base},
		{"edited", base, edited},
		{"unrelated", base, randBytes(2048)},
		{"no-source-random", nil, randBytes(2048)},
		{"no-source-run", nil, bytes.Repeat([]byte{'a'}, 500)},
		{"periodic", nil, bytes.Repeat([]byte("abcdef"), 300)},
		{"self-repeat", nil, append(append([]byte{}, randBytes(300)...), randBytes(300)...)},
		{"long-literal", nil, randBytes(300)},
		{"empty-target", base, nil},
		{"empty-both", nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { roundTrip(t, c.source, c.target) })
	}
}

func TestEncodeSelfRepeatUsesCopy(t *testing.T) {
	half := bytes.Repeat([]byte("ABCDEFGH"), 64) // 512 bytes
	target := append(append([]byte{}, half...), half...)
	delta := roundTrip(t, nil, target)
	if len(delta) >= len(target)/2 {
		t.Fatalf("delta %d not much smaller than target %d; COPY not used", len(delta), len(target))
	}
}

func TestDecodeTargetBulkCopy(t *testing.T) {
	// A novel block X repeated non-overlapping (separated by Y), with no source,
	// makes the second X decode as a bulk copy from already-produced target
	// (the a>=len(s) && a+size<=here branch in execInstruction).
	x := []byte("ABCDEFGHIJKLMNOP")
	y := []byte("0123456789abcdef")
	target := append(append(append([]byte{}, x...), y...), x...)
	// roundTrip asserts correct reconstruction; the point here is to exercise
	// the non-overlapping target-window bulk-copy path in the decoder.
	roundTrip(t, nil, target)
}

func TestEncoderReuse(t *testing.T) {
	enc := NewEncoder()
	rng := rand.New(rand.NewSource(11))
	mk := func(n int) (src, tgt []byte) {
		src = make([]byte, n)
		rng.Read(src)
		tgt = append([]byte(nil), src...)
		copy(tgt[n/4:n/4+8], []byte("EDITEDIT"))
		return
	}
	// First call allocates; the smaller follow-ups must reuse u/head/prev and
	// still produce exactly what a fresh encoder would.
	for _, n := range []int{8192, 512, 512} {
		src, tgt := mk(n)
		got := enc.EncodeBytes(src, tgt)
		if want := EncodeBytes(src, tgt); !bytes.Equal(got, want) {
			t.Fatalf("n=%d: reused-encoder output differs from a fresh encoder", n)
		}
		out, err := decodeBytes(t, src, got)
		if err != nil || !bytes.Equal(out, tgt) {
			t.Fatalf("n=%d: round-trip failed: %v", n, err)
		}
	}
}

func TestEncodeReusesSource(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	source := make([]byte, 8192)
	rng.Read(source)
	target := append(append([]byte{}, source...), []byte(" tail")...) // source + small suffix
	delta := roundTrip(t, source, target)
	if len(delta) >= len(target)/4 {
		t.Fatalf("delta %d not small; source not reused", len(delta))
	}
}

func TestEncodeMaxChain(t *testing.T) {
	// A highly repetitive source overfills a single hash bucket so findMatch hits
	// the maxChain cap; the round-trip must still be correct.
	source := bytes.Repeat([]byte("ABCD"), 200) // seed "ABCD" recurs 200x in source
	target := append(append([]byte{}, source...), []byte("ABCDtail")...)
	roundTrip(t, source, target)
}

func TestEncodeWriteError(t *testing.T) {
	if err := Encode(nil, []byte("data"), &failWriter{failAt: 0}); err == nil {
		t.Fatal("expected write error")
	}
}
