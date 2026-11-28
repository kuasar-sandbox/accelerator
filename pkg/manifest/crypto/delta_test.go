package crypto

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/rand"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/delta"
)

func deltaRand(seed int64, n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func deltaMutate(b []byte, seed int64, edits, editLen int) []byte {
	out := append([]byte(nil), b...)
	r := rand.New(rand.NewSource(seed))
	for i := 0; i < edits; i++ {
		at := r.Intn(len(out) - editLen)
		r.Read(out[at : at+editLen])
	}
	return out
}

type deltaFixture struct {
	salt         [32]byte
	base, target []byte
	baseObject   []byte
	baseHash     [32]byte
	baseKey      [32]byte
	stream       []byte
	object       []byte
	hash, key    [32]byte
}

func newDeltaFixture(t *testing.T) *deltaFixture {
	t.Helper()
	f := &deltaFixture{salt: sha256.Sum256([]byte("salt"))}
	f.base = deltaRand(1, 128<<10)
	f.target = deltaMutate(f.base, 2, 4, 200)
	enc := &AESChunkEncryptor{}
	var err error
	f.baseObject, f.baseHash, f.baseKey, err = enc.EncryptChunk(context.Background(), f.salt, f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.stream = delta.Encode(f.base, f.target)
	f.object, f.hash, f.key, err = enc.EncryptDeltaChunk(context.Background(), f.salt, f.deltaBase(), f.target, f.stream)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *deltaFixture) deltaBase() DeltaBase {
	return DeltaBase{ObjectKey: f.baseHash, Key: f.baseKey, PlaintextSize: len(f.base)}
}

// TestDeltaObjectLayout checks the cleartext header of a delta object: format
// byte 0x03, the base object key, the three size fields, and RAW payload
// encoding for an incompressible stream. The object must be exactly
// DeltaOverhead (78) bytes plus the stream, hash to its address, and leak
// neither K_base nor the stream in the clear.
func TestDeltaObjectLayout(t *testing.T) {
	f := newDeltaFixture(t)
	o := f.object
	if ChunkFormatAESDelta != 0x03 || o[0] != 0x03 {
		t.Fatalf("format byte 0x%02x", o[0])
	}
	if !bytes.Equal(o[1:33], f.baseHash[:]) {
		t.Fatal("base object key must be recorded in cleartext")
	}
	if binary.LittleEndian.Uint32(o[33:37]) != uint32(len(f.base)) ||
		binary.LittleEndian.Uint32(o[37:41]) != uint32(len(f.target)) ||
		binary.LittleEndian.Uint32(o[41:45]) != uint32(len(f.stream)) {
		t.Fatal("size fields wrong")
	}
	if o[45] != deltaEncRaw {
		t.Fatalf("a stream of random edits is incompressible; expected RAW, got %d", o[45])
	}
	if len(o) != DeltaOverhead+len(f.stream) || DeltaOverhead != 78 {
		t.Fatalf("object %d bytes, stream %d, overhead const %d", len(o), len(f.stream), DeltaOverhead)
	}
	if f.hash != sha256.Sum256(o) {
		t.Fatal("hash must be SHA-256 of the whole physical object")
	}
	if bytes.Contains(o, f.baseKey[:]) {
		t.Fatal("K_base must not appear in clear")
	}
	if bytes.Contains(o, f.stream[len(f.stream)/2:len(f.stream)/2+64]) {
		t.Fatal("the stream must not appear in clear")
	}
}

// TestDeltaRoundTripRAW opens a RAW delta object with its delta key and checks
// the recovered header, base key and stream. It then does what the reader does:
// decrypts the base with the recovered K_base and applies the stream to get the
// target back.
func TestDeltaRoundTripRAW(t *testing.T) {
	f := newDeltaFixture(t)
	opened, err := (&AESChunkEncryptor{}).OpenDeltaChunk(context.Background(), f.key, f.object)
	if err != nil {
		t.Fatal(err)
	}
	if opened.BaseKey != f.baseKey {
		t.Fatal("wrapped K_base mismatch")
	}
	if opened.Header.BaseObjectKey != f.baseHash || opened.Header.TargetSize != uint32(len(f.target)) || opened.Header.Snappy {
		t.Fatalf("header %+v", opened.Header)
	}
	if !bytes.Equal(opened.Stream, f.stream) {
		t.Fatal("stream mismatch")
	}
	// Close the loop the way the reader will: decrypt the base with the
	// recovered K_base, then apply.
	base := make([]byte, opened.Header.BaseSize)
	if err := (&AESChunkEncryptor{}).DecryptChunkTo(context.Background(), opened.BaseKey, f.baseObject, base); err != nil {
		t.Fatal(err)
	}
	got, err := delta.Apply(base, opened.Stream, int(opened.Header.TargetSize))
	if err != nil || !bytes.Equal(got, f.target) {
		t.Fatalf("reconstruction failed: %v", err)
	}
}

// TestDeltaRoundTripSnappyPayload checks the Snappy branch. It uses a repetitive
// stream, because a real VCDIFF stream of random edits would not compress. The
// object must be marked Snappy and be smaller than the same stream stored RAW,
// must round-trip, and must record the decoded stream size in its header. A
// corrupted payload must not decode back to the original stream.
func TestDeltaRoundTripSnappyPayload(t *testing.T) {
	salt := sha256.Sum256([]byte("s"))
	base := deltaRand(1, 64<<10)
	target := deltaRand(2, 128<<10)
	enc := &AESChunkEncryptor{}
	_, baseHash, baseKey, _ := enc.EncryptChunk(context.Background(), salt, base)
	b := DeltaBase{ObjectKey: baseHash, Key: baseKey, PlaintextSize: len(base)}

	// The crypto layer treats the stream as opaque bytes. A real VCDIFF stream
	// is rarely Snappy-compressible (its literals are the incompressible
	// remainder, and its copies already removed the redundancy), so exercise the
	// Snappy branch with a stream that is.
	stream := []byte(strings.Repeat("the quick brown fox jumps over the lazy dog. ", 1000))
	object, _, key, err := enc.EncryptDeltaChunk(context.Background(), salt, b, target, stream)
	if err != nil {
		t.Fatal(err)
	}
	if object[45] != deltaEncSnappy {
		t.Fatalf("expected Snappy payload encoding, got %d (stream %d, object %d)", object[45], len(stream), len(object))
	}
	if len(object) >= DeltaOverhead+len(stream) {
		t.Fatal("Snappy must shrink the object")
	}
	opened, err := enc.OpenDeltaChunk(context.Background(), key, object)
	if err != nil {
		t.Fatal(err)
	}
	if !opened.Header.Snappy || !bytes.Equal(opened.Stream, stream) || opened.BaseKey != baseKey {
		t.Fatal("Snappy payload did not round trip")
	}
	if opened.Header.StreamSize != uint32(len(stream)) {
		t.Fatal("header must record the decoded stream size, not the payload size")
	}

	// A corrupted Snappy payload must be rejected, not misdecoded.
	bad := append([]byte(nil), object...)
	for i := deltaPayloadStart; i < len(bad); i += 7 {
		bad[i] ^= 0xff
	}
	if o, err := enc.OpenDeltaChunk(context.Background(), key, bad); err == nil && bytes.Equal(o.Stream, stream) {
		t.Fatal("corruption went unnoticed")
	}
}

// TestDeltaKeyDerivation checks K_d. It must differ from the target's ordinary
// convergent key, and must change when the base, the salt or the target changes.
// Encrypting the same inputs again must give a byte-identical object, hash and key.
func TestDeltaKeyDerivation(t *testing.T) {
	f := newDeltaFixture(t)
	if f.key == DeriveKey(f.salt, f.target) {
		t.Fatal("K_d must differ from the target's convergent key")
	}
	if f.key != DeriveDeltaKey(f.salt, f.baseHash, f.target) {
		t.Fatal("returned key is not K_d")
	}
	var otherBase [32]byte
	otherBase[0] = 1
	if DeriveDeltaKey(f.salt, otherBase, f.target) == f.key {
		t.Fatal("K_d must be bound to the base, otherwise one target under two bases reuses a keystream")
	}
	var otherSalt [32]byte
	if DeriveDeltaKey(otherSalt, f.baseHash, f.target) == f.key {
		t.Fatal("K_d must be bound to the salt")
	}
	if DeriveDeltaKey(f.salt, f.baseHash, f.base) == f.key {
		t.Fatal("K_d must be bound to the target")
	}
	// Deterministic end to end: same inputs, byte-identical object.
	o2, h2, k2, err := (&AESChunkEncryptor{}).EncryptDeltaChunk(context.Background(), f.salt, f.deltaBase(), f.target, f.stream)
	if err != nil || !bytes.Equal(o2, f.object) || h2 != f.hash || k2 != f.key {
		t.Fatal("delta encryption must be deterministic (it fixes the object's address)")
	}
}

// TestDeltaSameTargetTwoBasesDoesNotReuseKeystream covers the failure the
// base-bound key exists to prevent. The same target stored against two different
// bases must get different keys, and the first 32 keystream bytes under each must
// differ. Otherwise the two objects would share a keystream.
func TestDeltaSameTargetTwoBasesDoesNotReuseKeystream(t *testing.T) {
	f := newDeltaFixture(t)
	enc := &AESChunkEncryptor{}
	base2 := deltaMutate(f.base, 99, 2, 100)
	_, h2, k2, _ := enc.EncryptChunk(context.Background(), f.salt, base2)
	o2, _, key2, err := enc.EncryptDeltaChunk(context.Background(), f.salt,
		DeltaBase{ObjectKey: h2, Key: k2, PlaintextSize: len(base2)}, f.target, delta.Encode(base2, f.target))
	if err != nil {
		t.Fatal(err)
	}
	if key2 == f.key {
		t.Fatal("two bases produced the same K_d")
	}
	// Recover both keystreams over the first 32 bytes (the wrapped K_base slot)
	// by XORing known plaintext; they must differ.
	ks := func(object []byte, knownKey [32]byte) []byte {
		out := make([]byte, 32)
		for i := range out {
			out[i] = object[deltaHeaderSize+i] ^ knownKey[i]
		}
		return out
	}
	if bytes.Equal(ks(f.object, f.baseKey), ks(o2, k2)) {
		t.Fatal("identical keystream under two bases")
	}
}

// TestDeltaWrongKeyDoesNotReproduceTarget opens a delta object with the wrong key.
// That must not recover the real base key, and applying whatever stream comes out
// to the real base must not reproduce the target.
func TestDeltaWrongKeyDoesNotReproduceTarget(t *testing.T) {
	f := newDeltaFixture(t)
	var wrong [32]byte
	wrong[0] = 1
	opened, err := (&AESChunkEncryptor{}).OpenDeltaChunk(context.Background(), wrong, f.object)
	if err == nil {
		if opened.BaseKey == f.baseKey {
			t.Fatal("wrong key recovered K_base")
		}
		if got, aerr := delta.Apply(f.base, opened.Stream, len(f.target)); aerr == nil && bytes.Equal(got, f.target) {
			t.Fatal("wrong key reproduced the target")
		}
	}
}

// TestParseDeltaHeaderRejects lists object headers a conforming writer would never
// emit: truncated or padded objects, a wrong format byte, an unknown payload
// encoding, zero or oversized sizes, a RAW length mismatch, and a delta that saves
// less than 4 KiB. ParseDeltaHeader and OpenDeltaChunk must reject each one, and
// the genuine object must still parse.
func TestParseDeltaHeaderRejects(t *testing.T) {
	f := newDeltaFixture(t)
	mutate := func(fn func(o []byte) []byte) []byte { return fn(append([]byte(nil), f.object...)) }
	put := func(off int, v uint32) func([]byte) []byte {
		return func(o []byte) []byte { binary.LittleEndian.PutUint32(o[off:], v); return o }
	}
	cases := map[string][]byte{
		"empty":                 nil,
		"shorter than header":   f.object[:40],
		"header only":           f.object[:deltaPayloadStart],
		"wrong format byte":     mutate(func(o []byte) []byte { o[0] = ChunkFormatAESRaw; return o }),
		"unknown encoding":      mutate(func(o []byte) []byte { o[45] = 2; return o }),
		"zero base size":        mutate(put(33, 0)),
		"huge base size":        mutate(put(33, MaxChunkDecodedSize+1)),
		"zero target size":      mutate(put(37, 0)),
		"huge target size":      mutate(put(37, MaxChunkDecodedSize+1)),
		"zero stream size":      mutate(put(41, 0)),
		"stream size too large": mutate(put(41, uint32(len(f.base)+len(f.target)+maxDeltaStreamSlack+1))),
		"RAW length mismatch":   mutate(put(41, uint32(len(f.stream)-1))),
		"truncated payload":     f.object[:len(f.object)-1],
		"trailing byte":         append(append([]byte(nil), f.object...), 0),
		"saves 4095 not 4096":   mutate(put(37, uint32(len(f.object)+4094))), // plausibility needs target+1-object >= 4096
	}
	for name, o := range cases {
		if _, err := ParseDeltaHeader(o); err == nil {
			t.Errorf("%s: expected rejection", name)
		}
		if _, err := (&AESChunkEncryptor{}).OpenDeltaChunk(context.Background(), f.key, o); err == nil {
			t.Errorf("%s: OpenDeltaChunk must reject too", name)
		}
	}
	if _, err := ParseDeltaHeader(f.object); err != nil {
		t.Fatalf("the genuine object must parse: %v", err)
	}
}

// TestParseDeltaHeaderRejectsNonCanonicalSnappy sets the Snappy encoding on a
// payload no shorter than the stream. A conforming writer would never choose
// Snappy there, so the header must be rejected.
func TestParseDeltaHeaderRejectsNonCanonicalSnappy(t *testing.T) {
	f := newDeltaFixture(t)
	// Claim Snappy for a payload that is the same length as the stream: that
	// saves nothing, so no conforming writer would have chosen Snappy.
	o := append([]byte(nil), f.object...)
	o[45] = deltaEncSnappy
	if _, err := ParseDeltaHeader(o); err == nil {
		t.Fatal("non-canonical Snappy payload accepted")
	}
}

// TestEncryptDeltaChunkRejectsBadInputs checks the writer side of the same rules.
// An empty target or stream, a zero base size, an oversized stream, and a delta
// that cannot save 4 KiB must return ErrDeltaNotApplicable. A canceled context
// must stop both encrypt and open.
func TestEncryptDeltaChunkRejectsBadInputs(t *testing.T) {
	f := newDeltaFixture(t)
	enc := &AESChunkEncryptor{}
	ctx := context.Background()
	b := f.deltaBase()
	if _, _, _, err := enc.EncryptDeltaChunk(ctx, f.salt, b, nil, f.stream); !errors.Is(err, ErrDeltaNotApplicable) {
		t.Errorf("empty target: %v", err)
	}
	if _, _, _, err := enc.EncryptDeltaChunk(ctx, f.salt, b, f.target, nil); !errors.Is(err, ErrDeltaNotApplicable) {
		t.Errorf("empty stream: %v", err)
	}
	if _, _, _, err := enc.EncryptDeltaChunk(ctx, f.salt, DeltaBase{ObjectKey: b.ObjectKey}, f.target, f.stream); !errors.Is(err, ErrDeltaNotApplicable) {
		t.Errorf("zero base size: %v", err)
	}
	if _, _, _, err := enc.EncryptDeltaChunk(ctx, f.salt, b, f.target, make([]byte, len(f.base)+len(f.target)+maxDeltaStreamSlack+1)); !errors.Is(err, ErrDeltaNotApplicable) {
		t.Errorf("oversize stream: %v", err)
	}
	// A "delta" that saves under 4 KiB cannot satisfy the reader's plausibility
	// bound, and the writer refuses to emit what its own reader rejects.
	small := f.target[:4000] // 4001-byte raw object minus a ~100-byte delta saves < 4 KiB
	if _, _, _, err := enc.EncryptDeltaChunk(ctx, f.salt, b, small, delta.Encode(f.base, small)); !errors.Is(err, ErrDeltaNotApplicable) {
		t.Errorf("implausible object: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, _, err := enc.EncryptDeltaChunk(cctx, f.salt, b, f.target, f.stream); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled ctx: %v", err)
	}
	if _, err := enc.OpenDeltaChunk(cctx, f.key, f.object); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled ctx on open: %v", err)
	}
}

// TestEncryptDeltaChunkDoesNotMutateInputs checks that encrypting a delta leaves
// the caller's target and stream bytes unchanged.
func TestEncryptDeltaChunkDoesNotMutateInputs(t *testing.T) {
	f := newDeltaFixture(t)
	target := append([]byte(nil), f.target...)
	stream := append([]byte(nil), f.stream...)
	if _, _, _, err := (&AESChunkEncryptor{}).EncryptDeltaChunk(context.Background(), f.salt, f.deltaBase(), target, stream); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(target, f.target) || !bytes.Equal(stream, f.stream) {
		t.Fatal("inputs were modified")
	}
}

// TestOrdinaryDecryptRejectsDeltaObject checks that the ordinary chunk decryptor
// refuses a delta object with "unknown chunk format 0x03" rather than misdecoding
// it. Until the read path understands 0x03 (phase 2), an old or other reader must
// fail loudly.
func TestOrdinaryDecryptRejectsDeltaObject(t *testing.T) {
	f := newDeltaFixture(t)
	dst := make([]byte, len(f.target))
	err := (&AESChunkEncryptor{}).DecryptChunkTo(context.Background(), f.key, f.object, dst)
	if err == nil || !strings.Contains(err.Error(), "unknown chunk format 0x03") {
		t.Fatalf("got %v", err)
	}
}

// TestCodecImplementsDeltaInterfaces checks that the AES codec returned by New
// implements DeltaEncryptor and DeltaDecryptor, and that encrypting and opening
// through those interfaces matches the fixture.
func TestCodecImplementsDeltaInterfaces(t *testing.T) {
	enc, dec, err := New(Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	de, ok := enc.(DeltaEncryptor)
	if !ok {
		t.Fatal("codec must implement DeltaEncryptor")
	}
	dd, ok := dec.(DeltaDecryptor)
	if !ok {
		t.Fatal("codec must implement DeltaDecryptor")
	}
	f := newDeltaFixture(t)
	obj, _, key, err := de.EncryptDeltaChunk(context.Background(), f.salt, f.deltaBase(), f.target, f.stream)
	if err != nil || !bytes.Equal(obj, f.object) {
		t.Fatalf("codec delta encrypt: %v", err)
	}
	if o, err := dd.OpenDeltaChunk(context.Background(), key, obj); err != nil || !bytes.Equal(o.Stream, f.stream) {
		t.Fatalf("codec delta open: %v", err)
	}
}
