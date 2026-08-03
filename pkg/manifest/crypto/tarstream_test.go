package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

func decodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestRFC5297AppendixA1(t *testing.T) {
	key := decodeHex(t, "fffefdfcfbfaf9f8f7f6f5f4f3f2f1f0f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff")
	ad := decodeHex(t, "101112131415161718191a1b1c1d1e1f2021222324252627")
	plaintext := decodeHex(t, "112233445566778899aabbccddee")
	wantSIV := decodeHex(t, "85632d07c6e8f37f950acd320a2ecc93")
	wantCiphertext := decodeHex(t, "40c02b9690c4dc04daef7f6afe5c")

	macBlock, err := aes.NewCipher(key[:16])
	if err != nil {
		t.Fatal(err)
	}
	ctrBlock, err := aes.NewCipher(key[16:])
	if err != nil {
		t.Fatal(err)
	}
	siv := s2v(macBlock, ad, plaintext)
	if !bytes.Equal(siv[:], wantSIV) {
		t.Fatalf("S2V = %x, want %x", siv, wantSIV)
	}
	iv := maskedCTRIV(siv)
	got := make([]byte, len(plaintext))
	cipher.NewCTR(ctrBlock, iv[:]).XORKeyStream(got, plaintext)
	if !bytes.Equal(got, wantCiphertext) {
		t.Fatalf("CTR ciphertext = %x, want %x", got, wantCiphertext)
	}
}

func TestRFC5297AppendixA2(t *testing.T) {
	key := decodeHex(t, "7f7e7d7c7b7a79787776757473727170404142434445464748494a4b4c4d4e4f")
	ad1 := decodeHex(t, "00112233445566778899aabbccddeeffdeaddadadeaddadaffeeddccbbaa99887766554433221100")
	ad2 := decodeHex(t, "102030405060708090a0")
	nonce := decodeHex(t, "09f911029d74e35bd84156c5635688c0")
	plaintext := decodeHex(t, "7468697320697320736f6d6520706c61696e7465787420746f20656e6372797074207573696e67205349562d414553")
	wantSIV := decodeHex(t, "7bdb6e3b432667eb06f4d14bff2fbd0f")
	wantCiphertext := decodeHex(t, "cb900f2fddbe404326601965c889bf17dba77ceb094fa663b7a3f748ba8af829ea64ad544a272e9c485b62a3fd5c0d")

	macBlock, err := aes.NewCipher(key[:16])
	if err != nil {
		t.Fatal(err)
	}
	ctrBlock, err := aes.NewCipher(key[16:])
	if err != nil {
		t.Fatal(err)
	}
	siv := s2v(macBlock, ad1, ad2, nonce, plaintext)
	if !bytes.Equal(siv[:], wantSIV) {
		t.Fatalf("S2V = %x, want %x", siv, wantSIV)
	}
	iv := maskedCTRIV(siv)
	got := make([]byte, len(plaintext))
	cipher.NewCTR(ctrBlock, iv[:]).XORKeyStream(got, plaintext)
	if !bytes.Equal(got, wantCiphertext) {
		t.Fatalf("CTR ciphertext = %x, want %x", got, wantCiphertext)
	}
}

func TestRFC4493CMACVectors(t *testing.T) {
	key := decodeHex(t, "2b7e151628aed2a6abf7158809cf4f3c")
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	messages := []string{
		"",
		"6bc1bee22e409f96e93d7e117393172a",
		"6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e5130c81c46a35ce411",
		"6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e5130c81c46a35ce411e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710",
	}
	tags := []string{
		"bb1d6929e95937287fa37d129b756746",
		"070a16b46b4d4144f79bdd9dd04a287c",
		"dfa66747de9ae63030ca32611497c827",
		"51f0bebf7e3b9d92fc49741779363cfe",
	}
	for i := range messages {
		got := cmacSum(block, decodeHex(t, messages[i]))
		want := decodeHex(t, tags[i])
		if !bytes.Equal(got[:], want) {
			t.Fatalf("CMAC vector %d = %x, want %x", i, got, want)
		}
	}
}

func TestAESSIVCTRMask(t *testing.T) {
	siv := [aes.BlockSize]byte{}
	for i := range siv {
		siv[i] = 0xff
	}
	masked := maskedCTRIV(siv)
	for i := range masked {
		want := byte(0xff)
		if i == 8 || i == 12 {
			want = 0x7f
		}
		if masked[i] != want {
			t.Fatalf("masked IV byte %d = 0x%02x, want 0x%02x", i, masked[i], want)
		}
	}
}

func TestTarStreamCodecRoundTripAndBuffers(t *testing.T) {
	var key [32]byte
	for i := range key {
		key[i] = byte(i)
	}
	codec, err := NewTarStreamCodec(key)
	if err != nil {
		t.Fatal(err)
	}
	plaintext := bytes.Repeat([]byte("record"), 683)
	aad := []byte("authenticated geometry")
	prefix := []byte("keep:")
	dst := make([]byte, len(prefix), len(prefix)+codec.CiphertextSize(len(plaintext)))
	copy(dst, prefix)
	before := &dst[0]
	sealed, err := codec.Encrypt(dst, plaintext, aad)
	if err != nil {
		t.Fatal(err)
	}
	if &sealed[0] != before {
		t.Fatal("Encrypt did not reuse append destination capacity")
	}
	if !bytes.Equal(sealed[:len(prefix)], prefix) {
		t.Fatal("Encrypt modified destination prefix")
	}
	record := sealed[len(prefix):]
	bodyStart := &record[aesSIVOverhead]
	opened, err := codec.DecryptInPlace(record, aad)
	if err != nil {
		t.Fatal(err)
	}
	if len(opened) > 0 && &opened[0] != bodyStart {
		t.Fatal("DecryptInPlace did not reuse ciphertext backing array")
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatal("round-trip plaintext mismatch")
	}
}

func TestTarStreamCodecGolden(t *testing.T) {
	var key [32]byte
	for i := range key {
		key[i] = byte(i)
	}
	codec, err := NewTarStreamCodec(key)
	if err != nil {
		t.Fatal(err)
	}
	plaintext := decodeHex(t, "00112233445566778899aabbccddeeff1020304050607080")
	sealed, err := codec.Encrypt(nil, plaintext, []byte("kuasar-golden-aad"))
	if err != nil {
		t.Fatal(err)
	}
	wantSealed := decodeHex(t, "013b960dd1364682b2894cfa12ea95eb2b9ffeb76642214a3b2c3429ab7a99e22fcbdaafb578179492")
	if !bytes.Equal(sealed, wantSealed) {
		t.Fatalf("sealed record = %x, want %x", sealed, wantSealed)
	}
	var plainDigest [32]byte
	for i := range plainDigest {
		plainDigest[i] = byte(0xff - i)
	}
	if got, want := codec.KeyedDigest(plainDigest), decodeHex(t, "e859e9ccf8c926d30dbdd7e9f1a1256dd2ba046b67bef092a70347167be574fd"); !bytes.Equal(got[:], want) {
		t.Fatalf("keyed digest = %x, want %x", got, want)
	}
}

func TestTarStreamCodecAuthenticationFailureClearsPlaintext(t *testing.T) {
	codec, err := NewTarStreamCodec([32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := codec.Encrypt(nil, []byte("secret plaintext"), []byte("aad"))
	if err != nil {
		t.Fatal(err)
	}
	sealed[1] ^= 0x80
	if plaintext, err := codec.DecryptInPlace(sealed, []byte("aad")); plaintext != nil || !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("DecryptInPlace tamper = (%x, %v)", plaintext, err)
	}
	if !bytes.Equal(sealed[aesSIVOverhead:], make([]byte, len(sealed)-aesSIVOverhead)) {
		t.Fatal("tentative plaintext retained after authentication failure")
	}
}

func TestTarStreamCodecWrongKeyAndKeyedDigest(t *testing.T) {
	key := [32]byte{1, 2, 3}
	codec, _ := NewTarStreamCodec(key)
	other, _ := NewTarStreamCodec([32]byte{4, 5, 6})
	sealed, err := codec.Encrypt(nil, []byte("body"), []byte("aad"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.DecryptInPlace(append([]byte(nil), sealed...), []byte("aad")); !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("wrong-key error = %v", err)
	}
	plainDigest := sha256.Sum256([]byte("canonical plaintext"))
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(plainDigest[:])
	if got := codec.KeyedDigest(plainDigest); !bytes.Equal(got[:], mac.Sum(nil)) {
		t.Fatalf("KeyedDigest = %x, want %x", got, mac.Sum(nil))
	}
}

func TestTarStreamCodecConcurrent(t *testing.T) {
	codec, _ := NewTarStreamCodec([32]byte{9})
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			plaintext := bytes.Repeat([]byte{byte(i)}, 17+i*31)
			aad := []byte{byte(i), byte(i >> 8)}
			sealed, err := codec.Encrypt(nil, plaintext, aad)
			if err != nil {
				errs <- err
				return
			}
			opened, err := codec.DecryptInPlace(sealed, aad)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(opened, plaintext) {
				errs <- errors.New("plaintext mismatch")
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func FuzzTarStreamCodecRoundTrip(f *testing.F) {
	f.Add([]byte("plaintext"), []byte("aad"))
	f.Add([]byte(nil), []byte(nil))
	codec, _ := NewTarStreamCodec([32]byte{0x42})
	f.Fuzz(func(t *testing.T, plaintext, aad []byte) {
		if len(plaintext) > 1<<16 || len(aad) > 1<<16 {
			t.Skip()
		}
		original := append([]byte(nil), plaintext...)
		sealed, err := codec.Encrypt(nil, plaintext, aad)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(plaintext, original) {
			t.Fatal("Encrypt modified plaintext")
		}
		opened, err := codec.DecryptInPlace(sealed, aad)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(opened, original) {
			t.Fatal("round-trip mismatch")
		}
	})
}
