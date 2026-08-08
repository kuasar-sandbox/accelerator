package crypto

import (
	"bytes"
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

func fixedRecordCodec(t *testing.T, key, salt [32]byte) tarstream.RecordCodec {
	t.Helper()
	codec, err := NewTarStreamCodec(key)
	if err != nil {
		t.Fatal(err)
	}
	records, err := codec.BindArtifact(salt)
	if err != nil {
		t.Fatal(err)
	}
	return records
}

func TestTarStreamCodecGolden(t *testing.T) {
	var key, salt [32]byte
	for i := range key {
		key[i] = byte(i)
		salt[i] = byte(0xff - i)
	}
	records := fixedRecordCodec(t, key, salt)
	plaintext := decodeHex(t, "00112233445566778899aabbccddeeff1020304050607080")
	sealed, err := records.Encrypt(nil, plaintext, []byte("kuasar-golden-aad"), 0x0102030405060708)
	if err != nil {
		t.Fatal(err)
	}
	want := decodeHex(t, "01fd39f9b96738900026cd157da1b5a43447a3803f6430153ffaa8325892f7efcc93a4e40052592a57")
	if !bytes.Equal(sealed, want) {
		t.Fatalf("sealed record = %x, want %x", sealed, want)
	}
	opened, err := records.DecryptInPlace(sealed, []byte("kuasar-golden-aad"), 0x0102030405060708)
	if err != nil || !bytes.Equal(opened, plaintext) {
		t.Fatalf("golden round trip = %x, %v", opened, err)
	}
}

func TestTarStreamCodecRoundTripAndBuffers(t *testing.T) {
	records := fixedRecordCodec(t, [32]byte{1}, [32]byte{2})
	plaintext := bytes.Repeat([]byte("record"), 683)
	aad := []byte("authenticated geometry")
	prefix := []byte("keep:")
	dst := make([]byte, len(prefix), len(prefix)+records.CiphertextSize(len(plaintext)))
	copy(dst, prefix)
	before := &dst[0]
	sealed, err := records.Encrypt(dst, plaintext, aad, 7)
	if err != nil {
		t.Fatal(err)
	}
	if &sealed[0] != before || !bytes.Equal(sealed[:len(prefix)], prefix) {
		t.Fatal("Encrypt did not preserve and reuse destination prefix")
	}
	record := sealed[len(prefix):]
	bodyStart := &record[1]
	opened, err := records.DecryptInPlace(record, aad, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(opened) > 0 && &opened[0] != bodyStart {
		t.Fatal("DecryptInPlace did not use record[1:] backing storage")
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatal("round-trip plaintext mismatch")
	}
}

func TestTarStreamCodecEncryptPreservesAliasedInputs(t *testing.T) {
	records := fixedRecordCodec(t, [32]byte{0x44}, [32]byte{0x55})
	for _, aliasPlaintext := range []bool{true, false} {
		name := "aad"
		if aliasPlaintext {
			name = "plaintext"
		}
		t.Run(name, func(t *testing.T) {
			const inputSize = 64
			backing := make([]byte, inputSize, inputSize+records.CiphertextSize(inputSize))
			for i := range backing {
				backing[i] = byte(i + 1)
			}
			plain, aad := []byte("independent plaintext"), []byte("independent associated data")
			if aliasPlaintext {
				plain = backing
			} else {
				aad = backing
			}
			wantPlain, wantAAD := append([]byte(nil), plain...), append([]byte(nil), aad...)
			sealed, err := records.Encrypt(backing[:0], plain, aad, 9)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(plain, wantPlain) || !bytes.Equal(aad, wantAAD) {
				t.Fatal("Encrypt modified an aliased input")
			}
			opened, err := records.DecryptInPlace(sealed, wantAAD, 9)
			if err != nil || !bytes.Equal(opened, wantPlain) {
				t.Fatalf("aliased round trip = %x, %v", opened, err)
			}
		})
	}
}

func TestTarStreamCodecAuthenticationFailureClearsPlaintext(t *testing.T) {
	records := fixedRecordCodec(t, [32]byte{1}, [32]byte{2})
	sealed, err := records.Encrypt(nil, []byte("secret plaintext"), []byte("aad"), 3)
	if err != nil {
		t.Fatal(err)
	}
	sealed[1] ^= 0x80
	if plaintext, err := records.DecryptInPlace(sealed, []byte("aad"), 3); plaintext != nil || !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("DecryptInPlace tamper = (%x, %v)", plaintext, err)
	}
	plainSize := len(sealed) - aesGCMOverhead
	if !bytes.Equal(sealed[1:1+plainSize], make([]byte, plainSize)) {
		t.Fatal("tentative plaintext retained after authentication failure")
	}
}

func TestTarStreamCodecAuthenticationInputs(t *testing.T) {
	key, salt := [32]byte{1, 2, 3}, [32]byte{7, 8, 9}
	records := fixedRecordCodec(t, key, salt)
	sealed, err := records.Encrypt(nil, []byte("body"), []byte("aad"), 11)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		rc   tarstream.RecordCodec
		aad  []byte
		seq  uint64
	}{
		{"wrong-key", fixedRecordCodec(t, [32]byte{4}, salt), []byte("aad"), 11},
		{"wrong-salt", fixedRecordCodec(t, key, [32]byte{8}), []byte("aad"), 11},
		{"wrong-aad", records, []byte("bad"), 11},
		{"wrong-sequence", records, []byte("aad"), 12},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			copyOfSealed := append([]byte(nil), sealed...)
			if _, err := tc.rc.DecryptInPlace(copyOfSealed, tc.aad, tc.seq); !errors.Is(err, tarstream.ErrAuthentication) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestTarStreamCodecKeyedDigestUnchanged(t *testing.T) {
	key := [32]byte{1, 2, 3}
	codec, _ := NewTarStreamCodec(key)
	plainDigest := sha256.Sum256([]byte("canonical plaintext"))
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(plainDigest[:])
	if got := codec.KeyedDigest(plainDigest); !bytes.Equal(got[:], mac.Sum(nil)) {
		t.Fatalf("KeyedDigest = %x, want %x", got, mac.Sum(nil))
	}
}

func TestTarStreamCodecConcurrent(t *testing.T) {
	records := fixedRecordCodec(t, [32]byte{9}, [32]byte{10})
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			plaintext := bytes.Repeat([]byte{byte(i)}, 17+i*31)
			aad := []byte{byte(i), byte(i >> 8)}
			sealed, err := records.Encrypt(nil, plaintext, aad, uint64(i+1))
			if err == nil {
				var opened []byte
				opened, err = records.DecryptInPlace(sealed, aad, uint64(i+1))
				if err == nil && !bytes.Equal(opened, plaintext) {
					err = errors.New("plaintext mismatch")
				}
			}
			if err != nil {
				errs <- err
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
	f.Add([]byte("plaintext"), []byte("aad"), uint64(1))
	f.Add([]byte(nil), []byte(nil), uint64(0))
	codec, _ := NewTarStreamCodec([32]byte{0x42})
	records, _ := codec.BindArtifact([32]byte{0x43})
	f.Fuzz(func(t *testing.T, plaintext, aad []byte, sequence uint64) {
		if len(plaintext) > 1<<16 || len(aad) > 1<<16 {
			t.Skip()
		}
		original := append([]byte(nil), plaintext...)
		sealed, err := records.Encrypt(nil, plaintext, aad, sequence)
		if err != nil {
			t.Fatal(err)
		}
		opened, err := records.DecryptInPlace(sealed, aad, sequence)
		if err != nil || !bytes.Equal(opened, original) || !bytes.Equal(plaintext, original) {
			t.Fatalf("round trip = %x, %v", opened, err)
		}
	})
}
