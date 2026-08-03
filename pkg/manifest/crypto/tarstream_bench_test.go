package crypto

import (
	"bytes"
	"testing"
)

func BenchmarkTarStreamCodecPrimitive4KiB(b *testing.B) {
	codec, _ := NewTarStreamCodec([32]byte{0x31})
	plaintext := bytes.Repeat([]byte{0x5a}, 4096)
	aad := bytes.Repeat([]byte{0xa5}, 128)
	sealed, _ := codec.Encrypt(nil, plaintext, aad)
	b.Run("encrypt", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(plaintext)))
		for i := 0; i < b.N; i++ {
			if _, err := codec.Encrypt(make([]byte, 0, len(sealed)), plaintext, aad); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decrypt", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(plaintext)))
		buffer := make([]byte, len(sealed))
		for i := 0; i < b.N; i++ {
			copy(buffer, sealed)
			if _, err := codec.DecryptInPlace(buffer, aad); err != nil {
				b.Fatal(err)
			}
		}
	})
}
