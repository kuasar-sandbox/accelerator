package crypto

import (
	"bytes"
	"context"
	"testing"
)

func FuzzDecryptChunkTo(f *testing.F) {
	codec := &AESChunkEncryptor{}
	for _, plain := range [][]byte{
		[]byte("raw seed"),
		bytes.Repeat([]byte("compressed-seed\x00"), 4096),
	} {
		object, _, key, err := codec.EncryptChunk(context.Background(), [32]byte{0x72}, plain)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(object, key[:], uint32(len(plain)))
	}
	f.Add([]byte{}, make([]byte, 32), uint32(0))
	f.Add([]byte{0xff, 1, 2, 3}, make([]byte, 32), uint32(17))

	f.Fuzz(func(t *testing.T, object, keyBytes []byte, decodedSize uint32) {
		if len(object) > 2<<20 || decodedSize > 2<<20 {
			t.Skip()
		}
		var key [32]byte
		copy(key[:], keyBytes)
		original := append([]byte(nil), object...)
		dst := make([]byte, decodedSize)
		_ = codec.DecryptChunkTo(context.Background(), key, object, dst)
		if !bytes.Equal(object, original) {
			t.Fatal("decrypt mutated fuzz input")
		}
	})
}
