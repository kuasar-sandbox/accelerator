package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

const (
	// FlagAESSIV is the fixed encrypted tarstream v1 record encoding flag.
	FlagAESSIV byte = 0x01

	aesSIVOverhead = 1 + aes.BlockSize
)

var (
	macKeyDomain = []byte("kuasar/tarstream/aes-siv/mac-key/v1")
	ctrKeyDomain = []byte("kuasar/tarstream/aes-siv/ctr-key/v1")
)

// TarStreamCodec is the fixed customer-key-backed AES-256-SIV-CMAC record
// codec for encrypted tarstream v1. It is immutable and safe for concurrent
// use.
type TarStreamCodec struct {
	customerKey [32]byte
	macKey      [32]byte
	ctrKey      [32]byte
}

// NewTarStreamCodec derives the two AES-256-SIV keys from customerKey. It does
// not read configuration, resolve credentials, or connect to storage.
func NewTarStreamCodec(customerKey [32]byte) (*TarStreamCodec, error) {
	c := &TarStreamCodec{customerKey: customerKey}
	c.macKey = keyedHMAC(customerKey, macKeyDomain)
	c.ctrKey = keyedHMAC(customerKey, ctrKeyDomain)
	return c, nil
}

func keyedHMAC(key [32]byte, data []byte) [32]byte {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(data)
	var result [32]byte
	copy(result[:], mac.Sum(nil))
	return result
}

// CiphertextSize returns flag + synthetic IV + CTR ciphertext length. A
// negative result marks invalid or overflowing input for the framing layer.
func (*TarStreamCodec) CiphertextSize(plaintextSize int) int {
	if plaintextSize < 0 || plaintextSize > int(^uint(0)>>1)-aesSIVOverhead {
		return -1
	}
	return plaintextSize + aesSIVOverhead
}

// Encrypt appends one deterministic AES-SIV record to dst. S2V's vector is
// exactly {FlagAESSIV, associatedData, plaintext}.
func (c *TarStreamCodec) Encrypt(dst, plaintext, associatedData []byte) ([]byte, error) {
	size := c.CiphertextSize(len(plaintext))
	if size < 0 {
		return nil, fmt.Errorf("crypto: tarstream: plaintext size overflow")
	}
	macBlock, err := aes.NewCipher(c.macKey[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: tarstream MAC cipher: %w", err)
	}
	ctrBlock, err := aes.NewCipher(c.ctrKey[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: tarstream CTR cipher: %w", err)
	}

	siv := s2v(macBlock, []byte{FlagAESSIV}, associatedData, plaintext)
	start := len(dst)
	dst = append(dst, make([]byte, size)...)
	record := dst[start:]
	record[0] = FlagAESSIV
	copy(record[1:1+aes.BlockSize], siv[:])
	iv := maskedCTRIV(siv)
	cipher.NewCTR(ctrBlock, iv[:]).XORKeyStream(record[aesSIVOverhead:], plaintext)
	return dst, nil
}

// DecryptInPlace authenticates and decrypts one record into its ciphertext
// backing array. Authentication failure clears the tentative plaintext before
// returning and never exposes it to the caller.
func (c *TarStreamCodec) DecryptInPlace(record, associatedData []byte) ([]byte, error) {
	if len(record) < aesSIVOverhead || record[0] != FlagAESSIV {
		return nil, fmt.Errorf("%w: invalid AES-SIV record", tarstream.ErrAuthentication)
	}
	macBlock, err := aes.NewCipher(c.macKey[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: tarstream MAC cipher: %w", err)
	}
	ctrBlock, err := aes.NewCipher(c.ctrKey[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: tarstream CTR cipher: %w", err)
	}

	var siv [aes.BlockSize]byte
	copy(siv[:], record[1:aesSIVOverhead])
	plaintext := record[aesSIVOverhead:]
	iv := maskedCTRIV(siv)
	cipher.NewCTR(ctrBlock, iv[:]).XORKeyStream(plaintext, plaintext)
	want := s2v(macBlock, []byte{FlagAESSIV}, associatedData, plaintext)
	if subtle.ConstantTimeCompare(siv[:], want[:]) != 1 {
		clear(plaintext)
		return nil, tarstream.ErrAuthentication
	}
	return plaintext, nil
}

// KeyedDigest computes HMAC-SHA256(customerKey, plainDigestRaw32Bytes).
func (c *TarStreamCodec) KeyedDigest(plainDigest [32]byte) [32]byte {
	return keyedHMAC(c.customerKey, plainDigest[:])
}

func maskedCTRIV(siv [aes.BlockSize]byte) [aes.BlockSize]byte {
	iv := siv
	iv[8] &= 0x7f
	iv[12] &= 0x7f
	return iv
}

func s2v(block cipher.Block, values ...[]byte) [aes.BlockSize]byte {
	var zero [aes.BlockSize]byte
	if len(values) == 0 {
		zero[aes.BlockSize-1] = 1
		return cmacSum(block, zero[:])
	}
	d := cmacSum(block, zero[:])
	for _, value := range values[:len(values)-1] {
		d = xorBlock(doubleBlock(d), cmacSum(block, value))
	}
	last := values[len(values)-1]
	if len(last) >= aes.BlockSize {
		input := append([]byte(nil), last...)
		start := len(input) - aes.BlockSize
		for i := range aes.BlockSize {
			input[start+i] ^= d[i]
		}
		return cmacSum(block, input)
	}
	padded := doubleBlock(d)
	for i := range last {
		padded[i] ^= last[i]
	}
	padded[len(last)] ^= 0x80
	return cmacSum(block, padded[:])
}

func cmacSum(block cipher.Block, message []byte) [aes.BlockSize]byte {
	var zero [aes.BlockSize]byte
	var l [aes.BlockSize]byte
	block.Encrypt(l[:], zero[:])
	k1 := doubleBlock(l)
	k2 := doubleBlock(k1)

	blocks := (len(message) + aes.BlockSize - 1) / aes.BlockSize
	complete := len(message) > 0 && len(message)%aes.BlockSize == 0
	if blocks == 0 {
		blocks = 1
	}
	var state [aes.BlockSize]byte
	for i := 0; i < blocks-1; i++ {
		var input [aes.BlockSize]byte
		copy(input[:], message[i*aes.BlockSize:(i+1)*aes.BlockSize])
		input = xorBlock(input, state)
		block.Encrypt(state[:], input[:])
	}

	var last [aes.BlockSize]byte
	start := (blocks - 1) * aes.BlockSize
	if complete {
		copy(last[:], message[start:start+aes.BlockSize])
		last = xorBlock(last, k1)
	} else {
		if start < len(message) {
			copy(last[:], message[start:])
		}
		last[len(message)-start] = 0x80
		last = xorBlock(last, k2)
	}
	last = xorBlock(last, state)
	block.Encrypt(state[:], last[:])
	return state
}

func doubleBlock(value [aes.BlockSize]byte) [aes.BlockSize]byte {
	carry := value[0] >> 7
	for i := 0; i < aes.BlockSize-1; i++ {
		value[i] = value[i]<<1 | value[i+1]>>7
	}
	value[aes.BlockSize-1] <<= 1
	value[aes.BlockSize-1] ^= byte(0x87 * carry)
	return value
}

func xorBlock(a, b [aes.BlockSize]byte) [aes.BlockSize]byte {
	for i := range aes.BlockSize {
		a[i] ^= b[i]
	}
	return a
}

var _ tarstream.Codec = (*TarStreamCodec)(nil)
