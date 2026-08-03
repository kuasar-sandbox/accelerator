package tarstream

import (
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
)

const (
	DigestSchemeSHA256 = "sha256"
	DigestSchemeHMAC   = "hmac"
)

type WriteOption interface {
	applyWrite(*writeOptions) error
}

type ReadOption interface {
	applyRead(*readOptions) error
}

type CodecOption interface {
	WriteOption
	ReadOption
}

type writeOptions struct {
	codec    Codec
	codecSet bool
}

type readOptions struct {
	codec       Codec
	codecSet    bool
	required    bool
	expected    [32]byte
	expectedSet bool
	scheme      string
}

type codecOption struct {
	codec    Codec
	required bool
}

// WithCodec supplies the fixed v1 record codec. Required only controls whether
// readers reject plaintext; every writer with a codec emits encrypted v1.
func WithCodec(codec Codec, required bool) CodecOption {
	return codecOption{codec: codec, required: required}
}

func (o codecOption) applyWrite(dst *writeOptions) error {
	if dst.codecSet {
		return fmt.Errorf("%w: duplicate WithCodec", ErrInvalidOption)
	}
	if nilInterface(o.codec) {
		return fmt.Errorf("%w: WithCodec requires a non-nil codec", ErrInvalidOption)
	}
	dst.codec, dst.codecSet = o.codec, true
	return nil
}

func (o codecOption) applyRead(dst *readOptions) error {
	if dst.codecSet {
		return fmt.Errorf("%w: duplicate WithCodec", ErrInvalidOption)
	}
	if nilInterface(o.codec) {
		return fmt.Errorf("%w: WithCodec requires a non-nil codec", ErrInvalidOption)
	}
	dst.codec, dst.codecSet, dst.required = o.codec, true, o.required
	return nil
}

type expectedDigestOption struct {
	scheme string
	digest string
}

// WithExpectedDigest requires the canonical artifact identity to match the
// supplied scheme and 64-character lowercase hex digest.
func WithExpectedDigest(scheme, digest string) ReadOption {
	return expectedDigestOption{scheme: scheme, digest: digest}
}

func (o expectedDigestOption) applyRead(dst *readOptions) error {
	if dst.expectedSet {
		return fmt.Errorf("%w: duplicate WithExpectedDigest", ErrInvalidOption)
	}
	digest, err := decodeDigest(o.scheme, o.digest)
	if err != nil {
		return err
	}
	dst.expected, dst.expectedSet, dst.scheme = digest, true, o.scheme
	return nil
}

func parseWriteOptions(options []WriteOption) (writeOptions, error) {
	var result writeOptions
	for i, option := range options {
		if option == nil {
			return result, fmt.Errorf("%w: nil write option %d", ErrInvalidOption, i)
		}
		if err := option.applyWrite(&result); err != nil {
			return result, err
		}
	}
	return result, nil
}

func parseReadOptions(options []ReadOption) (readOptions, error) {
	var result readOptions
	for i, option := range options {
		if option == nil {
			return result, fmt.Errorf("%w: nil read option %d", ErrInvalidOption, i)
		}
		if err := option.applyRead(&result); err != nil {
			return result, err
		}
	}
	wantScheme := DigestSchemeSHA256
	if result.codecSet {
		wantScheme = DigestSchemeHMAC
	}
	if result.expectedSet && result.scheme != wantScheme {
		return result, fmt.Errorf("%w: expected digest scheme is incompatible with codec policy", ErrInvalidOption)
	}
	return result, nil
}

func decodeDigest(scheme, digest string) ([32]byte, error) {
	var result [32]byte
	if scheme != DigestSchemeSHA256 && scheme != DigestSchemeHMAC {
		return result, fmt.Errorf("%w: unsupported digest scheme", ErrInvalidDigest)
	}
	if len(digest) != hex.EncodedLen(len(result)) || strings.ToLower(digest) != digest {
		return result, fmt.Errorf("%w: digest must be 64 lowercase hex characters", ErrInvalidDigest)
	}
	if _, err := hex.Decode(result[:], []byte(digest)); err != nil {
		return result, fmt.Errorf("%w: malformed hexadecimal digest", ErrInvalidDigest)
	}
	return result, nil
}

func externalDigest(codec Codec, plain [32]byte) (string, [32]byte) {
	if codec == nil {
		return DigestSchemeSHA256, plain
	}
	return DigestSchemeHMAC, codec.KeyedDigest(plain)
}

func checkExpected(options readOptions, actual [32]byte) error {
	if options.expectedSet && subtle.ConstantTimeCompare(options.expected[:], actual[:]) != 1 {
		return ErrDigestMismatch
	}
	return nil
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
