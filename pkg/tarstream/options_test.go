package tarstream

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

type optionTestCodec struct{}

func (c *optionTestCodec) BindArtifact([32]byte) (RecordCodec, error) { return c, nil }
func (*optionTestCodec) CiphertextSize(size int) int                  { return size + recordOverhead }
func (*optionTestCodec) Encrypt(dst, plaintext, _ []byte, _ uint64) ([]byte, error) {
	start := len(dst)
	dst = append(dst, make([]byte, len(plaintext)+recordOverhead)...)
	copy(dst[start+1:], plaintext)
	return dst, nil
}
func (*optionTestCodec) DecryptInPlace(ciphertext, _ []byte, _ uint64) ([]byte, error) {
	return ciphertext[1 : len(ciphertext)-recordOverhead+1], nil
}
func (*optionTestCodec) KeyedDigest(digest [32]byte) [32]byte { return digest }

type shortCodec struct{ optionTestCodec }

func (c *shortCodec) BindArtifact([32]byte) (RecordCodec, error) { return c, nil }
func (*shortCodec) CiphertextSize(size int) int                  { return size + recordOverhead - 1 }

type nilBindingCodec struct{ optionTestCodec }

func (*nilBindingCodec) BindArtifact([32]byte) (RecordCodec, error) {
	var recordCodec *optionTestCodec
	return recordCodec, nil
}

type nonInPlaceCodec struct{ optionTestCodec }

func (c *nonInPlaceCodec) BindArtifact([32]byte) (RecordCodec, error) { return c, nil }
func (*nonInPlaceCodec) Encrypt(dst, plaintext, _ []byte, _ uint64) ([]byte, error) {
	start := len(dst)
	dst = append(dst, make([]byte, len(plaintext)+recordOverhead)...)
	copy(dst[start+recordOverhead:], plaintext)
	return dst, nil
}

func (*nonInPlaceCodec) DecryptInPlace(ciphertext, _ []byte, _ uint64) ([]byte, error) {
	return append([]byte(nil), ciphertext[recordOverhead:]...), nil
}

func TestCodecOptionValidation(t *testing.T) {
	source := sparse.Dense(bytes.NewReader(nil), 0)
	var output bytes.Buffer
	var nilCodec *optionTestCodec
	if _, _, err := WriteTo(context.Background(), &output, "empty", source, WithCodec(nil, false)); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("WithCodec(nil) error = %v", err)
	}
	if _, _, err := WriteTo(context.Background(), &output, "empty", source, WithCodec(nilCodec, false)); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("WithCodec(typed nil) error = %v", err)
	}
	codec := &optionTestCodec{}
	if _, _, err := WriteTo(context.Background(), &output, "empty", source, WithCodec(codec, false), WithCodec(codec, true)); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("duplicate WithCodec error = %v", err)
	}
}

func TestTarstreamRejectsCodecContractViolations(t *testing.T) {
	source := sparse.Dense(bytes.NewReader(nil), 0)
	var output bytes.Buffer
	if _, _, err := WriteTo(context.Background(), &output, "empty", source, WithCodec(&nilBindingCodec{}, false)); !errors.Is(err, ErrMalformedEnvelope) {
		t.Fatalf("nil bound codec write error = %v", err)
	}

	if _, _, err := WriteTo(context.Background(), &output, "empty", source, WithCodec(&shortCodec{}, false)); !errors.Is(err, ErrMalformedEnvelope) {
		t.Fatalf("invalid CiphertextSize error = %v", err)
	}

	output.Reset()
	validCodec := &optionTestCodec{}
	if _, _, err := WriteTo(context.Background(), &output, "empty", source, WithCodec(validCodec, false)); err != nil {
		t.Fatal(err)
	}
	artifact := append([]byte(nil), output.Bytes()...)
	if _, _, err := SourceAt(bytes.NewReader(artifact), int64(len(artifact)), "", WithCodec(&nilBindingCodec{}, true)); !errors.Is(err, ErrMalformedEnvelope) {
		t.Fatalf("nil bound codec random read error = %v", err)
	}
	if _, _, err := SourceFrom(bytes.NewBuffer(artifact), "", WithCodec(&nilBindingCodec{}, true)); !errors.Is(err, ErrMalformedEnvelope) {
		t.Fatalf("nil bound codec sequential read error = %v", err)
	}

	output.Reset()
	codec := &nonInPlaceCodec{}
	if _, _, err := WriteTo(context.Background(), &output, "empty", source, WithCodec(codec, false)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := SourceAt(bytes.NewReader(output.Bytes()), int64(output.Len()), "", WithCodec(codec, true)); !errors.Is(err, ErrMalformedEnvelope) {
		t.Fatalf("non-in-place decrypt error = %v", err)
	}
}

func TestExpectedDigestOptionValidation(t *testing.T) {
	for _, option := range []ReadOption{
		WithExpectedDigest("hmac-sha256", string(bytes.Repeat([]byte{'a'}, 64))),
		WithExpectedDigest(DigestSchemeSHA256, "ABC"),
		WithExpectedDigest(DigestSchemeSHA256, string(bytes.Repeat([]byte{'g'}, 64))),
	} {
		if _, err := parseReadOptions([]ReadOption{option}); !errors.Is(err, ErrInvalidDigest) {
			t.Fatalf("invalid expected digest error = %v", err)
		}
	}
	codec := &optionTestCodec{}
	if _, err := parseReadOptions([]ReadOption{WithCodec(codec, false), WithExpectedDigest(DigestSchemeSHA256, string(bytes.Repeat([]byte{'a'}, 64)))}); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("codec/SHA-256 expected scheme error = %v", err)
	}
	if _, err := parseReadOptions([]ReadOption{WithExpectedDigest(DigestSchemeHMAC, string(bytes.Repeat([]byte{'a'}, 64)))}); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("unkeyed/HMAC expected scheme error = %v", err)
	}
}

func TestPlaintextExpectedDigest(t *testing.T) {
	var artifact bytes.Buffer
	scheme, digest, err := WriteTo(context.Background(), &artifact, "empty", sparse.Dense(bytes.NewReader(nil), 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := SourceAt(bytes.NewReader(artifact.Bytes()), int64(artifact.Len()), "", WithExpectedDigest(scheme, digest)); err != nil {
		t.Fatal(err)
	}
	wrong := string(bytes.Repeat([]byte{'0'}, 64))
	if _, _, err := SourceAt(bytes.NewReader(artifact.Bytes()), int64(artifact.Len()), "", WithExpectedDigest(scheme, wrong)); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("wrong expected SHA-256 error = %v", err)
	}
}
