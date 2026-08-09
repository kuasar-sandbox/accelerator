package tarstream

import (
	"errors"
	"fmt"
)

// Codec binds the customer-key-backed primitive to one encrypted tarstream v1
// artifact and computes its stable logical identity. Implementations must be
// safe for concurrent use.
type Codec interface {
	// BindArtifact derives and constructs the primitive for one artifact salt.
	// A successful call must return a non-nil RecordCodec.
	BindArtifact(salt [32]byte) (RecordCodec, error)
	// KeyedDigest returns HMAC-SHA256(customerKey, plainDigest[:]).
	KeyedDigest(plainDigest [32]byte) [32]byte
}

// RecordCodec is the artifact-bound authenticated record primitive used by
// encrypted tarstream v1. Implementations must be safe for concurrent use.
type RecordCodec interface {
	CiphertextSize(plaintextSize int) int
	// Encrypt appends an authenticated record to dst without retaining or
	// modifying plaintext or associatedData.
	Encrypt(dst, plaintext, associatedData []byte, sequence uint64) ([]byte, error)
	// DecryptInPlace authenticates a record containing n plaintext bytes before
	// returning exactly ciphertext[1:1+n]. It must not retain or modify
	// associatedData. Authentication failure must clear tentative plaintext and
	// return no plaintext.
	DecryptInPlace(ciphertext, associatedData []byte, sequence uint64) ([]byte, error)
}

func bindRecordCodec(codec Codec, salt [32]byte) (RecordCodec, error) {
	recordCodec, err := codec.BindArtifact(salt)
	if err != nil {
		return nil, err
	}
	if nilInterface(recordCodec) {
		return nil, fmt.Errorf("%w: codec returned a nil record codec", ErrMalformedEnvelope)
	}
	return recordCodec, nil
}

var (
	ErrInvalidOption             = errors.New("tarstream: invalid option")
	ErrCodecRequired             = errors.New("tarstream: codec required")
	ErrPlaintextForbidden        = errors.New("tarstream: plaintext forbidden")
	ErrUnsupportedVersion        = errors.New("tarstream: unsupported encrypted version")
	ErrMalformedEnvelope         = errors.New("tarstream: malformed encrypted envelope")
	ErrAuthentication            = errors.New("tarstream: authentication failed")
	ErrInvalidDigest             = errors.New("tarstream: invalid digest")
	ErrDigestMismatch            = errors.New("tarstream: digest mismatch")
	ErrInvalidCanonicalTarstream = errors.New("tarstream: invalid canonical artifact")
)
