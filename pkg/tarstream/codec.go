package tarstream

import "errors"

// Codec binds the customer-key-backed primitive to one encrypted tarstream v1
// artifact and computes its stable logical identity. Implementations must be
// safe for concurrent use.
type Codec interface {
	// BindArtifact derives and constructs the primitive for one artifact salt.
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
	// DecryptInPlace authenticates before returning a plaintext slice backed by
	// ciphertext. Authentication failure must not return plaintext.
	DecryptInPlace(ciphertext, associatedData []byte, sequence uint64) ([]byte, error)
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
