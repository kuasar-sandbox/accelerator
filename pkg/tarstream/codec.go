package tarstream

import "errors"

// Codec is the customer-key-bound authenticated record primitive used by the
// fixed encrypted tarstream v1 framing. Implementations must be safe for
// concurrent use. Tarstream validates every reported and returned length.
type Codec interface {
	// CiphertextSize returns the exact encoded size for a plaintext record.
	CiphertextSize(plaintextSize int) int
	// Encrypt appends an authenticated record to dst without retaining or
	// modifying plaintext or associatedData.
	Encrypt(dst, plaintext, associatedData []byte) ([]byte, error)
	// DecryptInPlace authenticates before returning a plaintext slice backed by
	// ciphertext. Authentication failure must not return plaintext.
	DecryptInPlace(ciphertext, associatedData []byte) ([]byte, error)
	// KeyedDigest returns HMAC-SHA256(customerKey, plainDigest[:]).
	KeyedDigest(plainDigest [32]byte) [32]byte
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
