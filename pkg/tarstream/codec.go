package tarstream

import "errors"

// Codec is the customer-key-bound authenticated record primitive used by the
// fixed encrypted tarstream v1 framing. Implementations must be safe for
// concurrent use. Tarstream validates every reported and returned length.
type Codec interface {
	CiphertextSize(plaintextSize int) int
	Encrypt(dst, plaintext, associatedData []byte) ([]byte, error)
	DecryptInPlace(ciphertext, associatedData []byte) ([]byte, error)
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
