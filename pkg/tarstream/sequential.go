package tarstream

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

type trackedPlainReader struct {
	r           io.Reader
	hash        hash.Hash
	hashEnabled bool
	read        int64
}

func newTrackedPlainReader(r io.Reader) *trackedPlainReader {
	return &trackedPlainReader{r: r, hash: sha256.New(), hashEnabled: true}
}

func (r *trackedPlainReader) Read(dst []byte) (int, error) {
	n, err := r.r.Read(dst)
	if n > 0 {
		if r.hashEnabled {
			_, _ = r.hash.Write(dst[:n])
		}
		r.read += int64(n)
	}
	return n, err
}

type sequentialVerifier struct {
	reader  *trackedPlainReader
	meta    *meta
	options readOptions
	done    bool
	err     error
}

func (v *sequentialVerifier) finish() error {
	if v.done {
		return v.err
	}
	v.done = true
	pad := int((512 - v.meta.stored%512) % 512)
	if pad > 0 {
		padding := make([]byte, pad)
		if _, err := io.ReadFull(v.reader, padding); err != nil {
			if errors.Is(err, ErrAuthentication) || errors.Is(err, ErrMalformedEnvelope) {
				v.err = err
				return v.err
			}
			v.err = fmt.Errorf("%w: invalid payload padding", ErrInvalidCanonicalTarstream)
			return v.err
		}
		if !isZeroBlock(padding) {
			v.err = fmt.Errorf("%w: invalid payload padding", ErrInvalidCanonicalTarstream)
			return v.err
		}
	}
	v.reader.hashEnabled = false
	var marker [512]byte
	if _, err := io.ReadFull(v.reader, marker[:]); err != nil {
		if errors.Is(err, ErrAuthentication) || errors.Is(err, ErrMalformedEnvelope) {
			v.err = err
			return v.err
		}
		v.err = fmt.Errorf("%w: missing digest marker", ErrInvalidCanonicalTarstream)
		return v.err
	}
	plainDigest, err := parseCanonicalMarker(marker[:])
	if err != nil {
		v.err = err
		return v.err
	}
	var trailer [1024]byte
	if _, err := io.ReadFull(v.reader, trailer[:]); err != nil {
		if errors.Is(err, ErrAuthentication) || errors.Is(err, ErrMalformedEnvelope) {
			v.err = err
			return v.err
		}
		v.err = fmt.Errorf("%w: invalid end-of-archive", ErrInvalidCanonicalTarstream)
		return v.err
	}
	if !isZeroBlock(trailer[:512]) || !isZeroBlock(trailer[512:]) {
		v.err = fmt.Errorf("%w: invalid end-of-archive", ErrInvalidCanonicalTarstream)
		return v.err
	}
	var extra [1]byte
	n, err := readOneByte(v.reader, extra[:])
	if n != 0 {
		v.err = fmt.Errorf("%w: trailing artifact bytes", ErrInvalidCanonicalTarstream)
		return v.err
	}
	if err != io.EOF {
		if err != nil {
			v.err = err
		} else {
			v.err = fmt.Errorf("%w: trailing artifact bytes", ErrInvalidCanonicalTarstream)
		}
		return v.err
	}
	var recomputed [32]byte
	copy(recomputed[:], v.reader.hash.Sum(nil))
	if subtle.ConstantTimeCompare(recomputed[:], plainDigest[:]) != 1 {
		v.err = ErrDigestMismatch
		return v.err
	}
	_, external := externalDigest(v.options.codec, recomputed)
	if err := checkExpected(v.options, external); err != nil {
		v.err = err
	}
	return v.err
}

func parseCanonicalMarker(block []byte) ([32]byte, error) {
	var zero [32]byte
	if len(block) != 512 || string(block[257:262]) != "ustar" || !validHeaderChecksum(block) {
		return zero, fmt.Errorf("%w: invalid digest marker header", ErrInvalidCanonicalTarstream)
	}
	typeflag := block[156]
	if typeflag != 0 && typeflag != '0' {
		return zero, fmt.Errorf("%w: digest marker is not a regular file", ErrInvalidCanonicalTarstream)
	}
	size, err := parseNumeric(block[124:136])
	if err != nil || size != 0 {
		return zero, fmt.Errorf("%w: digest marker is not empty", ErrInvalidCanonicalTarstream)
	}
	digest, ok := parseDigestMarker(normalizeName(ustarName(block)))
	if !ok {
		return zero, fmt.Errorf("%w: invalid digest marker", ErrInvalidCanonicalTarstream)
	}
	return digest, nil
}

func validHeaderChecksum(block []byte) bool {
	want, err := parseNumeric(block[148:156])
	if err != nil {
		return false
	}
	var unsigned, signed int64
	for i, value := range block {
		if i >= 148 && i < 156 {
			value = ' '
		}
		unsigned += int64(value)
		signed += int64(int8(value))
	}
	return want == unsigned || want == signed
}

const maxConsecutiveEmptyReads = 100

func readOneByte(reader io.Reader, buffer []byte) (int, error) {
	for range maxConsecutiveEmptyReads {
		n, err := reader.Read(buffer)
		if n != 0 || err != nil {
			return n, err
		}
	}
	return 0, io.ErrNoProgress
}

func openSequentialPlaintext(r io.Reader, options readOptions) (io.Reader, *envelopeHeader, error) {
	first := make([]byte, len(envelopeMagic))
	n, err := io.ReadFull(r, first)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if options.required {
				return nil, nil, ErrPlaintextForbidden
			}
			return io.MultiReader(bytes.NewReader(first[:n]), r), nil, nil
		}
		return nil, nil, err
	}
	if !bytes.Equal(first, envelopeMagic[:]) {
		if options.required {
			return nil, nil, ErrPlaintextForbidden
		}
		return io.MultiReader(bytes.NewReader(first), r), nil, nil
	}
	if options.codec == nil {
		return nil, nil, ErrCodecRequired
	}
	var prefix [envelopePrefixSize]byte
	copy(prefix[:8], first)
	if _, err := io.ReadFull(r, prefix[8:]); err != nil {
		return nil, nil, fmt.Errorf("%w: truncated clear prefix", ErrMalformedEnvelope)
	}
	if err := validatePrefix(prefix); err != nil {
		return nil, nil, err
	}
	recordCodec, err := options.codec.BindArtifact(prefixSalt(prefix))
	if err != nil {
		return nil, nil, err
	}
	if recordCodec.CiphertextSize(envelopeHeaderSize) != envelopeHeaderSealed {
		return nil, nil, fmt.Errorf("%w: codec header size", ErrMalformedEnvelope)
	}
	sealed := make([]byte, envelopeHeaderSealed)
	if _, err := io.ReadFull(r, sealed); err != nil {
		return nil, nil, fmt.Errorf("%w: truncated encrypted header", ErrMalformedEnvelope)
	}
	plaintext, err := recordCodec.DecryptInPlace(sealed, headerAAD(prefix), 0)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: encrypted header", ErrAuthentication)
	}
	if len(plaintext) != envelopeHeaderSize || &plaintext[0] != &sealed[1] {
		return nil, nil, fmt.Errorf("%w: codec violated in-place header contract", ErrMalformedEnvelope)
	}
	var plainHeader [envelopeHeaderSize]byte
	copy(plainHeader[:], plaintext)
	geometry, err := parseEnvelopeHeader(plainHeader[:])
	if err != nil {
		return nil, nil, err
	}
	if err := validateCodecGeometry(recordCodec, geometry); err != nil {
		return nil, nil, err
	}
	return newRecordSeqReader(r, recordCodec, prefix, plainHeader, geometry), &geometry, nil
}

func sourceFromSequential(r io.Reader, name string, options readOptions) (sparse.Source, string, error) {
	plaintext, geometry, err := openSequentialPlaintext(r, options)
	if err != nil {
		return nil, "", err
	}
	tracked := newTrackedPlainReader(plaintext)
	view, err := newSeqView(tracked, name)
	if err != nil {
		return nil, "", keyBoundCanonicalError(options, err)
	}
	if geometry != nil {
		packedSize := view.meta.stored - view.meta.mapLen
		if tracked.read < 0 || packedSize < 0 || uint64(tracked.read) != geometry.packedStart || uint64(packedSize) != geometry.packedSize {
			return nil, "", fmt.Errorf("%w: inner tar layout does not match authenticated header", ErrMalformedEnvelope)
		}
	}
	verifier := &sequentialVerifier{reader: tracked, meta: &view.meta, options: options}
	view.finish = verifier.finish
	if view.meta.stored-view.meta.mapLen == 0 {
		if err := view.finishNow(); err != nil {
			return nil, "", err
		}
	}
	return &sourceView{m: &view.meta, seq: view}, view.meta.name, nil
}
