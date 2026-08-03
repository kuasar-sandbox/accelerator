package tarstream

import (
	"fmt"
	"io"
)

// recordSeqReader authenticates encrypted records in order and exposes the
// complete canonical plaintext stream with O(recordSize) live memory.
type recordSeqReader struct {
	r        io.Reader
	codec    Codec
	prefix   [envelopePrefixSize]byte
	header   [envelopeHeaderSize]byte
	geometry envelopeHeader
	index    uint64
	current  []byte
	position int
	finished bool
	finalErr error
}

func newRecordSeqReader(r io.Reader, codec Codec, prefix [envelopePrefixSize]byte, plainHeader [envelopeHeaderSize]byte, geometry envelopeHeader) *recordSeqReader {
	return &recordSeqReader{r: r, codec: codec, prefix: prefix, header: plainHeader, geometry: geometry}
}

func (r *recordSeqReader) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if r.finished {
		if r.finalErr != nil {
			return 0, r.finalErr
		}
		return 0, io.EOF
	}
	written := 0
	for written < len(dst) {
		if r.position == len(r.current) {
			if r.index == r.geometry.recordCount {
				r.finishOuter()
				if r.finalErr != nil {
					return written, r.finalErr
				}
				if written == 0 {
					return 0, io.EOF
				}
				return written, nil
			}
			if err := r.loadRecord(); err != nil {
				r.finished = true
				r.finalErr = err
				return written, err
			}
		}
		n := copy(dst[written:], r.current[r.position:])
		r.position += n
		written += n
	}
	return written, nil
}

func (r *recordSeqReader) loadRecord() error {
	plainSize := int(r.geometry.recordPlainSize(r.index))
	sealed := make([]byte, plainSize+recordOverhead)
	if _, err := io.ReadFull(r.r, sealed); err != nil {
		return fmt.Errorf("%w: truncated encrypted record", ErrMalformedEnvelope)
	}
	aad := recordAAD(r.prefix, r.header, r.index, uint32(plainSize))
	plaintext, err := r.codec.DecryptInPlace(sealed, aad)
	if err != nil {
		return fmt.Errorf("%w: encrypted data record", ErrAuthentication)
	}
	if len(plaintext) != plainSize || plainSize > 0 && &plaintext[0] != &sealed[recordOverhead] {
		return fmt.Errorf("%w: codec violated in-place record contract", ErrMalformedEnvelope)
	}
	r.current = plaintext
	r.position = 0
	r.index++
	return nil
}

func (r *recordSeqReader) finishOuter() {
	r.finished = true
	var extra [1]byte
	n, err := readOneByte(r.r, extra[:])
	if n != 0 || err == nil {
		r.finalErr = fmt.Errorf("%w: trailing ciphertext", ErrMalformedEnvelope)
		return
	}
	if err != io.EOF {
		r.finalErr = fmt.Errorf("%w: outer EOF: %v", ErrMalformedEnvelope, err)
	}
}
