package tarstream

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	envelopeVersion       = 1
	envelopePrefixSize    = 16
	envelopeHeaderSize    = 64
	envelopeHeaderSealed  = 81
	recordSize            = 4096
	recordOverhead        = 17
	canonicalSuffixSize   = 3 * 512
	maxCoalesceBytes      = 1 << 20
	defaultRecordCacheMax = 8 << 20
)

var (
	envelopeMagic = [8]byte{0x89, 'K', 'T', 'S', 'E', 'N', 'C', '\n'}
	headerDomain  = []byte("kuasar/tarstream/header/v1\x00")
	recordDomain  = []byte("kuasar/tarstream/record/v1\x00")
)

type envelopeHeader struct {
	plaintextSize uint64
	packedStart   uint64
	packedSize    uint64
	recordCount   uint64
	recordSize    uint32
	firstSize     uint32
}

func makeEnvelopeHeader(plan *writePlan) (envelopeHeader, error) {
	if plan.plaintextSize < canonicalSuffixSize || plan.plaintextSize%512 != 0 {
		return envelopeHeader{}, fmt.Errorf("%w: invalid plaintext artifact size", ErrInvalidCanonicalTarstream)
	}
	first := uint32(plan.packedStart % recordSize)
	if first == 0 {
		first = recordSize
	}
	header := envelopeHeader{
		plaintextSize: uint64(plan.plaintextSize),
		packedStart:   uint64(plan.packedStart),
		packedSize:    uint64(plan.packedSize),
		recordSize:    recordSize,
		firstSize:     first,
	}
	header.recordCount = derivedRecordCount(header.plaintextSize, uint64(first))
	if err := header.validate(); err != nil {
		return envelopeHeader{}, err
	}
	return header, nil
}

func derivedRecordCount(plaintextSize, firstSize uint64) uint64 {
	if plaintextSize <= firstSize {
		return 1
	}
	remaining := plaintextSize - firstSize
	return 1 + (remaining+recordSize-1)/recordSize
}

func (h envelopeHeader) validate() error {
	if h.plaintextSize < canonicalSuffixSize || h.plaintextSize > 1<<62 || h.plaintextSize%512 != 0 {
		return fmt.Errorf("%w: invalid plaintext size", ErrMalformedEnvelope)
	}
	if h.recordSize != recordSize || h.firstSize == 0 || h.firstSize > recordSize {
		return fmt.Errorf("%w: invalid record geometry", ErrMalformedEnvelope)
	}
	wantFirst := uint32(h.packedStart % recordSize)
	if wantFirst == 0 {
		wantFirst = recordSize
	}
	if h.firstSize != wantFirst {
		return fmt.Errorf("%w: packed-data alignment mismatch", ErrMalformedEnvelope)
	}
	markerOffset := h.plaintextSize - canonicalSuffixSize
	packedEnd, overflow := addUint64(h.packedStart, h.packedSize)
	if overflow || packedEnd > markerOffset {
		return fmt.Errorf("%w: packed-data bounds", ErrMalformedEnvelope)
	}
	if h.recordCount != derivedRecordCount(h.plaintextSize, uint64(h.firstSize)) {
		return fmt.Errorf("%w: record count mismatch", ErrMalformedEnvelope)
	}
	return nil
}

func (h envelopeHeader) marshal() [envelopeHeaderSize]byte {
	var out [envelopeHeaderSize]byte
	binary.BigEndian.PutUint64(out[0:8], h.plaintextSize)
	binary.BigEndian.PutUint64(out[8:16], h.packedStart)
	binary.BigEndian.PutUint64(out[16:24], h.packedSize)
	binary.BigEndian.PutUint64(out[24:32], h.recordCount)
	binary.BigEndian.PutUint32(out[32:36], h.recordSize)
	binary.BigEndian.PutUint32(out[36:40], h.firstSize)
	return out
}

func parseEnvelopeHeader(plaintext []byte) (envelopeHeader, error) {
	if len(plaintext) != envelopeHeaderSize {
		return envelopeHeader{}, fmt.Errorf("%w: invalid header size", ErrMalformedEnvelope)
	}
	for _, value := range plaintext[40:64] {
		if value != 0 {
			return envelopeHeader{}, fmt.Errorf("%w: nonzero reserved header bytes", ErrMalformedEnvelope)
		}
	}
	header := envelopeHeader{
		plaintextSize: binary.BigEndian.Uint64(plaintext[0:8]),
		packedStart:   binary.BigEndian.Uint64(plaintext[8:16]),
		packedSize:    binary.BigEndian.Uint64(plaintext[16:24]),
		recordCount:   binary.BigEndian.Uint64(plaintext[24:32]),
		recordSize:    binary.BigEndian.Uint32(plaintext[32:36]),
		firstSize:     binary.BigEndian.Uint32(plaintext[36:40]),
	}
	if err := header.validate(); err != nil {
		return envelopeHeader{}, err
	}
	return header, nil
}

func clearPrefix() [envelopePrefixSize]byte {
	var prefix [envelopePrefixSize]byte
	copy(prefix[:8], envelopeMagic[:])
	binary.BigEndian.PutUint16(prefix[8:10], envelopeVersion)
	binary.BigEndian.PutUint16(prefix[10:12], envelopePrefixSize)
	return prefix
}

func validatePrefix(prefix [envelopePrefixSize]byte) error {
	if string(prefix[:8]) != string(envelopeMagic[:]) {
		return fmt.Errorf("%w: invalid clear prefix", ErrMalformedEnvelope)
	}
	if binary.BigEndian.Uint16(prefix[8:10]) != envelopeVersion {
		return ErrUnsupportedVersion
	}
	if binary.BigEndian.Uint16(prefix[10:12]) != envelopePrefixSize || binary.BigEndian.Uint32(prefix[12:16]) != 0 {
		return fmt.Errorf("%w: invalid clear prefix", ErrMalformedEnvelope)
	}
	return nil
}

func headerAAD(prefix [envelopePrefixSize]byte) []byte {
	aad := make([]byte, 0, len(headerDomain)+len(prefix))
	aad = append(aad, headerDomain...)
	aad = append(aad, prefix[:]...)
	return aad
}

func recordAAD(prefix [envelopePrefixSize]byte, header [envelopeHeaderSize]byte, index uint64, actual uint32) []byte {
	aad := make([]byte, 0, len(recordDomain)+len(prefix)+len(header)+8+4)
	aad = append(aad, recordDomain...)
	aad = append(aad, prefix[:]...)
	aad = append(aad, header[:]...)
	var integers [12]byte
	binary.BigEndian.PutUint64(integers[0:8], index)
	binary.BigEndian.PutUint32(integers[8:12], actual)
	aad = append(aad, integers[:]...)
	return aad
}

func writeEncryptedHeader(w io.Writer, codec Codec, header envelopeHeader) ([envelopePrefixSize]byte, [envelopeHeaderSize]byte, error) {
	prefix := clearPrefix()
	plainHeader := header.marshal()
	if codec.CiphertextSize(envelopeHeaderSize) != envelopeHeaderSealed {
		return prefix, plainHeader, fmt.Errorf("%w: codec header size", ErrMalformedEnvelope)
	}
	sealed, err := codec.Encrypt(nil, plainHeader[:], headerAAD(prefix))
	if err != nil {
		return prefix, plainHeader, err
	}
	if len(sealed) != envelopeHeaderSealed {
		return prefix, plainHeader, fmt.Errorf("%w: codec returned invalid header size", ErrMalformedEnvelope)
	}
	if err := writeFull(w, prefix[:]); err != nil {
		return prefix, plainHeader, err
	}
	if err := writeFull(w, sealed); err != nil {
		return prefix, plainHeader, err
	}
	return prefix, plainHeader, nil
}

func addUint64(a, b uint64) (uint64, bool) {
	result := a + b
	return result, result < a
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n < 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

type recordWriter struct {
	w           io.Writer
	codec       Codec
	prefix      [envelopePrefixSize]byte
	header      [envelopeHeaderSize]byte
	geometry    envelopeHeader
	buffer      []byte
	recordIndex uint64
	written     uint64
}

func newRecordWriter(w io.Writer, codec Codec, prefix [envelopePrefixSize]byte, header [envelopeHeaderSize]byte, geometry envelopeHeader) *recordWriter {
	return &recordWriter{w: w, codec: codec, prefix: prefix, header: header, geometry: geometry, buffer: make([]byte, 0, recordSize)}
}

func (w *recordWriter) Write(data []byte) (int, error) {
	if uint64(len(data)) > w.geometry.plaintextSize-w.written {
		return 0, fmt.Errorf("%w: plaintext exceeds planned size", ErrInvalidCanonicalTarstream)
	}
	consumed := 0
	for len(data) > 0 {
		target := recordSize
		if w.recordIndex == 0 {
			target = int(w.geometry.firstSize)
		}
		remaining := target - len(w.buffer)
		if remaining > len(data) {
			remaining = len(data)
		}
		w.buffer = append(w.buffer, data[:remaining]...)
		data = data[remaining:]
		consumed += remaining
		w.written += uint64(remaining)
		if len(w.buffer) == target {
			if err := w.flush(); err != nil {
				return consumed, err
			}
		}
	}
	return consumed, nil
}

func (w *recordWriter) Close() error {
	if w.written != w.geometry.plaintextSize {
		return fmt.Errorf("%w: wrote %d of %d plaintext bytes", ErrInvalidCanonicalTarstream, w.written, w.geometry.plaintextSize)
	}
	if len(w.buffer) > 0 {
		if err := w.flush(); err != nil {
			return err
		}
	}
	if w.recordIndex != w.geometry.recordCount {
		return fmt.Errorf("%w: wrote %d of %d records", ErrInvalidCanonicalTarstream, w.recordIndex, w.geometry.recordCount)
	}
	return nil
}

func (w *recordWriter) flush() error {
	actual := len(w.buffer)
	wantSize := w.codec.CiphertextSize(actual)
	if wantSize != actual+recordOverhead {
		return fmt.Errorf("%w: codec record size", ErrMalformedEnvelope)
	}
	aad := recordAAD(w.prefix, w.header, w.recordIndex, uint32(actual))
	sealed, err := w.codec.Encrypt(nil, w.buffer, aad)
	if err != nil {
		return err
	}
	if len(sealed) != wantSize {
		return fmt.Errorf("%w: codec returned invalid record size", ErrMalformedEnvelope)
	}
	if err := writeFull(w.w, sealed); err != nil {
		return err
	}
	w.recordIndex++
	w.buffer = w.buffer[:0]
	return nil
}
