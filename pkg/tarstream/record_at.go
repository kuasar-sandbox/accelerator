package tarstream

import (
	"fmt"
	"io"
	"sync/atomic"
)

const encryptedDataOffset = envelopePrefixSize + envelopeHeaderSealed

// recordReaderAt exposes the authenticated envelope as a plaintext io.ReaderAt.
// Record addressing is O(1). Reads coalesce physical I/O, authenticate every
// selected record, copy its requested plaintext directly to the caller, and do
// not retain plaintext. Cache ownership belongs to consumers and lower layers.
type recordReaderAt struct {
	ra       io.ReaderAt
	codec    RecordCodec
	prefix   [envelopePrefixSize]byte
	header   [envelopeHeaderSize]byte
	geometry envelopeHeader

	closed atomic.Bool
}

func openRecordReaderAt(ra io.ReaderAt, artifactSize int64, codec Codec) (*recordReaderAt, error) {
	if codec == nil {
		return nil, ErrCodecRequired
	}
	if artifactSize < encryptedDataOffset {
		return nil, fmt.Errorf("%w: truncated encrypted header", ErrMalformedEnvelope)
	}
	var prefix [envelopePrefixSize]byte
	if err := readAtFull(ra, prefix[:], 0); err != nil {
		return nil, fmt.Errorf("%w: read clear prefix", ErrMalformedEnvelope)
	}
	if err := validatePrefix(prefix); err != nil {
		return nil, err
	}
	recordCodec, err := bindRecordCodec(codec, prefixSalt(prefix))
	if err != nil {
		return nil, err
	}
	if recordCodec.CiphertextSize(envelopeHeaderSize) != envelopeHeaderSealed {
		return nil, fmt.Errorf("%w: codec header size", ErrMalformedEnvelope)
	}
	sealed := make([]byte, envelopeHeaderSealed)
	if err := readAtFull(ra, sealed, envelopePrefixSize); err != nil {
		return nil, fmt.Errorf("%w: read encrypted header", ErrMalformedEnvelope)
	}
	plaintext, err := recordCodec.DecryptInPlace(sealed, headerAAD(prefix), 0)
	if err != nil {
		return nil, fmt.Errorf("%w: encrypted header", ErrAuthentication)
	}
	if len(plaintext) != envelopeHeaderSize || &plaintext[0] != &sealed[1] {
		return nil, fmt.Errorf("%w: codec violated in-place header contract", ErrMalformedEnvelope)
	}
	var plainHeader [envelopeHeaderSize]byte
	copy(plainHeader[:], plaintext)
	geometry, err := parseEnvelopeHeader(plainHeader[:])
	if err != nil {
		return nil, err
	}
	if err := validateCodecGeometry(recordCodec, geometry); err != nil {
		return nil, err
	}
	wantSize, err := geometry.physicalSize()
	if err != nil || wantSize != artifactSize {
		return nil, fmt.Errorf("%w: physical size mismatch", ErrMalformedEnvelope)
	}
	return &recordReaderAt{
		ra:       ra,
		codec:    recordCodec,
		prefix:   prefix,
		header:   plainHeader,
		geometry: geometry,
	}, nil
}

func validateCodecGeometry(codec RecordCodec, geometry envelopeHeader) error {
	lengths := []int{envelopeHeaderSize, int(geometry.firstSize), recordSize, int(geometry.recordPlainSize(geometry.recordCount - 1))}
	for _, length := range lengths {
		if length < 0 || codec.CiphertextSize(length) != length+recordOverhead {
			return fmt.Errorf("%w: codec ciphertext geometry", ErrMalformedEnvelope)
		}
	}
	return nil
}

func (h envelopeHeader) physicalSize() (int64, error) {
	overhead, overflow := multiplyUint64(h.recordCount, recordOverhead)
	if overflow {
		return 0, fmt.Errorf("%w: physical size overflow", ErrMalformedEnvelope)
	}
	total, overflow := addUint64(encryptedDataOffset, h.plaintextSize)
	if overflow {
		return 0, fmt.Errorf("%w: physical size overflow", ErrMalformedEnvelope)
	}
	total, overflow = addUint64(total, overhead)
	if overflow || total > uint64(^uint64(0)>>1) {
		return 0, fmt.Errorf("%w: physical size overflow", ErrMalformedEnvelope)
	}
	return int64(total), nil
}

func multiplyUint64(a, b uint64) (uint64, bool) {
	if a != 0 && b > ^uint64(0)/a {
		return 0, true
	}
	return a * b, false
}

func (h envelopeHeader) recordPlainStart(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	return uint64(h.firstSize) + (index-1)*recordSize
}

func (h envelopeHeader) recordPlainSize(index uint64) uint64 {
	start := h.recordPlainStart(index)
	remaining := h.plaintextSize - start
	target := uint64(recordSize)
	if index == 0 {
		target = uint64(h.firstSize)
	}
	if remaining < target {
		return remaining
	}
	return target
}

func (h envelopeHeader) recordForOffset(offset uint64) uint64 {
	if offset < uint64(h.firstSize) {
		return 0
	}
	return 1 + (offset-uint64(h.firstSize))/recordSize
}

func (h envelopeHeader) recordCipherOffset(index uint64) (int64, error) {
	plainStart := h.recordPlainStart(index)
	overhead, overflow := multiplyUint64(index, recordOverhead)
	if overflow {
		return 0, fmt.Errorf("%w: record offset overflow", ErrMalformedEnvelope)
	}
	offset, overflow := addUint64(encryptedDataOffset, plainStart)
	if overflow {
		return 0, fmt.Errorf("%w: record offset overflow", ErrMalformedEnvelope)
	}
	offset, overflow = addUint64(offset, overhead)
	if overflow || offset > uint64(^uint64(0)>>1) {
		return 0, fmt.Errorf("%w: record offset overflow", ErrMalformedEnvelope)
	}
	return int64(offset), nil
}

func (r *recordReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("%w: negative plaintext offset", ErrMalformedEnvelope)
	}
	if len(dst) == 0 {
		return 0, nil
	}
	plainOffset := uint64(offset)
	if plainOffset >= r.geometry.plaintextSize {
		return 0, io.EOF
	}
	want := len(dst)
	var eof error
	if uint64(want) > r.geometry.plaintextSize-plainOffset {
		want = int(r.geometry.plaintextSize - plainOffset)
		eof = io.EOF
	}
	written := 0
	for written < want {
		lastOffset := plainOffset + uint64(want-written) - 1
		last := r.geometry.recordForOffset(lastOffset)
		n, err := r.readBatch(dst[written:want], plainOffset, last)
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrNoProgress
		}
		written += n
		plainOffset += uint64(n)
	}
	return written, eof
}

func (r *recordReaderAt) checkOpen() error {
	if r.closed.Load() {
		return fmt.Errorf("tarstream: encrypted reader is closed")
	}
	return nil
}

func (r *recordReaderAt) readBatch(dst []byte, plainOffset, last uint64) (int, error) {
	if err := r.checkOpen(); err != nil {
		return 0, err
	}
	first := r.geometry.recordForOffset(plainOffset)
	end := first
	total := 0
	for end <= last {
		cipherSize := int(r.geometry.recordPlainSize(end)) + recordOverhead
		if total > 0 && total+cipherSize > maxCoalesceBytes {
			break
		}
		total += cipherSize
		end++
	}
	if end == first {
		return 0, nil
	}
	physical, err := r.geometry.recordCipherOffset(first)
	if err != nil {
		return 0, err
	}
	buffer := make([]byte, total+recordAADSize())
	slab := buffer[:total:total]
	if err := readAtFull(r.ra, slab, physical); err != nil {
		return 0, fmt.Errorf("%w: truncated encrypted record", ErrMalformedEnvelope)
	}

	aad := buffer[total:]
	initRecordAAD(aad, r.prefix, r.header)
	position := 0
	written := 0
	for index := first; index < end; index++ {
		plainSize := int(r.geometry.recordPlainSize(index))
		cipherSize := plainSize + recordOverhead
		part := slab[position : position+cipherSize]
		if index == ^uint64(0) {
			return written, fmt.Errorf("%w: record sequence overflow", ErrMalformedEnvelope)
		}
		setRecordAAD(aad, index, uint32(plainSize))
		plaintext, err := r.codec.DecryptInPlace(part, aad, index+1)
		if err != nil {
			return written, fmt.Errorf("%w: encrypted data record", ErrAuthentication)
		}
		if len(plaintext) != plainSize || plainSize > 0 && &plaintext[0] != &part[1] {
			return written, fmt.Errorf("%w: codec violated in-place record contract", ErrMalformedEnvelope)
		}
		recordStart := r.geometry.recordPlainStart(index)
		within := 0
		if plainOffset > recordStart {
			within = int(plainOffset - recordStart)
		}
		n := min(len(dst)-written, len(plaintext)-within)
		copy(dst[written:written+n], plaintext[within:within+n])
		written += n
		plainOffset += uint64(n)
		position += cipherSize
	}
	return written, nil
}

func (r *recordReaderAt) Close() error {
	r.closed.Store(true)
	return nil
}

func readAtFull(reader io.ReaderAt, dst []byte, offset int64) error {
	for len(dst) > 0 {
		n, err := reader.ReadAt(dst, offset)
		if n < 0 || n > len(dst) {
			return io.ErrUnexpectedEOF
		}
		dst = dst[n:]
		offset += int64(n)
		if err != nil {
			if len(dst) == 0 && err == io.EOF {
				return nil
			}
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}
