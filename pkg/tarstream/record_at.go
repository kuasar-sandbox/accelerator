package tarstream

import (
	"container/list"
	"fmt"
	"io"
	"sync"
)

const encryptedDataOffset = envelopePrefixSize + envelopeHeaderSealed

type cacheEntry struct {
	index uint64
	data  []byte
}

// recordReaderAt exposes the authenticated envelope as a plaintext io.ReaderAt.
// Record addressing is O(1); the LRU owns independent record copies and is
// bounded by bytes. Coalesced slab subslices are never retained.
type recordReaderAt struct {
	ra       io.ReaderAt
	codec    Codec
	prefix   [envelopePrefixSize]byte
	header   [envelopeHeaderSize]byte
	geometry envelopeHeader

	mu         sync.Mutex
	closed     bool
	cacheBytes int
	cacheMax   int
	cache      map[uint64]*list.Element
	lru        list.List
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
	if codec.CiphertextSize(envelopeHeaderSize) != envelopeHeaderSealed {
		return nil, fmt.Errorf("%w: codec header size", ErrMalformedEnvelope)
	}
	sealed := make([]byte, envelopeHeaderSealed)
	if err := readAtFull(ra, sealed, envelopePrefixSize); err != nil {
		return nil, fmt.Errorf("%w: read encrypted header", ErrMalformedEnvelope)
	}
	plaintext, err := codec.DecryptInPlace(sealed, headerAAD(prefix))
	if err != nil {
		return nil, fmt.Errorf("%w: encrypted header", ErrAuthentication)
	}
	if len(plaintext) != envelopeHeaderSize || &plaintext[0] != &sealed[recordOverhead] {
		return nil, fmt.Errorf("%w: codec violated in-place header contract", ErrMalformedEnvelope)
	}
	var plainHeader [envelopeHeaderSize]byte
	copy(plainHeader[:], plaintext)
	geometry, err := parseEnvelopeHeader(plainHeader[:])
	if err != nil {
		return nil, err
	}
	if err := validateCodecGeometry(codec, geometry); err != nil {
		return nil, err
	}
	wantSize, err := geometry.physicalSize()
	if err != nil || wantSize != artifactSize {
		return nil, fmt.Errorf("%w: physical size mismatch", ErrMalformedEnvelope)
	}
	return &recordReaderAt{
		ra:       ra,
		codec:    codec,
		prefix:   prefix,
		header:   plainHeader,
		geometry: geometry,
		cacheMax: defaultRecordCacheMax,
		cache:    make(map[uint64]*list.Element),
	}, nil
}

func validateCodecGeometry(codec Codec, geometry envelopeHeader) error {
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
		index := r.geometry.recordForOffset(plainOffset)
		record, ok, err := r.cached(index)
		if err != nil {
			return written, err
		}
		if !ok {
			lastOffset := plainOffset + uint64(want-written) - 1
			last := r.geometry.recordForOffset(lastOffset)
			if err := r.loadBatch(index, last); err != nil {
				return written, err
			}
			record, ok, err = r.cached(index)
			if err != nil {
				return written, err
			}
			if !ok {
				return written, fmt.Errorf("%w: authenticated record missing from cache", ErrMalformedEnvelope)
			}
		}
		recordStart := r.geometry.recordPlainStart(index)
		within := int(plainOffset - recordStart)
		n := min(want-written, len(record)-within)
		copy(dst[written:written+n], record[within:within+n])
		written += n
		plainOffset += uint64(n)
	}
	return written, eof
}

func (r *recordReaderAt) cached(index uint64) ([]byte, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, false, fmt.Errorf("tarstream: encrypted reader is closed")
	}
	element, ok := r.cache[index]
	if !ok {
		return nil, false, nil
	}
	r.lru.MoveToFront(element)
	return element.Value.(*cacheEntry).data, true, nil
}

func (r *recordReaderAt) loadBatch(first, last uint64) error {
	end := first
	total := 0
	for end <= last {
		if end > first {
			if _, ok, err := r.cached(end); err != nil || ok {
				if err != nil {
					return err
				}
				break
			}
		}
		cipherSize := int(r.geometry.recordPlainSize(end)) + recordOverhead
		if total > 0 && total+cipherSize > maxCoalesceBytes {
			break
		}
		total += cipherSize
		end++
	}
	if end == first {
		return nil
	}
	physical, err := r.geometry.recordCipherOffset(first)
	if err != nil {
		return err
	}
	slab := make([]byte, total)
	if err := readAtFull(r.ra, slab, physical); err != nil {
		return fmt.Errorf("%w: truncated encrypted record", ErrMalformedEnvelope)
	}

	owned := make([]cacheEntry, 0, end-first)
	position := 0
	for index := first; index < end; index++ {
		plainSize := int(r.geometry.recordPlainSize(index))
		cipherSize := plainSize + recordOverhead
		part := slab[position : position+cipherSize]
		aad := recordAAD(r.prefix, r.header, index, uint32(plainSize))
		plaintext, err := r.codec.DecryptInPlace(part, aad)
		if err != nil {
			return fmt.Errorf("%w: encrypted data record", ErrAuthentication)
		}
		if len(plaintext) != plainSize || plainSize > 0 && &plaintext[0] != &part[recordOverhead] {
			return fmt.Errorf("%w: codec violated in-place record contract", ErrMalformedEnvelope)
		}
		owned = append(owned, cacheEntry{index: index, data: append([]byte(nil), plaintext...)})
		position += cipherSize
	}
	r.insertBatch(owned)
	return nil
}

func (r *recordReaderAt) insertBatch(entries []cacheEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	for i := range entries {
		entry := &entries[i]
		if existing, ok := r.cache[entry.index]; ok {
			r.lru.MoveToFront(existing)
			continue
		}
		element := r.lru.PushFront(entry)
		r.cache[entry.index] = element
		r.cacheBytes += len(entry.data)
	}
	for r.cacheBytes > r.cacheMax && r.lru.Len() > 1 {
		element := r.lru.Back()
		entry := element.Value.(*cacheEntry)
		delete(r.cache, entry.index)
		r.cacheBytes -= len(entry.data)
		r.lru.Remove(element)
	}
}

func (r *recordReaderAt) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.cache = nil
	r.cacheBytes = 0
	r.lru.Init()
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
