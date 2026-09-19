package tailzip

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// Footer describes relative ZIP geometry independently of entry semantics.
type Footer struct {
	Base, CentralStart, CentralSize uint64
	Count                           int
}

func parseEndRecord(end []byte, offset uint64) (Footer, error) {
	if len(end) < 22 || binary.LittleEndian.Uint32(end) != 0x06054b50 {
		return Footer{}, errors.New("EOCD is not at logical EOF")
	}
	disk := binary.LittleEndian.Uint16(end[4:])
	centralDisk := binary.LittleEndian.Uint16(end[6:])
	countDisk := binary.LittleEndian.Uint16(end[8:])
	count := binary.LittleEndian.Uint16(end[10:])
	size := binary.LittleEndian.Uint32(end[12:])
	start := binary.LittleEndian.Uint32(end[16:])
	if count == math.MaxUint16 || size == math.MaxUint32 || start == math.MaxUint32 {
		return Footer{}, errors.New("ZIP64 is unsupported")
	}
	if disk != 0 || centralDisk != 0 || countDisk != count {
		return Footer{}, errors.New("multi-disk ZIP is unsupported")
	}
	if uint64(size) > offset || uint64(start) > offset-uint64(size) {
		return Footer{}, errors.New("central directory geometry is invalid")
	}
	return Footer{Base: offset - uint64(size) - uint64(start), CentralStart: offset - uint64(size), CentralSize: uint64(size), Count: int(count)}, nil
}

// ReadFooter reads the fixed comment-free footer used by canonical artifacts.
func ReadFooter(ctx context.Context, src sparse.Source) (Footer, error) {
	if src == nil || src.Size() < 22 {
		return Footer{}, io.ErrUnexpectedEOF
	}
	end, err := readLogical(ctx, src, src.Size()-22, 22)
	if err != nil {
		return Footer{}, err
	}
	if binary.LittleEndian.Uint16(end[20:]) != 0 {
		return Footer{}, errors.New("EOCD comment is not empty")
	}
	return parseEndRecord(end, src.Size()-22)
}
func readLogical(ctx context.Context, src sparse.Source, offset, size uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset > src.Size() || size > src.Size()-offset || size > uint64(defaultLimit) {
		return nil, errors.New("tailzip: read outside bounded logical tail")
	}
	b := make([]byte, size)
	n, err := src.ReadAt(ctx, b, offset)
	if err != nil {
		return nil, err
	}
	if n != len(b) {
		return nil, io.ErrUnexpectedEOF
	}
	return b, nil
}

// ReadCanonical validates one contiguous, comment-free STORED archive.
// names is the exact ordered entry set; limits are application-supplied bounds.
// Bodies share only the bounded tail allocation and contain no payload data.
func ReadCanonical(ctx context.Context, src sparse.Source, names []string, limits map[string]int) (uint64, map[string][]byte, error) {
	footer, err := ReadFooter(ctx, src)
	if err != nil {
		return 0, nil, err
	}
	if footer.Count != len(names) {
		return 0, nil, fmt.Errorf("tailzip: %d entries, want %d", footer.Count, len(names))
	}
	if src.Size()-footer.Base > defaultLimit {
		return 0, nil, errors.New("tailzip: canonical tail exceeds limit")
	}
	raw, err := readLogical(ctx, src, footer.Base, src.Size()-footer.Base)
	if err != nil {
		return 0, nil, err
	}
	central := footer.CentralStart - footer.Base
	pos := central
	local := uint64(0)
	bodies := make(map[string][]byte, len(names))
	rangeAt := func(off, size uint64) ([]byte, error) {
		if off > uint64(len(raw)) || size > uint64(len(raw))-off {
			return nil, io.ErrUnexpectedEOF
		}
		return raw[off : off+size], nil
	}
	expectedNames := make(map[string]bool, len(names))
	for _, name := range names {
		expectedNames[name] = true
	}
	seenNames := make(map[string]bool, len(names))
	scan := central
	for index := 0; index < footer.Count; index++ {
		h, err := rangeAt(scan, 46)
		if err != nil {
			return 0, nil, err
		}
		if binary.LittleEndian.Uint32(h) != 0x02014b50 {
			return 0, nil, errors.New("tailzip: invalid central header signature")
		}
		size := uint64(binary.LittleEndian.Uint16(h[28:]))
		name, err := rangeAt(scan+46, size)
		if err != nil {
			return 0, nil, err
		}
		if !expectedNames[string(name)] {
			return 0, nil, fmt.Errorf("tailzip: unknown entry %q", name)
		}
		if seenNames[string(name)] {
			return 0, nil, fmt.Errorf("tailzip: duplicate entry %q", name)
		}
		seenNames[string(name)] = true
		scan += 46 + size + uint64(binary.LittleEndian.Uint16(h[30:])) + uint64(binary.LittleEndian.Uint16(h[32:]))
	}
	for _, wantName := range names {
		fixed, err := rangeAt(pos, 46)
		if err != nil {
			return 0, nil, err
		}
		if binary.LittleEndian.Uint32(fixed) != 0x02014b50 {
			return 0, nil, errors.New("tailzip: invalid central header signature")
		}
		if binary.LittleEndian.Uint16(fixed[4:]) != 20 || binary.LittleEndian.Uint16(fixed[6:]) != 20 || binary.LittleEndian.Uint16(fixed[8:]) != 0 || binary.LittleEndian.Uint16(fixed[10:]) != zip.Store {
			return 0, nil, errors.New("tailzip: non-canonical version, unsupported flags or method")
		}
		if binary.LittleEndian.Uint16(fixed[12:]) != 0 || binary.LittleEndian.Uint16(fixed[14:]) != 33 || binary.LittleEndian.Uint16(fixed[34:]) != 0 || binary.LittleEndian.Uint16(fixed[36:]) != 0 || binary.LittleEndian.Uint32(fixed[38:]) != 0 {
			return 0, nil, errors.New("tailzip: non-canonical metadata")
		}
		nameSize := uint64(binary.LittleEndian.Uint16(fixed[28:]))
		extra := binary.LittleEndian.Uint16(fixed[30:])
		comment := binary.LittleEndian.Uint16(fixed[32:])
		if extra != 0 || comment != 0 {
			return 0, nil, errors.New("tailzip: extra data or comment metadata")
		}
		stored := uint64(binary.LittleEndian.Uint32(fixed[20:]))
		decoded := uint64(binary.LittleEndian.Uint32(fixed[24:]))
		localOffset := uint64(binary.LittleEndian.Uint32(fixed[42:]))
		if stored == math.MaxUint32 || decoded == math.MaxUint32 || localOffset == math.MaxUint32 {
			return 0, nil, errors.New("tailzip: ZIP64 entry is unsupported")
		}
		if stored != decoded {
			return 0, nil, errors.New("tailzip: Store sizes differ")
		}
		name, err := rangeAt(pos+46, nameSize)
		if err != nil {
			return 0, nil, err
		}
		if string(name) != wantName {
			return 0, nil, fmt.Errorf("tailzip: entry order is %q, want %q", name, wantName)
		}
		if _, duplicate := bodies[wantName]; duplicate {
			return 0, nil, errors.New("tailzip: duplicate entry")
		}
		limit, ok := limits[wantName]
		if !ok || limit < 0 || decoded > uint64(limit) {
			return 0, nil, fmt.Errorf("tailzip: entry %s exceeds limit", wantName)
		}
		if localOffset != local {
			return 0, nil, errors.New("tailzip: local entries are not contiguous")
		}
		header, err := rangeAt(local, 30)
		if err != nil {
			return 0, nil, err
		}
		if binary.LittleEndian.Uint32(header) != 0x04034b50 {
			return 0, nil, errors.New("tailzip: invalid local signature")
		}
		if !bytes.Equal(header[4:26], fixed[6:28]) || binary.LittleEndian.Uint16(header[28:]) != 0 {
			return 0, nil, errors.New("tailzip: local/central header mismatch")
		}
		localNameSize := uint64(binary.LittleEndian.Uint16(header[26:]))
		localName, err := rangeAt(local+30, localNameSize)
		if err != nil {
			return 0, nil, err
		}
		if !bytes.Equal(localName, name) {
			return 0, nil, errors.New("tailzip: local/central name mismatch")
		}
		start := local + 30 + localNameSize
		if start > central || stored > central-start {
			return 0, nil, errors.New("tailzip: entry overlaps central directory")
		}
		body, err := rangeAt(start, stored)
		if err != nil {
			return 0, nil, err
		}
		if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(fixed[16:]) {
			return 0, nil, fmt.Errorf("tailzip: CRC mismatch for %s", wantName)
		}
		bodies[wantName] = body
		local = start + stored
		pos += 46 + nameSize
	}
	if local != central {
		return 0, nil, errors.New("tailzip: gap before central directory")
	}
	if pos != central+footer.CentralSize {
		return 0, nil, errors.New("tailzip: central directory size mismatch")
	}
	return footer.Base, bodies, nil
}

// EncodeCanonical preserves the fixed CreateRaw layout used by S/E writers.
func EncodeCanonical(entries []Entry) ([]byte, error) {
	total := uint64(22)
	seen := map[string]bool{}
	for _, e := range entries {
		if e.Name == "" || len(e.Name) > math.MaxUint16 || seen[e.Name] {
			return nil, errors.New("tailzip: invalid or duplicate entry name")
		}
		seen[e.Name] = true
		total += 76 + uint64(2*len(e.Name)) + uint64(len(e.Body))
		if total > defaultLimit {
			return nil, errors.New("tailzip: canonical tail exceeds limit")
		}
	}
	var out bytes.Buffer
	writer := zip.NewWriter(&out)
	for _, e := range entries {
		header := &zip.FileHeader{Name: e.Name, Method: zip.Store, Flags: 0, CreatorVersion: 20, ReaderVersion: 20, CRC32: crc32.ChecksumIEEE(e.Body), CompressedSize64: uint64(len(e.Body)), UncompressedSize64: uint64(len(e.Body)), ModifiedTime: 0, ModifiedDate: 33}
		entry, err := writer.CreateRaw(header)
		if err != nil {
			return nil, err
		}
		if _, err = entry.Write(e.Body); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// Names reads bounded central-directory names without consuming entry bodies.
// found is false only when a fixed-footer suffix is absent. Callers classify
// application roles from names before applying their full format validator.
func Names(ctx context.Context, src sparse.Source, maxEntries int, maxDirectoryBytes uint64) (names []string, found bool, err error) {
	if src == nil {
		return nil, false, errors.New("tailzip: source is required")
	}
	if src.Size() < 22 {
		return nil, false, nil
	}
	end, err := readLogical(ctx, src, src.Size()-22, 22)
	if err != nil {
		return nil, false, err
	}
	if binary.LittleEndian.Uint32(end) != 0x06054b50 {
		return nil, false, nil
	}
	if binary.LittleEndian.Uint16(end[20:]) != 0 {
		return nil, true, errors.New("tailzip: EOCD comment is not empty")
	}
	footer, err := parseEndRecord(end, src.Size()-22)
	if err != nil {
		return nil, true, err
	}
	if maxEntries < 0 || footer.Count > maxEntries || footer.CentralSize > maxDirectoryBytes {
		return nil, true, errors.New("tailzip: central directory exceeds detection limits")
	}
	directory, err := readLogical(ctx, src, footer.CentralStart, footer.CentralSize)
	if err != nil {
		return nil, true, err
	}
	position := uint64(0)
	for index := 0; index < footer.Count; index++ {
		if position > uint64(len(directory)) || uint64(len(directory))-position < 46 {
			return nil, true, io.ErrUnexpectedEOF
		}
		fixed := directory[position : position+46]
		if binary.LittleEndian.Uint32(fixed) != 0x02014b50 {
			return nil, true, errors.New("tailzip: central entry signature is invalid")
		}
		nameSize := uint64(binary.LittleEndian.Uint16(fixed[28:]))
		extraSize := uint64(binary.LittleEndian.Uint16(fixed[30:]))
		commentSize := uint64(binary.LittleEndian.Uint16(fixed[32:]))
		length := nameSize + extraSize + commentSize
		if length > uint64(len(directory))-position-46 {
			return nil, true, io.ErrUnexpectedEOF
		}
		names = append(names, string(directory[position+46:position+46+nameSize]))
		position += 46 + length
	}
	if position != uint64(len(directory)) {
		return nil, true, errors.New("tailzip: central directory size mismatch")
	}
	return names, true, nil
}
