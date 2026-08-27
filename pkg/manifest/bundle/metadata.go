package bundle

import (
	"archive/zip"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// Metadata is the immutable Bundle metadata prefix. ReadMetadata validates
// only this prefix; callers that consume objects must still use Open/NewReader
// to require and validate the indexed profile.
type Metadata struct {
	refs      []string
	admission store.WriteAdmission
}

func (m Metadata) Refs() []string                  { return append([]string(nil), m.refs...) }
func (m Metadata) Admission() store.WriteAdmission { return m.admission }

// OpenMetadata reads refs/admission without mmap or Central Directory/object
// indexing. It is intended for preflight location discovery.
func OpenMetadata(path string) (Metadata, error) {
	file, err := os.Open(path)
	if err != nil {
		return Metadata{}, fmt.Errorf("manifest bundle: open metadata %s: %w", path, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return Metadata{}, fmt.Errorf("manifest bundle: stat metadata %s: %w", path, err)
	}
	return ReadMetadata(file, info.Size())
}

// ReadMetadata reads the continuous local-header prefix at offset zero. It
// does not read Manifest/Chunk payloads or the Central Directory.
func ReadMetadata(source io.ReaderAt, size int64) (Metadata, error) {
	metadata, _, _, err := readMetadataPrefix(source, size)
	return metadata, err
}

// readMetadataPrefix returns the exact end of the canonical refs/admission
// prefix and its physical entry count. NewReader first obtains that end from
// the validated index footer, performs one source range read, and invokes this
// parser against the in-memory buffer. ReadMetadata keeps its lighter prefix-
// only contract by calling the same parser directly on its source.
func readMetadataPrefix(source io.ReaderAt, size int64) (Metadata, int64, int, error) {
	if source == nil {
		return Metadata{}, 0, 0, fmt.Errorf("manifest bundle: metadata source is required")
	}
	first, err := readPrefixEntry(source, size, 0)
	if err != nil {
		return Metadata{}, 0, 0, err
	}
	var metadata Metadata
	admissionEntry := first
	entryCount := 1
	if first.name == refsName {
		if first.size == 0 {
			return Metadata{}, 0, 0, fmt.Errorf("manifest bundle: refs payload must be non-empty")
		}
		if first.size > maxRefsPayload {
			return Metadata{}, 0, 0, fmt.Errorf("manifest bundle: refs payload exceeds %d bytes", maxRefsPayload)
		}
		payload := make([]byte, first.size)
		if err := readFullAt(source, payload, first.dataOffset); err != nil {
			return Metadata{}, 0, 0, fmt.Errorf("manifest bundle: read %s: %w", refsName, err)
		}
		if crc32.ChecksumIEEE(payload) != first.crc {
			return Metadata{}, 0, 0, fmt.Errorf("manifest bundle: %s CRC mismatch", refsName)
		}
		metadata.refs, err = ParseRefs(payload)
		if err != nil {
			return Metadata{}, 0, 0, err
		}
		admissionEntry, err = readPrefixEntry(source, size, first.endOffset)
		if err != nil {
			return Metadata{}, 0, 0, err
		}
		entryCount = 2
	}
	if strings.HasPrefix(admissionEntry.name, legacyAdmissionPrefix) {
		return Metadata{}, 0, 0, fmt.Errorf("manifest bundle: legacy admission entry %q is not allowed", admissionEntry.name)
	}
	if !strings.HasPrefix(admissionEntry.name, admissionPrefix) {
		return Metadata{}, 0, 0, fmt.Errorf("manifest bundle: metadata prefix expected admission, got %q", admissionEntry.name)
	}
	if admissionEntry.size != 0 || admissionEntry.crc != crc32.ChecksumIEEE(nil) {
		return Metadata{}, 0, 0, fmt.Errorf("manifest bundle: admission payload must be empty")
	}
	metadata.admission, err = parseAdmissionName(admissionEntry.name)
	if err != nil {
		return Metadata{}, 0, 0, err
	}
	wantSalt, err := store.SaltForGeneration(metadata.admission.Generation)
	if err != nil {
		return Metadata{}, 0, 0, fmt.Errorf("manifest bundle: admission: %w", err)
	}
	if metadata.admission.Salt != wantSalt {
		return Metadata{}, 0, 0, fmt.Errorf("manifest bundle: admission salt is not canonical for generation %q", metadata.admission.Generation)
	}
	return metadata, admissionEntry.endOffset, entryCount, nil
}

type prefixEntry struct {
	name       string
	dataOffset int64
	endOffset  int64
	size       int
	crc        uint32
}

func readPrefixEntry(source io.ReaderAt, archiveSize, offset int64) (prefixEntry, error) {
	const fixedSize = 30
	if archiveSize < 0 || offset < 0 || fixedSize > archiveSize-offset {
		return prefixEntry{}, fmt.Errorf("manifest bundle: truncated metadata local header at offset %d", offset)
	}
	var fixed [fixedSize]byte
	if err := readFullAt(source, fixed[:], offset); err != nil {
		return prefixEntry{}, fmt.Errorf("manifest bundle: read metadata local header at offset %d: %w", offset, err)
	}
	if !bytesEqual4(fixed[:4], zipLocalHeaderMagic) {
		return prefixEntry{}, fmt.Errorf("manifest bundle: invalid metadata local-header magic at offset %d", offset)
	}
	if version := binary.LittleEndian.Uint16(fixed[4:6]); version != 45 {
		return prefixEntry{}, fmt.Errorf("manifest bundle: metadata local reader version %d is not canonical", version)
	}
	if flags := binary.LittleEndian.Uint16(fixed[6:8]); flags != 0 {
		return prefixEntry{}, fmt.Errorf("manifest bundle: metadata local flags 0x%x are not allowed", flags)
	}
	if method := binary.LittleEndian.Uint16(fixed[8:10]); method != zip.Store {
		return prefixEntry{}, fmt.Errorf("manifest bundle: metadata local method %d, want Store", method)
	}
	if binary.LittleEndian.Uint16(fixed[10:12]) != 0 || binary.LittleEndian.Uint16(fixed[12:14]) != 0 {
		return prefixEntry{}, fmt.Errorf("manifest bundle: metadata local timestamp is not canonical")
	}
	compressed := binary.LittleEndian.Uint32(fixed[18:22])
	uncompressed := binary.LittleEndian.Uint32(fixed[22:26])
	if compressed != uncompressed || uint64(uncompressed) > math.MaxInt {
		return prefixEntry{}, fmt.Errorf("manifest bundle: metadata Store sizes are invalid")
	}
	nameSize := int(binary.LittleEndian.Uint16(fixed[26:28]))
	if nameSize == 0 {
		return prefixEntry{}, fmt.Errorf("manifest bundle: metadata entry name is empty")
	}
	if binary.LittleEndian.Uint16(fixed[28:30]) != 0 {
		return prefixEntry{}, fmt.Errorf("manifest bundle: metadata local extra field is not allowed")
	}
	nameOffset := offset + fixedSize
	if int64(nameSize) > archiveSize-nameOffset {
		return prefixEntry{}, fmt.Errorf("manifest bundle: truncated metadata entry name")
	}
	nameBytes := make([]byte, nameSize)
	if err := readFullAt(source, nameBytes, nameOffset); err != nil {
		return prefixEntry{}, fmt.Errorf("manifest bundle: read metadata entry name: %w", err)
	}
	dataOffset := nameOffset + int64(nameSize)
	size := int(uncompressed)
	if int64(size) > archiveSize-dataOffset {
		return prefixEntry{}, fmt.Errorf("manifest bundle: truncated metadata entry %q", string(nameBytes))
	}
	return prefixEntry{
		name:       string(nameBytes),
		dataOffset: dataOffset,
		endOffset:  dataOffset + int64(size),
		size:       size,
		crc:        binary.LittleEndian.Uint32(fixed[14:18]),
	}, nil
}
