package bundle

import (
	"archive/zip"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// verifyContainer is the explicit strict ZIP/profile path. Ordinary Open and
// Get deliberately do not call archive/zip or inspect object Local Headers;
// FullVerify and every actual source used by exact verify/upload call this
// method before content verification or any target Put.
func (r *Reader) verifyContainer(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.prepareChunks(ctx); err != nil {
		return err
	}
	if err := r.retainBlob(); err != nil {
		return err
	}
	defer r.releaseBlob()

	const directoryEndSize = 22
	var directoryEnd [directoryEndSize]byte
	if err := readFullAt(r.source, directoryEnd[:], r.size-directoryEndSize); err != nil {
		return fmt.Errorf("manifest bundle: strict read ZIP directory end: %w", err)
	}
	directoryOffset, directoryRecords, err := readCentralDirectoryLocation(r.source, r.size, directoryEnd)
	if err != nil {
		return err
	}
	if uint64(directoryOffset) != r.directoryOffset || directoryRecords != r.directoryCount {
		return fmt.Errorf("manifest bundle: ZIP tail changed after Reader.Open")
	}
	directoryEndOffset := r.size - directoryEndSize
	if binary.LittleEndian.Uint16(directoryEnd[10:12]) == math.MaxUint16 {
		directoryEndOffset -= 20 + 56 // canonical ZIP64 locator and ZIP64 EOCD
	}
	centralOffsets, err := readStrictCentralDirectoryOffsets(ctx, r.source, directoryOffset, directoryEndOffset, directoryRecords)
	if err != nil {
		return err
	}
	zr, err := zip.NewReader(r.source, r.size)
	if err != nil {
		return fmt.Errorf("manifest bundle: strict parse ZIP: %w", err)
	}
	if zr.Comment != "" {
		return fmt.Errorf("manifest bundle: ZIP archive comment is not allowed")
	}
	if len(zr.File) > maxBundleEntries || uint64(len(zr.File)) != directoryRecords {
		return fmt.Errorf("manifest bundle: Central Directory record count %d differs from parsed count %d", directoryRecords, len(zr.File))
	}

	seenNames := make(map[string]struct{}, len(zr.File))
	seenManifests := make(map[store.ContentKey]struct{}, len(r.manifests))
	seenChunks := make(map[store.ContentKey]struct{}, len(r.chunks))
	admissions := 0
	indexEntries := 0
	refsSeen := false
	var indexFile *zip.File
	var indexDataOffset int64
	var expectedOffset int64
	var localHeaderScratch [256]byte
	for position, file := range zr.File {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, duplicate := seenNames[file.Name]; duplicate {
			return fmt.Errorf("manifest bundle: duplicate ZIP entry %q", file.Name)
		}
		seenNames[file.Name] = struct{}{}
		if err := validateEntryHeader(file); err != nil {
			return err
		}
		headerOffset := expectedOffset
		dataOffset, nextOffset, err := r.validateLocalHeaderAt(file, headerOffset, localHeaderScratch[:])
		if err != nil {
			return err
		}
		if centralOffsets[position] != uint64(headerOffset) {
			return fmt.Errorf("manifest bundle: ZIP entry %q Central Directory Local Header offset differs from physical order", file.Name)
		}
		expectedOffset = nextOffset
		switch {
		case file.Name == refsName:
			if position != 0 || refsSeen {
				return fmt.Errorf("manifest bundle: %s must be the sole first ZIP entry", refsName)
			}
			if file.UncompressedSize64 == 0 || file.UncompressedSize64 > maxRefsPayload {
				return fmt.Errorf("manifest bundle: refs payload size %d is invalid", file.UncompressedSize64)
			}
			payload, err := r.readStrictEntry(file, dataOffset, maxRefsPayload)
			if err != nil {
				return err
			}
			refs, err := ParseRefs(payload)
			if err != nil {
				return err
			}
			if !equalStrings(refs, r.refs) {
				return fmt.Errorf("manifest bundle: refs metadata differs from Open-time prefix")
			}
			refsSeen = true
		case strings.HasPrefix(file.Name, admissionPrefix):
			admissions++
			if admissions != 1 {
				return fmt.Errorf("manifest bundle: multiple admission entries")
			}
			wantPosition := 0
			if refsSeen {
				wantPosition = 1
			}
			if position != wantPosition || file.UncompressedSize64 != 0 || file.CRC32 != crc32.ChecksumIEEE(nil) {
				return fmt.Errorf("manifest bundle: admission entry is not the canonical metadata prefix")
			}
			admission, err := parseAdmissionName(file.Name)
			if err != nil {
				return err
			}
			if admission != r.admission {
				return fmt.Errorf("manifest bundle: admission differs from Open-time prefix")
			}
		case strings.HasPrefix(file.Name, legacyAdmissionPrefix):
			return fmt.Errorf("manifest bundle: legacy admission entry %q is not allowed", file.Name)
		case strings.HasPrefix(file.Name, manifestPrefix):
			if admissions == 0 {
				return fmt.Errorf("manifest bundle: object entry %q precedes admission", file.Name)
			}
			key, err := parseObjectEntry(file.Name, manifestPrefix)
			if err != nil {
				return err
			}
			if file.UncompressedSize64 < 5 || file.UncompressedSize64 > uint64(codec.MaxManifestDecodedSize)+1 {
				return fmt.Errorf("manifest bundle: Manifest %s physical size %d is invalid", hex.EncodeToString(key[:]), file.UncompressedSize64)
			}
			if err := crossCheckIndexedEntry(store.PartitionManifest, key, file, dataOffset, r.manifests, seenManifests); err != nil {
				return err
			}
		case strings.HasPrefix(file.Name, chunkPrefix):
			if admissions == 0 {
				return fmt.Errorf("manifest bundle: object entry %q precedes admission", file.Name)
			}
			key, err := parseObjectEntry(file.Name, chunkPrefix)
			if err != nil {
				return err
			}
			if file.UncompressedSize64 == 0 || file.UncompressedSize64 > uint64(codec.MaxChunkDecodedSize)+1 {
				return fmt.Errorf("manifest bundle: Chunk %s physical size %d is invalid", hex.EncodeToString(key[:]), file.UncompressedSize64)
			}
			if err := crossCheckIndexedEntry(store.PartitionChunk, key, file, dataOffset, r.chunks, seenChunks); err != nil {
				return err
			}
		case file.Name == indexName:
			indexEntries++
			if indexEntries != 1 || position != len(zr.File)-1 {
				return fmt.Errorf("manifest bundle: %s must be the sole final ZIP entry", indexName)
			}
			if uint64(headerOffset) != r.index.IndexHeaderOffset || uint64(dataOffset) != r.index.IndexPayloadOffset ||
				file.UncompressedSize64 != r.index.IndexPayloadSize || file.CRC32 != r.indexCRC32 {
				return fmt.Errorf("manifest bundle: index CD/LFH location, size, or CRC differs from footer")
			}
			indexFile = file
			indexDataOffset = dataOffset
		default:
			if strings.HasPrefix(file.Name, "bundle/") {
				return fmt.Errorf("manifest bundle: unknown bundle metadata entry %q", file.Name)
			}
			return fmt.Errorf("manifest bundle: unknown ZIP entry %q", file.Name)
		}
	}
	if expectedOffset != directoryOffset {
		return fmt.Errorf("manifest bundle: local entries are not contiguous with the Central Directory")
	}
	if admissions != 1 || indexEntries != 1 || indexFile == nil {
		return fmt.Errorf("manifest bundle: exactly one admission and one final index entry are required")
	}
	if refsSeen != (len(r.refs) != 0) {
		return fmt.Errorf("manifest bundle: refs prefix presence differs from Open-time metadata")
	}
	if len(seenManifests) != len(r.manifests) || len(seenChunks) != len(r.chunks) {
		return fmt.Errorf("manifest bundle: index contains object records absent from the ZIP entries")
	}
	return r.verifyStrictIndexPayload(indexFile, indexDataOffset)
}

func readStrictCentralDirectoryOffsets(ctx context.Context, source io.ReaderAt, start, end int64, records uint64) ([]uint64, error) {
	count, err := uint64AsInt(records, "Central Directory record count")
	if err != nil {
		return nil, err
	}
	offsets := make([]uint64, count)
	position := start
	const fixedSize = 46
	var fixed [fixedSize]byte
	for index := range offsets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if position < 0 || position > end-fixedSize {
			return nil, fmt.Errorf("manifest bundle: truncated Central Directory record %d", index)
		}
		if err := readFullAt(source, fixed[:], position); err != nil {
			return nil, fmt.Errorf("manifest bundle: read Central Directory record %d: %w", index, err)
		}
		if !bytesEqual4(fixed[:4], zipDirectoryMagic) {
			return nil, fmt.Errorf("manifest bundle: invalid Central Directory signature at record %d", index)
		}
		if binary.LittleEndian.Uint32(fixed[20:24]) == math.MaxUint32 || binary.LittleEndian.Uint32(fixed[24:28]) == math.MaxUint32 {
			return nil, fmt.Errorf("manifest bundle: Central Directory record %d uses non-canonical ZIP64 object size", index)
		}
		if binary.LittleEndian.Uint16(fixed[34:36]) != 0 {
			return nil, fmt.Errorf("manifest bundle: Central Directory record %d uses a non-zero disk", index)
		}
		nameSize := int64(binary.LittleEndian.Uint16(fixed[28:30]))
		extraSize := int64(binary.LittleEndian.Uint16(fixed[30:32]))
		commentSize := int64(binary.LittleEndian.Uint16(fixed[32:34]))
		variableSize := nameSize + extraSize + commentSize
		if variableSize < 0 || variableSize > end-position-fixedSize {
			return nil, fmt.Errorf("manifest bundle: truncated Central Directory variable fields at record %d", index)
		}
		localOffset32 := binary.LittleEndian.Uint32(fixed[42:46])
		if localOffset32 != math.MaxUint32 {
			if extraSize != 0 {
				return nil, fmt.Errorf("manifest bundle: Central Directory record %d has an unnecessary extra field", index)
			}
			offsets[index] = uint64(localOffset32)
		} else {
			if extraSize != 12 {
				return nil, fmt.Errorf("manifest bundle: Central Directory record %d has a non-canonical ZIP64 offset extra", index)
			}
			var extra [12]byte
			extraOffset := position + fixedSize + nameSize
			if err := readFullAt(source, extra[:], extraOffset); err != nil {
				return nil, fmt.Errorf("manifest bundle: read ZIP64 offset extra at record %d: %w", index, err)
			}
			if binary.LittleEndian.Uint16(extra[0:2]) != 0x0001 || binary.LittleEndian.Uint16(extra[2:4]) != 8 {
				return nil, fmt.Errorf("manifest bundle: Central Directory record %d has an invalid ZIP64 offset extra", index)
			}
			offsets[index] = binary.LittleEndian.Uint64(extra[4:12])
			if offsets[index] < math.MaxUint32 {
				return nil, fmt.Errorf("manifest bundle: Central Directory record %d uses an unnecessary ZIP64 offset", index)
			}
		}
		position += fixedSize + variableSize
	}
	if position != end {
		return nil, fmt.Errorf("manifest bundle: Central Directory records do not exactly fill the declared range")
	}
	return offsets, nil
}

func crossCheckIndexedEntry(
	partition store.Partition,
	key store.ContentKey,
	file *zip.File,
	dataOffset int64,
	indexed map[store.ContentKey]archiveEntry,
	seen map[store.ContentKey]struct{},
) error {
	if _, duplicate := seen[key]; duplicate {
		return fmt.Errorf("manifest bundle: duplicate %s key %s", partition, hex.EncodeToString(key[:]))
	}
	want, ok := indexed[key]
	if !ok {
		return fmt.Errorf("manifest bundle: unindexed %s entry %s", partition, hex.EncodeToString(key[:]))
	}
	if dataOffset < 0 || want.dataOffset != uint64(dataOffset) || uint64(want.size) != file.UncompressedSize64 || want.crc32 != file.CRC32 {
		return fmt.Errorf("manifest bundle: %s %s index record differs from CD/LFH/data range", partition, hex.EncodeToString(key[:]))
	}
	seen[key] = struct{}{}
	return nil
}

func (r *Reader) readStrictEntry(file *zip.File, dataOffset int64, limit uint64) ([]byte, error) {
	if file.UncompressedSize64 > limit {
		return nil, fmt.Errorf("manifest bundle: ZIP entry %q exceeds strict read limit %d", file.Name, limit)
	}
	size, err := uint64AsInt(file.UncompressedSize64, "ZIP entry payload")
	if err != nil {
		return nil, err
	}
	payload := make([]byte, size)
	if err := readFullAt(r.source, payload, dataOffset); err != nil {
		return nil, fmt.Errorf("manifest bundle: strict read ZIP entry %q: %w", file.Name, err)
	}
	if crc32.ChecksumIEEE(payload) != file.CRC32 {
		return nil, fmt.Errorf("manifest bundle: ZIP entry %q CRC mismatch", file.Name)
	}
	return payload, nil
}

func (r *Reader) verifyStrictIndexPayload(file *zip.File, dataOffset int64) error {
	payload, err := r.readStrictEntry(file, dataOffset, maxIndexPayloadSize)
	if err != nil {
		return err
	}
	if len(payload) < indexFooterSize {
		return fmt.Errorf("manifest bundle: index payload is shorter than footer")
	}
	footer, err := decodeIndexFooter(payload[len(payload)-indexFooterSize:], r.directoryOffset)
	if err != nil {
		return err
	}
	if footer != r.index {
		return fmt.Errorf("manifest bundle: strict index footer differs from Open-time footer")
	}
	chunkBytes, err := indexSectionSlice(payload, footer, footer.Chunk)
	if err != nil {
		return err
	}
	chunks, err := decodeIndexRecords(chunkBytes, footer.Chunk, store.PartitionChunk, footer)
	if err != nil {
		return err
	}
	manifestBytes, err := indexSectionSlice(payload, footer, footer.Manifest)
	if err != nil {
		return err
	}
	manifests, err := decodeIndexRecords(manifestBytes, footer.Manifest, store.PartitionManifest, footer)
	if err != nil {
		return err
	}
	if !equalEntryMaps(chunks, r.chunks) || !equalEntryMaps(manifests, r.manifests) {
		return fmt.Errorf("manifest bundle: strict index records differ from Open-time indexes")
	}
	return nil
}

func indexSectionSlice(payload []byte, footer indexFooter, section indexSection) ([]byte, error) {
	if section.Offset < footer.IndexPayloadOffset {
		return nil, fmt.Errorf("manifest bundle: index section precedes payload")
	}
	start := section.Offset - footer.IndexPayloadOffset
	end, err := checkedAdd64(start, section.Size)
	if err != nil || end > uint64(len(payload)-indexFooterSize) {
		return nil, fmt.Errorf("manifest bundle: index section is outside record payload")
	}
	return payload[int(start):int(end)], nil
}

func equalEntryMaps(left, right map[store.ContentKey]archiveEntry) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if other, ok := right[key]; !ok || other != value {
			return false
		}
	}
	return true
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
