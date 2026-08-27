package bundle

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"sort"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

const (
	indexVersion    = 1
	indexRecordSize = 48
	indexFooterSize = 256

	// One admission and the mandatory index consume at least two ZIP entries.
	// A refs entry, when present, is accounted for after the metadata prefix is
	// decoded. This bound is also the allocation ceiling for either section.
	maxIndexRecords     = maxBundleEntries - 2
	maxIndexPayloadSize = maxIndexRecords*indexRecordSize + indexFooterSize

	zipLocalHeaderFixedSize = 30
	indexLocalHeaderSize    = zipLocalHeaderFixedSize + len(indexName)
)

var (
	indexMagic    = [16]byte{'K', 'U', 'A', 'S', 'A', 'R', 'B', 'N', 'D', 'L', 'I', 'N', 'D', 'E', 'X', '1'}
	indexCRCTable = crc32.MakeTable(crc32.Castagnoli)
)

// indexRecord is encoded explicitly as 32-byte key, uint64 data offset,
// uint32 size, and uint32 CRC32. Its Go layout is never used as wire format.
type indexRecord struct {
	Key        store.ContentKey
	DataOffset uint64
	Size       uint32
	CRC32      uint32
}

type indexSection struct {
	Offset uint64
	Count  uint64
	Size   uint64
	Digest [sha256.Size]byte
}

type indexFooter struct {
	IndexPayloadOffset uint64
	IndexPayloadSize   uint64
	IndexHeaderOffset  uint64
	MetadataPrefixEnd  uint64
	Manifest           indexSection
	Chunk              indexSection
}

func checkedAdd64(left, right uint64) (uint64, error) {
	if right > math.MaxUint64-left {
		return 0, fmt.Errorf("unsigned addition overflows: %d + %d", left, right)
	}
	return left + right, nil
}

func checkedMul64(left, right uint64) (uint64, error) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, fmt.Errorf("unsigned multiplication overflows: %d * %d", left, right)
	}
	return left * right, nil
}

func uint64AsInt(value uint64, field string) (int, error) {
	if value > uint64(maxInt()) {
		return 0, fmt.Errorf("manifest bundle: %s %d exceeds platform int", field, value)
	}
	return int(value), nil
}

func uint64AsInt64(value uint64, field string) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("manifest bundle: %s %d exceeds ReaderAt offset range", field, value)
	}
	return int64(value), nil
}

func encodeIndexRecord(dst []byte, record indexRecord) error {
	if len(dst) != indexRecordSize {
		return fmt.Errorf("manifest bundle: index record buffer has %d bytes, want %d", len(dst), indexRecordSize)
	}
	copy(dst[:32], record.Key[:])
	binary.LittleEndian.PutUint64(dst[32:40], record.DataOffset)
	binary.LittleEndian.PutUint32(dst[40:44], record.Size)
	binary.LittleEndian.PutUint32(dst[44:48], record.CRC32)
	return nil
}

func decodeIndexRecord(src []byte) (indexRecord, error) {
	var record indexRecord
	if len(src) != indexRecordSize {
		return record, fmt.Errorf("manifest bundle: index record has %d bytes, want %d", len(src), indexRecordSize)
	}
	copy(record.Key[:], src[:32])
	record.DataOffset = binary.LittleEndian.Uint64(src[32:40])
	record.Size = binary.LittleEndian.Uint32(src[40:44])
	record.CRC32 = binary.LittleEndian.Uint32(src[44:48])
	return record, nil
}

func encodeIndexFooter(footer indexFooter) ([indexFooterSize]byte, error) {
	var encoded [indexFooterSize]byte
	if err := validateIndexFooterFields(footer); err != nil {
		return encoded, err
	}
	copy(encoded[:16], indexMagic[:])
	binary.LittleEndian.PutUint16(encoded[16:18], indexVersion)
	binary.LittleEndian.PutUint16(encoded[18:20], indexFooterSize)
	binary.LittleEndian.PutUint16(encoded[20:22], indexRecordSize)
	// [22:24] is reserved and remains zero.
	binary.LittleEndian.PutUint64(encoded[24:32], footer.IndexPayloadOffset)
	binary.LittleEndian.PutUint64(encoded[32:40], footer.IndexPayloadSize)
	binary.LittleEndian.PutUint64(encoded[40:48], footer.IndexHeaderOffset)
	binary.LittleEndian.PutUint64(encoded[48:56], footer.MetadataPrefixEnd)
	binary.LittleEndian.PutUint64(encoded[56:64], footer.Manifest.Offset)
	binary.LittleEndian.PutUint64(encoded[64:72], footer.Manifest.Count)
	binary.LittleEndian.PutUint64(encoded[72:80], footer.Manifest.Size)
	copy(encoded[80:112], footer.Manifest.Digest[:])
	binary.LittleEndian.PutUint64(encoded[112:120], footer.Chunk.Offset)
	binary.LittleEndian.PutUint64(encoded[120:128], footer.Chunk.Count)
	binary.LittleEndian.PutUint64(encoded[128:136], footer.Chunk.Size)
	copy(encoded[136:168], footer.Chunk.Digest[:])
	// [168:252] is reserved and remains zero. The checksum covers every byte
	// before the checksum field, including all reserved bytes.
	binary.LittleEndian.PutUint32(encoded[252:256], crc32.Checksum(encoded[:252], indexCRCTable))
	return encoded, nil
}

func decodeIndexFooter(encoded []byte, directoryOffset uint64) (indexFooter, error) {
	var footer indexFooter
	if len(encoded) != indexFooterSize {
		return footer, fmt.Errorf("manifest bundle: index footer has %d bytes, want %d", len(encoded), indexFooterSize)
	}
	if !bytes.Equal(encoded[:16], indexMagic[:]) {
		return footer, fmt.Errorf("manifest bundle: invalid index footer magic")
	}
	if version := binary.LittleEndian.Uint16(encoded[16:18]); version != indexVersion {
		return footer, fmt.Errorf("manifest bundle: unsupported index version %d", version)
	}
	if size := binary.LittleEndian.Uint16(encoded[18:20]); size != indexFooterSize {
		return footer, fmt.Errorf("manifest bundle: index footer size %d, want %d", size, indexFooterSize)
	}
	if size := binary.LittleEndian.Uint16(encoded[20:22]); size != indexRecordSize {
		return footer, fmt.Errorf("manifest bundle: index record size %d, want %d", size, indexRecordSize)
	}
	if encoded[22] != 0 || encoded[23] != 0 || !allZero(encoded[168:252]) {
		return footer, fmt.Errorf("manifest bundle: index footer reserved bytes are non-zero")
	}
	wantChecksum := binary.LittleEndian.Uint32(encoded[252:256])
	if got := crc32.Checksum(encoded[:252], indexCRCTable); got != wantChecksum {
		return footer, fmt.Errorf("manifest bundle: index footer checksum mismatch")
	}
	footer.IndexPayloadOffset = binary.LittleEndian.Uint64(encoded[24:32])
	footer.IndexPayloadSize = binary.LittleEndian.Uint64(encoded[32:40])
	footer.IndexHeaderOffset = binary.LittleEndian.Uint64(encoded[40:48])
	footer.MetadataPrefixEnd = binary.LittleEndian.Uint64(encoded[48:56])
	footer.Manifest.Offset = binary.LittleEndian.Uint64(encoded[56:64])
	footer.Manifest.Count = binary.LittleEndian.Uint64(encoded[64:72])
	footer.Manifest.Size = binary.LittleEndian.Uint64(encoded[72:80])
	copy(footer.Manifest.Digest[:], encoded[80:112])
	footer.Chunk.Offset = binary.LittleEndian.Uint64(encoded[112:120])
	footer.Chunk.Count = binary.LittleEndian.Uint64(encoded[120:128])
	footer.Chunk.Size = binary.LittleEndian.Uint64(encoded[128:136])
	copy(footer.Chunk.Digest[:], encoded[136:168])
	if err := validateIndexFooterFields(footer); err != nil {
		return indexFooter{}, err
	}
	payloadEnd, _ := checkedAdd64(footer.IndexPayloadOffset, footer.IndexPayloadSize)
	if payloadEnd != directoryOffset {
		return indexFooter{}, fmt.Errorf("manifest bundle: index payload end %d differs from Central Directory offset %d", payloadEnd, directoryOffset)
	}
	footerOffset, _ := checkedAdd64(footer.Manifest.Offset, footer.Manifest.Size)
	footerEnd, err := checkedAdd64(footerOffset, indexFooterSize)
	if err != nil || footerEnd != directoryOffset {
		return indexFooter{}, fmt.Errorf("manifest bundle: index footer is not adjacent to the Central Directory")
	}
	return footer, nil
}

func validateIndexFooterFields(footer indexFooter) error {
	if footer.MetadataPrefixEnd == 0 || footer.MetadataPrefixEnd > maxMetadataPrefixBytes {
		return fmt.Errorf("manifest bundle: metadata prefix end %d is outside limit %d", footer.MetadataPrefixEnd, maxMetadataPrefixBytes)
	}
	if footer.IndexPayloadSize < indexFooterSize || footer.IndexPayloadSize > maxIndexPayloadSize {
		return fmt.Errorf("manifest bundle: index payload size %d is outside [%d,%d]", footer.IndexPayloadSize, indexFooterSize, maxIndexPayloadSize)
	}
	payloadOffset, err := checkedAdd64(footer.IndexHeaderOffset, uint64(indexLocalHeaderSize))
	if err != nil || payloadOffset != footer.IndexPayloadOffset {
		return fmt.Errorf("manifest bundle: index payload offset does not follow its canonical Local Header")
	}
	if footer.IndexHeaderOffset < footer.MetadataPrefixEnd {
		return fmt.Errorf("manifest bundle: index Local Header overlaps metadata prefix")
	}
	manifestSize, err := checkedMul64(footer.Manifest.Count, indexRecordSize)
	if err != nil || manifestSize != footer.Manifest.Size {
		return fmt.Errorf("manifest bundle: Manifest index section size/count mismatch")
	}
	chunkSize, err := checkedMul64(footer.Chunk.Count, indexRecordSize)
	if err != nil || chunkSize != footer.Chunk.Size {
		return fmt.Errorf("manifest bundle: Chunk index section size/count mismatch")
	}
	if footer.Manifest.Count == 0 {
		return fmt.Errorf("manifest bundle: index must contain at least one Manifest record")
	}
	totalCount, err := checkedAdd64(footer.Manifest.Count, footer.Chunk.Count)
	if err != nil || totalCount > maxIndexRecords {
		return fmt.Errorf("manifest bundle: index record count exceeds limit %d", maxIndexRecords)
	}
	if footer.Chunk.Offset != footer.IndexPayloadOffset {
		return fmt.Errorf("manifest bundle: Chunk index section is not first in index payload")
	}
	chunkEnd, err := checkedAdd64(footer.Chunk.Offset, footer.Chunk.Size)
	if err != nil || footer.Manifest.Offset != chunkEnd {
		return fmt.Errorf("manifest bundle: index sections overlap or contain a gap")
	}
	manifestEnd, err := checkedAdd64(footer.Manifest.Offset, footer.Manifest.Size)
	if err != nil {
		return fmt.Errorf("manifest bundle: Manifest index section range overflows")
	}
	payloadEnd, err := checkedAdd64(footer.IndexPayloadOffset, footer.IndexPayloadSize)
	if err != nil {
		return fmt.Errorf("manifest bundle: index payload range overflows")
	}
	footerEnd, err := checkedAdd64(manifestEnd, indexFooterSize)
	if err != nil || footerEnd != payloadEnd {
		return fmt.Errorf("manifest bundle: index sections do not exactly precede the footer")
	}
	if _, err := uint64AsInt64(payloadEnd, "index payload end"); err != nil {
		return err
	}
	return nil
}

func allZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

func buildIndexPayload(metadataPrefixEnd, indexHeaderOffset uint64, manifestRecords, chunkRecords []indexRecord) ([]byte, indexFooter, error) {
	manifests := append([]indexRecord(nil), manifestRecords...)
	chunks := append([]indexRecord(nil), chunkRecords...)
	sortIndexRecords(manifests)
	sortIndexRecords(chunks)
	if err := validateIndexRecords(manifests, store.PartitionManifest, metadataPrefixEnd, indexHeaderOffset); err != nil {
		return nil, indexFooter{}, err
	}
	if err := validateIndexRecords(chunks, store.PartitionChunk, metadataPrefixEnd, indexHeaderOffset); err != nil {
		return nil, indexFooter{}, err
	}
	totalCount, err := checkedAdd64(uint64(len(manifests)), uint64(len(chunks)))
	if err != nil || totalCount > maxIndexRecords {
		return nil, indexFooter{}, fmt.Errorf("manifest bundle: index record count exceeds limit %d", maxIndexRecords)
	}
	chunkBytes, err := encodeIndexRecords(chunks)
	if err != nil {
		return nil, indexFooter{}, err
	}
	manifestBytes, err := encodeIndexRecords(manifests)
	if err != nil {
		return nil, indexFooter{}, err
	}
	payloadOffset, err := checkedAdd64(indexHeaderOffset, uint64(indexLocalHeaderSize))
	if err != nil {
		return nil, indexFooter{}, fmt.Errorf("manifest bundle: index Local Header range overflows")
	}
	manifestOffset, err := checkedAdd64(payloadOffset, uint64(len(chunkBytes)))
	if err != nil {
		return nil, indexFooter{}, fmt.Errorf("manifest bundle: Chunk index section range overflows")
	}
	payloadSize, err := checkedAdd64(uint64(len(chunkBytes)+len(manifestBytes)), indexFooterSize)
	if err != nil || payloadSize > maxIndexPayloadSize {
		return nil, indexFooter{}, fmt.Errorf("manifest bundle: index payload exceeds %d bytes", maxIndexPayloadSize)
	}
	footer := indexFooter{
		IndexPayloadOffset: payloadOffset,
		IndexPayloadSize:   payloadSize,
		IndexHeaderOffset:  indexHeaderOffset,
		MetadataPrefixEnd:  metadataPrefixEnd,
		Manifest: indexSection{
			Offset: manifestOffset,
			Count:  uint64(len(manifests)),
			Size:   uint64(len(manifestBytes)),
			Digest: sha256.Sum256(manifestBytes),
		},
		Chunk: indexSection{
			Offset: payloadOffset,
			Count:  uint64(len(chunks)),
			Size:   uint64(len(chunkBytes)),
			Digest: sha256.Sum256(chunkBytes),
		},
	}
	encodedFooter, err := encodeIndexFooter(footer)
	if err != nil {
		return nil, indexFooter{}, err
	}
	payload := make([]byte, 0, int(payloadSize))
	payload = append(payload, chunkBytes...)
	payload = append(payload, manifestBytes...)
	payload = append(payload, encodedFooter[:]...)
	return payload, footer, nil
}

func sortIndexRecords(records []indexRecord) {
	sort.Slice(records, func(left, right int) bool {
		return bytes.Compare(records[left].Key[:], records[right].Key[:]) < 0
	})
}

func encodeIndexRecords(records []indexRecord) ([]byte, error) {
	size, err := checkedMul64(uint64(len(records)), indexRecordSize)
	if err != nil || size > maxIndexPayloadSize {
		return nil, fmt.Errorf("manifest bundle: index record section is too large")
	}
	length, err := uint64AsInt(size, "index record section")
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, length)
	for index, record := range records {
		if err := encodeIndexRecord(encoded[index*indexRecordSize:(index+1)*indexRecordSize], record); err != nil {
			return nil, err
		}
	}
	return encoded, nil
}

func decodeIndexRecords(encoded []byte, section indexSection, partition store.Partition, footer indexFooter) (map[store.ContentKey]archiveEntry, error) {
	if uint64(len(encoded)) != section.Size {
		return nil, fmt.Errorf("manifest bundle: %s index read %d bytes, want %d", partition, len(encoded), section.Size)
	}
	if sha256.Sum256(encoded) != section.Digest {
		return nil, fmt.Errorf("manifest bundle: %s index section digest mismatch", partition)
	}
	count, err := uint64AsInt(section.Count, string(partition)+" index count")
	if err != nil {
		return nil, err
	}
	records := make([]indexRecord, count)
	for index := range records {
		record, err := decodeIndexRecord(encoded[index*indexRecordSize : (index+1)*indexRecordSize])
		if err != nil {
			return nil, err
		}
		records[index] = record
	}
	if err := validateIndexRecords(records, partition, footer.MetadataPrefixEnd, footer.IndexHeaderOffset); err != nil {
		return nil, err
	}
	entries := make(map[store.ContentKey]archiveEntry, len(records))
	for _, record := range records {
		entries[record.Key] = archiveEntry{dataOffset: record.DataOffset, size: record.Size, crc32: record.CRC32}
	}
	return entries, nil
}

func validateIndexRecords(records []indexRecord, partition store.Partition, metadataPrefixEnd, indexHeaderOffset uint64) error {
	if len(records) > maxIndexRecords {
		return fmt.Errorf("manifest bundle: %s index record count exceeds limit %d", partition, maxIndexRecords)
	}
	for index, record := range records {
		if index != 0 && bytes.Compare(records[index-1].Key[:], record.Key[:]) >= 0 {
			return fmt.Errorf("manifest bundle: %s index records are not strictly sorted at record %d", partition, index)
		}
		switch partition {
		case store.PartitionManifest:
			if record.Size < 5 || uint64(record.Size) > uint64(codec.MaxManifestDecodedSize)+1 {
				return fmt.Errorf("manifest bundle: Manifest %x physical size %d is invalid", record.Key, record.Size)
			}
		case store.PartitionChunk:
			if record.Size == 0 || uint64(record.Size) > uint64(codec.MaxChunkDecodedSize)+1 {
				return fmt.Errorf("manifest bundle: Chunk %x physical size %d is invalid", record.Key, record.Size)
			}
		default:
			return fmt.Errorf("manifest bundle: unsupported index partition %q", partition)
		}
		end, err := checkedAdd64(record.DataOffset, uint64(record.Size))
		if err != nil {
			return fmt.Errorf("manifest bundle: %s %x data range overflows", partition, record.Key)
		}
		if record.DataOffset < metadataPrefixEnd || end > indexHeaderOffset {
			return fmt.Errorf("manifest bundle: %s %x data range [%d,%d) is outside the object region", partition, record.Key, record.DataOffset, end)
		}
	}
	return nil
}

type indexedObjectRange struct {
	start     uint64
	end       uint64
	partition store.Partition
	key       store.ContentKey
}

func validateArchiveEntryRanges(manifests, chunks map[store.ContentKey]archiveEntry) error {
	ranges := make([]indexedObjectRange, 0, len(manifests)+len(chunks))
	appendEntries := func(partition store.Partition, entries map[store.ContentKey]archiveEntry) error {
		for key, entry := range entries {
			end, err := checkedAdd64(entry.dataOffset, uint64(entry.size))
			if err != nil {
				return fmt.Errorf("manifest bundle: %s %x data range overflows", partition, key)
			}
			ranges = append(ranges, indexedObjectRange{start: entry.dataOffset, end: end, partition: partition, key: key})
		}
		return nil
	}
	if err := appendEntries(store.PartitionManifest, manifests); err != nil {
		return err
	}
	if err := appendEntries(store.PartitionChunk, chunks); err != nil {
		return err
	}
	sort.Slice(ranges, func(left, right int) bool {
		if ranges[left].start != ranges[right].start {
			return ranges[left].start < ranges[right].start
		}
		return ranges[left].end < ranges[right].end
	})
	for index := 1; index < len(ranges); index++ {
		previous := ranges[index-1]
		current := ranges[index]
		if current.start < previous.end {
			return fmt.Errorf("manifest bundle: indexed object ranges overlap: %s %x and %s %x", previous.partition, previous.key, current.partition, current.key)
		}
	}
	return nil
}
