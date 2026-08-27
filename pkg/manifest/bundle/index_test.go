package bundle

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestIndexRecordAndFooterCanonicalCodec(t *testing.T) {
	record := indexRecord{Key: store.ContentKey{0: 1, 31: 2}, DataOffset: 0x0102030405060708, Size: 0x11223344, CRC32: 0xaabbccdd}
	encodedRecord := make([]byte, indexRecordSize)
	if err := encodeIndexRecord(encodedRecord, record); err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint64(encodedRecord[32:40]); got != record.DataOffset {
		t.Fatalf("record DataOffset = %#x", got)
	}
	if got := binary.LittleEndian.Uint32(encodedRecord[40:44]); got != record.Size {
		t.Fatalf("record Size = %#x", got)
	}
	if got := binary.LittleEndian.Uint32(encodedRecord[44:48]); got != record.CRC32 {
		t.Fatalf("record CRC32 = %#x", got)
	}
	decodedRecord, err := decodeIndexRecord(encodedRecord)
	if err != nil || decodedRecord != record {
		t.Fatalf("decodeIndexRecord = %#v, %v", decodedRecord, err)
	}

	manifestRecord := indexRecord{Key: store.ContentKey{1}, DataOffset: 512, Size: 5, CRC32: 7}
	chunkRecord := indexRecord{Key: store.ContentKey{2}, DataOffset: 1024, Size: 1, CRC32: 8}
	payload, footer, err := buildIndexPayload(256, 2048, []indexRecord{manifestRecord}, []indexRecord{chunkRecord})
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) != 2*indexRecordSize+indexFooterSize {
		t.Fatalf("index payload size = %d", len(payload))
	}
	footerBytes := payload[len(payload)-indexFooterSize:]
	if !allZero(footerBytes[168:252]) || footerBytes[22] != 0 || footerBytes[23] != 0 {
		t.Fatal("canonical footer reserved bytes are non-zero")
	}
	if got, want := binary.LittleEndian.Uint32(footerBytes[252:]), crc32.Checksum(footerBytes[:252], indexCRCTable); got != want {
		t.Fatalf("footer checksum = %#x, want %#x", got, want)
	}
	directoryOffset := footer.IndexPayloadOffset + footer.IndexPayloadSize
	decodedFooter, err := decodeIndexFooter(footerBytes, directoryOffset)
	if err != nil || decodedFooter != footer {
		t.Fatalf("decodeIndexFooter = %#v, %v", decodedFooter, err)
	}
}

func mutateFooter(t *testing.T, data []byte, updateChecksum bool, mutate func([]byte)) []byte {
	t.Helper()
	mutated := append([]byte(nil), data...)
	layout := inspectBundleIORanges(t, mutated)
	directoryOffset := int(layout.footer.IndexPayloadOffset + layout.footer.IndexPayloadSize)
	footer := mutated[directoryOffset-indexFooterSize : directoryOffset]
	mutate(footer)
	if updateChecksum {
		binary.LittleEndian.PutUint32(footer[252:256], crc32.Checksum(footer[:252], indexCRCTable))
	}
	return mutated
}

func mutateManifestSection(t *testing.T, data []byte, updateDigest bool, mutate func([]byte)) []byte {
	t.Helper()
	mutated := append([]byte(nil), data...)
	layout := inspectBundleIORanges(t, mutated)
	start := int(layout.footer.Manifest.Offset)
	end := int(layout.footer.Manifest.Offset + layout.footer.Manifest.Size)
	section := mutated[start:end]
	mutate(section)
	if updateDigest {
		directoryOffset := int(layout.footer.IndexPayloadOffset + layout.footer.IndexPayloadSize)
		footer := mutated[directoryOffset-indexFooterSize : directoryOffset]
		digest := sha256.Sum256(section)
		copy(footer[80:112], digest[:])
		binary.LittleEndian.PutUint32(footer[252:256], crc32.Checksum(footer[:252], indexCRCTable))
	}
	return mutated
}

func mutateChunkSection(t *testing.T, data []byte, mutate func([]byte, indexFooter)) []byte {
	t.Helper()
	mutated := append([]byte(nil), data...)
	layout := inspectBundleIORanges(t, mutated)
	start := int(layout.footer.Chunk.Offset)
	end := int(layout.footer.Chunk.Offset + layout.footer.Chunk.Size)
	section := mutated[start:end]
	mutate(section, layout.footer)
	directoryOffset := int(layout.footer.IndexPayloadOffset + layout.footer.IndexPayloadSize)
	footer := mutated[directoryOffset-indexFooterSize : directoryOffset]
	digest := sha256.Sum256(section)
	copy(footer[136:168], digest[:])
	binary.LittleEndian.PutUint32(footer[252:256], crc32.Checksum(footer[:252], indexCRCTable))
	return mutated
}

func TestReaderRejectsMissingAndMalformedMandatoryIndex(t *testing.T) {
	admission := canonicalAdmissionEntry(t)
	manifest := canonicalManifestEntry(t)
	withoutIndex := rawZIPWithoutIndex(t, []rawEntry{admission, manifest}, "")
	if _, err := NewReader(bytes.NewReader(withoutIndex), int64(len(withoutIndex))); err == nil {
		t.Fatal("Bundle without mandatory index was accepted")
	}

	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	for _, tc := range []struct {
		name           string
		updateChecksum bool
		mutate         func([]byte)
	}{
		{name: "bad magic", mutate: func(footer []byte) { footer[0] ^= 0xff }},
		{name: "bad version", mutate: func(footer []byte) { binary.LittleEndian.PutUint16(footer[16:18], indexVersion+1) }},
		{name: "bad footer size", mutate: func(footer []byte) { binary.LittleEndian.PutUint16(footer[18:20], indexFooterSize-1) }},
		{name: "bad record size", mutate: func(footer []byte) { binary.LittleEndian.PutUint16(footer[20:22], indexRecordSize-1) }},
		{name: "bad checksum", mutate: func(footer []byte) { footer[252] ^= 1 }},
		{name: "reserved", updateChecksum: true, mutate: func(footer []byte) { footer[168] = 1 }},
		{name: "bad metadata prefix", updateChecksum: true, mutate: func(footer []byte) { binary.LittleEndian.PutUint64(footer[48:56], maxMetadataPrefixBytes+1) }},
		{name: "count multiplication overflow", updateChecksum: true, mutate: func(footer []byte) { binary.LittleEndian.PutUint64(footer[120:128], math.MaxUint64) }},
		{name: "section overlap", updateChecksum: true, mutate: func(footer []byte) {
			binary.LittleEndian.PutUint64(footer[56:64], binary.LittleEndian.Uint64(footer[112:120]))
		}},
		{name: "section gap", updateChecksum: true, mutate: func(footer []byte) {
			binary.LittleEndian.PutUint64(footer[56:64], binary.LittleEndian.Uint64(footer[56:64])+1)
		}},
		{name: "section covers footer", updateChecksum: true, mutate: func(footer []byte) {
			binary.LittleEndian.PutUint64(footer[64:72], binary.LittleEndian.Uint64(footer[64:72])+1)
			binary.LittleEndian.PutUint64(footer[72:80], binary.LittleEndian.Uint64(footer[72:80])+indexRecordSize)
		}},
		{name: "payload not adjacent to CD", updateChecksum: true, mutate: func(footer []byte) {
			binary.LittleEndian.PutUint64(footer[32:40], binary.LittleEndian.Uint64(footer[32:40])-1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := mutateFooter(t, fixture.data, tc.updateChecksum, tc.mutate)
			if _, err := NewReader(bytes.NewReader(data), int64(len(data))); err == nil {
				t.Fatal("malformed index footer was accepted")
			}
		})
	}
}

func TestChunkIndexRejectsOrderDuplicatesSizesAndRangesAtPreparation(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	for _, tc := range []struct {
		name   string
		mutate func([]byte, indexFooter)
	}{
		{name: "duplicate key", mutate: func(section []byte, _ indexFooter) {
			copy(section[indexRecordSize:indexRecordSize+32], section[:32])
		}},
		{name: "unsorted", mutate: func(section []byte, _ indexFooter) {
			first := append([]byte(nil), section[:indexRecordSize]...)
			copy(section[:indexRecordSize], section[indexRecordSize:2*indexRecordSize])
			copy(section[indexRecordSize:2*indexRecordSize], first)
		}},
		{name: "before metadata", mutate: func(section []byte, _ indexFooter) {
			binary.LittleEndian.PutUint64(section[32:40], 0)
		}},
		{name: "enters index", mutate: func(section []byte, footer indexFooter) {
			binary.LittleEndian.PutUint64(section[32:40], footer.IndexHeaderOffset)
		}},
		{name: "overlapping object range", mutate: func(section []byte, _ indexFooter) {
			copy(section[indexRecordSize+32:indexRecordSize+40], section[32:40])
		}},
		{name: "zero size", mutate: func(section []byte, _ indexFooter) {
			binary.LittleEndian.PutUint32(section[40:44], 0)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := mutateChunkSection(t, fixture.data, tc.mutate)
			reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatalf("Open unexpectedly loaded the malformed Chunk index: %v", err)
			}
			defer reader.Close()
			if err := reader.prepareChunks(context.Background()); err == nil {
				t.Fatal("malformed Chunk index was accepted during preparation")
			}
		})
	}
}

func TestReaderRejectsManifestIndexDigestOrderDuplicateAndRanges(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	for _, tc := range []struct {
		name         string
		updateDigest bool
		mutate       func([]byte, indexFooter)
	}{
		{name: "section digest", mutate: func(section []byte, _ indexFooter) { section[0] ^= 1 }},
		{name: "duplicate key", updateDigest: true, mutate: func(section []byte, _ indexFooter) {
			copy(section[indexRecordSize:indexRecordSize+32], section[:32])
		}},
		{name: "unsorted", updateDigest: true, mutate: func(section []byte, _ indexFooter) {
			first := append([]byte(nil), section[:indexRecordSize]...)
			copy(section[:indexRecordSize], section[indexRecordSize:2*indexRecordSize])
			copy(section[indexRecordSize:2*indexRecordSize], first)
		}},
		{name: "offset overflow", updateDigest: true, mutate: func(section []byte, _ indexFooter) {
			binary.LittleEndian.PutUint64(section[32:40], math.MaxUint64-1)
		}},
		{name: "range enters index", updateDigest: true, mutate: func(section []byte, footer indexFooter) {
			binary.LittleEndian.PutUint64(section[32:40], footer.IndexHeaderOffset)
		}},
		{name: "zero Manifest size", updateDigest: true, mutate: func(section []byte, _ indexFooter) {
			binary.LittleEndian.PutUint32(section[40:44], 0)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			layout := inspectBundleIORanges(t, fixture.data)
			data := mutateManifestSection(t, fixture.data, tc.updateDigest, func(section []byte) {
				tc.mutate(section, layout.footer)
			})
			if _, err := NewReader(bytes.NewReader(data), int64(len(data))); err == nil {
				t.Fatal("malformed Manifest index was accepted")
			}
		})
	}
}

func TestReaderRejectsNonCanonicalIndexLocalHeader(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	layout := inspectBundleIORanges(t, fixture.data)
	for _, tc := range []struct {
		name   string
		offset int
		value  byte
	}{
		{name: "data descriptor flag", offset: 6, value: 8},
		{name: "non-Store", offset: 8, value: byte(zip.Deflate)},
		{name: "local extra", offset: 28, value: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := append([]byte(nil), fixture.data...)
			data[int(layout.footer.IndexHeaderOffset)+tc.offset] = tc.value
			if _, err := NewReader(bytes.NewReader(data), int64(len(data))); err == nil {
				t.Fatal("non-canonical index Local Header was accepted")
			}
		})
	}
}

func TestReaderRejectsDuplicateAndNonFinalIndexWithoutCDFallback(t *testing.T) {
	admission := canonicalAdmissionEntry(t)
	manifest := canonicalManifestEntry(t)
	duplicate := rawZIP(t, []rawEntry{admission, manifest, {name: indexName, data: []byte("earlier index")}}, "")
	if _, err := NewReader(bytes.NewReader(duplicate), int64(len(duplicate))); err == nil {
		t.Fatal("duplicate index was accepted")
	}

	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	layout := inspectBundleIORanges(t, fixture.data)
	directoryOffset := int(layout.footer.IndexPayloadOffset + layout.footer.IndexPayloadSize)
	inserted := []byte{'P', 'K', 0x03, 0x04, 0}
	data := append([]byte(nil), fixture.data[:directoryOffset]...)
	data = append(data, inserted...)
	data = append(data, fixture.data[directoryOffset:]...)
	eocd := len(data) - zipDirectoryEndSize
	binary.LittleEndian.PutUint32(data[eocd+16:eocd+20], uint32(directoryOffset+len(inserted)))
	if _, err := NewReader(bytes.NewReader(data), int64(len(data))); err == nil {
		t.Fatal("index not adjacent to the Central Directory was accepted")
	}
}

func insertGapBeforeIndexAndRebaseFooter(t *testing.T, data, gap []byte) []byte {
	t.Helper()
	layout := inspectBundleIORanges(t, data)
	if layout.zip64 {
		t.Fatal("gap mutation helper expects ZIP32")
	}
	oldHeader := int(layout.footer.IndexHeaderOffset)
	oldDirectory := int(layout.footer.IndexPayloadOffset + layout.footer.IndexPayloadSize)
	mutated := append([]byte(nil), data[:oldHeader]...)
	mutated = append(mutated, gap...)
	mutated = append(mutated, data[oldHeader:]...)
	delta := uint64(len(gap))
	newHeader := oldHeader + len(gap)
	newDirectory := oldDirectory + len(gap)
	eocd := len(mutated) - zipDirectoryEndSize
	binary.LittleEndian.PutUint32(mutated[eocd+16:eocd+20], uint32(newDirectory))
	footer := mutated[newDirectory-indexFooterSize : newDirectory]
	for _, field := range [][2]int{{24, 32}, {40, 48}, {56, 64}, {112, 120}} {
		binary.LittleEndian.PutUint64(footer[field[0]:field[1]], binary.LittleEndian.Uint64(footer[field[0]:field[1]])+delta)
	}
	binary.LittleEndian.PutUint32(footer[252:256], crc32.Checksum(footer[:252], indexCRCTable))
	indexCRC := crc32.ChecksumIEEE(mutated[newHeader+indexLocalHeaderSize : newDirectory])
	binary.LittleEndian.PutUint32(mutated[newHeader+14:newHeader+18], indexCRC)

	position := newDirectory
	count := int(binary.LittleEndian.Uint16(mutated[eocd+10 : eocd+12]))
	updated := false
	for range count {
		if position+46 > eocd {
			t.Fatal("truncated mutated Central Directory")
		}
		nameSize := int(binary.LittleEndian.Uint16(mutated[position+28 : position+30]))
		extraSize := int(binary.LittleEndian.Uint16(mutated[position+30 : position+32]))
		commentSize := int(binary.LittleEndian.Uint16(mutated[position+32 : position+34]))
		name := string(mutated[position+46 : position+46+nameSize])
		if name == indexName {
			binary.LittleEndian.PutUint32(mutated[position+16:position+20], indexCRC)
			binary.LittleEndian.PutUint32(mutated[position+42:position+46], uint32(newHeader))
			updated = true
		}
		position += 46 + nameSize + extraSize + commentSize
	}
	if !updated {
		t.Fatal("index Central Directory record not found")
	}
	return mutated
}

func TestStrictVerifierRejectsGapThatHotIndexDoesNotRead(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	data := insertGapBeforeIndexAndRebaseFooter(t, fixture.data, []byte("hidden gap"))
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("hot Reader unexpectedly inspected the object/index physical gap: %v", err)
	}
	defer reader.Close()
	if err := reader.verifyContainer(context.Background()); err == nil {
		t.Fatal("strict verifier accepted a hidden physical gap")
	}
}

func TestWriterIndexIsLastAndRecordsDoNotReorderPayloads(t *testing.T) {
	salt, err := store.SaltForGeneration("INDEX")
	if err != nil {
		t.Fatal(err)
	}
	admission := store.WriteAdmission{Generation: "INDEX", Salt: salt}
	var output bytes.Buffer
	w, err := NewWriter(&output, admission, WriterOptions{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	physicalKeys := []store.ContentKey{{3}, {1}, {2}}
	payloads := map[store.ContentKey][]byte{}
	for ordinal, key := range physicalKeys {
		payload := []byte{byte(ordinal + 1)}
		payloads[key] = payload
		if _, err := w.PutChunkOrdered(context.Background(), admission, key, payload, uint64(ordinal)); err != nil {
			t.Fatal(err)
		}
	}
	manifestData, err := codec.Marshal(&codec.Manifest{Version: codec.Version1, ChunkMode: codec.ChunkModeFixed}, nil)
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := store.ContentKey(sha256.Sum256(manifestData))
	if _, err := w.Put(context.Background(), admission, store.PartitionManifest, manifestKey, manifestData); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(manifestKey); err != nil {
		t.Fatal(err)
	}
	data := output.Bytes()
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if zr.File[len(zr.File)-1].Name != indexName {
		t.Fatalf("last ZIP entry = %q, want %q", zr.File[len(zr.File)-1].Name, indexName)
	}
	for index, key := range physicalKeys {
		wantName, _ := objectName(store.PartitionChunk, key)
		if zr.File[index+1].Name != wantName {
			t.Fatalf("physical Chunk[%d] = %q, want %q", index, zr.File[index+1].Name, wantName)
		}
		opened, err := zr.File[index+1].Open()
		if err != nil {
			t.Fatal(err)
		}
		payload, err := io.ReadAll(opened)
		_ = opened.Close()
		if err != nil || !bytes.Equal(payload, payloads[key]) {
			t.Fatalf("physical payload[%d] = %x, %v", index, payload, err)
		}
	}
	layout := inspectBundleIORanges(t, data)
	if got := layout.footer.IndexPayloadOffset + layout.footer.IndexPayloadSize - indexFooterSize; got+indexFooterSize != uint64(layout.centralDir.start) {
		t.Fatalf("footer end %d is not adjacent to Central Directory %d", got+indexFooterSize, layout.centralDir.start)
	}
	chunkBytes := data[layout.footer.Chunk.Offset : layout.footer.Chunk.Offset+layout.footer.Chunk.Size]
	decoded, err := decodeIndexRecords(chunkBytes, layout.footer.Chunk, store.PartitionChunk, layout.footer)
	if err != nil {
		t.Fatal(err)
	}
	keys := sortedKeys(decoded)
	if !reflect.DeepEqual(keys, []store.ContentKey{{1}, {2}, {3}}) {
		t.Fatalf("sorted Chunk index keys = %v", keys)
	}
	for _, file := range zr.File {
		if file.Flags != 0 || file.Method != zip.Store {
			t.Fatalf("entry %q flags/method = %#x/%d", file.Name, file.Flags, file.Method)
		}
		dataOffset, err := file.DataOffset()
		if err != nil {
			t.Fatal(err)
		}
		headerOffset := dataOffset - int64(zipLocalHeaderFixedSize+len(file.Name))
		if binary.LittleEndian.Uint16(data[headerOffset+28:headerOffset+30]) != 0 {
			t.Fatalf("entry %q Local Header extra is non-empty", file.Name)
		}
	}
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := reader.prepareChunks(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, file := range zr.File {
		var entries map[store.ContentKey]archiveEntry
		var prefix string
		switch {
		case bytes.HasPrefix([]byte(file.Name), []byte(manifestPrefix)):
			entries, prefix = reader.manifests, manifestPrefix
		case bytes.HasPrefix([]byte(file.Name), []byte(chunkPrefix)):
			entries, prefix = reader.chunks, chunkPrefix
		default:
			continue
		}
		key, err := parseObjectEntry(file.Name, prefix)
		if err != nil {
			t.Fatal(err)
		}
		dataOffset, err := file.DataOffset()
		if err != nil {
			t.Fatal(err)
		}
		record := entries[key]
		if record.dataOffset != uint64(dataOffset) || uint64(record.size) != file.UncompressedSize64 || record.crc32 != file.CRC32 {
			t.Fatalf("entry %q index = %#v, CD DataOffset/Size/CRC = %d/%d/%08x", file.Name, record, dataOffset, file.UncompressedSize64, file.CRC32)
		}
	}
	if err := reader.verifyContainer(context.Background()); err != nil {
		t.Fatalf("strict verification of Writer offsets: %v", err)
	}
}

func FuzzIndexFooter(f *testing.F) {
	manifest := indexRecord{Key: store.ContentKey{1}, DataOffset: 512, Size: 5}
	payload, footer, err := buildIndexPayload(256, 2048, []indexRecord{manifest}, nil)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(append([]byte(nil), payload[len(payload)-indexFooterSize:]...), footer.IndexPayloadOffset+footer.IndexPayloadSize)
	f.Fuzz(func(t *testing.T, encoded []byte, directoryOffset uint64) {
		if len(encoded) > 4*indexFooterSize {
			return
		}
		_, _ = decodeIndexFooter(encoded, directoryOffset)
	})
}

func FuzzIndexRecords(f *testing.F) {
	f.Add(make([]byte, indexRecordSize), byte(0))
	f.Add(bytes.Repeat([]byte{1}, 2*indexRecordSize), byte(1))
	f.Fuzz(func(t *testing.T, encoded []byte, kind byte) {
		if len(encoded) > 1<<20 || len(encoded)%indexRecordSize != 0 {
			return
		}
		partition := store.PartitionManifest
		if kind&1 != 0 {
			partition = store.PartitionChunk
		}
		section := indexSection{Count: uint64(len(encoded) / indexRecordSize), Size: uint64(len(encoded)), Digest: sha256.Sum256(encoded)}
		footer := indexFooter{MetadataPrefixEnd: 1, IndexHeaderOffset: math.MaxUint32, Manifest: section}
		if partition == store.PartitionChunk {
			footer.Manifest = indexSection{}
			footer.Chunk = section
		}
		_, _ = decodeIndexRecords(encoded, section, partition, footer)
	})
}

func FuzzStrictContainer(f *testing.F) {
	fixture := newTestFixture(f, "FUZZ-INDEX")
	f.Add(append([]byte(nil), fixture.data...))
	_ = fixture.reader.Close()
	f.Add([]byte("PK\x03\x04"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 16<<20 {
			return
		}
		reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return
		}
		_ = reader.verifyContainer(context.Background())
		_ = reader.Close()
	})
}

func FuzzFullVerifyIndexCrossCheck(f *testing.F) {
	fixture := newTestFixture(f, "FUZZ-FULL")
	f.Add(append([]byte(nil), fixture.data...))
	_ = fixture.reader.Close()
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 16<<20 {
			return
		}
		reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return
		}
		_ = reader.FullVerify(context.Background(), fixture.root, fixture.customer, fixture.decryptor, VerifyOptions{Workers: 1})
		_ = reader.Close()
	})
}

func TestIndexDigestIsDamageDetectionNotSignature(t *testing.T) {
	// This test intentionally states the trust boundary in executable form:
	// changing a record and recomputing its unkeyed digest/footer checksum can
	// pass hot index parsing, while strict CD/LFH cross-checking still rejects it.
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	data := mutateManifestSection(t, fixture.data, true, func(section []byte) {
		binary.LittleEndian.PutUint32(section[44:48], binary.LittleEndian.Uint32(section[44:48])^1)
	})
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("recomputed unkeyed index digest should parse on hot path: %v", err)
	}
	defer reader.Close()
	if err := reader.verifyContainer(context.Background()); err == nil {
		t.Fatalf("strict verifier error = %v", err)
	}
}
