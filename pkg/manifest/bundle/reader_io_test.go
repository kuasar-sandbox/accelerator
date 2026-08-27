package bundle

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type recordedRangeRead struct {
	offset int64
	size   int
}

type rangeRecordingReaderAt struct {
	inner io.ReaderAt
	delay time.Duration

	mu    sync.Mutex
	reads []recordedRangeRead
}

type blockingRangeReaderAt struct {
	inner  io.ReaderAt
	target byteRange

	started chan struct{}
	release chan struct{}
	once    sync.Once
	reads   atomic.Int64
}

type sparseReadSegment struct {
	offset int64
	data   []byte
}

type sparseSegmentReaderAt struct {
	size     int64
	segments []sparseReadSegment
}

func (r *sparseSegmentReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	if offset < 0 || offset >= r.size {
		return 0, io.EOF
	}
	want := len(dst)
	if int64(want) > r.size-offset {
		want = int(r.size - offset)
	}
	clear(dst[:want])
	readEnd := offset + int64(want)
	for _, segment := range r.segments {
		segmentEnd := segment.offset + int64(len(segment.data))
		start := offset
		if segment.offset > start {
			start = segment.offset
		}
		end := readEnd
		if segmentEnd < end {
			end = segmentEnd
		}
		if start < end {
			copy(dst[start-offset:end-offset], segment.data[start-segment.offset:end-segment.offset])
		}
	}
	if want != len(dst) {
		return want, io.EOF
	}
	return want, nil
}

func canonicalLocalHeader(name string, payload []byte) []byte {
	return canonicalLocalHeaderFields(name, uint32(len(payload)), crc32.ChecksumIEEE(payload))
}

func canonicalLocalHeaderFields(name string, size, checksum uint32) []byte {
	header := make([]byte, zipLocalHeaderFixedSize+len(name))
	copy(header[:4], zipLocalHeaderMagic[:])
	binary.LittleEndian.PutUint16(header[4:6], 45)
	binary.LittleEndian.PutUint16(header[8:10], zip.Store)
	binary.LittleEndian.PutUint32(header[14:18], checksum)
	binary.LittleEndian.PutUint32(header[18:22], size)
	binary.LittleEndian.PutUint32(header[22:26], size)
	binary.LittleEndian.PutUint16(header[26:28], uint16(len(name)))
	copy(header[zipLocalHeaderFixedSize:], name)
	return header
}

type sparseZIPEntry struct {
	name         string
	headerOffset uint64
	size         uint32
	checksum     uint32
}

func canonicalCentralHeader(entry sparseZIPEntry) []byte {
	var extra []byte
	localOffset := uint32(entry.headerOffset)
	if entry.headerOffset >= math.MaxUint32 {
		localOffset = math.MaxUint32
		extra = make([]byte, 12)
		binary.LittleEndian.PutUint16(extra[0:2], 0x0001)
		binary.LittleEndian.PutUint16(extra[2:4], 8)
		binary.LittleEndian.PutUint64(extra[4:12], entry.headerOffset)
	}
	header := make([]byte, 46+len(entry.name)+len(extra))
	copy(header[:4], zipDirectoryMagic[:])
	binary.LittleEndian.PutUint16(header[4:6], 45)
	binary.LittleEndian.PutUint16(header[6:8], 45)
	binary.LittleEndian.PutUint16(header[10:12], zip.Store)
	binary.LittleEndian.PutUint32(header[16:20], entry.checksum)
	binary.LittleEndian.PutUint32(header[20:24], entry.size)
	binary.LittleEndian.PutUint32(header[24:28], entry.size)
	binary.LittleEndian.PutUint16(header[28:30], uint16(len(entry.name)))
	binary.LittleEndian.PutUint16(header[30:32], uint16(len(extra)))
	binary.LittleEndian.PutUint32(header[42:46], localOffset)
	copy(header[46:46+len(entry.name)], entry.name)
	copy(header[46+len(entry.name):], extra)
	return header
}

func (r *blockingRangeReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	if offset == r.target.start && offset+int64(len(dst)) == r.target.end {
		r.reads.Add(1)
		r.once.Do(func() { close(r.started) })
		<-r.release
	}
	return r.inner.ReadAt(dst, offset)
}

func (r *rangeRecordingReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	if len(dst) != 0 {
		r.mu.Lock()
		r.reads = append(r.reads, recordedRangeRead{offset: offset, size: len(dst)})
		r.mu.Unlock()
		if r.delay != 0 {
			time.Sleep(r.delay)
		}
	}
	return r.inner.ReadAt(dst, offset)
}

func (r *rangeRecordingReaderAt) snapshot() []recordedRangeRead {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedRangeRead(nil), r.reads...)
}

func (r *rangeRecordingReaderAt) reset() {
	r.mu.Lock()
	r.reads = nil
	r.mu.Unlock()
}

type bundleIORanges struct {
	footer        indexFooter
	tail          []byteRange
	indexFooter   byteRange
	indexHeader   byteRange
	metadata      byteRange
	manifestIndex byteRange
	chunkIndex    byteRange
	centralDir    byteRange
	objectHeaders []byteRange
	objectPayload []byteRange
	zip64         bool
}

func inspectBundleIORanges(t testing.TB, data []byte) bundleIORanges {
	t.Helper()
	if len(data) < zipDirectoryEndSize {
		t.Fatal("short test ZIP")
	}
	var end [zipDirectoryEndSize]byte
	copy(end[:], data[len(data)-zipDirectoryEndSize:])
	directoryOffset, _, err := readCentralDirectoryLocation(bytes.NewReader(data), int64(len(data)), end)
	if err != nil {
		t.Fatal(err)
	}
	footerOffset := directoryOffset - indexFooterSize
	footer, err := decodeIndexFooter(data[footerOffset:directoryOffset], uint64(directoryOffset))
	if err != nil {
		t.Fatal(err)
	}
	eocdStart := int64(len(data) - zipDirectoryEndSize)
	directoryEnd := eocdStart
	tail := []byteRange{{start: eocdStart, end: int64(len(data))}}
	zip64 := binary.LittleEndian.Uint32(end[16:20]) == ^uint32(0)
	if zip64 {
		locatorOffset := int64(len(data) - zipDirectoryEndSize - 20)
		zip64Offset := int64(binary.LittleEndian.Uint64(data[locatorOffset+8 : locatorOffset+16]))
		directoryEnd = zip64Offset
		tail = append(tail,
			byteRange{start: locatorOffset, end: eocdStart},
			byteRange{start: zip64Offset, end: locatorOffset},
		)
	}
	layout := bundleIORanges{
		footer:        footer,
		tail:          tail,
		indexFooter:   byteRange{start: footerOffset, end: directoryOffset},
		indexHeader:   byteRange{start: int64(footer.IndexHeaderOffset), end: int64(footer.IndexPayloadOffset)},
		metadata:      byteRange{start: 0, end: int64(footer.MetadataPrefixEnd)},
		manifestIndex: byteRange{start: int64(footer.Manifest.Offset), end: int64(footer.Manifest.Offset + footer.Manifest.Size)},
		chunkIndex:    byteRange{start: int64(footer.Chunk.Offset), end: int64(footer.Chunk.Offset + footer.Chunk.Size)},
		centralDir:    byteRange{start: directoryOffset, end: directoryEnd},
		zip64:         zip64,
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range zr.File {
		dataOffset, err := file.DataOffset()
		if err != nil {
			t.Fatal(err)
		}
		headerStart := dataOffset - int64(zipLocalHeaderFixedSize+len(file.Name))
		if stringsHasObjectPrefix(file.Name) {
			layout.objectHeaders = append(layout.objectHeaders, byteRange{start: headerStart, end: dataOffset})
			layout.objectPayload = append(layout.objectPayload, byteRange{start: dataOffset, end: dataOffset + int64(file.UncompressedSize64)})
		}
	}
	return layout
}

func stringsHasObjectPrefix(name string) bool {
	return len(name) >= len(manifestPrefix) && name[:len(manifestPrefix)] == manifestPrefix ||
		len(name) >= len(chunkPrefix) && name[:len(chunkPrefix)] == chunkPrefix
}

func readOverlaps(read recordedRangeRead, candidate byteRange) bool {
	return read.offset < candidate.end && read.offset+int64(read.size) > candidate.start
}

func assertReadsAvoid(t testing.TB, reads []recordedRangeRead, label string, ranges ...byteRange) {
	t.Helper()
	for _, read := range reads {
		for _, candidate := range ranges {
			if candidate.end > candidate.start && readOverlaps(read, candidate) {
				t.Fatalf("read [%d,%d) overlaps forbidden %s range [%d,%d)", read.offset, read.offset+int64(read.size), label, candidate.start, candidate.end)
			}
		}
	}
}

func exactRangeReadCount(reads []recordedRangeRead, candidate byteRange) int {
	count := 0
	for _, read := range reads {
		if read.offset == candidate.start && read.offset+int64(read.size) == candidate.end {
			count++
		}
	}
	return count
}

func readBytes(reads []recordedRangeRead) int64 {
	var total int64
	for _, read := range reads {
		total += int64(read.size)
	}
	return total
}

func TestBundleOpenUsesConstantTailIndexRanges(t *testing.T) {
	for _, tc := range []struct {
		name      string
		chunks    int
		wantReads int
		wantZip64 bool
	}{
		{name: "4K", chunks: 4_000, wantReads: 5},
		{name: "20K", chunks: 20_000, wantReads: 5},
		{name: "100K-profile-limit", chunks: maxBundleEntries - 3, wantReads: 7, wantZip64: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := syntheticBundle(t, tc.chunks, 1)
			layout := inspectBundleIORanges(t, data)
			if layout.zip64 != tc.wantZip64 {
				t.Fatalf("ZIP64 = %v, want %v", layout.zip64, tc.wantZip64)
			}
			source := &rangeRecordingReaderAt{inner: bytes.NewReader(data)}
			reader, err := NewReader(source, int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			reads := source.snapshot()
			if len(reads) != tc.wantReads {
				t.Fatalf("Open ReadAt calls = %d (%v), want %d fixed tail/profile reads", len(reads), reads, tc.wantReads)
			}
			for _, tailRange := range layout.tail {
				if exactRangeReadCount(reads, tailRange) != 1 {
					t.Fatalf("ZIP tail range [%d,%d) was not read exactly once: %v", tailRange.start, tailRange.end, reads)
				}
			}
			for label, expected := range map[string]byteRange{
				"index footer":       layout.indexFooter,
				"index Local Header": layout.indexHeader,
				"metadata prefix":    layout.metadata,
				"Manifest index":     layout.manifestIndex,
			} {
				if exactRangeReadCount(reads, expected) != 1 {
					t.Fatalf("%s range [%d,%d) was not read exactly once: %v", label, expected.start, expected.end, reads)
				}
			}
			assertReadsAvoid(t, reads, "Central Directory", layout.centralDir)
			assertReadsAvoid(t, reads, "Chunk index", layout.chunkIndex)
			assertReadsAvoid(t, reads, "object Local Header", layout.objectHeaders...)
			assertReadsAvoid(t, reads, "object payload", layout.objectPayload...)
			before := len(reads)
			_ = reader.HasManifest(store.ContentKey{0xff})
			_ = reader.ManifestKeys()
			if after := len(source.snapshot()); after != before {
				t.Fatalf("Manifest lookup added %d source reads", after-before)
			}
		})
	}
}

func TestSelectedBundlePreparesChunkIndexOnceAndGetReadsOnlyPayload(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	layout := inspectBundleIORanges(t, fixture.data)
	source := &rangeRecordingReaderAt{inner: bytes.NewReader(fixture.data)}
	reader, err := NewReader(source, int64(len(fixture.data)))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	source.reset()

	local := fetch.NewFetcher(fixture.customer, reader.Getter(), fixture.decryptor)
	stream, err := NewManifestFetcher(reader, local, nil).OpenRootManifest(context.Background(), fixture.root)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	reads := source.snapshot()
	if exactRangeReadCount(reads, layout.chunkIndex) != 1 {
		t.Fatalf("selected source Chunk index reads = %v, want one contiguous range", reads)
	}
	assertReadsAvoid(t, reads, "index footer", layout.indexFooter)
	assertReadsAvoid(t, reads, "index Local Header", layout.indexHeader)
	assertReadsAvoid(t, reads, "Manifest index", layout.manifestIndex)
	assertReadsAvoid(t, reads, "Central Directory", layout.centralDir)
	assertReadsAvoid(t, reads, "object Local Header", layout.objectHeaders...)

	source.reset()
	chunkKey := reader.ChunkKeys()[0]
	entry := reader.chunks[chunkKey]
	result, blob, err := reader.Getter().Get(context.Background(), store.PartitionChunk, chunkKey)
	if err != nil || blob == nil {
		t.Fatalf("Get Chunk = %v, %v, %v", result, blob, err)
	}
	blob.Release()
	reads = source.snapshot()
	wantPayload := byteRange{start: int64(entry.dataOffset), end: int64(entry.dataOffset) + int64(entry.size)}
	if len(reads) != 1 || exactRangeReadCount(reads, wantPayload) != 1 {
		t.Fatalf("prepared Get reads = %v, want only payload [%d,%d)", reads, wantPayload.start, wantPayload.end)
	}
	assertReadsAvoid(t, reads, "Central Directory", layout.centralDir)
	assertReadsAvoid(t, reads, "Chunk index", layout.chunkIndex)
	assertReadsAvoid(t, reads, "object Local Header", layout.objectHeaders...)
}

func TestConcurrentChunkPreparationReadsOneRange(t *testing.T) {
	data := syntheticBundle(t, 20_000, 1)
	layout := inspectBundleIORanges(t, data)
	source := &rangeRecordingReaderAt{inner: bytes.NewReader(data)}
	reader, err := NewReader(source, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	source.reset()
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := reader.prepareChunks(context.Background()); err != nil {
				t.Errorf("prepareChunks: %v", err)
			}
		}()
	}
	wg.Wait()
	reads := source.snapshot()
	if len(reads) != 1 || exactRangeReadCount(reads, layout.chunkIndex) != 1 {
		t.Fatalf("concurrent prepare reads = %v, want one contiguous Chunk index range", reads)
	}
}

func TestRefsCleanMissDoesNotReadChunkIndex(t *testing.T) {
	ref := "file://parent.bundle"
	currentData := syntheticBundleWithRefs(t, 4_000, 1, []string{ref})
	missData := syntheticBundle(t, 20_000, 1)
	currentSource := &rangeRecordingReaderAt{inner: bytes.NewReader(currentData)}
	missSource := &rangeRecordingReaderAt{inner: bytes.NewReader(missData)}
	current, err := NewReader(currentSource, int64(len(currentData)))
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	miss, err := NewReader(missSource, int64(len(missData)))
	if err != nil {
		t.Fatal(err)
	}
	defer miss.Close()
	currentSource.reset()
	missSource.reset()
	fetcher := NewManifestFetcherWithResolver(current, nil, SourceResolverFunc(func(context.Context, string) (ManifestSource, error) {
		return ManifestSource{Reader: miss}, nil
	}), nil)
	if _, err := fetcher.SelectManifest(context.Background(), store.ContentKey{0xff}); err == nil {
		t.Fatal("clean miss unexpectedly selected a source")
	}
	if reads := currentSource.snapshot(); len(reads) != 0 {
		t.Fatalf("current clean miss performed source reads: %v", reads)
	}
	if reads := missSource.snapshot(); len(reads) != 0 {
		t.Fatalf("refs clean miss performed source reads: %v", reads)
	}
}

func TestChunkPreparationCancellationIsShared(t *testing.T) {
	data := syntheticBundle(t, 20_000, 1)
	layout := inspectBundleIORanges(t, data)
	source := &blockingRangeReaderAt{
		inner:   bytes.NewReader(data),
		target:  layout.chunkIndex,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	reader, err := NewReader(source, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- reader.prepareChunks(ctx) }()
	<-source.started
	cancel()
	close(source.release)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("prepareChunks error = %v, want context.Canceled", err)
	}
	if err := reader.prepareChunks(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("shared prepare error = %v, want context.Canceled", err)
	}
	if got := source.reads.Load(); got != 1 {
		t.Fatalf("Chunk index reads = %d, want 1", got)
	}
}

func TestReaderCloseDuringChunkPreparationDefersCleanup(t *testing.T) {
	data := syntheticBundle(t, 4_000, 1)
	layout := inspectBundleIORanges(t, data)
	source := &blockingRangeReaderAt{
		inner:   bytes.NewReader(data),
		target:  layout.chunkIndex,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	var cleanups atomic.Int64
	reader, err := newReader(source, int64(len(data)), nil, func() error {
		cleanups.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- reader.prepareChunks(context.Background()) }()
	<-source.started
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if got := cleanups.Load(); got != 0 {
		t.Fatalf("cleanup ran %d times while prepare retained the source", got)
	}
	close(source.release)
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("prepareChunks error = %v, want ErrClosed", err)
	}
	if got := cleanups.Load(); got != 1 {
		t.Fatalf("cleanup calls = %d, want 1", got)
	}
	if err := reader.prepareChunks(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("shared Close preparation error = %v, want ErrClosed", err)
	}
}

func TestCorruptChunkIndexFailureIsSharedWithoutOpenFallback(t *testing.T) {
	data := syntheticBundle(t, 20_000, 1)
	layout := inspectBundleIORanges(t, data)
	data = append([]byte(nil), data...)
	data[layout.chunkIndex.start] ^= 0xff
	source := &rangeRecordingReaderAt{inner: bytes.NewReader(data)}
	reader, err := NewReader(source, int64(len(data)))
	if err != nil {
		t.Fatalf("Open should not load the Chunk index: %v", err)
	}
	defer reader.Close()
	source.reset()
	results := make(chan error, 32)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- reader.prepareChunks(context.Background())
		}()
	}
	wg.Wait()
	close(results)
	var first string
	for err := range results {
		if err == nil {
			t.Fatal("corrupt Chunk index preparation succeeded")
		}
		if first == "" {
			first = err.Error()
		} else if err.Error() != first {
			t.Fatalf("concurrent prepare errors differ: %q != %q", err, first)
		}
	}
	reads := source.snapshot()
	if len(reads) != 1 || exactRangeReadCount(reads, layout.chunkIndex) != 1 {
		t.Fatalf("corrupt Chunk index triggered fallback/additional reads: %v", reads)
	}
}

func TestReaderSupportsZIP64DirectoryOffsetBeyondFourGiB(t *testing.T) {
	salt, err := store.SaltForGeneration("LARGE")
	if err != nil {
		t.Fatal(err)
	}
	admission := store.WriteAdmission{Generation: "LARGE", Salt: salt}
	var segments []sparseReadSegment
	var directoryEntries []sparseZIPEntry
	admissionEntry := sparseZIPEntry{name: admissionName(admission), checksum: crc32.ChecksumIEEE(nil)}
	admissionHeader := canonicalLocalHeaderFields(admissionEntry.name, 0, admissionEntry.checksum)
	segments = append(segments, sparseReadSegment{offset: 0, data: admissionHeader})
	directoryEntries = append(directoryEntries, admissionEntry)
	nextOffset := uint64(len(admissionHeader))
	metadataEnd := nextOffset

	manifestData, err := codec.Marshal(&codec.Manifest{Version: codec.Version1, ChunkMode: codec.ChunkModeFixed}, nil)
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := store.ContentKey(sha256.Sum256(manifestData))
	manifestName, _ := objectName(store.PartitionManifest, manifestKey)
	manifestHeader := canonicalLocalHeader(manifestName, manifestData)
	manifestHeaderOffset := nextOffset
	manifestDataOffset := manifestHeaderOffset + uint64(len(manifestHeader))
	manifestChecksum := crc32.ChecksumIEEE(manifestData)
	segments = append(segments,
		sparseReadSegment{offset: int64(manifestHeaderOffset), data: manifestHeader},
		sparseReadSegment{offset: int64(manifestDataOffset), data: manifestData},
	)
	directoryEntries = append(directoryEntries, sparseZIPEntry{
		name: manifestName, headerOffset: manifestHeaderOffset, size: uint32(len(manifestData)), checksum: manifestChecksum,
	})
	nextOffset = manifestDataOffset + uint64(len(manifestData))
	manifestRecord := indexRecord{
		Key:        manifestKey,
		DataOffset: manifestDataOffset,
		Size:       uint32(len(manifestData)),
		CRC32:      manifestChecksum,
	}

	// Sparse zero-filled payload ranges make the archive physically contiguous
	// past 4 GiB without allocating or reading a multi-gigabyte test fixture.
	const chunkCount = 65
	const chunkSize = uint32(codec.MaxChunkDecodedSize)
	zeroBlock := make([]byte, 1<<20)
	var chunkChecksum uint32
	for remaining := uint64(chunkSize); remaining != 0; {
		block := uint64(len(zeroBlock))
		if remaining < block {
			block = remaining
		}
		chunkChecksum = crc32.Update(chunkChecksum, crc32.IEEETable, zeroBlock[:block])
		remaining -= block
	}
	chunkRecords := make([]indexRecord, 0, chunkCount)
	for index := 0; index < chunkCount; index++ {
		var key store.ContentKey
		binary.LittleEndian.PutUint64(key[:8], uint64(index+1))
		name, err := objectName(store.PartitionChunk, key)
		if err != nil {
			t.Fatal(err)
		}
		headerOffset := nextOffset
		header := canonicalLocalHeaderFields(name, chunkSize, chunkChecksum)
		dataOffset := headerOffset + uint64(len(header))
		segments = append(segments, sparseReadSegment{offset: int64(headerOffset), data: header})
		directoryEntries = append(directoryEntries, sparseZIPEntry{name: name, headerOffset: headerOffset, size: chunkSize, checksum: chunkChecksum})
		chunkRecords = append(chunkRecords, indexRecord{Key: key, DataOffset: dataOffset, Size: chunkSize, CRC32: chunkChecksum})
		nextOffset = dataOffset + uint64(chunkSize)
	}
	indexHeaderOffset := nextOffset
	if indexHeaderOffset <= math.MaxUint32 {
		t.Fatalf("test index Local Header offset = %d, want > 4 GiB", indexHeaderOffset)
	}
	indexPayload, footer, err := buildIndexPayload(metadataEnd, indexHeaderOffset, []indexRecord{manifestRecord}, chunkRecords)
	if err != nil {
		t.Fatal(err)
	}
	indexHeader := canonicalLocalHeader(indexName, indexPayload)
	indexChecksum := crc32.ChecksumIEEE(indexPayload)
	segments = append(segments,
		sparseReadSegment{offset: int64(indexHeaderOffset), data: indexHeader},
		sparseReadSegment{offset: int64(footer.IndexPayloadOffset), data: indexPayload},
	)
	directoryEntries = append(directoryEntries, sparseZIPEntry{
		name: indexName, headerOffset: indexHeaderOffset, size: uint32(len(indexPayload)), checksum: indexChecksum,
	})
	directoryOffset := footer.IndexPayloadOffset + footer.IndexPayloadSize
	var directory bytes.Buffer
	for _, entry := range directoryEntries {
		_, _ = directory.Write(canonicalCentralHeader(entry))
	}
	directoryBytes := directory.Bytes()
	directorySize := uint64(len(directoryBytes))
	zip64Offset := directoryOffset + directorySize
	var zip64End [56]byte
	copy(zip64End[:4], zipDirectory64Magic[:])
	binary.LittleEndian.PutUint64(zip64End[4:12], 44)
	binary.LittleEndian.PutUint16(zip64End[12:14], 45)
	binary.LittleEndian.PutUint16(zip64End[14:16], 45)
	binary.LittleEndian.PutUint64(zip64End[24:32], uint64(len(directoryEntries)))
	binary.LittleEndian.PutUint64(zip64End[32:40], uint64(len(directoryEntries)))
	binary.LittleEndian.PutUint64(zip64End[40:48], directorySize)
	binary.LittleEndian.PutUint64(zip64End[48:56], directoryOffset)
	var locator [20]byte
	copy(locator[:4], zipDirectory64LocatorMagic[:])
	binary.LittleEndian.PutUint64(locator[8:16], zip64Offset)
	binary.LittleEndian.PutUint32(locator[16:20], 1)
	var end [zipDirectoryEndSize]byte
	copy(end[:4], zipDirectoryEndMagic[:])
	binary.LittleEndian.PutUint16(end[8:10], math.MaxUint16)
	binary.LittleEndian.PutUint16(end[10:12], math.MaxUint16)
	binary.LittleEndian.PutUint32(end[12:16], math.MaxUint32)
	binary.LittleEndian.PutUint32(end[16:20], math.MaxUint32)
	archiveSize := int64(zip64Offset + uint64(len(zip64End)) + uint64(len(locator)) + uint64(len(end)))
	segments = append(segments,
		sparseReadSegment{offset: int64(directoryOffset), data: directoryBytes},
		sparseReadSegment{offset: int64(zip64Offset), data: zip64End[:]},
		sparseReadSegment{offset: int64(zip64Offset) + int64(len(zip64End)), data: locator[:]},
		sparseReadSegment{offset: archiveSize - int64(len(end)), data: end[:]},
	)
	source := &sparseSegmentReaderAt{size: archiveSize, segments: segments}
	reader, err := NewReader(source, archiveSize)
	if err != nil {
		t.Fatalf("NewReader with >4 GiB index/CD offset: %v", err)
	}
	defer reader.Close()
	if reader.directoryOffset <= math.MaxUint32 {
		t.Fatalf("directory offset = %d, want > 4 GiB", reader.directoryOffset)
	}
	if err := reader.verifyContainer(context.Background()); err != nil {
		t.Fatalf("strict verification of sparse >4 GiB Bundle: %v", err)
	}
	if got := len(reader.ChunkKeys()); got != chunkCount {
		t.Fatalf("Chunk records = %d, want %d", got, chunkCount)
	}
	result, blob, err := reader.Getter().Get(context.Background(), store.PartitionManifest, manifestKey)
	if err != nil || blob == nil || !bytes.Equal(blob.Bytes(), manifestData) {
		t.Fatalf("Get large-offset Manifest = %v, %v, %v", result, blob, err)
	}
	blob.Release()
}

func BenchmarkBundleOpen100K(b *testing.B) {
	benchmarkBundleOpen(b, maxBundleEntries-3)
}

func BenchmarkBundlePrepareChunks4K(b *testing.B) {
	benchmarkBundlePrepareChunks(b, 4_000)
}

func BenchmarkBundlePrepareChunks20K(b *testing.B) {
	benchmarkBundlePrepareChunks(b, 20_000)
}

func benchmarkBundlePrepareChunks(b *testing.B, chunks int) {
	data := syntheticBundle(b, chunks, 1)
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := reader.prepareChunks(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}

func BenchmarkBundleGetAfterPrepare(b *testing.B) {
	data := syntheticBundle(b, 4_000, 256)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()
	if err := reader.prepareChunks(context.Background()); err != nil {
		b.Fatal(err)
	}
	key := reader.ChunkKeys()[0]
	b.ReportAllocs()
	b.SetBytes(int64(reader.chunks[key].size))
	b.ResetTimer()
	for range b.N {
		_, blob, err := reader.Getter().Get(context.Background(), store.PartitionChunk, key)
		if err != nil {
			b.Fatal(err)
		}
		blob.Release()
	}
}

func BenchmarkBundleOpenHighRTT20K(b *testing.B) {
	data := syntheticBundle(b, 20_000, 1)
	const delay = time.Millisecond
	var calls, bytesRead int64
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		source := &rangeRecordingReaderAt{inner: bytes.NewReader(data), delay: delay}
		reader, err := NewReader(source, int64(len(data)))
		if err != nil {
			b.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
		reads := source.snapshot()
		calls += int64(len(reads))
		bytesRead += readBytes(reads)
	}
	b.ReportMetric(float64(calls)/float64(b.N), "read-calls/op")
	b.ReportMetric(float64(bytesRead)/float64(b.N), "read-bytes/op")
	b.ReportMetric(float64(delay.Milliseconds()), "injected-RTT-ms")
}
