package bundle

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"golang.org/x/sys/unix"
)

var (
	zipLocalHeaderMagic        = [4]byte{'P', 'K', 0x03, 0x04}
	zipDirectoryMagic          = [4]byte{'P', 'K', 0x01, 0x02}
	zipDirectory64Magic        = [4]byte{'P', 'K', 0x06, 0x06}
	zipDirectory64LocatorMagic = [4]byte{'P', 'K', 0x06, 0x07}
	zipDirectoryEndMagic       = [4]byte{'P', 'K', 0x05, 0x06}
)

const (
	readBufferMinClass = 4 << 10
	readBufferMaxClass = 2 << 20
	readBufferMaxBytes = 32 << 20
)

// readBufferPool bounds retained ReaderAt fallback buffers. mmap-backed
// Readers bypass it entirely; unusually large objects are allocated exactly
// and released to the garbage collector instead of being retained.
type readBufferPool struct {
	mu       sync.Mutex
	retained int
	bins     map[int][][]byte
}

func (p *readBufferPool) get(size int) ([]byte, bool) {
	class := readBufferMinClass
	for class < size && class < readBufferMaxClass {
		class <<= 1
	}
	if class < size || class > readBufferMaxClass {
		return make([]byte, size), false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if buffers := p.bins[class]; len(buffers) != 0 {
		last := len(buffers) - 1
		buffer := buffers[last]
		buffers[last] = nil
		if last == 0 {
			delete(p.bins, class)
		} else {
			p.bins[class] = buffers[:last]
		}
		p.retained -= class
		return buffer[:size], true
	}
	return make([]byte, size, class), true
}

func (p *readBufferPool) put(buffer []byte) {
	class := cap(buffer)
	if class < readBufferMinClass || class > readBufferMaxClass || class&(class-1) != 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.retained+class > readBufferMaxBytes {
		return
	}
	if p.bins == nil {
		p.bins = make(map[int][][]byte)
	}
	p.bins[class] = append(p.bins[class], buffer[:class])
	p.retained += class
}

func (p *readBufferPool) clear() {
	p.mu.Lock()
	p.bins = nil
	p.retained = 0
	p.mu.Unlock()
}

type entrySlicer interface {
	Slice(offset int64, size int) ([]byte, error)
}

type archiveEntry struct {
	dataOffset uint64
	size       uint32
	crc32      uint32
}

// Reader indexes one immutable Bundle from its mandatory tail index. Open
// reads the canonical ZIP tail, index footer and Local Header, metadata prefix,
// and Manifest section. The Chunk section is loaded once only after this
// Bundle has been selected as a Manifest source.
type Reader struct {
	source io.ReaderAt
	slicer entrySlicer
	size   int64

	admission       store.WriteAdmission
	refs            []string
	directoryOffset uint64
	directoryCount  uint64
	index           indexFooter
	indexCRC32      uint32
	manifests       map[store.ContentKey]archiveEntry
	chunks          map[store.ContentKey]archiveEntry
	chunkMu         sync.Mutex
	buffers         readBufferPool

	lifeMu         sync.Mutex
	closeRequested bool
	closed         bool
	blobRefs       int64
	cleanup        func() error
	cleanupErr     error
}

// Open memory-maps path read-only when possible and falls back to ReaderAt.
func Open(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("manifest bundle: open %s: %w", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("manifest bundle: stat %s: %w", path, err)
	}
	if info.Size() <= 0 || info.Size() > int64(maxInt()) {
		_ = file.Close()
		return nil, readerr.Mark(fmt.Errorf("manifest bundle: invalid file size %d", info.Size()), false)
	}
	if mapped, mapErr := unix.Mmap(int(file.Fd()), 0, int(info.Size()), unix.PROT_READ, unix.MAP_SHARED); mapErr == nil {
		_ = file.Close()
		source := &mappedSource{data: mapped}
		reader, err := newReader(source, info.Size(), source, source.Close)
		if err != nil {
			_ = source.Close()
			return nil, err
		}
		return reader, nil
	}
	reader, err := newReader(file, info.Size(), nil, file.Close)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return reader, nil
}

// NewReader indexes a caller-owned ReaderAt. Returned object payloads are
// copied into bounded reusable buffers so caller mutations cannot change a
// live Blob.
func NewReader(source io.ReaderAt, size int64) (*Reader, error) {
	return newReader(source, size, nil, nil)
}

func readFullAt(source io.ReaderAt, dst []byte, offset int64) error {
	n, err := source.ReadAt(dst, offset)
	if readerr.IsPermanent(err) {
		return err
	}
	if n < 0 || n > len(dst) {
		if err != nil {
			return readerr.Mark(fmt.Errorf("invalid ReaderAt byte count %d for %d-byte buffer: %w", n, len(dst), err), false)
		}
		return readerr.Mark(fmt.Errorf("invalid ReaderAt byte count %d for %d-byte buffer", n, len(dst)), false)
	}
	if n == len(dst) {
		if err == nil || err == io.EOF {
			return nil
		}
		if err == io.ErrUnexpectedEOF {
			return readerr.Mark(err, false)
		}
		return err
	}
	if err == nil || err == io.EOF || err == io.ErrUnexpectedEOF {
		return readerr.Mark(fmt.Errorf("%w: ReaderAt read %d of %d bytes", io.ErrUnexpectedEOF, n, len(dst)), false)
	}
	return fmt.Errorf("ReaderAt read %d of %d bytes: %w", n, len(dst), err)
}

func newReader(source io.ReaderAt, size int64, slicer entrySlicer, cleanup func() error) (*Reader, error) {
	if source == nil {
		return nil, readerr.Mark(fmt.Errorf("manifest bundle: source is required"), false)
	}
	const directoryEndSize = 22
	if size < directoryEndSize {
		return nil, readerr.Mark(fmt.Errorf("manifest bundle: truncated ZIP"), false)
	}
	var directoryEnd [directoryEndSize]byte
	if err := readFullAt(source, directoryEnd[:], size-directoryEndSize); err != nil {
		return nil, fmt.Errorf("manifest bundle: read ZIP directory end: %w", err)
	}
	if !bytesEqual4(directoryEnd[:4], zipDirectoryEndMagic) || binary.LittleEndian.Uint16(directoryEnd[20:22]) != 0 {
		return nil, readerr.Mark(fmt.Errorf("manifest bundle: Central Directory does not end at archive EOF with an empty comment"), false)
	}
	directoryOffset, directoryRecords, err := readCentralDirectoryLocation(source, size, directoryEnd)
	if err != nil {
		return nil, err
	}
	if directoryRecords > maxBundleEntries {
		return nil, readerr.Mark(fmt.Errorf("manifest bundle: Central Directory record count %d exceeds limit %d", directoryRecords, maxBundleEntries), false)
	}
	if directoryOffset < indexFooterSize {
		return nil, readerr.Mark(fmt.Errorf("manifest bundle: mandatory index footer is missing"), false)
	}
	var encodedFooter [indexFooterSize]byte
	if err := readFullAt(source, encodedFooter[:], directoryOffset-indexFooterSize); err != nil {
		return nil, fmt.Errorf("manifest bundle: read index footer: %w", err)
	}
	footer, err := decodeIndexFooter(encodedFooter[:], uint64(directoryOffset))
	if err != nil {
		return nil, err
	}
	indexCRC, err := readAndValidateIndexLocalHeader(source, size, footer)
	if err != nil {
		return nil, err
	}
	metadataSize, err := uint64AsInt(footer.MetadataPrefixEnd, "metadata prefix")
	if err != nil {
		return nil, err
	}
	metadataPrefix := make([]byte, metadataSize)
	if err := readFullAt(source, metadataPrefix, 0); err != nil {
		return nil, fmt.Errorf("manifest bundle: read metadata prefix: %w", err)
	}
	metadata, metadataEnd, metadataEntries, err := readMetadataPrefix(bytes.NewReader(metadataPrefix), int64(len(metadataPrefix)))
	if err != nil {
		return nil, err
	}
	if uint64(metadataEnd) != footer.MetadataPrefixEnd {
		return nil, readerr.Mark(fmt.Errorf("manifest bundle: metadata prefix end %d differs from index footer %d", metadataEnd, footer.MetadataPrefixEnd), false)
	}
	expectedRecords, err := checkedAdd64(footer.Manifest.Count, footer.Chunk.Count)
	if err == nil {
		expectedRecords, err = checkedAdd64(expectedRecords, uint64(metadataEntries+1)) // mandatory index entry
	}
	if err != nil || expectedRecords != directoryRecords || expectedRecords > maxBundleEntries {
		return nil, readerr.Mark(fmt.Errorf("manifest bundle: index and metadata describe %d ZIP entries, Central Directory reports %d", expectedRecords, directoryRecords), false)
	}
	manifestBytes, err := readIndexSectionAt(source, footer.Manifest, store.PartitionManifest)
	if err != nil {
		return nil, err
	}
	manifests, err := decodeIndexRecords(manifestBytes, footer.Manifest, store.PartitionManifest, footer)
	if err != nil {
		return nil, err
	}
	if err := validateArchiveEntryRanges(manifests, nil); err != nil {
		return nil, err
	}
	return &Reader{
		source:          source,
		slicer:          slicer,
		size:            size,
		admission:       metadata.admission,
		refs:            metadata.refs,
		directoryOffset: uint64(directoryOffset),
		directoryCount:  directoryRecords,
		index:           footer,
		indexCRC32:      indexCRC,
		manifests:       manifests,
		cleanup:         cleanup,
	}, nil
}

func readAndValidateIndexLocalHeader(source io.ReaderAt, archiveSize int64, footer indexFooter) (uint32, error) {
	headerOffset, err := uint64AsInt64(footer.IndexHeaderOffset, "index Local Header offset")
	if err != nil {
		return 0, err
	}
	if headerOffset < 0 || int64(indexLocalHeaderSize) > archiveSize-headerOffset {
		return 0, readerr.Mark(fmt.Errorf("manifest bundle: index Local Header is outside archive"), false)
	}
	var header [indexLocalHeaderSize]byte
	if err := readFullAt(source, header[:], headerOffset); err != nil {
		return 0, fmt.Errorf("manifest bundle: read index Local Header: %w", err)
	}
	if !bytesEqual4(header[:4], zipLocalHeaderMagic) {
		return 0, readerr.Mark(fmt.Errorf("manifest bundle: index Local Header magic is invalid"), false)
	}
	if binary.LittleEndian.Uint16(header[4:6]) != 45 || binary.LittleEndian.Uint16(header[6:8]) != 0 ||
		binary.LittleEndian.Uint16(header[8:10]) != zip.Store || binary.LittleEndian.Uint16(header[10:12]) != 0 ||
		binary.LittleEndian.Uint16(header[12:14]) != 0 {
		return 0, readerr.Mark(fmt.Errorf("manifest bundle: index Local Header is not canonical Store metadata"), false)
	}
	if footer.IndexPayloadSize > math.MaxUint32 || uint64(binary.LittleEndian.Uint32(header[18:22])) != footer.IndexPayloadSize ||
		uint64(binary.LittleEndian.Uint32(header[22:26])) != footer.IndexPayloadSize {
		return 0, readerr.Mark(fmt.Errorf("manifest bundle: index Local Header size differs from footer"), false)
	}
	if int(binary.LittleEndian.Uint16(header[26:28])) != len(indexName) || binary.LittleEndian.Uint16(header[28:30]) != 0 {
		return 0, readerr.Mark(fmt.Errorf("manifest bundle: index Local Header name or extra field is not canonical"), false)
	}
	if string(header[zipLocalHeaderFixedSize:]) != indexName {
		return 0, readerr.Mark(fmt.Errorf("manifest bundle: index Local Header name is invalid"), false)
	}
	if footer.IndexHeaderOffset+uint64(indexLocalHeaderSize) != footer.IndexPayloadOffset {
		return 0, readerr.Mark(fmt.Errorf("manifest bundle: index Local Header does not precede its payload"), false)
	}
	return binary.LittleEndian.Uint32(header[14:18]), nil
}

func readIndexSectionAt(source io.ReaderAt, section indexSection, partition store.Partition) ([]byte, error) {
	size, err := uint64AsInt(section.Size, string(partition)+" index section")
	if err != nil {
		return nil, err
	}
	offset, err := uint64AsInt64(section.Offset, string(partition)+" index section offset")
	if err != nil {
		return nil, err
	}
	payload := make([]byte, size)
	if size != 0 {
		if err := readFullAt(source, payload, offset); err != nil {
			return nil, fmt.Errorf("manifest bundle: read %s index section: %w", partition, err)
		}
	}
	return payload, nil
}

func readCentralDirectoryLocation(source io.ReaderAt, archiveSize int64, end [22]byte) (int64, uint64, error) {
	if binary.LittleEndian.Uint16(end[4:6]) != 0 || binary.LittleEndian.Uint16(end[6:8]) != 0 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: multi-disk ZIP is not allowed"), false)
	}
	recordsDisk := binary.LittleEndian.Uint16(end[8:10])
	recordsTotal := binary.LittleEndian.Uint16(end[10:12])
	directorySize32 := binary.LittleEndian.Uint32(end[12:16])
	directoryOffset32 := binary.LittleEndian.Uint32(end[16:20])
	zip64 := recordsDisk == math.MaxUint16 || recordsTotal == math.MaxUint16 ||
		directorySize32 == math.MaxUint32 || directoryOffset32 == math.MaxUint32
	if !zip64 {
		if recordsDisk != recordsTotal {
			return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: Central Directory record counts differ"), false)
		}
		offset := uint64(directoryOffset32)
		directorySize := uint64(directorySize32)
		endOffset := uint64(archiveSize - int64(len(end)))
		if offset > endOffset || directorySize != endOffset-offset {
			return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: Central Directory range is not contiguous with EOCD"), false)
		}
		return int64(offset), uint64(recordsTotal), nil
	}
	if recordsDisk != math.MaxUint16 || recordsTotal != math.MaxUint16 ||
		directorySize32 != math.MaxUint32 || directoryOffset32 != math.MaxUint32 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: non-canonical partial ZIP64 EOCD sentinels"), false)
	}

	const locatorSize = 20
	locatorOffset := archiveSize - int64(len(end)) - locatorSize
	if locatorOffset < 0 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP64 locator is missing"), false)
	}
	var locator [locatorSize]byte
	if err := readFullAt(source, locator[:], locatorOffset); err != nil {
		return 0, 0, fmt.Errorf("manifest bundle: read ZIP64 locator: %w", err)
	}
	if !bytesEqual4(locator[:4], zipDirectory64LocatorMagic) ||
		binary.LittleEndian.Uint32(locator[4:8]) != 0 || binary.LittleEndian.Uint32(locator[16:20]) != 1 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP64 locator is not canonical"), false)
	}
	zip64Offset := binary.LittleEndian.Uint64(locator[8:16])
	const zip64EndSize = 56
	if zip64Offset > uint64(locatorOffset) || zip64EndSize > uint64(locatorOffset)-zip64Offset {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP64 EOCD is outside archive"), false)
	}
	var zip64End [zip64EndSize]byte
	if err := readFullAt(source, zip64End[:], int64(zip64Offset)); err != nil {
		return 0, 0, fmt.Errorf("manifest bundle: read ZIP64 EOCD: %w", err)
	}
	if !bytesEqual4(zip64End[:4], zipDirectory64Magic) || binary.LittleEndian.Uint64(zip64End[4:12]) != 44 ||
		binary.LittleEndian.Uint16(zip64End[12:14]) != 45 || binary.LittleEndian.Uint16(zip64End[14:16]) != 45 ||
		binary.LittleEndian.Uint32(zip64End[16:20]) != 0 || binary.LittleEndian.Uint32(zip64End[20:24]) != 0 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP64 EOCD is not canonical"), false)
	}
	if zip64Offset+zip64EndSize != uint64(locatorOffset) {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP64 EOCD and locator are not contiguous"), false)
	}
	recordsDisk64 := binary.LittleEndian.Uint64(zip64End[24:32])
	recordsTotal64 := binary.LittleEndian.Uint64(zip64End[32:40])
	if recordsDisk64 != recordsTotal64 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP64 Central Directory record counts differ"), false)
	}
	directorySize := binary.LittleEndian.Uint64(zip64End[40:48])
	directoryOffset := binary.LittleEndian.Uint64(zip64End[48:56])
	if directoryOffset > zip64Offset || directorySize != zip64Offset-directoryOffset || directoryOffset > math.MaxInt64 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP64 Central Directory range is not contiguous with EOCD"), false)
	}
	return int64(directoryOffset), recordsTotal64, nil
}

func parseObjectEntry(name, prefix string) (store.ContentKey, error) {
	raw := strings.TrimPrefix(name, prefix)
	if strings.Contains(raw, "/") {
		return store.ContentKey{}, readerr.Mark(fmt.Errorf("manifest bundle: malformed ZIP entry %q", name), false)
	}
	key, err := parseLowerHexKey(raw)
	if err != nil {
		return key, fmt.Errorf("manifest bundle: malformed ZIP entry %q: %w", name, err)
	}
	return key, nil
}

func validateEntryHeader(file *zip.File) error {
	if file.Name == "" || strings.HasSuffix(file.Name, "/") {
		return readerr.Mark(fmt.Errorf("manifest bundle: directory or empty ZIP entry %q is not allowed", file.Name), false)
	}
	if file.Method != zip.Store {
		return readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q method %d, want Store", file.Name, file.Method), false)
	}
	if file.Flags != 0 {
		return readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q flags 0x%x are not allowed", file.Name, file.Flags), false)
	}
	if file.CompressedSize64 != file.UncompressedSize64 {
		return readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q compressed size differs under Store", file.Name), false)
	}
	if file.Comment != "" || file.ModifiedTime != 0 || file.ModifiedDate != 0 || file.ExternalAttrs != 0 || file.NonUTF8 {
		return readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q has non-canonical metadata", file.Name), false)
	}
	if file.CreatorVersion != 45 {
		return readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q creator version %d is not canonical", file.Name, file.CreatorVersion), false)
	}
	if file.ReaderVersion != 45 {
		return readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q reader version %d is not canonical", file.Name, file.ReaderVersion), false)
	}
	if err := validateExtra(file.Extra); err != nil {
		return fmt.Errorf("manifest bundle: ZIP entry %q: %w", file.Name, err)
	}
	return nil
}

func validateExtra(extra []byte) error {
	seenZIP64 := false
	for len(extra) != 0 {
		if len(extra) < 4 {
			return readerr.Mark(fmt.Errorf("truncated ZIP extra field"), false)
		}
		id := binary.LittleEndian.Uint16(extra[:2])
		size := int(binary.LittleEndian.Uint16(extra[2:4]))
		extra = extra[4:]
		if size > len(extra) {
			return readerr.Mark(fmt.Errorf("truncated ZIP extra payload"), false)
		}
		if id != 0x0001 || seenZIP64 {
			return readerr.Mark(fmt.Errorf("non-ZIP64 extra field 0x%04x is not allowed", id), false)
		}
		if size != 8 && size != 16 && size != 24 && size != 28 {
			return readerr.Mark(fmt.Errorf("invalid ZIP64 extra size %d", size), false)
		}
		seenZIP64 = true
		extra = extra[size:]
	}
	return nil
}

// Admission returns the exact admission recorded in the Bundle name entry.
func (r *Reader) Admission() store.WriteAdmission { return r.admission }

// Refs returns a copy of the immutable ordered external Bundle search path.
func (r *Reader) Refs() []string { return append([]string(nil), r.refs...) }

func (r *Reader) refsView() []string { return r.refs }

func (r *Reader) HasManifest(key store.ContentKey) bool {
	_, ok := r.manifests[key]
	return ok
}

func (r *Reader) HasChunk(key store.ContentKey) bool {
	if err := r.prepareChunks(context.Background()); err != nil {
		return false
	}
	_, ok := r.chunks[key]
	return ok
}

func (r *Reader) ManifestKeys() []store.ContentKey {
	return sortedKeys(r.manifests)
}

func (r *Reader) ChunkKeys() []store.ContentKey {
	if err := r.prepareChunks(context.Background()); err != nil {
		return nil
	}
	return sortedKeys(r.chunks)
}

// prepareChunks loads and validates the complete Chunk section in one
// contiguous ReaderAt call. Only a fully validated successful index is
// retained. Failure leaves the next caller free to make another attempt.
func (r *Reader) prepareChunks(ctx context.Context) error {
	if ctx == nil {
		return readerr.Mark(fmt.Errorf("manifest bundle: Chunk index preparation context is required"), false)
	}
	r.chunkMu.Lock()
	defer r.chunkMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	r.lifeMu.Lock()
	closed := r.closeRequested || r.closed
	r.lifeMu.Unlock()
	if closed {
		return ErrClosed
	}
	if r.chunks != nil {
		return nil
	}
	return r.loadChunkIndex(ctx)
}

func (r *Reader) loadChunkIndex(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.retainBlob(); err != nil {
		return err
	}
	defer r.releaseBlob()
	payload, err := readIndexSectionAt(r.source, r.index.Chunk, store.PartitionChunk)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	chunks, err := decodeIndexRecords(payload, r.index.Chunk, store.PartitionChunk, r.index)
	if err != nil {
		return err
	}
	if err := validateArchiveEntryRanges(r.manifests, chunks); err != nil {
		return err
	}
	r.lifeMu.Lock()
	defer r.lifeMu.Unlock()
	if r.closeRequested || r.closed {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.chunks = chunks
	return nil
}

// Getter returns the Bundle-only object Getter. Misses never consult another
// source; use NewManifestFetcher to make the source decision at Manifest level.
func (r *Reader) Getter() cache.Getter { return &getter{reader: r} }

type getter struct{ reader *Reader }

func (g *getter) Get(ctx context.Context, partition store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	if err := ctx.Err(); err != nil {
		return cache.CacheMiss, nil, err
	}
	var entry archiveEntry
	var ok bool
	switch partition {
	case store.PartitionManifest:
		entry, ok = g.reader.manifests[key]
	case store.PartitionChunk:
		if err := g.reader.prepareChunks(ctx); err != nil {
			return cache.CacheMiss, nil, err
		}
		entry, ok = g.reader.chunks[key]
	default:
		return cache.CacheMiss, nil, nil
	}
	if !ok {
		return cache.CacheMiss, nil, nil
	}
	blob, err := g.reader.entryBlob(partition, key, entry)
	if err != nil {
		return cache.CacheMiss, nil, err
	}
	return cache.CacheHit, blob, nil
}

// validateManifestClosure is reserved for explicit verification and exact
// upload. Ordinary Fetcher.OpenManifest deliberately does not expose this as
// fetch.ManifestValidator and therefore does not scan an unvisited closure.
func (r *Reader) validateManifestClosure(key store.ContentKey, manifest *codec.Manifest) error {
	if !r.HasManifest(key) {
		return readerr.Mark(fmt.Errorf("manifest bundle: Manifest %s is not local", hex.EncodeToString(key[:])), false)
	}
	for index, entry := range manifest.Entries {
		if entry.IsZero {
			continue
		}
		chunkKey := store.ContentKey(entry.CiphertextHash)
		if _, ok := r.chunks[chunkKey]; !ok {
			return fmt.Errorf("%w: Manifest %s Chunk %d (%s) is absent", ErrIncomplete, hex.EncodeToString(key[:]), index, hex.EncodeToString(chunkKey[:]))
		}
	}
	return nil
}

func (r *Reader) entryBlob(partition store.Partition, key store.ContentKey, entry archiveEntry) (cache.Blob, error) {
	if err := r.retainBlob(); err != nil {
		return nil, err
	}
	releaseOnError := true
	defer func() {
		if releaseOnError {
			r.releaseBlob()
		}
	}()
	size := int(entry.size)
	offset, err := uint64AsInt64(entry.dataOffset, string(partition)+" data offset")
	if err != nil {
		return nil, err
	}
	if offset < 0 || offset > r.size || int64(size) > r.size-offset {
		return nil, readerr.Mark(fmt.Errorf("manifest bundle: %s %s data range is outside archive", partition, hex.EncodeToString(key[:])), false)
	}
	var data []byte
	var pooled []byte
	if r.slicer != nil {
		data, err = r.slicer.Slice(offset, size)
	} else {
		var reusable bool
		data, reusable = r.buffers.get(size)
		if reusable {
			pooled = data
		}
		err = readFullAt(r.source, data, offset)
	}
	if err != nil {
		if pooled != nil {
			r.buffers.put(pooled)
		}
		return nil, fmt.Errorf("manifest bundle: read %s %s: %w", partition, hex.EncodeToString(key[:]), err)
	}
	backing := &blobBacking{data: data[:len(data):len(data)], pooled: pooled, pool: &r.buffers}
	backing.refs.Store(1)
	releaseOnError = false
	return &readerBlob{reader: r, backing: backing}, nil
}

// validateLocalHeaderAt proves that the next Central Directory item is also
// the next physical local entry. Starting at offset zero and carrying the
// returned end offset across every entry rejects reordering, hidden entries,
// and arbitrary gaps without reading any object payload.
func (r *Reader) validateLocalHeaderAt(file *zip.File, headerOffset int64, scratch []byte) (int64, int64, error) {
	const fixedSize = 30
	headerSize := fixedSize + len(file.Name)
	if headerOffset < 0 || int64(headerSize) > r.size-headerOffset {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q local header is outside archive", file.Name), false)
	}
	if headerSize > len(scratch) {
		scratch = make([]byte, headerSize)
	}
	header := scratch[:headerSize]
	if err := readFullAt(r.source, header, headerOffset); err != nil {
		return 0, 0, fmt.Errorf("manifest bundle: read ZIP entry %q local header: %w", file.Name, err)
	}
	if !bytesEqual4(header[:4], zipLocalHeaderMagic) {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q does not match physical local-header order", file.Name), false)
	}
	if version := binary.LittleEndian.Uint16(header[4:6]); version != 45 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q local reader version %d is not canonical", file.Name, version), false)
	}
	if flags := binary.LittleEndian.Uint16(header[6:8]); flags != 0 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q local flags 0x%x are not allowed", file.Name, flags), false)
	}
	if method := binary.LittleEndian.Uint16(header[8:10]); method != zip.Store {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q local method %d, want Store", file.Name, method), false)
	}
	if binary.LittleEndian.Uint16(header[10:12]) != 0 || binary.LittleEndian.Uint16(header[12:14]) != 0 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q has non-canonical local timestamp", file.Name), false)
	}
	if crc := binary.LittleEndian.Uint32(header[14:18]); crc != file.CRC32 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q local CRC differs from Central Directory", file.Name), false)
	}
	if file.CompressedSize64 > math.MaxUint32 || file.UncompressedSize64 > math.MaxUint32 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q requires a non-canonical local ZIP64 size", file.Name), false)
	}
	if size := binary.LittleEndian.Uint32(header[18:22]); uint64(size) != file.CompressedSize64 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q local compressed size differs from Central Directory", file.Name), false)
	}
	if size := binary.LittleEndian.Uint32(header[22:26]); uint64(size) != file.UncompressedSize64 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q local uncompressed size differs from Central Directory", file.Name), false)
	}
	if nameSize := int(binary.LittleEndian.Uint16(header[26:28])); nameSize != len(file.Name) {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q local name length differs from Central Directory", file.Name), false)
	}
	if extraSize := binary.LittleEndian.Uint16(header[28:30]); extraSize != 0 {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q local extra field is not allowed", file.Name), false)
	}
	if string(header[fixedSize:]) != file.Name {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q local name differs from Central Directory", file.Name), false)
	}
	dataOffset := headerOffset + int64(headerSize)
	if dataOffset < 0 || uint64(dataOffset) > uint64(r.size) || file.CompressedSize64 > uint64(r.size-dataOffset) {
		return 0, 0, readerr.Mark(fmt.Errorf("manifest bundle: ZIP entry %q data range is outside archive", file.Name), false)
	}
	return dataOffset, dataOffset + int64(file.CompressedSize64), nil
}

func bytesEqual4(data []byte, magic [4]byte) bool {
	return len(data) == len(magic) && data[0] == magic[0] && data[1] == magic[1] && data[2] == magic[2] && data[3] == magic[3]
}

func (r *Reader) retainBlob() error {
	r.lifeMu.Lock()
	defer r.lifeMu.Unlock()
	if r.closeRequested || r.closed {
		return ErrClosed
	}
	r.blobRefs++
	return nil
}

func (r *Reader) cloneBlob() error {
	r.lifeMu.Lock()
	defer r.lifeMu.Unlock()
	if r.closed {
		return ErrClosed
	}
	r.blobRefs++
	return nil
}

func (r *Reader) releaseBlob() {
	r.lifeMu.Lock()
	defer r.lifeMu.Unlock()
	r.blobRefs--
	if r.blobRefs < 0 {
		panic("manifest bundle: negative Blob reference count")
	}
	r.cleanupLocked()
}

func (r *Reader) cleanupLocked() {
	if !r.closeRequested || r.closed || r.blobRefs != 0 {
		return
	}
	r.closed = true
	if r.cleanup != nil {
		r.cleanupErr = r.cleanup()
	}
	r.buffers.clear()
}

// Close prevents new reads and releases the mmap/file after outstanding Blobs
// are released. Callers close Streams before the Reader in normal use.
func (r *Reader) Close() error {
	r.lifeMu.Lock()
	defer r.lifeMu.Unlock()
	r.closeRequested = true
	r.cleanupLocked()
	return r.cleanupErr
}

type readerBlob struct {
	reader   *Reader
	backing  *blobBacking
	released atomic.Bool
}

type blobBacking struct {
	data   []byte
	pooled []byte
	pool   *readBufferPool
	refs   atomic.Int64
}

func (b *blobBacking) retain() bool {
	for {
		refs := b.refs.Load()
		if refs == 0 {
			return false
		}
		if b.refs.CompareAndSwap(refs, refs+1) {
			return true
		}
	}
}

func (b *blobBacking) release() {
	refs := b.refs.Add(-1)
	if refs < 0 {
		panic("manifest bundle: negative Blob backing reference count")
	}
	if refs != 0 {
		return
	}
	if b.pooled != nil {
		b.pool.put(b.pooled)
	}
	b.data = nil
	b.pooled = nil
}

func (b *readerBlob) Bytes() []byte {
	if b.released.Load() {
		panic("manifest bundle: Bytes after Blob Release")
	}
	return b.backing.data
}

func (b *readerBlob) Clone() cache.Blob {
	if b.released.Load() {
		panic("manifest bundle: Clone after Blob Release")
	}
	if !b.backing.retain() {
		panic("manifest bundle: Clone after Blob backing release")
	}
	if err := b.reader.cloneBlob(); err != nil {
		b.backing.release()
		panic(err)
	}
	return &readerBlob{reader: b.reader, backing: b.backing}
}

func (b *readerBlob) Release() {
	if !b.released.CompareAndSwap(false, true) {
		panic("manifest bundle: double Blob Release")
	}
	b.backing.release()
	b.backing = nil
	b.reader.releaseBlob()
}

type mappedSource struct{ data []byte }

func (m *mappedSource) ReadAt(dst []byte, offset int64) (int, error) {
	if offset < 0 || offset >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(dst, m.data[offset:])
	if n != len(dst) {
		return n, io.EOF
	}
	return n, nil
}

func (m *mappedSource) Slice(offset int64, size int) ([]byte, error) {
	if offset < 0 || size < 0 || offset > int64(len(m.data)) || int64(size) > int64(len(m.data))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	end := offset + int64(size)
	return m.data[offset:end:end], nil
}

func (m *mappedSource) Close() error {
	if m.data == nil {
		return nil
	}
	err := unix.Munmap(m.data)
	m.data = nil
	return err
}

func maxInt() int { return int(math.MaxInt) }
