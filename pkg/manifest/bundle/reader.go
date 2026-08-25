package bundle

import (
	"archive/zip"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
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
	file   *zip.File
	offset int64
}

// Reader indexes one immutable Bundle. Construction reads ZIP metadata only;
// Manifest payloads are parsed when selected and Chunk payloads stay untouched
// until requested.
type Reader struct {
	source io.ReaderAt
	slicer entrySlicer
	size   int64

	admission store.WriteAdmission
	refs      []string
	manifests map[store.ContentKey]*archiveEntry
	chunks    map[store.ContentKey]*archiveEntry
	buffers   readBufferPool

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
		return nil, fmt.Errorf("manifest bundle: invalid file size %d", info.Size())
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
	if n < 0 || n > len(dst) {
		if err != nil {
			return fmt.Errorf("invalid ReaderAt byte count %d for %d-byte buffer: %w", n, len(dst), err)
		}
		return fmt.Errorf("invalid ReaderAt byte count %d for %d-byte buffer", n, len(dst))
	}
	if n == len(dst) {
		if err == nil || errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	if err == nil || errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: ReaderAt read %d of %d bytes", io.ErrUnexpectedEOF, n, len(dst))
	}
	return fmt.Errorf("ReaderAt read %d of %d bytes: %w", n, len(dst), err)
}

func newReader(source io.ReaderAt, size int64, slicer entrySlicer, cleanup func() error) (*Reader, error) {
	if source == nil {
		return nil, fmt.Errorf("manifest bundle: source is required")
	}
	const directoryEndSize = 22
	if size < directoryEndSize {
		return nil, fmt.Errorf("manifest bundle: truncated ZIP")
	}
	var magic [4]byte
	if err := readFullAt(source, magic[:], 0); err != nil {
		return nil, fmt.Errorf("manifest bundle: read ZIP magic: %w", err)
	}
	if magic != zipLocalHeaderMagic {
		return nil, fmt.Errorf("manifest bundle: invalid ZIP local-header magic %x", magic)
	}
	var directoryEnd [directoryEndSize]byte
	if err := readFullAt(source, directoryEnd[:], size-directoryEndSize); err != nil {
		return nil, fmt.Errorf("manifest bundle: read ZIP directory end: %w", err)
	}
	if !bytesEqual4(directoryEnd[:4], zipDirectoryEndMagic) || binary.LittleEndian.Uint16(directoryEnd[20:22]) != 0 {
		return nil, fmt.Errorf("manifest bundle: Central Directory does not end at archive EOF with an empty comment")
	}
	directoryOffset, directoryRecords, err := readCentralDirectoryLocation(source, size, directoryEnd)
	if err != nil {
		return nil, err
	}
	zr, err := zip.NewReader(source, size)
	if err != nil {
		return nil, fmt.Errorf("manifest bundle: parse ZIP: %w", err)
	}
	if zr.Comment != "" {
		return nil, fmt.Errorf("manifest bundle: ZIP archive comment is not allowed")
	}
	if len(zr.File) > maxBundleEntries {
		return nil, fmt.Errorf("manifest bundle: %d entries exceed limit %d", len(zr.File), maxBundleEntries)
	}
	if directoryRecords != uint64(len(zr.File)) {
		return nil, fmt.Errorf("manifest bundle: Central Directory record count %d differs from parsed count %d", directoryRecords, len(zr.File))
	}
	r := &Reader{
		source:    source,
		slicer:    slicer,
		size:      size,
		manifests: make(map[store.ContentKey]*archiveEntry),
		chunks:    make(map[store.ContentKey]*archiveEntry),
		cleanup:   cleanup,
	}
	seenNames := make(map[string]struct{}, len(zr.File))
	admissions := 0
	var refsEntry *archiveEntry
	var expectedOffset int64
	var localHeaderScratch [256]byte
	for index, file := range zr.File {
		if _, duplicate := seenNames[file.Name]; duplicate {
			return nil, fmt.Errorf("manifest bundle: duplicate ZIP entry %q", file.Name)
		}
		seenNames[file.Name] = struct{}{}
		if err := validateEntryHeader(file); err != nil {
			return nil, err
		}
		dataOffset, nextOffset, err := r.validateLocalHeaderAt(file, expectedOffset, localHeaderScratch[:])
		if err != nil {
			return nil, err
		}
		expectedOffset = nextOffset
		entry := &archiveEntry{file: file, offset: dataOffset}
		switch {
		case file.Name == refsName:
			if index != 0 {
				return nil, fmt.Errorf("manifest bundle: %s must be the first ZIP entry", refsName)
			}
			if file.UncompressedSize64 == 0 {
				return nil, fmt.Errorf("manifest bundle: refs payload must be non-empty")
			}
			if file.UncompressedSize64 > maxRefsPayload {
				return nil, fmt.Errorf("manifest bundle: refs payload exceeds %d bytes", maxRefsPayload)
			}
			refsEntry = entry
		case strings.HasPrefix(file.Name, admissionPrefix):
			admissions++
			if admissions > 1 {
				return nil, fmt.Errorf("manifest bundle: multiple admission entries")
			}
			if file.UncompressedSize64 != 0 {
				return nil, fmt.Errorf("manifest bundle: admission payload must be empty")
			}
			if file.CRC32 != crc32.ChecksumIEEE(nil) {
				return nil, fmt.Errorf("manifest bundle: admission entry has non-canonical CRC")
			}
			wantIndex := 0
			if refsEntry != nil {
				wantIndex = 1
			}
			if index != wantIndex {
				return nil, fmt.Errorf("manifest bundle: admission must be ZIP entry %d", wantIndex)
			}
			admission, err := parseAdmissionName(file.Name)
			if err != nil {
				return nil, err
			}
			r.admission = admission
		case strings.HasPrefix(file.Name, legacyAdmissionPrefix):
			return nil, fmt.Errorf("manifest bundle: legacy admission entry %q is not allowed", file.Name)
		case strings.HasPrefix(file.Name, manifestPrefix):
			if admissions == 0 {
				return nil, fmt.Errorf("manifest bundle: object entry %q precedes admission", file.Name)
			}
			key, err := parseObjectEntry(file.Name, manifestPrefix)
			if err != nil {
				return nil, err
			}
			if file.UncompressedSize64 < 5 || file.UncompressedSize64 > uint64(codec.MaxManifestDecodedSize)+1 {
				return nil, fmt.Errorf("manifest bundle: Manifest %s physical size %d is invalid", hex.EncodeToString(key[:]), file.UncompressedSize64)
			}
			if _, duplicate := r.manifests[key]; duplicate {
				return nil, fmt.Errorf("manifest bundle: duplicate Manifest key %s", hex.EncodeToString(key[:]))
			}
			r.manifests[key] = entry
		case strings.HasPrefix(file.Name, chunkPrefix):
			if admissions == 0 {
				return nil, fmt.Errorf("manifest bundle: object entry %q precedes admission", file.Name)
			}
			key, err := parseObjectEntry(file.Name, chunkPrefix)
			if err != nil {
				return nil, err
			}
			if file.UncompressedSize64 == 0 || file.UncompressedSize64 > uint64(codec.MaxChunkDecodedSize)+1 {
				return nil, fmt.Errorf("manifest bundle: Chunk %s physical size %d is invalid", hex.EncodeToString(key[:]), file.UncompressedSize64)
			}
			if _, duplicate := r.chunks[key]; duplicate {
				return nil, fmt.Errorf("manifest bundle: duplicate Chunk key %s", hex.EncodeToString(key[:]))
			}
			r.chunks[key] = entry
		default:
			if strings.HasPrefix(file.Name, "bundle/") {
				return nil, fmt.Errorf("manifest bundle: unknown bundle metadata entry %q", file.Name)
			}
			return nil, fmt.Errorf("manifest bundle: unknown ZIP entry %q", file.Name)
		}
	}
	if expectedOffset != directoryOffset {
		return nil, fmt.Errorf("manifest bundle: local entries are not contiguous with the Central Directory")
	}
	if admissions != 1 {
		return nil, fmt.Errorf("manifest bundle: exactly one admission entry is required")
	}
	canonicalSalt, err := store.SaltForGeneration(r.admission.Generation)
	if err != nil {
		return nil, fmt.Errorf("manifest bundle: admission: %w", err)
	}
	if r.admission.Salt != canonicalSalt {
		return nil, fmt.Errorf("manifest bundle: admission salt is not canonical for generation %q", r.admission.Generation)
	}
	if refsEntry != nil {
		payload, err := r.readMetadataEntry(refsEntry)
		if err != nil {
			return nil, err
		}
		r.refs, err = ParseRefs(payload)
		if err != nil {
			return nil, err
		}
	}
	if len(r.manifests) == 0 {
		return nil, fmt.Errorf("manifest bundle: at least one Manifest entry is required")
	}
	return r, nil
}

func readCentralDirectoryLocation(source io.ReaderAt, archiveSize int64, end [22]byte) (int64, uint64, error) {
	if binary.LittleEndian.Uint16(end[4:6]) != 0 || binary.LittleEndian.Uint16(end[6:8]) != 0 {
		return 0, 0, fmt.Errorf("manifest bundle: multi-disk ZIP is not allowed")
	}
	recordsDisk := binary.LittleEndian.Uint16(end[8:10])
	recordsTotal := binary.LittleEndian.Uint16(end[10:12])
	directorySize32 := binary.LittleEndian.Uint32(end[12:16])
	directoryOffset32 := binary.LittleEndian.Uint32(end[16:20])
	zip64 := recordsDisk == math.MaxUint16 || recordsTotal == math.MaxUint16 ||
		directorySize32 == math.MaxUint32 || directoryOffset32 == math.MaxUint32
	if !zip64 {
		if recordsDisk != recordsTotal {
			return 0, 0, fmt.Errorf("manifest bundle: Central Directory record counts differ")
		}
		offset := uint64(directoryOffset32)
		directorySize := uint64(directorySize32)
		endOffset := uint64(archiveSize - int64(len(end)))
		if offset > endOffset || directorySize != endOffset-offset {
			return 0, 0, fmt.Errorf("manifest bundle: Central Directory range is not contiguous with EOCD")
		}
		return int64(offset), uint64(recordsTotal), nil
	}
	if recordsDisk != math.MaxUint16 || recordsTotal != math.MaxUint16 ||
		directorySize32 != math.MaxUint32 || directoryOffset32 != math.MaxUint32 {
		return 0, 0, fmt.Errorf("manifest bundle: non-canonical partial ZIP64 EOCD sentinels")
	}

	const locatorSize = 20
	locatorOffset := archiveSize - int64(len(end)) - locatorSize
	if locatorOffset < 0 {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP64 locator is missing")
	}
	var locator [locatorSize]byte
	if err := readFullAt(source, locator[:], locatorOffset); err != nil {
		return 0, 0, fmt.Errorf("manifest bundle: read ZIP64 locator: %w", err)
	}
	if !bytesEqual4(locator[:4], zipDirectory64LocatorMagic) ||
		binary.LittleEndian.Uint32(locator[4:8]) != 0 || binary.LittleEndian.Uint32(locator[16:20]) != 1 {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP64 locator is not canonical")
	}
	zip64Offset := binary.LittleEndian.Uint64(locator[8:16])
	const zip64EndSize = 56
	if zip64Offset > uint64(locatorOffset) || zip64EndSize > uint64(locatorOffset)-zip64Offset {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP64 EOCD is outside archive")
	}
	var zip64End [zip64EndSize]byte
	if err := readFullAt(source, zip64End[:], int64(zip64Offset)); err != nil {
		return 0, 0, fmt.Errorf("manifest bundle: read ZIP64 EOCD: %w", err)
	}
	if !bytesEqual4(zip64End[:4], zipDirectory64Magic) || binary.LittleEndian.Uint64(zip64End[4:12]) != 44 ||
		binary.LittleEndian.Uint16(zip64End[12:14]) != 45 || binary.LittleEndian.Uint16(zip64End[14:16]) != 45 ||
		binary.LittleEndian.Uint32(zip64End[16:20]) != 0 || binary.LittleEndian.Uint32(zip64End[20:24]) != 0 {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP64 EOCD is not canonical")
	}
	if zip64Offset+zip64EndSize != uint64(locatorOffset) {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP64 EOCD and locator are not contiguous")
	}
	recordsDisk64 := binary.LittleEndian.Uint64(zip64End[24:32])
	recordsTotal64 := binary.LittleEndian.Uint64(zip64End[32:40])
	if recordsDisk64 != recordsTotal64 {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP64 Central Directory record counts differ")
	}
	directorySize := binary.LittleEndian.Uint64(zip64End[40:48])
	directoryOffset := binary.LittleEndian.Uint64(zip64End[48:56])
	if directoryOffset > zip64Offset || directorySize != zip64Offset-directoryOffset || directoryOffset > math.MaxInt64 {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP64 Central Directory range is not contiguous with EOCD")
	}
	return int64(directoryOffset), recordsTotal64, nil
}

func parseObjectEntry(name, prefix string) (store.ContentKey, error) {
	raw := strings.TrimPrefix(name, prefix)
	if strings.Contains(raw, "/") {
		return store.ContentKey{}, fmt.Errorf("manifest bundle: malformed ZIP entry %q", name)
	}
	key, err := parseLowerHexKey(raw)
	if err != nil {
		return key, fmt.Errorf("manifest bundle: malformed ZIP entry %q: %w", name, err)
	}
	return key, nil
}

func validateEntryHeader(file *zip.File) error {
	if file.Name == "" || strings.HasSuffix(file.Name, "/") {
		return fmt.Errorf("manifest bundle: directory or empty ZIP entry %q is not allowed", file.Name)
	}
	if file.Method != zip.Store {
		return fmt.Errorf("manifest bundle: ZIP entry %q method %d, want Store", file.Name, file.Method)
	}
	if file.Flags != 0 {
		return fmt.Errorf("manifest bundle: ZIP entry %q flags 0x%x are not allowed", file.Name, file.Flags)
	}
	if file.CompressedSize64 != file.UncompressedSize64 {
		return fmt.Errorf("manifest bundle: ZIP entry %q compressed size differs under Store", file.Name)
	}
	if file.Comment != "" || file.ModifiedTime != 0 || file.ModifiedDate != 0 || file.ExternalAttrs != 0 || file.NonUTF8 {
		return fmt.Errorf("manifest bundle: ZIP entry %q has non-canonical metadata", file.Name)
	}
	if file.CreatorVersion != 45 {
		return fmt.Errorf("manifest bundle: ZIP entry %q creator version %d is not canonical", file.Name, file.CreatorVersion)
	}
	if file.ReaderVersion != 45 {
		return fmt.Errorf("manifest bundle: ZIP entry %q reader version %d is not canonical", file.Name, file.ReaderVersion)
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
			return fmt.Errorf("truncated ZIP extra field")
		}
		id := binary.LittleEndian.Uint16(extra[:2])
		size := int(binary.LittleEndian.Uint16(extra[2:4]))
		extra = extra[4:]
		if size > len(extra) {
			return fmt.Errorf("truncated ZIP extra payload")
		}
		if id != 0x0001 || seenZIP64 {
			return fmt.Errorf("non-ZIP64 extra field 0x%04x is not allowed", id)
		}
		if size != 8 && size != 16 && size != 24 && size != 28 {
			return fmt.Errorf("invalid ZIP64 extra size %d", size)
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
	_, ok := r.chunks[key]
	return ok
}

func (r *Reader) ManifestKeys() []store.ContentKey {
	return sortedKeys(r.manifests)
}

func (r *Reader) ChunkKeys() []store.ContentKey {
	return sortedKeys(r.chunks)
}

// Getter returns the Bundle-only object Getter. Misses never consult another
// source; use NewManifestFetcher to make the source decision at Manifest level.
func (r *Reader) Getter() cache.Getter { return &getter{reader: r} }

type getter struct{ reader *Reader }

func (g *getter) Get(ctx context.Context, partition store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	if err := ctx.Err(); err != nil {
		return cache.CacheMiss, nil, err
	}
	var entry *archiveEntry
	switch partition {
	case store.PartitionManifest:
		entry = g.reader.manifests[key]
	case store.PartitionChunk:
		entry = g.reader.chunks[key]
	default:
		return cache.CacheMiss, nil, nil
	}
	if entry == nil {
		return cache.CacheMiss, nil, nil
	}
	blob, err := g.reader.entryBlob(entry)
	if err != nil {
		return cache.CacheMiss, nil, err
	}
	return cache.CacheHit, blob, nil
}

// ValidateManifest implements fetch.ManifestValidator. It proves a local
// Manifest's non-zero Chunk closure using directory lookups only.
func (g *getter) ValidateManifest(key store.ContentKey, manifest *codec.Manifest) error {
	if !g.reader.HasManifest(key) {
		return fmt.Errorf("manifest bundle: Manifest %s is not local", hex.EncodeToString(key[:]))
	}
	for index, entry := range manifest.Entries {
		if entry.IsZero {
			continue
		}
		chunkKey := store.ContentKey(entry.CiphertextHash)
		if !g.reader.HasChunk(chunkKey) {
			return fmt.Errorf("%w: Manifest %s Chunk %d (%s) is absent", ErrIncomplete, hex.EncodeToString(key[:]), index, hex.EncodeToString(chunkKey[:]))
		}
	}
	return nil
}

func (r *Reader) entryBlob(entry *archiveEntry) (cache.Blob, error) {
	if err := r.retainBlob(); err != nil {
		return nil, err
	}
	releaseOnError := true
	defer func() {
		if releaseOnError {
			r.releaseBlob()
		}
	}()
	if entry.file.UncompressedSize64 > uint64(maxInt()) {
		return nil, fmt.Errorf("manifest bundle: ZIP entry %q is too large", entry.file.Name)
	}
	size := int(entry.file.UncompressedSize64)
	if entry.offset < 0 || uint64(entry.offset) > uint64(r.size) || entry.file.UncompressedSize64 > uint64(r.size-entry.offset) {
		return nil, fmt.Errorf("manifest bundle: ZIP entry %q data range is outside archive", entry.file.Name)
	}
	var data []byte
	var pooled []byte
	var err error
	if r.slicer != nil {
		data, err = r.slicer.Slice(entry.offset, size)
	} else {
		var reusable bool
		data, reusable = r.buffers.get(size)
		if reusable {
			pooled = data
		}
		var n int
		n, err = r.source.ReadAt(data, entry.offset)
		if err == io.EOF && len(data) == 0 {
			err = nil
		} else if err == nil && n != len(data) {
			err = io.ErrUnexpectedEOF
		}
	}
	if err != nil {
		if pooled != nil {
			r.buffers.put(pooled)
		}
		return nil, fmt.Errorf("manifest bundle: read ZIP entry %q: %w", entry.file.Name, err)
	}
	backing := &blobBacking{data: data[:len(data):len(data)], pooled: pooled, pool: &r.buffers}
	backing.refs.Store(1)
	releaseOnError = false
	return &readerBlob{reader: r, backing: backing}, nil
}

func (r *Reader) readMetadataEntry(entry *archiveEntry) ([]byte, error) {
	size := int(entry.file.UncompressedSize64)
	payload := make([]byte, size)
	if err := readFullAt(r.source, payload, entry.offset); err != nil {
		return nil, fmt.Errorf("manifest bundle: read ZIP entry %q: %w", entry.file.Name, err)
	}
	if crc32.ChecksumIEEE(payload) != entry.file.CRC32 {
		return nil, fmt.Errorf("manifest bundle: ZIP entry %q CRC mismatch", entry.file.Name)
	}
	return payload, nil
}

// validateLocalHeaderAt proves that the next Central Directory item is also
// the next physical local entry. Starting at offset zero and carrying the
// returned end offset across every entry rejects reordering, hidden entries,
// and arbitrary gaps without reading any object payload.
func (r *Reader) validateLocalHeaderAt(file *zip.File, headerOffset int64, scratch []byte) (int64, int64, error) {
	const fixedSize = 30
	headerSize := fixedSize + len(file.Name)
	if headerOffset < 0 || int64(headerSize) > r.size-headerOffset {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP entry %q local header is outside archive", file.Name)
	}
	if headerSize > len(scratch) {
		scratch = make([]byte, headerSize)
	}
	header := scratch[:headerSize]
	if err := readFullAt(r.source, header, headerOffset); err != nil {
		return 0, 0, fmt.Errorf("manifest bundle: read ZIP entry %q local header: %w", file.Name, err)
	}
	if !bytesEqual4(header[:4], zipLocalHeaderMagic) {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP entry %q does not match physical local-header order", file.Name)
	}
	if version := binary.LittleEndian.Uint16(header[4:6]); version != 45 {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP entry %q local reader version %d is not canonical", file.Name, version)
	}
	if flags := binary.LittleEndian.Uint16(header[6:8]); flags != 0 {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP entry %q local flags 0x%x are not allowed", file.Name, flags)
	}
	if method := binary.LittleEndian.Uint16(header[8:10]); method != zip.Store {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP entry %q local method %d, want Store", file.Name, method)
	}
	if binary.LittleEndian.Uint16(header[10:12]) != 0 || binary.LittleEndian.Uint16(header[12:14]) != 0 {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP entry %q has non-canonical local timestamp", file.Name)
	}
	if crc := binary.LittleEndian.Uint32(header[14:18]); crc != file.CRC32 {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP entry %q local CRC differs from Central Directory", file.Name)
	}
	if file.CompressedSize64 > math.MaxUint32 || file.UncompressedSize64 > math.MaxUint32 {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP entry %q requires a non-canonical local ZIP64 size", file.Name)
	}
	if size := binary.LittleEndian.Uint32(header[18:22]); uint64(size) != file.CompressedSize64 {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP entry %q local compressed size differs from Central Directory", file.Name)
	}
	if size := binary.LittleEndian.Uint32(header[22:26]); uint64(size) != file.UncompressedSize64 {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP entry %q local uncompressed size differs from Central Directory", file.Name)
	}
	if nameSize := int(binary.LittleEndian.Uint16(header[26:28])); nameSize != len(file.Name) {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP entry %q local name length differs from Central Directory", file.Name)
	}
	if extraSize := binary.LittleEndian.Uint16(header[28:30]); extraSize != 0 {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP entry %q local extra field is not allowed", file.Name)
	}
	if string(header[fixedSize:]) != file.Name {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP entry %q local name differs from Central Directory", file.Name)
	}
	dataOffset := headerOffset + int64(headerSize)
	if dataOffset < 0 || uint64(dataOffset) > uint64(r.size) || file.CompressedSize64 > uint64(r.size-dataOffset) {
		return 0, 0, fmt.Errorf("manifest bundle: ZIP entry %q data range is outside archive", file.Name)
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
