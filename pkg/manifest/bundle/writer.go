package bundle

import (
	"archive/zip"
	"context"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"runtime"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// WriterOptions controls bounded Chunk encode/write parallelism. The ZIP
// writer itself is always serialized.
type WriterOptions struct {
	Concurrency int
	Refs        []string
}

// Writer is a multi-Manifest ingest.StoreWriter backed by one ZIP stream. It
// records one fixed WriteAdmission at construction and deduplicates shared
// objects by ContentKey.
type Writer struct {
	mu sync.Mutex

	zip             *zip.Writer
	admission       store.WriteAdmission
	objects         map[string]writtenObject
	manifests       map[store.ContentKey]struct{}
	manifestRecords []indexRecord
	chunkRecords    []indexRecord
	nextOffset      uint64
	metadataEnd     uint64

	concurrency int
	entryCount  int
	nextOrdinal uint64
	turn        chan struct{}
	closed      bool
	closeErr    error
	writeErr    error
}

type writtenObject struct {
	size uint64
}

type writtenEntry struct {
	headerOffset uint64
	dataOffset   uint64
	size         uint32
	crc32        uint32
}

// NewWriter fully validates the immutable refs list, then writes refs (when
// non-empty) followed by the sole admission entry. admission is required to
// use the canonical public generation salt.
func NewWriter(dst io.Writer, admission store.WriteAdmission, opts WriterOptions) (*Writer, error) {
	if dst == nil {
		return nil, fmt.Errorf("manifest bundle: destination is required")
	}
	refsPayload, err := EncodeRefs(opts.Refs)
	if err != nil {
		return nil, err
	}
	wantSalt, err := store.SaltForGeneration(admission.Generation)
	if err != nil {
		return nil, fmt.Errorf("manifest bundle: admission: %w", err)
	}
	if admission.Salt != wantSalt {
		return nil, fmt.Errorf("manifest bundle: admission salt is not canonical for generation %q", admission.Generation)
	}
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = runtime.GOMAXPROCS(0)
	}
	if concurrency < 1 {
		concurrency = 1
	}
	w := &Writer{
		zip:         zip.NewWriter(dst),
		admission:   admission,
		objects:     make(map[string]writtenObject),
		manifests:   make(map[store.ContentKey]struct{}),
		concurrency: concurrency,
		turn:        make(chan struct{}),
	}
	if len(refsPayload) != 0 {
		if _, err := w.writeEntryLocked(refsName, refsPayload); err != nil {
			_ = w.zip.Close()
			return nil, err
		}
	}
	if _, err := w.writeEntryLocked(admissionName(admission), nil); err != nil {
		_ = w.zip.Close()
		return nil, err
	}
	w.metadataEnd = w.nextOffset
	return w, nil
}

// Admission returns the one admission recorded and reused by this Bundle.
func (w *Writer) Admission() store.WriteAdmission { return w.admission }

// PoolSize lets ingest bound concurrent encoding and the reorder queue.
func (w *Writer) PoolSize() int { return w.concurrency }

// AdmitWrite returns the already-resolved Bundle admission. It performs no RPC
// or derivation, so any number of Ingest calls share one acquisition.
func (w *Writer) AdmitWrite(ctx context.Context) (store.WriteAdmission, error) {
	if err := ctx.Err(); err != nil {
		return store.WriteAdmission{}, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.stateErrorLocked(); err != nil {
		return store.WriteAdmission{}, err
	}
	return w.admission, nil
}

// Put writes one exact physical object. Shared ContentKeys are emitted once.
func (w *Writer) Put(ctx context.Context, admission store.WriteAdmission, partition store.Partition, key store.ContentKey, data []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if partition == store.PartitionChunk && w.nextOrdinal != 0 {
		return false, fmt.Errorf("manifest bundle: unordered Chunk Put during ordered ingest")
	}
	isNew, err := w.putLocked(admission, partition, key, data)
	if err == nil && partition == store.PartitionManifest {
		w.nextOrdinal = 0
		w.signalLocked()
	}
	return isNew, err
}

// PutChunkOrdered serializes concurrently encoded Chunks by logical ordinal.
// Waiting writers hold at most the bounded ingest worker/queue population.
func (w *Writer) PutChunkOrdered(ctx context.Context, admission store.WriteAdmission, key store.ContentKey, data []byte, ordinal uint64) (bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		w.mu.Lock()
		if err := w.stateErrorLocked(); err != nil {
			w.mu.Unlock()
			return false, err
		}
		if ordinal < w.nextOrdinal {
			w.mu.Unlock()
			return false, fmt.Errorf("manifest bundle: Chunk ordinal %d already passed %d", ordinal, w.nextOrdinal)
		}
		if ordinal == w.nextOrdinal {
			isNew, err := w.putLocked(admission, store.PartitionChunk, key, data)
			if err == nil {
				w.nextOrdinal++
			}
			w.signalLocked()
			w.mu.Unlock()
			return isNew, err
		}
		turn := w.turn
		w.mu.Unlock()
		select {
		case <-turn:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}

func (w *Writer) putLocked(admission store.WriteAdmission, partition store.Partition, key store.ContentKey, data []byte) (bool, error) {
	if err := w.stateErrorLocked(); err != nil {
		return false, err
	}
	if admission != w.admission {
		return false, fmt.Errorf("manifest bundle: object admission does not match recorded admission")
	}
	name, err := objectName(partition, key)
	if err != nil {
		return false, err
	}
	switch partition {
	case store.PartitionManifest:
		if len(data) < 5 || uint64(len(data)) > uint64(codec.MaxManifestDecodedSize)+1 {
			return false, fmt.Errorf("manifest bundle: Manifest %s physical size %d is invalid", hex.EncodeToString(key[:]), len(data))
		}
	case store.PartitionChunk:
		if len(data) == 0 || uint64(len(data)) > uint64(codec.MaxChunkDecodedSize)+1 {
			return false, fmt.Errorf("manifest bundle: Chunk %s physical size %d is invalid", hex.EncodeToString(key[:]), len(data))
		}
	}
	if previous, ok := w.objects[name]; ok {
		if previous.size != uint64(len(data)) {
			return false, fmt.Errorf("manifest bundle: repeated object %s changed size from %d to %d", name, previous.size, len(data))
		}
		return false, nil
	}
	written, err := w.writeEntryLocked(name, data)
	if err != nil {
		w.writeErr = err
		return false, err
	}
	w.objects[name] = writtenObject{size: uint64(len(data))}
	record := indexRecord{Key: key, DataOffset: written.dataOffset, Size: written.size, CRC32: written.crc32}
	if partition == store.PartitionManifest {
		w.manifests[key] = struct{}{}
		w.manifestRecords = append(w.manifestRecords, record)
	} else {
		w.chunkRecords = append(w.chunkRecords, record)
	}
	return true, nil
}

func (w *Writer) writeEntryLocked(name string, data []byte) (writtenEntry, error) {
	var written writtenEntry
	entryLimit := maxBundleEntries
	if name != indexName {
		entryLimit-- // every successfully written Bundle must still fit its index
	}
	if w.entryCount >= entryLimit {
		return written, fmt.Errorf("manifest bundle: entry count exceeds limit %d", maxBundleEntries)
	}
	if len(name) > math.MaxUint16 {
		return written, fmt.Errorf("manifest bundle: ZIP entry name is too long")
	}
	if uint64(len(data)) > math.MaxUint32 {
		return written, fmt.Errorf("manifest bundle: ZIP entry %q exceeds 32-bit object size", name)
	}
	headerSize := uint64(zipLocalHeaderFixedSize + len(name))
	dataOffset, err := checkedAdd64(w.nextOffset, headerSize)
	if err != nil {
		return written, fmt.Errorf("manifest bundle: ZIP entry %q Local Header range overflows", name)
	}
	nextOffset, err := checkedAdd64(dataOffset, uint64(len(data)))
	if err != nil {
		return written, fmt.Errorf("manifest bundle: ZIP entry %q data range overflows", name)
	}
	size := uint64(len(data))
	checksum := crc32.ChecksumIEEE(data)
	header := &zip.FileHeader{
		Name:               name,
		Method:             zip.Store,
		CreatorVersion:     45,
		ReaderVersion:      45,
		CRC32:              checksum,
		CompressedSize64:   size,
		UncompressedSize64: size,
	}
	entry, err := w.zip.CreateRaw(header)
	if err != nil {
		return written, fmt.Errorf("manifest bundle: create ZIP entry %q: %w", name, err)
	}
	if len(data) == 0 {
		written = writtenEntry{headerOffset: w.nextOffset, dataOffset: dataOffset, size: uint32(size), crc32: checksum}
		w.nextOffset = nextOffset
		w.entryCount++
		return written, nil
	}
	n, err := entry.Write(data)
	if err != nil {
		return written, fmt.Errorf("manifest bundle: write ZIP entry %q: %w", name, err)
	}
	if n != len(data) {
		return written, fmt.Errorf("manifest bundle: write ZIP entry %q: %w", name, io.ErrShortWrite)
	}
	written = writtenEntry{headerOffset: w.nextOffset, dataOffset: dataOffset, size: uint32(size), crc32: checksum}
	w.nextOffset = nextOffset
	w.entryCount++
	return written, nil
}

func (w *Writer) signalLocked() {
	close(w.turn)
	w.turn = make(chan struct{})
}

func (w *Writer) stateErrorLocked() error {
	if w.writeErr != nil {
		return w.writeErr
	}
	if w.closed {
		if w.closeErr != nil {
			return w.closeErr
		}
		return ErrClosed
	}
	return nil
}

// Finalize verifies root was emitted, writes the mandatory final index Local
// Entry, and closes the ZIP Central Directory.
func (w *Writer) Finalize(root store.ContentKey) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return w.closeErr
	}
	if w.writeErr != nil {
		w.closed = true
		w.closeErr = w.writeErr
		w.signalLocked()
		return w.closeErr
	}
	if _, ok := w.manifests[root]; !ok {
		return fmt.Errorf("manifest bundle: root Manifest %s was not written", hex.EncodeToString(root[:]))
	}
	return w.finalizeLocked()
}

// Close closes a Bundle when the caller has no external root selector. Normal
// snapshot creation should use Finalize(root).
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return w.closeErr
	}
	if len(w.manifests) == 0 {
		return fmt.Errorf("manifest bundle: at least one Manifest is required")
	}
	if w.writeErr != nil {
		w.closed = true
		w.closeErr = w.writeErr
		w.signalLocked()
		return w.closeErr
	}
	return w.finalizeLocked()
}

func (w *Writer) finalizeLocked() error {
	payload, _, err := buildIndexPayload(w.metadataEnd, w.nextOffset, w.manifestRecords, w.chunkRecords)
	if err == nil {
		var written writtenEntry
		written, err = w.writeEntryLocked(indexName, payload)
		if err == nil && written.headerOffset+uint64(indexLocalHeaderSize) != written.dataOffset {
			err = fmt.Errorf("manifest bundle: index Local Header offset accounting mismatch")
		}
	}
	w.closed = true
	if err != nil {
		w.writeErr = err
		w.closeErr = err
		_ = w.zip.Close()
	} else if closeErr := w.zip.Close(); closeErr != nil {
		w.closeErr = fmt.Errorf("manifest bundle: close ZIP: %w", closeErr)
	}
	w.signalLocked()
	return w.closeErr
}
