package bundle

import (
	"archive/zip"
	"context"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"runtime"
	"sync"

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

	zip       *zip.Writer
	admission store.WriteAdmission
	objects   map[string]writtenObject
	manifests map[store.ContentKey]struct{}

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
		if err := w.writeEntryLocked(refsName, refsPayload); err != nil {
			_ = w.zip.Close()
			return nil, err
		}
	}
	if err := w.writeEntryLocked(admissionName(admission), nil); err != nil {
		_ = w.zip.Close()
		return nil, err
	}
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
	if previous, ok := w.objects[name]; ok {
		if previous.size != uint64(len(data)) {
			return false, fmt.Errorf("manifest bundle: repeated object %s changed size from %d to %d", name, previous.size, len(data))
		}
		return false, nil
	}
	if err := w.writeEntryLocked(name, data); err != nil {
		w.writeErr = err
		return false, err
	}
	w.objects[name] = writtenObject{size: uint64(len(data))}
	if partition == store.PartitionManifest {
		w.manifests[key] = struct{}{}
	}
	return true, nil
}

func (w *Writer) writeEntryLocked(name string, data []byte) error {
	if w.entryCount >= maxBundleEntries {
		return fmt.Errorf("manifest bundle: entry count exceeds limit %d", maxBundleEntries)
	}
	size := uint64(len(data))
	header := &zip.FileHeader{
		Name:               name,
		Method:             zip.Store,
		CreatorVersion:     45,
		ReaderVersion:      45,
		CRC32:              crc32.ChecksumIEEE(data),
		CompressedSize64:   size,
		UncompressedSize64: size,
	}
	entry, err := w.zip.CreateRaw(header)
	if err != nil {
		return fmt.Errorf("manifest bundle: create ZIP entry %q: %w", name, err)
	}
	if len(data) == 0 {
		w.entryCount++
		return nil
	}
	n, err := entry.Write(data)
	if err != nil {
		return fmt.Errorf("manifest bundle: write ZIP entry %q: %w", name, err)
	}
	if n != len(data) {
		return fmt.Errorf("manifest bundle: write ZIP entry %q: %w", name, io.ErrShortWrite)
	}
	w.entryCount++
	return nil
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

// Finalize verifies root was emitted and closes the ZIP Central Directory.
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
	w.closed = true
	w.closeErr = w.zip.Close()
	if w.closeErr != nil {
		w.closeErr = fmt.Errorf("manifest bundle: close ZIP: %w", w.closeErr)
	}
	w.signalLocked()
	return w.closeErr
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
	w.closed = true
	if w.writeErr != nil {
		w.closeErr = w.writeErr
	} else if err := w.zip.Close(); err != nil {
		w.closeErr = fmt.Errorf("manifest bundle: close ZIP: %w", err)
	}
	w.signalLocked()
	return w.closeErr
}
