package bundle

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/ingest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type testFixture struct {
	data       []byte
	reader     *Reader
	admission  store.WriteAdmission
	customer   [32]byte
	decryptor  manifestcrypto.Decryptor
	root       store.ContentKey
	other      store.ContentKey
	rootPlain  []byte
	otherPlain []byte
}

func newTestFixture(t *testing.T, generation store.Generation) testFixture {
	t.Helper()
	salt, err := store.SaltForGeneration(generation)
	if err != nil {
		t.Fatal(err)
	}
	admission := store.WriteAdmission{Generation: generation, Salt: salt}
	var output bytes.Buffer
	w, err := NewWriter(&output, admission, WriterOptions{Concurrency: 4})
	if err != nil {
		t.Fatal(err)
	}
	chk, err := chunker.New(chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "4096"}})
	if err != nil {
		t.Fatal(err)
	}
	enc, dec, err := manifestcrypto.New(manifestcrypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	var customer [32]byte
	for index := range customer {
		customer[index] = byte(index + 1)
	}
	ing := ingest.NewIngester(func() ([32]byte, error) { return customer, nil }, nil, w, chk, enc)
	shared := bytes.Repeat([]byte{0x11}, 4096)
	otherPlain := append(append([]byte(nil), shared...), bytes.Repeat([]byte{0x22}, 4096)...)
	rootPlain := append(append([]byte(nil), shared...), bytes.Repeat([]byte{0x33}, 4096)...)
	other, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(otherPlain), uint64(len(otherPlain))), ingest.IngestOption{})
	if err != nil {
		t.Fatalf("Ingest(other): %v", err)
	}
	root, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(rootPlain), uint64(len(rootPlain))), ingest.IngestOption{})
	if err != nil {
		t.Fatalf("Ingest(root): %v", err)
	}
	if err := w.Finalize(root.ManifestKey); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	data := append([]byte(nil), output.Bytes()...)
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return testFixture{
		data:       data,
		reader:     r,
		admission:  admission,
		customer:   customer,
		decryptor:  dec,
		root:       root.ManifestKey,
		other:      other.ManifestKey,
		rootPlain:  rootPlain,
		otherPlain: otherPlain,
	}
}

func TestMultiManifestBundleSharedChunkAndFullVerify(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	if fixture.reader.Admission() != fixture.admission {
		t.Fatalf("admission = %#v, want %#v", fixture.reader.Admission(), fixture.admission)
	}
	if got := len(fixture.reader.ManifestKeys()); got != 2 {
		t.Fatalf("Manifest entries = %d, want 2", got)
	}
	if got := len(fixture.reader.ChunkKeys()); got != 3 {
		t.Fatalf("Chunk entries = %d, want 3 shared-object dedup", got)
	}
	if err := fixture.reader.FullVerify(context.Background(), fixture.root, fixture.customer, fixture.decryptor, VerifyOptions{Workers: 4}); err != nil {
		t.Fatalf("FullVerify: %v", err)
	}

	local := fetch.NewFetcher(fixture.customer, fixture.reader.Getter(), fixture.decryptor)
	bundleFetcher := NewManifestFetcher(fixture.reader, local, nil)
	stream, err := bundleFetcher.OpenManifest(context.Background(), fixture.root)
	if err != nil {
		t.Fatalf("OpenManifest(root): %v", err)
	}
	defer stream.Close()
	got := make([]byte, len(fixture.rootPlain))
	if n, err := stream.ReadAt(context.Background(), got, 0); err != nil || n != len(got) {
		t.Fatalf("ReadAt = %d, %v", n, err)
	}
	if !bytes.Equal(got, fixture.rootPlain) {
		t.Fatal("restored root bytes differ")
	}

	zr, err := zip.NewReader(bytes.NewReader(fixture.data), int64(len(fixture.data)))
	if err != nil {
		t.Fatal(err)
	}
	admissions := 0
	for _, file := range zr.File {
		if len(file.Name) >= len(admissionPrefix) && file.Name[:len(admissionPrefix)] == admissionPrefix {
			admissions++
		}
		if file.Method != zip.Store {
			t.Fatalf("entry %q method = %d", file.Name, file.Method)
		}
	}
	if admissions != 1 {
		t.Fatalf("admission entries = %d, want 1", admissions)
	}
}

func TestWriterReordersConcurrentChunksByLogicalOrdinal(t *testing.T) {
	salt, err := store.SaltForGeneration("G1")
	if err != nil {
		t.Fatal(err)
	}
	admission := store.WriteAdmission{Generation: "G1", Salt: salt}
	var output bytes.Buffer
	w, err := NewWriter(&output, admission, WriterOptions{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	key0 := store.ContentKey{1}
	key1 := store.ContentKey{2}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := w.PutChunkOrdered(context.Background(), admission, key1, []byte{2}, 1)
		done <- err
	}()
	<-started
	runtime.Gosched()
	select {
	case err := <-done:
		t.Fatalf("ordinal 1 completed before ordinal 0: %v", err)
	default:
	}
	if _, err := w.PutChunkOrdered(context.Background(), admission, key0, []byte{1}, 0); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	manifest := canonicalManifestEntry(t)
	manifestKey, err := parseLowerHexKey(manifest.name[len(manifestPrefix):])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put(context.Background(), admission, store.PartitionManifest, manifestKey, manifest.data); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(manifestKey); err != nil {
		t.Fatal(err)
	}
	zr, err := zip.NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	want0, _ := objectName(store.PartitionChunk, key0)
	want1, _ := objectName(store.PartitionChunk, key1)
	if zr.File[1].Name != want0 || zr.File[2].Name != want1 {
		t.Fatalf("Chunk order = %q, %q; want %q, %q", zr.File[1].Name, zr.File[2].Name, want0, want1)
	}
}

func TestWriterRejectsObjectsAndCopiesFromDifferentAdmission(t *testing.T) {
	if _, err := NewWriter(&bytes.Buffer{}, store.WriteAdmission{Generation: "G1"}, WriterOptions{}); err == nil {
		t.Fatal("non-canonical Writer admission was accepted")
	}
	source := newTestFixture(t, "G1")
	defer source.reader.Close()
	salt, err := store.SaltForGeneration("G2")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	w, err := NewWriter(&output, store.WriteAdmission{Generation: "G2", Salt: salt}, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put(context.Background(), source.admission, store.PartitionManifest, source.root, nil); err == nil {
		t.Fatal("object with a different admission was accepted")
	}
	if err := w.CopyManifestFromBundle(context.Background(), source.root, source.reader); err == nil {
		t.Fatal("exact object copy across admissions was accepted")
	}
}

func TestReaderBlobLifetimeDefersClose(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	result, blob, err := fixture.reader.Getter().Get(context.Background(), store.PartitionManifest, fixture.root)
	if err != nil || result != cache.CacheHit || blob == nil {
		t.Fatalf("Get = %v, %v, %v", result, blob, err)
	}
	clone := blob.Clone()
	if err := fixture.reader.Close(); err != nil {
		t.Fatal(err)
	}
	if len(blob.Bytes()) == 0 || len(clone.Bytes()) == 0 {
		t.Fatal("outstanding Blob lost bytes after Reader.Close")
	}
	if _, _, err := fixture.reader.Getter().Get(context.Background(), store.PartitionManifest, fixture.root); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close = %v, want ErrClosed", err)
	}
	blob.Release()
	clone.Release()
}

func TestNewReaderBlobIsImmutableFromCallerOwnedSource(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	data := append([]byte(nil), fixture.data...)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	result, blob, err := reader.Getter().Get(context.Background(), store.PartitionManifest, fixture.root)
	if err != nil || result != cache.CacheHit || blob == nil {
		t.Fatalf("Get = %v, %v, %v", result, blob, err)
	}
	defer blob.Release()
	want := append([]byte(nil), blob.Bytes()...)
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range zr.File {
		if file.Name != manifestPrefix+fmt.Sprintf("%x", fixture.root) {
			continue
		}
		offset, err := file.DataOffset()
		if err != nil {
			t.Fatal(err)
		}
		data[offset] ^= 0xff
		break
	}
	if !bytes.Equal(blob.Bytes(), want) {
		t.Fatal("caller mutation changed a live ReaderAt-backed Blob")
	}
}

func TestOpenMmapBlobLifetime(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	path := filepath.Join(t.TempDir(), "snapshot.bundle")
	if err := os.WriteFile(path, fixture.data, 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	result, blob, err := reader.Getter().Get(context.Background(), store.PartitionManifest, fixture.root)
	if err != nil || result != cache.CacheHit || blob == nil {
		t.Fatalf("Get = %v, %v, %v", result, blob, err)
	}
	want := objectBytes(t, fixture.reader, store.PartitionManifest, fixture.root)
	if !bytes.Equal(blob.Bytes(), want) {
		t.Fatal("mmap-backed Blob changed bytes")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(blob.Bytes(), want) {
		t.Fatal("mmap was released while Blob remained live")
	}
	blob.Release()
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

type routeGetter struct {
	mu      sync.Mutex
	objects map[store.Partition]map[store.ContentKey][]byte
	calls   map[store.Partition]int
}

func (g *routeGetter) Get(_ context.Context, partition store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.calls == nil {
		g.calls = make(map[store.Partition]int)
	}
	g.calls[partition]++
	data, ok := g.objects[partition][key]
	if !ok {
		return cache.CacheMiss, nil, nil
	}
	return cache.CacheHit, cache.NewMemBlob(append([]byte(nil), data...)), nil
}

func (g *routeGetter) callCount(partition store.Partition) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls[partition]
}

func objectBytes(t *testing.T, reader *Reader, partition store.Partition, key store.ContentKey) []byte {
	t.Helper()
	result, blob, err := reader.Getter().Get(context.Background(), partition, key)
	if err != nil || result != cache.CacheHit || blob == nil {
		t.Fatalf("Get(%s, %x) = %v, %v", partition, key, result, err)
	}
	defer blob.Release()
	return append([]byte(nil), blob.Bytes()...)
}

func manifestChunkKeys(t *testing.T, reader *Reader, key store.ContentKey) []store.ContentKey {
	t.Helper()
	physical := objectBytes(t, reader, store.PartitionManifest, key)
	manifest, _, err := codec.Unmarshal(physical)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]store.ContentKey, 0, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		if !entry.IsZero {
			keys = append(keys, store.ContentKey(entry.CiphertextHash))
		}
	}
	return keys
}

func TestLocalManifestMissingChunkNeverFallsBackRemote(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	manifestData := objectBytes(t, fixture.reader, store.PartitionManifest, fixture.root)

	var output bytes.Buffer
	w, err := NewWriter(&output, fixture.admission, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put(context.Background(), fixture.admission, store.PartitionManifest, fixture.root, manifestData); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(fixture.root); err != nil {
		t.Fatal(err)
	}
	localReader, err := NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	defer localReader.Close()
	remote := &routeGetter{objects: map[store.Partition]map[store.ContentKey][]byte{
		store.PartitionManifest: {fixture.root: manifestData},
		store.PartitionChunk:    {},
	}}
	for _, key := range fixture.reader.ChunkKeys() {
		remote.objects[store.PartitionChunk][key] = objectBytes(t, fixture.reader, store.PartitionChunk, key)
	}
	localFetcher := fetch.NewFetcher(fixture.customer, localReader.Getter(), fixture.decryptor)
	remoteFetcher := fetch.NewFetcher(fixture.customer, remote, fixture.decryptor)
	_, err = NewManifestFetcher(localReader, localFetcher, remoteFetcher).OpenManifest(context.Background(), fixture.root)
	if err == nil {
		t.Fatal("incomplete local Manifest fell back to remote")
	}
	if got := remote.callCount(store.PartitionManifest) + remote.callCount(store.PartitionChunk); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
}

func TestRemoteManifestNeverUsesBundleChunk(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	rootChunks := manifestChunkKeys(t, fixture.reader, fixture.root)
	chunkKey := rootChunks[0]
	chunkData := objectBytes(t, fixture.reader, store.PartitionChunk, chunkKey)
	emptyManifest, err := codec.Marshal(&codec.Manifest{Version: codec.Version1, ChunkMode: codec.ChunkModeFixed}, nil)
	if err != nil {
		t.Fatal(err)
	}
	emptyKey := store.ContentKey(sha256.Sum256(emptyManifest))
	var output bytes.Buffer
	w, err := NewWriter(&output, fixture.admission, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put(context.Background(), fixture.admission, store.PartitionChunk, chunkKey, chunkData); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put(context.Background(), fixture.admission, store.PartitionManifest, emptyKey, emptyManifest); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(emptyKey); err != nil {
		t.Fatal(err)
	}
	localReader, err := NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	defer localReader.Close()
	remote := &routeGetter{objects: map[store.Partition]map[store.ContentKey][]byte{
		store.PartitionManifest: {fixture.root: objectBytes(t, fixture.reader, store.PartitionManifest, fixture.root)},
		store.PartitionChunk:    {},
	}}
	for _, key := range rootChunks[1:] {
		remote.objects[store.PartitionChunk][key] = objectBytes(t, fixture.reader, store.PartitionChunk, key)
	}
	localFetcher := fetch.NewFetcher(fixture.customer, localReader.Getter(), fixture.decryptor)
	remoteFetcher := fetch.NewFetcher(fixture.customer, remote, fixture.decryptor)
	stream, err := NewManifestFetcher(localReader, localFetcher, remoteFetcher).OpenManifest(context.Background(), fixture.root)
	if err != nil {
		t.Fatalf("Open remote Manifest: %v", err)
	}
	defer stream.Close()
	if _, err := stream.ReadAt(context.Background(), make([]byte, len(fixture.rootPlain)), 0); err == nil {
		t.Fatal("remote Manifest read used a matching Bundle Chunk")
	}
	if got := remote.callCount(store.PartitionChunk); got == 0 {
		t.Fatal("remote Chunk Getter was not used")
	}
}

func TestCorruptLocalChunkNeverFallsBackRemote(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	rootChunks := manifestChunkKeys(t, fixture.reader, fixture.root)
	corruptKey := rootChunks[0]
	var output bytes.Buffer
	w, err := NewWriter(&output, fixture.admission, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range rootChunks {
		physical := objectBytes(t, fixture.reader, store.PartitionChunk, key)
		if key == corruptKey {
			physical[len(physical)-1] ^= 1
		}
		if _, err := w.Put(context.Background(), fixture.admission, store.PartitionChunk, key, physical); err != nil {
			t.Fatal(err)
		}
	}
	manifestData := objectBytes(t, fixture.reader, store.PartitionManifest, fixture.root)
	if _, err := w.Put(context.Background(), fixture.admission, store.PartitionManifest, fixture.root, manifestData); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(fixture.root); err != nil {
		t.Fatal(err)
	}
	localReader, err := NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	defer localReader.Close()
	remote := &routeGetter{objects: map[store.Partition]map[store.ContentKey][]byte{
		store.PartitionManifest: {fixture.root: manifestData},
		store.PartitionChunk:    {},
	}}
	for _, key := range rootChunks {
		remote.objects[store.PartitionChunk][key] = objectBytes(t, fixture.reader, store.PartitionChunk, key)
	}
	localFetcher := fetch.NewFetcher(fixture.customer, localReader.Getter(), fixture.decryptor)
	remoteFetcher := fetch.NewFetcher(fixture.customer, remote, fixture.decryptor)
	stream, err := NewManifestFetcher(localReader, localFetcher, remoteFetcher).OpenManifest(context.Background(), fixture.root)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if _, err := stream.ReadAt(context.Background(), make([]byte, len(fixture.rootPlain)), 0); err == nil {
		t.Fatal("corrupt local Chunk fell back to remote")
	}
	if got := remote.callCount(store.PartitionManifest) + remote.callCount(store.PartitionChunk); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
}

func TestCorruptLocalManifestNeverFallsBackRemote(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	validManifest := objectBytes(t, fixture.reader, store.PartitionManifest, fixture.root)
	tamperedManifest := append([]byte(nil), validManifest...)
	tamperedManifest[len(tamperedManifest)-1] ^= 1
	var output bytes.Buffer
	w, err := NewWriter(&output, fixture.admission, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range fixture.reader.ChunkKeys() {
		if _, err := w.Put(context.Background(), fixture.admission, store.PartitionChunk, key, objectBytes(t, fixture.reader, store.PartitionChunk, key)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Put(context.Background(), fixture.admission, store.PartitionManifest, fixture.root, tamperedManifest); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(fixture.root); err != nil {
		t.Fatal(err)
	}
	localReader, err := NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	defer localReader.Close()
	remote := &routeGetter{objects: map[store.Partition]map[store.ContentKey][]byte{
		store.PartitionManifest: {fixture.root: validManifest},
		store.PartitionChunk:    {},
	}}
	for _, key := range fixture.reader.ChunkKeys() {
		remote.objects[store.PartitionChunk][key] = objectBytes(t, fixture.reader, store.PartitionChunk, key)
	}
	localFetcher := fetch.NewFetcher(fixture.customer, localReader.Getter(), fixture.decryptor)
	remoteFetcher := fetch.NewFetcher(fixture.customer, remote, fixture.decryptor)
	if _, err := NewManifestFetcher(localReader, localFetcher, remoteFetcher).OpenManifest(context.Background(), fixture.root); err == nil {
		t.Fatal("corrupt local Manifest fell back to remote")
	}
	if got := remote.callCount(store.PartitionManifest) + remote.callCount(store.PartitionChunk); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
}

type passthroughDecryptor struct{}

func (passthroughDecryptor) DecryptChunkTo(_ context.Context, _ [32]byte, physical, dst []byte) error {
	copy(dst, physical)
	return nil
}

func (passthroughDecryptor) UnsealKeyTable(_ [32]byte, _, _ []byte) ([]byte, error) {
	return make([]byte, 32), nil
}

func TestBundleFetcherVerifyContentOptionControlsPhysicalSHA(t *testing.T) {
	salt, err := store.SaltForGeneration("G1")
	if err != nil {
		t.Fatal(err)
	}
	admission := store.WriteAdmission{Generation: "G1", Salt: salt}
	authentic := bytes.Repeat([]byte{0xa1}, 4096)
	tampered := bytes.Repeat([]byte{0xb2}, 4096)
	chunkKey := store.ContentKey(sha256.Sum256(authentic))
	manifestData, err := codec.Marshal(&codec.Manifest{
		Version:      codec.Version1,
		ChunkMode:    codec.ChunkModeFixed,
		ImageSize:    4096,
		MinChunkSize: 4096,
		MaxChunkSize: 4096,
		Entries: []codec.ChunkEntry{{
			Offset:         0,
			Size:           4096,
			CiphertextHash: chunkKey,
		}},
	}, []byte("ignored sealed table"))
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := store.ContentKey(sha256.Sum256(manifestData))
	var output bytes.Buffer
	w, err := NewWriter(&output, admission, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put(context.Background(), admission, store.PartitionChunk, chunkKey, tampered); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put(context.Background(), admission, store.PartitionManifest, manifestKey, manifestData); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(manifestKey); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	strict := fetch.NewFetcher([32]byte{}, reader.Getter(), passthroughDecryptor{})
	strictStream, err := strict.OpenManifest(context.Background(), manifestKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := strictStream.ReadAt(context.Background(), make([]byte, 4096), 0); err == nil {
		t.Fatal("strict Bundle Fetcher accepted mismatched Chunk bytes")
	}
	_ = strictStream.Close()

	unchecked := fetch.NewFetcherWithOptions([32]byte{}, reader.Getter(), passthroughDecryptor{}, fetch.Options{VerifyContent: false})
	uncheckedStream, err := unchecked.OpenManifest(context.Background(), manifestKey)
	if err != nil {
		t.Fatal(err)
	}
	defer uncheckedStream.Close()
	got := make([]byte, 4096)
	if _, err := uncheckedStream.ReadAt(context.Background(), got, 0); err != nil {
		t.Fatalf("verify_content=false read: %v", err)
	}
	if !bytes.Equal(got, tampered) {
		t.Fatal("verify_content=false changed Chunk bytes")
	}
}

type rawEntry struct {
	name   string
	method uint16
	flags  uint16
	data   []byte
}

func rawZIP(t *testing.T, entries []rawEntry, comment string) []byte {
	t.Helper()
	var output bytes.Buffer
	w := zip.NewWriter(&output)
	for _, raw := range entries {
		method := raw.method
		if method == 0 {
			method = zip.Store
		}
		header := &zip.FileHeader{
			Name:               raw.name,
			Method:             method,
			Flags:              raw.flags,
			CreatorVersion:     45,
			ReaderVersion:      45,
			CRC32:              crc32.ChecksumIEEE(raw.data),
			CompressedSize64:   uint64(len(raw.data)),
			UncompressedSize64: uint64(len(raw.data)),
		}
		entry, err := w.CreateRaw(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(raw.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.SetComment(comment); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func canonicalAdmissionEntry(t *testing.T) rawEntry {
	t.Helper()
	salt, err := store.SaltForGeneration("G1")
	if err != nil {
		t.Fatal(err)
	}
	return rawEntry{name: admissionName(store.WriteAdmission{Generation: "G1", Salt: salt})}
}

func canonicalManifestEntry(t *testing.T) rawEntry {
	t.Helper()
	data, err := codec.Marshal(&codec.Manifest{Version: codec.Version1, ChunkMode: codec.ChunkModeFixed}, nil)
	if err != nil {
		t.Fatal(err)
	}
	key := sha256.Sum256(data)
	return rawEntry{name: manifestPrefix + fmt.Sprintf("%x", key), data: data}
}

func TestReaderRejectsMalformedProfileAndTruncation(t *testing.T) {
	admission := canonicalAdmissionEntry(t)
	manifest := canonicalManifestEntry(t)
	nonCanonicalAdmission := admission
	nonCanonicalAdmission.name = admissionPrefix + "G1/" + string(bytes.Repeat([]byte{'0'}, 64))
	for _, tc := range []struct {
		name    string
		entries []rawEntry
		comment string
	}{
		{name: "missing admission", entries: []rawEntry{manifest}},
		{name: "non-canonical admission salt", entries: []rawEntry{nonCanonicalAdmission, manifest}},
		{name: "duplicate admission", entries: []rawEntry{admission, admission, manifest}},
		{name: "duplicate Manifest", entries: []rawEntry{admission, manifest, manifest}},
		{name: "unknown entry", entries: []rawEntry{admission, manifest, {name: "meta/root"}}},
		{name: "Deflate", entries: []rawEntry{admission, {name: manifest.name, method: zip.Deflate, data: manifest.data}}},
		{name: "encrypted flag", entries: []rawEntry{admission, {name: manifest.name, flags: 1, data: manifest.data}}},
		{name: "archive comment", entries: []rawEntry{admission, manifest}, comment: "not allowed"},
		{name: "uppercase key", entries: []rawEntry{admission, {name: "manifest/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", data: manifest.data}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := rawZIP(t, tc.entries, tc.comment)
			if _, err := NewReader(bytes.NewReader(data), int64(len(data))); err == nil {
				t.Fatal("invalid Bundle accepted")
			}
		})
	}
	valid := rawZIP(t, []rawEntry{admission, manifest}, "")
	if _, err := NewReader(bytes.NewReader(append(append([]byte(nil), valid...), 0)), int64(len(valid)+1)); err == nil {
		t.Fatal("Bundle with trailing bytes accepted")
	}
	for _, cut := range []int{1, 8, 32} {
		t.Run(fmt.Sprintf("truncated-%d", cut), func(t *testing.T) {
			data := valid[:len(valid)-cut]
			if _, err := NewReader(bytes.NewReader(data), int64(len(data))); err == nil {
				t.Fatal("truncated Bundle accepted")
			}
		})
	}
}

func TestReaderRejectsMismatchedLocalHeaderBeforePayload(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	data := append([]byte(nil), fixture.data...)
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	var target *zip.File
	for _, file := range zr.File {
		if file.Name == manifestPrefix+fmt.Sprintf("%x", fixture.root) {
			target = file
			break
		}
	}
	if target == nil {
		t.Fatal("root Manifest entry not found")
	}
	dataOffset, err := target.DataOffset()
	if err != nil {
		t.Fatal(err)
	}
	headerOffset := dataOffset - int64(30+len(target.Name))
	data[headerOffset+8] = byte(zip.Deflate) // Central Directory still says Store.

	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader should remain metadata-only: %v", err)
	}
	defer reader.Close()
	if _, _, err := reader.Getter().Get(context.Background(), store.PartitionManifest, fixture.root); err == nil {
		t.Fatal("mismatched local header was accepted")
	}
}

func TestZIP64EntryCount(t *testing.T) {
	if testing.Short() {
		t.Skip("ZIP64 entry-count profile test")
	}
	salt, err := store.SaltForGeneration("G1")
	if err != nil {
		t.Fatal(err)
	}
	admission := store.WriteAdmission{Generation: "G1", Salt: salt}
	var output bytes.Buffer
	w, err := NewWriter(&output, admission, WriterOptions{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	const chunks = 65_533 // + admission + Manifest = ZIP64's 65,535 threshold.
	for index := 0; index < chunks; index++ {
		var key store.ContentKey
		binary.LittleEndian.PutUint64(key[:8], uint64(index+1))
		if _, err := w.Put(context.Background(), admission, store.PartitionChunk, key, []byte{1}); err != nil {
			t.Fatalf("Put Chunk %d: %v", index, err)
		}
	}
	manifest := canonicalManifestEntry(t)
	manifestKey, err := parseLowerHexKey(manifest.name[len(manifestPrefix):])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put(context.Background(), admission, store.PartitionManifest, manifestKey, manifest.data); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(manifestKey); err != nil {
		t.Fatal(err)
	}
	data := output.Bytes()
	if !bytes.Contains(data, []byte{'P', 'K', 0x06, 0x06}) {
		t.Fatal("ZIP64 end-of-central-directory record is absent")
	}
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader ZIP64: %v", err)
	}
	defer reader.Close()
	if got := len(reader.ChunkKeys()); got != chunks {
		t.Fatalf("ZIP64 Chunk entries = %d, want %d", got, chunks)
	}
}

func FuzzReader(f *testing.F) {
	f.Add([]byte("PK\x03\x04"))
	salt, err := store.SaltForGeneration("FUZZ")
	if err != nil {
		f.Fatal(err)
	}
	var seed bytes.Buffer
	w, err := NewWriter(&seed, store.WriteAdmission{Generation: "FUZZ", Salt: salt}, WriterOptions{Concurrency: 1})
	if err != nil {
		f.Fatal(err)
	}
	manifest, err := codec.Marshal(&codec.Manifest{Version: codec.Version1, ChunkMode: codec.ChunkModeFixed}, nil)
	if err != nil {
		f.Fatal(err)
	}
	manifestKey := store.ContentKey(sha256.Sum256(manifest))
	if _, err := w.Put(context.Background(), w.Admission(), store.PartitionManifest, manifestKey, manifest); err != nil {
		f.Fatal(err)
	}
	if err := w.Finalize(manifestKey); err != nil {
		f.Fatal(err)
	}
	f.Add(seed.Bytes())
	f.Fuzz(func(t *testing.T, data []byte) {
		reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
		if err == nil {
			manifests := reader.ManifestKeys()
			if len(manifests) != 0 {
				_, blob, _ := reader.Getter().Get(context.Background(), store.PartitionManifest, manifests[0])
				if blob != nil {
					blob.Release()
				}
			}
			chunks := reader.ChunkKeys()
			if len(chunks) != 0 {
				_, blob, _ := reader.Getter().Get(context.Background(), store.PartitionChunk, chunks[0])
				if blob != nil {
					blob.Release()
				}
			}
			_ = reader.Close()
		}
	})
}
