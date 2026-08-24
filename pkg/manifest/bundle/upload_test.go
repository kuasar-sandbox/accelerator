package bundle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type recordedPut struct {
	admission store.WriteAdmission
	partition store.Partition
	key       store.ContentKey
	data      []byte
}

type recordingExactStore struct {
	mu sync.Mutex

	accepted       store.WriteAdmission
	admitErr       error
	admitCalls     int
	requested      []store.Generation
	puts           []recordedPut
	pool           int
	removeAfterPut int
}

func (s *recordingExactStore) PoolSize() int { return s.pool }

func (s *recordingExactStore) AdmitWriteFor(_ context.Context, generation store.Generation) (store.WriteAdmission, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.admitCalls++
	s.requested = append(s.requested, generation)
	if s.admitErr != nil {
		return store.WriteAdmission{}, s.admitErr
	}
	return s.accepted, nil
}

func (s *recordingExactStore) Put(_ context.Context, admission store.WriteAdmission, partition store.Partition, key store.ContentKey, data []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.removeAfterPut > 0 && len(s.puts) >= s.removeAfterPut {
		return false, errors.New("generation removed")
	}
	s.puts = append(s.puts, recordedPut{
		admission: admission,
		partition: partition,
		key:       key,
		data:      append([]byte(nil), data...),
	})
	return true, nil
}

func (s *recordingExactStore) snapshot() (int, []store.Generation, []recordedPut) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.admitCalls, append([]store.Generation(nil), s.requested...), append([]recordedPut(nil), s.puts...)
}

func TestExactUploadUsesRecordedAdmissionAndPublishesRootLast(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	target := &recordingExactStore{accepted: fixture.admission, pool: 4}
	if err := fixture.reader.Upload(context.Background(), fixture.root, fixture.customer, fixture.decryptor, target, VerifyOptions{}); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	admitCalls, requested, puts := target.snapshot()
	if admitCalls != 1 || len(requested) != 1 || requested[0] != fixture.admission.Generation {
		t.Fatalf("admission calls = %d %v", admitCalls, requested)
	}
	if len(puts) != 5 {
		t.Fatalf("Put calls = %d, want 3 Chunks + 2 Manifests", len(puts))
	}
	seenManifest := false
	for index, put := range puts {
		if put.admission != fixture.admission {
			t.Fatalf("Put[%d] admission changed", index)
		}
		if put.partition == store.PartitionManifest {
			seenManifest = true
		} else if seenManifest {
			t.Fatalf("Chunk Put[%d] occurred after a Manifest", index)
		}
		want := objectBytes(t, fixture.reader, put.partition, put.key)
		if !bytes.Equal(put.data, want) {
			t.Fatalf("Put[%d] rewrote physical object", index)
		}
	}
	last := puts[len(puts)-1]
	if last.partition != store.PartitionManifest || last.key != fixture.root {
		t.Fatalf("last Put = %s/%x, want root Manifest %x", last.partition, last.key, fixture.root)
	}
}

func TestExactUploadRejectsRemovedGenerationAndSaltMismatchBeforePut(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	for _, tc := range []struct {
		name   string
		target *recordingExactStore
	}{
		{name: "removed", target: &recordingExactStore{admitErr: errors.New("not current")}},
		{name: "salt mismatch", target: &recordingExactStore{accepted: store.WriteAdmission{Generation: "G1", Salt: [32]byte{0xff}}}},
		{name: "generation mismatch", target: &recordingExactStore{accepted: store.WriteAdmission{Generation: "G2", Salt: fixture.admission.Salt}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := fixture.reader.Upload(context.Background(), fixture.root, fixture.customer, fixture.decryptor, tc.target, VerifyOptions{}); err == nil {
				t.Fatal("invalid target admission accepted")
			}
			_, _, puts := tc.target.snapshot()
			if len(puts) != 0 {
				t.Fatalf("Put calls = %d before admission precheck", len(puts))
			}
		})
	}
}

func TestExactUploadGenerationRemovalDoesNotPublishRoot(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	target := &recordingExactStore{accepted: fixture.admission, pool: 1, removeAfterPut: 1}
	if err := fixture.reader.Upload(context.Background(), fixture.root, fixture.customer, fixture.decryptor, target, VerifyOptions{Workers: 1}); err == nil {
		t.Fatal("mid-upload generation removal was ignored")
	}
	_, _, puts := target.snapshot()
	for _, put := range puts {
		if put.partition == store.PartitionManifest && put.key == fixture.root {
			t.Fatal("root Manifest was published after generation removal")
		}
	}
}

func TestFullVerifyRejectsObjectsOutsideRecordedSaltDomain(t *testing.T) {
	source := newTestFixture(t, "G2")
	defer source.reader.Close()
	g1Salt, err := store.SaltForGeneration("G1")
	if err != nil {
		t.Fatal(err)
	}
	entries := []rawEntry{{name: admissionName(store.WriteAdmission{Generation: "G1", Salt: g1Salt})}}
	for _, key := range source.reader.ManifestKeys() {
		entries = append(entries, rawEntry{name: manifestPrefix + keyString(key), data: objectBytes(t, source.reader, store.PartitionManifest, key)})
	}
	for _, key := range source.reader.ChunkKeys() {
		entries = append(entries, rawEntry{name: chunkPrefix + keyString(key), data: objectBytes(t, source.reader, store.PartitionChunk, key)})
	}
	data := rawZIP(t, entries, "")
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	err = reader.FullVerify(context.Background(), source.root, source.customer, source.decryptor, VerifyOptions{Workers: 2})
	if err == nil || !strings.Contains(err.Error(), "outside recorded admission salt domain") {
		t.Fatalf("FullVerify error = %v", err)
	}
}

func TestFullVerifyRejectsInconsistentSharedChunkMetadata(t *testing.T) {
	salt, err := store.SaltForGeneration("G1")
	if err != nil {
		t.Fatal(err)
	}
	admission := store.WriteAdmission{Generation: "G1", Salt: salt}
	enc, dec, err := manifestcrypto.New(manifestcrypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	plain := bytes.Repeat([]byte{0x51}, 4096)
	physical, hash, key, err := enc.EncryptChunk(context.Background(), admission.Salt, plain)
	if err != nil {
		t.Fatal(err)
	}
	var customer [32]byte
	customer[0] = 0x99
	manifest := &codec.Manifest{
		Version:      codec.Version1,
		ChunkMode:    codec.ChunkModeFixed,
		ImageSize:    uint64(len(plain)),
		MinChunkSize: uint32(len(plain)),
		MaxChunkSize: uint32(len(plain)),
		Entries: []codec.ChunkEntry{{
			Offset:         0,
			Size:           uint32(len(plain)),
			CiphertextHash: hash,
		}},
	}
	manifestA := marshalWithKey(t, enc, manifest, customer, key)
	wrongKey := key
	wrongKey[0] ^= 0xff
	manifestB := marshalWithKey(t, enc, manifest, customer, wrongKey)
	keyA := store.ContentKey(sha256.Sum256(manifestA))
	keyB := store.ContentKey(sha256.Sum256(manifestB))
	var output bytes.Buffer
	w, err := NewWriter(&output, admission, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put(context.Background(), admission, store.PartitionChunk, store.ContentKey(hash), physical); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put(context.Background(), admission, store.PartitionManifest, keyA, manifestA); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put(context.Background(), admission, store.PartitionManifest, keyB, manifestB); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(keyA); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := reader.FullVerify(context.Background(), keyA, customer, dec, VerifyOptions{}); err == nil || !strings.Contains(err.Error(), "inconsistent key or plaintext size") {
		t.Fatalf("FullVerify error = %v", err)
	}
}

func TestFullVerifyRejectsUnreferencedAndCorruptChunks(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	enc, _, err := manifestcrypto.New(manifestcrypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	extra, extraHash, _, err := enc.EncryptChunk(context.Background(), fixture.admission.Salt, bytes.Repeat([]byte{0x71}, 4096))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	w, err := NewWriter(&output, fixture.admission, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.CopyManifestFromBundle(context.Background(), fixture.other, fixture.reader); err != nil {
		t.Fatal(err)
	}
	if err := w.CopyManifestFromBundle(context.Background(), fixture.root, fixture.reader); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put(context.Background(), fixture.admission, store.PartitionChunk, store.ContentKey(extraHash), extra); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(fixture.root); err != nil {
		t.Fatal(err)
	}
	unreferenced, err := NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if err := unreferenced.FullVerify(context.Background(), fixture.root, fixture.customer, fixture.decryptor, VerifyOptions{}); err == nil || !strings.Contains(err.Error(), "unreferenced Chunk") {
		t.Fatalf("unreferenced FullVerify error = %v", err)
	}
	_ = unreferenced.Close()

	entries := []rawEntry{{name: admissionName(fixture.admission)}}
	for _, key := range fixture.reader.ManifestKeys() {
		entries = append(entries, rawEntry{name: manifestPrefix + keyString(key), data: objectBytes(t, fixture.reader, store.PartitionManifest, key)})
	}
	corrupted := false
	for _, key := range fixture.reader.ChunkKeys() {
		physical := objectBytes(t, fixture.reader, store.PartitionChunk, key)
		if !corrupted {
			physical[len(physical)-1] ^= 1
			corrupted = true
		}
		entries = append(entries, rawEntry{name: chunkPrefix + keyString(key), data: physical})
	}
	data := rawZIP(t, entries, "")
	corruptReader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	defer corruptReader.Close()
	if err := corruptReader.FullVerify(context.Background(), fixture.root, fixture.customer, fixture.decryptor, VerifyOptions{}); err == nil || !strings.Contains(err.Error(), "physical ContentKey mismatch") {
		t.Fatalf("corrupt FullVerify error = %v", err)
	}
}

func TestFullVerifyRejectsUnexpectedManifestSet(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	err := fixture.reader.FullVerify(context.Background(), fixture.root, fixture.customer, fixture.decryptor, VerifyOptions{
		ExpectedManifests: []store.ContentKey{fixture.root},
	})
	if err == nil || !strings.Contains(err.Error(), "Manifest set") {
		t.Fatalf("FullVerify error = %v", err)
	}
	if err := fixture.reader.FullVerify(context.Background(), fixture.root, fixture.customer, fixture.decryptor, VerifyOptions{
		ExpectedManifests: []store.ContentKey{fixture.root, fixture.other},
	}); err != nil {
		t.Fatalf("FullVerify expected set: %v", err)
	}
}

func marshalWithKey(t *testing.T, encryptor manifestcrypto.Encryptor, manifest *codec.Manifest, customer, key [32]byte) []byte {
	t.Helper()
	sealed, err := encryptor.SealKeyTable(customer, key[:], codec.BuildAAD(manifest))
	if err != nil {
		t.Fatal(err)
	}
	data, err := codec.Marshal(manifest, sealed)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func keyString(key store.ContentKey) string { return hex.EncodeToString(key[:]) }
