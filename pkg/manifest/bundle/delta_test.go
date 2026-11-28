package bundle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/delta"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// deltaBundleFixture is a store-like object set with two images: v0, one
// independent chunk; v1, a Version2 manifest whose single entry is a delta of
// v0's chunk. The base is therefore outside v1's entries.
type deltaBundleFixture struct {
	admission store.WriteAdmission
	customer  [32]byte
	dec       manifestcrypto.Decryptor
	enc       manifestcrypto.Encryptor

	baseObj, deltaObj         []byte
	baseKey, deltaKey         store.ContentKey
	v0, v1                    []byte // manifest blobs
	v0Key, v1Key              store.ContentKey
	target                    []byte
	kBase, kDelta, wrongKBase [32]byte
}

func newDeltaBundleFixture(t *testing.T) *deltaBundleFixture {
	return newDeltaBundleFixtureWithChunkSalt(t, nil)
}

// newDeltaBundleFixtureWithChunkSalt encrypts the chunks under chunkSalt
// instead of the admission salt when it is non-nil, as a writer with an extra
// salt does.
func newDeltaBundleFixtureWithChunkSalt(t *testing.T, chunkSalt *[32]byte) *deltaBundleFixture {
	t.Helper()
	admissionSalt, err := store.SaltForGeneration("G1")
	if err != nil {
		t.Fatal(err)
	}
	salt := admissionSalt
	if chunkSalt != nil {
		salt = *chunkSalt
	}
	f := &deltaBundleFixture{admission: store.WriteAdmission{Generation: "G1", Salt: admissionSalt}}
	f.customer[0] = 0x42
	f.enc, f.dec, err = manifestcrypto.New(manifestcrypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	base := bytes.Repeat([]byte("bundle-base-block-"), 2000)
	f.target = append([]byte(nil), base...)
	f.target[77] ^= 0x11
	f.target[20000] ^= 0x22

	var baseHash [32]byte
	f.baseObj, baseHash, f.kBase, err = f.enc.EncryptChunk(ctx, salt, base)
	if err != nil {
		t.Fatal(err)
	}
	f.baseKey = store.ContentKey(baseHash)
	stream := delta.Encode(base, f.target)
	var deltaHash [32]byte
	f.deltaObj, deltaHash, f.kDelta, err = f.enc.(manifestcrypto.DeltaEncryptor).EncryptDeltaChunk(ctx, salt, manifestcrypto.DeltaBase{
		ObjectKey: baseHash, Key: f.kBase, PlaintextSize: len(base),
	}, f.target, stream)
	if err != nil {
		t.Fatal(err)
	}
	f.deltaKey = store.ContentKey(deltaHash)

	m0 := &codec.Manifest{Version: codec.Version1, ChunkMode: codec.ChunkModeFixed, ImageSize: uint64(len(base)),
		MinChunkSize: uint32(len(base)), MaxChunkSize: uint32(len(base)),
		Entries: []codec.ChunkEntry{{Size: uint32(len(base)), CiphertextHash: baseHash}}}
	f.v0 = marshalWithKey(t, f.enc, m0, f.customer, f.kBase)
	f.v0Key = store.ContentKey(sha256.Sum256(f.v0))

	m1 := &codec.Manifest{Version: codec.Version2, ChunkMode: codec.ChunkModeFixed, ImageSize: uint64(len(f.target)),
		MinChunkSize: uint32(len(f.target)), MaxChunkSize: uint32(len(f.target)),
		Entries: []codec.ChunkEntry{{Size: uint32(len(f.target)), CiphertextHash: deltaHash, IsDelta: true}},
		Deltas:  []codec.DeltaRef{{Entry: 0, BaseSize: uint32(len(base)), Base: baseHash}}}
	f.v1 = marshalWithKey(t, f.enc, m1, f.customer, f.kDelta)
	f.v1Key = store.ContentKey(sha256.Sum256(f.v1))
	return f
}

// bundle writes the given objects into a Bundle rooted at root.
func (f *deltaBundleFixture) bundle(t *testing.T, root store.ContentKey, chunks map[store.ContentKey][]byte, manifests map[store.ContentKey][]byte) *Reader {
	t.Helper()
	var out bytes.Buffer
	w, err := NewWriter(&out, f.admission, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for k, v := range chunks {
		if _, err := w.Put(ctx, f.admission, store.PartitionChunk, k, v); err != nil {
			t.Fatal(err)
		}
	}
	for k, v := range manifests {
		if _, err := w.Put(ctx, f.admission, store.PartitionManifest, k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finalize(root); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(bytes.NewReader(out.Bytes()), int64(out.Len()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func (f *deltaBundleFixture) store() *routeGetter {
	return &routeGetter{objects: map[store.Partition]map[store.ContentKey][]byte{
		store.PartitionChunk:    {f.baseKey: f.baseObj, f.deltaKey: f.deltaObj},
		store.PartitionManifest: {f.v0Key: f.v0, f.v1Key: f.v1},
	}}
}

// Checks that a Bundle holding only the root Manifest v1, its delta, and the base
// passes FullVerify. The base is present only through v1's delta table. Checks that
// Upload sends the base and the delta to the target and publishes the root last,
// and that VerifyExactManifests accepts the same closure.
func TestBundleFullVerifyAcceptsDeltaWithExternalBase(t *testing.T) {
	f := newDeltaBundleFixture(t)
	// Only v1 is in the Bundle; its base is present because the delta table
	// makes it part of v1's closure, not because any entry addresses it.
	r := f.bundle(t, f.v1Key, map[store.ContentKey][]byte{f.baseKey: f.baseObj, f.deltaKey: f.deltaObj},
		map[store.ContentKey][]byte{f.v1Key: f.v1})
	if err := r.FullVerify(context.Background(), f.v1Key, f.customer, f.dec, VerifyOptions{Workers: 2}); err != nil {
		t.Fatalf("FullVerify: %v", err)
	}
	// Upload publishes the base too (it must reach the target before any
	// reader can be handed v1).
	target := newAliasExactStore(f.admission)
	if err := r.Upload(context.Background(), f.v1Key, f.customer, f.dec, target, VerifyOptions{Workers: 2}); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	_, _, puts := target.snapshot()
	var sawBase, sawDelta bool
	for i, p := range puts {
		switch {
		case p.partition == store.PartitionChunk && p.key == f.baseKey:
			sawBase = true
		case p.partition == store.PartitionChunk && p.key == f.deltaKey:
			sawDelta = true
		case p.partition == store.PartitionManifest && p.key == f.v1Key:
			if i != len(puts)-1 {
				t.Fatal("root manifest was not published last")
			}
		}
	}
	if !sawBase || !sawDelta {
		t.Fatalf("upload missed objects: base=%v delta=%v", sawBase, sawDelta)
	}

	// Verifying the selected closure across Bundles works the same way.
	if err := VerifyExactManifests(context.Background(), ExactManifest{Reader: r, Key: f.v1Key}, nil, f.customer, f.dec, VerifyOptions{Workers: 2}); err != nil {
		t.Fatalf("VerifyExactManifests: %v", err)
	}
}

// Checks that a delta whose base is absent from the Bundle fails FullVerify with
// ErrIncomplete, and that the error names the base.
func TestBundleDeltaBaseMissingIsIncomplete(t *testing.T) {
	f := newDeltaBundleFixture(t)
	r := f.bundle(t, f.v1Key, map[store.ContentKey][]byte{f.deltaKey: f.deltaObj}, map[store.ContentKey][]byte{f.v1Key: f.v1})
	err := r.FullVerify(context.Background(), f.v1Key, f.customer, f.dec, VerifyOptions{})
	if !errors.Is(err, ErrIncomplete) || !strings.Contains(err.Error(), "base") {
		t.Fatalf("FullVerify without the base = %v, want ErrIncomplete naming the base", err)
	}
}

// Checks that each of three kinds of tampering fails verification: a manifest whose
// delta table names a different base, a base that is missing or corrupt, and a
// delta object whose Version 1 entry is not flagged as a delta. Each case is a
// subtest.
func TestBundleDeltaVerificationCatchesTampering(t *testing.T) {
	f := newDeltaBundleFixture(t)
	ctx := context.Background()

	t.Run("manifest names another base", func(t *testing.T) {
		// A Version2 manifest whose table names a different (present) object as
		// the base. The key table is sealed over the table, so re-seal; the delta
		// object itself still names the real base and must be caught.
		other, otherHash, _, err := f.enc.EncryptChunk(ctx, f.admission.Salt, bytes.Repeat([]byte{0x5c}, len(f.target)))
		if err != nil {
			t.Fatal(err)
		}
		m, _, err := codec.Unmarshal(f.v1)
		if err != nil {
			t.Fatal(err)
		}
		m.Deltas[0].Base = otherHash
		bad := marshalWithKey(t, f.enc, m, f.customer, f.kDelta)
		badKey := store.ContentKey(sha256.Sum256(bad))
		r := f.bundle(t, badKey, map[store.ContentKey][]byte{f.deltaKey: f.deltaObj, store.ContentKey(otherHash): other},
			map[store.ContentKey][]byte{badKey: bad})
		err = r.FullVerify(ctx, badKey, f.customer, f.dec, VerifyOptions{})
		if err == nil || !strings.Contains(err.Error(), "names base") {
			t.Fatalf("FullVerify = %v, want base disagreement", err)
		}
	})

	t.Run("base object corrupted", func(t *testing.T) {
		corrupt := append([]byte(nil), f.baseObj...)
		corrupt[len(corrupt)-1] ^= 1
		corruptKey := store.ContentKey(sha256.Sum256(corrupt))
		// The Bundle holds the corrupt bytes under their own (correct) key, so
		// the base the delta names is simply absent; and under the original key
		// the hash check fails. Either way verification must not pass.
		r := f.bundle(t, f.v1Key, map[store.ContentKey][]byte{f.deltaKey: f.deltaObj, corruptKey: corrupt},
			map[store.ContentKey][]byte{f.v1Key: f.v1})
		if err := r.FullVerify(ctx, f.v1Key, f.customer, f.dec, VerifyOptions{}); err == nil {
			t.Fatal("FullVerify passed with the base missing/corrupt")
		}
	})

	t.Run("entry not flagged for a delta object", func(t *testing.T) {
		m, _, err := codec.Unmarshal(f.v1)
		if err != nil {
			t.Fatal(err)
		}
		m.Version = codec.Version1
		m.Entries[0].IsDelta = false
		m.Deltas = nil
		v1asV1 := marshalWithKey(t, f.enc, m, f.customer, f.kDelta)
		k := store.ContentKey(sha256.Sum256(v1asV1))
		r := f.bundle(t, k, map[store.ContentKey][]byte{f.deltaKey: f.deltaObj, f.baseKey: f.baseObj, store.ContentKey(sha256.Sum256(f.baseObj)): f.baseObj},
			map[store.ContentKey][]byte{k: v1asV1, f.v0Key: f.v0})
		err = r.FullVerify(ctx, k, f.customer, f.dec, VerifyOptions{})
		if err == nil || !strings.Contains(err.Error(), "not flagged delta") {
			t.Fatalf("FullVerify = %v, want unflagged delta object rejected", err)
		}
	})
}

// Checks that CopyManifest copies a Version 2 Manifest's delta base even though no
// entry of that Manifest addresses it, and that the copied Bundle verifies. Also
// checks that a Bundle holding both v0 and v1 verifies with the base shared.
func TestBundleCopyManifestIncludesDeltaBases(t *testing.T) {
	f := newDeltaBundleFixture(t)
	var out bytes.Buffer
	w, err := NewWriter(&out, f.admission, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Copy only v1 from a store that also holds v0: the base must come along
	// although no entry of v1 addresses it.
	if err := w.CopyManifest(context.Background(), f.v1Key, f.store()); err != nil {
		t.Fatalf("CopyManifest: %v", err)
	}
	if err := w.Finalize(f.v1Key); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(bytes.NewReader(out.Bytes()), int64(out.Len()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if !r.HasChunk(f.baseKey) || !r.HasChunk(f.deltaKey) {
		t.Fatalf("copied Bundle lacks objects: base=%v delta=%v", r.HasChunk(f.baseKey), r.HasChunk(f.deltaKey))
	}
	if err := r.FullVerify(context.Background(), f.v1Key, f.customer, f.dec, VerifyOptions{}); err != nil {
		t.Fatalf("FullVerify of the copy: %v", err)
	}

	// Shared base: a Bundle holding both v0 (which addresses the base as a
	// chunk) and v1 (which needs it as a base) verifies, with the base counted
	// once and decrypted under the key v0's table holds.
	both := f.bundle(t, f.v1Key, map[store.ContentKey][]byte{f.baseKey: f.baseObj, f.deltaKey: f.deltaObj},
		map[store.ContentKey][]byte{f.v0Key: f.v0, f.v1Key: f.v1})
	if err := both.FullVerify(context.Background(), f.v1Key, f.customer, f.dec, VerifyOptions{}); err != nil {
		t.Fatalf("FullVerify with shared base: %v", err)
	}
}

// aliasExactStore is a recordingExactStore that also records aliases the way
// the Store server does: the first object named for a chunk wins, a later
// different object loses, and a losing object the same call created is deleted.
type aliasExactStore struct {
	*recordingExactStore

	amu         sync.Mutex
	aliases     map[store.ContentKey]store.ContentKey
	aliasAt     map[store.ContentKey]int // number of Puts recorded when the alias was attempted
	unsupported bool
}

func newAliasExactStore(admission store.WriteAdmission) *aliasExactStore {
	return &aliasExactStore{
		recordingExactStore: &recordingExactStore{accepted: admission, pool: 2},
		aliases:             map[store.ContentKey]store.ContentKey{},
		aliasAt:             map[store.ContentKey]int{},
	}
}

func (s *aliasExactStore) PutRepresenting(_ context.Context, _ store.WriteAdmission, key store.ContentKey, _ []byte, represents store.ContentKey) (store.PutResult, error) {
	_, _, puts := s.snapshot()
	exists := false
	for _, p := range puts {
		if p.partition == store.PartitionChunk && p.key == key {
			exists = true
		}
	}
	s.amu.Lock()
	defer s.amu.Unlock()
	s.aliasAt[key] = len(puts)
	if s.unsupported {
		return store.PutResult{}, nil
	}
	current, ok := s.aliases[represents]
	if !ok || current == key {
		s.aliases[represents] = key
		return store.PutResult{IsNew: !exists, Represented: store.RepresentedWon}, nil
	}
	return store.PutResult{IsNew: !exists, Represented: store.RepresentedLost, Key: current, Discarded: !exists}, nil
}

func (f *deltaBundleFixture) independentAddress(t *testing.T) store.ContentKey {
	t.Helper()
	_, hInd, _, err := f.enc.EncryptChunk(context.Background(), f.admission.Salt, f.target)
	if err != nil {
		t.Fatal(err)
	}
	return store.ContentKey(hInd)
}

func manifestPuts(puts []recordedPut) int {
	n := 0
	for _, p := range puts {
		if p.partition == store.PartitionManifest {
			n++
		}
	}
	return n
}

// Checks that uploading a Bundle with a delta object records the alias that
// names it under the chunk's independent address, after the object itself is
// stored and before any Manifest is published, so store-ctl delta-gc does not
// treat it as an orphan. Covers both single-Bundle and multi-Bundle upload.
func TestBundleUploadRecordsDeltaAliases(t *testing.T) {
	f := newDeltaBundleFixture(t)
	hInd := f.independentAddress(t)
	chunks := map[store.ContentKey][]byte{f.baseKey: f.baseObj, f.deltaKey: f.deltaObj}
	manifests := map[store.ContentKey][]byte{f.v1Key: f.v1}

	check := func(t *testing.T, target *aliasExactStore) {
		t.Helper()
		if got, ok := target.aliases[hInd]; !ok || got != f.deltaKey {
			t.Fatalf("alias for the chunk = %x (present %v), want the delta object %x", got[:4], ok, f.deltaKey[:4])
		}
		if len(target.aliases) != 1 {
			t.Fatalf("%d aliases recorded, want 1 (only delta objects get one)", len(target.aliases))
		}
		_, _, puts := target.snapshot()
		at := target.aliasAt[f.deltaKey]
		storedBefore := false
		for i, p := range puts {
			if p.partition == store.PartitionChunk && p.key == f.deltaKey && i < at {
				storedBefore = true
			}
			if p.partition == store.PartitionManifest && i < at {
				t.Fatal("a Manifest was published before the delta object's alias")
			}
		}
		if !storedBefore {
			t.Fatal("the alias was requested before the delta object was stored")
		}
	}

	t.Run("Upload", func(t *testing.T) {
		r := f.bundle(t, f.v1Key, chunks, manifests)
		target := newAliasExactStore(f.admission)
		if err := r.Upload(context.Background(), f.v1Key, f.customer, f.dec, target, VerifyOptions{Workers: 2}); err != nil {
			t.Fatalf("Upload: %v", err)
		}
		check(t, target)
	})
	t.Run("UploadExactManifests", func(t *testing.T) {
		r := f.bundle(t, f.v1Key, chunks, manifests)
		target := newAliasExactStore(f.admission)
		if err := UploadExactManifests(context.Background(), ExactManifest{Key: f.v1Key, Reader: r}, nil, f.customer, f.dec, target, VerifyOptions{Workers: 2}); err != nil {
			t.Fatalf("UploadExactManifests: %v", err)
		}
		check(t, target)
	})
	t.Run("re-upload finds its own alias", func(t *testing.T) {
		r := f.bundle(t, f.v1Key, chunks, manifests)
		target := newAliasExactStore(f.admission)
		target.aliases[hInd] = f.deltaKey
		if err := r.Upload(context.Background(), f.v1Key, f.customer, f.dec, target, VerifyOptions{}); err != nil {
			t.Fatalf("Upload over an alias that already names the same object: %v", err)
		}
	})
}

// Checks each case where the alias cannot be recorded. The upload must fail
// with ErrDeltaAlias and must not publish any Manifest, so the uploaded delta
// object stays unreferenced and delta-gc may remove it. An object the target
// already holds is never deleted by the alias attempt.
func TestBundleUploadRefusesDeltaWithoutAlias(t *testing.T) {
	ctx := context.Background()

	t.Run("target cannot record aliases", func(t *testing.T) {
		f := newDeltaBundleFixture(t)
		r := f.bundle(t, f.v1Key, map[store.ContentKey][]byte{f.baseKey: f.baseObj, f.deltaKey: f.deltaObj}, map[store.ContentKey][]byte{f.v1Key: f.v1})
		target := &recordingExactStore{accepted: f.admission, pool: 2}
		err := r.Upload(ctx, f.v1Key, f.customer, f.dec, target, VerifyOptions{})
		if !errors.Is(err, ErrDeltaAlias) {
			t.Fatalf("Upload = %v, want ErrDeltaAlias", err)
		}
		if _, _, puts := target.snapshot(); len(puts) != 0 {
			t.Fatalf("%d objects uploaded; the capability check must run before any Put", len(puts))
		}
		err = UploadExactManifests(ctx, ExactManifest{Key: f.v1Key, Reader: r}, nil, f.customer, f.dec, target, VerifyOptions{})
		if !errors.Is(err, ErrDeltaAlias) {
			t.Fatalf("UploadExactManifests = %v, want ErrDeltaAlias", err)
		}
	})

	t.Run("store does not record the alias", func(t *testing.T) {
		f := newDeltaBundleFixture(t)
		r := f.bundle(t, f.v1Key, map[store.ContentKey][]byte{f.baseKey: f.baseObj, f.deltaKey: f.deltaObj}, map[store.ContentKey][]byte{f.v1Key: f.v1})
		target := newAliasExactStore(f.admission)
		target.unsupported = true
		err := r.Upload(ctx, f.v1Key, f.customer, f.dec, target, VerifyOptions{})
		if !errors.Is(err, ErrDeltaAlias) {
			t.Fatalf("Upload = %v, want ErrDeltaAlias", err)
		}
		if _, _, puts := target.snapshot(); manifestPuts(puts) != 0 {
			t.Fatal("a Manifest was published although its delta object has no alias")
		}
	})

	t.Run("target represents the chunk by another delta object", func(t *testing.T) {
		f := newDeltaBundleFixture(t)
		r := f.bundle(t, f.v1Key, map[store.ContentKey][]byte{f.baseKey: f.baseObj, f.deltaKey: f.deltaObj}, map[store.ContentKey][]byte{f.v1Key: f.v1})
		target := newAliasExactStore(f.admission)
		other := store.ContentKey{0x0f}
		target.aliases[f.independentAddress(t)] = other
		err := r.Upload(ctx, f.v1Key, f.customer, f.dec, target, VerifyOptions{})
		if !errors.Is(err, ErrDeltaAlias) || !strings.Contains(err.Error(), "another delta object") {
			t.Fatalf("Upload = %v, want ErrDeltaAlias naming the conflict", err)
		}
		if _, _, puts := target.snapshot(); manifestPuts(puts) != 0 {
			t.Fatal("a Manifest was published although the alias names another object")
		}
		if target.aliases[f.independentAddress(t)] != other {
			t.Fatal("the existing alias was changed")
		}
	})

	t.Run("manifest written under an extra salt", func(t *testing.T) {
		extra := [32]byte{0xe5}
		f := newDeltaBundleFixtureWithChunkSalt(t, &extra)
		r := f.bundle(t, f.v1Key, map[store.ContentKey][]byte{f.baseKey: f.baseObj, f.deltaKey: f.deltaObj}, map[store.ContentKey][]byte{f.v1Key: f.v1})
		if err := r.FullVerify(ctx, f.v1Key, f.customer, f.dec, VerifyOptions{}); err != nil {
			t.Fatalf("setup: the Bundle must still verify: %v", err)
		}
		target := newAliasExactStore(f.admission)
		err := r.Upload(ctx, f.v1Key, f.customer, f.dec, target, VerifyOptions{})
		if !errors.Is(err, ErrDeltaAlias) || !strings.Contains(err.Error(), "extra salt") {
			t.Fatalf("Upload = %v, want ErrDeltaAlias for an extra salt", err)
		}
		_, _, puts := target.snapshot()
		for _, p := range puts {
			if p.key == f.deltaKey || p.partition == store.PartitionManifest {
				t.Fatalf("uploaded %s %x; the delta object and Manifests must not be uploaded", p.partition, p.key[:4])
			}
		}
		if len(target.aliases) != 0 {
			t.Fatal("an alias was recorded under a guessed independent address")
		}
	})
}
