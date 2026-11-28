package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/delta"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

const deltaChunk = 64 << 10

// memStore is a StoreWriter that actually keeps what it is given, so tests can
// read the image back through the manifest the way a reader would.
type memStore struct {
	mu        sync.Mutex
	salt      [32]byte
	chunks    map[store.ContentKey][]byte
	manifests map[store.ContentKey][]byte
	chunkPuts int // all chunk Put calls
	newBytes  int // bytes of chunk objects newly stored
	workers   int
	failDelta bool // fail any Put of a delta object

	// Aliases: one write-once record per chunk, exactly as the real stores keep.
	aliases     map[store.ContentKey]store.ContentKey
	resolves    int                                                     // Resolve calls
	noAliases   bool                                                    // behave like a store without alias support
	resolveHook func(h store.ContentKey) (*store.Resolved, error)       // overrides Resolve when it returns non-nil/err
	repHook     func(key, represents store.ContentKey) *store.PutResult // overrides the alias outcome of PutRepresenting
	keepLosers  bool                                                    // behave like a server that predates discarding losing objects
}

func newMemStore() *memStore {
	return &memStore{
		chunks:    map[store.ContentKey][]byte{},
		manifests: map[store.ContentKey][]byte{},
		aliases:   map[store.ContentKey]store.ContentKey{},
	}
}

// Resolve implements Resolver the way the server does: the independent object
// wins over an alias.
func (s *memStore) Resolve(_ context.Context, _ store.Generation, h store.ContentKey) (store.Resolved, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resolves++
	if s.noAliases {
		return store.Resolved{}, store.ErrUnsupported
	}
	if s.resolveHook != nil {
		if r, err := s.resolveHook(h); r != nil || err != nil {
			if r == nil {
				return store.Resolved{}, err
			}
			return *r, err
		}
	}
	if _, ok := s.chunks[h]; ok {
		return store.Resolved{Resolution: store.ResolutionIndependent}, nil
	}
	if d, ok := s.aliases[h]; ok {
		return store.Resolved{Resolution: store.ResolutionDelta, DeltaKey: d}, nil
	}
	return store.Resolved{}, nil
}

// PutRepresenting stores the object like Put and then tries to create the
// write-once alias, reporting what the server would: alias first, and a losing
// object that this call created is deleted again (Discarded).
func (s *memStore) PutRepresenting(ctx context.Context, a store.WriteAdmission, key store.ContentKey, data []byte, represents store.ContentKey) (store.PutResult, error) {
	isNew, err := s.Put(ctx, a, store.PartitionChunk, key, data)
	if err != nil {
		return store.PutResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := store.PutResult{IsNew: isNew}
	if s.noAliases {
		return out, nil
	}
	if s.repHook != nil {
		if r := s.repHook(key, represents); r != nil {
			r.IsNew = isNew
			if r.Represented == store.RepresentedLost && isNew && !s.keepLosers {
				delete(s.chunks, key)
				s.newBytes -= len(data)
				r.Discarded = true
			}
			return *r, nil
		}
	}
	if cur, ok := s.aliases[represents]; ok && cur != key {
		out.Represented, out.Key = store.RepresentedLost, cur
		if isNew && !s.keepLosers {
			delete(s.chunks, key)
			s.newBytes -= len(data)
			out.Discarded = true
		}
		return out, nil
	}
	s.aliases[represents] = key
	out.Represented = store.RepresentedWon
	return out, nil
}

func (s *memStore) AdmitWrite(context.Context) (store.WriteAdmission, error) {
	return store.WriteAdmission{Generation: "G1", Salt: s.salt}, nil
}

func (s *memStore) PoolSize() int { return max(1, s.workers) }

func (s *memStore) Put(_ context.Context, _ store.WriteAdmission, p store.Partition, key store.ContentKey, data []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p == store.PartitionManifest {
		_, had := s.manifests[key]
		s.manifests[key] = append([]byte(nil), data...)
		return !had, nil
	}
	s.chunkPuts++
	if s.failDelta && len(data) > 0 && data[0] == crypto.ChunkFormatAESDelta {
		return false, errors.New("injected store failure")
	}
	if sha256.Sum256(data) != key {
		return false, fmt.Errorf("content key does not match object hash")
	}
	if _, had := s.chunks[key]; had {
		return false, nil
	}
	s.chunks[key] = append([]byte(nil), data...)
	s.newBytes += len(data)
	return true, nil
}

func (s *memStore) Get(_ context.Context, p store.Partition, key store.ContentKey) (bool, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var src map[store.ContentKey][]byte
	if p == store.PartitionManifest {
		src = s.manifests
	} else {
		src = s.chunks
	}
	b, ok := src[key]
	if !ok {
		return false, nil, nil
	}
	return true, append([]byte(nil), b...), nil
}

// unreadableStore is a store whose objects cannot be fetched back, so a base
// that has left the in-memory pool is gone for delta purposes.
type unreadableStore struct{ *memStore }

func (unreadableStore) Get(context.Context, store.Partition, store.ContentKey) (bool, []byte, error) {
	return false, nil, nil
}

func (s *memStore) snapshotCounts() (puts, objects int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chunkPuts, len(s.chunks)
}

func (s *memStore) countFormat(f byte) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, o := range s.chunks {
		if o[0] == f {
			n++
		}
	}
	return n
}

// orderedMemStore is a memStore that also implements OrderedChunkStoreWriter,
// the way the bundle writers do.
type orderedMemStore struct {
	*memStore
	supportsDelta *bool // nil = does not implement DeltaWriter's answer
	ordered       atomic.Int32
}

func (o *orderedMemStore) PutChunkOrdered(ctx context.Context, a store.WriteAdmission, key store.ContentKey, data []byte, _ uint64) (bool, error) {
	o.ordered.Add(1)
	return o.memStore.Put(ctx, a, store.PartitionChunk, key, data)
}

type optedInStore struct{ *orderedMemStore }

func (optedInStore) SupportsDelta() bool { return true }

type optedOutStore struct{ *orderedMemStore }

func (optedOutStore) SupportsDelta() bool { return false }

func deltaEngine(t *testing.T, mut func(*delta.Config)) *delta.Engine {
	t.Helper()
	c := delta.Config{Enabled: true}
	c.BasePool.MaxBytes = "256MiB"
	c.Index.KnownCapacity = 1 << 16
	c.Index.MaxRepresentations = 1 << 12
	if mut != nil {
		mut(&c)
	}
	p, err := c.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	e, err := delta.NewEngine(p, func() ([32]byte, error) { return [32]byte{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func rnd(seed int64, n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func edit(b []byte, seed int64, edits, editLen int) []byte {
	out := append([]byte(nil), b...)
	r := rand.New(rand.NewSource(seed))
	for i := 0; i < edits; i++ {
		at := r.Intn(len(out) - editLen)
		r.Read(out[at : at+editLen])
	}
	return out
}

// image builds n 64 KiB chunks; one chunk is all zeros to exercise the zero path.
func image(seed int64, n int) []byte {
	var img []byte
	for i := 0; i < n; i++ {
		if i == 3 {
			img = append(img, make([]byte, deltaChunk)...)
			continue
		}
		img = append(img, rnd(seed*1000+int64(i), deltaChunk)...)
	}
	return img
}

// editImage returns a "next version": every chunk gets a few small edits.
func editImage(img []byte, seed int64) []byte {
	out := append([]byte(nil), img...)
	for off := 0; off < len(out); off += deltaChunk {
		if bytes.Equal(out[off:off+deltaChunk], make([]byte, deltaChunk)) {
			continue
		}
		copy(out[off:off+deltaChunk], edit(out[off:off+deltaChunk], seed+int64(off), 3, 64))
	}
	return out
}

func ingestImage(t *testing.T, ing Ingester, img []byte) *Result {
	t.Helper()
	res, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(img), uint64(len(img))), IngestOption{})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return res
}

func newTestIngester(t *testing.T, sw StoreWriter, eng *delta.Engine) Ingester {
	t.Helper()
	return NewIngesterWithDelta(testKeyFn, nil, sw, fixedChunker(t, deltaChunk), testEncryptor(), eng)
}

// readBack reconstructs an image exactly as a reader would, using only the
// manifest and the store: unseal the key table, then for each chunk verify the
// object hash, and decode it, following a delta object to its base. It also
// asserts the structural rules readers rely on (base exists, base is
// independent, sizes agree).
func readBack(t *testing.T, st *memStore, manifestKey store.ContentKey) []byte {
	t.Helper()
	blob, ok := st.manifests[manifestKey]
	if !ok {
		t.Fatal("manifest not stored")
	}
	m, sealed, err := codec.Unmarshal(blob)
	if err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	enc, dec, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	_ = enc
	if err != nil {
		t.Fatal(err)
	}
	keys, err := codec.UnsealKeys(m, sealed, [32]byte{}, dec)
	if err != nil {
		t.Fatalf("unseal: %v", err)
	}
	dd := dec.(crypto.DeltaDecryptor)
	ctx := context.Background()
	// The manifest version and delta table must describe exactly the delta
	// objects the entries address (Version2 iff any delta entry).
	deltaEntries := 0
	for _, e := range m.Entries {
		if e.IsDelta {
			deltaEntries++
		}
	}
	if wantVer := codec.Version1; deltaEntries > 0 {
		wantVer = codec.Version2
		if m.Version != wantVer {
			t.Fatalf("manifest version %d with %d delta entries, want %d", m.Version, deltaEntries, wantVer)
		}
	} else if m.Version != wantVer {
		t.Fatalf("manifest version %d without delta entries, want %d", m.Version, wantVer)
	}
	if len(m.Deltas) != deltaEntries {
		t.Fatalf("delta table has %d refs for %d delta entries", len(m.Deltas), deltaEntries)
	}
	var out []byte
	for idx, e := range m.Entries {
		if e.IsZero {
			out = append(out, make([]byte, e.Size)...)
			continue
		}
		obj, ok := st.chunks[store.ContentKey(e.CiphertextHash)]
		if !ok {
			t.Fatalf("chunk %d: object %x missing from store", idx, e.CiphertextHash[:4])
		}
		if sha256.Sum256(obj) != e.CiphertextHash {
			t.Fatalf("chunk %d: object does not hash to its manifest entry", idx)
		}
		plain := make([]byte, e.Size)
		if obj[0] != crypto.ChunkFormatAESDelta {
			if e.IsDelta {
				t.Fatalf("chunk %d: flagged delta but the object is format %#x", idx, obj[0])
			}
			if err := dec.DecryptChunkTo(ctx, keys[idx], obj, plain); err != nil {
				t.Fatalf("chunk %d: decrypt: %v", idx, err)
			}
			out = append(out, plain...)
			continue
		}
		if !e.IsDelta {
			t.Fatalf("chunk %d: delta object but the entry is not flagged", idx)
		}
		opened, err := dd.OpenDeltaChunk(ctx, keys[idx], obj)
		if err != nil {
			t.Fatalf("chunk %d: open delta: %v", idx, err)
		}
		if opened.Header.TargetSize != e.Size {
			t.Fatalf("chunk %d: delta target size %d != entry size %d", idx, opened.Header.TargetSize, e.Size)
		}
		ref, ok := m.DeltaFor(idx)
		if !ok {
			t.Fatalf("chunk %d: no delta ref", idx)
		}
		if ref.Base != opened.Header.BaseObjectKey || ref.BaseSize != opened.Header.BaseSize {
			t.Fatalf("chunk %d: delta ref (%x, %d) disagrees with the object header (%x, %d)",
				idx, ref.Base[:4], ref.BaseSize, opened.Header.BaseObjectKey[:4], opened.Header.BaseSize)
		}
		baseObj, ok := st.chunks[store.ContentKey(opened.Header.BaseObjectKey)]
		if !ok {
			t.Fatalf("chunk %d: base object %x missing", idx, opened.Header.BaseObjectKey[:4])
		}
		if baseObj[0] == crypto.ChunkFormatAESDelta {
			t.Fatalf("chunk %d: base is itself a delta (depth must be one)", idx)
		}
		base := make([]byte, opened.Header.BaseSize)
		if err := dec.DecryptChunkTo(ctx, opened.BaseKey, baseObj, base); err != nil {
			t.Fatalf("chunk %d: decrypt base: %v", idx, err)
		}
		got, err := delta.Apply(base, opened.Stream, int(opened.Header.TargetSize))
		if err != nil {
			t.Fatalf("chunk %d: apply: %v", idx, err)
		}
		out = append(out, got...)
	}
	return out
}

func nonZeroChunks(img []byte) int {
	n := 0
	for off := 0; off < len(img); off += deltaChunk {
		if !bytes.Equal(img[off:off+deltaChunk], make([]byte, deltaChunk)) {
			n++
		}
	}
	return n
}

// ---- end to end ----------------------------------------------------------

// Checks the full path on two versions of a 12-chunk image. v0 is stored whole.
// In v1 every chunk is lightly edited, so each non-zero chunk is stored as a
// delta. Both images must read back, v1 must add less than a tenth of its size in
// new bytes, and the accounting (delta bytes, raw/compressed counts, stored bytes,
// diagnostics) must add up. Runs with one and four store workers.
func TestDeltaEndToEnd(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			st := newMemStore()
			st.workers = workers
			ing := newTestIngester(t, st, deltaEngine(t, nil))

			v0 := image(1, 12)
			v1 := editImage(v0, 77)

			r0 := ingestImage(t, ing, v0)
			if r0.DeltaChunks != 0 {
				t.Fatalf("v0 has nothing similar to delta against, got %d delta chunks", r0.DeltaChunks)
			}
			if !bytes.Equal(readBack(t, st, r0.ManifestKey), v0) {
				t.Fatal("v0 does not read back")
			}
			bytesAfterV0 := st.newBytes

			r1 := ingestImage(t, ing, v1)
			want := uint32(nonZeroChunks(v1))
			if r1.DeltaChunks != want {
				t.Fatalf("v1: %d delta chunks, want %d (%+v)", r1.DeltaChunks, want, r1)
			}
			if got := readBack(t, st, r1.ManifestKey); !bytes.Equal(got, v1) {
				t.Fatal("v1 does not read back through its deltas")
			}
			// v0 must still read back: writing deltas must never disturb bases.
			if !bytes.Equal(readBack(t, st, r0.ManifestKey), v0) {
				t.Fatal("v0 no longer reads back after v1")
			}

			// The point of the feature.
			added := st.newBytes - bytesAfterV0
			if added > len(v1)/10 {
				t.Fatalf("v1 added %d physical bytes for %d logical; expected a large reduction", added, len(v1))
			}
			if st.countFormat(crypto.ChunkFormatAESDelta) != int(want) {
				t.Fatalf("%d delta objects in store, want %d", st.countFormat(crypto.ChunkFormatAESDelta), want)
			}

			// Accounting invariants.
			if r1.DeltaLogicalBytes != uint64(want)*deltaChunk {
				t.Fatalf("DeltaLogicalBytes %d", r1.DeltaLogicalBytes)
			}
			if r1.DeltaSavedBytes < uint64(want)*(deltaChunk-8<<10) {
				t.Fatalf("DeltaSavedBytes %d implausibly small", r1.DeltaSavedBytes)
			}
			if r1.RawChunks+r1.CompressedChunks != want {
				t.Fatalf("Raw+Compressed %d != non-zero chunks %d", r1.RawChunks+r1.CompressedChunks, want)
			}
			if r1.ZeroChunks != 1 {
				t.Fatalf("zero chunks %d", r1.ZeroChunks)
			}
			if r1.StoredChunks != want || r1.DedupChunks != 0 {
				t.Fatalf("stored/dedup %d/%d", r1.StoredChunks, r1.DedupChunks)
			}
			var deltaObjBytes uint64
			for _, o := range st.chunks {
				if o[0] == crypto.ChunkFormatAESDelta {
					deltaObjBytes += uint64(len(o))
				}
			}
			if r1.StoredBytes != deltaObjBytes+r1.ManifestStoredBytes {
				t.Fatalf("StoredBytes %d != delta objects %d + manifest %d", r1.StoredBytes, deltaObjBytes, r1.ManifestStoredBytes)
			}
			if r1.DeltaAttempts != want || r1.DeltaNoCandidate != 0 || r1.DeltaErrors != 0 {
				t.Fatalf("diagnostics: %+v", r1)
			}
		})
	}
}

// Checks that a nil engine and a disabled engine both produce the same manifest
// as an ingester with no delta code, and that neither writes a delta object or
// reports any delta statistics.
func TestDeltaDisabledMatchesPlainIngester(t *testing.T) {
	img := image(1, 6)
	v1 := editImage(img, 5)

	plain := newMemStore()
	pIng := NewIngester(testKeyFn, nil, plain, fixedChunker(t, deltaChunk), testEncryptor())
	ingestImage(t, pIng, img)
	pRes := ingestImage(t, pIng, v1)

	for name, eng := range map[string]*delta.Engine{
		"nil engine": nil,
		"disabled":   deltaEngine(t, func(c *delta.Config) { c.Enabled = false }),
	} {
		st := newMemStore()
		ing := newTestIngester(t, st, eng)
		ingestImage(t, ing, img)
		res := ingestImage(t, ing, v1)
		if res.ManifestKey != pRes.ManifestKey {
			t.Errorf("%s: manifest key differs from the plain ingester", name)
		}
		if res.DeltaChunks != 0 || res.DeltaAttempts != 0 || res.DeltaNoCandidate != 0 {
			t.Errorf("%s: delta stats must be zero: %+v", name, res)
		}
		if st.countFormat(crypto.ChunkFormatAESDelta) != 0 {
			t.Errorf("%s: wrote a delta object", name)
		}
	}
}

// With delta on but nothing to delta against, the manifest must be exactly the
// one delta-off produces: turning the feature on cannot perturb a first image.
func TestDeltaFirstImageIsByteIdentical(t *testing.T) {
	img := image(2, 8)
	plain := ingestImage(t, NewIngester(testKeyFn, nil, newMemStore(), fixedChunker(t, deltaChunk), testEncryptor()), img)
	with := ingestImage(t, newTestIngester(t, newMemStore(), deltaEngine(t, nil)), img)
	if with.ManifestKey != plain.ManifestKey || with.StoredBytes != plain.StoredBytes {
		t.Fatalf("first image differs with delta on: %x vs %x", with.ManifestKey, plain.ManifestKey)
	}
}

// Checks that re-ingesting the same edited image in the same process gives the
// same manifest, with every delta chunk served as a representation hit. The store
// must receive no new puts or objects, and the image must read back.
func TestDeltaReingestIsIdempotent(t *testing.T) {
	st := newMemStore()
	ing := newTestIngester(t, st, deltaEngine(t, nil))
	v0 := image(1, 8)
	v1 := editImage(v0, 9)
	ingestImage(t, ing, v0)
	r1 := ingestImage(t, ing, v1)
	puts, objs := st.snapshotCounts()

	r1b := ingestImage(t, ing, v1)
	if r1b.ManifestKey != r1.ManifestKey {
		t.Fatal("re-ingesting the same image in the same process must give the same manifest")
	}
	if r1b.DeltaChunks != r1.DeltaChunks || r1b.DeltaRepHits != r1b.DeltaChunks {
		t.Fatalf("every delta chunk should be a representation hit: %+v", r1b)
	}
	if p2, o2 := st.snapshotCounts(); p2 != puts || o2 != objs {
		t.Fatalf("re-ingest wrote to the store: puts %d->%d objects %d->%d", puts, p2, objs, o2)
	}
	if r1b.StoredChunks != 0 || r1b.StoredBytes != 0 {
		t.Fatalf("nothing new should be stored: %+v", r1b)
	}
	if r1b.DedupChunks != r1b.DeltaChunks {
		t.Fatalf("rep hits are present objects: dedup %d delta %d", r1b.DedupChunks, r1b.DeltaChunks)
	}
	if !bytes.Equal(readBack(t, st, r1b.ManifestKey), v1) {
		t.Fatal("does not read back")
	}
}

// Checks that re-ingesting v0 must not re-delta chunks that are already stored
// whole. The second ingest must yield the same manifest with no deltas, one
// known-skip per non-zero chunk, and no attempt against a chunk's own copy.
func TestDeltaKnownIndependentChunksAreSkipped(t *testing.T) {
	st := newMemStore()
	ing := newTestIngester(t, st, deltaEngine(t, nil))
	v0 := image(1, 8)
	r0 := ingestImage(t, ing, v0)
	r0b := ingestImage(t, ing, v0)
	if r0b.ManifestKey != r0.ManifestKey || r0b.DeltaChunks != 0 {
		t.Fatalf("second ingest of identical image: %+v", r0b)
	}
	if r0b.DeltaKnownSkips != uint32(nonZeroChunks(v0)) {
		t.Fatalf("known skips %d, want %d", r0b.DeltaKnownSkips, nonZeroChunks(v0))
	}
	if r0b.DeltaAttempts != 0 {
		t.Fatal("must not attempt a delta against a chunk's own copy")
	}
}

// Checks restart behaviour. An engine loaded from the snapshot must reproduce the
// manifest with every delta as a representation hit and no store writes. An
// engine with no snapshot has no memory, but must still reproduce the same
// manifest by adopting the deltas the store already names, with nothing
// re-uploaded, and the result must read back.
func TestDeltaRepresentationHitsSurviveRestartViaSnapshot(t *testing.T) {
	snap := filepath.Join(t.TempDir(), "delta.snap")
	withSnap := func(c *delta.Config) { c.Index.SnapshotPath = snap }

	st := newMemStore()
	engA := deltaEngine(t, withSnap)
	ingA := newTestIngester(t, st, engA)
	v0 := image(1, 8)
	v1 := editImage(v0, 3)
	ingestImage(t, ingA, v0)
	r1 := ingestImage(t, ingA, v1)
	if err := engA.Close(); err != nil {
		t.Fatal(err)
	}
	puts, _ := st.snapshotCounts()

	// "Restart": a new engine from the snapshot, empty pool, same store.
	engB := deltaEngine(t, withSnap)
	if engB.LoadErr() != nil {
		t.Fatal(engB.LoadErr())
	}
	ingB := newTestIngester(t, st, engB)
	r1b := ingestImage(t, ingB, v1)
	if r1b.ManifestKey != r1.ManifestKey {
		t.Fatal("a restarted process must reproduce the manifest from the persisted index")
	}
	if r1b.DeltaRepHits != r1.DeltaChunks {
		t.Fatalf("rep hits %d, want %d", r1b.DeltaRepHits, r1.DeltaChunks)
	}
	if p2, _ := st.snapshotCounts(); p2 != puts {
		t.Fatal("restart re-wrote objects")
	}

	// Without the snapshot the restart has no memory of the deltas and no base
	// plaintext, but the store does: it resolves each chunk to the delta object
	// already written, and the new process adopts it after proving that it
	// reproduces the chunk. The manifest is therefore the same one, nothing is
	// re-uploaded, and the bases come back through read-back.
	engC := deltaEngine(t, nil)
	rc := ingestImage(t, newTestIngester(t, st, engC), v1)
	if rc.ManifestKey != r1.ManifestKey {
		t.Fatal("a process that lost its soft state must still reproduce the manifest")
	}
	if rc.DeltaAdopted != r1.DeltaChunks || rc.DeltaChunks != r1.DeltaChunks || rc.StoredChunks != 0 {
		t.Fatalf("cold engine should adopt every delta and write nothing: %+v", rc)
	}
	if rc.DeltaReadbacks == 0 {
		t.Fatalf("adoption must fetch bases it does not have: %+v", rc)
	}
	if p3, _ := st.snapshotCounts(); p3 != puts {
		t.Fatal("cold adoption re-wrote objects")
	}
	if !bytes.Equal(readBack(t, st, rc.ManifestKey), v1) {
		t.Fatal("cold re-ingest does not read back")
	}
}

// Checks that a restarted engine with an 80 KiB base pool still deltas an edited
// image. It fetches the bases it lacks back from the store, and the result must
// read back.
func TestDeltaReadBackAfterRestart(t *testing.T) {
	snap := filepath.Join(t.TempDir(), "delta.snap")
	st := newMemStore()
	eng := deltaEngine(t, func(c *delta.Config) { c.Index.SnapshotPath = snap })
	v0 := image(1, 6)
	ingestImage(t, newTestIngester(t, st, eng), v0)
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	eng2 := deltaEngine(t, func(c *delta.Config) {
		c.Index.SnapshotPath = snap
		c.BasePool.MaxBytes = "80KiB"
	})
	if eng2.LoadErr() != nil {
		t.Fatal(eng2.LoadErr())
	}
	edited := editImage(v0, 9)
	r := ingestImage(t, newTestIngester(t, st, eng2), edited)
	if r.DeltaChunks == 0 || r.DeltaReadbacks == 0 {
		t.Fatalf("restarted engine should read bases back from the store: %+v", r)
	}
	if !bytes.Equal(readBack(t, st, r.ManifestKey), edited) {
		t.Fatal("read back")
	}
}

// Checks that an unrelated image, with entirely different content, produces no
// deltas and still reads back.
func TestDeltaNotAppliedWhenDissimilar(t *testing.T) {
	st := newMemStore()
	ing := newTestIngester(t, st, deltaEngine(t, nil))
	ingestImage(t, ing, image(1, 8))
	other := image(2, 8) // entirely different content
	r := ingestImage(t, ing, other)
	if r.DeltaChunks != 0 {
		t.Fatalf("unrelated image produced %d deltas", r.DeltaChunks)
	}
	if !bytes.Equal(readBack(t, st, r.ManifestKey), other) {
		t.Fatal("read back")
	}
}

// A similar chunk whose delta does not save 20% must be stored whole. The
// fixture must really reach the acceptance rule (a candidate is found and
// encoded), otherwise the test would pass vacuously on "no candidate".
func TestDeltaAcceptanceRuleRejectsPoorSavings(t *testing.T) {
	st := newMemStore()
	eng := deltaEngine(t, func(c *delta.Config) {
		c.Candidates.MinSimilarity = 0.05
		c.Sketch.GroupSize = 1 // maximum recall so a weak match is still found
	})
	ing := newTestIngester(t, st, eng)
	base := rnd(1, deltaChunk)
	ingestImage(t, ing, base)

	// Keep ~15% of the chunk: the delta carries the other 85% as literals, so it
	// saves ~15% (< 20%) while still being a findable, weakly similar chunk.
	var target []byte
	for seed := int64(2); ; seed++ {
		target = append([]byte(nil), base...)
		copy(target[:deltaChunk*85/100], rnd(seed, deltaChunk*85/100))
		probe := newMemStore()
		pe := deltaEngine(t, func(c *delta.Config) { c.Candidates.MinSimilarity = 0.05; c.Sketch.GroupSize = 1 })
		pi := newTestIngester(t, probe, pe)
		ingestImage(t, pi, base)
		if pr := ingestImage(t, pi, target); pr.DeltaAttempts == 1 {
			break // this variant reaches the rule
		}
		if seed > 40 {
			t.Skip("no variant produced a weak-but-findable candidate")
		}
	}
	r := ingestImage(t, ing, target)
	if r.DeltaChunks != 0 {
		t.Fatalf("a delta that saves too little was accepted: %+v", r)
	}
	if r.DeltaAttempts != 1 || r.DeltaRejected != 1 {
		t.Fatalf("the acceptance rule was not exercised: %+v", r)
	}
	if st.countFormat(crypto.ChunkFormatAESDelta) != 0 {
		t.Fatal("a delta object was stored")
	}
	if !bytes.Equal(readBack(t, st, r.ManifestKey), target) {
		t.Fatal("read back")
	}
}

// Checks that with a one-base pool and a store that cannot be read back, at most
// one chunk is delta-encoded. The others must be counted as having no usable
// base, and the image must still read back.
func TestDeltaBaseMustBeResident(t *testing.T) {
	st := newMemStore()
	// Room for exactly one 64 KiB base: each new chunk evicts the previous.
	// Reads return nothing, so a base that has left the pool is unusable.
	eng := deltaEngine(t, func(c *delta.Config) { c.BasePool.MaxBytes = "80KiB" })
	ing := newTestIngester(t, unreadableStore{st}, eng)
	v0 := image(1, 6)
	ingestImage(t, ing, v0)
	r := ingestImage(t, ing, editImage(v0, 4))
	if r.DeltaChunks > 1 {
		t.Fatalf("%d deltas with a one-base pool and no read-back", r.DeltaChunks)
	}
	if r.DeltaNoCandidate == 0 && r.DeltaNotResident == 0 {
		t.Fatalf("expected chunks with no usable base: %+v", r)
	}
	if !bytes.Equal(readBack(t, st, r.ManifestKey), editImage(v0, 4)) {
		t.Fatal("read back")
	}
}

// Checks that with the same one-base pool but a readable store, more than one
// chunk deltas, because evicted bases are fetched back from the store. Read-backs
// must be counted, and the image must read back.
func TestDeltaReadBackServesEvictedBases(t *testing.T) {
	st := newMemStore()
	eng := deltaEngine(t, func(c *delta.Config) { c.BasePool.MaxBytes = "80KiB" })
	ing := newTestIngester(t, st, eng)
	v0 := image(1, 6)
	ingestImage(t, ing, v0)
	edited := editImage(v0, 4)
	r := ingestImage(t, ing, edited)
	if r.DeltaChunks < 2 {
		t.Fatalf("read-back should delta more than the one resident base: %+v", r)
	}
	if r.DeltaReadbacks == 0 {
		t.Fatalf("expected store read-backs: %+v", r)
	}
	if !bytes.Equal(readBack(t, st, r.ManifestKey), edited) {
		t.Fatal("read back")
	}
}

// Checks that a delta's base is never itself a delta. Three versions are ingested,
// with v2 similar to both v0 and v1. v2 must produce deltas and read back, and
// every delta object in the store must name a base that exists and is whole.
func TestDeltaDepthIsOne(t *testing.T) {
	st := newMemStore()
	ing := newTestIngester(t, st, deltaEngine(t, nil))
	v0 := image(1, 8)
	v1 := editImage(v0, 10)
	v2 := editImage(v1, 20) // similar to v1 AND (transitively, closely) to v0
	ingestImage(t, ing, v0)
	ingestImage(t, ing, v1)
	r2 := ingestImage(t, ing, v2)
	if r2.DeltaChunks == 0 {
		t.Fatalf("v2 should delta against v0's chunks: %+v", r2)
	}
	// readBack asserts every base is independent; also check globally.
	if !bytes.Equal(readBack(t, st, r2.ManifestKey), v2) {
		t.Fatal("v2 does not read back")
	}
	for k, o := range st.chunks {
		if o[0] != crypto.ChunkFormatAESDelta {
			continue
		}
		h, err := crypto.ParseDeltaHeader(o)
		if err != nil {
			t.Fatal(err)
		}
		if b := st.chunks[store.ContentKey(h.BaseObjectKey)]; b == nil || b[0] == crypto.ChunkFormatAESDelta {
			t.Fatalf("delta %x has a missing or delta base", k[:4])
		}
	}
}

// Checks that tenants with different salts do not share bases. tenant-b must get
// no deltas against tenant-a's chunks, while tenant-a still finds its own bases on
// a later ingest.
func TestDeltaDomainsAreIsolatedBySalt(t *testing.T) {
	st := newMemStore()
	eng := deltaEngine(t, nil)
	salted := func(s string) Ingester {
		return NewIngesterWithDelta(testKeyFn, func() ([]byte, error) { return []byte(s), nil }, st, fixedChunker(t, deltaChunk), testEncryptor(), eng)
	}
	v0 := image(1, 6)
	ingestImage(t, salted("tenant-a"), v0)
	r := ingestImage(t, salted("tenant-b"), editImage(v0, 3))
	if r.DeltaChunks != 0 {
		t.Fatalf("tenant-b delta-encoded against tenant-a's chunks: %+v", r)
	}
	rSame := ingestImage(t, salted("tenant-a"), editImage(v0, 3))
	if rSame.DeltaChunks == 0 {
		t.Fatalf("same tenant should still find its bases: %+v", rSame)
	}
	if !bytes.Equal(readBack(t, st, rSame.ManifestKey), editImage(v0, 3)) {
		t.Fatal("read back")
	}
}

// Checks that six images ingested at once through one engine and four store
// workers all read back, and that deltas are still produced under concurrency.
func TestDeltaConcurrentIngestsShareOneEngine(t *testing.T) {
	st := newMemStore()
	st.workers = 4
	eng := deltaEngine(t, nil)
	ing := newTestIngester(t, st, eng)
	v0 := image(1, 10)
	ingestImage(t, ing, v0)

	var wg sync.WaitGroup
	keys := make([]store.ContentKey, 6)
	imgs := make([][]byte, 6)
	for g := range keys {
		imgs[g] = editImage(v0, int64(100+g))
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			res, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(imgs[g]), uint64(len(imgs[g]))), IngestOption{})
			if err != nil {
				t.Errorf("ingest %d: %v", g, err)
				return
			}
			keys[g] = res.ManifestKey
		}(g)
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	for g := range keys {
		if !bytes.Equal(readBack(t, st, keys[g]), imgs[g]) {
			t.Fatalf("image %d does not read back", g)
		}
	}
	if st.countFormat(crypto.ChunkFormatAESDelta) == 0 {
		t.Fatal("no deltas produced under concurrency")
	}
}

// ---- writer capability ---------------------------------------------------

// Checks DeltaSupport for each writer and encryptor combination. Ordinary stores
// and ordered writers that opt in are allowed. Bundle-style ordered writers,
// opted-out writers, encryptors without delta support, and stores missing
// Resolve, PutRepresenting, or Get are refused, each with a non-empty reason.
func TestDeltaSupportMatrix(t *testing.T) {
	enc := testEncryptor()
	plain := newMemStore()
	ordered := &orderedMemStore{memStore: newMemStore()}
	cases := []struct {
		name string
		sw   StoreWriter
		enc  crypto.Encryptor
		ok   bool
	}{
		{"ordinary store", plain, enc, true},
		{"ordered writer without opt-in (bundle)", ordered, enc, false},
		{"ordered writer opted in (transfer, no bundle)", optedInStore{ordered}, enc, true},
		{"ordered writer opted out (transfer + bundle)", optedOutStore{ordered}, enc, false},
		{"encryptor without delta support", plain, returnedHashEncryptor{}, false},
		{"store that cannot resolve aliases", noResolveStore{plain}, enc, false},
		{"store that cannot record representations", noRepresentStore{plain}, enc, false},
		{"store that cannot be read", noReadStore{plain}, enc, false},
	}
	for _, tc := range cases {
		err := DeltaSupport(tc.sw, tc.enc)
		if (err == nil) != tc.ok {
			t.Errorf("%s: DeltaSupport = %v, want ok=%v", tc.name, err, tc.ok)
		}
		if err != nil && err.Error() == "" {
			t.Errorf("%s: reason must be non-empty", tc.name)
		}
	}
}

// Writers that lack one capability delta coordination rests on. Each embeds only
// the StoreWriter methods plus the others, so exactly one capability is missing.
type noResolveStore struct{ m *memStore }

func (w noResolveStore) AdmitWrite(ctx context.Context) (store.WriteAdmission, error) {
	return w.m.AdmitWrite(ctx)
}
func (w noResolveStore) Put(ctx context.Context, a store.WriteAdmission, p store.Partition, k store.ContentKey, d []byte) (bool, error) {
	return w.m.Put(ctx, a, p, k, d)
}
func (w noResolveStore) Get(ctx context.Context, p store.Partition, k store.ContentKey) (bool, []byte, error) {
	return w.m.Get(ctx, p, k)
}
func (w noResolveStore) PutRepresenting(ctx context.Context, a store.WriteAdmission, k store.ContentKey, d []byte, r store.ContentKey) (store.PutResult, error) {
	return w.m.PutRepresenting(ctx, a, k, d, r)
}

type noRepresentStore struct{ m *memStore }

func (w noRepresentStore) AdmitWrite(ctx context.Context) (store.WriteAdmission, error) {
	return w.m.AdmitWrite(ctx)
}
func (w noRepresentStore) Put(ctx context.Context, a store.WriteAdmission, p store.Partition, k store.ContentKey, d []byte) (bool, error) {
	return w.m.Put(ctx, a, p, k, d)
}
func (w noRepresentStore) Get(ctx context.Context, p store.Partition, k store.ContentKey) (bool, []byte, error) {
	return w.m.Get(ctx, p, k)
}
func (w noRepresentStore) Resolve(ctx context.Context, g store.Generation, h store.ContentKey) (store.Resolved, error) {
	return w.m.Resolve(ctx, g, h)
}

type noReadStore struct{ m *memStore }

func (w noReadStore) AdmitWrite(ctx context.Context) (store.WriteAdmission, error) {
	return w.m.AdmitWrite(ctx)
}
func (w noReadStore) Put(ctx context.Context, a store.WriteAdmission, p store.Partition, k store.ContentKey, d []byte) (bool, error) {
	return w.m.Put(ctx, a, p, k, d)
}
func (w noReadStore) Resolve(ctx context.Context, g store.Generation, h store.ContentKey) (store.Resolved, error) {
	return w.m.Resolve(ctx, g, h)
}
func (w noReadStore) PutRepresenting(ctx context.Context, a store.WriteAdmission, k store.ContentKey, d []byte, r store.ContentKey) (store.PutResult, error) {
	return w.m.PutRepresenting(ctx, a, k, d, r)
}

// Checks that bundle-style writers never get deltas, even when bases are available.
// Bases are primed through an ordinary writer on the SAME engine first, so that
// a bundle ingester would find bases and delta if it were allowed to. Without
// that the test would pass vacuously (no bases exist in a disabled ingester).
func TestDeltaDisabledForBundleWriters(t *testing.T) {
	v0 := image(1, 6)
	v1 := editImage(v0, 3)
	run := func(sw StoreWriter) *Result {
		eng := deltaEngine(t, nil)
		ingestImage(t, newTestIngester(t, newMemStore(), eng), v0) // primes bases
		return ingestImage(t, newTestIngester(t, sw, eng), v1)
	}

	control := run(optedInStore{&orderedMemStore{memStore: newMemStore()}})
	if control.DeltaChunks == 0 {
		t.Fatalf("control: an opted-in writer with primed bases must delta: %+v", control)
	}
	for name, sw := range map[string]StoreWriter{
		"ordered, no opt-in": &orderedMemStore{memStore: newMemStore()},
		"opted out":          optedOutStore{&orderedMemStore{memStore: newMemStore()}},
	} {
		if r := run(sw); r.DeltaChunks != 0 || r.DeltaAttempts != 0 || r.DeltaKnownSkips != 0 || r.DeltaNoCandidate != 0 {
			t.Errorf("%s: delta ran against a bundle writer: %+v", name, r)
		}
	}
}

// Checks that an ordered writer which opts in gets deltas, that chunks still go
// through PutChunkOrdered, and that the image reads back.
func TestDeltaWorksWithOptedInOrderedWriter(t *testing.T) {
	ord := &orderedMemStore{memStore: newMemStore()}
	sw := optedInStore{ord}
	ing := newTestIngester(t, sw, deltaEngine(t, nil))
	v0 := image(1, 6)
	ingestImage(t, ing, v0)
	r := ingestImage(t, ing, editImage(v0, 3))
	if r.DeltaChunks == 0 {
		t.Fatalf("opted-in ordered writer should get deltas: %+v", r)
	}
	if ord.ordered.Load() == 0 {
		t.Fatal("chunks must still flow through PutChunkOrdered")
	}
	if !bytes.Equal(readBack(t, ord.memStore, r.ManifestKey), editImage(v0, 3)) {
		t.Fatal("read back")
	}
}

// ---- failure behaviour ---------------------------------------------------

// Checks that if the delta object cannot be stored, the ingest fails, and no
// representation entry may be recorded for an object that does not exist. Once
// the failure clears, the same image must delta with no stale hit and read back.
func TestDeltaFailedPutLeavesNoStaleRepresentation(t *testing.T) {
	st := newMemStore()
	eng := deltaEngine(t, nil)
	ing := newTestIngester(t, st, eng)
	v0 := image(1, 6)
	v1 := editImage(v0, 3)
	ingestImage(t, ing, v0)

	st.failDelta = true
	if _, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(v1), uint64(len(v1))), IngestOption{}); err == nil {
		t.Fatal("expected the injected store failure to fail the ingest")
	}
	if got := eng.Stats().Representations; got != 0 {
		t.Fatalf("%d representation entries recorded for objects that were never stored", got)
	}

	st.failDelta = false
	r := ingestImage(t, ing, v1)
	if r.DeltaRepHits != 0 {
		t.Fatalf("stale representation hit after a failed put: %+v", r)
	}
	if r.DeltaChunks == 0 || !bytes.Equal(readBack(t, st, r.ManifestKey), v1) {
		t.Fatalf("recovery ingest failed: %+v", r)
	}
}

// Checks that a representation-index hit is confirmed against the store's own
// answer before the Put is skipped; the store, not the soft state, is
// authoritative. Subtests cover: store agrees (hit used); alias absent (hit
// forgotten and re-encoded); independent object present (delta abandoned);
// Resolve error (hit not trusted); verify_hit false (index trusted, no Resolve).
func TestDeltaVerifyHitAgainstResolve(t *testing.T) {
	v0 := image(1, 6)
	v1 := editImage(v0, 3)
	run := func(t *testing.T, hook func(store.ContentKey) (*store.Resolved, error), mut func(*delta.Config)) *Result {
		st := newMemStore()
		ing := newTestIngester(t, st, deltaEngine(t, mut))
		ingestImage(t, ing, v0)
		ingestImage(t, ing, v1)
		st.resolveHook = hook
		return ingestImage(t, ing, v1)
	}

	t.Run("store agrees: trusted", func(t *testing.T) {
		r := run(t, nil, nil)
		if r.DeltaRepHits == 0 || r.DeltaResolves == 0 {
			t.Fatalf("a confirmed hit must be used, and confirmed with Resolve: %+v", r)
		}
	})
	t.Run("alias absent: forgotten, re-encoded", func(t *testing.T) {
		r := run(t, func(store.ContentKey) (*store.Resolved, error) { return &store.Resolved{}, nil }, nil)
		if r.DeltaRepHits != 0 {
			t.Fatalf("a hit the store does not know must not be used: %+v", r)
		}
		if r.DeltaErrors == 0 {
			t.Fatal("stale entries should be counted")
		}
		if r.DeltaChunks == 0 {
			t.Fatalf("the chunks should have been delta-encoded again: %+v", r)
		}
	})
	t.Run("independent object exists: delta abandoned", func(t *testing.T) {
		r := run(t, func(store.ContentKey) (*store.Resolved, error) {
			return &store.Resolved{Resolution: store.ResolutionIndependent}, nil
		}, nil)
		if r.DeltaChunks != 0 || r.DeltaRepHits != 0 || r.DeltaKnownSkips == 0 {
			t.Fatalf("an independent object supersedes the delta: %+v", r)
		}
	})
	t.Run("resolve error: not trusted", func(t *testing.T) {
		r := run(t, func(store.ContentKey) (*store.Resolved, error) { return nil, errors.New("rpc down") }, nil)
		if r.DeltaRepHits != 0 || r.DeltaChunks != 0 {
			t.Fatalf("an unverifiable hit must not be used: %+v", r)
		}
		if r.DeltaErrors == 0 {
			t.Fatal("resolve failures should be counted")
		}
	})
	t.Run("verify_hit=false: index trusted, no Resolve", func(t *testing.T) {
		f := false
		r := run(t, func(store.ContentKey) (*store.Resolved, error) { return &store.Resolved{}, nil }, func(c *delta.Config) { c.Index.VerifyHit = &f })
		if r.DeltaRepHits == 0 || r.DeltaResolves != 0 {
			t.Fatalf("with verify_hit=false the index is trusted: %+v", r)
		}
	})
}

// Checks that a chunk the store already holds independently is not delta-encoded.
// Such chunks must be skipped as known, and the answer is remembered so a second
// ingest makes no Resolve calls.
func TestDeltaResolveIndependentAvoidsRedeltaOfStoredChunks(t *testing.T) {
	st := newMemStore()
	eng := deltaEngine(t, nil)
	ing := newTestIngester(t, st, eng)
	v0 := image(1, 6)
	v1 := editImage(v0, 3)
	ingestImage(t, ing, v0)
	// v1 is already stored whole (by a writer without delta).
	ingestImage(t, NewIngester(testKeyFn, nil, st, fixedChunker(t, deltaChunk), testEncryptor()), v1)

	r := ingestImage(t, ing, v1)
	if r.DeltaChunks != 0 || r.StoredChunks != 0 {
		t.Fatalf("chunks that exist independently must be skipped: %+v", r)
	}
	if r.DeltaKnownSkips != uint32(nonZeroChunks(v1)) {
		t.Fatalf("known skips = %d, want %d", r.DeltaKnownSkips, nonZeroChunks(v1))
	}
	// And the answer is remembered: the next pass does not ask again.
	asked := r.DeltaResolves
	r2 := ingestImage(t, ing, v1)
	if r2.DeltaResolves != 0 || asked == 0 {
		t.Fatalf("resolves: first %d, second %d", asked, r2.DeltaResolves)
	}
}

// ---- object contract -----------------------------------------------------

type deltaObjEncryptor struct {
	crypto.Encryptor
	object []byte
}

func (e deltaObjEncryptor) EncryptChunk(context.Context, [32]byte, []byte) ([]byte, [32]byte, [32]byte, error) {
	return append([]byte(nil), e.object...), sha256.Sum256(e.object), [32]byte{}, nil
}

// Checks that a delta-format (0x03) object is accepted only if its header matches
// the chunk it claims to encode. Garbage that starts with the 0x03 byte, or a
// 0x03 object returned by the encryptor as an independent object, must be rejected.
func TestIngestValidatesDeltaFormatObjects(t *testing.T) {
	// A well-formed delta object is only valid if its header agrees with the
	// chunk it claims to encode.
	enc := &crypto.AESChunkEncryptor{}
	salt := [32]byte{1}
	base := rnd(1, deltaChunk)
	target := edit(base, 2, 3, 64)
	_, bh, bk, _ := enc.EncryptChunk(context.Background(), salt, base)
	obj, _, _, err := enc.EncryptDeltaChunk(context.Background(), salt, crypto.DeltaBase{ObjectKey: bh, Key: bk, PlaintextSize: len(base)}, target, delta.Encode(base, target))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateEncryptedChunkObject(obj, uint32(len(target))); err != nil {
		t.Fatalf("a valid delta object was rejected: %v", err)
	}
	if err := validateEncryptedChunkObject(obj, uint32(len(target))+1); err == nil {
		t.Fatal("a delta object whose target size disagrees with the chunk was accepted")
	}
	if err := validateEncryptedChunkObject(obj[:len(obj)-1], uint32(len(target))); err == nil {
		t.Fatal("a truncated delta object was accepted")
	}
	garbage := append([]byte{crypto.ChunkFormatAESDelta}, make([]byte, 200)...)
	if err := validateEncryptedChunkObject(garbage, deltaChunk); err == nil {
		t.Fatal("a garbage 0x03 object was accepted")
	}

	// And the ingester refuses an encryptor that returns one as an independent object.
	ing := NewIngester(testKeyFn, nil, newMemStore(), fixedChunker(t, deltaChunk), deltaObjEncryptor{testEncryptor(), garbage})
	img := rnd(5, deltaChunk)
	if _, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(img), uint64(len(img))), IngestOption{}); err == nil {
		t.Fatal("ingest accepted an invalid delta-format object")
	}
}

// Checks that 4 KiB chunks are never attempted, because they cannot save the
// 4 KiB minimum. No base may be registered for them.
func TestDeltaSkipsChunksTooSmallToBenefit(t *testing.T) {
	st := newMemStore()
	ing := NewIngesterWithDelta(testKeyFn, nil, st, fixedChunker(t, 4096), testEncryptor(), deltaEngine(t, nil))
	img := rnd(1, 4096*8)
	ingestImage(t, ing, img)
	r := ingestImage(t, ing, edit(img, 3, 8, 16))
	if r.DeltaChunks != 0 || r.DeltaAttempts != 0 || r.DeltaNoCandidate != 0 {
		t.Fatalf("a 4 KiB chunk can never save 4 KiB; it must not even be attempted: %+v", r)
	}
	if got := deltaEngineBases(ing); got != 0 {
		t.Fatalf("%d ineligible chunks were registered as bases", got)
	}
}

func deltaEngineBases(ing Ingester) int {
	return ing.(*ingester).delta.eng.Stats().SimilarityBases
}

// Checks that chunks above max_chunk_size are neither attempted nor delta-encoded.
// The test sets the limit to 32 KiB, and the 64 KiB chunks must be excluded.
func TestDeltaMaxChunkSizeExcludesLargeChunks(t *testing.T) {
	st := newMemStore()
	eng := deltaEngine(t, func(c *delta.Config) { c.Candidates.MaxChunkSize = "32KiB" })
	ing := newTestIngester(t, st, eng)
	v0 := image(1, 6)
	ingestImage(t, ing, v0)
	r := ingestImage(t, ing, editImage(v0, 3))
	if r.DeltaChunks != 0 || r.DeltaAttempts != 0 {
		t.Fatalf("64 KiB chunks exceed max_chunk_size 32KiB: %+v", r)
	}
}
