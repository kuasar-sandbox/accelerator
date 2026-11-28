package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/delta"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// Scenarios for replicas: nodes that share a store but not their soft state must
// still record the same representation for a chunk. Each test gives every "node"
// its own delta.Engine over one shared memStore, which models exactly that.

// nearImage returns img with one tiny edit in every non-zero chunk: closer to img
// than any other edit of the original is, so a node that has it as a base prefers
// it over the original base.
func nearImage(img []byte, seed int64) []byte {
	out := append([]byte(nil), img...)
	for off := 0; off < len(out); off += deltaChunk {
		if bytes.Equal(out[off:off+deltaChunk], make([]byte, deltaChunk)) {
			continue
		}
		copy(out[off:off+deltaChunk], edit(out[off:off+deltaChunk], seed+int64(off), 1, 16))
	}
	return out
}

func newNode(t *testing.T, st StoreWriter, mut func(*delta.Config)) (Ingester, *delta.Engine) {
	t.Helper()
	eng := deltaEngine(t, mut)
	return newTestIngester(t, st, eng), eng
}

// Scenario: node A ingests two versions and wins every alias it creates. A node
// with no local state then ingests the same second version. It must adopt A's
// delta objects, write nothing, and produce the same manifest. Its next ingest of
// that version must use the adopted entries as local representation hits.
func TestSecondNodeAdoptsFirstNodesRepresentation(t *testing.T) {
	st := newMemStore()
	v0 := image(1, 8)
	v1 := editImage(v0, 3)

	a, _ := newNode(t, st, nil)
	ingestImage(t, a, v0)
	ra := ingestImage(t, a, v1)
	if ra.DeltaChunks == 0 || ra.DeltaAliasWon != ra.DeltaChunks {
		t.Fatalf("first node should win every alias it creates: %+v", ra)
	}
	objects := st.countFormat(crypto.ChunkFormatAESDelta)
	puts, _ := st.snapshotCounts()

	// A node with no state at all.
	b, engB := newNode(t, st, nil)
	rb := ingestImage(t, b, v1)
	if rb.ManifestKey != ra.ManifestKey {
		t.Fatal("two nodes ingesting the same image must produce the same manifest")
	}
	if rb.DeltaAdopted != ra.DeltaChunks || rb.DeltaChunks != ra.DeltaChunks || rb.DeltaAdoptFailed != 0 {
		t.Fatalf("second node should adopt every delta: %+v", rb)
	}
	if rb.StoredChunks != 0 || rb.DeltaOrphanBytes != 0 {
		t.Fatalf("adoption must not write: %+v", rb)
	}
	if st.countFormat(crypto.ChunkFormatAESDelta) != objects {
		t.Fatal("a second delta object was created for an already-represented chunk")
	}
	if p2, _ := st.snapshotCounts(); p2 != puts {
		t.Fatalf("chunk puts %d -> %d: adoption must not Put", puts, p2)
	}
	if !bytes.Equal(readBack(t, st, rb.ManifestKey), v1) {
		t.Fatal("adopted image does not read back")
	}

	// The adopted entries are remembered, so the next pass needs no verification.
	rb2 := ingestImage(t, b, v1)
	if rb2.DeltaRepHits != rb.DeltaAdopted || rb2.DeltaAdopted != 0 || rb2.ManifestKey != ra.ManifestKey {
		t.Fatalf("adopted representations should become local hits: %+v", rb2)
	}
	if engB.Stats().Representations == 0 {
		t.Fatal("adoption did not record representations")
	}
}

// Scenario: a race. Node B resolves before node A's alias exists (modelled by a
// Resolve that says ABSENT), picks a different base, and uploads a different
// patch. The store tells B it lost, and B must adopt the winner's object so both
// manifests match, whichever node goes first. The store deletes the losing
// patches, so none are left as orphans.
func TestLosingNodeAdoptsTheWinnersObject(t *testing.T) {
	v0 := image(1, 8)
	v1 := editImage(v0, 3)
	v0b := nearImage(v1, 7)

	for _, tc := range []struct {
		name         string
		first, other []byte // the base image each node knows
	}{
		{"original base wins", v0, v0b},
		{"near base wins", v0b, v0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := newMemStore()
			a, _ := newNode(t, st, nil)
			ingestImage(t, a, tc.first)
			ra := ingestImage(t, a, v1)

			b, _ := newNode(t, st, nil)
			ingestImage(t, b, tc.other)
			objects := st.countFormat(crypto.ChunkFormatAESDelta)
			st.resolveHook = func(store.ContentKey) (*store.Resolved, error) { return &store.Resolved{}, nil }
			rb := ingestImage(t, b, v1)
			st.resolveHook = nil

			if rb.ManifestKey != ra.ManifestKey {
				t.Fatalf("losing node recorded a different manifest\nA %+v\nB %+v", ra, rb)
			}
			if rb.DeltaAliasLost == 0 || rb.DeltaAdopted != rb.DeltaAliasLost || rb.DeltaAdoptFailed != 0 {
				t.Fatalf("expected every chunk to lose and adopt: %+v", rb)
			}
			if rb.DeltaDiscardedBytes == 0 || rb.DeltaOrphanBytes != 0 || rb.StoredChunks != 0 {
				t.Fatalf("the losing patches must have been discarded by the store, not left as orphans: %+v", rb)
			}
			if got := st.countFormat(crypto.ChunkFormatAESDelta); got != objects {
				t.Fatalf("delta objects %d, want %d (losing patches are deleted)", got, objects)
			}
			if !bytes.Equal(readBack(t, st, rb.ManifestKey), v1) {
				t.Fatal("does not read back")
			}
		})
	}
}

// Scenario: the same race, but the store predates discarding losing objects and
// keeps them. B still adopts the winner, and its leftover patch is counted as an
// orphan.
func TestLosingPatchIsAnOrphanOnAnOlderStore(t *testing.T) {
	v0 := image(1, 8)
	v1 := editImage(v0, 3)
	v0b := nearImage(v1, 7)
	st := newMemStore()
	st.keepLosers = true
	a, _ := newNode(t, st, nil)
	ingestImage(t, a, v0)
	ra := ingestImage(t, a, v1)
	b, _ := newNode(t, st, nil)
	ingestImage(t, b, v0b)
	objects := st.countFormat(crypto.ChunkFormatAESDelta)
	st.resolveHook = func(store.ContentKey) (*store.Resolved, error) { return &store.Resolved{}, nil }
	rb := ingestImage(t, b, v1)
	st.resolveHook = nil
	if rb.ManifestKey != ra.ManifestKey || rb.DeltaAdopted != rb.DeltaAliasLost {
		t.Fatalf("losing node must still adopt: %+v", rb)
	}
	if rb.DeltaOrphanBytes == 0 || rb.DeltaDiscardedBytes != 0 {
		t.Fatalf("an old store keeps the losing patch; it must be counted as an orphan: %+v", rb)
	}
	if got := st.countFormat(crypto.ChunkFormatAESDelta); got != objects+int(rb.DeltaAliasLost) {
		t.Fatalf("delta objects %d, want %d", got, objects+int(rb.DeltaAliasLost))
	}
}

// Scenario: four nodes, each with its own engine and its own base image, ingest
// the same second version at the same time. Repeated twelve times, every node must
// record the same manifest, and it must read back.
func TestConcurrentNodesConvergeOnOneManifest(t *testing.T) {
	v0 := image(1, 8)
	v1 := editImage(v0, 3)
	v0b := nearImage(v1, 7)
	v0c := nearImage(v1, 11)
	bases := [][]byte{v0, v0b, v0c, v0}

	for round := 0; round < 12; round++ {
		st := newMemStore()
		var (
			wg   sync.WaitGroup
			keys = make([]store.ContentKey, len(bases))
			errs = make([]error, len(bases))
			dump = make([]*Result, len(bases))
		)
		start := make(chan struct{})
		for n := range bases {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				ing, _ := newNode(t, st, nil)
				ctx := context.Background()
				// Prime this node's own bases first, then race on v1.
				if _, err := ing.Ingest(ctx, denseSource(bases[n]), IngestOption{}); err != nil {
					errs[n] = err
					return
				}
				<-start
				res, err := ing.Ingest(ctx, denseSource(v1), IngestOption{})
				if err != nil {
					errs[n] = err
					return
				}
				keys[n] = res.ManifestKey
				dump[n] = res
			}(n)
		}
		close(start)
		wg.Wait()
		for n := range keys {
			if errs[n] != nil {
				t.Fatalf("round %d node %d: %v", round, n, errs[n])
			}
			if keys[n] != keys[0] {
				for i, r := range dump {
					t.Logf("node %d: stored=%d dedup=%d delta=%d won=%d lost=%d adopted=%d adoptFailed=%d indep=%d known=%d noCand=%d rej=%d errs=%d", i, r.StoredChunks, r.DedupChunks, r.DeltaChunks, r.DeltaAliasWon, r.DeltaAliasLost, r.DeltaAdopted, r.DeltaAdoptFailed, r.DeltaAliasIndependent, r.DeltaKnownSkips, r.DeltaNoCandidate, r.DeltaRejected, r.DeltaErrors)
				}
				t.Fatalf("round %d: node %d recorded manifest %x, node 0 recorded %x", round, n, keys[n][:4], keys[0][:4])
			}
		}
		if !bytes.Equal(readBack(t, st, keys[0]), v1) {
			t.Fatalf("round %d: does not read back", round)
		}
	}
}

// Scenario: node A stores the image whole and never deltas. Node B resolves those
// chunks as independent and must register them as bases, because they are as good
// as any it stored itself. Without that, B would store independently what its
// peers store as deltas, and replicas would diverge. The next version must delta
// against them and read back.
func TestChunksResolvedAsIndependentBecomeBases(t *testing.T) {
	st := newMemStore()
	v0 := image(1, 8)
	v1 := editImage(v0, 3)

	a, _ := newNode(t, st, func(c *delta.Config) { c.Candidates.MaxChunkSize = "1KiB" }) // a writer that never deltas
	ingestImage(t, a, v0)

	b, engB := newNode(t, st, nil)
	r0 := ingestImage(t, b, v0)
	if r0.DeltaKnownSkips != uint32(nonZeroChunks(v0)) || r0.StoredChunks != 0 {
		t.Fatalf("setup: every chunk should resolve as independent: %+v", r0)
	}
	if engB.Stats().SimilarityBases != nonZeroChunks(v0) {
		t.Fatalf("resolved-independent chunks were not registered as bases: %d", engB.Stats().SimilarityBases)
	}
	r1 := ingestImage(t, b, v1)
	if r1.DeltaChunks != uint32(nonZeroChunks(v1)) {
		t.Fatalf("the next version should delta against them: %+v", r1)
	}
	if !bytes.Equal(readBack(t, st, r1.ManifestKey), v1) {
		t.Fatal("does not read back")
	}
}

// Scenario: the store names a representation that cannot be used. Each subtest
// breaks it one way: the target object is missing; two aliases are swapped; the
// alias names a non-delta object; the delta carries a wrong base key, which must
// not reach the base pool; or the store read fails. A representation that does not
// reproduce the chunk must never be adopted, and the image must still read back.
func TestAdoptionRejectsUnusableAliases(t *testing.T) {
	v0 := image(1, 8)
	v1 := editImage(v0, 3)
	chunks := nonZeroChunks(v1)

	primed := func(t *testing.T) *memStore {
		st := newMemStore()
		a, _ := newNode(t, st, nil)
		ingestImage(t, a, v0)
		ingestImage(t, a, v1)
		if len(st.aliases) != chunks {
			t.Fatalf("setup: %d aliases, want %d", len(st.aliases), chunks)
		}
		return st
	}

	t.Run("target object missing", func(t *testing.T) {
		st := primed(t)
		var h, d store.ContentKey
		for h, d = range st.aliases {
			break
		}
		delete(st.chunks, d)
		b, _ := newNode(t, st, nil)
		rb := ingestImage(t, b, v1)
		if rb.DeltaAdoptFailed != 1 || rb.DeltaAdopted != uint32(chunks-1) {
			t.Fatalf("one alias is dangling: %+v", rb)
		}
		if rb.StoredChunks != 1 {
			t.Fatalf("the unadoptable chunk must be stored independently: %+v", rb)
		}
		if r, _ := st.Resolve(context.Background(), "G1", h); r.Resolution != store.ResolutionIndependent {
			t.Fatalf("the independent object must supersede the bad alias: %v", r.Resolution)
		}
		if !bytes.Equal(readBack(t, st, rb.ManifestKey), v1) {
			t.Fatal("does not read back")
		}
	})

	t.Run("alias names another chunk's delta", func(t *testing.T) {
		st := primed(t)
		var hs, ds []store.ContentKey
		for h, d := range st.aliases {
			hs, ds = append(hs, h), append(ds, d)
			if len(hs) == 2 {
				break
			}
		}
		st.aliases[hs[0]], st.aliases[hs[1]] = ds[1], ds[0]
		b, _ := newNode(t, st, nil)
		rb := ingestImage(t, b, v1)
		if rb.DeltaAdoptFailed != 2 || rb.DeltaAdopted != uint32(chunks-2) {
			t.Fatalf("two swapped aliases must both be refused: %+v", rb)
		}
		if !bytes.Equal(readBack(t, st, rb.ManifestKey), v1) {
			t.Fatal("does not read back")
		}
	})

	t.Run("alias names a non-delta object", func(t *testing.T) {
		st := primed(t)
		var h store.ContentKey
		for h = range st.aliases {
			break
		}
		junk := rnd(99, 300)
		jk := store.ContentKey(sha256sum(junk))
		st.chunks[jk] = junk
		st.aliases[h] = jk
		b, _ := newNode(t, st, nil)
		rb := ingestImage(t, b, v1)
		if rb.DeltaAdoptFailed != 1 {
			t.Fatalf("%+v", rb)
		}
		if !bytes.Equal(readBack(t, st, rb.ManifestKey), v1) {
			t.Fatal("does not read back")
		}
	})

	// The delta object opens correctly (its K_d is right) but carries a wrong key
	// for its base. Decrypting the base with it yields garbage that a RAW object
	// would accept silently; adopting it must be refused, and the garbage must not
	// reach the shared pool under the real object's name.
	t.Run("delta carries a wrong base key", func(t *testing.T) {
		st := newMemStore()
		a, _ := newNode(t, st, nil)
		ingestImage(t, a, v0) // stores v0's chunks as independent objects

		ctx := context.Background()
		enc := testEncryptor()
		salt := [32]byte{} // memStore admission salt, no extra salt
		basePlain := v0[:deltaChunk]
		target := v1[:deltaChunk]
		_, baseObj, _, err := enc.EncryptChunk(ctx, salt, basePlain)
		if err != nil {
			t.Fatal(err)
		}
		_, indepHash, _, err := enc.EncryptChunk(ctx, salt, target)
		if err != nil {
			t.Fatal(err)
		}
		wrong := [32]byte{0xde, 0xad}
		obj, objHash, _, err := enc.(crypto.DeltaEncryptor).EncryptDeltaChunk(ctx, salt,
			crypto.DeltaBase{ObjectKey: baseObj, Key: wrong, PlaintextSize: len(basePlain)}, target, delta.Encode(basePlain, target))
		if err != nil {
			t.Fatal(err)
		}
		st.chunks[store.ContentKey(objHash)] = obj
		st.aliases[store.ContentKey(indepHash)] = store.ContentKey(objHash)

		b, engB := newNode(t, st, nil)
		rb := ingestImage(t, b, v1)
		if rb.DeltaAdoptFailed != 1 {
			t.Fatalf("a wrong base key must be refused: %+v", rb)
		}
		if _, release, ok := engB.Domain(salt).AcquireBase(baseObj); ok {
			release()
			t.Fatal("a base decrypted with an untrusted key reached the pool")
		}
		if !bytes.Equal(readBack(t, st, rb.ManifestKey), v1) {
			t.Fatal("does not read back")
		}
	})

	// The adopting node already holds the base in its pool, so leaseBase never
	// reads it back. The delta's stream is valid against that base, but its base
	// key or its header's base size is wrong. Cold readers would fail on either,
	// so a pool hit must be verified as strictly as a read-back.
	for _, tc := range []struct {
		name    string
		key     func(real [32]byte) [32]byte
		sizeAdd int
	}{
		{"pooled base, delta carries a wrong base key", func([32]byte) [32]byte { return [32]byte{0xde, 0xad} }, 0},
		{"pooled base, delta header claims another base size", func(real [32]byte) [32]byte { return real }, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := newMemStore()
			a, _ := newNode(t, st, nil)
			ingestImage(t, a, v0)

			ctx := context.Background()
			enc := testEncryptor()
			salt := [32]byte{}
			basePlain := v0[:deltaChunk]
			target := v1[:deltaChunk]
			_, baseObj, baseKey, err := enc.EncryptChunk(ctx, salt, basePlain)
			if err != nil {
				t.Fatal(err)
			}
			_, indepHash, _, err := enc.EncryptChunk(ctx, salt, target)
			if err != nil {
				t.Fatal(err)
			}
			obj, objHash, _, err := enc.(crypto.DeltaEncryptor).EncryptDeltaChunk(ctx, salt,
				crypto.DeltaBase{ObjectKey: baseObj, Key: tc.key(baseKey), PlaintextSize: len(basePlain) + tc.sizeAdd},
				target, delta.Encode(basePlain, target))
			if err != nil {
				t.Fatal(err)
			}
			st.chunks[store.ContentKey(objHash)] = obj
			st.aliases[store.ContentKey(indepHash)] = store.ContentKey(objHash)

			b, engB := newNode(t, st, nil)
			ingestImage(t, b, v0) // registers v0's chunks as bases: the base is pooled
			if _, release, ok := engB.Domain(salt).AcquireBase(baseObj); !ok {
				t.Fatal("setup: the base must be in the adopting node's pool")
			} else {
				release()
			}
			rb := ingestImage(t, b, v1)
			if rb.DeltaAdoptFailed != 1 {
				t.Fatalf("a pooled base must be verified against the delta's key and size: %+v", rb)
			}
			if !bytes.Equal(readBack(t, st, rb.ManifestKey), v1) {
				t.Fatal("does not read back")
			}
		})
	}

	t.Run("cannot be checked: store read fails", func(t *testing.T) {
		st := primed(t)
		var failing atomic.Bool
		sw := flakyReadStore{memStore: st, fail: &failing}
		failing.Store(true)
		b, _ := newNode(t, sw, nil)
		rb := ingestImage(t, b, v1)
		failing.Store(false)
		if rb.DeltaAdoptFailed == 0 || rb.DeltaAdopted != 0 || rb.DeltaErrors == 0 {
			t.Fatalf("unreadable alias targets must be counted: %+v", rb)
		}
		if !bytes.Equal(readBack(t, st, rb.ManifestKey), v1) {
			t.Fatal("does not read back after degrading to independent objects")
		}
	})
}

type flakyReadStore struct {
	*memStore
	fail *atomic.Bool
}

func (f flakyReadStore) Get(ctx context.Context, p store.Partition, k store.ContentKey) (bool, []byte, error) {
	if f.fail.Load() {
		return false, nil, errors.New("injected read failure")
	}
	return f.memStore.Get(ctx, p, k)
}

// Scenario: what the node does with each answer from PutRepresenting. Independent:
// the whole object already exists, so the deltas are moot and the chunks become
// bases. Lost to an object it cannot adopt, or lost while the store kept the patch:
// the chunk is stored whole. None: the alias was not recorded, so the upload is an
// orphan and the chunk is stored whole.
func TestPutRepresentingOutcomes(t *testing.T) {
	v0 := image(1, 6)
	v1 := editImage(v0, 3)
	chunks := nonZeroChunks(v1)

	prime := func(t *testing.T) (*memStore, Ingester, *delta.Engine) {
		st := newMemStore()
		ing, eng := newNode(t, st, nil)
		ingestImage(t, ing, v0)
		return st, ing, eng
	}

	t.Run("independent object appeared first", func(t *testing.T) {
		st, ing, eng := prime(t)
		// The independent form of every v1 chunk, as another writer would store it.
		indep := map[store.ContentKey][]byte{}
		enc := testEncryptor()
		for off := 0; off < len(v1); off += deltaChunk {
			if bytes.Equal(v1[off:off+deltaChunk], make([]byte, deltaChunk)) {
				continue
			}
			obj, h, _, err := enc.EncryptChunk(context.Background(), [32]byte{}, v1[off:off+deltaChunk])
			if err != nil {
				t.Fatal(err)
			}
			indep[store.ContentKey(h)] = obj
		}
		st.repHook = func(_, represents store.ContentKey) *store.PutResult {
			st.chunks[represents] = indep[represents] // as the server would find it
			return &store.PutResult{Represented: store.RepresentedIndependent}
		}
		r := ingestImage(t, ing, v1)
		if r.DeltaChunks != 0 || r.DeltaAliasIndependent != uint32(chunks) || r.DeltaOrphanBytes == 0 {
			t.Fatalf("every delta is moot: %+v", r)
		}
		if !bytes.Equal(readBack(t, st, r.ManifestKey), v1) {
			t.Fatal("does not read back")
		}
		// The independent chunks are usable as bases from now on.
		if eng.Stats().SimilarityBases < 2*chunks {
			t.Fatalf("independent outcome did not register bases: %d", eng.Stats().SimilarityBases)
		}
	})

	// The store deleted our losing object and the winner's cannot be adopted:
	// the chunk must be stored independently. Referencing our own object would
	// point the manifest at something the store no longer holds.
	t.Run("lost to an object we cannot adopt: store independently", func(t *testing.T) {
		st, ing, eng := prime(t)
		st.repHook = func(_, _ store.ContentKey) *store.PutResult {
			return &store.PutResult{Represented: store.RepresentedLost, Key: store.ContentKey{0xff}}
		}
		r := ingestImage(t, ing, v1)
		if r.DeltaChunks != 0 || r.DeltaAliasLost != uint32(chunks) || r.DeltaAdoptFailed != uint32(chunks) {
			t.Fatalf("every chunk must lose, fail to adopt and fall back: %+v", r)
		}
		if r.StoredChunks != uint32(chunks) || r.DeltaDiscardedBytes == 0 || r.DeltaOrphanBytes != 0 {
			t.Fatalf("the fallback stores the independent object; the losing patch was discarded: %+v", r)
		}
		if st.countFormat(crypto.ChunkFormatAESDelta) != 0 {
			t.Fatal("a losing delta object survived in the store")
		}
		if eng.Stats().Representations != 0 {
			t.Fatal("a representation the store did not choose was recorded")
		}
		if !bytes.Equal(readBack(t, st, r.ManifestKey), v1) {
			t.Fatal("does not read back")
		}
	})

	// Same, but the store kept the losing object (an older server): the manifest
	// still must not reference it, because a current server would have deleted
	// it and the client cannot tell which it is talking to from this answer alone.
	t.Run("lost and not discarded: still store independently", func(t *testing.T) {
		st, ing, _ := prime(t)
		st.keepLosers = true
		st.repHook = func(_, _ store.ContentKey) *store.PutResult {
			return &store.PutResult{Represented: store.RepresentedLost, Key: store.ContentKey{0xff}}
		}
		r := ingestImage(t, ing, v1)
		if r.DeltaChunks != 0 || r.StoredChunks != uint32(chunks) || r.DeltaOrphanBytes == 0 {
			t.Fatalf("%+v", r)
		}
		if !bytes.Equal(readBack(t, st, r.ManifestKey), v1) {
			t.Fatal("does not read back")
		}
	})

	// The alias step failed: the object is stored but unnamed. An unnamed delta
	// object is never referenced; the chunk is stored independently and the
	// upload is the one kind of orphan left.
	t.Run("store recorded nothing", func(t *testing.T) {
		st, ing, eng := prime(t)
		st.repHook = func(_, _ store.ContentKey) *store.PutResult {
			return &store.PutResult{Represented: store.RepresentedNone}
		}
		r := ingestImage(t, ing, v1)
		if r.DeltaChunks != 0 || r.DeltaAliasWon != 0 || r.DeltaAliasLost != 0 || r.StoredChunks != uint32(chunks) {
			t.Fatalf("%+v", r)
		}
		if r.DeltaOrphanBytes == 0 || r.DeltaErrors != uint32(chunks) {
			t.Fatalf("the unnamed uploads are orphans and the failure is counted: %+v", r)
		}
		if eng.Stats().Representations != 0 {
			t.Fatalf("a representation the store did not record must not be remembered: %d", eng.Stats().Representations)
		}
		if !bytes.Equal(readBack(t, st, r.ManifestKey), v1) {
			t.Fatal("does not read back")
		}
	})
}

// Scenario: a store that cannot answer Resolve cannot coordinate replicas, so no
// delta object is written. The engine must remember that, so later ingests stop
// calling Resolve, and the image must still read back.
func TestDeltaDisabledWhenStoreCannotResolve(t *testing.T) {
	st := newMemStore()
	st.noAliases = true
	ing, eng := newNode(t, st, nil)
	v0 := image(1, 6)
	v1 := editImage(v0, 3)

	r0 := ingestImage(t, ing, v0)
	r1 := ingestImage(t, ing, v1)
	if r0.DeltaChunks+r1.DeltaChunks != 0 || st.countFormat(crypto.ChunkFormatAESDelta) != 0 {
		t.Fatalf("delta objects were written to a store that cannot coordinate them: %+v %+v", r0, r1)
	}
	if !eng.AliasUnsupported() {
		t.Fatal("the engine did not remember that the store has no alias support")
	}
	if r1.DeltaResolves != 0 {
		t.Fatalf("Resolve kept being called after it was found unsupported: %d", r1.DeltaResolves)
	}
	if !bytes.Equal(readBack(t, st, r1.ManifestKey), v1) {
		t.Fatal("does not read back")
	}
}

// Scenario: one Resolve call fails with an RPC error. The chunks are stored whole
// and the error is counted. A transient error must not disable delta for the
// process.
func TestResolveFailureStoresIndependently(t *testing.T) {
	st := newMemStore()
	ing, eng := newNode(t, st, nil)
	v0 := image(1, 6)
	v1 := editImage(v0, 3)
	ingestImage(t, ing, v0)
	st.resolveHook = func(store.ContentKey) (*store.Resolved, error) { return nil, errors.New("rpc down") }
	r := ingestImage(t, ing, v1)
	if r.DeltaChunks != 0 || r.DeltaErrors == 0 {
		t.Fatalf("a failed Resolve must degrade to independent objects and be counted: %+v", r)
	}
	if eng.AliasUnsupported() {
		t.Fatal("a transient error must not disable delta for the process")
	}
	if !bytes.Equal(readBack(t, st, r.ManifestKey), v1) {
		t.Fatal("does not read back")
	}
}

func sha256sum(b []byte) [32]byte { return sha256.Sum256(b) }

func denseSource(b []byte) sparse.Source { return sparse.Dense(bytes.NewReader(b), uint64(len(b))) }
