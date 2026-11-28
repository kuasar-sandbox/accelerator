package fetch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/delta"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// mapGetter is an in-memory cache.Getter keyed by content address. It counts
// every Get per key, so tests can assert how often an object was fetched.
type mapGetter struct {
	mu   sync.Mutex
	objs map[store.ContentKey][]byte
	gets map[store.ContentKey]int
}

// newMapGetter returns an empty mapGetter.
func newMapGetter() *mapGetter {
	return &mapGetter{objs: map[store.ContentKey][]byte{}, gets: map[store.ContentKey]int{}}
}

// put stores obj under its SHA-256 address and returns that address. Tests use it
// to publish base and delta objects where the getter can find them.
func (g *mapGetter) put(obj []byte) store.ContentKey {
	key := sha256.Sum256(obj)
	g.mu.Lock()
	g.objs[key] = obj
	g.mu.Unlock()
	return key
}

// Get implements cache.Getter. It returns a hit with the stored bytes, or a miss
// for an unknown key. Every call is counted, hit or miss.
func (g *mapGetter) Get(_ context.Context, _ store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.gets[key]++
	b, ok := g.objs[key]
	if !ok {
		return cache.CacheMiss, nil, nil
	}
	return cache.CacheHit, cache.NewMemBlob(b), nil
}

// count returns how many times key has been requested through Get, hits and
// misses alike.
func (g *mapGetter) count(key store.ContentKey) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.gets[key]
}

// deltaFixture builds the shared test data. The base is a 33 KiB repeated
// block, and the target is the base with two bytes flipped. The base is
// encrypted as an ordinary chunk, and the target is encrypted as a delta against
// it. Both objects are put in a new mapGetter. It returns the target, the delta
// object and its key, the base address, the decryptor, and the getter.
func deltaFixture(t *testing.T) (target []byte, deltaObj []byte, deltaKey [32]byte, baseKey store.ContentKey, dec crypto.Decryptor, g *mapGetter) {
	t.Helper()
	enc, dec, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	g = newMapGetter()
	var salt [32]byte
	salt[0] = 7
	base := bytes.Repeat([]byte("base-block-"), 3000) // 33 KiB, comfortably past the 4 KiB rule
	target = append([]byte(nil), base...)
	target[100] ^= 0x5a
	target[8000] ^= 0xa5

	baseObj, baseHash, kBase, err := enc.EncryptChunk(context.Background(), salt, base)
	if err != nil {
		t.Fatal(err)
	}
	if g.put(baseObj) != baseHash {
		t.Fatal("base address")
	}
	stream := delta.Encode(base, target)
	deltaObj, _, deltaKey, err = enc.(crypto.DeltaEncryptor).EncryptDeltaChunk(context.Background(), salt, crypto.DeltaBase{
		ObjectKey: baseHash, Key: kBase, PlaintextSize: len(base),
	}, target, stream)
	if err != nil {
		t.Fatal(err)
	}
	g.put(deltaObj)
	return target, deltaObj, deltaKey, baseHash, dec, g
}

// deltaManifest returns a one-entry Version2 manifest whose entry is flagged
// delta and whose delta table names the base the object header names.
func deltaManifest(t *testing.T, target, deltaObj []byte) *codec.Manifest {
	t.Helper()
	hdr, err := crypto.ParseDeltaHeader(deltaObj)
	if err != nil {
		t.Fatal(err)
	}
	return &codec.Manifest{
		Version:   codec.Version2,
		ImageSize: uint64(len(target)),
		Entries:   []codec.ChunkEntry{{Offset: 0, Size: uint32(len(target)), CiphertextHash: sha256.Sum256(deltaObj), IsDelta: true}},
		Deltas:    []codec.DeltaRef{{Entry: 0, BaseSize: hdr.BaseSize, Base: hdr.BaseObjectKey}},
	}
}

// deltaStream wraps deltaObj in a one-entry manifestStream. The entry is keyed by
// deltaKey and reads through g. The delta scratch budget is set to scratch, so a
// test can make it too small on purpose.
func deltaStream(t *testing.T, target, deltaObj []byte, deltaKey [32]byte, enc crypto.Decryptor, g cache.Getter, scratch int64) *manifestStream {
	t.Helper()
	return newManifestStreamWithOptions(
		deltaManifest(t, target, deltaObj),
		[][32]byte{deltaKey},
		g, g, enc,
		Options{VerifyContent: true, DeltaScratchBytes: scratch, DeltaBaseCacheBytes: 4 << 20},
	)
}

// TestMaterializeDeltaRoundTripAndRange checks that a delta chunk rebuilds
// correctly both as a full read and as a 64-byte range read. The base must be
// fetched once, because the second read has to hit the cached plaintext.
func TestMaterializeDeltaRoundTripAndRange(t *testing.T) {
	target, obj, key, baseKey, enc, g := deltaFixture(t)
	s := deltaStream(t, target, obj, key, enc, g, 32<<20)
	defer s.Close()

	got := make([]byte, len(target))
	if _, err := s.readChunkAt(context.Background(), got, 0, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, target) {
		t.Fatal("full read does not rebuild the chunk")
	}

	part := make([]byte, 64)
	off := uint64(1000)
	if _, err := s.readChunkAt(context.Background(), part, 0, off); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(part, target[off:off+64]) {
		t.Fatal("range read")
	}
	if g.count(baseKey) != 1 {
		t.Fatalf("base fetched %d times, want 1 (second read must hit the plaintext cache)", g.count(baseKey))
	}
}

// TestMaterializeDeltaRejectsADeltaBase checks depth one. A delta whose base is
// itself a delta object (format byte 0x03) must fail with a "depth one" error,
// rather than being applied on top of another delta.
func TestMaterializeDeltaRejectsADeltaBase(t *testing.T) {
	target, obj, key, _, dec, g := deltaFixture(t)
	// Point a new delta at the existing delta object, so the named base is 0x03.
	var salt [32]byte
	salt[0] = 9
	baseHash := sha256.Sum256(obj)
	stream := delta.Encode(target, target)
	enc, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	nested, _, nestedKey, err := enc.(crypto.DeltaEncryptor).EncryptDeltaChunk(context.Background(), salt, crypto.DeltaBase{
		ObjectKey: baseHash, Key: key, PlaintextSize: len(target),
	}, target, stream)
	if err != nil {
		t.Fatal(err)
	}
	g.put(nested)
	s := deltaStream(t, target, nested, nestedKey, dec, g, 32<<20)
	defer s.Close()
	err = func() error {
		_, err := s.readChunkAt(context.Background(), make([]byte, len(target)), 0, 0)
		return err
	}()
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("depth one")) {
		t.Fatalf("want a depth-one failure, got %v", err)
	}
}

// TestMaterializeDeltaMissingBase checks that a read fails when the base object is
// absent from the store, rather than returning partial plaintext.
func TestMaterializeDeltaMissingBase(t *testing.T) {
	target, obj, key, baseKey, enc, g := deltaFixture(t)
	g.mu.Lock()
	delete(g.objs, baseKey)
	g.mu.Unlock()
	s := deltaStream(t, target, obj, key, enc, g, 32<<20)
	defer s.Close()
	if _, err := s.readChunkAt(context.Background(), make([]byte, len(target)), 0, 0); err == nil {
		t.Fatal("missing base must fail the read")
	}
}

// TestBaseCacheIsKeyedByDecryptionKey checks that the base cache is keyed by
// (object address, decryption key, size). Two delta objects name the same base,
// one with the right base key and one with a wrong one, and the wrong one is read
// first. Keyed on the address alone, the cache would hand its garbage plaintext
// to the second reader. Keyed on the full triple, the second reader decrypts the
// base itself and rebuilds its chunk. The base is fetched once per distinct key,
// and a repeat read of the same entry does not fetch it again.
func TestBaseCacheIsKeyedByDecryptionKey(t *testing.T) {
	enc, dec, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	g := newMapGetter()
	var salt [32]byte
	salt[0] = 11
	base := bytes.Repeat([]byte("shared-base-"), 3000)
	target := append([]byte(nil), base...)
	target[500] ^= 0x33

	baseObj, baseHash, kBase, err := enc.EncryptChunk(context.Background(), salt, base)
	if err != nil {
		t.Fatal(err)
	}
	g.put(baseObj)
	stream := delta.Encode(base, target)
	denc := enc.(crypto.DeltaEncryptor)
	good, _, goodKey, err := denc.EncryptDeltaChunk(context.Background(), salt, crypto.DeltaBase{
		ObjectKey: baseHash, Key: kBase, PlaintextSize: len(base),
	}, target, stream)
	if err != nil {
		t.Fatal(err)
	}
	wrong := kBase
	wrong[0] ^= 0xff
	bad, _, badKey, err := denc.EncryptDeltaChunk(context.Background(), salt, crypto.DeltaBase{
		ObjectKey: baseHash, Key: wrong, PlaintextSize: len(base),
	}, target, stream)
	if err != nil {
		t.Fatal(err)
	}
	g.put(good)
	g.put(bad)

	// One manifest, two entries, one shared base cache.
	n := uint64(len(target))
	m := &codec.Manifest{Version: codec.Version2, ImageSize: 2 * n, Entries: []codec.ChunkEntry{
		{Offset: 0, Size: uint32(n), CiphertextHash: sha256.Sum256(bad), IsDelta: true},
		{Offset: n, Size: uint32(n), CiphertextHash: sha256.Sum256(good), IsDelta: true},
	}, Deltas: []codec.DeltaRef{
		{Entry: 0, BaseSize: uint32(len(base)), Base: baseHash},
		{Entry: 1, BaseSize: uint32(len(base)), Base: baseHash},
	}}
	s := newManifestStreamWithOptions(m, [][32]byte{badKey, goodKey}, g, g, dec,
		Options{VerifyContent: true, DeltaScratchBytes: 32 << 20, DeltaBaseCacheBytes: 4 << 20})
	defer s.Close()

	// The poisoned delta first. Whether it errors or yields garbage is not the
	// point; it must not leave anything the good delta can pick up.
	junk := make([]byte, n)
	_, _ = s.readChunkAt(context.Background(), junk, 0, 0)
	if bytes.Equal(junk, target) {
		t.Fatal("a delta carrying the wrong base key rebuilt the chunk; the fixture is wrong")
	}

	got := make([]byte, n)
	if _, err := s.readChunkAt(context.Background(), got, 1, n); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, target) {
		t.Fatal("the good delta read the base plaintext the bad delta decrypted")
	}
	if c := g.count(baseHash); c != 2 {
		t.Fatalf("base fetched %d times, want 2 (one decryption per distinct base key)", c)
	}

	// Same key and size: the entry is shared and the base is not fetched again.
	again := make([]byte, n)
	if _, err := s.readChunkAt(context.Background(), again, 1, n); err != nil {
		t.Fatal(err)
	}
	if c := g.count(baseHash); c != 2 {
		t.Fatalf("base fetched %d times after a repeat read, want 2", c)
	}
}

// TestPrefetchWarmsDeltaBases checks prefetch of delta chunks. Prefetch fetches
// the base named by the delta table through the prefetch getter, alongside the
// delta object, and a read after prefetch still rebuilds the chunk. A base
// missing from the prefetch getter must be reported as an error.
func TestPrefetchWarmsDeltaBases(t *testing.T) {
	target, obj, key, baseKey, dec, onDemand := deltaFixture(t)
	hash := sha256.Sum256(obj)

	t.Run("warms the base", func(t *testing.T) {
		prefetch := newMapGetter()
		prefetch.put(obj)
		prefetch.objs[baseKey] = onDemand.objs[baseKey]
		s := newManifestStreamWithOptions(deltaManifest(t, target, obj), [][32]byte{key}, onDemand, prefetch, dec,
			Options{VerifyContent: true, DeltaScratchBytes: 32 << 20, DeltaBaseCacheBytes: 4 << 20})
		defer s.Close()
		if err := prefetchStream(context.Background(), s); err != nil {
			t.Fatal(err)
		}
		if got := prefetch.count(hash); got != 1 {
			t.Fatalf("delta object prefetched %d times, want 1", got)
		}
		if got := prefetch.count(baseKey); got != 1 {
			t.Fatalf("base prefetched %d times, want 1", got)
		}
		// Prefetch warms the cache only; the read still rebuilds correctly.
		got := make([]byte, len(target))
		if _, err := s.readChunkAt(context.Background(), got, 0, 0); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, target) {
			t.Fatal("read after prefetch does not rebuild the chunk")
		}
	})

	t.Run("missing base is reported", func(t *testing.T) {
		prefetch := newMapGetter()
		prefetch.put(obj)
		s := newManifestStreamWithOptions(deltaManifest(t, target, obj), [][32]byte{key}, onDemand, prefetch, dec,
			Options{VerifyContent: true, DeltaScratchBytes: 32 << 20, DeltaBaseCacheBytes: 4 << 20})
		defer s.Close()
		if err := prefetchStream(context.Background(), s); err == nil {
			t.Fatal("prefetch must report a base the store does not have")
		}
	})
}

// TestMaterializeDeltaScratchBudget checks that applying a delta is capped by the
// delta scratch budget. With a 1024-byte budget, smaller than the base plaintext,
// the read must fail with a scratch error. The caller's destination buffer is not
// charged against this budget.
func TestMaterializeDeltaScratchBudget(t *testing.T) {
	target, obj, key, _, dec, g := deltaFixture(t)
	s := deltaStream(t, target, obj, key, dec, g, 1024) // smaller than the base plaintext
	defer s.Close()
	_, err := s.readChunkAt(context.Background(), make([]byte, len(target)), 0, 0)
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("scratch")) {
		t.Fatalf("want a scratch-budget failure, got %v", err)
	}
}

// TestDeltaReadsBindToManifest checks that a read trusts the authenticated
// manifest over the object: the entry's delta flag must match the object's
// format byte, and the delta table must name the base and base size the object
// header names. Each case runs with content verification on and off, through
// the full-chunk path and the partial-read path, and the base is never fetched.
func TestDeltaReadsBindToManifest(t *testing.T) {
	target, obj, key, baseKey, dec, g := deltaFixture(t)
	baseObj := g.objs[baseKey]

	for _, tc := range []struct {
		name string
		mut  func(m *codec.Manifest)
		want string
	}{
		{"delta object, entry not flagged", func(m *codec.Manifest) {
			m.Version, m.Entries[0].IsDelta, m.Deltas = codec.Version1, false, nil
		}, "not flagged delta"},
		{"flagged entry, independent object", func(m *codec.Manifest) {
			m.Entries[0].CiphertextHash = sha256.Sum256(baseObj)
		}, "object is not a delta"},
		{"table names another base", func(m *codec.Manifest) {
			m.Deltas[0].Base[0] ^= 0xff
		}, "manifest says"},
		{"table names another base size", func(m *codec.Manifest) {
			m.Deltas[0].BaseSize++
		}, "manifest says"},
	} {
		for _, verify := range []bool{true, false} {
			for _, partial := range []bool{false, true} {
				name := fmt.Sprintf("%s/verify=%v/partial=%v", tc.name, verify, partial)
				t.Run(name, func(t *testing.T) {
					m := deltaManifest(t, target, obj)
					tc.mut(m)
					s := newManifestStreamWithOptions(m, [][32]byte{key}, g, g, dec,
						Options{VerifyContent: verify, DeltaScratchBytes: 32 << 20, DeltaBaseCacheBytes: 4 << 20})
					defer s.Close()
					before := g.count(baseKey)
					buf := make([]byte, len(target))
					if partial {
						buf = buf[:64]
					}
					_, err := s.readChunkAt(context.Background(), buf, 0, 0)
					if err == nil || !strings.Contains(err.Error(), tc.want) {
						t.Fatalf("want an error containing %q, got %v", tc.want, err)
					}
					if tc.name != "flagged entry, independent object" && g.count(baseKey) != before {
						t.Fatal("the base must not be fetched when the manifest and the object disagree")
					}
				})
			}
		}
	}
}
