package manifest_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/bundle"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/ingest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type bundleTestObject struct {
	partition store.Partition
	key       store.ContentKey
}

type bundleTestStore struct {
	mu        sync.Mutex
	admission store.WriteAdmission
	objects   map[bundleTestObject][]byte
}

func (s *bundleTestStore) AdmitWrite(ctx context.Context) (store.WriteAdmission, error) {
	return s.admission, ctx.Err()
}
func (*bundleTestStore) PoolSize() int { return 3 }
func (s *bundleTestStore) Put(ctx context.Context, a store.WriteAdmission, p store.Partition, k store.ContentKey, data []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if a != s.admission {
		return false, errors.New("admission changed during ingest")
	}
	if sha256.Sum256(data) != k {
		return false, errors.New("physical ContentKey mismatch")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := bundleTestObject{p, k}
	if old, ok := s.objects[id]; ok {
		if !bytes.Equal(old, data) {
			return false, errors.New("same key has different data")
		}
		return false, nil
	}
	s.objects[id] = bytes.Clone(data)
	return true, nil
}

func bundleTestConfig(t *testing.T) *manifest.Config {
	t.Helper()
	t.Setenv(manifest.CustomerKeyEnv, "")
	return &manifest.Config{
		Manifest: manifest.ManifestSubConfig{Key: strings.Repeat("ab", 32), WriteGeneration: "G1"},
		Chunker:  chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "4096"}},
		Crypto:   manifestcrypto.Config{Chunk: "aes", Manifest: "aes"},
	}
}

func TestBundleExtraSaltMatchesStoreObjectsAndReadsWithoutSalt(t *testing.T) {
	cfg := bundleTestConfig(t)
	ctx := context.Background()
	admission, err := cfg.WriteAdmission(ctx)
	if err != nil {
		t.Fatal(err)
	}
	customer, err := cfg.CustomerKey()
	if err != nil {
		t.Fatal(err)
	}
	_, dec, err := manifestcrypto.New(cfg.Crypto)
	if err != nil {
		t.Fatal(err)
	}
	// Include a nonzero repeated Chunk, explicit zero data, and a hole.
	data := append(bytes.Repeat([]byte{0x31}, 4096), make([]byte, 8192)...)
	data = append(data, bytes.Repeat([]byte{0x31}, 4096)...)
	newSource := func() sparse.Source {
		src, err := sparse.NewSource(bytes.NewReader(data), uint64(len(data)), []sparse.Extent{{Offset: 8192, Size: 4096}})
		if err != nil {
			t.Fatal(err)
		}
		return src
	}
	for _, extra := range []string{"", "tenant-a", "tenant-b"} {
		t.Run(fmt.Sprintf("extra=%q", extra), func(t *testing.T) {
			saltFn := func() ([]byte, error) { return []byte(extra), nil }
			target := &bundleTestStore{admission: admission, objects: make(map[bundleTestObject][]byte)}
			storeIngester, err := cfg.NewIngesterWithWriter(cfg.IngestKeyFunc(), saltFn, target)
			if err != nil {
				t.Fatal(err)
			}
			stored, err := storeIngester.Ingest(ctx, newSource(), ingest.IngestOption{})
			if err != nil {
				t.Fatal(err)
			}
			var encoded bytes.Buffer
			writer, err := bundle.NewWriter(&encoded, admission, bundle.WriterOptions{Concurrency: 3})
			if err != nil {
				t.Fatal(err)
			}
			bundleIngester, err := cfg.NewBundleIngesterWithExtraSalt(writer, saltFn)
			if err != nil {
				t.Fatal(err)
			}
			bundled, err := bundleIngester.Ingest(ctx, newSource(), ingest.IngestOption{})
			if err != nil {
				t.Fatal(err)
			}
			if bundled.ManifestKey != stored.ManifestKey {
				t.Fatalf("Bundle key %x differs from Store key %x", bundled.ManifestKey, stored.ManifestKey)
			}
			if err := writer.Finalize(bundled.ManifestKey); err != nil {
				t.Fatal(err)
			}
			reader, err := bundle.NewReader(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			// Verification obtains only the customer key and recorded base admission.
			if err := reader.FullVerify(ctx, bundled.ManifestKey, customer, dec, bundle.VerifyOptions{}); err != nil {
				t.Fatal(err)
			}
			if got := len(reader.ManifestKeys()) + len(reader.ChunkKeys()); got != len(target.objects) {
				t.Fatalf("Bundle object count %d differs from Store %d", got, len(target.objects))
			}
			for id, expected := range target.objects {
				_, blob, err := reader.Getter().Get(ctx, id.partition, id.key)
				if err != nil {
					t.Fatal(err)
				}
				equal := bytes.Equal(blob.Bytes(), expected)
				blob.Release()
				if !equal {
					t.Fatalf("different physical object %s/%x", id.partition, id.key)
				}
			}
			local := fetch.NewFetcher(customer, reader.Getter(), dec)
			stream, err := bundle.NewManifestFetcher(reader, local, nil).OpenManifest(ctx, bundled.ManifestKey)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			actual := make([]byte, len(data))
			if n, err := stream.ReadAt(ctx, actual, 0); err != nil || n != len(actual) {
				t.Fatalf("ReadAt = %d, %v", n, err)
			}
			if !bytes.Equal(actual, data) {
				t.Fatal("roundtrip content mismatch")
			}
			run, err := stream.RunAt(8192, 4096)
			if err != nil {
				t.Fatal(err)
			}
			if run.Kind() != sparse.Hole {
				t.Fatalf("hole became %v", run.Kind())
			}
			wrongCustomer := customer
			wrongCustomer[0] ^= 1
			if err := reader.FullVerify(ctx, bundled.ManifestKey, wrongCustomer, dec, bundle.VerifyOptions{}); err == nil {
				t.Fatal("wrong customer key accepted")
			}
		})
	}
}

func TestBundleExtraSaltDefaultsAndChangesIdentity(t *testing.T) {
	cfg := bundleTestConfig(t)
	ctx := context.Background()
	admission, err := cfg.WriteAdmission(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{0x51}, 8192)
	keys := map[string]store.ContentKey{}
	chunkKeys := map[string][]store.ContentKey{}
	for _, mode := range []string{"legacy", "nil", "empty", "tenant-a", "tenant-b"} {
		var output bytes.Buffer
		writer, err := bundle.NewWriter(&output, admission, bundle.WriterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var ing ingest.Ingester
		if mode == "legacy" {
			ing, err = cfg.NewBundleIngester(writer)
		} else {
			var extra ingest.ExtraSaltFunc
			if mode != "nil" {
				value := mode
				if value == "empty" {
					value = ""
				}
				extra = func() ([]byte, error) { return []byte(value), nil }
			}
			ing, err = cfg.NewBundleIngesterWithExtraSalt(writer, extra)
		}
		if err != nil {
			t.Fatal(err)
		}
		result, err := ing.Ingest(ctx, sparse.Dense(bytes.NewReader(data), uint64(len(data))), ingest.IngestOption{})
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Finalize(result.ManifestKey); err != nil {
			t.Fatal(err)
		}
		reader, err := bundle.NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
		if err != nil {
			t.Fatal(err)
		}
		keys[mode], chunkKeys[mode] = result.ManifestKey, reader.ChunkKeys()
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if keys["legacy"] != keys["nil"] || keys["nil"] != keys["empty"] {
		t.Fatal("empty extra salt changed the existing Bundle identity")
	}
	if keys["tenant-a"] == keys["tenant-b"] || keys["tenant-a"] == keys["legacy"] {
		t.Fatal("extra salt did not separate nonzero-content Manifest identities")
	}
	for _, mode := range []string{"legacy", "tenant-a", "tenant-b"} {
		if len(chunkKeys[mode]) != 1 {
			t.Fatalf("%s has %d chunks, want repeated-content dedup", mode, len(chunkKeys[mode]))
		}
	}
	if chunkKeys["tenant-a"][0] == chunkKeys["tenant-b"][0] || chunkKeys["tenant-a"][0] == chunkKeys["legacy"][0] {
		t.Fatal("extra salt did not separate nonzero Chunk keys")
	}
}

func TestBundleExtraSaltResolverFailure(t *testing.T) {
	cfg := bundleTestConfig(t)
	ctx := context.Background()
	admission, err := cfg.WriteAdmission(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target := &bundleTestStore{admission: admission, objects: make(map[bundleTestObject][]byte)}
	sentinel := errors.New("extra salt unavailable")
	ing, err := cfg.NewBundleIngesterWithExtraSalt(target, func() ([]byte, error) { return nil, sentinel })
	if err != nil {
		t.Fatal(err)
	}
	_, err = ing.Ingest(ctx, sparse.Dense(bytes.NewReader([]byte{1}), 1), ingest.IngestOption{})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Ingest = %v, want resolver error", err)
	}
	if len(target.objects) != 0 {
		t.Fatal("objects written after extra-salt resolver failure")
	}
	if _, err := cfg.NewBundleIngesterWithExtraSalt(nil, nil); err == nil {
		t.Fatal("nil writer accepted")
	}
}
