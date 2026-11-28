package manifest_test

import (
	"bytes"
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/bundle"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/delta"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/ingest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

const deltaCfgChunk = 64 << 10

func deltaTestConfig(t *testing.T, enabled bool) *manifest.Config {
	t.Helper()
	cfg := bundleTestConfig(t)
	cfg.Chunker.Fixed.Size = "65536"
	cfg.Delta = delta.Config{Enabled: enabled}
	cfg.Delta.BasePool.MaxBytes = "64MiB"
	cfg.Delta.Index.KnownCapacity = 1 << 14
	return cfg
}

func deltaImage(seed int64, chunks int) []byte {
	b := make([]byte, chunks*deltaCfgChunk)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func deltaEdited(img []byte, seed int64) []byte {
	out := append([]byte(nil), img...)
	r := rand.New(rand.NewSource(seed))
	for off := 0; off < len(out); off += deltaCfgChunk {
		for i := 0; i < 3; i++ {
			at := off + r.Intn(deltaCfgChunk-64)
			r.Read(out[at : at+64])
		}
	}
	return out
}

func ingestBytes(t *testing.T, ing ingest.Ingester, data []byte) *ingest.Result {
	t.Helper()
	res, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(data), uint64(len(data))), ingest.IngestOption{})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// TestConfigDeltaYAML checks the delta block in a config file. A valid block must
// decode onto the config, and a misspelt key must be rejected and named in the
// error. A config with no delta block, as written before this feature, must still
// load with delta disabled.
func TestConfigDeltaYAML(t *testing.T) {
	doc := `
manifest: {key: "` + strings.Repeat("ab", 32) + `"}
chunker: {mode: fixed, fixed: {size: "64KiB"}}
crypto: {chunk: aes, manifest: aes}
delta:
  enabled: true
  sketch: {slots: 32, group_size: 2}
  candidates: {min_similarity: 0.6, max: 3}
  base_pool: {max_bytes: "1GiB"}
`
	cfg, err := manifest.ParseConfig([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Delta.Enabled || cfg.Delta.Sketch.Slots != 32 || cfg.Delta.Candidates.Max != 3 || cfg.Delta.BasePool.MaxBytes != "1GiB" {
		t.Fatalf("delta block not decoded: %+v", cfg.Delta)
	}

	bad := strings.Replace(doc, "min_similarity", "min_simlarity", 1)
	if _, err := manifest.ParseConfig([]byte(bad)); err == nil || !strings.Contains(err.Error(), "min_simlarity") {
		t.Fatalf("a misspelt delta key must be rejected and named: %v", err)
	}

	// Files written before this feature have no delta block at all.
	old, err := manifest.ParseConfig([]byte("manifest: {key: \"" + strings.Repeat("ab", 32) + "\"}\n"))
	if err != nil || old.Delta.Enabled {
		t.Fatalf("a config without a delta block must still load, disabled: %v", err)
	}
}

// TestConfigDeltaInvalidFailsEvenWhenDisabled checks that an invalid delta block
// (sketch.slots = 12, not a power of two) makes building the ingester fail, even
// with delta disabled. The error must not wait until someone turns the feature on.
// This is to ensure that log2(slots) produces a valid(non-decimal) index size.
func TestConfigDeltaInvalidFailsEvenWhenDisabled(t *testing.T) {
	cfg := deltaTestConfig(t, false)
	cfg.Delta.Sketch.Slots = 12
	target := &bundleTestStore{objects: map[bundleTestObject][]byte{}}
	if _, err := cfg.NewIngesterWithWriter(cfg.IngestKeyFunc(), nil, target); err == nil {
		t.Fatal("an invalid delta block must fail at construction, not on the day it is enabled")
	}
}

// TestConfigDeltaSharesOneEngineAcrossIngesters checks that ingesters built from
// the same Config share one delta engine. The first ingester stores a base image
// with no deltas. A second ingester must delta-encode an edited copy against all 8
// chunks, using the bases the first one registered, and record savings.
func TestConfigDeltaSharesOneEngineAcrossIngesters(t *testing.T) {
	cfg := deltaTestConfig(t, true)
	admission, err := cfg.WriteAdmission(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	target := &bundleTestStore{admission: admission, objects: map[bundleTestObject][]byte{}}
	v0 := deltaImage(1, 8)

	// Two independent ingesters from the same Config, as writeTransfer creates
	// per call: the second must find the bases the first registered.
	ing1, err := cfg.NewIngesterWithWriter(cfg.IngestKeyFunc(), nil, target)
	if err != nil {
		t.Fatal(err)
	}
	if r := ingestBytes(t, ing1, v0); r.DeltaChunks != 0 {
		t.Fatalf("first image: %+v", r)
	}
	ing2, err := cfg.NewIngesterWithWriter(cfg.IngestKeyFunc(), nil, target)
	if err != nil {
		t.Fatal(err)
	}
	r := ingestBytes(t, ing2, deltaEdited(v0, 5))
	if r.DeltaChunks != 8 {
		t.Fatalf("second ingester did not reuse the first one's bases: %+v", r)
	}
	if r.DeltaSavedBytes == 0 {
		t.Fatal("no savings recorded")
	}
}

// TestConfigDeltaOffIsInert checks that with delta disabled, ingesting an edited
// image reports no delta chunks and no delta attempts, and CloseDelta succeeds.
func TestConfigDeltaOffIsInert(t *testing.T) {
	cfg := deltaTestConfig(t, false)
	admission, _ := cfg.WriteAdmission(context.Background())
	target := &bundleTestStore{admission: admission, objects: map[bundleTestObject][]byte{}}
	ing, err := cfg.NewIngesterWithWriter(cfg.IngestKeyFunc(), nil, target)
	if err != nil {
		t.Fatal(err)
	}
	v0 := deltaImage(1, 6)
	ingestBytes(t, ing, v0)
	if r := ingestBytes(t, ing, deltaEdited(v0, 2)); r.DeltaChunks != 0 || r.DeltaAttempts != 0 {
		t.Fatalf("delta ran while disabled: %+v", r)
	}
	if err := cfg.CloseDelta(); err != nil {
		t.Fatal(err)
	}
}

// TestConfigDeltaDisabledForBundlesWhichStillVerify checks that bundles stay
// self-contained and verifiable even with delta enabled: the bundle ingester must
// silently produce ordinary objects. The test primes bases through a store
// ingester, then writes bundles of both images. Each must report zero delta chunks
// and zero delta attempts, and must pass FullVerify.
func TestConfigDeltaDisabledForBundlesWhichStillVerify(t *testing.T) {
	cfg := deltaTestConfig(t, true)
	ctx := context.Background()
	admission, err := cfg.WriteAdmission(ctx)
	if err != nil {
		t.Fatal(err)
	}
	customer, _ := cfg.CustomerKey()
	_, dec, _ := manifestcrypto.New(cfg.Crypto)

	v0 := deltaImage(1, 8)
	v1 := deltaEdited(v0, 3)

	// Prime the shared engine through a NON-bundle ingester so bases exist and a
	// bundle ingester would delta if it were allowed to.
	target := &bundleTestStore{admission: admission, objects: map[bundleTestObject][]byte{}}
	storeIng, _ := cfg.NewIngesterWithWriter(cfg.IngestKeyFunc(), nil, target)
	ingestBytes(t, storeIng, v0)

	for _, data := range [][]byte{v0, v1} {
		var encoded bytes.Buffer
		writer, err := bundle.NewWriter(&encoded, admission, bundle.WriterOptions{Concurrency: 3})
		if err != nil {
			t.Fatal(err)
		}
		bundleIng, err := cfg.NewBundleIngester(writer)
		if err != nil {
			t.Fatal(err)
		}
		res := ingestBytes(t, bundleIng, data)
		if res.DeltaChunks != 0 || res.DeltaAttempts != 0 {
			t.Fatalf("delta ran for a bundle: %+v", res)
		}
		if err := writer.Finalize(res.ManifestKey); err != nil {
			t.Fatal(err)
		}
		reader, err := bundle.NewReader(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()))
		if err != nil {
			t.Fatal(err)
		}
		if err := reader.FullVerify(ctx, res.ManifestKey, customer, dec, bundle.VerifyOptions{}); err != nil {
			t.Fatalf("bundle written with delta enabled does not verify: %v", err)
		}
		reader.Close()
	}
}

// TestConfigDeltaSnapshotPersistsOnClose checks that CloseDelta writes the index
// snapshot to the configured path and that the file is non-empty.
func TestConfigDeltaSnapshotPersistsOnClose(t *testing.T) {
	cfg := deltaTestConfig(t, true)
	cfg.Delta.Index.SnapshotPath = filepath.Join(t.TempDir(), "snap")
	admission, _ := cfg.WriteAdmission(context.Background())
	target := &bundleTestStore{admission: admission, objects: map[bundleTestObject][]byte{}}
	ing, err := cfg.NewIngesterWithWriter(cfg.IngestKeyFunc(), nil, target)
	if err != nil {
		t.Fatal(err)
	}
	v0 := deltaImage(1, 6)
	ingestBytes(t, ing, v0)
	ingestBytes(t, ing, deltaEdited(v0, 2))
	if err := cfg.CloseDelta(); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(cfg.Delta.Index.SnapshotPath)
	if err != nil || st.Size() == 0 {
		t.Fatalf("no snapshot written: %v", err)
	}
}
