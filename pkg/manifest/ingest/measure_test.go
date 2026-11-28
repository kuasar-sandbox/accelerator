package ingest

import (
	"context"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/delta"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// TestDeltaCorpusMeasurements ingests one base image and four light edits of it,
// once with delta off and once with delta on, then reads the last image back
// through the real fetch path. It logs stored bytes, ingest speed, fetch latency,
// and how savings change as the similarity index is capped. It fails only when a
// delta image does not round-trip or delta does not reduce the bytes stored.
func TestDeltaCorpusMeasurements(t *testing.T) {
	const (
		chunks   = 16
		versions = 5
	)
	base := image(1, chunks)
	imgs := [][]byte{base}
	for v := 1; v < versions; v++ {
		imgs = append(imgs, editImage(imgs[v-1], int64(100+v)))
	}
	logical := 0
	for _, img := range imgs {
		logical += len(img)
	}

	off := measureCorpus(t, imgs, func(c *delta.Config) { c.Enabled = false })
	on := measureCorpus(t, imgs, func(c *delta.Config) {
		c.Candidates.AttemptDeadline = "500ms"
	})
	t.Logf("corpus: %d images x %d KiB, logical total %d KiB", versions, len(base)/1024, logical/1024)
	t.Logf("delta off: stored %d KiB in %s (%.1f MiB/s)", off.stored/1024, off.ingest, mebibytesPerSecond(logical, off.ingest))
	t.Logf("delta on:  stored %d KiB in %s (%.1f MiB/s) chunks=%d saved=%d KiB readbacks=%d",
		on.stored/1024, on.ingest, mebibytesPerSecond(logical, on.ingest), on.deltaChunks, on.saved/1024, on.readbacks)
	t.Logf("storage: delta stores %.1f%% of the delta-off bytes (%.1f%% saved)",
		100*float64(on.stored)/float64(off.stored), 100*float64(off.stored-on.stored)/float64(off.stored))
	t.Logf("ingest speed with delta, relative to delta off: %.2fx", float64(off.ingest)/float64(on.ingest))
	t.Logf("fetch off cold p50=%s p99=%s warm p50=%s p99=%s restore %s (%.1f MiB/s)",
		off.cold.p50, off.cold.p99, off.warm.p50, off.warm.p99, off.restore, mebibytesPerSecond(len(base), off.restore))
	t.Logf("fetch on  cold p50=%s p99=%s warm p50=%s p99=%s restore %s (%.1f MiB/s)",
		on.cold.p50, on.cold.p99, on.warm.p50, on.warm.p99, on.restore, mebibytesPerSecond(len(base), on.restore))

	t.Logf("index memory versus savings (logical record ~236 B/base, k=16 g=2):")
	for _, capEntries := range []int{1, 4, 16, 64} {
		r := measureCorpus(t, imgs, func(c *delta.Config) {
			c.Index.MaxEntries = capEntries
			c.Candidates.AttemptDeadline = "500ms"
		})
		t.Logf("  max_entries=%d bases=%d estIndex=%d B stored=%d KiB savedVsOff=%.1f%% deltas=%d",
			capEntries, r.bases, estIndexBytes(r.bases), r.stored/1024,
			100*float64(off.stored-r.stored)/float64(off.stored), r.deltaChunks)
	}
	rb := measureCorpus(t, imgs, func(c *delta.Config) {
		c.BasePool.MaxBytes = "80KiB"
		c.Index.MaxEntries = 64
		c.Candidates.AttemptDeadline = "500ms"
	})
	t.Logf("read-back (pool 80KiB, index holds the bases): stored %d KiB deltas=%d readbacks=%d savedVsOff=%.1f%%",
		rb.stored/1024, rb.deltaChunks, rb.readbacks, 100*float64(off.stored-rb.stored)/float64(off.stored))
	if on.stored >= off.stored {
		t.Fatalf("delta did not reduce stored bytes: off %d on %d", off.stored, on.stored)
	}
}

type corpusResult struct {
	stored      int
	saved       uint64
	deltaChunks uint32
	readbacks   uint32
	bases       int
	ingest      time.Duration
	cold, warm  latency
	restore     time.Duration
}

type latency struct {
	p50, p99 time.Duration
}

// measureCorpus ingests every image under one delta configuration, checks each
// reads back from the manifest, and times a fetch of the last image: one cold and
// one warm pass of 64 KiB reads, then one full restore.
func measureCorpus(t *testing.T, imgs [][]byte, mut func(*delta.Config)) corpusResult {
	t.Helper()
	st := newMemStore()
	eng := deltaEngine(t, mut)
	ing := newTestIngester(t, st, eng)
	var res corpusResult
	start := time.Now()
	var last *Result
	for _, img := range imgs {
		r := ingestImage(t, ing, img)
		res.saved += r.DeltaSavedBytes
		res.deltaChunks += r.DeltaChunks
		res.readbacks += r.DeltaReadbacks
		last = r
		if !bytesEqual(img, readBack(t, st, r.ManifestKey)) {
			t.Fatal("ingest read-back mismatch")
		}
	}
	res.ingest = time.Since(start)
	res.stored = st.newBytes
	res.bases = eng.Stats().SimilarityBases

	_, dec, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	f := fetch.NewFetcherWithOptions([32]byte{}, memGetter{st}, dec, fetch.Options{VerifyContent: true})
	if c, ok := f.(io.Closer); ok {
		defer c.Close()
	}
	stream, err := f.OpenManifest(context.Background(), last.ManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	// Time the last image: it is the one with deltas when delta is on.
	res.cold = timeChunkReads(t, stream, imgs[len(imgs)-1])
	res.warm = timeChunkReads(t, stream, imgs[len(imgs)-1])
	buf := make([]byte, len(imgs[len(imgs)-1]))
	t0 := time.Now()
	if _, err := stream.ReadAt(context.Background(), buf, 0); err != nil {
		t.Fatal(err)
	}
	res.restore = time.Since(t0)
	if !bytesEqual(buf, imgs[len(imgs)-1]) {
		t.Fatal("fetch restore mismatch")
	}
	return res
}

// timeChunkReads reads the image one 64 KiB chunk at a time and returns the median
// and 99th-percentile read latency.
func timeChunkReads(t *testing.T, stream fetch.Stream, img []byte) latency {
	t.Helper()
	var ds []time.Duration
	buf := make([]byte, deltaChunk)
	for off := uint64(0); off+deltaChunk <= uint64(len(img)); off += deltaChunk {
		start := time.Now()
		n, err := stream.ReadAt(context.Background(), buf, off)
		if err != nil || n != len(buf) {
			t.Fatalf("read at %d: %v n=%d", off, err, n)
		}
		ds = append(ds, time.Since(start))
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	return latency{p50: percentile(ds, 0.50), p99: percentile(ds, 0.99)}
}

// percentile returns the entry at fraction p of a sorted duration slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

// mebibytesPerSecond converts a byte count and a duration into MiB/s.
func mebibytesPerSecond(nbytes int, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(nbytes) / (1024 * 1024) / d.Seconds()
}

// estIndexBytes is the logical record of one base: object key, base key, size,
// 16-slot compact sketch, and 8 super-features with a posting pointer each.
func estIndexBytes(bases int) int {
	return bases * (32 + 32 + 4 + 16*2 + 8 + 8*8 + 8*8)
}

// bytesEqual reports whether two buffers have the same length and contents.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// memGetter serves the in-memory store through the cache.Getter interface the
// fetcher reads from.
type memGetter struct{ st *memStore }

func (g memGetter) Get(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	ok, b, err := g.st.Get(ctx, p, key)
	if err != nil {
		return 0, nil, err
	}
	if !ok {
		return cache.CacheMiss, nil, nil
	}
	return cache.CacheHit, cache.NewMemBlob(b), nil
}
