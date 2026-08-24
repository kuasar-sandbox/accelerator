package ingest

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

var benchmarkIngestResult *Result

// BenchmarkIngestCanonicalCompression measures the complete unavoidable
// chunker + Snappy candidate + AES + physical hash path. The dedup case returns
// false from every Put to make the CPU cost of discovering an already-present
// content address visible instead of hiding it behind a storage fake.
func BenchmarkIngestCanonicalCompression(b *testing.B) {
	const inputSize = 8 << 20
	inputs := map[string][]byte{
		"raw":    ingestNoise(inputSize),
		"snappy": bytes.Repeat([]byte("snapshot-active-page\x00"), inputSize/21+1)[:inputSize],
	}
	chk, err := chunker.New(chunker.Config{
		Mode:  "fixed",
		Fixed: chunker.FixedConfig{Size: "524288"},
	})
	if err != nil {
		b.Fatal(err)
	}
	enc, _, err := crypto.New(crypto.Config{Chunk: "aes", Manifest: "aes"})
	if err != nil {
		b.Fatal(err)
	}

	for corpusName, input := range inputs {
		for _, tc := range []struct {
			name  string
			isNew bool
		}{
			{name: "new", isNew: true},
			{name: "dedup", isNew: false},
		} {
			b.Run(corpusName+"/"+tc.name, func(b *testing.B) {
				ing := NewIngester(testKeyFn, nil, benchmarkStore{isNew: tc.isNew}, chk, enc)
				warm, err := ing.Ingest(context.Background(), sparse.Dense(bytes.NewReader(input), uint64(len(input))), IngestOption{})
				if err != nil {
					b.Fatal(err)
				}
				if warm.LogicalChunkBytes != uint64(len(input)) || warm.EncodedChunkBytes == 0 {
					b.Fatalf("invalid warm compression stats: %+v", warm)
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(input)))
				samples := make([]int64, b.N)
				b.ResetTimer()
				for i := range b.N {
					started := time.Now()
					benchmarkIngestResult, err = ing.Ingest(
						context.Background(),
						sparse.Dense(bytes.NewReader(input), uint64(len(input))),
						IngestOption{},
					)
					if err != nil {
						b.Fatal(err)
					}
					samples[i] = time.Since(started).Nanoseconds()
				}
				b.StopTimer()
				sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
				b.ReportMetric(float64(warm.EncodedChunkBytes)/float64(warm.LogicalChunkBytes), "encoded/logical")
				b.ReportMetric(float64(ingestPercentileNanos(samples, 0.50)), "p50-ns")
				b.ReportMetric(float64(ingestPercentileNanos(samples, 0.95)), "p95-ns")
				b.ReportMetric(float64(ingestPercentileNanos(samples, 0.99)), "p99-ns")
			})
		}
	}
}

func ingestPercentileNanos(sorted []int64, percentile float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(percentile*float64(len(sorted)-1))]
}

type benchmarkStore struct {
	isNew bool
}

func (benchmarkStore) AdmitWrite(context.Context) (store.WriteAdmission, error) {
	return store.WriteAdmission{Generation: "benchmark", Salt: [32]byte{0x72}}, nil
}

func (s benchmarkStore) Put(context.Context, store.WriteAdmission, store.Partition, store.ContentKey, []byte) (bool, error) {
	return s.isNew, nil
}
