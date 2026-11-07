package cache

import (
	"context"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// Keep this fixture compatible with the pre-reuse implementation so the same
// benchmark can compare the ordinary local-hit path on both revisions.
type benchmarkFillHitTier struct{ hitTier }

func (*benchmarkFillHitTier) StableFillTarget() {}

func BenchmarkTieredCacheLocalHit(b *testing.B) {
	for _, parallel := range []bool{false, true} {
		name := "serial"
		if parallel {
			name = "parallel"
		}
		b.Run(name, func(b *testing.B) {
			tier := &benchmarkFillHitTier{hitTier{data: make([]byte, 1024)}}
			tc := NewTieredCache(missGetter{}, tier)
			defer tc.Close()
			ctx := context.Background()
			key := store.ContentKey{1}
			b.ReportAllocs()
			read := func() {
				found, blob, err := tc.Get(ctx, store.PartitionChunk, key)
				if err != nil || !found || blob == nil {
					b.Errorf("local hit: found=%v, blob=%T, err=%v", found, blob, err)
					return
				}
				blob.Release()
			}
			if parallel {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						read()
					}
				})
			} else {
				for b.Loop() {
					read()
				}
			}
		})
	}
}
