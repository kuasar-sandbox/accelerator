package bundle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type benchmarkRemote struct{}

func (benchmarkRemote) OpenManifest(context.Context, store.ContentKey) (fetch.Stream, error) {
	return nil, errors.New("benchmark remote miss")
}

func BenchmarkManifestSourceSearch(b *testing.B) {
	for _, refsCount := range []int{0, 1, 8, 32} {
		refs := make([]string, refsCount)
		for index := range refs {
			refs[index] = fmt.Sprintf("file://%064x.bundle", index+1)
		}
		currentData := syntheticBundleWithRefs(b, 0, 0, refs)
		current, err := NewReader(bytes.NewReader(currentData), int64(len(currentData)))
		if err != nil {
			b.Fatal(err)
		}
		missData := syntheticBundle(b, 0, 0)
		miss, err := NewReader(bytes.NewReader(missData), int64(len(missData)))
		if err != nil {
			b.Fatal(err)
		}
		resolver := SourceResolverFunc(func(context.Context, string) (ManifestSource, error) {
			return ManifestSource{Reader: miss}, nil
		})
		key := store.ContentKey{0xff}
		if refsCount == 0 {
			currentKey := current.ManifestKeys()[0]
			b.Run("refs_0/current_hit", func(b *testing.B) {
				fetcher := NewManifestFetcher(current, benchmarkRemote{}, benchmarkRemote{})
				b.ReportAllocs()
				for range b.N {
					if _, err := fetcher.SelectManifest(context.Background(), currentKey); err != nil {
						b.Fatal(err)
					}
				}
			})
		}

		b.Run(fmt.Sprintf("refs_%d/clean_miss", refsCount), func(b *testing.B) {
			fetcher := NewManifestFetcherWithResolver(current, nil, resolver, nil)
			b.ReportAllocs()
			for range b.N {
				if _, err := fetcher.SelectManifest(context.Background(), key); err == nil {
					b.Fatal("clean miss unexpectedly selected a source")
				}
			}
		})
		b.Run(fmt.Sprintf("refs_%d/tail_store_fallback", refsCount), func(b *testing.B) {
			fetcher := NewManifestFetcherWithResolver(current, nil, resolver, benchmarkRemote{})
			b.ReportAllocs()
			for range b.N {
				if _, err := fetcher.OpenManifest(context.Background(), key); err == nil {
					b.Fatal("remote miss unexpectedly opened a Manifest")
				}
			}
		})
		b.Run(fmt.Sprintf("refs_%d/tail_store_select", refsCount), func(b *testing.B) {
			fetcher := NewManifestFetcherWithResolver(current, nil, resolver, benchmarkRemote{})
			b.ReportAllocs()
			for range b.N {
				if source, err := fetcher.SelectManifest(context.Background(), key); err != nil || source.Reader != nil {
					b.Fatalf("SelectManifest = %#v, %v", source, err)
				}
			}
		})
		_ = current.Close()
		_ = miss.Close()
	}
}
