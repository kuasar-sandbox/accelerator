package bundle

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// NewManifestFetcher composes already-distinct local and remote Fetchers. The
// Bundle is consulted only for Manifest membership; a local hit uses the local
// Fetcher for that Manifest and all of its Chunks, while a miss uses only the
// remote Fetcher. Local errors never fall back.
func NewManifestFetcher(reader *Reader, local, remote fetch.Fetcher) fetch.Fetcher {
	return &manifestFetcher{reader: reader, local: local, remote: remote}
}

type manifestFetcher struct {
	reader *Reader
	local  fetch.Fetcher
	remote fetch.Fetcher
}

func (f *manifestFetcher) OpenManifest(ctx context.Context, key store.ContentKey) (fetch.Stream, error) {
	if f.reader != nil && f.reader.HasManifest(key) {
		if f.local == nil {
			return nil, fmt.Errorf("manifest bundle: local Fetcher is unavailable for Manifest %s", hex.EncodeToString(key[:]))
		}
		return f.local.OpenManifest(ctx, key)
	}
	if f.remote == nil {
		return nil, fmt.Errorf("manifest bundle: Manifest %s is not in Bundle and no remote Fetcher is configured", hex.EncodeToString(key[:]))
	}
	return f.remote.OpenManifest(ctx, key)
}
