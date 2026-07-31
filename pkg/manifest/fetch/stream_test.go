package fetch

import (
	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
)

// newTestManifestStream exposes the package-private manifest stream constructor
// only to tests. Production callers must open manifests through Fetcher.
func newTestManifestStream(m *codec.Manifest, keys [][32]byte, getter cache.Getter, enc crypto.ChunkEncryptor) Stream {
	var onDemand, prefetch cache.Getter
	if getter != nil {
		client := newScheduledCacheClient(getter)
		onDemand = client.OnDemandGetter()
		prefetch = client.PrefetchGetter()
	}
	return newManifestStream(m, keys, onDemand, prefetch, enc)
}
