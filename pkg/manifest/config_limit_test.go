package manifest

import (
	"strconv"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
)

func TestNewIngesterRejectsChunkerAboveCanonicalDecodedLimit(t *testing.T) {
	cfg := &Config{
		Store: StoreConfig{Endpoint: "unused.sock"},
		Chunker: chunker.Config{
			Mode: "fixed",
			Fixed: chunker.FixedConfig{
				Size: strconv.FormatUint(uint64(codec.MaxChunkDecodedSize)+(4<<10), 10),
			},
		},
	}

	if _, err := cfg.NewIngester(nil, nil); err == nil {
		t.Fatal("NewIngester accepted a chunker above the canonical decoded limit")
	} else if !strings.Contains(err.Error(), "exceeds canonical decoded chunk limit") {
		t.Fatalf("NewIngester error = %v, want canonical decoded-limit error", err)
	}
}
