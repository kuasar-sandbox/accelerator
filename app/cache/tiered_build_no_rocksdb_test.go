//go:build no_rocksdb

package cache

import (
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/rocks"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

func TestBuildTieredChain_EmbeddedNotCompiled(t *testing.T) {
	for _, afterEC := range []bool{false, true} {
		name := "first"
		index := "tiers[0] (embedded): open rocks:"
		tiers := []runtime.TierConfig{embeddedTier(t)}
		if afterEC {
			name = "after-ec"
			index = "tiers[1] (embedded): open rocks:"
			tiers = append([]runtime.TierConfig{ecTier()}, tiers...)
		}
		t.Run(name, func(t *testing.T) {
			comps, err := buildTieredChain(&runtime.Config{Mode: "tiered", Tiers: tiers}, nil)
			if comps != nil || !errors.Is(err, rocks.ErrNotCompiled) {
				t.Fatalf("buildTieredChain: comps=%v err=%v, want nil and ErrNotCompiled", comps, err)
			}
			if !strings.Contains(err.Error(), index) {
				t.Fatalf("unsupported embedded backend lost tier context: %v", err)
			}
		})
	}
}

func TestBuildTieredChain_RedisRollsBackWhenEmbeddedNotCompiled(t *testing.T) {
	err := requireRedisRollbackAfter(t, embeddedTier(t))
	if !errors.Is(err, rocks.ErrNotCompiled) || !strings.Contains(err.Error(), "tiers[1] (embedded): open rocks:") {
		t.Fatalf("want unsupported embedded backend after Redis construction, got %v", err)
	}
}
