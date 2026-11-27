//go:build !no_rocksdb

package cache

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	pkgcache "github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/rocks"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

func TestBuildTieredChain_EmbeddedOnly(t *testing.T) {
	cfg := &runtime.Config{
		Mode:  "tiered",
		Tiers: []runtime.TierConfig{embeddedTier(t)},
	}
	comps, err := buildTieredChain(cfg, nil)
	if err != nil {
		t.Fatalf("buildTieredChain: %v", err)
	}
	defer comps.Close()

	if got := len(comps.Tiers); got != 1 {
		t.Fatalf("Tiers length: got %d, want 1", got)
	}
	if comps.Tiers[0] == nil {
		t.Fatal("Tiers[0] should be non-nil")
	}
	if comps.EmbeddedStore == nil {
		t.Fatal("EmbeddedStore should be non-nil for embedded tier")
	}
	if _, ok := comps.Tiers[0].(pkgcache.Semaphore); ok {
		t.Fatal("unlimited embedded tier should remain unwrapped")
	}
	// Sketch-construction invariant is covered by rocks package tests;
	// this test only verifies tier-chain assembly.
}

func TestBuildTieredChain_AppliesTierMaxInflight(t *testing.T) {
	tier := embeddedTier(t)
	tier.MaxInflight = 1
	cfg := &runtime.Config{Mode: "tiered", Tiers: []runtime.TierConfig{tier}}
	comps, err := buildTieredChain(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer comps.Close()
	if _, ok := comps.Tiers[0].(pkgcache.Semaphore); !ok {
		t.Fatalf("limited tier type %T does not implement pkgcache.Semaphore", comps.Tiers[0])
	}
	if comps.TierSpecs[0].EmbeddedStore == nil {
		t.Fatal("limiting the tier discarded its concrete Info reference")
	}
}

func TestBuildTieredChain_EmbeddedThenEC(t *testing.T) {
	cfg := &runtime.Config{
		Mode:  "tiered",
		Tiers: []runtime.TierConfig{embeddedTier(t), ecTier()},
	}
	comps, err := buildTieredChain(cfg, nil)
	if err != nil {
		t.Fatalf("buildTieredChain: %v", err)
	}
	defer comps.Close()

	if got := len(comps.Tiers); got != 2 {
		t.Fatalf("Tiers length: got %d, want 2", got)
	}
	if comps.TierSpecs[0].Type != "embedded" || comps.TierSpecs[1].Type != "ec" {
		t.Fatalf("tier order: got [%s, %s], want [embedded, ec]",
			comps.TierSpecs[0].Type, comps.TierSpecs[1].Type)
	}
}

// TestBuildTieredChain_ECThenEmbedded is the regression guard for the
// original bug: pre-refactor main.go unconditionally prepended the embedded
// store, collapsing `[ec, embedded]` and `[embedded, ec]` to the same chain.
// The new builder must honour YAML order verbatim.
func TestBuildTieredChain_ECThenEmbedded(t *testing.T) {
	cfg := &runtime.Config{
		Mode:  "tiered",
		Tiers: []runtime.TierConfig{ecTier(), embeddedTier(t)},
	}
	comps, err := buildTieredChain(cfg, nil)
	if err != nil {
		t.Fatalf("buildTieredChain: %v", err)
	}
	defer comps.Close()

	if got := len(comps.Tiers); got != 2 {
		t.Fatalf("Tiers length: got %d, want 2", got)
	}
	if comps.TierSpecs[0].Type != "ec" || comps.TierSpecs[1].Type != "embedded" {
		t.Fatalf("tier order: got [%s, %s], want [ec, embedded] (YAML-order regression)",
			comps.TierSpecs[0].Type, comps.TierSpecs[1].Type)
	}
}

// TestBuildTieredChain_DoubleEmbedded exercises the defensive duplicate-embedded
// guard in buildTieredChain (Validate normally catches this earlier, but the
// builder must be safe on its own).
func TestBuildTieredChain_DoubleEmbedded(t *testing.T) {
	cfg := &runtime.Config{
		Mode:  "tiered",
		Tiers: []runtime.TierConfig{embeddedTier(t), embeddedTier(t)},
	}
	comps, err := buildTieredChain(cfg, nil)
	if err == nil {
		t.Fatal("buildTieredChain should reject two embedded tiers")
	}
	if comps != nil {
		t.Fatal("comps should be nil on error")
	}
	if !strings.Contains(err.Error(), "more than one embedded") {
		t.Fatalf("error should mention duplicate embedded, got: %v", err)
	}
}

// TestBuildTieredChain_PartialFailureRollsBack asserts that when a later
// tier fails to construct, the earlier successfully-constructed tiers are
// Close()d (released). Reaching the second tier proves the embedded Open
// succeeded; its database must exist and be reopenable after rollback.
func TestBuildTieredChain_PartialFailureRollsBack(t *testing.T) {
	embedded := embeddedTier(t)
	cfg := &runtime.Config{
		Mode:  "tiered",
		Tiers: []runtime.TierConfig{embedded, badEcTier()},
	}
	comps, err := buildTieredChain(cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "tiers[1] (ec): create tier:") {
		t.Fatalf("want second-tier EC construction failure after embedded Open, got %v", err)
	}
	if comps != nil {
		t.Fatal("comps should be nil on partial failure")
	}

	// A fresh tmpdir must now contain a real RocksDB database. Absence is
	// not evidence of rollback: it could mean the first Open never happened.
	current, err := os.ReadFile(filepath.Join(embedded.Rocks.Path, "CURRENT"))
	if err != nil || len(current) == 0 {
		t.Fatalf("embedded tier did not create its database before rollback: CURRENT=%q err=%v", current, err)
	}
	// Reopening the same database must succeed; an omitted Close keeps the
	// exclusive RocksDB lock and makes this assertion fail.
	reopened, err := rocks.Open(*embedded.Rocks, runtime.FreqConfig{})
	if err != nil {
		t.Fatalf("rocks still locked after rollback: %v", err)
	}
	reopened.Close()
}
