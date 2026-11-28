package delta

import (
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// Checks that an empty Config resolves to the documented defaults: disabled; feature
// sizes 256/1KiB/4KiB; 16 slots with group size 2 (8 super-features); minimum
// similarity 0.5; 4 candidates; a 50ms deadline; a 1MiB chunk limit. A 4GiB pool
// gives 65,536 index entries. Verify is on, snapshots run every 5 minutes, and
// there are 4 domains and 64 postings.
func TestResolveDefaults(t *testing.T) {
	p, err := Config{}.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if p.Enabled {
		t.Error("must default to disabled")
	}
	if p.FeatMin != 256 || p.FeatAvg != 1024 || p.FeatMax != 4096 {
		t.Errorf("feature chunker defaults: %d/%d/%d", p.FeatMin, p.FeatAvg, p.FeatMax)
	}
	if p.Slots != 16 || p.GroupSize != 2 || p.SuperFeatures() != 8 {
		t.Errorf("sketch defaults: k=%d g=%d s=%d", p.Slots, p.GroupSize, p.SuperFeatures())
	}
	if p.MinSimilarity != 0.5 || p.MaxCandidates != 4 || p.AttemptDeadline != 50*time.Millisecond || p.MaxChunkSize != 1<<20 {
		t.Errorf("candidate defaults: %+v", p)
	}
	if p.PoolMaxBytes != 4<<30 || p.MaxEntries != (4<<30)/(64<<10) {
		t.Errorf("pool defaults: bytes=%d entries=%d", p.PoolMaxBytes, p.MaxEntries)
	}
	if !p.VerifyHit || p.SnapshotInterval != 5*time.Minute || p.MaxDomains != 4 || p.MaxPostings != 64 {
		t.Errorf("index defaults: %+v", p)
	}
}

// Checks that each invalid config is rejected with an error. Cases cover sketch
// shape, similarity outside (0, 1], candidate count and deadline, feature size
// order and limits, chunk and pool size limits, negative index values, too many
// domains, a known_capacity above 2^30, and a bad snapshot interval. Each case
// starts from an empty Config and changes one field.
func TestResolveRejectsInvalid(t *testing.T) {
	f := false
	_ = f
	tests := map[string]func(*Config){
		"slots not power of two":   func(c *Config) { c.Sketch.Slots = 12 },
		"slots too large":          func(c *Config) { c.Sketch.Slots = 128 },
		"slots negative":           func(c *Config) { c.Sketch.Slots = -2 },
		"group not dividing":       func(c *Config) { c.Sketch.Slots = 16; c.Sketch.GroupSize = 3 },
		"group larger than slots":  func(c *Config) { c.Sketch.Slots = 4; c.Sketch.GroupSize = 8 },
		"similarity above one":     func(c *Config) { c.Candidates.MinSimilarity = 1.5 },
		"similarity negative":      func(c *Config) { c.Candidates.MinSimilarity = -0.1 },
		"candidates too many":      func(c *Config) { c.Candidates.Max = 1000 },
		"candidates negative":      func(c *Config) { c.Candidates.Max = -1 },
		"bad deadline":             func(c *Config) { c.Candidates.AttemptDeadline = "soon" },
		"negative deadline":        func(c *Config) { c.Candidates.AttemptDeadline = "-5s" },
		"feature order":            func(c *Config) { c.FeatureChunker = FeatureChunkerConfig{Min: "1KiB", Avg: "1KiB", Max: "4KiB"} },
		"feature size garbage":     func(c *Config) { c.FeatureChunker.Avg = "lots" },
		"feature zero":             func(c *Config) { c.FeatureChunker.Min = "0" },
		"feature max too big":      func(c *Config) { c.FeatureChunker.Max = "2MiB" },
		"max chunk size garbage":   func(c *Config) { c.Candidates.MaxChunkSize = "x" },
		"max chunk size too big":   func(c *Config) { c.Candidates.MaxChunkSize = "1GiB" },
		"pool garbage":             func(c *Config) { c.BasePool.MaxBytes = "4 gigs" },
		"max entries negative":     func(c *Config) { c.Index.MaxEntries = -1 },
		"postings negative":        func(c *Config) { c.Index.MaxPostings = -1 },
		"domains too many":         func(c *Config) { c.Index.MaxDomains = 100000 },
		"known capacity too large": func(c *Config) { c.Index.KnownCapacity = 1 << 31 },
		"bad snapshot interval":    func(c *Config) { c.Index.SnapshotInterval = "5 minutes" },
	}
	for name, mut := range tests {
		var c Config
		mut(&c)
		if _, err := c.Resolve(); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

// Checks that validation runs even when enabled is false, so a typo fails at
// startup rather than on the day the feature is switched on.
func TestResolveValidatesWhenDisabled(t *testing.T) {
	c := Config{Enabled: false}
	c.Sketch.Slots = 12
	if _, err := c.Resolve(); err == nil {
		t.Fatal("a typo must fail at startup even while the feature is off")
	}
}

// Checks that an explicit verify_hit: false in YAML reaches Params as false. An
// omitted key keeps verify on; TestResolveDefaults covers that.
func TestVerifyHitExplicitFalse(t *testing.T) {
	var c Config
	if err := yaml.Unmarshal([]byte("index:\n  verify_hit: false\n"), &c); err != nil {
		t.Fatal(err)
	}
	p, err := c.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if p.VerifyHit {
		t.Fatal("explicit verify_hit: false must be honoured")
	}
}

// Checks that a full, valid YAML document decodes into every Config field and
// resolves to the expected Params. Then checks that an unknown top-level key, an
// unknown nested key, and a wrong value type each fail to decode, with the error
// attributed to delta.
func TestYAMLStrictDecode(t *testing.T) {
	good := `
enabled: true
feature_chunker: {min: "512", avg: "2KiB", max: "8KiB"}
sketch: {slots: 32, group_size: 1}
candidates: {min_similarity: 0.4, max: 3, attempt_deadline: "10ms", max_chunk_size: "512KiB"}
index: {max_entries: 100, max_postings: 8, max_representations: 50, known_capacity: 1000, max_domains: 2, snapshot_path: "/tmp/x", snapshot_interval: "1s"}
base_pool: {max_bytes: "128MiB"}
`
	var c Config
	if err := yaml.Unmarshal([]byte(good), &c); err != nil {
		t.Fatal(err)
	}
	p, err := c.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if !p.Enabled || p.FeatAvg != 2048 || p.Slots != 32 || p.GroupSize != 1 || p.MinSimilarity != 0.4 ||
		p.MaxCandidates != 3 || p.MaxChunkSize != 512<<10 || p.MaxEntries != 100 || p.PoolMaxBytes != 128<<20 ||
		p.SnapshotInterval != time.Second || p.SnapshotPath != "/tmp/x" {
		t.Fatalf("unexpected params: %+v", p)
	}

	for name, doc := range map[string]string{
		"unknown top-level key": "enabld: true\n",
		"unknown nested key":    "sketch:\n  slotz: 16\n",
		"wrong type":            "sketch:\n  slots: many\n",
	} {
		var c Config
		err := yaml.Unmarshal([]byte(doc), &c)
		if err == nil {
			t.Errorf("%s: expected a decode error", name)
		} else if !strings.Contains(err.Error(), "delta") {
			t.Errorf("%s: error should be attributed to delta: %v", name, err)
		}
	}
}

// Checks that delta: stays strict when nested inside a larger document, which is
// how manifest.Config embeds it. A valid delta block decodes, and an unknown key
// inside it is rejected.
func TestYAMLStrictWhenNested(t *testing.T) {
	type outer struct {
		Delta Config `yaml:"delta"`
		Other string `yaml:"other"`
	}
	var o outer
	if err := yaml.Unmarshal([]byte("other: x\ndelta:\n  enabled: true\n"), &o); err != nil || !o.Delta.Enabled || o.Other != "x" {
		t.Fatalf("nested decode failed: %v %+v", err, o)
	}
	if err := yaml.Unmarshal([]byte("delta:\n  bogus: 1\n"), &o); err == nil {
		t.Fatal("nested unknown key must be rejected")
	}
}

// Checks the snapshot fingerprint. It is stable across identical configs. Changing
// slots, group size, feature average, or known_capacity changes it, because those
// change what persisted state means. Changing candidate tuning leaves it the same,
// so tuning changes keep existing snapshots valid.
func TestFingerprint(t *testing.T) {
	base := testParams(t, nil)
	same := testParams(t, nil)
	if base.Fingerprint() != same.Fingerprint() {
		t.Fatal("fingerprint not stable")
	}
	for name, mut := range map[string]func(*Config){
		"slots":     func(c *Config) { c.Sketch.Slots = 32 },
		"group":     func(c *Config) { c.Sketch.GroupSize = 4 },
		"feat avg":  func(c *Config) { c.FeatureChunker.Avg = "2KiB" },
		"known cap": func(c *Config) { c.Index.KnownCapacity = 1 << 15 },
	} {
		if testParams(t, mut).Fingerprint() == base.Fingerprint() {
			t.Errorf("%s must change the fingerprint", name)
		}
	}
	// Parameters that do not change what persisted state means must not.
	other := testParams(t, func(c *Config) { c.Candidates.Max = 9; c.Candidates.MinSimilarity = 0.7 })
	if other.Fingerprint() != base.Fingerprint() {
		t.Error("candidate tuning must not invalidate snapshots")
	}
}
