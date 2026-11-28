package delta

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/kuasar-sandbox/accelerator/internal/util"
)

// Config is the YAML-tagged `delta:` block. It follows chunker.Config's
// convention: sizes are human-readable strings, durations are Go duration
// strings, and every field has a documented default so an empty block is valid.
// Resolve turns it into validated numeric Params.
//
// Only Enabled is required to turn the feature on; everything else tunes it.
type Config struct {
	// Enabled is the master switch. False (the default) preserves the existing
	// pipeline exactly, including manifest determinism.
	Enabled bool `yaml:"enabled"`

	FeatureChunker FeatureChunkerConfig `yaml:"feature_chunker"`
	Sketch         SketchConfig         `yaml:"sketch"`
	Candidates     CandidatesConfig     `yaml:"candidates"`
	Index          IndexConfig          `yaml:"index"`
	BasePool       BasePoolConfig       `yaml:"base_pool"`
	Fetch          FetchConfig          `yaml:"fetch"`
}

// FetchConfig sizes the read path. Readers honour it whether or not delta
// compression is enabled for writes: an object written as a delta has to be
// readable.
type FetchConfig struct {
	// BaseCacheBytes bounds the shared plaintext cache of decoded bases. Default "64MiB".
	BaseCacheBytes string `yaml:"base_cache_bytes"`
	// ScratchBytes bounds concurrent delta-apply memory (base plaintext plus the
	// decoded VCDIFF stream), separate from the crypto scratch budget. Default "128MiB".
	ScratchBytes string `yaml:"scratch_bytes"`
	// MaxReadbacks caps concurrent store read-backs during ingest. Default 4.
	MaxReadbacks int `yaml:"max_readbacks"`
}

// FeatureChunkerConfig bounds the sub-chunking used only to compute similarity
// features. Unlike the storage chunker these sizes need not be page aligned.
type FeatureChunkerConfig struct {
	Min string `yaml:"min"` // default "256"
	Avg string `yaml:"avg"` // default "1KiB"
	Max string `yaml:"max"` // default "4KiB"
}

// SketchConfig selects the min-hash geometry. Both values are part of the
// sketch's identity and are recorded in snapshots.
type SketchConfig struct {
	// Slots is k, the number of min-hash slots per chunk. Power of two in [2, 64]. Default 16.
	Slots int `yaml:"slots"`
	// GroupSize is g, the number of slots folded into one super-feature. Must
	// divide Slots. s = Slots/GroupSize super-features per chunk. Default 2.
	GroupSize int `yaml:"group_size"`
}

// CandidatesConfig controls candidate selection and the per-chunk work budget.
type CandidatesConfig struct {
	// MinSimilarity is the estimated Jaccard threshold, in (0, 1]. Default 0.5.
	MinSimilarity float64 `yaml:"min_similarity"`
	// Max is how many candidates are fully tried with VCDIFF per chunk. Default 4.
	Max int `yaml:"max"`
	// AttemptDeadline is the wall-clock budget per chunk before falling back to
	// an independent object. Default "50ms".
	AttemptDeadline string `yaml:"attempt_deadline"`
	// MaxChunkSize: larger chunks are never delta encoded. Default "1MiB".
	MaxChunkSize string `yaml:"max_chunk_size"`
}

// IndexConfig sizes the soft-state indexes.
type IndexConfig struct {
	// MaxEntries caps similarity-index bases per dedup domain. 0 derives it
	// from base_pool.max_bytes (one entry per 64 KiB of pool, at least 1024).
	MaxEntries int `yaml:"max_entries"`
	// MaxPostings caps the posting list of any one super-feature. Default 64.
	MaxPostings int `yaml:"max_postings"`
	// MaxRepresentations caps representation-index entries per domain (LRU). Default 10,000,000.
	MaxRepresentations int `yaml:"max_representations"`
	// KnownCapacity sizes the per-domain Bloom filter of chunks already stored
	// independently, at ~10 bits per key. Default 8,000,000.
	KnownCapacity int `yaml:"known_capacity"`
	// MaxDomains caps how many dedup domains (salts) keep state at once. Default 4.
	MaxDomains int `yaml:"max_domains"`
	// VerifyHit asks the store (when it can answer Exists) to confirm a
	// representation-index hit before skipping the Put. Nil defaults to true.
	// Ignored, with the index trusted, when the store cannot be probed.
	VerifyHit *bool `yaml:"verify_hit"`
	// SnapshotPath persists the representation index and known-independent set
	// across restarts. Empty disables persistence.
	SnapshotPath string `yaml:"snapshot_path"`
	// SnapshotInterval rate-limits snapshots. Default "5m".
	SnapshotInterval string `yaml:"snapshot_interval"`
}

// BasePoolConfig sizes the in-memory base plaintext pool.
type BasePoolConfig struct {
	// MaxBytes bounds resident base plaintext. Default "4GiB".
	MaxBytes string `yaml:"max_bytes"`
}

// UnmarshalYAML decodes strictly: the repository's YAML loader is not strict
// (a misspelt key elsewhere is silently ignored), and for this block that would
// quietly run with a default the operator thought they had overridden.
func (c *Config) UnmarshalYAML(node *yaml.Node) error {
	raw, err := yaml.Marshal(node)
	if err != nil {
		return err
	}
	type plain Config // no UnmarshalYAML, so no recursion
	var p plain
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&p); err != nil {
		return fmt.Errorf("delta: %w", err)
	}
	*c = Config(p)
	return nil
}

// Limits on what a validated Config may ask for.
const (
	MaxSlots = 64
	// maxDomains is a sanity ceiling, not a tuning knob.
	maxDomainsCeiling = 1024
)

// Params is the validated, fully defaulted, numeric form of Config.
type Params struct {
	Enabled bool

	FeatMin, FeatAvg, FeatMax uint32

	Slots, GroupSize int

	MinSimilarity   float64
	MaxCandidates   int
	AttemptDeadline time.Duration
	MaxChunkSize    uint32

	MaxEntries         int
	MaxPostings        int
	MaxRepresentations int
	KnownCapacity      int
	MaxDomains         int
	VerifyHit          bool
	SnapshotPath       string
	SnapshotInterval   time.Duration

	PoolMaxBytes int64

	FetchBaseCache int64
	FetchScratch   int64
	MaxReadbacks   int
}

// SuperFeatures returns s = Slots/GroupSize.
func (p Params) SuperFeatures() int { return p.Slots / p.GroupSize }

// Fingerprint identifies every parameter that changes what a persisted index
// means. Snapshots carry it and are discarded on mismatch.
func (p Params) Fingerprint() [32]byte {
	h := sha256.New()
	h.Write([]byte("accelerator-delta-params/v1"))
	var b [8]byte
	for _, v := range []uint64{
		uint64(p.FeatMin), uint64(p.FeatAvg), uint64(p.FeatMax),
		uint64(p.Slots), uint64(p.GroupSize), uint64(p.KnownCapacity),
	} {
		binary.LittleEndian.PutUint64(b[:], v)
		h.Write(b[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// Resolve applies defaults and validates. It is cheap and has no side effects,
// so callers may call it repeatedly. A disabled config still validates: a
// typo should fail at startup, not on the day someone flips the switch.
func (c Config) Resolve() (Params, error) {
	var p Params
	p.Enabled = c.Enabled

	var err error
	size := func(field, s, def string, dst *uint64) {
		if err != nil {
			return
		}
		if strings.TrimSpace(s) == "" {
			s = def
		}
		v, perr := util.ParseSize(s)
		if perr != nil {
			err = fmt.Errorf("delta: %s: %w", field, perr)
			return
		}
		if v == 0 {
			err = fmt.Errorf("delta: %s must be > 0", field)
			return
		}
		*dst = v
	}
	var fmin, favg, fmax, maxChunk, poolBytes, baseCache, scratch uint64
	size("feature_chunker.min", c.FeatureChunker.Min, "256", &fmin)
	size("feature_chunker.avg", c.FeatureChunker.Avg, "1KiB", &favg)
	size("feature_chunker.max", c.FeatureChunker.Max, "4KiB", &fmax)
	size("candidates.max_chunk_size", c.Candidates.MaxChunkSize, "1MiB", &maxChunk)
	size("base_pool.max_bytes", c.BasePool.MaxBytes, "4GiB", &poolBytes)
	size("fetch.base_cache_bytes", c.Fetch.BaseCacheBytes, "64MiB", &baseCache)
	size("fetch.scratch_bytes", c.Fetch.ScratchBytes, "128MiB", &scratch)
	if err != nil {
		return Params{}, err
	}
	if !(fmin < favg && favg < fmax) {
		return Params{}, fmt.Errorf("delta: feature_chunker must satisfy min < avg < max (got %d/%d/%d)", fmin, favg, fmax)
	}
	if fmax > 1<<20 {
		return Params{}, fmt.Errorf("delta: feature_chunker.max %d exceeds 1MiB", fmax)
	}
	if favg < 2 {
		return Params{}, errors.New("delta: feature_chunker.avg too small")
	}
	p.FeatMin, p.FeatAvg, p.FeatMax = uint32(fmin), uint32(favg), uint32(fmax)

	if maxChunk > 64<<20 {
		return Params{}, fmt.Errorf("delta: candidates.max_chunk_size %d exceeds 64MiB", maxChunk)
	}
	p.MaxChunkSize = uint32(maxChunk)
	if poolBytes > 1<<50 {
		return Params{}, fmt.Errorf("delta: base_pool.max_bytes %d is implausibly large", poolBytes)
	}
	p.PoolMaxBytes = int64(poolBytes)
	if baseCache > 1<<40 || scratch > 1<<40 {
		return Params{}, fmt.Errorf("delta: fetch cache/scratch is implausibly large (%d/%d)", baseCache, scratch)
	}
	p.FetchBaseCache = int64(baseCache)
	p.FetchScratch = int64(scratch)
	p.MaxReadbacks = orDefault(c.Fetch.MaxReadbacks, 4)
	if p.MaxReadbacks < 1 || p.MaxReadbacks > 256 {
		return Params{}, fmt.Errorf("delta: fetch.max_readbacks must be in [1, 256] (got %d)", p.MaxReadbacks)
	}

	p.Slots = orDefault(c.Sketch.Slots, 16)
	p.GroupSize = orDefault(c.Sketch.GroupSize, 2)
	if p.Slots < 2 || p.Slots > MaxSlots || bits.OnesCount(uint(p.Slots)) != 1 {
		return Params{}, fmt.Errorf("delta: sketch.slots must be a power of two in [2, %d] (got %d)", MaxSlots, p.Slots)
	}
	if p.GroupSize < 1 || p.GroupSize > p.Slots || p.Slots%p.GroupSize != 0 {
		return Params{}, fmt.Errorf("delta: sketch.group_size %d must divide sketch.slots %d", p.GroupSize, p.Slots)
	}

	p.MinSimilarity = c.Candidates.MinSimilarity
	if p.MinSimilarity == 0 {
		p.MinSimilarity = 0.5
	}
	if !(p.MinSimilarity > 0 && p.MinSimilarity <= 1) {
		return Params{}, fmt.Errorf("delta: candidates.min_similarity must be in (0, 1] (got %v)", p.MinSimilarity)
	}
	if p.MaxCandidates = orDefault(c.Candidates.Max, 4); p.MaxCandidates < 1 || p.MaxCandidates > 64 {
		return Params{}, fmt.Errorf("delta: candidates.max must be in [1, 64] (got %d)", p.MaxCandidates)
	}
	if p.AttemptDeadline, err = parseDuration("candidates.attempt_deadline", c.Candidates.AttemptDeadline, 50*time.Millisecond); err != nil {
		return Params{}, err
	}

	if c.Index.MaxEntries < 0 {
		return Params{}, fmt.Errorf("delta: index.max_entries must be >= 0 (got %d)", c.Index.MaxEntries)
	}
	p.MaxEntries = c.Index.MaxEntries
	if p.MaxEntries == 0 {
		p.MaxEntries = int(max(int64(1024), p.PoolMaxBytes/(64<<10)))
	}
	p.MaxPostings = orDefault(c.Index.MaxPostings, 64)
	p.MaxRepresentations = orDefault(c.Index.MaxRepresentations, 10_000_000)
	p.KnownCapacity = orDefault(c.Index.KnownCapacity, 8_000_000)
	p.MaxDomains = orDefault(c.Index.MaxDomains, 4)
	for name, v := range map[string]int{
		"index.max_postings": p.MaxPostings, "index.max_representations": p.MaxRepresentations,
		"index.known_capacity": p.KnownCapacity, "index.max_domains": p.MaxDomains,
	} {
		if v < 1 {
			return Params{}, fmt.Errorf("delta: %s must be >= 1 (got %d)", name, v)
		}
	}
	if p.MaxDomains > maxDomainsCeiling {
		return Params{}, fmt.Errorf("delta: index.max_domains %d exceeds %d", p.MaxDomains, maxDomainsCeiling)
	}
	if p.KnownCapacity > 1<<30 {
		return Params{}, fmt.Errorf("delta: index.known_capacity %d exceeds %d", p.KnownCapacity, 1<<30)
	}
	p.VerifyHit = c.Index.VerifyHit == nil || *c.Index.VerifyHit
	p.SnapshotPath = c.Index.SnapshotPath
	if p.SnapshotInterval, err = parseDuration("index.snapshot_interval", c.Index.SnapshotInterval, 5*time.Minute); err != nil {
		return Params{}, err
	}
	return p, nil
}

func orDefault(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

func parseDuration(field, s string, def time.Duration) (time.Duration, error) {
	if strings.TrimSpace(s) == "" {
		return def, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("delta: %s: %w", field, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("delta: %s must be > 0 (got %s)", field, s)
	}
	return d, nil
}
