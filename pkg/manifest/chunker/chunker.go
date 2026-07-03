// Package chunker splits an io.Reader stream into chunks suitable for
// content-addressable storage. Two modes are offered: content-defined
// chunking (FastCDC) for cross-image dedup against insertions/deletions,
// and fixed-size chunking for predictable layouts.
//
// Use via:
//
//	chk, err := chunker.New(chunker.Config{Mode: "cdc", CDC: chunker.CDCConfig{...}})
//	err = chk.Chunk(r, func(cr ChunkResult) error { ... })
//
// Config strings (sizes like "128KiB") are parsed once at New time; the
// returned Chunker holds resolved uint32 byte counts and has no
// per-Chunk validation cost.
package chunker

import (
	"fmt"
	"io"

	"github.com/kuasar-sandbox/accelerator/internal/util"
)

// PageSize is the alignment boundary for chunk boundaries (4 KiB).
const PageSize = 4096

// ChunkResult carries a single chunk's offset, size, and contents.
// IsZero is set when the chunk's payload is all zero bytes (the
// caller may skip storing it).
type ChunkResult struct {
	Offset uint64
	Size   uint32
	Data   []byte
	IsZero bool
}

// Callback is invoked for each chunk produced.
type Callback func(ChunkResult) error

// Config is the YAML-tagged chunking schema. Size fields accept the
// human-readable form (e.g. "128KiB", "1MiB", "4096"). Empty fields
// inherit defaults: Mode="cdc", CDC=(128K min / 512K avg / 1M max),
// Fixed=(512K).
type Config struct {
	Mode  string      `yaml:"mode"` // "cdc" (default) | "fixed"
	CDC   CDCConfig   `yaml:"cdc"`
	Fixed FixedConfig `yaml:"fixed"`
}

// CDCConfig holds the content-defined chunking parameters.
type CDCConfig struct {
	Min string `yaml:"min"`
	Avg string `yaml:"avg"`
	Max string `yaml:"max"`
}

// FixedConfig holds the fixed-size chunking parameter.
type FixedConfig struct {
	Size string `yaml:"size"`
}

// Chunker streams chunks from an io.Reader. Construct via New(Config);
// concrete impls are private. Info exposes the resolved mode + size
// bounds so downstream consumers (typically pkg/manifest/ingest) can
// embed them in the manifest header for the read path's fast-bounds
// check.
type Chunker interface {
	Chunk(r io.Reader, cb Callback) error
	Info() Info
}

// Info reports a Chunker's resolved configuration. Mode is "cdc" or
// "fixed" (matches Config.Mode after defaults). For fixed mode,
// MinSize == MaxSize == the resolved fixed size.
type Info struct {
	Mode    string
	MinSize uint32
	MaxSize uint32
}

// New resolves cfg's string fields into byte counts, applies defaults
// for any empty fields, and returns a Chunker ready to use. Returns
// an error if any size string is malformed or any resolved size is
// not a multiple of PageSize.
//
// Default values match docs/manifest.md §4.1:
//
//	mode: cdc
//	cdc:   min 128 KiB / avg 512 KiB / max 1 MiB
//	fixed: 512 KiB
func New(cfg Config) (Chunker, error) {
	mode := cfg.Mode
	if mode == "" {
		mode = "cdc"
	}
	switch mode {
	case "cdc":
		minSize, err := resolveSize(cfg.CDC.Min, 128*1024)
		if err != nil {
			return nil, fmt.Errorf("chunker: cdc.min: %w", err)
		}
		avgSize, err := resolveSize(cfg.CDC.Avg, 512*1024)
		if err != nil {
			return nil, fmt.Errorf("chunker: cdc.avg: %w", err)
		}
		maxSize, err := resolveSize(cfg.CDC.Max, 1024*1024)
		if err != nil {
			return nil, fmt.Errorf("chunker: cdc.max: %w", err)
		}
		for _, p := range []struct {
			name string
			val  uint32
		}{{"cdc.min", minSize}, {"cdc.avg", avgSize}, {"cdc.max", maxSize}} {
			if p.val%PageSize != 0 {
				return nil, fmt.Errorf("chunker: %s %d not %d-byte aligned", p.name, p.val, PageSize)
			}
		}
		return &cdcChunker{min: minSize, avg: avgSize, max: maxSize}, nil
	case "fixed":
		size, err := resolveSize(cfg.Fixed.Size, 512*1024)
		if err != nil {
			return nil, fmt.Errorf("chunker: fixed.size: %w", err)
		}
		if size%PageSize != 0 {
			return nil, fmt.Errorf("chunker: fixed.size %d not %d-byte aligned", size, PageSize)
		}
		return &fixedChunker{size: size}, nil
	default:
		return nil, fmt.Errorf("chunker: unknown mode %q (want cdc|fixed)", mode)
	}
}

func resolveSize(s string, fallback uint32) (uint32, error) {
	if s == "" {
		return fallback, nil
	}
	v, err := util.ParseSize(s)
	if err != nil {
		return 0, err
	}
	if v > 1<<32-1 {
		return 0, fmt.Errorf("size %s exceeds 4 GiB", s)
	}
	return uint32(v), nil
}

type cdcChunker struct{ min, avg, max uint32 }

func (c *cdcChunker) Chunk(r io.Reader, cb Callback) error {
	return chunkCDC(r, c.min, c.avg, c.max, cb)
}

func (c *cdcChunker) Info() Info {
	return Info{Mode: "cdc", MinSize: c.min, MaxSize: c.max}
}

type fixedChunker struct{ size uint32 }

func (c *fixedChunker) Chunk(r io.Reader, cb Callback) error {
	return chunkFixed(r, c.size, cb)
}

func (c *fixedChunker) Info() Info {
	return Info{Mode: "fixed", MinSize: c.size, MaxSize: c.size}
}
