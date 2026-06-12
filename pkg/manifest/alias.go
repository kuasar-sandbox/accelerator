package manifest

import (
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/manifest/ingest"
)

// Re-exports of the most commonly used types and functions from the
// subpackages so callers can write `manifest.Manifest` /
// `manifest.Stream` / `manifest.Unmarshal` without importing every
// subpackage. Types and functions live in their subpackages; these
// aliases are purely import-ergonomic.

type (
	Manifest   = codec.Manifest
	ChunkEntry = codec.ChunkEntry
	ChunkMode  = codec.ChunkMode
	Stream     = fetch.Stream

	IngestOption = ingest.IngestOption
	IngestResult = ingest.Result
)

// Constants — re-exported so the codec subpackage import is optional
// in call sites that only need the public surface.
const (
	ChunkModeFastCDC = codec.ChunkModeFastCDC
	ChunkModeFixed   = codec.ChunkModeFixed
	Version1         = codec.Version1
)

// Function re-exports for the codec layer. The facade owns the public
// surface; codec stays the implementation home but callers shouldn't
// have to know about it.
var (
	Marshal              = codec.Marshal
	Unmarshal            = codec.Unmarshal
	BuildAAD             = codec.BuildAAD
	UnsealKeys           = codec.UnsealKeys
	ChunkIndexForOffset  = codec.ChunkIndexForOffset
)
