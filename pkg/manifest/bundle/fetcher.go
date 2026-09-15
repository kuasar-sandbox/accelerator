package bundle

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// ErrSourceUnavailable marks a Bundle ref that could not be resolved before a
// Manifest source was selected (for example a missing sibling file or an
// absent location mapping). Search may continue to later refs and remote.
// Existing malformed Bundles must return an ordinary error and fail closed.
var ErrSourceUnavailable = errors.New("manifest bundle: source unavailable")

// ManifestSource binds one Reader membership index to the Bundle-only Fetcher
// that must serve both the selected Manifest and all of its Chunks.
type ManifestSource struct {
	Reader  *Reader
	Fetcher fetch.Fetcher
	Ref     string

	diagnostics searchDiagnostics
}

// OpenManifest opens a key from this already-selected source while retaining
// aggregate search diagnostics for a remote failure.
func (s ManifestSource) OpenManifest(ctx context.Context, key store.ContentKey) (fetch.Stream, error) {
	if s.Fetcher == nil {
		return nil, fmt.Errorf("manifest bundle: selected source has no Fetcher for Manifest %s", hex.EncodeToString(key[:]))
	}
	if s.Reader != nil {
		if err := s.Reader.prepareChunks(ctx); err != nil {
			label := s.Ref
			if label == "" {
				label = "current Bundle"
			}
			return nil, fmt.Errorf("manifest bundle: selected %s failed to prepare Chunk index: %w", label, err)
		}
	}
	stream, err := s.Fetcher.OpenManifest(ctx, key)
	if err == nil {
		return stream, nil
	}
	if s.Reader != nil {
		label := s.Ref
		if label == "" {
			label = "current Bundle"
		}
		return nil, fmt.Errorf("manifest bundle: selected %s failed to open Manifest %s: %w", label, hex.EncodeToString(key[:]), err)
	}
	return nil, searchError(hex.EncodeToString(key[:]), s.diagnostics, err)
}

type searchDiagnostics struct {
	current     *Reader
	unavailable map[string]error
}

// SourceResolver resolves one canonical bundle/refs line. Path and location
// semantics deliberately belong to the caller. Implementations normally lazy
// open, cache, and eventually close each Reader; referenced Readers' own Refs
// are never consulted by ManifestFetcher.
type SourceResolver interface {
	ResolveBundle(ctx context.Context, ref string) (ManifestSource, error)
}

// SourceResolverFunc adapts a function to SourceResolver.
type SourceResolverFunc func(context.Context, string) (ManifestSource, error)

func (fn SourceResolverFunc) ResolveBundle(ctx context.Context, ref string) (ManifestSource, error) {
	return fn(ctx, ref)
}

// ManifestFetcher selects a source only at OpenManifest. A Bundle hit binds
// the complete Manifest/Chunk closure to that source; only clean misses or
// ErrSourceUnavailable advance the ordered search path.
type ManifestFetcher struct {
	current        *Reader
	currentFetcher fetch.Fetcher
	resolver       SourceResolver
	remote         fetch.Fetcher
}

// NewManifestFetcher retains the common current-Bundle/remote-only form.
func NewManifestFetcher(current *Reader, currentFetcher, remote fetch.Fetcher) *ManifestFetcher {
	return NewManifestFetcherWithResolver(current, currentFetcher, nil, remote)
}

// NewManifestFetcherWithResolver composes the current Bundle, its immutable
// ordered refs, and the default remote Fetcher. Referenced Bundle resolution is
// flat: a resolved Reader's own Refs are ignored.
func NewManifestFetcherWithResolver(current *Reader, currentFetcher fetch.Fetcher, resolver SourceResolver, remote fetch.Fetcher) *ManifestFetcher {
	return &ManifestFetcher{
		current:        current,
		currentFetcher: currentFetcher,
		resolver:       resolver,
		remote:         remote,
	}
}

// SelectRoot requires the root Manifest to be physically present in the
// current Bundle. It never consults refs or remote.
func (f *ManifestFetcher) SelectRoot(key store.ContentKey) (ManifestSource, error) {
	if f == nil || f.current == nil || !f.current.HasManifest(key) {
		return ManifestSource{}, fmt.Errorf("manifest bundle: root Manifest %s is absent from the current Bundle", hex.EncodeToString(key[:]))
	}
	if f.currentFetcher == nil {
		return ManifestSource{}, fmt.Errorf("manifest bundle: current Fetcher is unavailable for root Manifest %s", hex.EncodeToString(key[:]))
	}
	return ManifestSource{Reader: f.current, Fetcher: f.currentFetcher}, nil
}

// OpenRootManifest is the root-only counterpart of OpenManifest.
func (f *ManifestFetcher) OpenRootManifest(ctx context.Context, key store.ContentKey) (fetch.Stream, error) {
	source, err := f.SelectRoot(key)
	if err != nil {
		return nil, err
	}
	stream, err := source.OpenManifest(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("manifest bundle: open root Manifest %s from current Bundle: %w", hex.EncodeToString(key[:]), err)
	}
	return stream, nil
}

// SelectManifest applies current -> refs order -> remote source selection.
// The returned remote source has a nil Reader. A selected Bundle source is
// never replaced because its Fetcher later fails.
func (f *ManifestFetcher) SelectManifest(ctx context.Context, key store.ContentKey) (ManifestSource, error) {
	if err := ctx.Err(); err != nil {
		return ManifestSource{}, err
	}
	if f != nil && f.current != nil && f.current.HasManifest(key) {
		if f.currentFetcher == nil {
			return ManifestSource{}, fmt.Errorf("manifest bundle: current Fetcher is unavailable for Manifest %s", hex.EncodeToString(key[:]))
		}
		return ManifestSource{Reader: f.current, Fetcher: f.currentFetcher}, nil
	}

	var diagnostics searchDiagnostics
	if f != nil && f.current != nil {
		diagnostics.current = f.current
		for _, ref := range f.current.refsView() {
			if err := ctx.Err(); err != nil {
				return ManifestSource{}, err
			}
			if f.resolver == nil {
				if diagnostics.unavailable == nil {
					diagnostics.unavailable = make(map[string]error)
				}
				diagnostics.unavailable[ref] = errors.New("resolver unavailable")
				continue
			}
			source, err := f.resolver.ResolveBundle(ctx, ref)
			if err != nil {
				if errors.Is(err, ErrSourceUnavailable) {
					if diagnostics.unavailable == nil {
						diagnostics.unavailable = make(map[string]error)
					}
					diagnostics.unavailable[ref] = err
					continue
				}
				return ManifestSource{}, fmt.Errorf("manifest bundle: resolve %s for Manifest %s: %w", ref, hex.EncodeToString(key[:]), err)
			}
			if source.Reader == nil {
				return ManifestSource{}, fmt.Errorf("manifest bundle: resolver returned no Reader for %s", ref)
			}
			if !source.Reader.HasManifest(key) {
				continue
			}
			if source.Fetcher == nil {
				return ManifestSource{}, fmt.Errorf("manifest bundle: resolver returned no Fetcher for selected %s", ref)
			}
			source.Ref = ref
			return source, nil
		}
	}
	if f != nil && f.remote != nil {
		return ManifestSource{Fetcher: f.remote, Ref: "remote", diagnostics: diagnostics}, nil
	}
	return ManifestSource{}, searchError(hex.EncodeToString(key[:]), diagnostics, nil)
}

func (f *ManifestFetcher) OpenManifest(ctx context.Context, key store.ContentKey) (fetch.Stream, error) {
	source, err := f.SelectManifest(ctx, key)
	if err != nil {
		return nil, err
	}
	return source.OpenManifest(ctx, key)
}

func searchError(key string, diagnostics searchDiagnostics, remote error) error {
	detail := "no Bundle sources"
	if diagnostics.current != nil {
		parts := make([]string, 0, len(diagnostics.current.refsView())+1)
		parts = append(parts, "current Bundle: clean miss")
		for _, ref := range diagnostics.current.refsView() {
			if err := diagnostics.unavailable[ref]; err != nil {
				parts = append(parts, fmt.Sprintf("%s: %v", ref, err))
			} else {
				parts = append(parts, ref+": clean miss")
			}
		}
		detail = strings.Join(parts, "; ")
	}
	causes := make([]error, 0, len(diagnostics.unavailable)+1)
	if diagnostics.current != nil {
		for _, ref := range diagnostics.current.refsView() {
			if err := diagnostics.unavailable[ref]; err != nil {
				causes = append(causes, err)
			}
		}
	}
	message := fmt.Sprintf("manifest bundle: Manifest %s search failed (%s); remote Fetcher is unavailable", key, detail)
	if remote != nil {
		message = fmt.Sprintf("manifest bundle: Manifest %s search failed (%s); remote: %v", key, detail, remote)
		causes = append(causes, remote)
	} else if len(causes) == 0 {
		causes = append(causes, readerr.Mark(store.ErrNotFound, false))
	}
	err := error(&lookupError{message: message, causes: causes})
	if len(diagnostics.unavailable) != 0 && (remote == nil || onlyMissing(remote)) {
		// Remote absence does not establish absence in an unreachable ref.
		return readerr.Mark(err, true)
	}
	return err
}

// Only a single missing-cause chain can be made ambiguous by an unreachable
// ref. A joined error may contain a separate integrity failure.
func onlyMissing(err error) bool {
	for err != nil {
		if err == store.ErrNotFound {
			return true
		}
		if _, multiple := err.(interface{ Unwrap() []error }); multiple {
			return false
		}
		err = errors.Unwrap(err)
	}
	return false
}

// Preserve the existing ordered diagnostic without flattening error causes.
type lookupError struct {
	message string
	causes  []error
}

func (e *lookupError) Error() string   { return e.message }
func (e *lookupError) Unwrap() []error { return e.causes }
