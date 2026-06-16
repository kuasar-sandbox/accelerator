package remote

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/klauspost/compress/zstd"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/flatten"
)

// Resolved is a registry reference pinned to a concrete, platform-selected
// image-manifest digest.
type Resolved struct {
	Ref    name.Reference  // the reference as given (tag or digest)
	Repo   name.Repository // owning repository (where referrers live)
	Digest name.Digest     // repo@sha256:<image-manifest-digest>
	Hash   v1.Hash         // the image-manifest digest
}

// LooksLikeReference reports whether s parses as a registry image reference.
// flatten-ctl uses it (after ruling out stdin and an on-disk file) to decide
// a positional argument is a remote pull rather than a local docker-archive.
func LooksLikeReference(s string) bool {
	_, err := name.ParseReference(s)
	return err == nil
}

func nameOpts(insecure bool) []name.Option {
	if insecure {
		return []name.Option{name.Insecure}
	}
	return nil
}

func (c *Config) remoteOpts(ctx context.Context) []ggcrremote.Option {
	opts := []ggcrremote.Option{
		ggcrremote.WithContext(ctx),
		ggcrremote.WithAuth(authenticator()),
		ggcrremote.WithPlatform(c.platform),
		ggcrremote.WithJobs(c.PullJobs),
	}
	// A custom transport (TLS CA / skip-verify) must apply to every request,
	// including the CDN blob redirects where MITM proxies swap the cert.
	if c.transport != nil {
		opts = append(opts, ggcrremote.WithTransport(c.transport))
	}
	return opts
}

// Resolve parses ref, contacts the registry, selects the configured platform
// from an index (or takes the single manifest), and returns the pinned
// image-manifest digest. Resolving a moving tag here is what lets callers
// print and persist an immutable repo@sha256 for reproducibility.
func (c *Config) Resolve(ctx context.Context, ref string) (*Resolved, error) {
	r, err := name.ParseReference(ref, nameOpts(c.Insecure)...)
	if err != nil {
		return nil, fmt.Errorf("remote: parse ref %q: %w", ref, err)
	}
	desc, err := ggcrremote.Get(r, c.remoteOpts(ctx)...)
	if err != nil {
		return nil, fmt.Errorf("remote: resolve %q: %w", ref, err)
	}
	img, err := desc.Image()
	if err != nil {
		return nil, fmt.Errorf("remote: select platform %s for %q: %w", c.platform, ref, err)
	}
	d, err := img.Digest()
	if err != nil {
		return nil, fmt.Errorf("remote: image digest %q: %w", ref, err)
	}
	return &Resolved{
		Ref:    r,
		Repo:   r.Context(),
		Digest: r.Context().Digest(d.String()),
		Hash:   d,
	}, nil
}

// Pull ensures every blob of the resolved image (config + layers) is present
// in cache — downloading missing ones concurrently (bounded by PullJobs),
// each digest-verified and written atomically — then returns a flatten.Source
// that streams the layers (decompressed per their media type) from cache.
// Downloads run in parallel; flatten then applies layers sequentially.
func (c *Config) Pull(ctx context.Context, res *Resolved, cache *Cache) (flatten.Source, error) {
	img, err := ggcrremote.Image(res.Digest, c.remoteOpts(ctx)...)
	if err != nil {
		return nil, fmt.Errorf("remote: pull %s: %w", res.Digest, err)
	}
	mfst, err := img.Manifest()
	if err != nil {
		return nil, fmt.Errorf("remote: manifest %s: %w", res.Digest, err)
	}

	// Config blob — also kept in memory, since it feeds the config projection.
	cfgBytes, err := img.RawConfigFile()
	if err != nil {
		return nil, fmt.Errorf("remote: config %s: %w", res.Digest, err)
	}
	if !cache.Has(mfst.Config.Digest) {
		if _, err := cache.Put(mfst.Config.Digest, io.NopCloser(bytes.NewReader(cfgBytes))); err != nil {
			return nil, err
		}
	}

	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("remote: layers %s: %w", res.Digest, err)
	}

	// Pass 1: resolve every layer's digest + media type and note which ones
	// are cache misses, so the download progress total is known up front.
	refs := make([]layerRef, len(layers))
	var misses []int
	for i, l := range layers {
		d, err := l.Digest()
		if err != nil {
			return nil, fmt.Errorf("remote: layer digest: %w", err)
		}
		mt, err := l.MediaType()
		if err != nil {
			return nil, fmt.Errorf("remote: layer media type: %w", err)
		}
		refs[i] = layerRef{digest: d, mt: mt}
		if !cache.Has(d) {
			misses = append(misses, i)
		}
	}

	// Pass 2: download the misses concurrently (bounded by PullJobs),
	// reporting each completion through OnPullProgress.
	toPull := len(misses)
	sem := make(chan struct{}, c.PullJobs)
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		errVal error
		done   int
	)
	record := func(err error) {
		mu.Lock()
		if errVal == nil {
			errVal = err
		}
		mu.Unlock()
	}
	for _, i := range misses {
		l := layers[i]
		d := refs[i].digest
		wg.Add(1)
		sem <- struct{}{}
		go func(l v1.Layer, d v1.Hash) {
			defer wg.Done()
			defer func() { <-sem }()
			rc, err := l.Compressed() // digest-verified reader from the registry
			if err != nil {
				record(fmt.Errorf("remote: fetch %s: %w", d, err))
				return
			}
			if _, err := cache.Put(d, rc); err != nil {
				record(err)
				return
			}
			mu.Lock()
			done++
			n := done
			mu.Unlock()
			if c.OnPullProgress != nil {
				c.OnPullProgress(n, toPull)
			}
		}(l, d)
	}
	wg.Wait()
	if errVal != nil {
		return nil, errVal
	}

	return &registrySource{cache: cache, configJSON: cfgBytes, layers: refs}, nil
}

type layerRef struct {
	digest v1.Hash
	mt     types.MediaType
}

// registrySource is a flatten.Source backed by cached compressed layer blobs.
type registrySource struct {
	cache      *Cache
	configJSON []byte
	layers     []layerRef
}

func (s *registrySource) ConfigJSON() ([]byte, error) { return s.configJSON, nil }

func (s *registrySource) Layers() ([]flatten.LayerOpener, error) {
	openers := make([]flatten.LayerOpener, 0, len(s.layers))
	for _, lr := range s.layers {
		lr := lr
		openers = append(openers, func() (io.ReadCloser, error) {
			f, err := s.cache.Get(lr.digest)
			if err != nil {
				return nil, fmt.Errorf("remote: open cached layer %s: %w", lr.digest, err)
			}
			dr, err := decompress(f, lr.mt)
			if err != nil {
				f.Close()
				return nil, err
			}
			return &multiCloser{Reader: dr, closers: []io.Closer{dr, f}}, nil
		})
	}
	return openers, nil
}

// decompress wraps r with the decompressor implied by the layer media type,
// yielding a plain (uncompressed) tar stream. gzip and zstd are handled
// explicitly; anything else is assumed already-uncompressed tar.
func decompress(r io.Reader, mt types.MediaType) (io.ReadCloser, error) {
	s := string(mt)
	switch {
	case strings.HasSuffix(s, "gzip"):
		gz, err := gzip.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("remote: gzip layer: %w", err)
		}
		return gz, nil
	case strings.HasSuffix(s, "zstd"):
		zr, err := zstd.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("remote: zstd layer: %w", err)
		}
		return zr.IOReadCloser(), nil
	default:
		return io.NopCloser(r), nil
	}
}

// multiCloser reads from Reader and closes its closers in order.
type multiCloser struct {
	io.Reader
	closers []io.Closer
}

func (m *multiCloser) Close() error {
	var first error
	for _, c := range m.closers {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
