package remote

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/random"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"
)

type pullTestTransport func(*http.Request) (*http.Response, error)

func (f pullTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Exercise the real SDK transport, redirects, reads and atomic cache. Failures
// before headers exhaust all three SDK attempts; body failures bypass them.
func TestPullRecoversNetworkFailures(t *testing.T) {
	for _, tc := range []struct {
		mode            string
		configReads     int32
		layerReads      int32
		otherLayerReads int32
	}{
		{mode: "dns"},
		{mode: "obs_dial", configReads: 2},
		{mode: "config_body", configReads: 2},
		{mode: "layer_body", layerReads: 2, otherLayerReads: 1},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			mode := tc.mode
			img, err := random.Image(2048, 2)
			if err != nil {
				t.Fatal(err)
			}
			mfst, _ := img.Manifest()
			configPath := "/v2/test/app/blobs/" + mfst.Config.Digest.String()
			layerPath := "/v2/test/app/blobs/" + mfst.Layers[0].Digest.String()
			var armed atomic.Bool
			var recoverDNS atomic.Bool
			var configReads, layerReads, otherLayerReads, wireFailures atomic.Int32
			handler := registry.New()
			obs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler.ServeHTTP(w, r) }))
			defer obs.Close()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if armed.Load() && r.Method == http.MethodGet {
					isConfig := r.URL.Path == configPath
					isLayer := r.URL.Path == layerPath
					if isConfig {
						configReads.Add(1)
					}
					if isLayer {
						layerReads.Add(1)
					}
					if strings.HasSuffix(r.URL.Path, mfst.Layers[1].Digest.String()) {
						otherLayerReads.Add(1)
					}
					if mode == "obs_dial" && isConfig {
						http.Redirect(w, r, obs.URL+r.URL.Path, http.StatusTemporaryRedirect)
						return
					}
					if (mode == "config_body" && isConfig && configReads.Load() == 1) || (mode == "layer_body" && isLayer && layerReads.Load() == 1) {
						rr := httptest.NewRecorder()
						handler.ServeHTTP(rr, r)
						for k, vs := range rr.Header() {
							for _, v := range vs {
								w.Header().Add(k, v)
							}
						}
						w.WriteHeader(rr.Code)
						b := rr.Body.Bytes()
						w.Write(b[:len(b)/2])
						return
					}
				}
				handler.ServeHTTP(w, r)
			}))
			defer srv.Close()
			ref := strings.TrimPrefix(srv.URL, "http://") + "/test/app:v1"
			pushImage(t, ref, img)
			c := testConfig(t)
			var mu sync.Mutex
			var events []PullRetryEvent
			c.OnPullRetry = func(e PullRetryEvent) { mu.Lock(); events = append(events, e); mu.Unlock(); recoverDNS.Store(true) }
			base := ggcrremote.DefaultTransport.(*http.Transport).Clone()
			defer base.CloseIdleConnections()
			c.transport = pullTestTransport(func(r *http.Request) (*http.Response, error) {
				if armed.Load() && ((mode == "dns" && r.URL.Path == "/v2/") || (mode == "obs_dial" && r.URL.Host == strings.TrimPrefix(obs.URL, "http://"))) {
					if n := wireFailures.Add(1); (mode == "dns" && !recoverDNS.Load()) || (mode == "obs_dial" && n <= 3) {
						if mode == "dns" {
							return nil, &net.DNSError{Err: "i/o timeout", Name: r.URL.Host, IsTimeout: true, IsTemporary: true}
						}
						return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ETIMEDOUT}
					}
				}
				return base.RoundTrip(r)
			})
			armed.Store(true)
			res, err := c.Resolve(context.Background(), ref)
			if err != nil {
				t.Fatal(err)
			}
			cache, err := OpenCache(c.CacheDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			src, err := c.Pull(context.Background(), res, cache)
			if err != nil {
				t.Fatal(err)
			}
			got, _ := src.ConfigJSON()
			want, _ := img.RawConfigFile()
			if !bytes.Equal(got, want) {
				t.Fatalf("partial/wrong config: got %d bytes want %d", len(got), len(want))
			}
			for _, d := range mfst.Layers {
				if !cache.Has(d.Digest) {
					t.Fatalf("missing layer %s", d.Digest)
				}
			}
			layers, _ := img.Layers()
			rc, _ := layers[0].Compressed()
			expected, _ := io.ReadAll(rc)
			rc.Close()
			rc, err = cache.Get(mfst.Layers[0].Digest)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := io.ReadAll(rc)
			rc.Close()
			if err != nil || !bytes.Equal(actual, expected) {
				t.Fatal("incorrect recovered layer")
			}
			leftovers, _ := filepath.Glob(filepath.Join(c.CacheDir(), "blobs", "sha256", ".tmp-*"))
			if len(leftovers) != 0 {
				t.Fatalf("partial files remain: %v", leftovers)
			}
			if len(events) == 0 {
				t.Fatalf("no outer retry observed; wire failures=%d", wireFailures.Load())
			}
			if tc.configReads > 0 && configReads.Load() != tc.configReads {
				t.Fatalf("config requests=%d, want %d", configReads.Load(), tc.configReads)
			}
			if tc.layerReads > 0 && (layerReads.Load() != tc.layerReads || otherLayerReads.Load() != tc.otherLayerReads) {
				t.Fatalf("layer requests=%d other=%d, want %d and %d", layerReads.Load(), otherLayerReads.Load(), tc.layerReads, tc.otherLayerReads)
			}
		})
	}
}

func TestPullCacheLocalFailureDoesNotRetry(t *testing.T) {
	host := startRegistry(t)
	img, err := random.Image(128, 1)
	if err != nil {
		t.Fatal(err)
	}
	ref := host + "/test/local:v1"
	pushImage(t, ref, img)
	c := testConfig(t)
	c.OnPullRetry = func(e PullRetryEvent) { t.Errorf("retried local error: %+v", e) }
	res, err := c.Resolve(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := OpenCache(c.CacheDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(c.CacheDir(), "blobs"), 0700); err != nil {
		t.Fatal(err)
	}
	// Make the sha256 directory impossible to create, even when running as root.
	if err := os.WriteFile(filepath.Join(c.CacheDir(), "blobs", "sha256"), []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Pull(context.Background(), res, cache); err == nil {
		t.Fatal("expected local cache failure")
	}
}

func TestPullLayerCacheLocalFailureDoesNotRetry(t *testing.T) {
	img, err := random.Image(128, 1)
	if err != nil {
		t.Fatal(err)
	}
	mfst, err := img.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	layerPath := "/v2/test/local/blobs/" + mfst.Layers[0].Digest.String()
	var layerReads atomic.Int32
	handler := registry.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == layerPath {
			layerReads.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()
	ref := strings.TrimPrefix(srv.URL, "http://") + "/test/local:v1"
	pushImage(t, ref, img)
	c := testConfig(t)
	var retries atomic.Int32
	c.OnPullRetry = func(PullRetryEvent) { retries.Add(1) }
	res, err := c.Resolve(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := OpenCache(c.CacheDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	configBytes, err := img.RawConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Put(mfst.Config.Digest, io.NopCloser(bytes.NewReader(configBytes))); err != nil {
		t.Fatal(err)
	}
	// A directory at the layer blob path lets the download finish but makes
	// the cache's final rename fail, even when the test runs as root.
	if err := os.MkdirAll(cache.blobPath(mfst.Layers[0].Digest), 0700); err != nil {
		t.Fatal(err)
	}
	_, err = c.Pull(context.Background(), res, cache)
	var linkErr *os.LinkError
	if !errors.As(err, &linkErr) {
		t.Fatalf("expected local layer cache rename error, got %v", err)
	}
	if got := layerReads.Load(); got != 1 {
		t.Fatalf("layer GET requests=%d want=1", got)
	}
	if got := retries.Load(); got != 0 {
		t.Fatalf("retry events=%d want=0", got)
	}
}

func TestPullVerifiesConfigWithCacheHit(t *testing.T) {
	img, err := random.Image(128, 1)
	if err != nil {
		t.Fatal(err)
	}
	mfst, err := img.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	configPath := "/v2/test/app/blobs/" + mfst.Config.Digest.String()
	handler := registry.New()
	var corrupt atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if corrupt.Load() && r.Method == http.MethodGet && r.URL.Path == configPath {
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, r)
			for k, vs := range rr.Header() {
				for _, v := range vs {
					w.Header().Add(k, v)
				}
			}
			w.WriteHeader(rr.Code)
			body := rr.Body.Bytes()
			body[0] ^= 1 // same length, different digest
			_, _ = w.Write(body)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()
	ref := strings.TrimPrefix(srv.URL, "http://") + "/test/app:v1"
	pushImage(t, ref, img)
	c := testConfig(t)
	res, err := c.Resolve(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := OpenCache(c.CacheDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Pull(context.Background(), res, cache); err != nil {
		t.Fatal(err)
	}
	if !cache.Has(mfst.Config.Digest) {
		t.Fatal("expected cached config")
	}
	corrupt.Store(true)
	if _, err := c.Pull(context.Background(), res, cache); err == nil || !strings.Contains(err.Error(), "error verifying sha256 checksum") {
		t.Fatalf("corrupt fetched config accepted despite cache hit: %v", err)
	}
}
