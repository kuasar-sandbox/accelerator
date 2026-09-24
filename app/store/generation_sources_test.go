package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	storeconfig "github.com/kuasar-sandbox/accelerator/pkg/store/config"
)

func TestBuiltInInlineSourceIsCopiedAndStatic(t *testing.T) {
	names := []string{"old", "new"}
	cfg := Config{Generations: &storeconfig.GenerationsConfig{Config: names}}
	source, closeSource, err := builtInGenerationSource(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSource()
	names[0] = "changed"
	first, err := source.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	first[1] = "mutated"
	second, err := source.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second[0] != "old" || second[1] != "new" {
		t.Fatalf("inline generations = %v", second)
	}
}

func TestBuiltInFileSourceLoadsOnDemand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "generations")
	cfg := Config{Generations: &storeconfig.GenerationsConfig{
		RefreshInterval: "5s", File: &storeconfig.GenerationFileConfig{Path: path},
	}}
	source, closeSource, err := builtInGenerationSource(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("construction performed file I/O: %v", err)
	}
	defer closeSource()
	if source.RefreshInterval != 5*time.Second {
		t.Fatalf("refresh interval = %v", source.RefreshInterval)
	}
	if err := os.WriteFile(path, []byte(" new-oldest \n\n newest\t\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := source.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "new-oldest" || got[1] != "newest" {
		t.Fatalf("generations = %v", got)
	}
}

func TestReadGenerationListValidation(t *testing.T) {
	for name, body := range map[string]string{
		"empty":     " \n\t\n",
		"duplicate": "one\none\n",
		"invalid":   "bad/name\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := readGenerationList(strings.NewReader(body)); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestReadGenerationListLimit(t *testing.T) {
	boundary := bytes.Repeat([]byte("x"), maxGenerationListBytes)
	if _, err := readGenerationList(bytes.NewReader(boundary)); err == nil || strings.Contains(err.Error(), "maximum") {
		t.Fatalf("exact boundary returned wrong error: %v", err)
	}
	over := bytes.Repeat([]byte("x"), maxGenerationListBytes+4096)
	counting := &countingReader{reader: bytes.NewReader(over)}
	if _, err := readGenerationList(counting); err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("over-limit error = %v", err)
	}
	if counting.read > maxGenerationListBytes+1 {
		t.Fatalf("read %d bytes, want at most %d", counting.read, maxGenerationListBytes+1)
	}
}

type countingReader struct {
	reader *bytes.Reader
	read   int
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += n
	return n, err
}

func TestBuiltInFileSourceMissingIsUninitialised(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing")
	source, closeSource, err := builtInGenerationSource(context.Background(), Config{Generations: &storeconfig.GenerationsConfig{
		RefreshInterval: "5s", File: &storeconfig.GenerationFileConfig{Path: path},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSource()
	_, err = source.Load(context.Background())
	if !errors.Is(err, errGenerationSourceUninitialised) || !strings.Contains(err.Error(), path) ||
		!strings.Contains(err.Error(), "generation source is uninitialised (run `store-ctl init`)") {
		t.Fatalf("missing-file error = %v", err)
	}
}

type generationProvider struct {
	calls  atomic.Int32
	closed atomic.Bool
	err    error
}

func (p *generationProvider) Retrieve(context.Context) (Credentials, error) {
	p.calls.Add(1)
	if p.err != nil {
		return Credentials{}, p.err
	}
	return Credentials{AccessKeyID: "generation-ak", SecretAccessKey: "generation-sk", SessionToken: "generation-token"}, nil
}

func (p *generationProvider) Close() error {
	p.closed.Store(true)
	return nil
}

func generationS3Config(endpoint string) Config {
	pathStyle := true
	return Config{Generations: &storeconfig.GenerationsConfig{
		RefreshInterval: "5s",
		S3:              &storeconfig.GenerationS3Config{Endpoint: endpoint, Region: "test-region", Bucket: "metadata", Key: "state/generations", PathStyle: &pathStyle},
	}}
}

func setGenerationCredentialEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "fallback-ak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "fallback-sk")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

func TestBuiltInS3SourceIsLazySignedAndOrdered(t *testing.T) {
	setGenerationCredentialEnvironment(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.EscapedPath() != "/metadata/state/generations" {
			t.Errorf("path = %q", r.URL.EscapedPath())
		}
		if auth := r.Header.Get("Authorization"); !strings.Contains(auth, "Credential=generation-ak/") {
			t.Errorf("authorization = %q", auth)
		}
		if token := r.Header.Get("X-Amz-Security-Token"); token != "generation-token" {
			t.Errorf("session token = %q", token)
		}
		w.Header().Set("ETag", `"v1"`)
		_, _ = io.WriteString(w, " oldest \nnewest\n")
	}))
	defer server.Close()

	provider := &generationProvider{}
	source, cleanup, err := builtInGenerationSource(context.Background(), generationS3Config(server.URL), provider)
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 0 || provider.calls.Load() != 0 {
		t.Fatalf("construction performed I/O: requests=%d provider=%d", requests.Load(), provider.calls.Load())
	}
	if source.RefreshInterval != 5*time.Second {
		t.Fatalf("refresh interval = %v", source.RefreshInterval)
	}
	got, err := source.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "oldest" || got[1] != "newest" {
		t.Fatalf("generations = %v", got)
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if provider.closed.Load() {
		t.Fatal("cleanup closed borrowed provider")
	}
}

func TestBuiltInS3SourceErrorsAndValidation(t *testing.T) {
	setGenerationCredentialEnvironment(t)
	for _, tc := range []struct {
		name              string
		status            int
		etag, body        string
		wantUninitialised bool
		want              string
	}{
		{"missing", http.StatusNotFound, "", `<Error><Code>NoSuchKey</Code></Error>`, true, "state/generations"},
		{"absent etag", http.StatusOK, "", "one\n", false, "no ETag"},
		{"empty", http.StatusOK, `"v"`, "\n", false, "list is empty"},
		{"duplicate", http.StatusOK, `"v"`, "one\none\n", false, "duplicate generation"},
		{"invalid", http.StatusOK, `"v"`, "bad/name\n", false, "unsafe generation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.etag != "" {
					w.Header().Set("ETag", tc.etag)
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			source, cleanup, err := builtInGenerationSource(context.Background(), generationS3Config(server.URL), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			_, err = source.Load(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) || errors.Is(err, errGenerationSourceUninitialised) != tc.wantUninitialised {
				t.Fatalf("Load error = %v", err)
			}
		})
	}
}

func TestBuiltInS3SourceLimitAndProviderError(t *testing.T) {
	setGenerationCredentialEnvironment(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"v"`)
		w.Header().Set("Content-Length", fmt.Sprint(maxGenerationListBytes+1))
		_, _ = w.Write(bytes.Repeat([]byte{'x'}, maxGenerationListBytes+1))
	}))
	defer server.Close()
	source, cleanup, err := builtInGenerationSource(context.Background(), generationS3Config(server.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if _, err = source.Load(context.Background()); err == nil || !strings.Contains(err.Error(), fmt.Sprint(maxGenerationListBytes)) {
		t.Fatalf("limit error = %v", err)
	}

	want := errors.New("generation credentials unavailable")
	provider := &generationProvider{err: want}
	source, cleanup, err = builtInGenerationSource(context.Background(), generationS3Config(server.URL), provider)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if _, err = source.Load(context.Background()); !errors.Is(err, want) {
		t.Fatalf("provider error = %v", err)
	}
}

func TestBuiltInS3CleanupClosesOwnedTransportOnly(t *testing.T) {
	setGenerationCredentialEnvironment(t)
	var idle, closed atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"v"`)
		_, _ = io.WriteString(w, "one\n")
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateIdle {
			idle.Add(1)
		}
		if state == http.StateClosed {
			closed.Add(1)
		}
	}
	server.Start()
	defer server.Close()
	provider := &generationProvider{}
	source, cleanup, err := builtInGenerationSource(context.Background(), generationS3Config(server.URL), provider)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if idle.Load() == 0 {
		t.Fatal("connection did not become idle")
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for closed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if closed.Load() == 0 {
		t.Fatal("cleanup did not close owned transport")
	}
	if provider.closed.Load() {
		t.Fatal("cleanup closed borrowed provider")
	}
}
