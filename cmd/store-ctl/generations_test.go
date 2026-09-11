package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
	stores3 "github.com/kuasar-sandbox/accelerator/pkg/store/s3"
)

func TestParseGenerationListPreservesOrder(t *testing.T) {
	got, err := parseGenerationList([]byte("G9\nG1\nG5\n"))
	want := []store.Generation{"G9", "G1", "G5"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("parse = %v, %v; want %v", got, err, want)
	}
	if _, err := parseGenerationList([]byte("G1\nG1\n")); err == nil {
		t.Fatal("duplicate list accepted")
	}
	if _, err := parseGenerationList([]byte("G1\n../escape\n")); err == nil {
		t.Fatal("unsafe generation accepted")
	}
}

func TestDefaultGenerationSourcesRemainCompatible(t *testing.T) {
	root := t.TempDir()
	fsPath := filepath.Join(t.TempDir(), "fs.yaml")
	writeText(t, fsPath, fmt.Sprintf("backend: fs\nfs:\n  root: %s\n", root))
	fsConfig, err := LoadConfig(fsPath, false)
	if err != nil {
		t.Fatal(err)
	}
	wantFile := filepath.Join(root, "__meta", "generations")
	if fsConfig.Generations.File == nil || fsConfig.Generations.File.Path != wantFile {
		t.Fatalf("default file source = %+v, want %s", fsConfig.Generations, wantFile)
	}

	s3Path := filepath.Join(t.TempDir(), "s3.yaml")
	writeText(t, s3Path, "backend: s3\ns3:\n  endpoint: https://s3.example\n  bucket: data\n  prefix: production/\n")
	s3Config, err := LoadConfig(s3Path, false)
	if err != nil {
		t.Fatal(err)
	}
	if s3Config.Generations.S3 == nil || s3Config.Generations.S3.Key != "production/__meta/generations" {
		t.Fatalf("default s3 source = %+v", s3Config.Generations)
	}
}

func TestGenerationS3InsecureSkipVerifyScoping(t *testing.T) {
	t.Run("omitted generations inherits the data backend", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "s3.yaml")
		writeText(t, path, "backend: s3\ns3:\n  endpoint: https://s3.example\n  bucket: data\n  insecure_skip_verify: true\n")
		cfg, err := LoadConfig(path, false)
		if err != nil {
			t.Fatal(err)
		}
		if !cfg.Generations.S3.InsecureSkipVerify {
			t.Fatalf("default generation source did not inherit s3.insecure_skip_verify: %+v", cfg.Generations.S3)
		}
	})

	t.Run("explicit generations.s3 is independent of the data backend", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "store.yaml")
		writeText(t, path, `
backend: s3
s3:
  endpoint: https://objects.example.com
  bucket: data
  insecure_skip_verify: true
generations:
  refresh_interval: 5s
  s3:
    endpoint: https://meta.example
    bucket: metadata
    key: prod/generations
`)
		cfg, err := LoadConfig(path, false)
		if err != nil {
			t.Fatal(err)
		}
		if !cfg.S3.InsecureSkipVerify || cfg.Generations.S3.InsecureSkipVerify {
			t.Fatalf("unexpected scoping: data=%v generations=%v", cfg.S3.InsecureSkipVerify, cfg.Generations.S3.InsecureSkipVerify)
		}
	})
}

func TestGenerationSourceSelectionIsExclusive(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "store.yaml")
	writeText(t, configPath, "backend: fs\nfs:\n  root: /tmp/store\ngenerations:\n  config: [G1]\n  file:\n    path: /tmp/generations\n")
	if _, err := LoadConfig(configPath, false); err == nil {
		t.Fatal("mixed generation sources accepted")
	}
}

func TestIndependentGenerationS3Config(t *testing.T) {
	t.Setenv("GEN_META_AK", "meta-ak")
	t.Setenv("GEN_META_SK", "meta-sk")
	configPath := filepath.Join(t.TempDir(), "store.yaml")
	writeText(t, configPath, `
backend: fs
fs:
  root: /objects
generations:
  refresh_interval: 9s
  s3:
    endpoint: https://meta.example
    bucket: metadata
    key: prod/generations
    path_style: false
    access_key: ${GEN_META_AK}
    secret_key: ${GEN_META_SK}
`)
	cfg, err := LoadConfig(configPath, false)
	if err != nil {
		t.Fatal(err)
	}
	meta := cfg.Generations.S3
	if meta == nil || meta.Endpoint != "https://meta.example" || meta.Bucket != "metadata" ||
		meta.Key != "prod/generations" || meta.pathStyle() || meta.AccessKey != "meta-ak" || meta.SecretKey != "meta-sk" {
		t.Fatalf("generation S3 config = %+v", meta)
	}
	if cfg.GenerationRefreshInterval() != 9*time.Second {
		t.Fatalf("refresh interval = %v", cfg.GenerationRefreshInterval())
	}
}

func TestConfigSourceSIGHUPReloadAndFailureRetention(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "store.yaml")
	root := filepath.Join(directory, "objects")
	writeConfigGenerations(t, configPath, root, "    - G1\n")
	cfg, err := LoadConfig(configPath, false)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := openGenerationSource(context.Background(), cfg, configPath)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := newGenerationManager(context.Background(), handle)
	if err != nil {
		t.Fatal(err)
	}

	writeConfigGenerations(t, configPath, root, "    - G1\n    - G1\n")
	if err := manager.Refresh(context.Background()); err == nil {
		t.Fatal("invalid refresh succeeded")
	}
	if got := manager.Current(); !reflect.DeepEqual(got, []store.Generation{"G1"}) {
		t.Fatalf("failed refresh replaced current list: %v", got)
	}

	writeConfigGenerations(t, configPath, root, "    - G1\n    - G2\n")
	ctx, cancel := context.WithCancel(context.Background())
	hup := make(chan os.Signal, 1)
	done := make(chan struct{})
	go func() {
		manager.Run(ctx, hup, func(string, ...any) {})
		close(done)
	}()
	hup <- syscall.SIGHUP
	deadline := time.After(2 * time.Second)
	for !reflect.DeepEqual(manager.Current(), []store.Generation{"G1", "G2"}) {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("SIGHUP did not refresh list: %v", manager.Current())
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

func TestFileSourceMutationAndRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta", "generations")
	source := &fileGenerationSource{path: path}
	handle := &generationSourceHandle{source: source, kind: generationSourceFile, file: source}
	if err := handle.initialise(context.Background(), "G9"); err != nil {
		t.Fatal(err)
	}
	if err := handle.mutate(context.Background(), func(current []store.Generation) ([]store.Generation, error) {
		return appendGeneration(current, "G1")
	}); err != nil {
		t.Fatal(err)
	}
	manager, err := newGenerationManager(context.Background(), handle)
	if err != nil {
		t.Fatal(err)
	}
	if got := manager.Current(); !reflect.DeepEqual(got, []store.Generation{"G9", "G1"}) {
		t.Fatalf("file order = %v", got)
	}
	writeText(t, path, "G9\nG9\n")
	if err := manager.Refresh(context.Background()); err == nil {
		t.Fatal("duplicate refresh succeeded")
	}
	if got := manager.Current(); !reflect.DeepEqual(got, []store.Generation{"G9", "G1"}) {
		t.Fatalf("failed file refresh replaced list: %v", got)
	}
}

func TestFileSourcePeriodicRefresh(t *testing.T) {
	path := filepath.Join(t.TempDir(), "generations")
	writeText(t, path, "G1\n")
	source := &fileGenerationSource{path: path}
	handle := &generationSourceHandle{
		source:   source,
		kind:     generationSourceFile,
		file:     source,
		interval: 10 * time.Millisecond,
	}
	manager, err := newGenerationManager(context.Background(), handle)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		manager.Run(ctx, make(chan os.Signal), func(string, ...any) {})
		close(done)
	}()
	writeText(t, path, "G1\nG2\n")
	deadline := time.After(2 * time.Second)
	for !reflect.DeepEqual(manager.Current(), []store.Generation{"G1", "G2"}) {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("periodic refresh did not update list: %v", manager.Current())
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

type fakeGenerationClient struct {
	mu           sync.Mutex
	body         []byte
	etag         string
	version      int
	conflictOnce bool
	conflictBody []byte
	lastLimit    int64
}

func (f *fakeGenerationClient) GetLimited(_ context.Context, _ string, maxBytes int64) ([]byte, *stores3.ObjectMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastLimit = maxBytes
	if f.body == nil {
		return nil, nil, stores3.ErrNotFound
	}
	if int64(len(f.body)) > maxBytes {
		return nil, nil, fmt.Errorf("body exceeds limit %d", maxBytes)
	}
	return append([]byte(nil), f.body...), &stores3.ObjectMeta{Size: int64(len(f.body)), ETag: f.etag}, nil
}

func (f *fakeGenerationClient) Put(_ context.Context, _ string, body []byte, options stores3.PutOptions) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if options.IfNoneMatch == "*" && f.body != nil {
		return "", stores3.ErrPreconditionFailed
	}
	if options.IfMatch != "" {
		if f.conflictOnce {
			f.conflictOnce = false
			f.body = append([]byte(nil), f.conflictBody...)
			f.version++
			f.etag = fmt.Sprintf("etag-%d", f.version)
			return "", stores3.ErrPreconditionFailed
		}
		if options.IfMatch != f.etag {
			return "", stores3.ErrPreconditionFailed
		}
	}
	f.body = append([]byte(nil), body...)
	f.version++
	f.etag = fmt.Sprintf("etag-%d", f.version)
	return f.etag, nil
}

func (f *fakeGenerationClient) Delete(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.body = nil
	f.etag = ""
	return nil
}

func TestS3SourceBoundsObjectBeforeParsing(t *testing.T) {
	client := &fakeGenerationClient{
		body: make([]byte, maxGenerationListBytes+1),
		etag: "etag-1",
	}
	source := &s3GenerationSource{client: client, key: "meta/generations"}
	if _, err := source.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("Load error = %v, want size-limit error", err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.lastLimit != maxGenerationListBytes {
		t.Fatalf("GetLimited max = %d, want %d", client.lastLimit, maxGenerationListBytes)
	}
}

func TestS3SourceCASReloadsBeforeRetry(t *testing.T) {
	client := &fakeGenerationClient{body: []byte("G1\n"), etag: "etag-1", version: 1}
	source := &s3GenerationSource{client: client, key: "meta/generations"}
	handle := &generationSourceHandle{source: source, kind: generationSourceS3, s3: source}
	client.conflictOnce = true
	client.conflictBody = []byte("G1\nG2\n")
	if err := handle.mutate(context.Background(), func(current []store.Generation) ([]store.Generation, error) {
		return appendGeneration(current, "G3")
	}); err != nil {
		t.Fatal(err)
	}
	got, err := source.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []store.Generation{"G1", "G2", "G3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CAS result = %v, want %v", got, want)
	}
}

func TestS3SourcePeriodicRefreshPreservesOrderAndRetainsValidList(t *testing.T) {
	client := &fakeGenerationClient{body: []byte("G9\nG1\nG5\n"), etag: "etag-1", version: 1}
	source := &s3GenerationSource{client: client, key: "meta/generations"}
	handle := &generationSourceHandle{
		source:   source,
		kind:     generationSourceS3,
		s3:       source,
		interval: 10 * time.Millisecond,
	}
	manager, err := newGenerationManager(context.Background(), handle)
	if err != nil {
		t.Fatal(err)
	}
	wantInitial := []store.Generation{"G9", "G1", "G5"}
	if got := manager.Current(); !reflect.DeepEqual(got, wantInitial) {
		t.Fatalf("S3 order = %v, want %v", got, wantInitial)
	}

	client.mu.Lock()
	client.body = []byte("G9\nG9\n")
	client.etag = "etag-2"
	client.mu.Unlock()
	if err := manager.Refresh(context.Background()); err == nil {
		t.Fatal("invalid S3 refresh succeeded")
	}
	if got := manager.Current(); !reflect.DeepEqual(got, wantInitial) {
		t.Fatalf("failed S3 refresh replaced valid list: %v", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		manager.Run(ctx, make(chan os.Signal), func(string, ...any) {})
		close(done)
	}()
	client.mu.Lock()
	client.body = []byte("G9\nG1\nG5\nG2\n")
	client.etag = "etag-3"
	client.mu.Unlock()
	wantRefreshed := []store.Generation{"G9", "G1", "G5", "G2"}
	deadline := time.After(2 * time.Second)
	for !reflect.DeepEqual(manager.Current(), wantRefreshed) {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("periodic S3 refresh did not update list: %v", manager.Current())
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

func TestConfigSourceIsReadOnlyAndLastGenerationCannotBeRemoved(t *testing.T) {
	handle := &generationSourceHandle{kind: generationSourceConfig}
	if err := handle.initialise(context.Background(), "G1"); err == nil {
		t.Fatal("config source init succeeded")
	}
	if err := handle.mutate(context.Background(), func(g []store.Generation) ([]store.Generation, error) { return g, nil }); err == nil {
		t.Fatal("config source mutation succeeded")
	}
	if _, err := removeGeneration([]store.Generation{"G1"}, "G1"); err == nil {
		t.Fatal("last generation removal succeeded")
	}
	got, err := removeGeneration([]store.Generation{"G1", "G2"}, "G1")
	if err != nil || !reflect.DeepEqual(got, []store.Generation{"G2"}) {
		t.Fatalf("remove old generation = %v, %v", got, err)
	}
}

func TestS3SourceInitAndRemove(t *testing.T) {
	client := &fakeGenerationClient{}
	source := &s3GenerationSource{client: client, key: "meta/generations"}
	handle := &generationSourceHandle{source: source, kind: generationSourceS3, s3: source}
	if err := handle.initialise(context.Background(), "G1"); err != nil {
		t.Fatal(err)
	}
	if err := handle.initialise(context.Background(), "G2"); err == nil {
		t.Fatal("second init succeeded")
	}
	if err := handle.remove(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Load(context.Background()); !errors.Is(err, errGenerationSourceUninitialised) {
		t.Fatalf("Load after remove = %v", err)
	}
}

func writeConfigGenerations(t *testing.T, path, root, entries string) {
	t.Helper()
	writeText(t, path, fmt.Sprintf("backend: fs\nfs:\n  root: %s\ngenerations:\n  config:\n%s", root, entries))
}

func writeText(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
