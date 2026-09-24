package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	storeconfig "github.com/kuasar-sandbox/accelerator/pkg/store/config"
)

func TestBuiltInInlineSourceIsCopiedAndStatic(t *testing.T) {
	names := []string{"old", "new"}
	cfg := Config{Generations: &storeconfig.GenerationsConfig{Config: names}}
	source, closeSource, err := builtInGenerationSource(cfg)
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
	source, closeSource, err := builtInGenerationSource(cfg)
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
	source, closeSource, err := builtInGenerationSource(Config{Generations: &storeconfig.GenerationsConfig{
		RefreshInterval: "5s", File: &storeconfig.GenerationFileConfig{Path: path},
	}})
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
