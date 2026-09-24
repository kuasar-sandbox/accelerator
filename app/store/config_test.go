package store

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	pkgstore "github.com/kuasar-sandbox/accelerator/pkg/store"
	storeconfig "github.com/kuasar-sandbox/accelerator/pkg/store/config"
)

type unusedProvider struct{}

func (unusedProvider) Retrieve(context.Context) (Credentials, error) {
	panic("provider called during preparation")
}

func boolPointer(value bool) *bool { return &value }

func TestPrepareConfigDeepCopyAndNoExternalEffects(t *testing.T) {
	t.Setenv("STORE_PREPARE_ROOT", "/expanded")
	verify := false
	cfg := Config{
		Listen: "127.0.0.1:1", Backend: "fs",
		FS:          &storeconfig.FSConfig{Root: "${STORE_PREPARE_ROOT}", VerifyContentKey: &verify},
		Generations: &storeconfig.GenerationsConfig{Config: []string{"one"}},
	}
	effective, err := prepareConfig(cfg, Options{Generations: &GenerationSource{
		Load: func(context.Context) ([]pkgstore.Generation, error) { panic("load called during preparation") },
	}})
	if err != nil {
		t.Fatal(err)
	}
	if effective.FS.Root != "${STORE_PREPARE_ROOT}" {
		t.Fatalf("environment expanded: %q", effective.FS.Root)
	}
	cfg.FS.Root = "changed"
	*cfg.FS.VerifyContentKey = true
	cfg.Generations.Config[0] = "changed"
	if effective.FS.Root != "${STORE_PREPARE_ROOT}" || *effective.FS.VerifyContentKey || effective.Generations.Config[0] != "one" {
		t.Fatalf("copy aliases input: %#v", effective)
	}
}

func TestPrepareConfigOverrideIgnoresInvalidConfiguredGenerations(t *testing.T) {
	cfg := Config{Listen: "127.0.0.1:1", Backend: "fs", FS: &storeconfig.FSConfig{Root: "/store"},
		Generations: &storeconfig.GenerationsConfig{RefreshInterval: "bad", File: &storeconfig.GenerationFileConfig{Path: ""}}}
	_, err := prepareConfig(cfg, Options{Generations: &GenerationSource{Load: func(context.Context) ([]pkgstore.Generation, error) { return []pkgstore.Generation{"one"}, nil }}})
	if err != nil {
		t.Fatalf("overridden generations were validated: %v", err)
	}
	if _, err = prepareConfig(cfg, Options{}); err == nil {
		t.Fatal("active invalid generations accepted")
	}
}

func TestPrepareConfigRejectsUnusedProviders(t *testing.T) {
	fs := Config{Listen: "127.0.0.1:1", Backend: "fs", FS: &storeconfig.FSConfig{Root: "/store"}, Generations: &storeconfig.GenerationsConfig{Config: []string{"one"}}}
	if _, err := prepareConfig(fs, Options{ObjectCredentials: unusedProvider{}}); err == nil || !strings.Contains(err.Error(), "object credentials") {
		t.Fatalf("error = %v", err)
	}
	if _, err := prepareConfig(fs, Options{GenerationCredentials: unusedProvider{}}); err == nil || !strings.Contains(err.Error(), "generation credentials") {
		t.Fatalf("error = %v", err)
	}
	if _, err := prepareConfig(fs, Options{Generations: &GenerationSource{Load: func(context.Context) ([]pkgstore.Generation, error) { return []pkgstore.Generation{"one"}, nil }}, GenerationCredentials: unusedProvider{}}); err == nil {
		t.Fatal("generation provider accepted with override")
	}
}

func TestPrepareConfigDefaultGenerationCredentialsRemainIndependent(t *testing.T) {
	cfg := Config{Listen: "127.0.0.1:1", Backend: "s3", S3: &storeconfig.S3Config{
		Endpoint: "https://objects.example", Bucket: "bucket", Prefix: "prefix", AccessKey: "old-ak", SecretKey: "old-sk",
		PathStyle: boolPointer(false), TLS: &storeconfig.S3TLSConfig{CACert: "/ca"}, VerifyContentKey: boolPointer(false),
	}}
	effective, err := prepareConfig(cfg, Options{ObjectCredentials: unusedProvider{}})
	if err != nil {
		t.Fatal(err)
	}
	if effective.S3.AccessKey != "" || effective.S3.SecretKey != "" {
		t.Fatal("object static credentials not cleared")
	}
	generation := effective.Generations.S3
	if generation.AccessKey != "old-ak" || generation.SecretKey != "old-sk" {
		t.Fatalf("generation credentials = %q/%q", generation.AccessKey, generation.SecretKey)
	}
	if generation.TLS == effective.S3.TLS || generation.PathStyle == effective.S3.PathStyle {
		t.Fatal("default generation nested values alias object config")
	}
	if generation.Key != "prefix/__meta/generations" || generation.Region != "us-east-1" || generation.PathStyleEnabled() {
		t.Fatalf("default generation = %#v", generation)
	}
}

func TestPrepareConfigProviderOverridesOwnIncompleteStaticPair(t *testing.T) {
	cfg := Config{Listen: "127.0.0.1:1", Backend: "s3", S3: &storeconfig.S3Config{Endpoint: "https://objects.example", Bucket: "bucket", AccessKey: "stale"},
		Generations: &storeconfig.GenerationsConfig{S3: &storeconfig.GenerationS3Config{Endpoint: "https://meta.example", Bucket: "meta", Key: "generations", SecretKey: "stale"}}}
	if _, err := prepareConfig(cfg, Options{ObjectCredentials: unusedProvider{}, GenerationCredentials: unusedProvider{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareConfig(cfg, Options{ObjectCredentials: unusedProvider{}}); err == nil {
		t.Fatal("unbound generation incomplete pair accepted")
	}
}

func TestPrepareConfigMalformedTimeout(t *testing.T) {
	cfg := Config{Listen: "127.0.0.1:1", Backend: "s3", S3: &storeconfig.S3Config{Endpoint: "https://objects.example", Bucket: "bucket", OpTimeout: "bad"}, Generations: &storeconfig.GenerationsConfig{Config: []string{"one"}}}
	if _, err := prepareConfig(cfg, Options{}); err == nil || !strings.Contains(err.Error(), "op_timeout") {
		t.Fatalf("error = %v", err)
	}
	cfg.S3.OpTimeout = "${STORE_TIMEOUT}"
	os.Setenv("STORE_TIMEOUT", "1s")
	if _, err := prepareConfig(cfg, Options{}); err == nil {
		t.Fatal("direct normalization expanded environment")
	}
}

func TestPrepareConfigDefaults(t *testing.T) {
	cfg := Config{Listen: "127.0.0.1:1", Backend: "fs", FS: &storeconfig.FSConfig{Root: "/store"}}
	effective, err := prepareConfig(cfg, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if effective.GenerationRefreshInterval() != 5*time.Second || effective.StatsIntervalDur() != 30*time.Second || !effective.VerifyKey() {
		t.Fatalf("defaults changed: %#v", effective)
	}
}
