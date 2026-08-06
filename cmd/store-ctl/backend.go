package main

import (
	"context"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/store/fs"
	"github.com/kuasar-sandbox/accelerator/pkg/store/s3"
	"github.com/kuasar-sandbox/accelerator/pkg/store/s3/sdkclient"
)

// adminStore is the common surface that init/rollout/purge/info
// drive against either backend. Mirror of the Backend interface plus
// the admin methods that Store implementations expose.
type adminStore interface {
	ActiveGeneration() string
	Generations() []string
	Rollout(ctx context.Context, gen string) error
	Drop(ctx context.Context, gen string) error
	Wipe(ctx context.Context) error
	GenerationStats(ctx context.Context, gen string) (map[store.Partition]int, error)
}

// openFSStore opens an existing fs store (no init). Returns
// fs.ErrUninitialised on a fresh root so callers can suggest
// `store-ctl init`.
func openFSStore(cfg *Config) (*fs.Store, error) {
	return fs.New(fs.Config{
		Root:      cfg.FS.Root,
		VerifyKey: cfg.VerifyKey(),
	})
}

// openS3Store opens an existing S3-compatible object store. Returns
// s3.ErrUninitialised on a missing meta object.
func openS3Store(ctx context.Context, cfg *Config) (*s3.Store, error) {
	client, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	storeCfg, err := s3StoreConfig(cfg)
	if err != nil {
		return nil, err
	}
	return s3.New(ctx, client, storeCfg)
}

// initFSStore calls fs.Init on a fresh root.
func initFSStore(cfg *Config, generation string) error {
	return fs.Init(fs.Config{Root: cfg.FS.Root}, generation)
}

// initS3Store writes the initial meta object to S3-compatible storage.
func initS3Store(ctx context.Context, cfg *Config, generation string) error {
	client, err := newS3Client(ctx, cfg)
	if err != nil {
		return err
	}
	storeCfg, err := s3StoreConfig(cfg)
	if err != nil {
		return err
	}
	return s3.Init(ctx, client, storeCfg, generation)
}

// newS3Client opens the configured endpoint. Centralising this path keeps
// credential-provider and signing behavior consistent across subcommands.
func newS3Client(ctx context.Context, cfg *Config) (*sdkclient.Client, error) {
	client, err := sdkclient.New(ctx, sdkclient.Config{
		Endpoint:  cfg.S3.Endpoint,
		Region:    cfg.S3.Region,
		Bucket:    cfg.S3.Bucket,
		PathStyle: cfg.S3PathStyle(),
		AccessKey: cfg.S3.AccessKey,
		SecretKey: cfg.S3.SecretKey,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 client: %w", err)
	}
	return client, nil
}

// s3StoreConfig builds the s3.Config used by both Init and New.
// Picks up the per-call defenses (timeouts, max-inflight) from
// cfg.S3 so admin commands respect the same limits as serve.
func s3StoreConfig(cfg *Config) (s3.Config, error) {
	opTimeout, err := cfg.S3OpTimeout()
	if err != nil {
		return s3.Config{}, fmt.Errorf("s3.op_timeout: %w", err)
	}
	return s3.Config{
		Bucket:        cfg.S3.Bucket,
		Prefix:        cfg.S3.Prefix,
		VerifyKey:     cfg.VerifyKey(),
		MaxInflight:   cfg.S3.MaxInflight,
		OpTimeout:     opTimeout,
		MaxObjectSize: cfg.S3.MaxObjectSize,
	}, nil
}
