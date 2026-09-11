package main

import (
	"context"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/store/fs"
	"github.com/kuasar-sandbox/accelerator/pkg/store/s3"
	"github.com/kuasar-sandbox/accelerator/pkg/store/s3/sdkclient"
)

// adminStore is the explicit-generation object-data admin surface shared by
// purge and info. Generation lists are owned by generationSourceHandle.
type adminStore interface {
	DropGeneration(ctx context.Context, generation store.Generation) error
	Wipe(ctx context.Context) error
	GenerationStats(ctx context.Context, generation store.Generation) (map[store.Partition]int, error)
}

// openFSStore opens an existing filesystem object-data root.
func openFSStore(cfg *Config) (*fs.Store, error) {
	return fs.New(fs.Config{
		Root:     cfg.FS.Root,
		DirectIO: cfg.FS.DirectIO,
	})
}

// openS3Store opens the S3-compatible object data plane. It does not read the
// independently configured generation source.
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
		TLS:       tlsDialConfig(cfg.S3.TLS),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 client: %w", err)
	}
	return client, nil
}

// tlsDialConfig converts the YAML-facing tls block into the sdkclient's
// dialing-side TLSConfig. A nil block means strict defaults.
func tlsDialConfig(t *S3TLSConfig) sdkclient.TLSConfig {
	if t == nil {
		return sdkclient.TLSConfig{}
	}
	return sdkclient.TLSConfig{
		CACert:             t.CACert,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
}

// s3StoreConfig builds the data-plane config used by serve and admin helpers.
func s3StoreConfig(cfg *Config) (s3.Config, error) {
	opTimeout, err := cfg.S3OpTimeout()
	if err != nil {
		return s3.Config{}, fmt.Errorf("s3.op_timeout: %w", err)
	}
	return s3.Config{
		Bucket:        cfg.S3.Bucket,
		Prefix:        cfg.S3.Prefix,
		MaxInflight:   cfg.S3.MaxInflight,
		OpTimeout:     opTimeout,
		MaxObjectSize: cfg.S3.MaxObjectSize,
	}, nil
}
