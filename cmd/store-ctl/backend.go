package main

import (
	"context"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/store/fs"
	"github.com/kuasar-sandbox/accelerator/pkg/store/obs"
	"github.com/kuasar-sandbox/accelerator/pkg/store/obs/s3client"
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

// openOBSStore dials OBS, opens an existing store. Returns
// obs.ErrUninitialised on a missing meta object.
func openOBSStore(ctx context.Context, cfg *Config) (*obs.Store, error) {
	client, err := newOBSClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	storeCfg, err := obsStoreConfig(cfg)
	if err != nil {
		return nil, err
	}
	return obs.New(ctx, client, storeCfg)
}

// initFSStore calls fs.Init on a fresh root.
func initFSStore(cfg *Config, generation string) error {
	return fs.Init(fs.Config{Root: cfg.FS.Root}, generation)
}

// initOBSStore dials OBS and writes the initial meta object.
func initOBSStore(ctx context.Context, cfg *Config, generation string) error {
	client, err := newOBSClient(ctx, cfg)
	if err != nil {
		return err
	}
	storeCfg, err := obsStoreConfig(cfg)
	if err != nil {
		return err
	}
	return obs.Init(ctx, client, storeCfg, generation)
}

// newOBSClient dials the OBS s3-compatible endpoint with the
// resolved cfg. Centralised so every subcommand uses the same
// dialing path (avoids drift in credential/discovery handling).
func newOBSClient(ctx context.Context, cfg *Config) (*s3client.Client, error) {
	client, err := s3client.New(ctx, s3client.Config{
		Endpoint:  cfg.OBS.Endpoint,
		Region:    cfg.OBS.Region,
		Bucket:    cfg.OBS.Bucket,
		AccessKey: cfg.OBS.AccessKey,
		SecretKey: cfg.OBS.SecretKey,
	})
	if err != nil {
		return nil, fmt.Errorf("obs s3 client: %w", err)
	}
	return client, nil
}

// obsStoreConfig builds the obs.Config used by both Init and New.
// Picks up the per-call defenses (timeouts, max-inflight) from
// cfg.OBS so admin commands respect the same limits as serve.
func obsStoreConfig(cfg *Config) (obs.Config, error) {
	opTimeout, err := cfg.OBSOpTimeout()
	if err != nil {
		return obs.Config{}, fmt.Errorf("obs.op_timeout: %w", err)
	}
	return obs.Config{
		Bucket:        cfg.OBS.Bucket,
		Prefix:        cfg.OBS.Prefix,
		VerifyKey:     cfg.VerifyKey(),
		MaxInflight:   cfg.OBS.MaxInflight,
		OpTimeout:     opTimeout,
		MaxObjectSize: cfg.OBS.MaxObjectSize,
	}, nil
}
