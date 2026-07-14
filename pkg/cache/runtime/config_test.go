package runtime

import (
	"strings"
	"testing"
)

func baseConfig(mode, backendType string) Config {
	return Config{
		Mode:   mode,
		Type:   backendType,
		Listen: "127.0.0.1:7070",
	}
}

func TestValidateDirectBackend(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "local embedded",
			cfg: func() Config {
				cfg := baseConfig("local", "embedded")
				cfg.Rocks.Path = "/cache"
				return cfg
			}(),
		},
		{
			name: "shard redis",
			cfg: func() Config {
				cfg := baseConfig("shard", "redis")
				cfg.Redis = &RedisConfig{Socket: "/run/cache/redis.sock", GetPool: 4, SetPool: 2, Timeout: "2s"}
				return cfg
			}(),
		},
		{
			name:    "missing type",
			cfg:     baseConfig("local", ""),
			wantErr: "type must be",
		},
		{
			name:    "embedded missing rocks",
			cfg:     baseConfig("shard", "embedded"),
			wantErr: "rocks.path is required",
		},
		{
			name:    "redis missing config",
			cfg:     baseConfig("local", "redis"),
			wantErr: "redis config is required",
		},
		{
			name: "redis relative socket",
			cfg: func() Config {
				cfg := baseConfig("local", "redis")
				cfg.Redis = &RedisConfig{Socket: "redis.sock"}
				return cfg
			}(),
			wantErr: "absolute Unix socket",
		},
		{
			name: "redis rejects rocks",
			cfg: func() Config {
				cfg := baseConfig("local", "redis")
				cfg.Redis = &RedisConfig{Socket: "/run/redis.sock"}
				cfg.Rocks.DiskBytes = "1GiB"
				return cfg
			}(),
			wantErr: "rocks config is not valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate error=%v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateTieredRedis(t *testing.T) {
	cfg := Config{
		Mode:   "tiered",
		Listen: "127.0.0.1:7070",
		Tiers: []TierConfig{
			{Type: "redis", Redis: &RedisConfig{Socket: "/run/cache/redis.sock", GetPool: 4, SetPool: 2}},
			{Type: "upstream", Endpoint: "127.0.0.1:7072"},
		},
		Origin: &OriginConfig{Type: "store", Store: &StoreClientConfig{Endpoint: "127.0.0.1:7100"}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	cfg.Tiers[0].Rocks = &RocksConfig{Path: "/cache"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "rocks config is not valid") {
		t.Fatalf("Validate error=%v, want redis/rocks conflict", err)
	}
	cfg.Tiers[0].Rocks = nil
	cfg.Tiers[0].MaxInflight = 64
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "redis.get_pool and redis.set_pool") {
		t.Fatalf("Validate error=%v, want Redis pool guidance", err)
	}
}
