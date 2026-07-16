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
				cfg.Redis = &RedisConfig{Endpoint: "unix:///run/cache/redis.sock", GetPool: 4, SetPool: 2, Timeout: "2s"}
				return cfg
			}(),
		},
		{
			name: "local redis tcp",
			cfg: func() Config {
				cfg := baseConfig("local", "redis")
				cfg.Redis = &RedisConfig{Endpoint: "redis.internal:6379", GetPool: 4, SetPool: 2, Timeout: "2s"}
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
			name: "redis invalid endpoint",
			cfg: func() Config {
				cfg := baseConfig("local", "redis")
				cfg.Redis = &RedisConfig{Endpoint: "redis.sock"}
				return cfg
			}(),
			wantErr: "absolute Unix socket path, unix:/path, or TCP host:port",
		},
		{
			name: "redis rejects rocks",
			cfg: func() Config {
				cfg := baseConfig("local", "redis")
				cfg.Redis = &RedisConfig{Endpoint: "/run/redis.sock"}
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

func TestParseRedisEndpoint(t *testing.T) {
	tests := []struct {
		name        string
		endpoint    string
		wantNetwork string
		wantAddress string
		wantErr     bool
	}{
		{name: "absolute path", endpoint: "/run/cache/redis.sock", wantNetwork: "unix", wantAddress: "/run/cache/redis.sock"},
		{name: "unix short", endpoint: "unix:/run/cache/redis.sock", wantNetwork: "unix", wantAddress: "/run/cache/redis.sock"},
		{name: "unix URI", endpoint: "unix:///run/cache/redis.sock", wantNetwork: "unix", wantAddress: "/run/cache/redis.sock"},
		{name: "IPv4", endpoint: "10.0.1.60:6379", wantNetwork: "tcp", wantAddress: "10.0.1.60:6379"},
		{name: "DNS", endpoint: "redis.internal:6379", wantNetwork: "tcp", wantAddress: "redis.internal:6379"},
		{name: "IPv6", endpoint: "[2001:db8::60]:6379", wantNetwork: "tcp", wantAddress: "[2001:db8::60]:6379"},
		{name: "empty", wantErr: true},
		{name: "relative path", endpoint: "redis.sock", wantErr: true},
		{name: "relative unix", endpoint: "unix:redis.sock", wantErr: true},
		{name: "missing host", endpoint: ":6379", wantErr: true},
		{name: "missing port", endpoint: "redis.internal:", wantErr: true},
		{name: "unbracketed IPv6", endpoint: "2001:db8::60:6379", wantErr: true},
		{name: "surrounding whitespace", endpoint: " redis.internal:6379", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			network, address, err := ParseRedisEndpoint(tt.endpoint)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseRedisEndpoint(%q)=(%q,%q,nil), want error", tt.endpoint, network, address)
				}
				return
			}
			if err != nil || network != tt.wantNetwork || address != tt.wantAddress {
				t.Fatalf("ParseRedisEndpoint(%q)=(%q,%q,%v), want (%q,%q,nil)", tt.endpoint, network, address, err, tt.wantNetwork, tt.wantAddress)
			}
		})
	}
}

func TestValidateTieredRedis(t *testing.T) {
	cfg := Config{
		Mode:   "tiered",
		Listen: "127.0.0.1:7070",
		Tiers: []TierConfig{
			{Type: "redis", Redis: &RedisConfig{Endpoint: "/run/cache/redis.sock", GetPool: 4, SetPool: 2}},
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

func TestValidateTieredRejectsNegativeMaxInflight(t *testing.T) {
	cfg := Config{
		Mode:   "tiered",
		Listen: "127.0.0.1:7070",
		Tiers: []TierConfig{{
			Type: "ec", MaxInflight: -1,
			Cluster: &ECClusterConfig{Peers: []PeerConfig{{ID: "p0", Endpoint: "127.0.0.1:1"}}},
		}},
		Origin: &OriginConfig{Type: "store", Store: &StoreClientConfig{Endpoint: "127.0.0.1:7100"}},
	}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "max_inflight must be >= 0") {
		t.Fatalf("Validate error=%v, want negative max_inflight rejection", err)
	}
}
