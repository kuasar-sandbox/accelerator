package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	pkgstore "github.com/kuasar-sandbox/accelerator/pkg/store"
	storeconfig "github.com/kuasar-sandbox/accelerator/pkg/store/config"
	stores3 "github.com/kuasar-sandbox/accelerator/pkg/store/s3"
	"github.com/kuasar-sandbox/accelerator/pkg/store/s3/sdkclient"
)

const maxGenerationListBytes = 256 << 10

var errGenerationSourceUninitialised = errors.New("generation source is uninitialised (run `store-ctl init`)")

// builtInGenerationSource constructs the read-only inline and file sources
// selected by an already-prepared effective configuration. Construction never
// opens the configured file; each file Load observes its current contents.
func builtInGenerationSource(ctx context.Context, cfg Config, credentials CredentialsProvider) (*GenerationSource, func() error, error) {
	g := cfg.Generations
	if g == nil {
		return nil, nil, errors.New("store: no generation source configured")
	}
	if g.IsConfigSource() {
		generations, err := generationStrings(g.Config)
		if err != nil {
			return nil, nil, err
		}
		return &GenerationSource{Load: func(context.Context) ([]pkgstore.Generation, error) {
			return append([]pkgstore.Generation(nil), generations...), nil
		}}, func() error { return nil }, nil
	}
	if g.File != nil {
		path := g.File.Path
		return &GenerationSource{
			RefreshInterval: cfg.GenerationRefreshInterval(),
			Load: func(context.Context) ([]pkgstore.Generation, error) {
				file, err := os.Open(path)
				if err != nil {
					if os.IsNotExist(err) {
						return nil, fmt.Errorf("%w: missing %s", errGenerationSourceUninitialised, path)
					}
					return nil, fmt.Errorf("read generation file %s: %w", path, err)
				}
				defer file.Close()
				return readGenerationList(file)
			},
		}, func() error { return nil }, nil
	}
	if g.S3 != nil {
		configured := *g.S3
		if g.S3.PathStyle != nil {
			pathStyle := *g.S3.PathStyle
			configured.PathStyle = &pathStyle
		}
		if g.S3.TLS != nil {
			tls := *g.S3.TLS
			configured.TLS = &tls
		}
		client, err := sdkclient.New(ctx, sdkclient.Config{
			Endpoint: configured.Endpoint, Region: configured.Region, Bucket: configured.Bucket,
			PathStyle: configured.PathStyleEnabled(), AccessKey: configured.AccessKey,
			SecretKey: configured.SecretKey, Credentials: credentials,
			TLS: sdkclient.TLSConfig{CACert: tlsCACert(configured.TLS), InsecureSkipVerify: tlsInsecure(configured.TLS)},
		})
		if err != nil {
			return nil, nil, fmt.Errorf("store: construct generation s3 client: %w", err)
		}
		key := configured.Key
		return &GenerationSource{
			RefreshInterval: cfg.GenerationRefreshInterval(),
			Load: func(ctx context.Context) ([]pkgstore.Generation, error) {
				body, meta, err := client.GetLimited(ctx, key, maxGenerationListBytes)
				if errors.Is(err, stores3.ErrNotFound) {
					return nil, fmt.Errorf("%w: missing s3://.../%s", errGenerationSourceUninitialised, key)
				}
				if err != nil {
					return nil, fmt.Errorf("read generation object %s: %w", key, err)
				}
				if meta == nil || meta.ETag == "" {
					return nil, fmt.Errorf("generation object %s returned no ETag", key)
				}
				return readGenerationList(bytes.NewReader(body))
			},
		}, client.Close, nil
	}
	return nil, nil, errors.New("store: built-in generation source is not inline, file, or s3")
}

func tlsCACert(cfg *storeconfig.S3TLSConfig) string {
	if cfg == nil {
		return ""
	}
	return cfg.CACert
}

func tlsInsecure(cfg *storeconfig.S3TLSConfig) bool {
	return cfg != nil && cfg.InsecureSkipVerify
}

func readGenerationList(reader io.Reader) ([]pkgstore.Generation, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxGenerationListBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxGenerationListBytes {
		return nil, fmt.Errorf("generation list is %d bytes, maximum is %d", len(body), maxGenerationListBytes)
	}
	values := make([]string, 0, strings.Count(string(body), "\n")+1)
	for _, line := range strings.Split(string(body), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			values = append(values, line)
		}
	}
	return generationStrings(values)
}

func generationStrings(values []string) ([]pkgstore.Generation, error) {
	generations := make([]pkgstore.Generation, len(values))
	for i, value := range values {
		generations[i] = pkgstore.Generation(value)
	}
	if err := pkgstore.ValidateGenerations(generations); err != nil {
		return nil, err
	}
	return generations, nil
}
