package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	pkgstore "github.com/kuasar-sandbox/accelerator/pkg/store"
)

const maxGenerationListBytes = 256 << 10

var errGenerationSourceUninitialised = errors.New("generation source is uninitialised (run `store-ctl init`)")

// builtInGenerationSource constructs the read-only inline and file sources
// selected by an already-prepared effective configuration. Construction never
// opens the configured file; each file Load observes its current contents.
func builtInGenerationSource(cfg Config) (*GenerationSource, func(), error) {
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
		}}, func() {}, nil
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
		}, func() {}, nil
	}
	return nil, nil, errors.New("store: built-in generation source is not inline or file")
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
