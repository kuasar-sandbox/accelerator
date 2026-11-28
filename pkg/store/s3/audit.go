package s3

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// AuditDeltas scans one generation for delta objects and the aliases that
// name them (see store.DeltaAudit). It lists the alias and chunk prefixes and
// then makes one request per chunk object to read its format byte: a ranged
// GET of one byte when the client supports it (sdkclient does), otherwise a
// full GET. It is an admin-time operation whose cost is proportional to the
// generation's object count.
func (s *Store) AuditDeltas(ctx context.Context, generation store.Generation) (store.DeltaAudit, error) {
	var out store.DeltaAudit
	if err := store.ValidateGeneration(generation); err != nil {
		return out, fmt.Errorf("s3: %w", err)
	}

	// 1. Aliases and what they name.
	aliasPrefix := path.Join(s.prefix, string(store.PartitionAlias), string(generation)) + "/"
	var aliasKeys []string
	if err := s.listBounded(ctx, aliasPrefix, func(key string) bool {
		if _, ok := contentKeyOf(key); ok {
			aliasKeys = append(aliasKeys, key)
		}
		return true
	}); err != nil {
		return out, fmt.Errorf("s3: list %s: %w", aliasPrefix, err)
	}
	named := make(map[store.ContentKey]struct{}, len(aliasKeys))
	for _, key := range aliasKeys {
		body, _, err := s.boundedGet(ctx, func(ctx context.Context) ([]byte, *ObjectMeta, error) {
			return s.client.Get(ctx, key)
		})
		if errors.Is(err, ErrNotFound) {
			continue // purged between list and read
		}
		if err != nil {
			return out, fmt.Errorf("s3: read alias %s: %w", key, err)
		}
		delta, err := store.DecodeAlias(body)
		if err != nil {
			return out, fmt.Errorf("s3: alias %s: %w", key, err)
		}
		out.Aliases++
		named[delta] = struct{}{}
	}
	out.Named = len(named)

	// 2. Chunk objects.
	chunkPrefix := path.Join(s.prefix, string(store.PartitionChunk), string(generation)) + "/"
	var chunkKeys []string
	if err := s.listBounded(ctx, chunkPrefix, func(key string) bool {
		if _, ok := contentKeyOf(key); ok {
			chunkKeys = append(chunkKeys, key)
		}
		return true
	}); err != nil {
		return out, fmt.Errorf("s3: list %s: %w", chunkPrefix, err)
	}
	prefixReader, _ := s.client.(objectPrefixReader)
	present := make(map[store.ContentKey]struct{})
	for _, objKey := range chunkKeys {
		ck, _ := contentKeyOf(objKey)
		var (
			head []byte
			meta *ObjectMeta
			err  error
		)
		if prefixReader != nil {
			head, meta, err = s.boundedGet(ctx, func(ctx context.Context) ([]byte, *ObjectMeta, error) {
				return prefixReader.GetPrefix(ctx, objKey, 1)
			})
		} else {
			head, meta, err = s.boundedGet(ctx, func(ctx context.Context) ([]byte, *ObjectMeta, error) {
				return s.client.Get(ctx, objKey)
			})
		}
		if errors.Is(err, ErrNotFound) {
			continue // deleted between list and read
		}
		if err != nil {
			return out, fmt.Errorf("s3: read %s: %w", objKey, err)
		}
		out.Chunks++
		if len(head) == 0 || head[0] != store.ChunkFormatDelta {
			continue
		}
		var size uint64
		if meta != nil && meta.Size > 0 {
			size = uint64(meta.Size)
		}
		present[ck] = struct{}{}
		out.DeltaObjects++
		out.DeltaBytes += size
		if _, ok := named[ck]; ok {
			continue
		}
		orphan := store.OrphanDelta{Key: ck, Size: size}
		if meta != nil {
			orphan.ModTime = meta.LastModified
		}
		out.Orphans = append(out.Orphans, orphan)
		out.OrphanBytes += size
	}
	for key := range named {
		if _, ok := present[key]; !ok {
			out.Dangling++
		}
	}
	return out, nil
}

// contentKeyOf extracts the content key from an object key's last path
// element; anything that is not a 64-hex-digit name is not an object.
func contentKeyOf(objKey string) (store.ContentKey, bool) {
	name := objKey
	if i := strings.LastIndexByte(objKey, '/'); i >= 0 {
		name = objKey[i+1:]
	}
	raw, err := hex.DecodeString(name)
	if err != nil || len(raw) != len(store.ContentKey{}) {
		return store.ContentKey{}, false
	}
	return store.ContentKey(raw), true
}
