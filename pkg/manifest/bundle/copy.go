package bundle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// CopyManifestFromBundle copies one exact complete layer when the recorded
// source and destination write admissions match. The authenticated Manifest key
// table preserves the actual Chunk keys, including any writer-side extra salt.
func (w *Writer) CopyManifestFromBundle(ctx context.Context, key store.ContentKey, source *Reader) error {
	if source == nil {
		return fmt.Errorf("manifest bundle: source Bundle is required")
	}
	if source.Admission() != w.admission {
		return fmt.Errorf("manifest bundle: cannot copy objects across different admissions")
	}
	return w.CopyManifest(ctx, key, source.Getter())
}

// CopyManifest copies exact physical bytes from a caller-owned Bundle-only or
// Store Getter. It force-checks each physical ContentKey and complete Chunk
// closure. Source/target write-admission compatibility is established by the
// caller (or by CopyManifestFromBundle).
func (w *Writer) CopyManifest(ctx context.Context, key store.ContentKey, source cache.Getter) error {
	if source == nil {
		return fmt.Errorf("manifest bundle: copy source Getter is required")
	}
	manifestBlob, err := getVerified(ctx, source, store.PartitionManifest, key)
	if err != nil {
		return err
	}
	manifestData := manifestBlob.Bytes()
	manifest, _, err := codec.Unmarshal(manifestData)
	if err != nil {
		manifestBlob.Release()
		return fmt.Errorf("manifest bundle: copied Manifest %s parse: %w", hex.EncodeToString(key[:]), err)
	}
	seen := make(map[store.ContentKey]struct{})
	for _, entry := range manifest.Entries {
		if entry.IsZero {
			continue
		}
		chunkKey := store.ContentKey(entry.CiphertextHash)
		if _, ok := seen[chunkKey]; ok {
			continue
		}
		seen[chunkKey] = struct{}{}
		chunkBlob, err := getVerified(ctx, source, store.PartitionChunk, chunkKey)
		if err != nil {
			manifestBlob.Release()
			return fmt.Errorf("manifest bundle: copy Manifest %s closure: %w", hex.EncodeToString(key[:]), err)
		}
		_, putErr := w.Put(ctx, w.admission, store.PartitionChunk, chunkKey, chunkBlob.Bytes())
		chunkBlob.Release()
		if putErr != nil {
			manifestBlob.Release()
			return putErr
		}
	}
	_, err = w.Put(ctx, w.admission, store.PartitionManifest, key, manifestData)
	manifestBlob.Release()
	return err
}

func getVerified(ctx context.Context, source cache.Getter, partition store.Partition, key store.ContentKey) (cache.Blob, error) {
	result, blob, err := source.Get(ctx, partition, key)
	if err != nil {
		if blob != nil {
			blob.Release()
		}
		return nil, err
	}
	if result != cache.CacheHit || blob == nil {
		if blob != nil {
			blob.Release()
		}
		return nil, fmt.Errorf("manifest bundle: %s %s not found", partition, hex.EncodeToString(key[:]))
	}
	if sha256.Sum256(blob.Bytes()) != key {
		blob.Release()
		return nil, fmt.Errorf("manifest bundle: %s %s physical ContentKey mismatch", partition, hex.EncodeToString(key[:]))
	}
	return blob, nil
}
