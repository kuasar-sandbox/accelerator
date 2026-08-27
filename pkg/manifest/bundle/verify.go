package bundle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime"
	"sort"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// ExactStore is the target surface required by exact Bundle upload. The Store
// must explicitly re-admit the Bundle's recorded generation before any Put.
type ExactStore interface {
	AdmitWriteFor(ctx context.Context, generation store.Generation) (store.WriteAdmission, error)
	Put(ctx context.Context, admission store.WriteAdmission, partition store.Partition, key store.ContentKey, data []byte) (isNew bool, err error)
}

// VerifyOptions bounds physical Chunk verification and upload concurrency.
type VerifyOptions struct {
	Workers int

	// ExpectedManifests, when non-nil, is the exact local Manifest set
	// reachable from the selected root according to the caller's snapshot
	// metadata parser. This rejects unrelated Manifest entries without making
	// the accelerator package interpret sandboxer-specific snapshot.cfg bytes.
	ExpectedManifests []store.ContentKey
}

type chunkReference struct {
	decryptKey [32]byte
	plainSize  uint32
}

// FullVerify force-checks physical ContentKeys, every Manifest closure and key
// table, the recorded admission salt domain, unique Chunk plaintext, and the
// absence of unreferenced Chunk entries. It ignores runtime verify_content.
func (r *Reader) FullVerify(ctx context.Context, root store.ContentKey, customerKey [32]byte, decryptor manifestcrypto.Decryptor, opts VerifyOptions) error {
	return r.verifyAndUpload(ctx, root, customerKey, decryptor, nil, opts)
}

// Upload force-verifies and copies exact physical objects to target. Unique
// Chunks are uploaded first, non-root Manifests next, and root last. Objects are
// not re-chunked, compressed, encrypted, or re-sealed.
func (r *Reader) Upload(ctx context.Context, root store.ContentKey, customerKey [32]byte, decryptor manifestcrypto.Decryptor, target ExactStore, opts VerifyOptions) error {
	if target == nil {
		return fmt.Errorf("manifest bundle: upload target is required")
	}
	return r.verifyAndUpload(ctx, root, customerKey, decryptor, target, opts)
}

func (r *Reader) verifyAndUpload(ctx context.Context, root store.ContentKey, customerKey [32]byte, decryptor manifestcrypto.Decryptor, target ExactStore, opts VerifyOptions) error {
	defer clear(customerKey[:])
	if err := ctx.Err(); err != nil {
		return err
	}
	if decryptor == nil {
		return fmt.Errorf("manifest bundle: decryptor is required")
	}
	if err := r.verifyContainer(ctx); err != nil {
		return fmt.Errorf("manifest bundle: strict container verification: %w", err)
	}
	if !r.HasManifest(root) {
		return fmt.Errorf("manifest bundle: root Manifest %s is absent", hex.EncodeToString(root[:]))
	}
	if opts.ExpectedManifests != nil {
		expected := make(map[store.ContentKey]struct{}, len(opts.ExpectedManifests))
		for _, key := range opts.ExpectedManifests {
			if _, duplicate := expected[key]; duplicate {
				return fmt.Errorf("manifest bundle: duplicate expected Manifest %s", hex.EncodeToString(key[:]))
			}
			expected[key] = struct{}{}
		}
		if _, ok := expected[root]; !ok {
			return fmt.Errorf("manifest bundle: expected Manifest set omits root %s", hex.EncodeToString(root[:]))
		}
		if len(expected) != len(r.manifests) {
			return fmt.Errorf("manifest bundle: local Manifest set has %d entries, expected %d", len(r.manifests), len(expected))
		}
		for key := range r.manifests {
			if _, ok := expected[key]; !ok {
				return fmt.Errorf("manifest bundle: unreferenced Manifest %s", hex.EncodeToString(key[:]))
			}
		}
	}
	canonicalSalt, err := store.SaltForGeneration(r.admission.Generation)
	if err != nil {
		return fmt.Errorf("manifest bundle: recorded admission: %w", err)
	}
	if r.admission.Salt != canonicalSalt {
		return fmt.Errorf("manifest bundle: recorded admission salt is not canonical for generation %q", r.admission.Generation)
	}
	if target != nil {
		accepted, err := target.AdmitWriteFor(ctx, r.admission.Generation)
		if err != nil {
			return fmt.Errorf("manifest bundle: target admission for generation %q: %w", r.admission.Generation, err)
		}
		if accepted != r.admission {
			return fmt.Errorf("manifest bundle: target admission %q/%x does not exactly match recorded %q/%x", accepted.Generation, accepted.Salt, r.admission.Generation, r.admission.Salt)
		}
	}

	manifestKeys := sortedKeys(r.manifests)
	chunkRefs := make(map[store.ContentKey]chunkReference, len(r.chunks))
	defer func() {
		for key, reference := range chunkRefs {
			clear(reference.decryptKey[:])
			chunkRefs[key] = reference
		}
		clear(chunkRefs)
	}()
	for _, manifestKey := range manifestKeys {
		if err := ctx.Err(); err != nil {
			return err
		}
		blob, err := r.entryBlob(store.PartitionManifest, manifestKey, r.manifests[manifestKey])
		if err != nil {
			return err
		}
		data := blob.Bytes()
		if sha256.Sum256(data) != manifestKey {
			blob.Release()
			return fmt.Errorf("manifest bundle: Manifest %s physical ContentKey mismatch", hex.EncodeToString(manifestKey[:]))
		}
		manifest, sealed, err := codec.Unmarshal(data)
		if err != nil {
			blob.Release()
			return fmt.Errorf("manifest bundle: Manifest %s parse: %w", hex.EncodeToString(manifestKey[:]), err)
		}
		keys, err := codec.UnsealKeys(manifest, sealed, customerKey, decryptor)
		blob.Release()
		if err != nil {
			return fmt.Errorf("manifest bundle: Manifest %s unseal key table: %w", hex.EncodeToString(manifestKey[:]), err)
		}
		if err := r.validateManifestClosure(manifestKey, manifest); err != nil {
			clear(keys)
			return err
		}
		for index, entry := range manifest.Entries {
			if entry.IsZero {
				continue
			}
			chunkKey := store.ContentKey(entry.CiphertextHash)
			reference := chunkReference{decryptKey: keys[index], plainSize: entry.Size}
			if previous, ok := chunkRefs[chunkKey]; ok && previous != reference {
				clear(keys)
				clear(reference.decryptKey[:])
				return fmt.Errorf("manifest bundle: Chunk %s has inconsistent key or plaintext size across Manifests", hex.EncodeToString(chunkKey[:]))
			}
			chunkRefs[chunkKey] = reference
		}
		clear(keys)
	}
	if len(chunkRefs) != len(r.chunks) {
		for key := range r.chunks {
			if _, referenced := chunkRefs[key]; !referenced {
				return fmt.Errorf("manifest bundle: unreferenced Chunk %s", hex.EncodeToString(key[:]))
			}
		}
	}

	chunkKeys := sortedKeys(r.chunks)
	if err := r.verifyChunks(ctx, chunkKeys, chunkRefs, decryptor, target, opts); err != nil {
		return err
	}
	if target == nil {
		return nil
	}
	for _, manifestKey := range manifestKeys {
		if manifestKey == root {
			continue
		}
		if err := r.uploadManifest(ctx, target, manifestKey); err != nil {
			return err
		}
	}
	return r.uploadManifest(ctx, target, root)
}

func (r *Reader) verifyChunks(ctx context.Context, keys []store.ContentKey, refs map[store.ContentKey]chunkReference, decryptor manifestcrypto.Decryptor, target ExactStore, opts VerifyOptions) error {
	workers := opts.Workers
	if workers <= 0 && target != nil {
		if sized, ok := target.(interface{ PoolSize() int }); ok {
			workers = sized.PoolSize()
		}
	}
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers > 32 {
		workers = 32
	}
	if workers > len(keys) {
		workers = len(keys)
	}
	if workers < 1 {
		return ctx.Err()
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan store.ContentKey, workers)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	fail := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var plain []byte
			for key := range jobs {
				if workCtx.Err() != nil {
					continue
				}
				reference := refs[key]
				if cap(plain) < int(reference.plainSize) {
					plain = make([]byte, reference.plainSize)
				} else {
					plain = plain[:reference.plainSize]
				}
				blob, err := r.entryBlob(store.PartitionChunk, key, r.chunks[key])
				if err != nil {
					fail(err)
					continue
				}
				physical := blob.Bytes()
				if sha256.Sum256(physical) != key {
					blob.Release()
					fail(fmt.Errorf("manifest bundle: Chunk %s physical ContentKey mismatch", hex.EncodeToString(key[:])))
					continue
				}
				if err := decryptor.DecryptChunkTo(workCtx, reference.decryptKey, physical, plain); err != nil {
					blob.Release()
					clear(plain)
					fail(fmt.Errorf("manifest bundle: Chunk %s decrypt/decompress: %w", hex.EncodeToString(key[:]), err))
					continue
				}
				if derived := manifestcrypto.DeriveKey(r.admission.Salt, plain); derived != reference.decryptKey {
					blob.Release()
					clear(plain)
					fail(fmt.Errorf("manifest bundle: Chunk %s key is outside recorded admission salt domain", hex.EncodeToString(key[:])))
					continue
				}
				if target != nil {
					if _, err := target.Put(workCtx, r.admission, store.PartitionChunk, key, physical); err != nil {
						blob.Release()
						clear(plain)
						fail(fmt.Errorf("manifest bundle: upload Chunk %s: %w", hex.EncodeToString(key[:]), err))
						continue
					}
				}
				blob.Release()
				clear(plain)
			}
		}()
	}
	for _, key := range keys {
		select {
		case jobs <- key:
		case <-workCtx.Done():
			break
		}
		if workCtx.Err() != nil {
			break
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func (r *Reader) uploadManifest(ctx context.Context, target ExactStore, key store.ContentKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	blob, err := r.entryBlob(store.PartitionManifest, key, r.manifests[key])
	if err != nil {
		return err
	}
	defer blob.Release()
	physical := blob.Bytes()
	if sha256.Sum256(physical) != key {
		return fmt.Errorf("manifest bundle: Manifest %s physical ContentKey changed before upload", hex.EncodeToString(key[:]))
	}
	if _, err := target.Put(ctx, r.admission, store.PartitionManifest, key, physical); err != nil {
		return fmt.Errorf("manifest bundle: upload Manifest %s: %w", hex.EncodeToString(key[:]), err)
	}
	return nil
}

func sortedKeys[T any](objects map[store.ContentKey]T) []store.ContentKey {
	keys := make([]store.ContentKey, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	return keys
}
