package bundle

import (
	"context"
	"encoding/hex"
	"fmt"
	"runtime"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// ExactManifest identifies one logical Manifest and the already-selected
// Bundle that must provide its complete physical closure.
type ExactManifest struct {
	Key    store.ContentKey
	Reader *Reader
}

type exactChunk struct {
	reader *Reader
	key    store.ContentKey
}

// VerifyExactManifests force-verifies only the selected Manifest closures
// across multiple Bundle sources. Extra objects in a source Bundle are not
// part of this plan and are ignored.
func VerifyExactManifests(ctx context.Context, root ExactManifest, dependencies []ExactManifest, customerKey [32]byte, decryptor manifestcrypto.Decryptor, opts VerifyOptions) error {
	return verifyAndUploadExactManifests(ctx, root, dependencies, customerKey, decryptor, nil, opts)
}

// UploadExactManifests force-verifies and uploads selected closures from
// multiple Bundles without rewriting any physical object. Every distinct
// recorded admission is re-admitted before the first Put. All dependency
// Manifests are published after Chunks and the selected root is published last.
func UploadExactManifests(ctx context.Context, root ExactManifest, dependencies []ExactManifest, customerKey [32]byte, decryptor manifestcrypto.Decryptor, target ExactStore, opts VerifyOptions) error {
	if target == nil {
		return fmt.Errorf("manifest bundle: upload target is required")
	}
	return verifyAndUploadExactManifests(ctx, root, dependencies, customerKey, decryptor, target, opts)
}

func verifyAndUploadExactManifests(ctx context.Context, root ExactManifest, dependencies []ExactManifest, customerKey [32]byte, decryptor manifestcrypto.Decryptor, target ExactStore, opts VerifyOptions) error {
	defer clear(customerKey[:])
	if err := ctx.Err(); err != nil {
		return err
	}
	if decryptor == nil {
		return fmt.Errorf("manifest bundle: decryptor is required")
	}
	plan, readers, err := exactManifestPlan(root, dependencies)
	if err != nil {
		return err
	}
	for _, reader := range readers {
		if err := reader.verifyContainer(ctx); err != nil {
			return fmt.Errorf("manifest bundle: strict source container verification: %w", err)
		}
	}
	if target != nil {
		seenAdmissions := make(map[store.WriteAdmission]struct{}, len(readers))
		for _, reader := range readers {
			admission := reader.Admission()
			if _, checked := seenAdmissions[admission]; checked {
				continue
			}
			seenAdmissions[admission] = struct{}{}
			accepted, err := target.AdmitWriteFor(ctx, admission.Generation)
			if err != nil {
				return fmt.Errorf("manifest bundle: target admission for generation %q: %w", admission.Generation, err)
			}
			if accepted != admission {
				return fmt.Errorf("manifest bundle: target admission %q/%x does not exactly match recorded %q/%x", accepted.Generation, accepted.Salt, admission.Generation, admission.Salt)
			}
		}
	}

	globalRefs := make(map[store.ContentKey]chunkReference)
	chunkRefs := make(map[exactChunk]chunkReference)
	chunkOrder := make([]exactChunk, 0)
	defer func() {
		for key, reference := range globalRefs {
			clear(reference.decryptKey[:])
			globalRefs[key] = reference
		}
		for key, reference := range chunkRefs {
			clear(reference.decryptKey[:])
			chunkRefs[key] = reference
		}
		clear(globalRefs)
		clear(chunkRefs)
	}()

	for _, selected := range plan {
		if err := ctx.Err(); err != nil {
			return err
		}
		manifestBlob, err := getVerified(ctx, selected.Reader.Getter(), store.PartitionManifest, selected.Key)
		if err != nil {
			return fmt.Errorf("manifest bundle: verify selected Manifest %s: %w", hex.EncodeToString(selected.Key[:]), err)
		}
		manifest, sealed, err := codec.Unmarshal(manifestBlob.Bytes())
		if err != nil {
			manifestBlob.Release()
			return fmt.Errorf("manifest bundle: Manifest %s parse: %w", hex.EncodeToString(selected.Key[:]), err)
		}
		keys, err := codec.UnsealKeys(manifest, sealed, customerKey, decryptor)
		manifestBlob.Release()
		if err != nil {
			return fmt.Errorf("manifest bundle: Manifest %s unseal key table: %w", hex.EncodeToString(selected.Key[:]), err)
		}
		if err := selected.Reader.validateManifestClosure(selected.Key, manifest); err != nil {
			clear(keys)
			return err
		}
		for index, entry := range manifest.Entries {
			if entry.IsZero {
				continue
			}
			chunkKey := store.ContentKey(entry.CiphertextHash)
			reference := chunkReference{decryptKey: keys[index], plainSize: entry.Size}
			if previous, ok := globalRefs[chunkKey]; ok && previous != reference {
				clear(keys)
				clear(reference.decryptKey[:])
				return fmt.Errorf("manifest bundle: Chunk %s has inconsistent key or plaintext size across selected Manifests", hex.EncodeToString(chunkKey[:]))
			}
			globalRefs[chunkKey] = reference
			chunk := exactChunk{reader: selected.Reader, key: chunkKey}
			if previous, ok := chunkRefs[chunk]; ok {
				if previous != reference {
					clear(keys)
					clear(reference.decryptKey[:])
					return fmt.Errorf("manifest bundle: Chunk %s has inconsistent metadata in one selected Bundle", hex.EncodeToString(chunkKey[:]))
				}
				continue
			}
			chunkRefs[chunk] = reference
			chunkOrder = append(chunkOrder, chunk)
		}
		clear(keys)
	}

	if err := verifyExactChunks(ctx, chunkOrder, chunkRefs, decryptor, target, opts); err != nil {
		return err
	}
	if target == nil {
		return nil
	}
	for _, selected := range plan[:len(plan)-1] {
		if err := selected.Reader.uploadManifest(ctx, target, selected.Key); err != nil {
			return err
		}
	}
	return root.Reader.uploadManifest(ctx, target, root.Key)
}

func exactManifestPlan(root ExactManifest, dependencies []ExactManifest) ([]ExactManifest, []*Reader, error) {
	if root.Reader == nil {
		return nil, nil, fmt.Errorf("manifest bundle: root source Reader is required")
	}
	if !root.Reader.HasManifest(root.Key) {
		return nil, nil, fmt.Errorf("manifest bundle: root Manifest %s is absent from its selected Bundle", hex.EncodeToString(root.Key[:]))
	}
	plan := make([]ExactManifest, 0, len(dependencies)+1)
	seenManifests := make(map[store.ContentKey]*Reader, len(dependencies)+1)
	seenReaders := make(map[*Reader]struct{}, len(dependencies)+1)
	readers := make([]*Reader, 0, len(dependencies)+1)
	appendSelected := func(selected ExactManifest, isRoot bool) error {
		label := "dependency"
		if isRoot {
			label = "root"
		}
		if selected.Reader == nil {
			return fmt.Errorf("manifest bundle: %s Manifest %s has no selected Bundle", label, hex.EncodeToString(selected.Key[:]))
		}
		if !selected.Reader.HasManifest(selected.Key) {
			return fmt.Errorf("manifest bundle: %s Manifest %s is absent from its selected Bundle", label, hex.EncodeToString(selected.Key[:]))
		}
		if previous, duplicate := seenManifests[selected.Key]; duplicate {
			if previous != selected.Reader {
				return fmt.Errorf("manifest bundle: Manifest %s is assigned to multiple Bundle sources", hex.EncodeToString(selected.Key[:]))
			}
			return nil
		}
		seenManifests[selected.Key] = selected.Reader
		if _, seen := seenReaders[selected.Reader]; !seen {
			seenReaders[selected.Reader] = struct{}{}
			readers = append(readers, selected.Reader)
		}
		plan = append(plan, selected)
		return nil
	}
	for _, dependency := range dependencies {
		if dependency.Key == root.Key {
			if dependency.Reader != root.Reader {
				return nil, nil, fmt.Errorf("manifest bundle: root Manifest %s is assigned to multiple Bundle sources", hex.EncodeToString(root.Key[:]))
			}
			continue
		}
		if err := appendSelected(dependency, false); err != nil {
			return nil, nil, err
		}
	}
	if err := appendSelected(root, true); err != nil {
		return nil, nil, err
	}
	return plan, readers, nil
}

func verifyExactChunks(ctx context.Context, chunks []exactChunk, refs map[exactChunk]chunkReference, decryptor manifestcrypto.Decryptor, target ExactStore, opts VerifyOptions) error {
	if len(chunks) == 0 {
		return ctx.Err()
	}
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
	if workers > len(chunks) {
		workers = len(chunks)
	}
	if workers < 1 {
		workers = 1
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan exactChunk, workers)
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
			for chunk := range jobs {
				if workCtx.Err() != nil {
					continue
				}
				reference := refs[chunk]
				if cap(plain) < int(reference.plainSize) {
					plain = make([]byte, reference.plainSize)
				} else {
					plain = plain[:reference.plainSize]
				}
				blob, err := getVerified(workCtx, chunk.reader.Getter(), store.PartitionChunk, chunk.key)
				if err != nil {
					fail(fmt.Errorf("manifest bundle: verify selected Chunk %s: %w", hex.EncodeToString(chunk.key[:]), err))
					continue
				}
				physical := blob.Bytes()
				if err := decryptor.DecryptChunkTo(workCtx, reference.decryptKey, physical, plain); err != nil {
					blob.Release()
					clear(plain)
					fail(fmt.Errorf("manifest bundle: Chunk %s decrypt/decompress: %w", hex.EncodeToString(chunk.key[:]), err))
					continue
				}
				admission := chunk.reader.Admission()
				if derived := manifestcrypto.DeriveKey(admission.Salt, plain); derived != reference.decryptKey {
					blob.Release()
					clear(plain)
					fail(fmt.Errorf("manifest bundle: Chunk %s key is outside source admission salt domain", hex.EncodeToString(chunk.key[:])))
					continue
				}
				if target != nil {
					if _, err := target.Put(workCtx, admission, store.PartitionChunk, chunk.key, physical); err != nil {
						blob.Release()
						clear(plain)
						fail(fmt.Errorf("manifest bundle: upload Chunk %s: %w", hex.EncodeToString(chunk.key[:]), err))
						continue
					}
				}
				blob.Release()
				clear(plain)
			}
		}()
	}
	for _, chunk := range chunks {
		select {
		case jobs <- chunk:
		case <-workCtx.Done():
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
