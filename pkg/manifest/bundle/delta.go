package bundle

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/delta"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// Delta-compressed chunks in Bundles.
//
// A Bundle writer never produces delta objects (ingest disables delta for
// bundle writers: a delta's base is some other image's chunk, which a
// self-contained Bundle would then have to carry in full, making the Bundle
// larger rather than smaller). A Bundle can still contain delta objects when
// it is a copy of a store image (CopyManifest) or was uploaded from one, so
// verification, upload and copy understand them:
//
//   - a Version2 manifest's delta table names each delta entry's base; the
//     closure of a manifest is its entries' objects plus those bases, and all of
//     them must be present in the Bundle (validateManifestClosure);
//   - a base referenced only through the delta table is still "referenced": it
//     is uploaded with the Bundle and does not count as an unreferenced Chunk;
//   - a delta entry is verified by opening the delta object with its key-table
//     key, checking that it names the base the manifest said it does, decrypting
//     that base with the key the delta carries, and applying the stream. The
//     base's bytes are hash-checked like every other object. Depth is one: a
//     base that is itself a delta object is rejected;
//   - uploading a delta object also records the Store alias that names it as
//     the chunk's representation. A manifest may reference a delta object only
//     if the alias names it, and store-ctl delta-gc deletes delta objects no
//     alias names. If the alias cannot be recorded for this object, the upload
//     fails before any Manifest is published.

// DeltaAliasStore is the ExactStore capability required to upload a Bundle that
// contains delta objects. See store.PutResult.
type DeltaAliasStore interface {
	PutRepresenting(ctx context.Context, admission store.WriteAdmission, key store.ContentKey, data []byte, represents store.ContentKey) (store.PutResult, error)
}

// ErrDeltaAlias reports that a delta object cannot be uploaded because its
// Store alias cannot be recorded. No Manifest has been published.
var ErrDeltaAlias = errors.New("manifest bundle: cannot record the Store alias for a delta object")

// checkDeltaUpload fails before any Put when the upload includes a delta object
// and either the target cannot record aliases or the decryptor cannot encrypt,
// which computing a delta chunk's independent address requires.
func checkDeltaUpload(target ExactStore, decryptor manifestcrypto.Decryptor, hasDelta bool) error {
	if target == nil || !hasDelta {
		return nil
	}
	if _, ok := target.(DeltaAliasStore); !ok {
		return fmt.Errorf("%w: the upload target cannot record aliases", ErrDeltaAlias)
	}
	if _, ok := decryptor.(manifestcrypto.Encryptor); !ok {
		return fmt.Errorf("%w: the decryptor cannot encrypt, which computing a chunk's independent address requires", ErrDeltaAlias)
	}
	return nil
}

// deltaIndependentKey returns H_ind, the address the delta chunk's plaintext
// would have as an independent object under admission's salt, which names the
// chunk's alias. plain is the verified plaintext. The delta key in the manifest
// is re-derived under the same salt first: a manifest written with a writer
// extra salt does not match, and its H_ind cannot be computed here.
func deltaIndependentKey(ctx context.Context, decryptor manifestcrypto.Decryptor, admission store.WriteAdmission, key store.ContentKey, reference chunkReference, physical, plain []byte) (store.ContentKey, error) {
	hdr, err := manifestcrypto.ParseDeltaHeader(physical)
	if err != nil {
		return store.ContentKey{}, fmt.Errorf("manifest bundle: Chunk %s: %w", hex.EncodeToString(key[:]), err)
	}
	if manifestcrypto.DeriveDeltaKey(admission.Salt, hdr.BaseObjectKey, plain) != reference.decryptKey {
		return store.ContentKey{}, fmt.Errorf("%w: Chunk %s was written under a writer extra salt, so its independent address is unknown",
			ErrDeltaAlias, hex.EncodeToString(key[:]))
	}
	enc, ok := decryptor.(manifestcrypto.Encryptor)
	if !ok {
		return store.ContentKey{}, fmt.Errorf("%w: the decryptor cannot encrypt", ErrDeltaAlias)
	}
	object, hInd, chunkKey, err := enc.EncryptChunk(ctx, admission.Salt, plain)
	clear(object)
	clear(chunkKey[:])
	if err != nil {
		return store.ContentKey{}, fmt.Errorf("manifest bundle: Chunk %s independent address: %w", hex.EncodeToString(key[:]), err)
	}
	return store.ContentKey(hInd), nil
}

// recordDeltaAlias records key as the representation of the chunk at hInd. The
// object must already be stored: a representing Put of an existing object never
// deletes it, whatever the outcome. Only a recorded or existing alias naming key
// is success.
func recordDeltaAlias(ctx context.Context, target ExactStore, admission store.WriteAdmission, key, hInd store.ContentKey, physical []byte) error {
	aliases, ok := target.(DeltaAliasStore)
	if !ok {
		return fmt.Errorf("%w: the upload target cannot record aliases", ErrDeltaAlias)
	}
	res, err := aliases.PutRepresenting(ctx, admission, key, physical, hInd)
	if err != nil {
		return fmt.Errorf("manifest bundle: record alias for Chunk %s: %w", hex.EncodeToString(key[:]), err)
	}
	switch {
	case res.Discarded:
		return fmt.Errorf("%w: the Store deleted Chunk %s after a lost alias", ErrDeltaAlias, hex.EncodeToString(key[:]))
	case res.Represented == store.RepresentedWon:
		return nil
	case res.Represented == store.RepresentedLost:
		return fmt.Errorf("%w: the target already represents chunk %s by another delta object %s",
			ErrDeltaAlias, hex.EncodeToString(hInd[:]), hex.EncodeToString(res.Key[:]))
	default:
		return fmt.Errorf("%w: the Store did not record an alias for Chunk %s (%v); it may predate delta aliases",
			ErrDeltaAlias, hex.EncodeToString(key[:]), res.Represented)
	}
}

// uploadChunk stores one verified chunk object on target, and records the alias
// for a delta object.
func uploadChunk(ctx context.Context, target ExactStore, decryptor manifestcrypto.Decryptor, admission store.WriteAdmission, key store.ContentKey, reference chunkReference, physical, plain []byte) error {
	var hInd store.ContentKey
	if reference.kind == refDelta {
		var err error
		if hInd, err = deltaIndependentKey(ctx, decryptor, admission, key, reference, physical, plain); err != nil {
			return err
		}
	}
	if _, err := target.Put(ctx, admission, store.PartitionChunk, key, physical); err != nil {
		return fmt.Errorf("manifest bundle: upload Chunk %s: %w", hex.EncodeToString(key[:]), err)
	}
	if reference.kind == refDelta {
		return recordDeltaAlias(ctx, target, admission, key, hInd, physical)
	}
	return nil
}

// refKind says how a manifest references a chunk object.
type refKind uint8

const (
	// refChunk: an entry addresses the object and it is an independent chunk.
	refChunk refKind = iota
	// refDelta: an entry addresses the object and it is a delta object.
	refDelta
	// refBase: no entry addresses the object; a delta entry depends on it.
	// decryptKey is unknown (it lives inside the delta object).
	refBase
)

// manifestChunkRefs reports every chunk object the manifest references, with
// how it references it, to add. keys are the unsealed per-entry keys.
func manifestChunkRefs(manifest *codec.Manifest, keys [][32]byte, add func(store.ContentKey, chunkReference) error) error {
	for index, entry := range manifest.Entries {
		if entry.IsZero {
			continue
		}
		reference := chunkReference{decryptKey: keys[index], plainSize: entry.Size}
		if entry.IsDelta {
			ref, ok := manifest.DeltaFor(index)
			if !ok {
				return fmt.Errorf("manifest bundle: entry %d is flagged delta without a delta ref", index)
			}
			reference.kind = refDelta
			reference.base = store.ContentKey(ref.Base)
			reference.baseSize = ref.BaseSize
		}
		if err := add(store.ContentKey(entry.CiphertextHash), reference); err != nil {
			return err
		}
	}
	for _, ref := range manifest.Deltas {
		if err := add(store.ContentKey(ref.Base), chunkReference{kind: refBase, plainSize: ref.BaseSize}); err != nil {
			return err
		}
	}
	return nil
}

// mergeChunkReference reconciles two references to the same object. An
// entry's reference (which knows the key) wins over a base-only reference,
// provided the sizes agree; two entry references must be identical; and a
// base may not be a delta object.
func mergeChunkReference(key store.ContentKey, previous, next chunkReference) (chunkReference, error) {
	if previous.plainSize != next.plainSize {
		return chunkReference{}, fmt.Errorf("manifest bundle: Chunk %s has inconsistent plaintext size across references", hex.EncodeToString(key[:]))
	}
	switch {
	case previous.kind == refBase && next.kind == refBase:
		return previous, nil
	case previous.kind == refBase:
		if next.kind == refDelta {
			return chunkReference{}, fmt.Errorf("manifest bundle: Chunk %s is both a delta object and another delta's base", hex.EncodeToString(key[:]))
		}
		return next, nil
	case next.kind == refBase:
		if previous.kind == refDelta {
			return chunkReference{}, fmt.Errorf("manifest bundle: Chunk %s is both a delta object and another delta's base", hex.EncodeToString(key[:]))
		}
		return previous, nil
	}
	if previous != next {
		return chunkReference{}, fmt.Errorf("manifest bundle: Chunk %s has inconsistent key or plaintext size across Manifests", hex.EncodeToString(key[:]))
	}
	return previous, nil
}

// manifestClosureKeys lists every chunk object a manifest needs: its entries'
// objects and its deltas' bases, each once, in manifest order.
func manifestClosureKeys(manifest *codec.Manifest) []store.ContentKey {
	seen := make(map[store.ContentKey]struct{}, len(manifest.Entries)+len(manifest.Deltas))
	keys := make([]store.ContentKey, 0, len(manifest.Entries)+len(manifest.Deltas))
	add := func(k store.ContentKey) {
		if _, ok := seen[k]; ok {
			return
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	for _, entry := range manifest.Entries {
		if !entry.IsZero {
			add(store.ContentKey(entry.CiphertextHash))
		}
	}
	for _, ref := range manifest.Deltas {
		add(store.ContentKey(ref.Base))
	}
	return keys
}

// baseGetter returns the verified physical bytes of a base object. release
// must be called once the bytes are no longer needed.
type baseGetter func(ctx context.Context, key store.ContentKey) (physical []byte, release func(), err error)

// verifyChunkPlaintext proves that physical (already hash-checked against key)
// decodes under reference: an independent chunk decrypts and decompresses into
// plain; a delta opens, names the expected base, and applies onto its base's
// plaintext into plain; a base-only reference has nothing to decode here (it
// is decoded when one of its dependents is verified).
func verifyChunkPlaintext(ctx context.Context, decryptor manifestcrypto.Decryptor, key store.ContentKey, reference chunkReference, physical, plain []byte, getBase baseGetter) error {
	switch reference.kind {
	case refBase:
		if len(physical) == 0 || physical[0] == manifestcrypto.ChunkFormatAESDelta {
			return fmt.Errorf("manifest bundle: Chunk %s is a delta's base but is itself a delta object", hex.EncodeToString(key[:]))
		}
		return nil
	case refChunk:
		if len(physical) > 0 && physical[0] == manifestcrypto.ChunkFormatAESDelta {
			return fmt.Errorf("manifest bundle: Chunk %s is a delta object but its Manifest entry is not flagged delta", hex.EncodeToString(key[:]))
		}
		if err := decryptor.DecryptChunkTo(ctx, reference.decryptKey, physical, plain); err != nil {
			return fmt.Errorf("manifest bundle: Chunk %s decrypt/decompress: %w", hex.EncodeToString(key[:]), err)
		}
		return nil
	}

	opener, ok := decryptor.(manifestcrypto.DeltaDecryptor)
	if !ok {
		return fmt.Errorf("manifest bundle: Chunk %s is a delta object but the decryptor cannot open deltas", hex.EncodeToString(key[:]))
	}
	opened, err := opener.OpenDeltaChunk(ctx, reference.decryptKey, physical)
	if err != nil {
		return fmt.Errorf("manifest bundle: Chunk %s open delta: %w", hex.EncodeToString(key[:]), err)
	}
	defer clear(opened.BaseKey[:])
	if store.ContentKey(opened.Header.BaseObjectKey) != reference.base || opened.Header.BaseSize != reference.baseSize {
		return fmt.Errorf("manifest bundle: Chunk %s names base %s (%d B), its Manifest says %s (%d B)",
			hex.EncodeToString(key[:]), hex.EncodeToString(opened.Header.BaseObjectKey[:4]), opened.Header.BaseSize,
			hex.EncodeToString(reference.base[:4]), reference.baseSize)
	}
	if opened.Header.TargetSize != reference.plainSize {
		return fmt.Errorf("manifest bundle: Chunk %s delta target size %d, Manifest entry size %d", hex.EncodeToString(key[:]), opened.Header.TargetSize, reference.plainSize)
	}
	baseBytes, release, err := getBase(ctx, reference.base)
	if err != nil {
		return fmt.Errorf("manifest bundle: Chunk %s base %s: %w", hex.EncodeToString(key[:]), hex.EncodeToString(reference.base[:]), err)
	}
	defer release()
	if len(baseBytes) == 0 || baseBytes[0] == manifestcrypto.ChunkFormatAESDelta {
		return fmt.Errorf("manifest bundle: Chunk %s base %s is itself a delta object (depth one)", hex.EncodeToString(key[:]), hex.EncodeToString(reference.base[:]))
	}
	basePlain := make([]byte, reference.baseSize)
	defer clear(basePlain)
	if err := decryptor.DecryptChunkTo(ctx, opened.BaseKey, baseBytes, basePlain); err != nil {
		return fmt.Errorf("manifest bundle: Chunk %s base %s decrypt: %w", hex.EncodeToString(key[:]), hex.EncodeToString(reference.base[:]), err)
	}
	if err := delta.ApplyTo(basePlain, opened.Stream, plain); err != nil {
		return fmt.Errorf("manifest bundle: Chunk %s apply delta: %w", hex.EncodeToString(key[:]), err)
	}
	return nil
}
