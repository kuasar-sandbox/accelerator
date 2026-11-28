package fetch

import (
	"context"
	"crypto/sha256"
	"fmt"

	"golang.org/x/sync/semaphore"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/delta"
	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

const (
	defaultBaseCacheBytes = 64 << 20
	defaultScratchBytes   = 128 << 20
)

// deltaScratch bounds the extra memory a delta apply holds at once (the base
// plaintext plus the decoded VCDIFF stream), separate from the crypto scratch
// budget. The caller's destination buffer is not charged: the caller owns it.
type deltaScratch struct {
	sem    *semaphore.Weighted
	budget int64
}

func newDeltaScratch(budget int64) *deltaScratch {
	if budget <= 0 {
		budget = defaultScratchBytes
	}
	return &deltaScratch{sem: semaphore.NewWeighted(budget), budget: budget}
}

func (s *deltaScratch) acquire(ctx context.Context, n int64) error {
	if n < 0 {
		return fmt.Errorf("negative charge")
	}
	if n > s.budget {
		return fmt.Errorf("need %d bytes, budget %d", n, s.budget)
	}
	return s.sem.Acquire(ctx, n)
}

func (s *deltaScratch) release(n int64) {
	if n > 0 {
		s.sem.Release(n)
	}
}

// materializeDelta turns one 0x03 object into plaintext in dst, which must be
// exactly the entry's plaintext size. The base is fetched, checked to be an
// independent chunk (depth one), decrypted, and cached.
func (s *manifestStream) materializeDelta(ctx context.Context, chunkIdx uint64, e codec.ChunkEntry, ciphertext, dst []byte) error {
	if uint32(len(dst)) != e.Size {
		return readerr.Mark(fmt.Errorf("chunk %d: delta destination is %d bytes, entry size %d", chunkIdx, len(dst), e.Size), false)
	}
	opener, ok := s.decryptor.(crypto.DeltaDecryptor)
	if !ok {
		return readerr.Mark(fmt.Errorf("chunk %d: decryptor cannot open delta objects", chunkIdx), false)
	}
	// The 46-byte header is cleartext, so the scratch charge is known before
	// OpenDeltaChunk decrypts the payload and allocates the decoded stream.
	hdr, err := crypto.ParseDeltaHeader(ciphertext)
	if err != nil {
		return fmt.Errorf("chunk %d: open delta: %w", chunkIdx, err)
	}
	if hdr.TargetSize != e.Size {
		return readerr.Mark(fmt.Errorf("chunk %d: delta target size %d, entry size %d", chunkIdx, hdr.TargetSize, e.Size), false)
	}
	// The header is unauthenticated when content verification is off; the
	// manifest's delta table is covered by the key table's AAD.
	ref, ok := s.m.DeltaFor(int(chunkIdx))
	if !ok {
		return readerr.Mark(fmt.Errorf("chunk %d: delta entry has no delta table row", chunkIdx), false)
	}
	if hdr.BaseObjectKey != ref.Base || hdr.BaseSize != ref.BaseSize {
		return readerr.Mark(fmt.Errorf("chunk %d: delta names base %x (%d B), manifest says %x (%d B)",
			chunkIdx, hdr.BaseObjectKey[:4], hdr.BaseSize, ref.Base[:4], ref.BaseSize), false)
	}
	charge := int64(hdr.BaseSize) + int64(hdr.StreamSize)
	if err := s.scratch.acquire(ctx, charge); err != nil {
		return fmt.Errorf("chunk %d: delta scratch: %w", chunkIdx, err)
	}
	defer s.scratch.release(charge)

	opened, err := opener.OpenDeltaChunk(ctx, s.keys[chunkIdx], ciphertext)
	if err != nil {
		return fmt.Errorf("chunk %d: open delta: %w", chunkIdx, err)
	}
	defer clear(opened.BaseKey[:])
	defer clearSlice(opened.Stream)
	if uint32(len(opened.Stream)) != hdr.StreamSize {
		return readerr.Mark(fmt.Errorf("chunk %d: decoded delta stream is %d bytes, header says %d", chunkIdx, len(opened.Stream), hdr.StreamSize), false)
	}

	base, release, err := s.basePlaintext(ctx, chunkIdx, opened.Header.BaseObjectKey, opened.BaseKey, int(opened.Header.BaseSize))
	if err != nil {
		return err
	}
	defer release()
	if err := delta.ApplyTo(base, opened.Stream, dst); err != nil {
		return fmt.Errorf("chunk %d: apply delta: %w", chunkIdx, err)
	}
	return nil
}

// basePlaintext leases the plaintext of the base a delta names, decrypted with
// the key and size that delta carries. The cache entry is specific to that
// (object, key, size) triple: see baseCache for why the object address alone
// would let one bad delta poison every reader of the base.
func (s *manifestStream) basePlaintext(ctx context.Context, chunkIdx uint64, objectKey, baseKey [32]byte, size int) ([]byte, func(), error) {
	if size <= 0 || size > crypto.MaxChunkDecodedSize {
		return nil, nil, readerr.Mark(fmt.Errorf("chunk %d: base plaintext size %d is implausible", chunkIdx, size), false)
	}
	id := baseID{objectKey: objectKey, baseKey: baseKey, size: uint32(size)}
	return s.baseCache.getOrLoad(ctx, id, func(ctx context.Context) ([]byte, error) {
		object, err := s.getChunkObject(ctx, chunkIdx, store.ContentKey(objectKey))
		if err != nil {
			return nil, err
		}
		if len(object) == 0 || object[0] == crypto.ChunkFormatAESDelta {
			return nil, readerr.Mark(fmt.Errorf("chunk %d: base %x is itself a delta (depth one)", chunkIdx, objectKey[:4]), false)
		}
		plain := make([]byte, size)
		if err := s.decryptor.DecryptChunkTo(ctx, baseKey, object, plain); err != nil {
			clearSlice(plain)
			return nil, fmt.Errorf("chunk %d: decrypt base: %w", chunkIdx, err)
		}
		return plain, nil
	})
}

// getChunkObject fetches one chunk object and, when verification is on, checks
// that its bytes hash to key. The returned slice is owned by the caller.
func (s *manifestStream) getChunkObject(ctx context.Context, chunkIdx uint64, key store.ContentKey) ([]byte, error) {
	if s.onDemandGetter == nil {
		return nil, readerr.Mark(fmt.Errorf("chunk %d: cache getter is nil", chunkIdx), false)
	}
	result, blob, err := s.onDemandGetter.Get(ctx, store.PartitionChunk, key)
	if err != nil {
		if blob != nil {
			blob.Release()
		}
		return nil, fmt.Errorf("chunk %d: %w", chunkIdx, err)
	}
	if result != cache.CacheHit || blob == nil {
		if blob != nil {
			blob.Release()
		}
		return nil, readerr.Mark(fmt.Errorf("chunk %d: base not found", chunkIdx), false)
	}
	raw := blob.Bytes()
	if s.verifyContent && sha256.Sum256(raw) != key {
		blob.Release()
		return nil, readerr.Mark(fmt.Errorf("chunk %d: base content hash mismatch (corrupt or tampered store/cache)", chunkIdx), false)
	}
	out := append([]byte(nil), raw...)
	blob.Release()
	return out, nil
}
