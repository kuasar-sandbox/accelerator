package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/delta"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/internal/objectformat"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// DeltaWriter is an optional StoreWriter capability that states whether chunks
// written through it may be stored as delta objects.
//
// A delta object is only meaningful next to the base object it names, so a
// writer that packages chunks into a self-contained unit (a bundle verified for
// completeness) must refuse. Absent the interface, a writer that is NOT an
// OrderedChunkStoreWriter is assumed to be an ordinary store and allowed; an
// OrderedChunkStoreWriter must opt in explicitly, because the local-bundle
// writers are ordered and an ordered writer is not necessarily a bundle (the
// transfer writer is ordered either way and only sometimes writes a bundle).
type DeltaWriter interface {
	SupportsDelta() bool
}

// Resolver reports how the store holds a chunk, given the address H_ind it
// would have as an independent object: absent, as that independent object, or
// as a delta object that an alias names. A store that cannot answer returns
// store.ErrUnsupported.
//
// Delta objects are addressed by the hash of their patch, so two nodes that
// encode the same chunk against different bases would otherwise publish
// different objects and different manifests. Resolve is how a node learns that
// another already decided.
type Resolver interface {
	Resolve(ctx context.Context, generation store.Generation, hInd store.ContentKey) (store.Resolved, error)
}

// RepresentingWriter stores a chunk-partition delta object and, in the same
// call, asks the store to record it as the representation of the chunk whose
// independent address is represents. The store decides once; the result says
// whether this object won, lost to another delta object (PutResult.Key), or is
// moot because the independent object exists.
type RepresentingWriter interface {
	PutRepresenting(ctx context.Context, admission store.WriteAdmission, key store.ContentKey, data []byte, represents store.ContentKey) (store.PutResult, error)
}

// ObjectReader is a StoreWriter capability. Delta read-back uses it to fetch a
// base object that is no longer in the plaintext pool, and adoption uses it to
// fetch another writer's delta object. A miss is (false, nil, nil). An error
// means the read failed and must not be treated as a miss, because a miss drops
// the base from the index.
type ObjectReader interface {
	Get(ctx context.Context, p store.Partition, key store.ContentKey) (bool, []byte, error)
}

// DeltaSupport reports why delta compression cannot be used with this writer
// and encryptor, or nil if it can. It exists so callers can say why a
// configured feature is inactive instead of silently dropping it.
//
// Beyond refusing bundle writers, it requires everything cross-node
// coordination rests on: Resolve and PutRepresenting so replicas converge on
// one representation per chunk, and Get plus the ability to open delta objects
// so a node can verify another node's representation before adopting it. A
// store that implements the interfaces but not the RPCs behind them is detected
// at run time (see Engine.AliasUnsupported).
func DeltaSupport(sw StoreWriter, enc crypto.Encryptor) error {
	if _, ok := enc.(crypto.DeltaEncryptor); !ok {
		return errors.New("the chunk encryptor does not support delta objects")
	}
	if _, ok := enc.(crypto.DeltaDecryptor); !ok {
		return errors.New("the chunk encryptor cannot open delta objects, which adopting another writer's representation requires")
	}
	if _, ok := enc.(crypto.Decryptor); !ok {
		return errors.New("the chunk encryptor cannot decrypt chunks, which reading a base back requires")
	}
	if dw, ok := sw.(DeltaWriter); ok {
		if !dw.SupportsDelta() {
			return errors.New("the store writer packages chunks into a self-contained bundle, where a delta object's base may be absent")
		}
	} else if _, ordered := sw.(OrderedChunkStoreWriter); ordered {
		return errors.New("the store writer is an ordered (bundle) writer that has not opted in to delta objects")
	}
	if _, ok := sw.(Resolver); !ok {
		return errors.New("the store writer cannot resolve delta aliases (Resolve), so replicas could not agree on a chunk's representation")
	}
	if _, ok := sw.(RepresentingWriter); !ok {
		return errors.New("the store writer cannot record a delta object as a chunk's representation (PutRepresenting)")
	}
	if _, ok := sw.(ObjectReader); !ok {
		return errors.New("the store writer cannot read objects back, which adopting another writer's delta object requires")
	}
	return nil
}

// NewIngesterWithDelta is NewIngester plus delta compression of unique chunks
// against similar, previously stored independent chunks.
//
// eng is shared process-wide state (see delta.Engine) and must outlive the
// ingester. A nil eng, a disabled configuration, or a writer or encryptor that
// cannot support delta objects yields exactly the ingester NewIngester returns:
// delta is an optimization that is never required.
func NewIngesterWithDelta(keyFn CustomerKeyFunc, extraSaltFn ExtraSaltFunc, sw StoreWriter, chk chunker.Chunker, enc crypto.Encryptor, eng *delta.Engine) Ingester {
	ing := NewIngester(keyFn, extraSaltFn, sw, chk, enc).(*ingester)
	if eng == nil || !eng.Params().Enabled || DeltaSupport(sw, enc) != nil {
		return ing
	}
	ing.delta = &deltaRuntime{
		eng:      eng,
		p:        eng.Params(),
		indep:    enc,
		enc:      enc.(crypto.DeltaEncryptor),
		dec:      enc.(crypto.Decryptor),
		open:     enc.(crypto.DeltaDecryptor),
		resolver: sw.(Resolver),
		writer:   sw.(RepresentingWriter),
		reader:   sw.(ObjectReader),
		readSem:  semaphore.NewWeighted(int64(eng.Params().MaxReadbacks)),
	}
	return ing
}

type deltaRuntime struct {
	eng      *delta.Engine
	p        delta.Params
	indep    crypto.Encryptor // independent objects: used to validate an adopted base
	enc      crypto.DeltaEncryptor
	dec      crypto.Decryptor
	open     crypto.DeltaDecryptor
	resolver Resolver
	writer   RepresentingWriter
	reader   ObjectReader
	readSem  *semaphore.Weighted
}

// deltaCounters are per-Ingest diagnostics, updated concurrently by workers.
type deltaCounters struct {
	attempts     atomic.Uint32 // chunks for which at least one candidate was encoded
	noCandidate  atomic.Uint32 // eligible chunks with no usable candidate
	notResident  atomic.Uint32 // index hits whose plaintext was not in the pool
	readbacks    atomic.Uint32 // of those, bases fetched from the store and decrypted
	rejected     atomic.Uint32 // attempted, but no delta met the acceptance rule
	knownSkips   atomic.Uint32 // skipped: already stored independently
	repHits      atomic.Uint32 // delta chunks resolved from the representation index
	repStale     atomic.Uint32 // representation hits dropped because the store disagreed
	timeouts     atomic.Uint32 // attempts cut short by the per-chunk deadline
	encodeErrors atomic.Uint32 // unexpected failures that fell back to an independent object

	resolves    atomic.Uint32 // Resolve calls made
	adopted     atomic.Uint32 // chunks that took another writer's verified delta object
	adoptFailed atomic.Uint32 // aliases that could not be adopted (unusable or unverifiable)
	aliasWon    atomic.Uint32 // delta objects this ingest's Puts made the representation
	aliasLost   atomic.Uint32 // delta objects another writer's alias beat
	aliasIndep  atomic.Uint32 // delta objects mooted by an independent object (older servers)
	orphanBytes atomic.Uint64 // bytes of delta objects we uploaded that nothing references and the store kept

	discardedBytes atomic.Uint64 // bytes of delta objects we uploaded that lost and the store deleted again
}

// deltaOutcome is what tryDelta decides for one chunk.
type deltaOutcome struct {
	// repHit: the chunk is already stored as the delta object named by rep (from
	// the local index, or adopted from the store's alias); no Put is needed. key
	// is its key-table key.
	repHit bool
	rep    delta.RepEntry

	// object, when non-nil, is a new delta object to Put instead of the
	// independent one.
	object   []byte
	hash     [32]byte
	key      [32]byte
	baseKey  [32]byte // the base's object key
	baseSize uint32   // the base's plaintext size

	// registerBase: after the independent object is stored, make this chunk
	// available as a future base, with this sketch.
	registerBase bool
	sketch       delta.Sketch
}

// tryDelta decides whether the chunk (plain, whose independent object is indep
// with address hInd) should be stored as a delta. It never fails the ingest:
// any problem means "store it independently".
//
// The store, not this process, has the last word on whether the chunk already
// has a representation: Resolve is asked first, and an answer from another
// writer is adopted (after verification) instead of encoding a second patch.
func (r *deltaRuntime) tryDelta(ctx context.Context, dom *delta.Domain, gen store.Generation, salt [32]byte, plain, indep []byte, hInd [32]byte, c *deltaCounters) (out deltaOutcome) {
	// A delta object must save 4 KiB and carries 78 bytes of overhead, so a
	// chunk whose independent object is smaller can never qualify; and very
	// large chunks are excluded by configuration to bound per-chunk work.
	if uint64(len(indep)) < objectformat.DeltaMinSavings+crypto.DeltaOverhead || len(plain) > int(r.p.MaxChunkSize) {
		return out
	}
	// Without Resolve, replicas cannot agree on a representation, so no delta
	// object is written and none is trusted.
	if r.eng.AliasUnsupported() {
		return out
	}

	useRep := func(rep delta.RepEntry) deltaOutcome {
		c.repHits.Add(1)
		out.repHit = true
		out.rep = rep
		out.key = crypto.DeriveDeltaKey(salt, rep.BaseObjectKey, plain)
		return out
	}

	// 1. What do we already believe about this chunk?
	rep, haveRep := dom.LookupRep(hInd)
	if haveRep && !rep.Complete() {
		// Loaded from a snapshot that predates BaseSize: the manifest's delta
		// table needs it, so the entry is only a hint. Resolve below re-learns
		// the representation (with its base size) from the object itself.
		dom.ForgetRep(hInd)
		haveRep = false
	}
	if !haveRep && dom.Known(hInd) {
		// Stored independently: the independent Put will dedup, and re-deltaing a
		// chunk we already hold whole would only create a second copy of it.
		c.knownSkips.Add(1)
		return out
	}
	if haveRep && !r.p.VerifyHit {
		return useRep(rep)
	}

	// 2. What does the store say? This is the cross-node step: it is what lets a
	// replica that never saw this chunk, or lost its soft state, still produce
	// the manifest its peers did.
	resolved, ok := r.resolve(ctx, gen, hInd, c)
	if !ok {
		return out // cannot tell; never trust a guess about a shared store
	}
	poisoned := false
	switch resolved.Resolution {
	case store.ResolutionIndependent:
		if haveRep {
			dom.ForgetRep(hInd)
			c.repStale.Add(1)
		}
		dom.MarkKnown(hInd)
		c.knownSkips.Add(1)
		// Another writer stored it, so this process has never seen the chunk. It
		// is still a fine base for later chunks, exactly as if we had stored it
		// ourselves; skipping that would leave a node that only ever meets
		// already-stored chunks with nothing to delta against, and it would then
		// store independently what its peers store as deltas.
		out.sketch = r.eng.Sketcher().Sketch(plain)
		out.registerBase = true
		return out
	case store.ResolutionDelta:
		if haveRep && rep.DeltaObjectKey == [32]byte(resolved.DeltaKey) {
			return useRep(rep)
		}
		if haveRep {
			dom.ForgetRep(hInd)
			c.repStale.Add(1)
		}
		entry, key, verdict := r.adopt(ctx, dom, salt, plain, indep, hInd, resolved.DeltaKey, c)
		switch verdict {
		case adoptOK:
			out.repHit = true
			out.rep = entry
			out.key = key
			return out
		case adoptBad:
			// The alias points at something that does not reproduce this chunk.
			// Storing the chunk independently supersedes it (Resolve prefers the
			// independent object), so the next writer is not misled again.
			poisoned = true
		}
		// Inconclusive (a read failed): fall through and encode our own delta.
		// If the alias is good we lose the race below, the server discards our
		// object and putDelta stores the chunk independently; nothing is
		// superseded on a transient error, and nothing unnamed is referenced.
	default: // absent
		if haveRep {
			dom.ForgetRep(hInd)
			c.repStale.Add(1)
		}
	}

	// 3. Similarity. The sketch is needed whether or not a delta results: it is
	// how this chunk becomes a base for later ones.
	sk := r.eng.Sketcher().Sketch(plain)
	out.sketch = sk
	out.registerBase = true
	if poisoned {
		return out
	}

	cands, notResident := dom.Candidates(sk)
	if notResident > 0 {
		c.notResident.Add(uint32(notResident))
	}
	if len(cands) == 0 {
		c.noCandidate.Add(1)
		return out
	}

	// 4. Encode against candidates (best estimated similarity first) within the
	// per-chunk budget; keep the smallest stream. The estimate only ranks and
	// filters: actually encoding is what measures the saving.
	var (
		best       []byte
		bestCand   delta.Candidate
		start      = time.Now()
		encodedAny bool
	)
	for n, cand := range cands {
		if n > 0 && time.Since(start) > r.p.AttemptDeadline {
			c.timeouts.Add(1)
			break
		}
		if ctx.Err() != nil {
			return out
		}
		base, release, err := r.leaseBase(ctx, dom, cand, false, c)
		if err != nil {
			if ctx.Err() != nil {
				return out
			}
			c.encodeErrors.Add(1)
			log.Printf("delta: base %x: %v", cand.ObjectKey[:4], err)
			continue
		}
		if base == nil {
			continue
		}
		stream := delta.Encode(base, plain)
		release()
		encodedAny = true
		// Cheap pre-filter: the stream alone must already undercut the
		// independent object by the delta overhead.
		if uint64(len(stream))+crypto.DeltaOverhead >= uint64(len(indep)) {
			continue
		}
		if best == nil || len(stream) < len(best) {
			best, bestCand = stream, cand
		}
	}
	if encodedAny {
		c.attempts.Add(1)
	}
	if best == nil {
		if encodedAny {
			c.rejected.Add(1)
		}
		return out
	}

	object, hash, key, err := r.enc.EncryptDeltaChunk(ctx, salt, crypto.DeltaBase{
		ObjectKey:     bestCand.ObjectKey,
		Key:           bestCand.BaseKey,
		PlaintextSize: int(bestCand.Size),
	}, plain, best)
	if err != nil {
		if !errors.Is(err, crypto.ErrDeltaNotApplicable) && ctx.Err() == nil {
			c.encodeErrors.Add(1)
		}
		return out
	}
	// 5. The canonical acceptance rule: at least 4 KiB and 20% saved.
	if !objectformat.DeltaBeneficial(uint64(len(indep)), uint64(len(object))) {
		c.rejected.Add(1)
		return out
	}

	out.object, out.hash, out.key = object, hash, key
	out.baseKey, out.baseSize = bestCand.ObjectKey, bestCand.Size
	out.registerBase = false // depth one: a delta is never itself a base
	return out
}

// resolve asks the store how it holds the chunk. ok=false means "no usable
// answer": either the store cannot resolve at all (which disables delta for the
// process, once, with a log line) or the call failed.
func (r *deltaRuntime) resolve(ctx context.Context, gen store.Generation, hInd [32]byte, c *deltaCounters) (store.Resolved, bool) {
	c.resolves.Add(1)
	res, err := r.resolver.Resolve(ctx, gen, store.ContentKey(hInd))
	if err == nil {
		return res, true
	}
	switch {
	case errors.Is(err, store.ErrUnsupported):
		if r.eng.MarkAliasUnsupported() {
			log.Printf("delta: the store cannot resolve delta aliases (it predates Resolve or its backend has none); " +
				"delta compression is disabled for this process because replicas could not agree on a chunk's representation")
		}
	case ctx.Err() == nil:
		c.encodeErrors.Add(1)
	}
	return store.Resolved{}, false
}

type adoptVerdict int

const (
	// adoptOK: the alias names a delta object that reproduces this chunk.
	adoptOK adoptVerdict = iota
	// adoptBad: it provably does not (missing, malformed, wrong content).
	adoptBad
	// adoptInconclusive: it could not be checked (a read failed, a base is gone).
	adoptInconclusive
)

// errBaseRejected marks a base that decrypted but is not the object it claims to
// be, so the key that produced it cannot be trusted.
var errBaseRejected = errors.New("base object does not match its key")

// adopt takes another writer's delta object as this chunk's representation,
// after proving that it reproduces plain. The store does not check delta
// objects, so a writer's say-so is not enough: we fetch the object, re-derive
// K_d from the plaintext we hold, open it, rebuild the chunk from the base and
// compare. Only then is it recorded and used.
//
// The base key inside the delta object is untrusted input too. It is validated
// before the base is admitted to the shared pool (a wrong key must not poison
// the pool entry for a real object).
func (r *deltaRuntime) adopt(ctx context.Context, dom *delta.Domain, salt [32]byte, plain, indep []byte, hInd, deltaKey [32]byte, c *deltaCounters) (delta.RepEntry, [32]byte, adoptVerdict) {
	bad := func(format string, args ...any) (delta.RepEntry, [32]byte, adoptVerdict) {
		c.adoptFailed.Add(1)
		log.Printf("delta: alias for chunk %x names delta object %x which cannot be adopted: %s", hInd[:4], deltaKey[:4], fmt.Sprintf(format, args...))
		return delta.RepEntry{}, [32]byte{}, adoptBad
	}
	unsure := func(err error) (delta.RepEntry, [32]byte, adoptVerdict) {
		c.adoptFailed.Add(1)
		if ctx.Err() == nil {
			c.encodeErrors.Add(1)
			log.Printf("delta: cannot check alias target %x for chunk %x: %v", deltaKey[:4], hInd[:4], err)
		}
		return delta.RepEntry{}, [32]byte{}, adoptInconclusive
	}

	object, err := r.readObject(ctx, deltaKey)
	if err != nil {
		return unsure(err)
	}
	if object == nil {
		return bad("object is missing")
	}
	if sha256.Sum256(object) != deltaKey {
		return bad("object does not hash to its key")
	}
	hdr, err := crypto.ParseDeltaHeader(object)
	if err != nil {
		return bad("%v", err)
	}
	if hdr.TargetSize != uint32(len(plain)) {
		return bad("target size %d, chunk is %d", hdr.TargetSize, len(plain))
	}
	if !objectformat.DeltaBeneficial(uint64(len(indep)), uint64(len(object))) {
		return bad("does not meet the acceptance rule")
	}
	kd := crypto.DeriveDeltaKey(salt, hdr.BaseObjectKey, plain)
	opened, err := r.open.OpenDeltaChunk(ctx, kd, object)
	if err != nil {
		if ctx.Err() != nil {
			return unsure(err)
		}
		return bad("open: %v", err)
	}
	base, release, err := r.leaseBase(ctx, dom, delta.Candidate{
		ObjectKey: hdr.BaseObjectKey,
		BaseKey:   opened.BaseKey,
		Size:      hdr.BaseSize,
	}, true, c)
	switch {
	case errors.Is(err, errBaseRejected):
		return bad("%v", err)
	case err != nil:
		return unsure(err)
	case base == nil:
		return unsure(errors.New("base object is unavailable"))
	}
	rebuilt := make([]byte, len(plain))
	applyErr := delta.ApplyTo(base, opened.Stream, rebuilt)
	release()
	defer clear(rebuilt)
	if applyErr != nil {
		return bad("%v", applyErr)
	}
	if !bytes.Equal(rebuilt, plain) {
		return bad("does not reproduce the chunk")
	}

	entry := delta.RepEntry{DeltaObjectKey: deltaKey, BaseObjectKey: hdr.BaseObjectKey, ObjectSize: uint32(len(object)), BaseSize: hdr.BaseSize}
	dom.RecordRep(hInd, entry)
	c.adopted.Add(1)
	return entry, kd, adoptOK
}

// readObject fetches one chunk-partition object under the read-back limit. A
// miss is (nil, nil).
func (r *deltaRuntime) readObject(ctx context.Context, key [32]byte) ([]byte, error) {
	if err := r.readSem.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer r.readSem.Release(1)
	found, object, err := r.reader.Get(ctx, store.PartitionChunk, store.ContentKey(key))
	if err != nil || !found {
		return nil, err
	}
	return object, nil
}

// leaseBase returns a read-only view of a candidate's plaintext. A pool miss
// is read back from the store: the object is verified, decrypted with the key
// the caller supplied, and admitted to the pool so the next chunk does not fetch
// it again. A nil buffer with a nil error means "skip this candidate".
//
// The key and size normally come from our own index and are trusted. With
// verify, they came from a delta object someone else wrote, so the plaintext,
// whether pooled or read back, must re-encrypt to the object's key under the
// candidate's base key before it is used; otherwise errBaseRejected.
func (r *deltaRuntime) leaseBase(ctx context.Context, dom *delta.Domain, cand delta.Candidate, verify bool, c *deltaCounters) ([]byte, func(), error) {
	if plain, release, ok := dom.AcquireBase(cand.ObjectKey); ok {
		if uint32(len(plain)) != cand.Size {
			release()
			if verify {
				return nil, nil, fmt.Errorf("%w: base is %d bytes, header claims %d", errBaseRejected, len(plain), cand.Size)
			}
			return nil, nil, fmt.Errorf("pooled base is %d bytes, index says %d", len(plain), cand.Size)
		}
		if verify {
			if err := r.verifyBase(ctx, dom, cand, plain); err != nil {
				release()
				return nil, nil, err
			}
		}
		return plain, release, nil
	}
	object, err := r.readObject(ctx, cand.ObjectKey)
	if err != nil {
		return nil, nil, err
	}
	if object == nil {
		dom.EvictBase(cand.ObjectKey)
		return nil, nil, nil
	}
	if sha256.Sum256(object) != cand.ObjectKey {
		return nil, nil, fmt.Errorf("content hash mismatch")
	}
	if len(object) == 0 || object[0] == crypto.ChunkFormatAESDelta {
		// Depth one: a delta is never a base. Drop the stale index entry.
		dom.EvictBase(cand.ObjectKey)
		return nil, nil, nil
	}
	if cand.Size == 0 || int(cand.Size) > crypto.MaxChunkDecodedSize {
		return nil, nil, fmt.Errorf("implausible base size %d", cand.Size)
	}
	plain := make([]byte, cand.Size)
	if err := r.dec.DecryptChunkTo(ctx, cand.BaseKey, object, plain); err != nil {
		clear(plain)
		if verify && ctx.Err() == nil {
			return nil, nil, fmt.Errorf("%w: %v", errBaseRejected, err)
		}
		return nil, nil, err
	}
	if verify {
		if err := r.verifyBase(ctx, dom, cand, plain); err != nil {
			clear(plain)
			return nil, nil, err
		}
	}
	c.readbacks.Add(1)
	buf := append([]byte(nil), plain...)
	clear(plain)
	leased, release, ok := dom.PutLeased(cand.ObjectKey, buf)
	if !ok {
		return buf, func() { clear(buf) }, nil
	}
	if len(leased) == 0 || &leased[0] != &buf[0] {
		clear(buf)
	}
	return leased, release, nil
}

// verifyBase proves that plain is the plaintext of the independent object
// cand.ObjectKey under this domain's salt and that cand.BaseKey is the key that
// decrypts it: encrypting plain again must reproduce the object's address and
// derive the same key. Otherwise errBaseRejected.
func (r *deltaRuntime) verifyBase(ctx context.Context, dom *delta.Domain, cand delta.Candidate, plain []byte) error {
	_, again, key, err := r.indep.EncryptChunk(ctx, dom.Salt(), plain)
	if err != nil {
		return err
	}
	if again != cand.ObjectKey || key != cand.BaseKey {
		return errBaseRejected
	}
	return nil
}

// deltaPut is what the store ended up holding for a chunk this node encoded as a
// delta, after reconciling with whatever representation the store already had.
type deltaPut struct {
	isNew       bool           // the object we uploaded was new to the store
	written     uint64         // bytes we uploaded
	hash, key   [32]byte       // what the manifest entry must record
	objectSize  uint32         // size of the recorded object
	isDelta     bool           // the recorded object is a delta object
	independent bool           // the chunk turned out to be stored as the independent object
	rep         delta.RepEntry // when isDelta: the recorded delta's base, for the manifest's delta table
}

// putDelta uploads the delta object and asks the store to make it the chunk's
// representation. The store answers once for everyone, and the manifest may
// record our object only if the store's alias names it:
//
//   - won: ours is the representation; record it.
//   - lost: another delta object is. The store deleted our upload if it was
//     new (nothing may reference an object the alias does not name), and we
//     adopt theirs after verifying it so our manifest matches our peers'. If
//     theirs cannot be verified we store the chunk independently: an
//     independent object is never deleted, so it is always safe to reference.
//   - independent (older servers only): the chunk exists whole; record that.
//   - none: the store could not record anything. Store independently; our
//     upload stays as an orphan, the one kind this design still produces.
//
// Soft state is recorded here, after the Put, so a failed upload leaves none.
// putIndep stores the independent object through the ingester's normal path.
func (r *deltaRuntime) putDelta(ctx context.Context, dom *delta.Domain, admission store.WriteAdmission, salt [32]byte, o *deltaOutcome, plain, indep []byte, hInd, indepKey [32]byte, putIndep func(context.Context) (bool, error), c *deltaCounters) (deltaPut, error) {
	res, err := r.writer.PutRepresenting(ctx, admission, store.ContentKey(o.hash), o.object, store.ContentKey(hInd))
	if err != nil {
		return deltaPut{}, err
	}
	own := delta.RepEntry{DeltaObjectKey: o.hash, BaseObjectKey: o.baseKey, ObjectSize: uint32(len(o.object)), BaseSize: o.baseSize}
	dp := deltaPut{
		isNew: res.IsNew, written: uint64(len(o.object)),
		hash: o.hash, key: o.key, objectSize: uint32(len(o.object)), isDelta: true, rep: own,
	}
	// Our object is not the representation: account for what became of it.
	notOurs := func() {
		switch {
		case res.Discarded:
			c.discardedBytes.Add(uint64(len(o.object)))
		case res.IsNew:
			c.orphanBytes.Add(uint64(len(o.object)))
		}
		dp.isNew, dp.written = false, 0
	}
	independent := func() (deltaPut, error) {
		isNew, err := putIndep(ctx)
		if err != nil {
			return deltaPut{}, err
		}
		dom.MarkKnown(hInd)
		o.registerBase = true // the independent object is a fine base for later chunks
		dp.hash, dp.key, dp.objectSize = hInd, indepKey, uint32(len(indep))
		dp.isDelta, dp.independent = false, true
		dp.isNew = isNew
		if isNew {
			dp.written = uint64(len(indep))
		}
		return dp, nil
	}
	switch res.Represented {
	case store.RepresentedLost:
		c.aliasLost.Add(1)
		notOurs()
		entry, key, verdict := r.adopt(ctx, dom, salt, plain, indep, hInd, [32]byte(res.Key), c)
		if verdict == adoptOK {
			dp.hash, dp.key, dp.objectSize, dp.rep = entry.DeltaObjectKey, key, entry.ObjectSize, entry
			return dp, nil
		}
		return independent()
	case store.RepresentedIndependent:
		c.aliasIndep.Add(1)
		notOurs()
		dom.MarkKnown(hInd)
		o.registerBase = true
		dp.hash, dp.key, dp.objectSize = hInd, indepKey, uint32(len(indep))
		dp.isDelta, dp.independent = false, true
		return dp, nil
	case store.RepresentedWon:
		c.aliasWon.Add(1)
		dom.RecordRep(hInd, own)
		return dp, nil
	default:
		// Nothing recorded: the store has aliases (Resolve worked) but this
		// alias step failed. Our object is valid but unnamed, and an unnamed
		// delta object must not be referenced.
		c.encodeErrors.Add(1)
		notOurs()
		return independent()
	}
}

// afterIndependentPut records a chunk that was stored as an ordinary object so
// later chunks can find it. plain's ownership passes to the base pool when it
// is registered: the caller must not touch plain afterwards.
func (r *deltaRuntime) afterIndependentPut(dom *delta.Domain, o deltaOutcome, hInd [32]byte, key [32]byte, plain []byte) {
	if !o.registerBase {
		return
	}
	dom.MarkKnown(hInd)
	dom.RegisterBase(delta.BaseInfo{ObjectKey: hInd, BaseKey: key, Size: uint32(len(plain))}, plain, o.sketch)
}

func (r *deltaRuntime) flush() {
	if err := r.eng.MaybeSnapshot(); err != nil {
		log.Printf("delta: snapshot failed (state is soft; continuing): %v", err)
	}
}

func validateDeltaObject(object []byte, plaintextSize uint32) error {
	h, err := crypto.ParseDeltaHeader(object)
	if err != nil {
		return err
	}
	if h.TargetSize != plaintextSize {
		return fmt.Errorf("delta target size %d, want %d", h.TargetSize, plaintextSize)
	}
	return nil
}
