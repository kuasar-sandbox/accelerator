package delta

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// Snapshot persists the parts of the state that stay valid across a restart:
// per domain, the representation index, the known-independent filter, and the
// similarity index. The base pool is not persisted. After a restart a base is
// usable again once its object is read back from the store.
//
// File layout (little endian):
//
//	"KDLTSNP1"  version u32 (= snapVersion)  fileID [16]
//	sealed (see seal.go):
//	  params fingerprint [32]  ndomains u32
//	  per domain: salt [32] | bloom count u64 | bloom words u64[] |
//	              nrep u64 | nrep * (hInd[32] deltaKey[32] baseKey[32] size u32 baseSize u32)
//	              nbases u64 | bases (objectKey[32] K_B[32] size u32 sketch...)
//
// The body is sealed because the base records carry K_B, the convergent key
// that decrypts each base chunk. A file is trusted only if its version is
// snapVersion, every frame authenticates, and the parameter fingerprint
// equals the running one; otherwise it is discarded whole. It is written to a
// temp file and renamed, so a crash leaves the previous snapshot.
const (
	snapMagic   = "KDLTSNP1"
	snapVersion = 3

	repRecordLen = 104 // hInd ‖ deltaObjectKey ‖ baseObjectKey ‖ objectSize u32 ‖ baseSize u32
)

// ErrSnapshotMismatch means the file is intact but was written under different
// sketch parameters, so its entries would be misread.
var ErrSnapshotMismatch = errors.New("delta: snapshot parameters differ from running configuration")

// ErrSnapshotCorrupt means the file failed structural or authentication checks.
var ErrSnapshotCorrupt = errors.New("delta: snapshot is corrupt")

// Snapshot writes the current state. It is a no-op without a configured path.
func (e *Engine) Snapshot() error {
	if e.p.SnapshotPath == "" {
		return nil
	}
	e.snapMu.Lock()
	defer e.snapMu.Unlock()
	return e.snapshotLocked()
}

// MaybeSnapshot snapshots when state changed and either nothing has been
// written yet by this process or the interval has elapsed. The first call after
// start therefore always persists, which is what a short-lived single-image
// process needs; a long-lived one is rate-limited.
func (e *Engine) MaybeSnapshot() error {
	if e.p.SnapshotPath == "" || !e.dirty.Load() {
		return nil
	}
	e.snapMu.Lock()
	defer e.snapMu.Unlock()
	if !e.lastSnap.IsZero() && e.now().Sub(e.lastSnap) < e.p.SnapshotInterval {
		return nil
	}
	return e.snapshotLocked()
}

// Close writes a final snapshot if state changed since the last one.
func (e *Engine) Close() error {
	if e.p.SnapshotPath == "" || !e.dirty.Load() {
		return nil
	}
	e.snapMu.Lock()
	defer e.snapMu.Unlock()
	return e.snapshotLocked()
}

func (e *Engine) snapshotLocked() error {
	// Clear before collecting: a change racing the write re-marks dirty and is
	// picked up next time rather than lost.
	e.dirty.Store(false)

	e.mu.Lock()
	doms := make([]*Domain, 0, len(e.domains))
	for _, d := range e.domains {
		doms = append(doms, d)
	}
	e.mu.Unlock()
	sort.Slice(doms, func(i, j int) bool { return bytes.Compare(doms[i].salt[:], doms[j].salt[:]) < 0 })

	dir := filepath.Dir(e.p.SnapshotPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		e.dirty.Store(true)
		return fmt.Errorf("delta: snapshot: %w", err)
	}
	f, err := os.CreateTemp(dir, ".delta-snapshot-*")
	if err != nil {
		e.dirty.Store(true)
		return fmt.Errorf("delta: snapshot: %w", err)
	}
	tmp := f.Name()
	fail := func(err error) error {
		f.Close()
		os.Remove(tmp)
		e.dirty.Store(true)
		return fmt.Errorf("delta: snapshot: %w", err)
	}

	bw := bufio.NewWriterSize(f, 1<<20)
	fileID, err := newFileID()
	if err != nil {
		return fail(err)
	}
	header := make([]byte, 0, len(snapMagic)+4+sealFileIDLen)
	header = append(header, snapMagic...)
	header = binary.LittleEndian.AppendUint32(header, snapVersion)
	header = append(header, fileID...)
	if _, err := bw.Write(header); err != nil {
		return fail(err)
	}
	sw, err := newSealWriter(bw, e.snapKey, header)
	if err != nil {
		return fail(err)
	}
	if err := writeSnapshotBody(sw, e.p.Fingerprint(), doms); err != nil {
		return fail(err)
	}
	if err := sw.Close(); err != nil {
		return fail(err)
	}
	if err := bw.Flush(); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		e.dirty.Store(true)
		return fmt.Errorf("delta: snapshot: %w", err)
	}
	if err := os.Rename(tmp, e.p.SnapshotPath); err != nil {
		os.Remove(tmp)
		e.dirty.Store(true)
		return fmt.Errorf("delta: snapshot: %w", err)
	}
	e.lastSnap = e.now()
	return nil
}

// writeSnapshotBody writes everything after the header: the fingerprint and
// the per-domain state, in the sealed snapshot layout.
func writeSnapshotBody(w io.Writer, fp [32]byte, doms []*Domain) error {
	write := func(v any) error { return binary.Write(w, binary.LittleEndian, v) }
	if _, err := w.Write(fp[:]); err != nil {
		return err
	}
	if err := write(uint32(len(doms))); err != nil {
		return err
	}
	for _, d := range doms {
		if _, err := w.Write(d.salt[:]); err != nil {
			return err
		}
		if err := write(d.known.Count()); err != nil {
			return err
		}
		words := make([]uint64, len(d.known.words))
		for i := range words {
			words[i] = d.known.words[i].Load()
		}
		if err := write(words); err != nil {
			return err
		}
		items := d.reps.items()
		if err := write(uint64(len(items))); err != nil {
			return err
		}
		var rec [repRecordLen]byte
		for _, it := range items {
			copy(rec[0:32], it.hInd[:])
			copy(rec[32:64], it.DeltaObjectKey[:])
			copy(rec[64:96], it.BaseObjectKey[:])
			binary.LittleEndian.PutUint32(rec[96:100], it.ObjectSize)
			binary.LittleEndian.PutUint32(rec[100:104], it.BaseSize)
			if _, err := w.Write(rec[:repRecordLen]); err != nil {
				return err
			}
		}
		bases := d.sim.export()
		if err := write(uint64(len(bases))); err != nil {
			return err
		}
		for _, b := range bases {
			if err := writeStoredBase(w, b); err != nil {
				return err
			}
		}
	}
	return nil
}

type snapDomain struct {
	salt  [32]byte
	count uint64
	words []uint64
	reps  []repItem
	bases []storedBase
}

// loadSnapshot restores state from path. A missing file is not an error.
func (e *Engine) loadSnapshot(path string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delta: snapshot: %w", err)
	}
	defer f.Close()

	doms, err := e.parseSnapshot(f)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, sd := range doms {
		e.tick++
		d := e.newDomainLocked(sd.salt)
		for i, w := range sd.words {
			d.known.words[i].Store(w)
		}
		d.known.count.Store(sd.count)
		for _, it := range sd.reps {
			d.reps.Put(it.hInd, it.RepEntry)
		}
		for _, b := range sd.bases {
			d.sim.restore(b)
		}
	}
	e.evictDomainsLocked()
	return nil
}

func (e *Engine) parseSnapshot(r io.Reader) ([]snapDomain, error) {
	corrupt := func(why string) error { return fmt.Errorf("%w: %s", ErrSnapshotCorrupt, why) }
	br := bufio.NewReaderSize(r, 1<<20)

	// Magic and version are cleartext. Anything other than snapVersion is
	// refused before the body is read.
	header := make([]byte, len(snapMagic)+4)
	if _, err := io.ReadFull(br, header); err != nil || string(header[:len(snapMagic)]) != snapMagic {
		return nil, corrupt("bad magic")
	}
	ver := binary.LittleEndian.Uint32(header[len(snapMagic):])
	if ver != snapVersion {
		return nil, corrupt(fmt.Sprintf("unsupported version %d", ver))
	}

	fileID := make([]byte, sealFileIDLen)
	if _, err := io.ReadFull(br, fileID); err != nil {
		return nil, corrupt("truncated header")
	}
	sealed, err := newSealReader(br, e.snapKey, append(header, fileID...))
	if err != nil {
		return nil, corrupt(err.Error())
	}
	doms, err := e.parseSnapshotBody(sealed)
	if err != nil {
		return nil, err
	}
	// Drain to the authenticated end: the last frame may be empty.
	if _, err := io.Copy(io.Discard, sealed); err != nil {
		return nil, corrupt(err.Error())
	}
	if !sealed.atEnd() {
		return nil, corrupt(errSealTruncated.Error())
	}
	if _, err := br.ReadByte(); err != io.EOF {
		return nil, corrupt("trailing bytes")
	}
	return doms, nil
}

// parseSnapshotBody reads the fingerprint and the domains. The body must have
// been authenticated by the time its contents are used: the caller checks the
// seal before returning them.
func (e *Engine) parseSnapshotBody(hr io.Reader) ([]snapDomain, error) {
	corrupt := func(why string) error { return fmt.Errorf("%w: %s", ErrSnapshotCorrupt, why) }
	read := func(v any) error {
		if err := binary.Read(hr, binary.LittleEndian, v); err != nil {
			return corrupt(err.Error())
		}
		return nil
	}
	var fp [32]byte
	if err := read(&fp); err != nil {
		return nil, err
	}
	if fp != e.p.Fingerprint() {
		return nil, ErrSnapshotMismatch
	}
	var nd uint32
	if err := read(&nd); err != nil {
		return nil, err
	}
	if int(nd) > maxDomainsCeiling {
		return nil, corrupt("implausible domain count")
	}
	wantWords := len(newBloom(e.p.KnownCapacity).words)

	doms := make([]snapDomain, 0, min(int(nd), e.p.MaxDomains))
	for i := uint32(0); i < nd; i++ {
		var sd snapDomain
		if err := read(&sd.salt); err != nil {
			return nil, err
		}
		if err := read(&sd.count); err != nil {
			return nil, err
		}
		// The fingerprint covers known_capacity, so the filter size is known.
		sd.words = make([]uint64, wantWords)
		if err := read(sd.words); err != nil {
			return nil, err
		}
		var nrep uint64
		if err := read(&nrep); err != nil {
			return nil, err
		}
		if nrep > uint64(e.p.MaxRepresentations) {
			return nil, corrupt("representation count exceeds configured maximum")
		}
		sd.reps = make([]repItem, 0, min(nrep, 1<<20))
		var rec [repRecordLen]byte
		for j := uint64(0); j < nrep; j++ {
			if _, err := io.ReadFull(hr, rec[:]); err != nil {
				return nil, corrupt(err.Error())
			}
			var it repItem
			copy(it.hInd[:], rec[0:32])
			copy(it.DeltaObjectKey[:], rec[32:64])
			copy(it.BaseObjectKey[:], rec[64:96])
			it.ObjectSize = binary.LittleEndian.Uint32(rec[96:100])
			it.BaseSize = binary.LittleEndian.Uint32(rec[100:104])
			sd.reps = append(sd.reps, it)
		}
		var nb uint64
		if err := read(&nb); err != nil {
			return nil, err
		}
		if nb > uint64(e.p.MaxEntries) {
			return nil, corrupt("base count exceeds configured maximum")
		}
		sd.bases = make([]storedBase, 0, nb)
		for j := uint64(0); j < nb; j++ {
			b, err := readStoredBase(hr, e.p.Slots)
			if err != nil {
				return nil, corrupt(err.Error())
			}
			sd.bases = append(sd.bases, b)
		}
		doms = append(doms, sd)
	}
	return doms, nil
}

func writeStoredBase(w io.Writer, b storedBase) error {
	var hdr [78]byte
	copy(hdr[0:32], b.info.ObjectKey[:])
	copy(hdr[32:64], b.info.BaseKey[:])
	binary.LittleEndian.PutUint32(hdr[64:68], b.info.Size)
	binary.LittleEndian.PutUint64(hdr[68:76], b.compact.Empty)
	if int(b.compact.K) > MaxSlots || len(b.sfs) > MaxSlots {
		return fmt.Errorf("base record too wide")
	}
	hdr[76] = b.compact.K
	hdr[77] = byte(len(b.sfs))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	var two [2]byte
	for i := 0; i < int(b.compact.K); i++ {
		binary.LittleEndian.PutUint16(two[:], b.compact.FP[i])
		if _, err := w.Write(two[:]); err != nil {
			return err
		}
	}
	var eight [8]byte
	for _, f := range b.sfs {
		binary.LittleEndian.PutUint64(eight[:], f)
		if _, err := w.Write(eight[:]); err != nil {
			return err
		}
	}
	return nil
}

func readStoredBase(r io.Reader, slots int) (storedBase, error) {
	var hdr [78]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return storedBase{}, err
	}
	var b storedBase
	copy(b.info.ObjectKey[:], hdr[0:32])
	copy(b.info.BaseKey[:], hdr[32:64])
	b.info.Size = binary.LittleEndian.Uint32(hdr[64:68])
	b.compact.Empty = binary.LittleEndian.Uint64(hdr[68:76])
	b.compact.K = hdr[76]
	nsf := int(hdr[77])
	if int(b.compact.K) != slots || nsf > MaxSlots {
		return storedBase{}, fmt.Errorf("base record geometry %d slots, %d features", b.compact.K, nsf)
	}
	for i := 0; i < slots; i++ {
		var two [2]byte
		if _, err := io.ReadFull(r, two[:]); err != nil {
			return storedBase{}, err
		}
		b.compact.FP[i] = binary.LittleEndian.Uint16(two[:])
	}
	b.sfs = make([]uint64, nsf)
	for i := 0; i < nsf; i++ {
		var eight [8]byte
		if _, err := io.ReadFull(r, eight[:]); err != nil {
			return storedBase{}, err
		}
		b.sfs[i] = binary.LittleEndian.Uint64(eight[:])
	}
	return b, nil
}
