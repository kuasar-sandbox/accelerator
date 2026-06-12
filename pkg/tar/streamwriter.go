package tar

import (
	stdtar "archive/tar"
	"bytes"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
)

// Hole marks [Offset, Offset+Length) of a logical file as a hole.
type Hole struct {
	Offset, Length int64
}

// StreamWriter is a pure-Go tar stream writer whose specialty is
// emitting sparse entries from an in-memory description — a logical
// data view plus its hole map — with nothing spooled and no tar binary
// involved: only the data bytes flow. Plain entries pass through the
// stdlib writer, so normal and sparse members mix freely in one
// archive.
//
// Sparse entries use the GNU PAX sparse 1.0 encoding, byte-mirroring
// GNU tar's own output (including the trailing zero-length sentinel
// extent); GNU tar and Go's archive/tar reader both decode it. This is
// the one place the package owns wire-format bytes — the stdlib writer
// has no sparse support and silently drops GNU.sparse.* PAX records —
// and it is pinned by interop tests against both readers.
type StreamWriter struct {
	tw *stdtar.Writer
	w  io.Writer
}

// NewStreamWriter returns a tar stream writer on w. Close finishes the
// archive.
func NewStreamWriter(w io.Writer) *StreamWriter {
	return &StreamWriter{tw: stdtar.NewWriter(w), w: w}
}

// WriteHeader writes a plain entry header (stdlib semantics).
func (sw *StreamWriter) WriteHeader(hdr *stdtar.Header) error { return sw.tw.WriteHeader(hdr) }

// Write writes the current plain entry's data (stdlib semantics).
func (sw *StreamWriter) Write(b []byte) (int, error) { return sw.tw.Write(b) }

// Close finishes the archive (trailer blocks).
func (sw *StreamWriter) Close() error { return sw.tw.Close() }

// WriteSparse emits one regular file whose holes are known up front.
// hdr.Size is the logical size; data is the logical view of the file
// (holes read as zeros, like an *os.File of a sparse file) — only the
// data extents are read, via Seek. An empty hole list degrades to a
// plain stdlib entry. Holes may be unsorted; they must be non-empty,
// in-bounds and non-overlapping (adjacent holes merge).
func (sw *StreamWriter) WriteSparse(hdr *stdtar.Header, data io.ReadSeeker, holes []Hole) error {
	if hdr.Typeflag != 0 && hdr.Typeflag != stdtar.TypeReg {
		return fmt.Errorf("tar: WriteSparse on non-regular entry %q", hdr.Name)
	}
	logical := hdr.Size
	extents, sparse, err := holesToExtents(logical, holes)
	if err != nil {
		return fmt.Errorf("tar: %s: %w", hdr.Name, err)
	}
	if !sparse {
		h := *hdr
		h.Typeflag = stdtar.TypeReg
		if h.Format == stdtar.FormatUnknown {
			h.Format = stdtar.FormatPAX
		}
		if err := sw.tw.WriteHeader(&h); err != nil {
			return err
		}
		if _, err := data.Seek(0, io.SeekStart); err != nil {
			return err
		}
		return copyExactly(sw.tw, data, logical, h.Name)
	}

	// Sparse map: decimal extent count, then offset/size per line —
	// real data extents plus GNU's trailing (size, 0) sentinel —
	// NUL-padded to a 512 boundary and prepended to the stored data.
	var mapBuf bytes.Buffer
	mapBuf.WriteString(strconv.Itoa(len(extents) + 1))
	mapBuf.WriteByte('\n')
	var dataSize int64
	for _, e := range extents {
		mapBuf.WriteString(strconv.FormatInt(e.Offset, 10))
		mapBuf.WriteByte('\n')
		mapBuf.WriteString(strconv.FormatInt(e.Size, 10))
		mapBuf.WriteByte('\n')
		dataSize += e.Size
	}
	mapBuf.WriteString(strconv.FormatInt(logical, 10))
	mapBuf.WriteString("\n0\n")
	if pad := (512 - mapBuf.Len()%512) % 512; pad > 0 {
		mapBuf.Write(zeroBlock[:pad])
	}
	stored := int64(mapBuf.Len()) + dataSize

	// GNU mangles the ustar name of sparse entries; readers recover
	// the real one from GNU.sparse.name. "0" where GNU writes its pid
	// keeps output deterministic.
	mangled := path.Join(path.Dir(hdr.Name), "GNUSparseFile.0", path.Base(hdr.Name))

	recs := map[string]string{
		"GNU.sparse.major":    "1",
		"GNU.sparse.minor":    "0",
		"GNU.sparse.name":     hdr.Name,
		"GNU.sparse.realsize": strconv.FormatInt(logical, 10),
		"path":                mangled,
		"size":                strconv.FormatInt(stored, 10),
	}
	if !hdr.ModTime.IsZero() {
		recs["mtime"] = paxTime(hdr.ModTime.Unix(), int64(hdr.ModTime.Nanosecond()))
	}
	if int64(hdr.Uid) > maxOctal7 {
		recs["uid"] = strconv.Itoa(hdr.Uid)
	}
	if int64(hdr.Gid) > maxOctal7 {
		recs["gid"] = strconv.Itoa(hdr.Gid)
	}
	for k, v := range hdr.PAXRecords {
		if _, taken := recs[k]; !taken {
			recs[k] = v
		}
	}

	// Complete any previous stdlib-written entry's padding, then take
	// over the raw stream for this entry: an extended ('x') header
	// carrying the PAX records, then the data header, map and extents.
	if err := sw.tw.Flush(); err != nil {
		return err
	}
	pax := encodePAX(recs)
	xhdr := ustarBlock(rawHeader{
		name:     "PaxHeaders.0/" + truncateStr(path.Base(hdr.Name), 80),
		mode:     0o644,
		size:     int64(len(pax)),
		typeflag: stdtar.TypeXHeader,
	})
	if _, err := sw.w.Write(xhdr[:]); err != nil {
		return err
	}
	if _, err := sw.w.Write(pax); err != nil {
		return err
	}
	if pad := (512 - len(pax)%512) % 512; pad > 0 {
		if _, err := sw.w.Write(zeroBlock[:pad]); err != nil {
			return err
		}
	}
	dhdr := ustarBlock(rawHeader{
		name:     truncateStr(mangled, 100),
		mode:     hdr.Mode & 0o7777,
		uid:      hdr.Uid,
		gid:      hdr.Gid,
		size:     stored,
		mtime:    hdr.ModTime.Unix(),
		typeflag: stdtar.TypeReg,
	})
	if _, err := sw.w.Write(dhdr[:]); err != nil {
		return err
	}
	if _, err := sw.w.Write(mapBuf.Bytes()); err != nil {
		return err
	}
	for _, e := range extents {
		if _, err := data.Seek(e.Offset, io.SeekStart); err != nil {
			return err
		}
		if err := copyExactly(sw.w, data, e.Size, hdr.Name); err != nil {
			return err
		}
	}
	if pad := int((512 - stored%512) % 512); pad > 0 {
		if _, err := sw.w.Write(zeroBlock[:pad]); err != nil {
			return err
		}
	}
	return nil
}

// maxOctal7 is the largest value a 7-digit octal ustar numeric field
// holds (uid/gid fields are 8 bytes with a terminator).
const maxOctal7 = 1<<21 - 1

// holesToExtents validates and normalizes the hole map and inverts it
// into data extents. sparse=false means there is nothing to encode.
func holesToExtents(size int64, holes []Hole) ([]extent, bool, error) {
	if size < 0 {
		return nil, false, fmt.Errorf("negative size %d", size)
	}
	hs := make([]Hole, 0, len(holes))
	for _, h := range holes {
		if h.Length == 0 {
			continue
		}
		if h.Length < 0 || h.Offset < 0 || h.Offset+h.Length > size {
			return nil, false, fmt.Errorf("hole [%d,+%d) out of bounds (size %d)", h.Offset, h.Length, size)
		}
		hs = append(hs, h)
	}
	if len(hs) == 0 {
		return nil, false, nil
	}
	sort.Slice(hs, func(i, j int) bool { return hs[i].Offset < hs[j].Offset })
	merged := hs[:1]
	for _, h := range hs[1:] {
		last := &merged[len(merged)-1]
		switch {
		case h.Offset < last.Offset+last.Length:
			return nil, false, fmt.Errorf("overlapping holes at %d", h.Offset)
		case h.Offset == last.Offset+last.Length: // adjacent: merge
			last.Length += h.Length
		default:
			merged = append(merged, h)
		}
	}
	var extents []extent
	var pos int64
	for _, h := range merged {
		if h.Offset > pos {
			extents = append(extents, extent{Offset: pos, Size: h.Offset - pos})
		}
		pos = h.Offset + h.Length
	}
	if pos < size {
		extents = append(extents, extent{Offset: pos, Size: size - pos})
	}
	return extents, true, nil
}

// extent is one data run of a sparse file.
type extent struct {
	Offset, Size int64
}

// copyExactly copies exactly n bytes and turns a short source into a
// hard error — the header size is already committed, so a source that
// came up short must fail loudly rather than corrupt the stream.
func copyExactly(dst io.Writer, src io.Reader, n int64, name string) error {
	written, err := io.CopyN(dst, src, n)
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("%s: source ended early (%d of %d bytes)", name, written, n)
		}
		return err
	}
	return nil
}

// encodePAX renders PAX records ("len key=value\n", sorted by key for
// deterministic output).
func encodePAX(recs map[string]string) []byte {
	keys := make([]string, 0, len(recs))
	for k := range recs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	for _, k := range keys {
		v := recs[k]
		size := len(k) + len(v) + 3 // " " + "=" + "\n"
		size += len(strconv.Itoa(size))
		record := strconv.Itoa(size) + " " + k + "=" + v + "\n"
		if len(record) != size { // digit-count changed; re-render once
			size = len(record)
			record = strconv.Itoa(size) + " " + k + "=" + v + "\n"
		}
		buf.WriteString(record)
	}
	return buf.Bytes()
}

// paxTime renders seconds(+nanoseconds) the way PAX time records
// expect.
func paxTime(sec, nsec int64) string {
	if nsec == 0 {
		return strconv.FormatInt(sec, 10)
	}
	return fmt.Sprintf("%d.%09d", sec, nsec)
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// rawHeader carries the few ustar fields the raw encoder needs; PAX
// records override everything that matters on the reader side.
type rawHeader struct {
	name     string
	mode     int64
	uid, gid int
	size     int64
	mtime    int64
	typeflag byte
}

// ustarBlock encodes a 512-byte ustar header with a valid checksum.
func ustarBlock(h rawHeader) [512]byte {
	var b [512]byte
	copy(b[0:100], h.name)
	octal(b[100:108], h.mode)
	octal(b[108:116], int64(h.uid))
	octal(b[116:124], int64(h.gid))
	octal(b[124:136], h.size)
	octal(b[136:148], h.mtime)
	copy(b[148:156], "        ") // checksum placeholder
	b[156] = h.typeflag
	copy(b[257:263], "ustar\x00")
	copy(b[263:265], "00")
	var sum int64
	for _, c := range b {
		sum += int64(c)
	}
	// "%06o\x00 " per ustar convention.
	s := strconv.FormatInt(sum, 8)
	for len(s) < 6 {
		s = "0" + s
	}
	copy(b[148:154], s)
	b[154] = 0
	b[155] = ' '
	return b
}

// octal writes a NUL-terminated octal field; out-of-range values
// become zero (the PAX record carries the real one).
func octal(dst []byte, v int64) {
	width := len(dst) - 1
	if v < 0 {
		v = 0
	}
	s := strconv.FormatInt(v, 8)
	if len(s) > width {
		s = "0"
	}
	for len(s) < width {
		s = "0" + s
	}
	copy(dst, s)
	dst[width] = 0
}
