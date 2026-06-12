package tarstream

import (
	"bytes"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
)

// WriteTo packages data as a complete single-file tar stream on w. The
// logical size is measured from data (Seek to its end and back); holes
// may be unsorted and merge when adjacent. Only the data extents are
// read — via Seek, so holes cost nothing — and a file with no holes is
// written as a plain entry. The envelope metadata is fixed and
// deterministic (mode 0644, uid/gid 0, epoch mtime): the entry is a
// transport vessel, not a filesystem snapshot.
func WriteTo(w io.Writer, name string, data io.ReadSeeker, holes []Hole) error {
	name = normalizeName(name)
	if name == "" {
		return fmt.Errorf("tarstream: empty entry name")
	}
	size, err := data.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("tarstream: measure %s: %w", name, err)
	}
	extents, sparse, err := holesToExtents(size, holes)
	if err != nil {
		return fmt.Errorf("tarstream: %s: %w", name, err)
	}
	if !sparse {
		extents = nil
		if size > 0 {
			extents = []extent{{Offset: 0, Size: size}}
		}
		return emit(w, name, size, size, nil, extents, data)
	}

	// Sparse map: decimal extent count, then offset/size per line —
	// the data extents plus GNU's trailing (size, 0) sentinel —
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
	mapBuf.WriteString(strconv.FormatInt(size, 10))
	mapBuf.WriteString("\n0\n")
	if pad := (512 - mapBuf.Len()%512) % 512; pad > 0 {
		mapBuf.Write(zeroBlock[:pad])
	}
	stored := int64(mapBuf.Len()) + dataSize
	return emit(w, name, size, stored, mapBuf.Bytes(), extents, data)
}

// emit writes the PAX extended header, the data header, the optional
// sparse map, the data extents and the end-of-archive trailer.
// sparseMap == nil emits a plain entry of logical == stored size.
func emit(w io.Writer, name string, logical, stored int64, sparseMap []byte, extents []extent, data io.ReadSeeker) error {
	ustarName := name
	recs := map[string]string{
		"path": name,
		"size": strconv.FormatInt(stored, 10),
	}
	if sparseMap != nil {
		// GNU mangles the ustar name of sparse entries; readers
		// recover the real one from GNU.sparse.name. "0" where GNU
		// writes its pid keeps output deterministic.
		ustarName = path.Join(path.Dir(name), "GNUSparseFile.0", path.Base(name))
		recs["path"] = ustarName
		recs["GNU.sparse.major"] = "1"
		recs["GNU.sparse.minor"] = "0"
		recs["GNU.sparse.name"] = name
		recs["GNU.sparse.realsize"] = strconv.FormatInt(logical, 10)
	}

	pax := encodePAX(recs)
	xhdr := ustarBlock(rawHeader{
		name:     "PaxHeaders.0/" + truncateStr(path.Base(name), 80),
		mode:     0o644,
		size:     int64(len(pax)),
		typeflag: 'x',
	})
	if _, err := w.Write(xhdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(pax); err != nil {
		return err
	}
	if pad := (512 - len(pax)%512) % 512; pad > 0 {
		if _, err := w.Write(zeroBlock[:pad]); err != nil {
			return err
		}
	}

	dhdr := ustarBlock(rawHeader{
		name:     truncateStr(ustarName, 100),
		mode:     0o644,
		size:     stored,
		typeflag: '0',
	})
	if _, err := w.Write(dhdr[:]); err != nil {
		return err
	}
	if sparseMap != nil {
		if _, err := w.Write(sparseMap); err != nil {
			return err
		}
	}
	for _, e := range extents {
		if _, err := data.Seek(e.Offset, io.SeekStart); err != nil {
			return err
		}
		if err := copyExactly(w, data, e.Size, name); err != nil {
			return err
		}
	}
	if pad := int((512 - stored%512) % 512); pad > 0 {
		if _, err := w.Write(zeroBlock[:pad]); err != nil {
			return err
		}
	}
	// End of archive: two zero blocks.
	_, err := w.Write(zeroBlock2[:])
	return err
}

var (
	zeroBlock  [512]byte
	zeroBlock2 [1024]byte
)

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
	size     int64
	typeflag byte
}

// ustarBlock encodes a 512-byte ustar header with a valid checksum.
// uid/gid/mtime are fixed to zero (deterministic transport envelope).
func ustarBlock(h rawHeader) [512]byte {
	var b [512]byte
	copy(b[0:100], h.name)
	octal(b[100:108], h.mode)
	octal(b[108:116], 0) // uid
	octal(b[116:124], 0) // gid
	octal(b[124:136], h.size)
	octal(b[136:148], 0) // mtime: epoch
	copy(b[148:156], "        ")
	b[156] = h.typeflag
	copy(b[257:263], "ustar\x00")
	copy(b[263:265], "00")
	var sum int64
	for _, c := range b {
		sum += int64(c)
	}
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
