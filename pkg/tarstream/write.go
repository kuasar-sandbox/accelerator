package tarstream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/sparse"
)

// WriteTo packages src as a complete single-file tar stream on w. The
// sparse map comes from src.RunAt — Hole runs become the entry's
// holes (costing nothing on the wire); Zero and Data runs are the
// data extents, with Zero runs written as synthesized zero bytes
// without calling ReadAt (zeros are data; only holes are absent). A
// source with no holes is written as a plain entry. The metadata
// sweep precedes any data read (sparse law 1), so one-pass sources
// work. The envelope metadata is fixed and deterministic (mode 0644,
// uid/gid 0, epoch mtime): the entry is a transport vessel, not a
// filesystem snapshot.
func WriteTo(ctx context.Context, w io.Writer, name string, src sparse.Source) error {
	name = normalizeName(name)
	if name == "" {
		return fmt.Errorf("tarstream: empty entry name")
	}
	if src.Size() > 1<<62 {
		return fmt.Errorf("tarstream: %s: size %d overflows", name, src.Size())
	}
	size := int64(src.Size())

	// Metadata sweep: data extents = Zero|Data runs, merged across
	// kind changes; holes are everything else.
	var extents []extent
	hasHole := false
	for off := uint64(0); off < uint64(size); {
		kind, end, err := src.RunAt(off, uint64(size)-off)
		if err != nil {
			return fmt.Errorf("tarstream: %s: classify @ %d: %w", name, off, err)
		}
		if kind == sparse.Hole {
			hasHole = true
		} else if n := len(extents); n > 0 && uint64(extents[n-1].Offset+extents[n-1].Size) == off {
			extents[n-1].Size += int64(end - off)
		} else {
			extents = append(extents, extent{Offset: int64(off), Size: int64(end - off)})
		}
		off = end
	}
	if !hasHole {
		return emit(ctx, w, name, size, size, nil, extents, src)
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
	return emit(ctx, w, name, size, stored, mapBuf.Bytes(), extents, src)
}

// emit writes the PAX extended header, the data header, the optional
// sparse map, the data extents and the end-of-archive trailer.
// sparseMap == nil emits a plain entry of logical == stored size.
func emit(ctx context.Context, w io.Writer, name string, logical, stored int64, sparseMap []byte, extents []extent, src sparse.Source) error {
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
	if len(extents) > 0 {
		buf := make([]byte, copyBufSize)
		for _, e := range extents {
			if err := copyExtent(ctx, w, src, e, name, buf); err != nil {
				return err
			}
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

const copyBufSize = 256 << 10

var (
	zeroBlock  [512]byte
	zeroBlock2 [1024]byte
)

// copyExtent writes the logical bytes of one data extent: Data runs
// are read from src, Zero runs are synthesized without a read. A
// short or failing source is a hard error — the header size is
// already committed, so the stream must fail loudly rather than be
// silently corrupt.
func copyExtent(ctx context.Context, w io.Writer, src sparse.Source, e extent, name string, buf []byte) error {
	off, end := uint64(e.Offset), uint64(e.Offset+e.Size)
	for off < end {
		kind, runEnd, err := src.RunAt(off, end-off)
		if err != nil {
			return fmt.Errorf("%s: classify @ %d: %w", name, off, err)
		}
		if kind == sparse.Hole {
			return fmt.Errorf("%s: hole @ %d inside a data extent (inconsistent RunAt)", name, off)
		}
		for off < runEnd {
			n := len(buf)
			if rest := runEnd - off; rest < uint64(n) {
				n = int(rest)
			}
			chunk := buf[:n]
			if kind == sparse.Zero {
				clear(chunk)
			} else if m, err := src.ReadAt(ctx, chunk, off); err != nil && err != io.EOF {
				return fmt.Errorf("%s: read @ %d: %w", name, off, err)
			} else if m < n {
				return fmt.Errorf("%s: source ended early (%d of %d bytes @ %d)", name, m, n, off)
			}
			if _, err := w.Write(chunk); err != nil {
				return err
			}
			off += uint64(n)
		}
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
