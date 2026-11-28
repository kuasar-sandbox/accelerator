package vcdiff

import (
	"bufio"
	"fmt"
	"io"
)

// File header bytes and indicator flags (RFC 3284 §4.1).
var headerMagic = [3]byte{0xD6, 0xC3, 0xC4}

const (
	versionRFC byte = 0x00 // standard RFC 3284
	versionExt byte = 0x53 // 'S': open-vcdiff/xdelta3 interleaved/checksum extensions
)

const (
	hdrDecompress byte = 0x01 // VCD_DECOMPRESS: file-wide secondary compressor
	hdrCodeTable  byte = 0x02 // VCD_CODETABLE: application-defined code table
	hdrAppHeader  byte = 0x04 // VCD_APPHEADER: application header present
)

// Win_Indicator flags (RFC 3284 §4.2) plus the open-vcdiff checksum extension.
const (
	winSource   byte = 0x01 // VCD_SOURCE: window copies from the source file
	winTarget   byte = 0x02 // VCD_TARGET: window copies from produced target
	winChecksum byte = 0x04 // open-vcdiff per-window Adler32 (not RFC)
)

// Delta_Indicator flags (per-section secondary compression; unsupported here).
const deltaCompMask byte = 0x07

// Default address-cache sizes for the default code table.
const (
	defaultNearCache = 4
	defaultSameCache = 3
)

// appendHeader appends the standard, minimal VCDIFF file header: the magic, a
// version byte of 0x00, and a Hdr_Indicator of 0x00 (no secondary compressor,
// no custom code table, no application header).
func appendHeader(dst []byte) []byte {
	return append(dst, headerMagic[0], headerMagic[1], headerMagic[2], versionRFC, 0x00)
}

// readHeader consumes and validates the VCDIFF file header from br. It accepts
// both the RFC version byte (0x00) and the open-vcdiff extension byte (0x53),
// skips an application header, and rejects secondary compression and custom
// code tables (which would otherwise be silently mis-decoded).
func readHeader(br *bufio.Reader) error {
	var m [4]byte
	if _, err := io.ReadFull(br, m[:]); err != nil {
		return fmt.Errorf("vcdiff: read header: %w", err)
	}
	if m[0] != headerMagic[0] || m[1] != headerMagic[1] || m[2] != headerMagic[2] {
		return fmt.Errorf("vcdiff: bad magic %#x", m[:3])
	}
	if m[3] != versionRFC && m[3] != versionExt {
		return fmt.Errorf("vcdiff: unsupported version 0x%02x", m[3])
	}
	ind, err := br.ReadByte()
	if err != nil {
		return fmt.Errorf("vcdiff: read header indicator: %w", err)
	}
	if ind&hdrDecompress != 0 {
		return fmt.Errorf("vcdiff: secondary decompression not supported")
	}
	if ind&hdrCodeTable != 0 {
		return fmt.Errorf("vcdiff: custom code table not supported")
	}
	if ind&^(hdrDecompress|hdrCodeTable|hdrAppHeader) != 0 {
		return fmt.Errorf("vcdiff: unknown header indicator bits 0x%02x", ind)
	}
	if ind&hdrAppHeader != 0 {
		n, err := readVarint(br)
		if err != nil {
			return fmt.Errorf("vcdiff: read app-header length: %w", err)
		}
		if _, err := io.CopyN(io.Discard, br, int64(n)); err != nil {
			return fmt.Errorf("vcdiff: skip app header: %w", err)
		}
	}
	return nil
}

// sliceSegment returns buf[pos:pos+length], validating the bounds.
func sliceSegment(buf []byte, pos, length uint64) ([]byte, error) {
	end := pos + length
	if end < pos || end > uint64(len(buf)) {
		return nil, fmt.Errorf("vcdiff: segment [%d,%d) out of range (len %d)", pos, end, len(buf))
	}
	return buf[pos:end], nil
}
