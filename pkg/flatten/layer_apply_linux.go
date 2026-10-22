//go:build linux

package flatten

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/opencontainers/umoci/oci/layer"
	"golang.org/x/sys/unix"
)

// applyVerifiedLayer owns the reader until the synchronous applier, decoder and
// Close have all returned. On any error the entire private rootfs is invalid;
// callers must discard it, never replay a layer into this half-applied tree.
func applyVerifiedLayer(ctx context.Context, in LayerInput, root string, limits LayerLimits) (err error) {
	limits, err = limits.normalized()
	if err != nil {
		return err
	}
	s, err := openLayer(ctx, in, limits)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	if err = unpackLayer(ctx, s, root, limits.MetadataBytes); err != nil {
		return err
	}
	return s.finish()
}

// unpackLayer is the UnpackLayer loop using umoci's public TarExtractor API.
// Keeping the loop here permits accounting for its path table before each
// UnpackEntry and checking xattrs that umoci can silently omit in a userns or on
// filesystems without xattr support. All filesystem application semantics stay
// in umoci; no CLI, CAS, bundle or rootless ownership mapping is used.
func unpackLayer(ctx context.Context, r io.Reader, root string, budget int64) error {
	metadata, err := newLayerMetadataBudget(budget)
	if err != nil {
		return err
	}
	te := layer.NewTarExtractor(&layer.UnpackOptions{OnDiskFormat: layer.DirRootfs{}})
	tr := tar.NewReader(r)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("flatten: tar header: %w", err)
		}
		if err := layerPath(hdr.Name); err != nil {
			return err
		}
		if hdr.Typeflag == tar.TypeLink {
			if err := layerPath(hdr.Linkname); err != nil {
				return err
			}
		}
		if err := metadata.reserve(root, hdr); err != nil {
			return err
		}
		if err := te.UnpackEntry(root, hdr, tr); err != nil {
			return fmt.Errorf("flatten: umoci unpack %q: %w", hdr.Name, err)
		}
		if err := checkLayerXattrs(root, hdr); err != nil {
			return err
		}
	}
}

// layerMetadataBudget bounds retained path bookkeeping, rather than the total
// allocation traffic over the lifetime of a layer. Header/PAX storage is live
// for one entry only; shared ancestors and overwritten paths remain one map key.
// Keep a matching set because TarExtractor's upperPaths is private. Charge both
// sets, and resolve parent symlinks just as UnpackEntry does before extracting.
type layerMetadataBudget struct {
	limit, remaining int64
	// Each value bounds the full entry path backing this key, not root+"/"+key.
	// umoci's ancestor keys are substrings of entry paths and are reassigned by
	// later descendants. String-map assignments can update that backing storage.
	paths map[string]int
}

func newLayerMetadataBudget(limit int64) (*layerMetadataBudget, error) {
	// archive/tar bounds each extended-header body to 1 MiB. Reserve parsing
	// buffers and the extractor before asking for a header.
	if limit < 8<<20 {
		return nil, errors.New("flatten: layer metadata budget must be at least 8 MiB")
	}
	return &layerMetadataBudget{limit: limit, remaining: limit - (8 << 20), paths: make(map[string]int)}, nil
}

func (b *layerMetadataBudget) reserve(root string, hdr *tar.Header) error {
	// This storage is released/reused between entries, not retained per file.
	header := int64(2048 + len(hdr.Name) + len(hdr.Linkname))
	for k, v := range hdr.PAXRecords {
		header += int64(256 + 2*len(k) + 2*len(v))
	}
	check := func(charge int64) error {
		if header > b.remaining || charge > b.remaining-header {
			return fmt.Errorf("flatten: layer metadata budget exceeded (limit=%d, retained=%d, header=%d, next=%d)", b.limit, b.limit-b.remaining, header, charge)
		}
		return nil
	}
	if err := check(0); err != nil {
		return err
	}
	name := path.Clean(hdr.Name)
	// This adapter uses DirRootfs. umoci applies ordinary/opaque whiteouts
	// and returns without adding their names or parents to upperPaths.
	// Their current header still has to fit, but no path state is retained.
	if strings.HasPrefix(path.Base(name), ".wh.") {
		return nil
	}
	parent, err := securejoin.SecureJoin(root, path.Dir(name))
	if err != nil {
		return fmt.Errorf("flatten: resolve layer metadata path: %w", err)
	}
	absolute := filepath.Join(parent, path.Base(name))
	resolved, err := filepath.Rel(root, absolute)
	if err != nil {
		return fmt.Errorf("flatten: relative layer metadata path: %w", err)
	}
	// absolute stays the full entry path while p walks its ancestors. A longer
	// sibling can therefore increase the backing bound of an existing ancestor.
	for p := resolved; p != "."; p = filepath.Dir(p) {
		longest, exists := b.paths[p]
		if exists && longest >= len(absolute) {
			continue
		}
		// 256 bytes covers both map entries, including growth slack. Count
		// two full absolute strings even when umoci shares one between
		// ancestors; filepath.Rel may retain the root prefix backing storage.
		charge := int64(256 + 2*len(absolute))
		if exists {
			charge = int64(2 * (len(absolute) - longest))
		}
		if err := check(charge); err != nil {
			return err
		}
		b.remaining -= charge
		b.paths[strings.Clone(p)] = len(absolute)
	}
	return nil
}

func layerPath(name string) error {
	if name == "" || len(name) > 4096 || strings.HasPrefix(name, "/") || strings.IndexByte(name, 0) >= 0 {
		return fmt.Errorf("flatten: invalid layer path %q", name)
	}
	for _, part := range strings.Split(name, "/") {
		if part == ".." {
			return fmt.Errorf("flatten: parent traversal in layer path %q", name)
		}
	}
	return nil
}

func checkLayerXattrs(root string, hdr *tar.Header) error {
	if len(hdr.Xattrs) == 0 || strings.HasPrefix(path.Base(hdr.Name), ".wh.") {
		return nil
	}
	name := path.Clean(hdr.Name)
	target := root
	if name != "." {
		parent, err := securejoin.SecureJoin(root, path.Dir(name))
		if err != nil {
			return err
		}
		target = filepath.Join(parent, path.Base(name))
	}
	for name, want := range hdr.Xattrs {
		// Linux xattr values are at most 64 KiB. Read exactly the expected
		// size, so a different/missing value or a library omission fails.
		if len(want) > 65536 {
			return fmt.Errorf("flatten: oversized xattr %q on %q", name, hdr.Name)
		}
		got := make([]byte, len(want)+1)
		n, err := unix.Lgetxattr(target, name, got)
		if err != nil {
			return fmt.Errorf("flatten: xattr %q on %q was not preserved: %w", name, hdr.Name, err)
		}
		if !bytes.Equal(got[:n], []byte(want)) {
			return fmt.Errorf("flatten: xattr %q on %q was not preserved", name, hdr.Name)
		}
	}
	return nil
}
