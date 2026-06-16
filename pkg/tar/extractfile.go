package tar

import (
	stdtar "archive/tar"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/tarstream"
)

// ExtractFile materializes one located tar member (a tarstream view)
// at dst with exact sparse fidelity: exactly the holes DECLARED by the
// archive's sparse map are skipped and left unallocated, everything
// else — zero-valued bytes included — is written out as allocated
// data. Nothing is ever inferred from content; written zeros and holes
// carry different meanings and survive the round trip distinct.
//
// The tarstream envelope carries no real metadata (fixed mode 0644,
// uid/gid 0, epoch mtime), so the file is created 0644 owned by the
// caller; Options.Chmod / Options.Chown override.
func ExtractFile(v tarstream.Reader, dst string, o Options) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := copyDeclared(f, v); err != nil {
		f.Close()
		return fmt.Errorf("tar extract %s: %w", v.Name(), err)
	}
	if err := f.Close(); err != nil {
		return err
	}

	mode := fs.FileMode(0o644)
	if o.Chmod != nil {
		mode = *o.Chmod
	}
	if err := os.Chmod(dst, mode); err != nil {
		return err
	}
	if o.Chown != nil {
		if err := os.Lchown(dst, o.Chown.UID, o.Chown.GID); err != nil {
			return fmt.Errorf("chown %s -> %d:%d: %w", dst, o.Chown.UID, o.Chown.GID, err)
		}
	}
	return nil
}

// copyDeclared walks the view's declared layout in order: data
// segments are copied, hole segments are skipped on both sides (Seek
// when the view is seekable, discard otherwise — a discard reads only
// synthesized zeros, never the underlying source) and land unallocated
// in f via the final Truncate.
func copyDeclared(f *os.File, v tarstream.Reader) error {
	size := v.Size()
	seeker, _ := v.(io.Seeker)
	pos := int64(0)
	copySection := func(off, n int64) error {
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			return err
		}
		if _, err := io.CopyN(f, v, n); err != nil {
			return err
		}
		return nil
	}
	for _, h := range v.Holes() { // sorted, merged, in bounds
		if dl := int64(h.Offset) - pos; dl > 0 {
			if err := copySection(pos, dl); err != nil {
				return err
			}
		}
		end := int64(h.Offset + h.Size)
		if seeker != nil {
			if _, err := seeker.Seek(end, io.SeekStart); err != nil {
				return err
			}
		} else {
			if _, err := io.CopyN(io.Discard, v, int64(h.Size)); err != nil {
				return err
			}
		}
		pos = end
	}
	if rest := size - pos; rest > 0 {
		if err := copySection(pos, rest); err != nil {
			return err
		}
	}
	// Sets the final size; the trailing hole (if any) stays a hole.
	return f.Truncate(size)
}

// sparseEntry reports whether hdr is a sparse-encoded member: the old
// GNU 'S' typeflag, or PAX GNU.sparse.* records (retained verbatim in
// Header.PAXRecords by the stdlib reader — the map itself is consumed
// and unexposed, which is why hole-exact extraction re-locates the
// member via tarstream).
func sparseEntry(hdr *stdtar.Header) bool {
	if hdr.Typeflag == stdtar.TypeGNUSparse {
		return true
	}
	for _, k := range []string{"GNU.sparse.major", "GNU.sparse.map", "GNU.sparse.numblocks", "GNU.sparse.size"} {
		if _, ok := hdr.PAXRecords[k]; ok {
			return true
		}
	}
	return false
}

// sparseRegular extracts the current (sparse) regular member
// hole-exact: a second handle on the archive (Options.Reopen)
// re-locates the member by ordinal via tarstream, recovering the hole
// map, and copyDeclared punches exactly the declared holes. The
// stdlib-side body is left unread — the next tr.Next() skips it
// (seeking past it on seekable input). Metadata comes from hdr, like
// any other member.
func (x *extractor) sparseRegular(hdr *stdtar.Header, dest, name string) error {
	if x.o.Reopen == nil {
		return fmt.Errorf("tar: %s is a sparse member; hole-exact extraction needs a re-openable archive (-f FILE) — or pass --dense to materialize the logical bytes", name)
	}
	rs, err := x.o.Reopen()
	if err != nil {
		return fmt.Errorf("tar: reopen archive for sparse member %s: %w", name, err)
	}
	defer rs.Close()
	v, err := tarstream.ReadSeekFromIndex(rs, x.ordinal)
	if err != nil {
		return fmt.Errorf("tar: locate sparse member %s (entry #%d): %w (pass --dense to materialize the logical bytes)", name, x.ordinal, err)
	}
	if err := x.prepare(dest); err != nil {
		return err
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := copyDeclared(f, v); err != nil {
		f.Close()
		return fmt.Errorf("extract sparse %s: %w", name, err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := x.applyMeta(dest, hdr, false); err != nil {
		return err
	}
	return os.Chtimes(dest, hdr.ModTime, hdr.ModTime)
}
