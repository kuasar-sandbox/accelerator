package tar

import (
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
