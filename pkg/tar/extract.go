package tar

import (
	stdtar "archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// Extract materializes the entries selected by rules from the tar
// stream: the rule's left side names the entry inside the archive, the
// right side the filesystem destination ("-" streams the content to
// stdout). No rules selects everything into the current directory.
//
// Extraction is pure Go and single-pass: the stdlib reader decodes the
// stream while entries are matched and written on the fly — pipes work
// with nothing spooled, and no tar binary is involved. Regular files
// are written dense: zero-valued bytes are data and stay allocated,
// never inferred into holes. SPARSE members are extracted hole-exact
// when Options.Reopen is available (the member is re-located by
// ordinal on a second handle, recovering the map the stdlib reader
// hides); without Reopen they are a hard error — never a silent
// multi-GiB densification — unless Options.Dense explicitly selects
// stdlib logical-bytes materialization.
// Existing files are replaced. The most specific rule wins per entry
// (exact file rule, then longest directory prefix). Entries escaping
// the archive root (..) are skipped with a warning; an entry that
// would resolve through a symlink to outside its rule's destination is
// a hard error. Without an explicit Chown override, ownership-restore
// failures as an unprivileged user downgrade to a one-time warning.
func Extract(r io.Reader, rules []Rule, o Options) error {
	if len(rules) == 0 {
		rules = []Rule{{Tar: "", FS: ".", Prefix: true}}
	}
	x := &extractor{
		tr:       stdtar.NewReader(r),
		o:        &o,
		rules:    rules,
		matched:  make([]bool, len(rules)),
		evalBase: map[string]string{},
		ordinal:  -1,
	}
	for {
		hdr, err := x.tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		x.ordinal++ // stdlib entry sequence == tarstream ordinal addressing
		if err := x.entry(hdr); err != nil {
			return err
		}
	}
	for i, rl := range rules {
		if !x.matched[i] && rl.Tar != "" {
			return fmt.Errorf("tar extract: %q not found in archive", rl.Tar)
		}
	}
	// Directory mtimes last, children before parents, so content
	// writes don't clobber them.
	sort.Slice(x.dirTimes, func(i, j int) bool {
		return len(x.dirTimes[i].path) > len(x.dirTimes[j].path)
	})
	for _, dt := range x.dirTimes {
		if err := os.Chtimes(dt.path, dt.mtime, dt.mtime); err != nil {
			return err
		}
	}
	return nil
}

type dirTime struct {
	path  string
	mtime time.Time
}

type extractor struct {
	tr          *stdtar.Reader
	o           *Options
	rules       []Rule
	matched     []bool
	dirTimes    []dirTime
	chownWarned bool
	evalBase    map[string]string // rule FS base → EvalSymlinks(abs base)
	ordinal     int               // current entry's ordinal (stdlib sequence)
}

// matchRule resolves an entry name to the most specific rule and marks
// it matched.
func (x *extractor) matchRule(name string) (Rule, string, bool) {
	rl, rel, ok := match(x.rules, name)
	if !ok {
		return Rule{}, "", false
	}
	for i := range x.rules {
		if x.rules[i] == rl {
			x.matched[i] = true
			break
		}
	}
	return rl, rel, true
}

func (x *extractor) entry(hdr *stdtar.Header) error {
	switch hdr.Typeflag {
	case stdtar.TypeXGlobalHeader, stdtar.TypeXHeader:
		return nil
	}
	name, err := normalizeTarPath(hdr.Name)
	if err != nil {
		x.o.warnf("tar: skipping entry with escaping name %q", hdr.Name)
		return nil
	}
	if name == "" {
		return nil // the "./" root entry
	}
	rl, rel, ok := x.matchRule(name)
	if !ok {
		return nil
	}

	// Stdio rule: stream the content, no filesystem object.
	if rl.FS == "-" {
		switch hdr.Typeflag {
		case stdtar.TypeReg:
			_, err := io.Copy(x.o.stdout(), x.tr)
			return err
		case stdtar.TypeLink:
			return nil // no data on a hardlink member (same as GNU -xO)
		default:
			x.o.warnf("tar: %s is not a regular file; nothing written to stdout", name)
			return nil
		}
	}

	dest, err := x.destPath(rl, rel)
	if err != nil {
		return fmt.Errorf("entry %s: %w", name, err)
	}

	switch hdr.Typeflag {
	case stdtar.TypeDir:
		if err := os.MkdirAll(dest, 0o755); err != nil {
			return err
		}
		if err := x.applyMeta(dest, hdr, false); err != nil {
			return err
		}
		x.dirTimes = append(x.dirTimes, dirTime{path: dest, mtime: hdr.ModTime})
		return nil

	case stdtar.TypeReg:
		if !x.o.Dense && sparseEntry(hdr) {
			return x.sparseRegular(hdr, dest, name)
		}
		if err := x.prepare(dest); err != nil {
			return err
		}
		f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, x.tr); err != nil {
			f.Close()
			return fmt.Errorf("extract %s: %w", name, err)
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := x.applyMeta(dest, hdr, false); err != nil {
			return err
		}
		return os.Chtimes(dest, hdr.ModTime, hdr.ModTime)

	case stdtar.TypeSymlink:
		if err := x.prepare(dest); err != nil {
			return err
		}
		if err := os.Symlink(hdr.Linkname, dest); err != nil {
			return err
		}
		if err := x.applyMeta(dest, hdr, true); err != nil {
			return err
		}
		return lchtimes(dest, hdr.ModTime)

	case stdtar.TypeLink:
		targetName, err := normalizeTarPath(hdr.Linkname)
		if err != nil {
			return fmt.Errorf("hardlink %s: bad target %q", name, hdr.Linkname)
		}
		trl, trel, tok := match(x.rules, targetName)
		if !tok || trl.FS == "-" {
			return fmt.Errorf("hardlink %s: target %q not selected by any rule", name, targetName)
		}
		targetDest, err := x.destPath(trl, trel)
		if err != nil {
			return err
		}
		if err := x.prepare(dest); err != nil {
			return err
		}
		return os.Link(targetDest, dest)

	case stdtar.TypeChar, stdtar.TypeBlock, stdtar.TypeFifo:
		if err := x.prepare(dest); err != nil {
			return err
		}
		if err := mknodEntry(dest, hdr); err != nil {
			if errors.Is(err, errUnsupportedNode) {
				x.o.warnf("tar: skipping %s: %v", name, err)
				return nil
			}
			return fmt.Errorf("mknod %s: %w", name, err)
		}
		if err := x.applyMeta(dest, hdr, false); err != nil {
			return err
		}
		return os.Chtimes(dest, hdr.ModTime, hdr.ModTime)

	default:
		x.o.warnf("tar: skipping unsupported entry type %q: %s", hdr.Typeflag, name)
		return nil
	}
}

// destPath maps a matched entry to its filesystem destination and, for
// directory rules, verifies the resolved parent stays under the rule's
// base (an entry written through a symlink planted by an earlier entry
// must not escape).
func (x *extractor) destPath(rl Rule, rel string) (string, error) {
	if !rl.Prefix {
		return rl.FS, nil
	}
	if rel == "" { // the rule's directory itself
		if _, err := x.baseEval(rl.FS); err != nil {
			return "", err
		}
		return rl.FS, nil
	}
	dest := filepath.Join(rl.FS, filepath.FromSlash(rel))
	if err := x.confine(rl.FS, dest); err != nil {
		return "", err
	}
	return dest, nil
}

// baseEval creates the rule's base directory once and caches its
// symlink-resolved absolute path.
func (x *extractor) baseEval(base string) (string, error) {
	if eb, ok := x.evalBase[base]; ok {
		return eb, nil
	}
	if err := os.MkdirAll(base, 0o755); err != nil {
		return "", err
	}
	ab, err := filepath.Abs(base)
	if err != nil {
		return "", err
	}
	eb, err := filepath.EvalSymlinks(ab)
	if err != nil {
		return "", err
	}
	x.evalBase[base] = eb
	return eb, nil
}

// confine ensures dest's parent directory, after resolving symlinks,
// still lives under base. Parents are created as needed.
func (x *extractor) confine(base, dest string) error {
	eb, err := x.baseEval(base)
	if err != nil {
		return err
	}
	parent := filepath.Dir(dest)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	ap, err := filepath.Abs(parent)
	if err != nil {
		return err
	}
	rp, err := filepath.EvalSymlinks(ap)
	if err != nil {
		return err
	}
	if rp != eb && !strings.HasPrefix(rp, eb+string(os.PathSeparator)) {
		return fmt.Errorf("destination %s escapes %s (via symlink)", dest, base)
	}
	return nil
}

// prepare clears whatever currently occupies dest (file rules may
// write outside any confined base; their parents are created here too).
func (x *extractor) prepare(dest string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	if err := os.Remove(dest); err != nil && !os.IsNotExist(err) {
		// Replacing a non-empty directory with a file is a real
		// conflict; surface it.
		return err
	}
	return nil
}

// applyMeta restores (or overrides) ownership and permissions.
// linkOnly skips the chmod (symlinks).
func (x *extractor) applyMeta(dest string, hdr *stdtar.Header, linkOnly bool) error {
	uid, gid := hdr.Uid, hdr.Gid
	if x.o.Chown != nil {
		uid, gid = x.o.Chown.UID, x.o.Chown.GID
	}
	if !x.o.NoChown {
		if err := os.Lchown(dest, uid, gid); err != nil {
			if x.o.Chown == nil && errors.Is(err, syscall.EPERM) {
				if !x.chownWarned {
					x.o.warnf("tar: cannot preserve ownership (not root); continuing without")
					x.chownWarned = true
				}
			} else {
				return fmt.Errorf("chown %s -> %d:%d: %w", dest, uid, gid, err)
			}
		}
	}
	if linkOnly {
		return nil
	}
	mode := hdr.FileInfo().Mode() & (fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky)
	if x.o.Chmod != nil {
		mode = *x.o.Chmod
	}
	return os.Chmod(dest, mode)
}
