package tar

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// unixEpoch normalizes runner-derived timestamps on spooled stdin
// entries.
var unixEpoch = time.Unix(0, 0)

// The engine drives a GNU tar binary instead of encoding tar streams
// itself: sparse handling, the wire format and its edge cases stay GNU
// tar's problem. The package contributes rule resolution, deterministic
// invocation flags and stream plumbing.
//
// Multi-rule archives are produced by one tar invocation per rule with
// `-b 1` (blocking factor 1), under which a GNU tar archive ends in
// exactly two 512-byte zero blocks; the engine strips that trailer from
// every invocation and appends a single fresh one, yielding one valid
// stream (verified against both GNU tar and Go's archive/tar reader).

// trailerSize is the end-of-archive marker emitted with -b 1: two
// 512-byte zero blocks.
const trailerSize = 1024

// minVersion is the oldest GNU tar the engine accepts: --sort=name
// appeared in 1.28.
var minVersion = [2]int{1, 28}

// TarPathEnv overrides binary discovery (same pattern as
// MKFS_EROFS_PATH for mkfs.erofs).
const TarPathEnv = "TAR_PATH"

var (
	locateOnce sync.Once
	tarPath    string
	tarErr     error
)

// LocateTar finds the GNU tar binary: $TAR_PATH, then a `tar` next to
// the running executable, then $PATH — and verifies it is GNU tar
// >= 1.28 (busybox/bsdtar lack the sparse and determinism flags this
// package relies on). The result is cached for the process lifetime.
func LocateTar() (string, error) {
	locateOnce.Do(func() {
		tarPath, tarErr = locate()
		if tarErr == nil {
			tarErr = probe(tarPath)
		}
	})
	return tarPath, tarErr
}

func locate() (string, error) {
	if p := os.Getenv(TarPathEnv); p != "" {
		return p, nil
	}
	if exe, err := os.Executable(); err == nil {
		cand := filepath.Join(filepath.Dir(exe), "tar")
		if st, err := os.Stat(cand); err == nil && !st.IsDir() {
			return cand, nil
		}
	}
	if p, err := exec.LookPath("tar"); err == nil {
		return p, nil
	}
	return "", fmt.Errorf("tar: GNU tar not found (set %s, place `tar` alongside the binary, or add it to PATH; `make tar` in sandbox-deps builds a static one)", TarPathEnv)
}

var versionRe = regexp.MustCompile(`GNU tar.*?(\d+)\.(\d+)`)

func probe(bin string) error {
	out, err := exec.Command(bin, "--version").Output()
	if err != nil {
		return fmt.Errorf("tar: probe %s: %w", bin, err)
	}
	m := versionRe.FindSubmatch(out)
	if m == nil {
		first, _, _ := strings.Cut(string(out), "\n")
		return fmt.Errorf("tar: %s is not GNU tar (%q); busybox/bsdtar lack the required sparse/determinism flags", bin, first)
	}
	maj, _ := strconv.Atoi(string(m[1]))
	min, _ := strconv.Atoi(string(m[2]))
	if maj < minVersion[0] || (maj == minVersion[0] && min < minVersion[1]) {
		return fmt.Errorf("tar: %s is GNU tar %d.%d; need >= %d.%d (--sort=name)", bin, maj, min, minVersion[0], minVersion[1])
	}
	return nil
}

// commonCreateArgs are the deterministic-output flags shared by every
// create invocation: PAX format with stable extended-header names and
// no atime/ctime records, sorted members, numeric ownership, GNU PAX
// sparse 1.0 hole encoding, exact 1024-byte trailer via -b 1.
func commonCreateArgs() []string {
	return []string{
		"-b", "1",
		"--format=posix",
		"--pax-option=exthdr.name=%d/PaxHeaders/%f,delete=atime,delete=ctime",
		"--sort=name",
		"--numeric-owner",
		"--sparse",
	}
}

// Create writes a tar stream containing the entries selected by rules:
// the rule's left side names the entry inside the archive, the right
// side the filesystem source ("-" reads the content from stdin via a
// spool file). Directory rules recurse. Holes in regular files are
// preserved (GNU --sparse, PAX 1.0). Rules run in order; each is one
// tar invocation and the streams are joined trailer-free.
func Create(w io.Writer, rules []Rule, o Options) error {
	if len(rules) == 0 {
		return fmt.Errorf("tar create: at least one rule is required")
	}
	bin, err := LocateTar()
	if err != nil {
		return err
	}
	for _, r := range rules {
		if err := createRule(bin, w, r, &o); err != nil {
			return err
		}
	}
	_, err = w.Write(make([]byte, trailerSize))
	return err
}

func createRule(bin string, w io.Writer, r Rule, o *Options) error {
	fsPath := r.FS
	stdinEntry := r.FS == "-"
	if stdinEntry { // stdin content: spool so tar can stat it
		tmp, err := os.CreateTemp(o.TmpDir, "tar-stdin-*")
		if err != nil {
			return err
		}
		defer os.Remove(tmp.Name())
		_, cpErr := io.Copy(tmp, o.stdin())
		if err := tmp.Close(); cpErr != nil || err != nil {
			return fmt.Errorf("spool stdin: %w", firstErr(cpErr, err))
		}
		// Deterministic entry metadata: the spool file's own owner and
		// timestamp are runner artifacts.
		if err := os.Chmod(tmp.Name(), 0o644); err != nil {
			return err
		}
		if err := os.Chtimes(tmp.Name(), unixEpoch, unixEpoch); err != nil {
			return err
		}
		fsPath = tmp.Name()
	}

	st, err := os.Lstat(fsPath)
	if err != nil {
		return err
	}
	args := commonCreateArgs()
	switch {
	case o.Chown != nil:
		args = append(args, "--owner=:"+strconv.Itoa(o.Chown.UID), "--group=:"+strconv.Itoa(o.Chown.GID))
	case stdinEntry:
		args = append(args, "--owner=:0", "--group=:0") // not the runner's uid
	}
	if o.Chmod != nil {
		args = append(args, "--mode="+strconv.FormatInt(tarMode(*o.Chmod), 8))
	}

	switch {
	case r.Prefix:
		if st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%s: directory rule on a symlink; point at its target", r.FS)
		}
		if !st.IsDir() {
			return fmt.Errorf("%s: directory rule (trailing /) on a non-directory", r.FS)
		}
		if r.Tar != "" {
			// "./x" → "<tar>/x"; the root entry matches as "." (GNU
			// applies transforms to names without their trailing /).
			e1, err := transformExpr(`^\./`, r.Tar+"/")
			if err != nil {
				return err
			}
			e2, err := transformExpr(`^\.$`, r.Tar)
			if err != nil {
				return err
			}
			args = append(args, e1, e2)
		}
		args = append(args, "-cf", "-", "-C", fsPath, ".")
	default:
		if st.IsDir() {
			return fmt.Errorf("%s is a directory; use a trailing / (%s/)", r.FS, r.FS)
		}
		dir, base := filepath.Split(fsPath)
		if dir == "" {
			dir = "."
		}
		if base != r.Tar {
			expr, err := transformExpr("^"+escapeBRE(base)+"$", r.Tar)
			if err != nil {
				return err
			}
			args = append(args, expr)
		}
		args = append(args, "-cf", "-", "-C", dir, base)
	}

	cmd := exec.Command(bin, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	tt := &tailTrim{w: w, keep: trailerSize}
	cmd.Stdout = tt
	if err := cmd.Run(); err != nil {
		return tarFail("create", r, err, &stderr)
	}
	if msg := strings.TrimSpace(stderr.String()); msg != "" {
		o.warnf("%s", msg)
	}
	if tt.buffered() != trailerSize {
		return fmt.Errorf("tar create %s: short archive (%d trailing bytes)", r.Tar, tt.buffered())
	}
	return nil
}

// Extract materializes the entries selected by rules from the tar
// stream: the rule's left side names the entry inside the archive, the
// right side the filesystem destination ("-" streams the content to
// stdout). No rules selects everything into the current directory.
// GNU tar performs the extraction (sparse members get their holes
// back; ../ members and symlink walk-outs are refused by its
// hardening); Chown/Chmod run as a post-pass over the extracted paths.
func Extract(r io.Reader, rules []Rule, o Options) error {
	if len(rules) == 0 {
		rules = []Rule{{Tar: "", FS: ".", Prefix: true}}
	}
	bin, err := LocateTar()
	if err != nil {
		return err
	}

	// The stream is consumed once per matching rule, so it must be
	// seekable: spool non-file readers.
	arch, cleanup, err := archiveFile(r, o.TmpDir)
	if err != nil {
		return err
	}
	defer cleanup()

	names, err := listArchive(bin, arch)
	if err != nil {
		return err
	}

	// Group stored member names by the rule that selects them.
	picked := make(map[int][]sel)
	for _, stored := range names {
		norm, err := normalizeTarPath(stored)
		if err != nil || norm == "" {
			continue
		}
		for i := range rules {
			if _, rel, ok := match(rules[i:i+1], norm); ok {
				picked[i] = append(picked[i], sel{stored: stored, norm: norm, rel: rel})
			}
		}
	}

	var extracted []string // for the chown/chmod post-pass
	for i, rl := range rules {
		members := picked[i]
		if len(members) == 0 {
			if rl.Tar != "" {
				return fmt.Errorf("tar extract: %q not found in archive", rl.Tar)
			}
			continue
		}
		switch {
		case rl.FS == "-":
			if err := extractStdout(bin, arch, members[0].stored, o.stdout()); err != nil {
				return err
			}
		case rl.Prefix:
			if err := os.MkdirAll(rl.FS, 0o755); err != nil {
				return err
			}
			e0, err := transformExpr(`^\./`, "")
			if err != nil {
				return err
			}
			args := []string{"-xf", arch, "--no-wildcards", "-C", rl.FS, e0}
			if rl.Tar != "" {
				// strip "<tar>/" → "" and map the bare "<tar>" dir → "."
				e1, err := transformExpr("^"+escapeBRE(rl.Tar)+"/", "")
				if err != nil {
					return err
				}
				e2, err := transformExpr("^"+escapeBRE(rl.Tar)+"$", ".")
				if err != nil {
					return err
				}
				args = append(args, e1, e2)
				// GNU flags each member argument "found" on its first
				// match only, so a descendant already covered by a dir
				// argument would report "Not found in archive": pass a
				// minimal covering set. (Archive-root rules pass no
				// member args at all and extract everything.)
				for _, m := range minimalCover(members) {
					args = append(args, m)
				}
			}
			if err := runTar(bin, args, "extract", rl, &o); err != nil {
				return err
			}
			for _, m := range members {
				if m.rel == "" {
					continue // the prefix directory itself == rl.FS
				}
				extracted = append(extracted, filepath.Join(rl.FS, filepath.FromSlash(m.rel)))
			}
			extracted = append(extracted, rl.FS)
		default:
			if err := extractFileRule(bin, arch, members[0].stored, rl, &o); err != nil {
				return err
			}
			extracted = append(extracted, rl.FS)
		}
	}
	return postChownChmod(extracted, &o)
}

// sel is one archive member selected by a rule.
type sel struct {
	stored string // exact name in the archive
	norm   string // normalized (matching) form
	rel    string // path relative to the rule's Tar prefix
}

// minimalCover returns the stored names whose normalized form has no
// proper ancestor in the same selection: tar extracts directories
// recursively, and redundant descendant arguments would be reported
// as "Not found in archive".
func minimalCover(members []sel) []string {
	in := make(map[string]bool, len(members))
	for _, m := range members {
		in[m.norm] = true
	}
	var out []string
	for _, m := range members {
		covered := false
		for p := m.norm; ; {
			i := strings.LastIndexByte(p, '/')
			if i < 0 {
				break
			}
			p = p[:i]
			if in[p] {
				covered = true
				break
			}
		}
		if !covered {
			out = append(out, m.stored)
		}
	}
	return out
}

// archiveFile returns a path to the archive content, spooling r to a
// temp file unless it already is a regular file with a real path. The
// path must be resolved through /proc/self/fd, not f.Name(): a
// redirected stdin reports "/dev/stdin", which a tar child process
// would resolve to its own fd 0.
func archiveFile(r io.Reader, tmpDir string) (string, func(), error) {
	if f, ok := r.(*os.File); ok {
		if st, err := f.Stat(); err == nil && st.Mode().IsRegular() {
			p, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", f.Fd()))
			if err == nil && strings.HasPrefix(p, "/") {
				if pst, err := os.Stat(p); err == nil && os.SameFile(st, pst) {
					return p, func() {}, nil
				}
			}
			// fall through: spool (non-linux, deleted file, ...)
		}
	}
	tmp, err := os.CreateTemp(tmpDir, "tar-extract-*")
	if err != nil {
		return "", nil, err
	}
	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", nil, fmt.Errorf("spool archive: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", nil, err
	}
	return tmp.Name(), func() { os.Remove(tmp.Name()) }, nil
}

// listArchive returns the stored member names (file names containing
// newlines are out of scope, as they are for tar -t itself).
func listArchive(bin, arch string) ([]string, error) {
	var out, stderr bytes.Buffer
	cmd := exec.Command(bin, "-tf", arch)
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, tarFail("list", Rule{}, err, &stderr)
	}
	var names []string
	for l := range strings.SplitSeq(out.String(), "\n") {
		if l != "" {
			names = append(names, l)
		}
	}
	return names, nil
}

func extractStdout(bin, arch, stored string, w io.Writer) error {
	var stderr bytes.Buffer
	cmd := exec.Command(bin, "-xOf", arch, "--no-wildcards", stored)
	cmd.Stdout = w
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return tarFail("extract", Rule{Tar: stored}, err, &stderr)
	}
	return nil
}

// extractFileRule extracts one member into a staging dir next to the
// destination (same filesystem) and renames it into place, so
// arbitrary renames need no pattern rewriting at all.
func extractFileRule(bin, arch, stored string, rl Rule, o *Options) error {
	destDir := filepath.Dir(rl.FS)
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(destDir, ".tar-extract-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	if err := runTar(bin, []string{"-xf", arch, "--no-wildcards", "-C", stage, stored}, "extract", rl, o); err != nil {
		return err
	}
	norm, err := normalizeTarPath(stored)
	if err != nil {
		return err
	}
	src := filepath.Join(stage, filepath.FromSlash(norm))
	if err := os.Remove(rl.FS); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(src, rl.FS)
}

func runTar(bin string, args []string, op string, rl Rule, o *Options) error {
	var stderr bytes.Buffer
	cmd := exec.Command(bin, args...)
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return tarFail(op, rl, err, &stderr)
	}
	if msg := strings.TrimSpace(stderr.String()); msg != "" {
		o.warnf("%s", msg)
	}
	return nil
}

func tarFail(op string, rl Rule, err error, stderr *bytes.Buffer) error {
	msg := strings.TrimSpace(stderr.String())
	what := rl.Tar
	if what == "" {
		what = rl.FS
	}
	if what != "" {
		what = " " + what
	}
	if msg != "" {
		return fmt.Errorf("tar %s%s: %w: %s", op, what, err, msg)
	}
	return fmt.Errorf("tar %s%s: %w", op, what, err)
}

// postChownChmod applies the Chown/Chmod overrides over the extracted
// paths (GNU tar has no extract-time owner override). Symlink modes
// are left alone.
func postChownChmod(paths []string, o *Options) error {
	if o.Chown == nil && o.Chmod == nil {
		return nil
	}
	for _, p := range paths {
		st, err := os.Lstat(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue // e.g. a member tar itself refused
			}
			return err
		}
		if o.Chown != nil {
			if err := os.Lchown(p, o.Chown.UID, o.Chown.GID); err != nil {
				return fmt.Errorf("chown %s: %w", p, err)
			}
		}
		if o.Chmod != nil && st.Mode()&os.ModeSymlink == 0 {
			if err := os.Chmod(p, *o.Chmod); err != nil {
				return fmt.Errorf("chmod %s: %w", p, err)
			}
		}
	}
	return nil
}

// tailTrim forwards everything written through it except the final
// `keep` bytes, which it holds back (the per-invocation end-of-archive
// trailer that must not reach the joined stream).
type tailTrim struct {
	w    io.Writer
	keep int
	tail []byte
}

func (t *tailTrim) Write(p []byte) (int, error) {
	n := len(p)
	t.tail = append(t.tail, p...)
	if over := len(t.tail) - t.keep; over > 0 {
		if _, err := t.w.Write(t.tail[:over]); err != nil {
			return 0, err
		}
		t.tail = append(t.tail[:0], t.tail[over:]...)
	}
	return n, nil
}

func (t *tailTrim) buffered() int { return len(t.tail) }

// transformDelim separates the s-expression parts of a --transform
// argument. \x01 cannot be escaped inside the expression, so paths
// containing it (or a newline, which breaks tar -t listing) are
// rejected outright. Arguments reach tar via execve, never a shell.
const transformDelim = "\x01"

// transformExpr renders a GNU --transform argument from a left-side
// BRE (already escaped where literal) and a literal replacement.
// flags=rhS rewrites member names and hardlink targets but leaves
// symlink targets alone — they are relative to the link's location,
// not the archive root.
func transformExpr(lhsBRE, rhs string) (string, error) {
	for _, part := range []string{lhsBRE, rhs} {
		if strings.ContainsAny(part, transformDelim+"\n") {
			return "", fmt.Errorf("tar: path %q contains bytes a --transform expression cannot carry", part)
		}
	}
	var b strings.Builder
	for _, c := range []byte(rhs) {
		if c == '\\' || c == '&' {
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	return "--transform=flags=rhS;s" + transformDelim + lhsBRE + transformDelim + b.String() + transformDelim, nil
}

// escapeBRE makes a literal string safe inside the basic (sed-style)
// regular expressions GNU --transform uses. Only BRE metacharacters
// are escaped; backslashing + ? ( ) { } | would turn those literals
// into GNU BRE operators.
func escapeBRE(s string) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		switch c {
		case '\\', '.', '[', ']', '*', '^', '$':
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	return b.String()
}

// tarMode converts permission + setuid/setgid/sticky bits of an
// fs.FileMode into tar mode bits (octal for --mode).
func tarMode(m os.FileMode) int64 {
	v := int64(m & os.ModePerm)
	if m&os.ModeSetuid != 0 {
		v |= 0o4000
	}
	if m&os.ModeSetgid != 0 {
		v |= 0o2000
	}
	if m&os.ModeSticky != 0 {
		v |= 0o1000
	}
	return v
}

func firstErr(a, b error) error {
	if a != nil {
		return a
	}
	return b
}
