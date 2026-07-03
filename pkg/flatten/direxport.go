package flatten

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/image"
)

// DirOptions configures BuildFromDir.
type DirOptions struct {
	// Skip lists paths relative to the rootfs to leave out of the
	// image, subtree and node included (mkfs.erofs --exclude-path
	// semantics).
	Skip []string
	// SkipMounts excludes every mount point strictly under the rootfs
	// (from /proc/self/mountinfo) the same way.
	SkipMounts bool
	// ConfigJSON is the runtime config to append: either an OCI image
	// config or an already-projected config.json
	// (image.WrapRuntimeConfigJSON sniffs the shape). nil appends an
	// empty config.
	ConfigJSON []byte
	// Warnf receives non-fatal notes (auto-excluded paths). nil
	// silences them.
	Warnf func(format string, args ...any)
}

// BuildFromDir flattens an already-materialized rootfs directory into
// a deterministic EROFS image with the runtime-config ZIP appended —
// the dual of Build for sources that are not layer tars. mkfs.erofs
// reads the directory in place: nothing is staged or copied, the
// source tree is never modified (timestamps are normalized by
// `-T0 --ignore-mtime` at image-build level), and exclusions
// (DirOptions.Skip, mount points) drop the excluded node entirely.
// Preserving file ownership in the image only requires read access to
// the tree, so a full-rootfs export runs as root.
func BuildFromDir(root, outputPath string, opts Options, dopts DirOptions) error {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	absRoot, err = filepath.EvalSymlinks(absRoot)
	if err != nil {
		return fmt.Errorf("flatten: rootfs %s: %w", root, err)
	}
	st, err := os.Stat(absRoot)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return fmt.Errorf("flatten: %s is not a directory", root)
	}
	if absRoot == "/" && !dopts.SkipMounts {
		return fmt.Errorf("flatten: exporting / requires --skip-mounts (otherwise /proc, /sys and friends are walked)")
	}

	warnf := dopts.Warnf
	if warnf == nil {
		warnf = func(string, ...any) {}
	}

	var excludes []string
	for _, s := range dopts.Skip {
		rel, err := normalizeSkip(s)
		if err != nil {
			return fmt.Errorf("flatten: --skip %q: %w", s, err)
		}
		excludes = append(excludes, rel)
	}
	if dopts.SkipMounts {
		mounts, err := mountsUnder(absRoot)
		if err != nil {
			return fmt.Errorf("flatten: scan mount points: %w", err)
		}
		excludes = append(excludes, mounts...)
	}
	// Never let the output image be walked into itself.
	if absOut, err := filepath.Abs(outputPath); err == nil {
		if rel, ok := strictlyUnder(absRoot, absOut); ok {
			excludes = append(excludes, rel)
			warnf("flatten: output %s lives inside the rootfs; auto-excluded", outputPath)
		}
	}
	excludes = dedupeSorted(excludes)

	mkfsPath, err := locateMkfsErofs()
	if err != nil {
		return err
	}
	extra := []string{"--ignore-mtime"}
	for _, e := range excludes {
		extra = append(extra, "--exclude-path="+e)
	}
	if opts.Progress != nil {
		opts.Progress("build-erofs", 0, 0)
	}
	if err := buildEROFS(mkfsPath, absRoot, outputPath, extra...); err != nil {
		return err
	}

	if opts.Progress != nil {
		opts.Progress("append-config", 0, 0)
	}
	cfgJSON := dopts.ConfigJSON
	if cfgJSON == nil {
		cfgJSON = []byte("{}")
	}
	wrapped, err := image.WrapRuntimeConfigJSON(cfgJSON)
	if err != nil {
		return fmt.Errorf("flatten: runtime config: %w", err)
	}
	cfg, err := image.ExtractRuntimeConfigFromJSON(wrapped)
	if err != nil {
		return fmt.Errorf("flatten: runtime config: %w", err)
	}
	if err := image.AppendConfigZip(outputPath, cfg); err != nil {
		return fmt.Errorf("flatten: append config zip: %w", err)
	}
	return nil
}

// normalizeSkip canonicalizes a --skip value to a clean path relative
// to the rootfs ("/x", "./x" and "x" are equivalent).
func normalizeSkip(s string) (string, error) {
	p := strings.TrimPrefix(s, "/")
	for strings.HasPrefix(p, "./") {
		p = strings.TrimPrefix(p, "./")
	}
	p = path.Clean(p)
	if p == "" || p == "." || p == ".." || strings.HasPrefix(p, "../") {
		return "", fmt.Errorf("not a path inside the rootfs")
	}
	return p, nil
}

// strictlyUnder reports whether target lies strictly under root,
// returning its relative path. root == "/" must work: the in-guest
// rootfs export walks literal "/", and a naive root+"/" prefix ("//")
// would silently match nothing — dropping every mount exclusion.
func strictlyUnder(root, target string) (string, bool) {
	prefix := root
	if prefix != string(os.PathSeparator) {
		prefix += string(os.PathSeparator)
	}
	if target == root || !strings.HasPrefix(target, prefix) {
		return "", false
	}
	return target[len(prefix):], true
}

func dedupeSorted(in []string) []string {
	sort.Strings(in)
	out := in[:0]
	var prev string
	for i, s := range in {
		if i > 0 && s == prev {
			continue
		}
		out = append(out, s)
		prev = s
	}
	return out
}

// mountsUnder lists mount points strictly below root (which must be
// absolute with symlinks resolved), as root-relative paths.
func mountsUnder(root string) ([]string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseMountinfo(f, root)
}

func parseMountinfo(r io.Reader, root string) ([]string, error) {
	var out []string
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 5 {
			continue
		}
		mp := unescapeMountPath(fields[4])
		if rel, ok := strictlyUnder(root, mp); ok {
			out = append(out, rel)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return dedupeSorted(out), nil
}

// unescapeMountPath decodes the octal escapes mountinfo uses for
// space, tab, newline and backslash.
func unescapeMountPath(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) &&
			s[i+1] >= '0' && s[i+1] <= '7' &&
			s[i+2] >= '0' && s[i+2] <= '7' &&
			s[i+3] >= '0' && s[i+3] <= '7' {
			b.WriteByte((s[i+1]-'0')<<6 | (s[i+2]-'0')<<3 | (s[i+3] - '0'))
			i += 3
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
