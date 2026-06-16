// Package flatten converts OCI container images into deterministic EROFS
// filesystem images. It applies layers in order, honours whiteout semantics,
// normalises timestamps, and invokes mkfs.erofs for byte-stable output, then
// appends the OCI runtime-config projection as a STORED-mode ZIP trailer.
//
// The engine is split into a Source (where the ordered layers + config come
// from) and a single sink (Build). Two sources share that sink:
//
//   - the docker-archive source in this package (FlattenWith / FlattenFile),
//     fed by `docker save` output on disk or stdin;
//   - the registry source in sibling package pkg/remote, fed by a pulled
//     image.
//
// Routing both through the same Build is what guarantees a registry pull and
// an equivalent docker-archive flatten produce byte-identical EROFS images.
// This package stays stdlib-only (plus pkg/image + internal/util); the
// go-containerregistry dependency lives entirely in pkg/remote.
package flatten

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/kuasar-sandbox/sandbox-accelerator/internal/util"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/image"
)

// dockerManifestEntry describes one image in a docker-archive tar.
type dockerManifestEntry struct {
	Config   string   `json:"Config"`
	RepoTags []string `json:"RepoTags"`
	Layers   []string `json:"Layers"`
}

// Options tweaks Flatten's environment. Zero value reproduces the
// historic behaviour (temp dir under $TMPDIR or /tmp).
type Options struct {
	// TmpDir overrides the parent of the per-run scratch directory.
	// Useful when /tmp is small and images are large. Empty → default
	// (os.MkdirTemp("", ...) which respects $TMPDIR).
	TmpDir string

	// Progress, if non-nil, receives coarse stage notifications during a
	// flatten so a caller (the CLI) can render progress; the library stays
	// silent when nil. stage is a short token; done/total carry per-stage
	// detail:
	//   - "extract-archive" (docker-archive source only): running / total
	//     bytes extracted from the input tar (total is 0 when the input is a
	//     pipe whose size isn't known ahead of time). Fires frequently — the
	//     consumer throttles its own output.
	//   - "apply-layers": 1-based layer index in done, layer count in total.
	//   - "build-erofs" / "append-config": done=total=0.
	Progress func(stage string, done, total int)
}

// LayerOpener opens one layer's *uncompressed* tar stream. The caller
// closes the returned ReadCloser. Each call should yield a fresh stream
// positioned at the start of the layer tar.
type LayerOpener func() (io.ReadCloser, error)

// Source supplies the ordered layers (base→top) and the raw OCI image
// config JSON for a single image. Both the docker-archive path here and
// the registry path in pkg/remote implement it so they share Build's
// deterministic sink.
type Source interface {
	// Layers returns layer openers in application order (base first).
	// Each opener yields an *uncompressed* tar stream.
	Layers() ([]LayerOpener, error)
	// ConfigJSON returns the raw OCI image-config JSON document, which
	// Build projects via image.ExtractRuntimeConfigFromJSON.
	ConfigJSON() ([]byte, error)
}

// Build flattens src into a deterministic EROFS image at outputPath with
// the OCI runtime-config ZIP appended. It is the single sink shared by
// every Source. Determinism comes from applying identical uncompressed
// tar streams in order, normalising all timestamps to the epoch, the
// fixed mkfs.erofs flags (buildEROFS), and the shared RuntimeConfig
// projection (image.ExtractRuntimeConfigFromJSON).
func Build(src Source, outputPath string, opts Options) error {
	workDir, err := os.MkdirTemp(opts.TmpDir, "flatten-rootfs-*")
	if err != nil {
		return fmt.Errorf("flatten: create temp dir: %w", err)
	}
	defer os.RemoveAll(workDir)

	rootfsDir := filepath.Join(workDir, "rootfs")
	if err := os.MkdirAll(rootfsDir, 0o755); err != nil {
		return fmt.Errorf("flatten: mkdir rootfs: %w", err)
	}

	// Step 1: apply layers in order to build the flattened rootfs.
	layers, err := src.Layers()
	if err != nil {
		return fmt.Errorf("flatten: list layers: %w", err)
	}
	if len(layers) == 0 {
		return fmt.Errorf("flatten: image has no layers")
	}
	for i, open := range layers {
		if opts.Progress != nil {
			opts.Progress("apply-layers", i+1, len(layers))
		}
		if err := applyLayerOpener(open, rootfsDir); err != nil {
			return fmt.Errorf("flatten: apply layer %d: %w", i, err)
		}
	}

	// Step 2: normalise the rootfs for deterministic output.
	if err := normalizeTimestamps(rootfsDir); err != nil {
		return fmt.Errorf("flatten: normalize timestamps: %w", err)
	}

	// Step 3: build the EROFS image.
	if opts.Progress != nil {
		opts.Progress("build-erofs", 0, 0)
	}
	if err := buildImage(rootfsDir, outputPath); err != nil {
		return err
	}

	// Step 4: project the OCI image config and append it as a STORED-mode
	// ZIP after the EROFS. EROFS mount/read remains correct because the
	// EROFS image's extent is described in its superblock; the ZIP trailer
	// is addressable independently via standard tools (`unzip -l`,
	// `archive/zip`, `flatten-ctl info`).
	if opts.Progress != nil {
		opts.Progress("append-config", 0, 0)
	}
	cfgJSON, err := src.ConfigJSON()
	if err != nil {
		return fmt.Errorf("flatten: read config: %w", err)
	}
	cfg, err := image.ExtractRuntimeConfigFromJSON(cfgJSON)
	if err != nil {
		return fmt.Errorf("flatten: extract config: %w", err)
	}
	if err := image.AppendConfigZip(outputPath, cfg); err != nil {
		return fmt.Errorf("flatten: append config zip: %w", err)
	}

	return nil
}

// Flatten converts a docker-archive tar stream into an EROFS image.
// The input must be a tar produced by `docker save`; layer tars inside
// may be plain or gzip-compressed. mkfs.erofs is located via
// locateMkfsErofs; if none is found the call fails rather than falling
// back silently.
func Flatten(input io.Reader, outputPath string) error {
	return FlattenWith(input, outputPath, Options{})
}

// FlattenWith is the explicit-options form of Flatten.
func FlattenWith(input io.Reader, outputPath string, opts Options) error {
	src, cleanup, err := newDockerArchiveSource(input, opts)
	if err != nil {
		return err
	}
	defer cleanup()
	return Build(src, outputPath, opts)
}

// FlattenFile converts a docker-archive tar file into an EROFS image.
func FlattenFile(inputPath, outputPath string) error {
	return FlattenFileWith(inputPath, outputPath, Options{})
}

// FlattenFileWith is the explicit-options form of FlattenFile.
func FlattenFileWith(inputPath, outputPath string, opts Options) error {
	f, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("flatten: open input: %w", err)
	}
	defer f.Close()
	return FlattenWith(f, outputPath, opts)
}

// Verify flattens the same input twice and checks byte-identical output.
// Returns (match, hash1, hash2, error). Both inputs are rewound to the start
// before flattening.
func Verify(input1, input2 io.ReadSeeker) (bool, string, string, error) {
	tmp1, err := os.CreateTemp("", "verify1-*.img")
	if err != nil {
		return false, "", "", fmt.Errorf("verify: create temp1: %w", err)
	}
	defer os.Remove(tmp1.Name())
	defer tmp1.Close()

	tmp2, err := os.CreateTemp("", "verify2-*.img")
	if err != nil {
		return false, "", "", fmt.Errorf("verify: create temp2: %w", err)
	}
	defer os.Remove(tmp2.Name())
	defer tmp2.Close()

	if _, err := input1.Seek(0, io.SeekStart); err != nil {
		return false, "", "", fmt.Errorf("verify: seek input1: %w", err)
	}
	if err := Flatten(input1, tmp1.Name()); err != nil {
		return false, "", "", fmt.Errorf("verify: flatten input1: %w", err)
	}

	if _, err := input2.Seek(0, io.SeekStart); err != nil {
		return false, "", "", fmt.Errorf("verify: seek input2: %w", err)
	}
	if err := Flatten(input2, tmp2.Name()); err != nil {
		return false, "", "", fmt.Errorf("verify: flatten input2: %w", err)
	}

	hash1, err := hashFile(tmp1.Name())
	if err != nil {
		return false, "", "", fmt.Errorf("verify: hash output1: %w", err)
	}

	hash2, err := hashFile(tmp2.Name())
	if err != nil {
		return false, "", "", fmt.Errorf("verify: hash output2: %w", err)
	}

	return hash1 == hash2, hash1, hash2, nil
}

// --- docker-archive source ---

// dockerArchiveSource is a flatten.Source backed by an extracted
// docker-archive on disk: layer files are read from archiveDir (and
// transparently un-gzipped if needed), config JSON from the path named in
// manifest.json's "Config" field.
type dockerArchiveSource struct {
	archiveDir string
	entry      *dockerManifestEntry
}

// newDockerArchiveSource extracts the docker-archive tar from input into a
// fresh temp dir under opts.TmpDir, parses manifest.json, and returns a Source
// over it plus a cleanup func that removes the temp dir. The cleanup is
// always safe to call (even on error it is a no-op nil). When opts.Progress is
// set it reports extraction progress as the "extract-archive" stage, with the
// running and total byte counts (total is the input file size when input is a
// regular file — `docker save … > img.tar` or stdin redirected from one — and
// 0 for a pipe, where the size isn't known ahead of time).
func newDockerArchiveSource(input io.Reader, opts Options) (*dockerArchiveSource, func(), error) {
	archiveDir, err := os.MkdirTemp(opts.TmpDir, "flatten-archive-*")
	if err != nil {
		return nil, func() {}, fmt.Errorf("flatten: create temp dir: %w", err)
	}
	cleanup := func() { os.RemoveAll(archiveDir) }

	var onBytes func(done int64)
	if opts.Progress != nil {
		var total int64
		if f, ok := input.(*os.File); ok {
			if fi, err := f.Stat(); err == nil && fi.Mode().IsRegular() {
				total = fi.Size()
			}
		}
		onBytes = func(done int64) { opts.Progress("extract-archive", int(done), int(total)) }
	}

	if err := extractTar(input, archiveDir, onBytes); err != nil {
		cleanup()
		return nil, func() {}, fmt.Errorf("flatten: extract archive: %w", err)
	}
	entry, err := parseDockerManifestEntry(archiveDir)
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	return &dockerArchiveSource{archiveDir: archiveDir, entry: entry}, cleanup, nil
}

func (s *dockerArchiveSource) Layers() ([]LayerOpener, error) {
	openers := make([]LayerOpener, 0, len(s.entry.Layers))
	for _, layerPath := range s.entry.Layers {
		full := filepath.Join(s.archiveDir, layerPath)
		openers = append(openers, func() (io.ReadCloser, error) {
			return openMaybeGzip(full)
		})
	}
	return openers, nil
}

func (s *dockerArchiveSource) ConfigJSON() ([]byte, error) {
	return os.ReadFile(filepath.Join(s.archiveDir, s.entry.Config))
}

// --- internal helpers ---

// extractTar extracts all entries from a tar stream into destDir. When onBytes
// is non-nil it is invoked with the running count of bytes consumed from r as
// extraction proceeds (callers throttle their own rendering).
func extractTar(r io.Reader, destDir string, onBytes func(done int64)) error {
	if onBytes != nil {
		r = &countingReader{r: r, onRead: onBytes}
	}
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		target := filepath.Join(destDir, hdr.Name)
		if !strings.HasPrefix(filepath.Clean(target), filepath.Clean(destDir)+string(os.PathSeparator)) {
			// Guard against path traversal.
			continue
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			if err := writeFile(target, hdr.FileInfo().Mode(), tr); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			// Remove any existing entry before creating symlink.
			os.Remove(target)
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
		case tar.TypeLink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			linkTarget := filepath.Join(destDir, hdr.Linkname)
			os.Remove(target)
			if err := os.Link(linkTarget, target); err != nil {
				return err
			}
		}
	}
}

// countingReader wraps r and reports the running total of bytes read after
// each Read via onRead. onRead must be cheap — it fires per Read call, so any
// throttling of the rendered output happens on the consumer side.
type countingReader struct {
	r      io.Reader
	n      int64
	onRead func(done int64)
}

func (c *countingReader) Read(p []byte) (int, error) {
	m, err := c.r.Read(p)
	if m > 0 {
		c.n += int64(m)
		c.onRead(c.n)
	}
	return m, err
}

// writeFile creates or truncates a file and writes data from r.
func writeFile(path string, mode os.FileMode, r io.Reader) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// parseDockerManifestEntry reads manifest.json from an extracted
// docker-archive and returns the first image entry, which carries
// both the layer list and the path of the OCI image config JSON.
func parseDockerManifestEntry(archiveDir string) (*dockerManifestEntry, error) {
	data, err := os.ReadFile(filepath.Join(archiveDir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("flatten: read manifest.json: %w", err)
	}

	var entries []dockerManifestEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("flatten: parse manifest.json: %w", err)
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("flatten: manifest.json contains no images")
	}
	if len(entries[0].Layers) == 0 {
		return nil, fmt.Errorf("flatten: image has no layers")
	}
	if entries[0].Config == "" {
		return nil, fmt.Errorf("flatten: manifest.json[0].Config is empty")
	}

	return &entries[0], nil
}

// openMaybeGzip opens path and, when it is gzip-compressed, wraps it in a
// gzip reader so the caller always sees an uncompressed tar stream. The
// returned ReadCloser closes the gzip reader (if any) and the file.
func openMaybeGzip(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	// Detect gzip by the magic bytes, then rewind.
	buf := make([]byte, 2)
	n, _ := f.Read(buf)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	if n == 2 && buf[0] == 0x1f && buf[1] == 0x8b {
		gz, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("gzip open: %w", err)
		}
		return &gzipReadCloser{gz: gz, f: f}, nil
	}
	return f, nil
}

// gzipReadCloser closes both the gzip reader and the underlying file.
type gzipReadCloser struct {
	gz *gzip.Reader
	f  *os.File
}

func (g *gzipReadCloser) Read(p []byte) (int, error) { return g.gz.Read(p) }

func (g *gzipReadCloser) Close() error {
	gerr := g.gz.Close()
	ferr := g.f.Close()
	if gerr != nil {
		return gerr
	}
	return ferr
}

// applyLayerOpener opens one layer's uncompressed tar stream and applies
// it to the rootfs, ensuring the stream is closed afterwards.
func applyLayerOpener(open LayerOpener, rootfsDir string) error {
	rc, err := open()
	if err != nil {
		return err
	}
	defer rc.Close()
	return applyLayerTar(rc, rootfsDir)
}

// applyLayerTar applies a single *uncompressed* layer tar stream onto the
// rootfs, handling OCI whiteouts. This is the determinism-critical core
// shared by the docker-archive and registry sources: identical tar bytes
// in identical order produce an identical rootfs.
func applyLayerTar(r io.Reader, rootfsDir string) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		name := filepath.Clean(hdr.Name)

		// Handle opaque whiteout: clear the entire directory.
		base := filepath.Base(name)
		dir := filepath.Dir(name)
		if base == ".wh..wh..opq" {
			opaqueDir := filepath.Join(rootfsDir, dir)
			if err := clearDirectory(opaqueDir); err != nil {
				return fmt.Errorf("opaque whiteout %s: %w", dir, err)
			}
			continue
		}

		// Handle file whiteout: delete the named entry.
		if strings.HasPrefix(base, ".wh.") {
			deleteName := strings.TrimPrefix(base, ".wh.")
			deletePath := filepath.Join(rootfsDir, dir, deleteName)
			if err := os.RemoveAll(deletePath); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("whiteout %s: %w", deletePath, err)
			}
			continue
		}

		target := filepath.Join(rootfsDir, name)
		if !strings.HasPrefix(filepath.Clean(target), filepath.Clean(rootfsDir)+string(os.PathSeparator)) &&
			filepath.Clean(target) != filepath.Clean(rootfsDir) {
			continue
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			if err := applyOwnerMode(target, hdr, true); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			// Remove existing file/symlink before writing.
			os.Remove(target)
			if err := writeFile(target, hdr.FileInfo().Mode(), tr); err != nil {
				return err
			}
			if err := applyOwnerMode(target, hdr, true); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			os.Remove(target)
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
			if err := applyOwnerMode(target, hdr, false); err != nil {
				return err
			}
		case tar.TypeLink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			linkTarget := filepath.Join(rootfsDir, hdr.Linkname)
			os.Remove(target)
			if err := os.Link(linkTarget, target); err != nil {
				return err
			}
			// hardlink shares the target inode's owner/mode; nothing to set here.
		}
	}
}

// applyOwnerMode preserves the tar entry's ownership and (for non-symlinks) mode
// onto target, so the flattened image keeps the source image's real uid/gid and
// permission bits — e.g. /tmp stays 1777 and /home/<user> stays user-owned, so a
// non-root guest user can write to its home. Owner is set BEFORE mode because
// chown clears setuid/setgid and the chmod restores them.
//
// Preserving the image's real uid/gid requires root (CAP_CHOWN); the e2b build
// runs flatten-ctl in the root sandbox-builder unit. An unprivileged flatten
// fails here with a clear error rather than silently producing an image whose
// ownership is all wrong (the previous --all-root behaviour).
func applyOwnerMode(target string, hdr *tar.Header, chmod bool) error {
	if err := os.Lchown(target, hdr.Uid, hdr.Gid); err != nil {
		return fmt.Errorf("chown %s -> %d:%d (preserving image ownership requires root/CAP_CHOWN): %w",
			hdr.Name, hdr.Uid, hdr.Gid, err)
	}
	if chmod {
		// hdr.FileInfo().Mode() carries permission + setuid/setgid/sticky; os.Chmod
		// applies those and ignores the type bits.
		if err := os.Chmod(target, hdr.FileInfo().Mode()); err != nil {
			return err
		}
	}
	return nil
}

// clearDirectory removes all entries inside dir but keeps dir itself.
func clearDirectory(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(dir, 0o755)
		}
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// normalizeTimestamps walks the rootfs and sets all file/directory timestamps
// to the Unix epoch (0) for deterministic output.
func normalizeTimestamps(rootfsDir string) error {
	epoch := unixEpoch
	return filepath.Walk(rootfsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Symlinks: Chtimes follows the link, which is fine for the target.
		// For the symlink itself, we skip since os.Chtimes follows symlinks
		// and Lchtimes is not in the standard library.
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		return os.Chtimes(path, epoch, epoch)
	})
}

// locateMkfsErofs resolves the mkfs.erofs binary with a fixed
// precedence: $MKFS_EROFS_PATH > directory of running flatten-ctl
// executable > $PATH. The hint message in the returned error keeps
// users pointed at `make deps-erofs`.
func locateMkfsErofs() (string, error) {
	if p := os.Getenv("MKFS_EROFS_PATH"); p != "" {
		return p, nil
	}
	p, err := util.LocateBinary("mkfs.erofs")
	if err != nil {
		return "", fmt.Errorf("flatten: mkfs.erofs not found " +
			"(set MKFS_EROFS_PATH, place it alongside flatten-ctl, " +
			"or add it to PATH; run `make deps-erofs` to build it)")
	}
	return p, nil
}

// buildImage produces the output image from the flattened rootfs directory
// by invoking mkfs.erofs.
func buildImage(rootfsDir, outputPath string) error {
	mkfsPath, err := locateMkfsErofs()
	if err != nil {
		return err
	}
	return buildEROFS(mkfsPath, rootfsDir, outputPath)
}

// buildEROFS invokes mkfs.erofs to produce a deterministic, dedup-friendly EROFS image.
// Uses chunk-based layout (-Ededupe --chunksize=4096) to minimize metadata size and
// stabilize data block offsets across images, maximizing CDC cross-image dedup.
// No compression: raw bytes enable CDC dedup (consistent with kuasar-sandbox.md §5 design matrix).
func buildEROFS(mkfsPath, rootfsDir, outputPath string, extra ...string) error {
	args := []string{
		"-Ededupe",         // intra-image file dedup
		"--chunksize=4096", // chunk-based layout: metadata 7.6MiB→0.6MiB
		// NOTE: no --all-root. The image's real uid/gid/mode is preserved by
		// applyLayerTar (chown+chmod from the layer tar headers) so a non-root
		// guest user can write to its home; mkfs.erofs records that ownership.
		// Determinism holds: ownership comes from the (fixed) image layers, and
		// flatten runs as root in the sandbox-builder unit. (The runtime erofs —
		// sandbox-init, all root — is built by separate shell scripts that keep
		// --all-root; this Go path is image-flatten only.)
		"-T0",                                        // fixed timestamp (epoch)
		"-b4096",                                     // 4K block size
		"-x-1",                                       // disable xattrs
		"-U", "00000000-0000-0000-0000-000000000000", // fixed UUID
		"--quiet",
	}
	args = append(args, extra...)
	args = append(args, outputPath, rootfsDir)
	cmd := exec.Command(mkfsPath, args...)
	// Chunk-mode mkfs.erofs stages dedup data through tmpfile()/$TMPDIR
	// (default /tmp). Pin it to the output's directory — the flatten scratch
	// — so the build works on rootfs without a /tmp (the in-guest import
	// sandbox boots an EMPTY ext4) and the staging lands on the disk sized
	// for it.
	cmd.Env = append(os.Environ(), "TMPDIR="+filepath.Dir(outputPath))
	cmd.Stdout = io.Discard
	// Capture stderr so a failure carries mkfs.erofs's own diagnostic rather
	// than a bare "exit status 1". (On success mkfs may still print benign
	// notes to stderr — e.g. the "Compression is not enabled" hint — which we
	// ignore.)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return fmt.Errorf("flatten: mkfs.erofs: %w: %s", err, msg)
		}
		return fmt.Errorf("flatten: mkfs.erofs: %w", err)
	}
	return nil
}

// hashFile computes the SHA-256 hex digest of a file.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// unixEpoch is the Unix epoch, used to normalize all timestamps for determinism.
var unixEpoch = time.Unix(0, 0)
