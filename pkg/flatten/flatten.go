// Package flatten converts OCI container images (docker-archive format) into
// EROFS filesystem images. It extracts layers in order, applies whiteout
// semantics, and invokes mkfs.erofs for deterministic output.
package flatten

import (
	"archive/tar"
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
}

// Flatten converts a docker-archive tar stream into an EROFS image.
// The input must be an uncompressed tar produced by `docker save`.
// mkfs.erofs is located via locateMkfsErofs; if none is found the call
// fails rather than falling back silently.
func Flatten(input io.Reader, outputPath string) error {
	return FlattenWith(input, outputPath, Options{})
}

// FlattenWith is the explicit-options form of Flatten.
func FlattenWith(input io.Reader, outputPath string, opts Options) error {
	workDir, err := os.MkdirTemp(opts.TmpDir, "flatten-*")
	if err != nil {
		return fmt.Errorf("flatten: create temp dir: %w", err)
	}
	defer os.RemoveAll(workDir)

	archiveDir := filepath.Join(workDir, "archive")
	rootfsDir := filepath.Join(workDir, "rootfs")
	if err := os.MkdirAll(archiveDir, 0o755); err != nil {
		return fmt.Errorf("flatten: mkdir archive: %w", err)
	}
	if err := os.MkdirAll(rootfsDir, 0o755); err != nil {
		return fmt.Errorf("flatten: mkdir rootfs: %w", err)
	}

	// Step 1: Extract the docker-archive tar.
	if err := extractTar(input, archiveDir); err != nil {
		return fmt.Errorf("flatten: extract archive: %w", err)
	}

	// Step 2: Parse manifest.json to determine layer order and the
	// path of the OCI image config we'll later embed.
	entry, err := parseDockerManifestEntry(archiveDir)
	if err != nil {
		return err
	}

	// Step 3: Apply layers in order to build the flattened rootfs.
	for _, layerPath := range entry.Layers {
		fullPath := filepath.Join(archiveDir, layerPath)
		if err := applyLayer(fullPath, rootfsDir); err != nil {
			return fmt.Errorf("flatten: apply layer %s: %w", layerPath, err)
		}
	}

	// Step 4: Normalize the rootfs for deterministic output.
	if err := normalizeTimestamps(rootfsDir); err != nil {
		return fmt.Errorf("flatten: normalize timestamps: %w", err)
	}

	// Step 5: Build the EROFS image.
	if err := buildImage(rootfsDir, outputPath); err != nil {
		return err
	}

	// Step 6: Extract the runtime-relevant subset of the OCI image
	// config and append it as a STORED-mode ZIP after the EROFS.
	// EROFS mount/read remains correct because the EROFS image's
	// extent is described in its superblock; the ZIP trailer is
	// addressable independently via standard tools (`unzip -l`,
	// `archive/zip`, `flatten-ctl info`).
	cfg, err := ExtractRuntimeConfig(archiveDir, entry.Config)
	if err != nil {
		return fmt.Errorf("flatten: extract config: %w", err)
	}
	if err := AppendConfigZip(outputPath, cfg); err != nil {
		return fmt.Errorf("flatten: append config zip: %w", err)
	}

	return nil
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

// --- internal helpers ---

// extractTar extracts all entries from a tar stream into destDir.
func extractTar(r io.Reader, destDir string) error {
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

// applyLayer extracts a layer tar onto the rootfs, handling whiteouts.
// The layer tar may be gzip-compressed.
func applyLayer(layerPath, rootfsDir string) error {
	f, err := os.Open(layerPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader = f
	// Detect gzip by reading the magic bytes.
	buf := make([]byte, 2)
	n, _ := f.Read(buf)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if n == 2 && buf[0] == 0x1f && buf[1] == 0x8b {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("gzip open: %w", err)
		}
		defer gz.Close()
		r = gz
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
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			// Remove existing file/symlink before writing.
			os.Remove(target)
			if err := writeFile(target, hdr.FileInfo().Mode(), tr); err != nil {
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
		case tar.TypeLink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			linkTarget := filepath.Join(rootfsDir, hdr.Linkname)
			os.Remove(target)
			if err := os.Link(linkTarget, target); err != nil {
				return err
			}
		}
	}
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
	if exe, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(exe), "mkfs.erofs")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	if p, err := exec.LookPath("mkfs.erofs"); err == nil {
		return p, nil
	}
	return "", fmt.Errorf("flatten: mkfs.erofs not found " +
		"(set MKFS_EROFS_PATH, place it alongside flatten-ctl, " +
		"or add it to PATH; run `make deps-erofs` to build it)")
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
// No compression: raw bytes enable CDC dedup (consistent with PROPOSAL §9.1).
func buildEROFS(mkfsPath, rootfsDir, outputPath string) error {
	cmd := exec.Command(mkfsPath,
		"-Ededupe",       // intra-image file dedup
		"--chunksize=4096", // chunk-based layout: metadata 7.6MiB→0.6MiB
		"--all-root",     // force uid/gid=0
		"-T0",            // fixed timestamp (epoch)
		"-b4096",         // 4K block size
		"-x-1",           // disable xattrs
		"-U", "00000000-0000-0000-0000-000000000000", // fixed UUID
		"--quiet",
		outputPath,
		rootfsDir,
	)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
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
