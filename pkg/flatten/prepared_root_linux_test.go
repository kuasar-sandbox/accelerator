//go:build linux

package flatten

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	erofs "github.com/kuasar-sandbox/go-erofs"
)

func TestPreparedLayerRootMetadata(t *testing.T) {
	tests := []struct {
		name, root string
		mask       int
		setgid     bool
	}{
		{"slash-default", "/", 0022, false},
		{"dot-default", "./", 0022, false},
		{"no-root-default", "", 0022, false},
		{"slash-private", "/", 0077, false},
		{"dot-private", "./", 0077, false},
		{"no-root-private", "", 0077, false},
		{"dot-group-mask", "./", 0027, false},
		{"setgid-default", "./", 0022, true},
		{"setgid-private", "./", 0077, true},
	}
	const helper = "KUASAR_TEST_PREPARED_ROOT"
	if value := os.Getenv(helper); value != "" {
		index, err := strconv.Atoi(value)
		if err != nil || index < 0 || index >= len(tests) {
			t.Fatalf("invalid helper case %q", value)
		}
		tc := tests[index]
		parent := t.TempDir()
		if tc.setgid {
			gid := os.Getgid()
			if os.Geteuid() == 0 {
				gid = 23456
			}
			if err := os.Chown(parent, os.Geteuid(), gid); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(parent, os.ModeSetgid|0755); err != nil {
				t.Fatal(err)
			}
		}
		// umask is process-wide; only the dedicated helper changes it.
		syscall.Umask(tc.mask)
		var entries []layerEntry
		if tc.root != "" {
			entries = append(entries, layerEntry{name: tc.root, kind: tar.TypeDir, mode: 0700})
		}
		entries = append(entries, layerEntry{name: "payload", body: "payload"})
		b := testLayerTar(t, entries...)
		body := &observedBody{Reader: bytes.NewReader(b)}
		layer := streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body)
		config := []byte(fmt.Sprintf(`{"rootfs":{"type":"layers","diff_ids":[%q]}}`, layer.DiffID))
		input, err := NewImageInput(config, []LayerInput{layer})
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := PrepareImage(context.Background(), input, Options{TmpDir: parent})
		if err != nil {
			t.Fatal(err)
		}
		defer prepared.Close()
		staging, err := os.Stat(prepared.selection.root)
		if err != nil {
			t.Fatal(err)
		}
		if staging.Mode() != os.ModeDir|0755 || staging.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
			t.Fatalf("staging root must be 0755 and retain builder ownership: %v", staging)
		}
		var raw bytes.Buffer
		if err := prepared.WriteRaw(context.Background(), &raw); err != nil {
			t.Fatal(err)
		}
		image, err := erofs.Open(bytes.NewReader(raw.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		root, err := fs.Stat(image, ".")
		if err != nil {
			t.Fatal(err)
		}
		metadata := root.Sys().(*erofs.Stat)
		if root.Mode() != fs.ModeDir|0755 || metadata.UID != 0 || metadata.GID != 0 {
			t.Fatalf("EROFS root = %v %d:%d; want 0755 0:0", root.Mode(), metadata.UID, metadata.GID)
		}
		if data, err := fs.ReadFile(image, "payload"); err != nil || string(data) != "payload" {
			t.Fatalf("payload = %q, %v", data, err)
		}
		var artifact bytes.Buffer
		if _, _, err := prepared.WriteArtifact(context.Background(), &artifact, "image"); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(artifact.Bytes())
		if err := os.WriteFile(os.Getenv(helper+"_RESULT"), digest[:], 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var baseline []byte
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := filepath.Join(t.TempDir(), "digest")
			cmd := exec.Command(executable, "-test.run=^TestPreparedLayerRootMetadata$")
			cmd.Env = append(os.Environ(), helper+"="+strconv.Itoa(index), helper+"_RESULT="+result)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
			digest, err := os.ReadFile(result)
			if err != nil {
				t.Fatal(err)
			}
			if baseline == nil {
				baseline = digest
			} else if !bytes.Equal(baseline, digest) {
				t.Fatalf("artifact digest %x differs from baseline %x", digest, baseline)
			}
		})
	}
}

func TestPreparedDirectoryRootMetadataPreserved(t *testing.T) {
	root := t.TempDir()
	if os.Geteuid() == 0 {
		if err := os.Chown(root, 12345, 23456); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(root, os.ModeSetgid|0751); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareDir(context.Background(), root, Options{TmpDir: t.TempDir()}, DirOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	var raw bytes.Buffer
	if err := prepared.WriteRaw(context.Background(), &raw); err != nil {
		t.Fatal(err)
	}
	image, err := erofs.Open(bytes.NewReader(raw.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	after, err := fs.Stat(image, ".")
	if err != nil {
		t.Fatal(err)
	}
	st, metadata := before.Sys().(*syscall.Stat_t), after.Sys().(*erofs.Stat)
	if after.Mode() != before.Mode() || metadata.UID != st.Uid || metadata.GID != st.Gid {
		t.Fatalf("directory root metadata changed: %v %d:%d -> %v %d:%d", before.Mode(), st.Uid, st.Gid, after.Mode(), metadata.UID, metadata.GID)
	}
}
