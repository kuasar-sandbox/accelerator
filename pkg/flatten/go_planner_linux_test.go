//go:build linux

package flatten

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	erofs "github.com/kuasar-sandbox/go-erofs"
	"golang.org/x/sys/unix"
)

func TestGoPlannerContentAndSelection(t *testing.T) {
	// The production path must not resolve or execute a native planner.
	t.Setenv("MKFS_EROFS_PATH", "/nonexistent/native-planner")
	root, work := t.TempDir(), t.TempDir()
	payload := append(bytes.Repeat([]byte("repeated content"), 4096), []byte("tail")...)
	for _, name := range []string{"data", "duplicate"} {
		if err := os.WriteFile(filepath.Join(root, name), payload, 0640); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Link(filepath.Join(root, "data"), filepath.Join(root, "hardlink")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "data"), filepath.Join(root, "skip-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("data", filepath.Join(root, "symlink")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "symlink"), filepath.Join(root, "symlink-alias")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "empty"), 0751); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(root, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "fifo"), filepath.Join(root, "fifo-alias")); err != nil {
		t.Fatal(err)
	}
	sparse, err := os.Create(filepath.Join(root, "sparse"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = sparse.WriteAt([]byte("end"), 2<<20); err != nil {
		t.Fatal(err)
	}
	if err = sparse.Close(); err != nil {
		t.Fatal(err)
	}
	if err = unix.Setxattr(filepath.Join(root, "data"), "user.not-exported", []byte("value"), 0); err != nil {
		t.Fatal(err)
	}
	if os.Geteuid() == 0 {
		if err = os.Chown(filepath.Join(root, "data"), 12345, 23456); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.Chmod(filepath.Join(root, "data"), fs.ModeSetuid|0640); err != nil {
		t.Fatal(err)
	}
	opts := Options{TmpDir: work}
	dopts := DirOptions{Skip: []string{"skip-link"}}
	p, err := PrepareDir(context.Background(), root, opts, dopts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	var raw bytes.Buffer
	if err = p.WriteRaw(context.Background(), &raw); err != nil {
		t.Fatal(err)
	}
	img, err := erofs.Open(bytes.NewReader(raw.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"data", "duplicate", "hardlink", "symlink", "symlink-alias", "sparse"} {
		want, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		got, err := fs.ReadFile(img, name)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s content: %v", name, err)
		}
	}
	if _, err = fs.Stat(img, "skip-link"); !os.IsNotExist(err) {
		t.Fatalf("excluded link: %v", err)
	}
	lstat := img.(interface {
		Lstat(string) (fs.FileInfo, error)
	})
	for _, pair := range [][2]string{{"data", "hardlink"}, {"symlink", "symlink-alias"}, {"fifo", "fifo-alias"}} {
		a, err := lstat.Lstat(pair[0])
		if err != nil {
			t.Fatal(err)
		}
		b, err := lstat.Lstat(pair[1])
		if err != nil {
			t.Fatal(err)
		}
		x, y := a.Sys().(*erofs.Stat), b.Sys().(*erofs.Stat)
		if x.Ino != y.Ino || x.Nlink != 2 {
			t.Fatalf("hard-link selection %v: %+v %+v", pair, x, y)
		}
	}
	info, err := fs.Stat(img, "data")
	if err != nil {
		t.Fatal(err)
	}
	stat := info.Sys().(*erofs.Stat)
	if info.Mode() != fs.ModeSetuid|0640 || !info.ModTime().Equal(time.Unix(0, 0)) {
		t.Fatalf("metadata: %v %v", info.Mode(), info.ModTime())
	}
	if os.Geteuid() == 0 && (stat.UID != 12345 || stat.GID != 23456) {
		t.Fatalf("ownership: %+v", stat)
	}
	if len(stat.Xattrs) != 0 {
		t.Fatal("xattrs exported")
	}
	duplicate, err := fs.Stat(img, "duplicate")
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Sys().(*erofs.Stat).Ino == stat.Ino {
		t.Fatal("content dedup merged independent inodes")
	}
	if p.plan.dataLength >= uint64(len(payload))*2 {
		t.Fatal("duplicate blocks were not shared")
	}
	again, err := PrepareDir(context.Background(), root, opts, dopts)
	if err != nil {
		t.Fatal(err)
	}
	var second bytes.Buffer
	if err = again.WriteRaw(context.Background(), &second); err != nil {
		t.Fatal(err)
	}
	if err = again.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw.Bytes(), second.Bytes()) {
		t.Fatal("repeated planning is not deterministic")
	}
	if fsck, err := exec.LookPath("fsck.erofs"); err == nil {
		file := filepath.Join(t.TempDir(), "image.erofs")
		if err = os.WriteFile(file, raw.Bytes()[:p.plan.Size()], 0600); err != nil {
			t.Fatal(err)
		}
		extract := filepath.Join(t.TempDir(), "extract")
		if out, err := exec.Command(fsck, "--extract="+extract, file).CombinedOutput(); err != nil {
			t.Fatalf("independent fsck: %v %s", err, out)
		}
		for _, name := range []string{"data", "duplicate", "hardlink", "sparse"} {
			want, err := os.ReadFile(filepath.Join(root, name))
			if err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(filepath.Join(extract, name))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("native extraction %s: %v", name, err)
			}
		}
	} else {
		t.Log("native fsck unavailable; Go semantic checks completed")
	}
}

func TestGoPlannerFailureCleansOwnedScratch(t *testing.T) {
	for _, kind := range []string{"plan-budget", "memory-budget", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			root, work := t.TempDir(), t.TempDir()
			opts := Options{TmpDir: work}
			ctx := context.Background()
			switch kind {
			case "plan-budget":
				opts.PlanLimits.PlanBytes = 192
			case "memory-budget":
				opts.PlanLimits.NativeMemoryBytes = 16 << 20
				file, err := os.Create(filepath.Join(root, "unique"))
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 50000; i++ {
					sum := sha256.Sum256([]byte(fmt.Sprint(i)))
					if _, err = file.Write(bytes.Repeat(sum[:], 128)); err != nil {
						t.Fatal(err)
					}
				}
				if err = file.Close(); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			p, err := PrepareDir(ctx, root, opts, DirOptions{})
			if err == nil {
				p.Close()
				t.Fatal("expected failure")
			}
			if kind == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if kind != "canceled" && !strings.Contains(err.Error(), "budget") {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(work)
			if err != nil || len(entries) != 0 {
				t.Fatalf("owned scratch remains: %v %v", entries, err)
			}
		})
	}
}

func TestPlanningTableGrowthAndCollisions(t *testing.T) {
	memory := planningMemory{limit: 64 << 10}
	table := planningTable[uint64, uint64]{memory: &memory, hash: func(k uint64) uint64 { return k % 4 }}
	var inserted uint64
	for ; inserted < 10000; inserted++ {
		err := table.insert(context.Background(), inserted, inserted+7)
		if memory.used > memory.limit || memory.peak > memory.limit {
			t.Fatal("table exceeded admitted budget")
		}
		if err != nil {
			break
		}
	}
	if inserted == 10000 {
		t.Fatal("table budget was not enforced")
	}
	for k := uint64(0); k < inserted; k++ {
		if v, ok := table.find(k); !ok || v != k+7 {
			t.Fatalf("lost collision entry %d", k)
		}
	}
	if _, ok := table.find(inserted + 1); ok {
		t.Fatal("unexpected table entry")
	}
}

func TestGoPlannerPaddedBlockSharing(t *testing.T) {
	root := t.TempDir()
	short := []byte("a partial block")
	block := make([]byte, 4096)
	copy(block, short)
	inputs := map[string][]byte{"a-short": short, "b-padded": block, "c-two": append(append([]byte(nil), block...), short...)}
	for name, data := range inputs {
		if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	p, err := PrepareDir(context.Background(), root, Options{}, DirOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.plan.count != 1 {
		t.Fatalf("padded content was not shared: %d blocks", p.plan.count)
	}
	rec, err := p.plan.record(0)
	if err != nil {
		t.Fatal(err)
	}
	if rec.length != uint32(len(short)) || rec.digest != sha256.Sum256(short) {
		t.Fatal("record does not verify actual source length")
	}
	raw := readPlanSource(t, p.plan)
	img, err := erofs.Open(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range inputs {
		got, err := fs.ReadFile(img, name)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}
