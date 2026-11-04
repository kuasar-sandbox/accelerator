//go:build linux

package flatten

import (
	"archive/tar"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path"
	"strings"

	"golang.org/x/sys/unix"
)

// Ensure Go's crypto/rand timer and runtime netpoller are initialized at
// package startup rather than lazily when file descriptors are near exhaustion.
var _ = rand.Text()

const (
	whPrefix                  = ".wh."
	whOpaque                  = ".wh..wh..opq"
	maxSymlinkLoops           = 255
	defaultLayerMetadataBytes = 128 << 20
)

type virtualInode struct {
	ino        uint64
	mode       uint32
	uid        uint32
	gid        uint32
	rdev       uint64
	size       uint64
	nlink      uint64
	linkTarget string
	xattrs     map[string]string
	dataOffset int64
	children   map[string]*virtualInode
}

func (v *virtualInode) isDir() bool     { return v.mode&unix.S_IFMT == unix.S_IFDIR }
func (v *virtualInode) isSymlink() bool { return v.mode&unix.S_IFMT == unix.S_IFLNK }
func (v *virtualInode) isReg() bool     { return v.mode&unix.S_IFMT == unix.S_IFREG }

func (v *virtualInode) stat() unix.Stat_t {
	var st unix.Stat_t
	st.Dev = 1
	st.Ino = v.ino
	st.Mode = v.mode
	st.Uid = v.uid
	st.Gid = v.gid
	if v.isReg() {
		st.Size = int64(v.size)
	}
	if v.mode&unix.S_IFMT == unix.S_IFCHR || v.mode&unix.S_IFMT == unix.S_IFBLK {
		st.Rdev = v.rdev
	}
	if v.nlink > 1 {
		st.Nlink = 2
	} else {
		st.Nlink = 1
	}
	return st
}

type virtualTree struct {
	work      string
	root      *virtualInode
	nextIno   uint64
	spool     *os.File
	spoolSize int64
	spoolID   fileIdentity
	workFiles map[string]fileIdentity
	sealed    bool
}

func newVirtualTree(work string) *virtualTree {
	t := &virtualTree{work: work, nextIno: 1}
	t.root = &virtualInode{
		ino:      1,
		mode:     unix.S_IFDIR | 0755,
		uid:      0,
		gid:      0,
		nlink:    2,
		children: make(map[string]*virtualInode),
	}
	return t
}

func (t *virtualTree) allocInode(fileType uint32) *virtualInode {
	t.nextIno++
	v := &virtualInode{
		ino:   t.nextIno,
		mode:  fileType,
		uid:   0,
		gid:   0,
		nlink: 1,
	}
	if fileType == unix.S_IFDIR {
		v.mode |= 0755
		v.nlink = 2
		v.children = make(map[string]*virtualInode)
	}
	return v
}

func (t *virtualTree) Close() error {
	if t == nil || t.spool == nil {
		return nil
	}
	err := t.spool.Close()
	t.spool = nil
	return err
}

func (t *virtualTree) ensureSpool() (*os.File, error) {
	if t.spool != nil {
		return t.spool, nil
	}
	if t.work == "" {
		return nil, fmt.Errorf("flatten: virtual tree has no scratch directory for payloads")
	}
	f, err := os.OpenFile(path.Join(t.work, "payloads"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	t.spool = f
	return f, nil
}

func (t *virtualTree) writePayload(r io.Reader, size int64) (int64, error) {
	if size == 0 {
		return 0, nil
	}
	f, err := t.ensureSpool()
	if err != nil {
		return 0, err
	}
	start := t.spoolSize
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return 0, err
	}
	n, err := io.Copy(f, r)
	if err != nil {
		return 0, err
	}
	if n != size {
		return 0, io.ErrShortWrite
	}
	t.spoolSize = start + size
	return start, nil
}

func (t *virtualTree) unlinkChild(dir *virtualInode, name string) {
	child, ok := dir.children[name]
	if !ok {
		return
	}
	if child.isDir() {
		for k := range child.children {
			t.unlinkChild(child, k)
		}
		delete(dir.children, name)
		return
	}
	delete(dir.children, name)
	if child.nlink > 0 {
		child.nlink--
	}
	if child.nlink == 0 && child.isReg() && child.size > 0 && t.spool != nil {
		end := child.dataOffset + int64(child.size)
		if end == t.spoolSize {
			if err := t.spool.Truncate(child.dataOffset); err == nil {
				t.spoolSize = child.dataOffset
			}
		} else {
			holeStart := (child.dataOffset + 4095) &^ 4095
			holeEnd := end &^ 4095
			if holeEnd > holeStart {
				_ = unix.Fallocate(int(t.spool.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, holeStart, holeEnd-holeStart)
			}
		}
	}
}

func (t *virtualTree) nodeAtParts(parts []string) *virtualInode {
	curr := t.root
	for _, p := range parts {
		if p == "" || p == "." {
			continue
		}
		if curr == nil || !curr.isDir() {
			return nil
		}
		curr = curr.children[p]
	}
	return curr
}

// resolveDirPath resolves a directory path relative to t.root, expanding any
// symlink components along the path (clamped at t.root, matching SecureJoinVFS).
// When createMissing is true, missing intermediate components are created as
// implicit directories and non-directory intermediate components are replaced
// with directories (matching umoci's mkdirAll).
func (t *virtualTree) resolveDirPath(rel string, createMissing bool) (*virtualInode, string, error) {
	if rel == "" || rel == "." {
		return t.root, ".", nil
	}
	var currParts []string
	remaining := strings.Split(rel, "/")
	symlinks := 0
	for len(remaining) > 0 {
		part := remaining[0]
		remaining = remaining[1:]
		if part == "" || part == "." {
			continue
		}
		if part == ".." {
			if len(currParts) > 0 {
				currParts = currParts[:len(currParts)-1]
			}
			continue
		}
		curr := t.nodeAtParts(currParts)
		if curr == nil || !curr.isDir() {
			return nil, "", unix.ENOTDIR
		}
		child, ok := curr.children[part]
		if ok && child.isSymlink() {
			symlinks++
			if symlinks > maxSymlinkLoops {
				return nil, "", fmt.Errorf("flatten: symlink loop resolving %q: %w", rel, unix.ELOOP)
			}
			if strings.HasPrefix(child.linkTarget, "/") {
				currParts = currParts[:0]
			}
			targetParts := strings.Split(child.linkTarget, "/")
			remaining = append(targetParts, remaining...)
			continue
		}
		if !ok {
			if !createMissing {
				return nil, "", os.ErrNotExist
			}
			child = t.allocInode(unix.S_IFDIR)
			curr.children[part] = child
		} else if !child.isDir() {
			if !createMissing {
				return nil, "", unix.ENOTDIR
			}
			t.unlinkChild(curr, part)
			child = t.allocInode(unix.S_IFDIR)
			curr.children[part] = child
		}
		currParts = append(currParts, part)
	}
	curr := t.nodeAtParts(currParts)
	if curr == nil || !curr.isDir() {
		return nil, "", unix.ENOTDIR
	}
	if len(currParts) == 0 {
		return curr, ".", nil
	}
	return curr, strings.Join(currParts, "/"), nil
}

func (t *virtualTree) whiteoutSubtree(dir *virtualInode, dirRel string, upperPaths map[string]struct{}) {
	for name, child := range dir.children {
		childRel := name
		if dirRel != "." {
			childRel = dirRel + "/" + name
		}
		if _, ok := upperPaths[childRel]; !ok {
			t.unlinkChild(dir, name)
		} else if child.isDir() {
			t.whiteoutSubtree(child, childRel, upperPaths)
		}
	}
}

func (t *virtualTree) applyWhiteout(dirRel, woFile string, upperPaths map[string]struct{}) error {
	dir, canonicalDirRel, err := t.resolveDirPath(dirRel, false)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ENOTDIR) {
			return nil
		}
		return err
	}
	if woFile == "" {
		t.whiteoutSubtree(dir, canonicalDirRel, upperPaths)
		return nil
	}
	child, ok := dir.children[woFile]
	if !ok {
		return nil
	}
	targetRel := woFile
	if canonicalDirRel != "." {
		targetRel = canonicalDirRel + "/" + woFile
	}
	if _, touched := upperPaths[targetRel]; !touched {
		t.unlinkChild(dir, woFile)
	} else if child.isDir() {
		t.whiteoutSubtree(child, targetRel, upperPaths)
	}
	return nil
}

func applyHeaderMetadata(v *virtualInode, hdr *tar.Header) error {
	if err := checkVirtualXattrs(hdr); err != nil {
		return err
	}
	if hdr.Uid < 0 || uint64(hdr.Uid) > math.MaxUint32 || hdr.Gid < 0 || uint64(hdr.Gid) > math.MaxUint32 {
		return fmt.Errorf("flatten: layer %q uid/gid out of range (%d:%d)", hdr.Name, hdr.Uid, hdr.Gid)
	}
	v.uid = uint32(hdr.Uid)
	v.gid = uint32(hdr.Gid)
	fileType := v.mode & unix.S_IFMT
	if fileType == unix.S_IFLNK {
		v.mode = unix.S_IFLNK | 0777
	} else {
		fiMode := hdr.FileInfo().Mode()
		mode := uint32(fiMode.Perm())
		if fiMode&os.ModeSetuid != 0 {
			mode |= unix.S_ISUID
		}
		if fiMode&os.ModeSetgid != 0 {
			mode |= unix.S_ISGID
		}
		if fiMode&os.ModeSticky != 0 {
			mode |= unix.S_ISVTX
		}
		v.mode = fileType | mode
	}
	if len(hdr.Xattrs) > 0 {
		v.xattrs = maps.Clone(hdr.Xattrs)
	} else {
		v.xattrs = nil
	}
	return nil
}

func checkVirtualXattrs(hdr *tar.Header) error {
	for k, want := range hdr.Xattrs {
		if len(want) > 64<<10 {
			return fmt.Errorf("flatten: layer %q xattr %q exceeds 64 KiB", hdr.Name, k)
		}
		// Retain the explicit rejection of host-specific SELinux xattrs that
		// umoci silently omitted.
		if k == "security.selinux" {
			return fmt.Errorf("flatten: layer %q xattr %q was not preserved", hdr.Name, k)
		}
	}
	return nil
}

func (t *virtualTree) lookup(rel string) (*virtualInode, error) {
	if rel == "." || rel == "" {
		return t.root, nil
	}
	curr := t.root
	for _, part := range strings.Split(rel, "/") {
		if curr == nil || !curr.isDir() {
			return nil, unix.ENOTDIR
		}
		next, ok := curr.children[part]
		if !ok {
			return nil, os.ErrNotExist
		}
		curr = next
	}
	return curr, nil
}

func (t *virtualTree) readlink(rel string) (string, error) {
	v, err := t.lookup(rel)
	if err != nil {
		return "", err
	}
	if !v.isSymlink() {
		return "", unix.EINVAL
	}
	return v.linkTarget, nil
}

func (t *virtualTree) openFile(rel string) (*virtualInode, fileIdentity, error) {
	v, err := t.lookup(rel)
	if err != nil {
		return nil, fileIdentity{}, err
	}
	if !v.isReg() {
		return nil, fileIdentity{}, fmt.Errorf("flatten: non-regular virtual source %s", rel)
	}
	id, err := t.statFile(v)
	if err != nil {
		return nil, fileIdentity{}, err
	}
	return v, id, nil
}

func (t *virtualTree) statFile(v *virtualInode) (fileIdentity, error) {
	if v == nil || !v.isReg() {
		return fileIdentity{}, fmt.Errorf("flatten: invalid virtual file")
	}
	if t.sealed && t.spool != nil {
		id, err := fileID(t.spool)
		if err != nil {
			return fileIdentity{}, err
		}
		if id != t.spoolID {
			return fileIdentity{}, fmt.Errorf("flatten: virtual payload spool changed")
		}
	}
	st := v.stat()
	return statID(&st), nil
}

func (t *virtualTree) readAt(ctx context.Context, v *virtualInode, b []byte, off int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if off < 0 || uint64(off) > v.size {
		return 0, io.EOF
	}
	if len(b) == 0 {
		return 0, nil
	}
	if t.spool == nil {
		return 0, io.ErrUnexpectedEOF
	}
	if t.sealed {
		id, err := fileID(t.spool)
		if err != nil {
			return 0, err
		}
		if id != t.spoolID {
			return 0, fmt.Errorf("flatten: virtual payload spool changed")
		}
	}
	rem := v.size - uint64(off)
	want := len(b)
	if uint64(want) > rem {
		want = int(rem)
	}
	n, err := t.spool.ReadAt(b[:want], v.dataOffset+off)
	if err == nil && want < len(b) {
		err = io.EOF
	}
	return n, err
}

func (t *virtualTree) sealWork(work string) error {
	if t.spool != nil {
		id, err := fileID(t.spool)
		if err != nil {
			return err
		}
		t.spoolID = id
	}
	if work != "" {
		entries, err := os.ReadDir(work)
		if err != nil {
			return err
		}
		t.workFiles = make(map[string]fileIdentity, len(entries))
		for _, entry := range entries {
			var st unix.Stat_t
			if err := unix.Lstat(path.Join(work, entry.Name()), &st); err != nil {
				return err
			}
			t.workFiles[entry.Name()] = statID(&st)
		}
	}
	t.sealed = true
	return nil
}

func (t *virtualTree) validateWork(work string) error {
	if t.spool != nil && t.sealed {
		id, err := fileID(t.spool)
		if err != nil {
			return err
		}
		if id != t.spoolID {
			return fmt.Errorf("flatten: virtual payload spool changed")
		}
	}
	if t.sealed && work != "" {
		entries, err := os.ReadDir(work)
		if err != nil {
			return err
		}
		if len(entries) != len(t.workFiles) {
			return fmt.Errorf("flatten: selected source tree changed")
		}
		for _, entry := range entries {
			want, ok := t.workFiles[entry.Name()]
			if !ok {
				return fmt.Errorf("flatten: selected source tree changed")
			}
			var st unix.Stat_t
			if err := unix.Lstat(path.Join(work, entry.Name()), &st); err != nil {
				return err
			}
			if statID(&st) != want {
				return fmt.Errorf("flatten: selected source tree changed")
			}
		}
	}
	return nil
}

// applyVerifiedLayer owns the reader until the synchronous applier, decoder and
// full-stream digest/DiffID checks return. Cancellation closes the underlying
// body through layerStream and waits for that close before returning.
func applyVerifiedLayer(ctx context.Context, in LayerInput, tree *virtualTree, limits LayerLimits) (err error) {
	limits, err = limits.normalized()
	if err != nil {
		return err
	}
	s, err := openLayer(ctx, in, limits)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	if err = unpackLayer(ctx, s, tree, limits.MetadataBytes); err != nil {
		return err
	}
	return s.finish()
}

// unpackLayer applies an OCI layer tar stream directly into the in-memory
// virtual overlay tree, spooling regular file payloads into tree.spool without
// extracting any files or metadata onto the host filesystem.
func unpackLayer(ctx context.Context, r io.Reader, tree *virtualTree, budget int64) error {
	limited := budget != 0
	if limited {
		if budget < 8<<20 {
			return errors.New("flatten: layer metadata budget must be at least 8 MiB")
		}
		budget -= 8 << 20
	}
	tr := tar.NewReader(r)
	upperPaths := make(map[string]struct{})
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
		clean, err := layerPath(hdr.Name)
		if err != nil {
			return err
		}
		if hdr.Typeflag == tar.TypeLink {
			if _, err := layerPath(hdr.Linkname); err != nil {
				return err
			}
		}
		if limited {
			charge := int64(2048 + len(hdr.Name) + len(hdr.Linkname))
			for p := clean; p != "."; p = path.Dir(p) {
				charge += int64(256 + 2*len(p))
			}
			for k, v := range hdr.PAXRecords {
				charge += int64(256 + 2*len(k) + 2*len(v))
			}
			if charge > budget {
				return errors.New("flatten: layer metadata budget exceeded")
			}
			budget -= charge
		}
		if err := tree.applyEntry(hdr, clean, tr, upperPaths); err != nil {
			return fmt.Errorf("flatten: unpack %q: %w", hdr.Name, err)
		}
	}
}

func (t *virtualTree) applyEntry(hdr *tar.Header, clean string, r io.Reader, upperPaths map[string]struct{}) error {
	if clean == "." {
		if hdr.Typeflag != tar.TypeDir {
			return fmt.Errorf("refusing to change type of root directory")
		}
		if err := applyHeaderMetadata(t.root, hdr); err != nil {
			return err
		}
		upperPaths["."] = struct{}{}
		return nil
	}
	dirRel, file := path.Split(clean)
	dirRel = path.Clean(dirRel)
	if dirRel == "" {
		dirRel = "."
	}
	if woFile, isWhiteout := strings.CutPrefix(file, whPrefix); isWhiteout {
		if file == whOpaque {
			woFile = ""
		}
		return t.applyWhiteout(dirRel, woFile, upperPaths)
	}
	dir, canonicalDirRel, err := t.resolveDirPath(dirRel, true)
	if err != nil {
		return err
	}
	existing := dir.children[file]
	if existing != nil && (!existing.isDir() || hdr.Typeflag != tar.TypeDir) {
		t.unlinkChild(dir, file)
		existing = nil
	}
	switch hdr.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		if hdr.Size < 0 {
			return fmt.Errorf("negative size %d", hdr.Size)
		}
		v := t.allocInode(unix.S_IFREG)
		v.size = uint64(hdr.Size)
		if hdr.Size > 0 {
			off, err := t.writePayload(r, hdr.Size)
			if err != nil {
				return err
			}
			v.dataOffset = off
		}
		if err := applyHeaderMetadata(v, hdr); err != nil {
			return err
		}
		dir.children[file] = v
	case tar.TypeDir:
		v := existing
		if v == nil {
			v = t.allocInode(unix.S_IFDIR)
			dir.children[file] = v
		}
		if err := applyHeaderMetadata(v, hdr); err != nil {
			return err
		}
	case tar.TypeSymlink:
		if hdr.Linkname == "" {
			return fmt.Errorf("empty symlink target")
		}
		v := t.allocInode(unix.S_IFLNK)
		v.linkTarget = hdr.Linkname
		if err := applyHeaderMetadata(v, hdr); err != nil {
			return err
		}
		dir.children[file] = v
	case tar.TypeLink:
		if err := checkVirtualXattrs(hdr); err != nil {
			return err
		}
		linkClean, err := layerPath(strings.TrimLeft(hdr.Linkname, "/"))
		if err != nil {
			return err
		}
		if linkClean == "." {
			return fmt.Errorf("hardlink to root directory is not allowed")
		}
		linkDirRel, linkFile := path.Split(linkClean)
		linkDirRel = path.Clean(linkDirRel)
		if linkDirRel == "" {
			linkDirRel = "."
		}
		linkDir, _, err := t.resolveDirPath(linkDirRel, false)
		if err != nil {
			return fmt.Errorf("resolve hardlink target %q: %w", hdr.Linkname, err)
		}
		target, ok := linkDir.children[linkFile]
		if !ok {
			return fmt.Errorf("hardlink target %q: %w", hdr.Linkname, os.ErrNotExist)
		}
		if target.isDir() {
			return fmt.Errorf("hardlink target %q is a directory", hdr.Linkname)
		}
		target.nlink++
		dir.children[file] = target
	case tar.TypeChar:
		v := t.allocInode(unix.S_IFCHR)
		v.rdev = unix.Mkdev(uint32(hdr.Devmajor), uint32(hdr.Devminor))
		if err := applyHeaderMetadata(v, hdr); err != nil {
			return err
		}
		dir.children[file] = v
	case tar.TypeBlock:
		v := t.allocInode(unix.S_IFBLK)
		v.rdev = unix.Mkdev(uint32(hdr.Devmajor), uint32(hdr.Devminor))
		if err := applyHeaderMetadata(v, hdr); err != nil {
			return err
		}
		dir.children[file] = v
	case tar.TypeFifo:
		v := t.allocInode(unix.S_IFIFO)
		if err := applyHeaderMetadata(v, hdr); err != nil {
			return err
		}
		dir.children[file] = v
	default:
		return fmt.Errorf("unsupported typeflag %x", hdr.Typeflag)
	}
	targetRel := file
	if canonicalDirRel != "." {
		targetRel = canonicalDirRel + "/" + file
	}
	for pth := targetRel; ; pth = path.Dir(pth) {
		upperPaths[pth] = struct{}{}
		if pth == "." || pth == "/" {
			break
		}
	}
	return nil
}

func layerPath(name string) (string, error) {
	if name == "" || len(name) > 4096 || strings.HasPrefix(name, "/") || strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("flatten: invalid layer path %q", name)
	}
	for _, part := range strings.Split(name, "/") {
		if part == ".." {
			return "", fmt.Errorf("flatten: parent traversal in layer path %q", name)
		}
	}
	return path.Clean(name), nil
}
