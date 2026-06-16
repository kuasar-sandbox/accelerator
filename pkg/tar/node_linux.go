//go:build linux

package tar

import (
	stdtar "archive/tar"
	"errors"
	"syscall"
	"time"
	"unsafe"
)

// errUnsupportedNode marks node types the platform cannot create.
// Never returned on Linux; a distinct sentinel so real mknod errnos
// (EINVAL would satisfy errors.Is(_, os.ErrInvalid)) are not mistaken
// for it.
var errUnsupportedNode = errors.New("device/FIFO nodes are not supported on this platform")

// mknodEntry creates a character/block device or FIFO described by hdr.
func mknodEntry(dest string, hdr *stdtar.Header) error {
	mode := uint32(hdr.Mode & 0o7777)
	switch hdr.Typeflag {
	case stdtar.TypeChar:
		mode |= syscall.S_IFCHR
	case stdtar.TypeBlock:
		mode |= syscall.S_IFBLK
	case stdtar.TypeFifo:
		mode |= syscall.S_IFIFO
	}
	return syscall.Mknod(dest, mode, int(mkdev(uint32(hdr.Devmajor), uint32(hdr.Devminor))))
}

// mkdev encodes major/minor into a Linux dev_t (glibc layout).
func mkdev(major, minor uint32) uint64 {
	return (uint64(major&0xfffff000) << 32) |
		(uint64(major&0xfff) << 8) |
		(uint64(minor&0xffffff00) << 12) |
		uint64(minor&0xff)
}

// lchtimes sets the mtime of a symlink itself (utimensat with
// AT_SYMLINK_NOFOLLOW; atime untouched via UTIME_OMIT).
func lchtimes(dest string, mtime time.Time) error {
	p, err := syscall.BytePtrFromString(dest)
	if err != nil {
		return err
	}
	ts := [2]syscall.Timespec{
		{Sec: 0, Nsec: utimeOmit},
		syscall.NsecToTimespec(mtime.UnixNano()),
	}
	fdcwd := atFDCWD
	_, _, errno := syscall.Syscall6(syscall.SYS_UTIMENSAT,
		uintptr(fdcwd), uintptr(unsafe.Pointer(p)),
		uintptr(unsafe.Pointer(&ts)), uintptr(atSymlinkNofollow), 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

// Linux ABI constants not exported by the syscall package.
const (
	atFDCWD           int = -0x64         // AT_FDCWD
	atSymlinkNofollow     = 0x100         // AT_SYMLINK_NOFOLLOW
	utimeOmit             = (1 << 30) - 2 // UTIME_OMIT
)
