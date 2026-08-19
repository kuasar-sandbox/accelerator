//go:build linux

package fs

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

func openExclusiveNoFollow(path string, mode os.FileMode) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, uint32(mode.Perm()))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func openReadNoFollow(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func readDirectFile(path string) ([]byte, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, directIOError("open", err)
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("direct I/O stat: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("direct I/O target is not a regular file (mode %s)", info.Mode())
	}
	if info.Size() < 0 {
		return nil, fmt.Errorf("direct I/O negative file size %d", info.Size())
	}
	if info.Size() == 0 {
		return []byte{}, nil
	}

	memAlign, offsetAlign, err := directIOAlignment(fd)
	if err != nil {
		return nil, err
	}
	readLen, err := roundUpDirectLength(info.Size(), int64(offsetAlign))
	if err != nil {
		return nil, err
	}
	if readLen > int64(maxInt())-int64(memAlign-1) {
		return nil, fmt.Errorf("direct I/O object length %d exceeds addressable memory", readLen)
	}

	raw, aligned := alignedBuffer(int(readLen), memAlign)
	total := 0
	for int64(total) < info.Size() {
		n, readErr := unix.Pread(fd, aligned[total:], int64(total))
		if n > 0 {
			total += n
		}
		if readErr != nil && !errors.Is(readErr, unix.EINTR) {
			return nil, directIOError("read", readErr)
		}
		if n == 0 {
			if errors.Is(readErr, unix.EINTR) {
				continue
			}
			return nil, fmt.Errorf("direct I/O short read: read %d of %d: %w", total, info.Size(), io.ErrUnexpectedEOF)
		}
		if int64(total) < info.Size() && (total%memAlign != 0 || total%offsetAlign != 0) {
			return nil, fmt.Errorf("direct I/O unaligned short read at offset %d (memory alignment %d, offset alignment %d)", total, memAlign, offsetAlign)
		}
	}
	runtime.KeepAlive(raw)
	return aligned[:int(info.Size()):int(info.Size())], nil
}

func directIOAlignment(fd int) (int, int, error) {
	var stat unix.Statx_t
	err := unix.Statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_STATX_DONT_SYNC, unix.STATX_DIOALIGN, &stat)
	if err != nil {
		return 0, 0, directIOError("statx(STATX_DIOALIGN)", err)
	}
	if stat.Mask&unix.STATX_DIOALIGN == 0 || stat.Dio_mem_align == 0 || stat.Dio_offset_align == 0 {
		return 0, 0, fmt.Errorf("%w: filesystem did not report STATX_DIOALIGN", ErrDirectIOUnsupported)
	}
	memAlign := int(stat.Dio_mem_align)
	offsetAlign := int(stat.Dio_offset_align)
	if stat.Dio_read_offset_align != 0 {
		offsetAlign = int(stat.Dio_read_offset_align)
	}
	if memAlign <= 0 || offsetAlign <= 0 {
		return 0, 0, fmt.Errorf("%w: invalid alignment memory=%d offset=%d", ErrDirectIOUnsupported, memAlign, offsetAlign)
	}
	return memAlign, offsetAlign, nil
}

func roundUpDirectLength(size, alignment int64) (int64, error) {
	if size < 0 || alignment <= 0 {
		return 0, fmt.Errorf("direct I/O invalid size/alignment %d/%d", size, alignment)
	}
	if size == 0 {
		return 0, nil
	}
	remainder := size % alignment
	if remainder == 0 {
		return size, nil
	}
	add := alignment - remainder
	if size > math.MaxInt64-add {
		return 0, errors.New("direct I/O aligned length overflow")
	}
	return size + add, nil
}

func alignedBuffer(length, alignment int) ([]byte, []byte) {
	raw := make([]byte, length+alignment-1)
	address := uintptr(unsafe.Pointer(&raw[0]))
	offset := int((uintptr(alignment) - address%uintptr(alignment)) % uintptr(alignment))
	return raw, raw[offset : offset+length]
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

func directIOError(operation string, err error) error {
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) {
		return fmt.Errorf("%w: %s: %w", ErrDirectIOUnsupported, operation, err)
	}
	return fmt.Errorf("direct I/O %s: %w", operation, err)
}
