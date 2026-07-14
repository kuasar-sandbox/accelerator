package redisstore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
)

const (
	maxRESPLine = 512
	maxBulkSize = 1 << 30
)

var (
	getPrefix = []byte("*2\r\n$3\r\nGET\r\n")
	setPrefix = []byte("*3\r\n$3\r\nSET\r\n")
	crlf      = []byte("\r\n")
)

type protocolError struct{ msg string }

func (e *protocolError) Error() string { return "redisstore: protocol: " + e.msg }

type responseError struct{ msg string }

func (e *responseError) Error() string { return "redisstore: server: " + e.msg }

func writeGet(conn net.Conn, key []byte) error {
	var keyLen [32]byte
	line := appendBulkLength(keyLen[:0], len(key))
	return writeAll(conn, net.Buffers{getPrefix, line, key, crlf})
}

func writeSet(conn net.Conn, key, value []byte) error {
	var keyLen, valueLen [32]byte
	keyLine := appendBulkLength(keyLen[:0], len(key))
	valueLine := appendBulkLength(valueLen[:0], len(value))
	return writeAll(conn, net.Buffers{
		setPrefix,
		keyLine,
		key,
		crlf,
		valueLine,
		value,
		crlf,
	})
}

func appendBulkLength(dst []byte, n int) []byte {
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(n), 10)
	return append(dst, '\r', '\n')
}

func writeAll(conn net.Conn, buffers net.Buffers) error {
	var want int64
	for _, b := range buffers {
		want += int64(len(b))
	}
	written, err := buffers.WriteTo(conn)
	if err != nil {
		return err
	}
	if written != want {
		return io.ErrShortWrite
	}
	return nil
}

func readGetResponse(r *bufio.Reader, pool cache.BlobPool) (cache.CacheResult, cache.Blob, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return cache.CacheMiss, nil, err
	}
	line, err := readRESPLine(r)
	if err != nil {
		return cache.CacheMiss, nil, err
	}
	switch prefix {
	case '-':
		return cache.CacheMiss, nil, &responseError{msg: string(line)}
	case '$':
		if len(line) == 2 && line[0] == '-' && line[1] == '1' {
			return cache.CacheMiss, nil, nil
		}
		size, err := parseBulkSize(line)
		if err != nil {
			return cache.CacheMiss, nil, err
		}
		buf, blob := pool.Alloc(size)
		if _, err := io.ReadFull(r, buf); err != nil {
			blob.Release()
			return cache.CacheMiss, nil, err
		}
		var suffix [2]byte
		if _, err := io.ReadFull(r, suffix[:]); err != nil {
			blob.Release()
			return cache.CacheMiss, nil, err
		}
		if suffix != [2]byte{'\r', '\n'} {
			blob.Release()
			return cache.CacheMiss, nil, &protocolError{msg: "bulk response missing CRLF"}
		}
		return cache.CacheHit, blob, nil
	default:
		return cache.CacheMiss, nil, &protocolError{msg: fmt.Sprintf("unexpected GET response prefix %q", prefix)}
	}
}

func readSetResponse(r *bufio.Reader) error {
	prefix, err := r.ReadByte()
	if err != nil {
		return err
	}
	line, err := readRESPLine(r)
	if err != nil {
		return err
	}
	switch prefix {
	case '+':
		if string(line) != "OK" {
			return &protocolError{msg: fmt.Sprintf("unexpected SET status %q", line)}
		}
		return nil
	case '-':
		return &responseError{msg: string(line)}
	default:
		return &protocolError{msg: fmt.Sprintf("unexpected SET response prefix %q", prefix)}
	}
}

func readRESPLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return nil, &protocolError{msg: "response line exceeds limit"}
	}
	if err != nil {
		return nil, err
	}
	if len(line) > maxRESPLine {
		return nil, &protocolError{msg: "response line exceeds limit"}
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, &protocolError{msg: "response line missing CRLF"}
	}
	return line[:len(line)-2], nil
}

func parseBulkSize(line []byte) (int, error) {
	if len(line) == 0 {
		return 0, &protocolError{msg: "empty bulk length"}
	}
	var size int64
	for _, b := range line {
		if b < '0' || b > '9' {
			return 0, &protocolError{msg: fmt.Sprintf("invalid bulk length %q", line)}
		}
		size = size*10 + int64(b-'0')
		if size > maxBulkSize {
			return 0, &protocolError{msg: "bulk response exceeds size limit"}
		}
	}
	return int(size), nil
}

func isProtocolError(err error) bool {
	var target *protocolError
	return errors.As(err, &target)
}

func isResponseError(err error) bool {
	var target *responseError
	return errors.As(err, &target)
}
