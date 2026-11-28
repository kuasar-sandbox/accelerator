package delta

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// The snapshot holds chunk decryption keys (each base's K_B, needed to read a
// base back and delta against it), so it is sealed: everything after the
// cleartext header is AES-256-GCM under a key that only a holder of the
// customer key can derive. Without the customer key the file is noise; with
// the store alone, nothing in it decrypts a chunk.
//
// Key derivation. The customer key never touches the file directly:
//
//	K_snap = HMAC-SHA256(customerKey, "accelerator-delta-snapshot/v1")
//	K_file = HMAC-SHA256(K_snap, "accelerator-delta-snapshot-file/v1" || fileID)
//
// fileID is 16 random bytes written in the header, so every snapshot file has
// its own key and the GCM nonces below can be a plain counter.
//
// Framing. GCM wants the whole message in memory and a snapshot can be large
// (ten million representations is a gigabyte), so the body is a sequence of
// frames of at most sealFrameSize plaintext bytes, each sealed on its own:
//
//	flag u8 (0 = more frames follow, 1 = last)  len u32  ciphertext[len]
//
// nonce = 0u32 || frame counter u64; AAD = header || flag. The counter in the
// nonce pins each frame to its position, the flag in the AAD makes the end of
// the file authenticated, and a reader that reaches EOF before a last frame
// knows the file was truncated. No separate checksum is needed.
const (
	sealFrameSize = 1 << 20
	sealFileIDLen = 16
	sealNonceLen  = 12
	sealTagLen    = 16
	sealFlagMore  = 0
	sealFlagLast  = 1

	snapshotKeyLabel     = "accelerator-delta-snapshot/v1"
	snapshotFileKeyLabel = "accelerator-delta-snapshot-file/v1"
)

// deriveSnapshotKey turns the customer key into the snapshot sealing key. The
// HMAC label keeps this use of the customer key apart from every other.
func deriveSnapshotKey(customerKey [32]byte) [32]byte {
	mac := hmac.New(sha256.New, customerKey[:])
	mac.Write([]byte(snapshotKeyLabel))
	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
}

func deriveFileAEAD(snapKey [32]byte, fileID []byte) (cipher.AEAD, error) {
	mac := hmac.New(sha256.New, snapKey[:])
	mac.Write([]byte(snapshotFileKeyLabel))
	mac.Write(fileID)
	fileKey := mac.Sum(nil)
	defer clear(fileKey)
	block, err := aes.NewCipher(fileKey)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// sealWriter frames and seals everything written to it. Close writes the last
// frame; nothing is valid until it has.
type sealWriter struct {
	w      io.Writer
	aead   cipher.AEAD
	header []byte
	buf    []byte
	n      uint64
	out    []byte
}

func newSealWriter(w io.Writer, snapKey [32]byte, header []byte) (*sealWriter, error) {
	aead, err := deriveFileAEAD(snapKey, header[len(header)-sealFileIDLen:])
	if err != nil {
		return nil, err
	}
	return &sealWriter{
		w:      w,
		aead:   aead,
		header: append([]byte(nil), header...),
		buf:    make([]byte, 0, sealFrameSize),
		out:    make([]byte, 0, sealFrameSize+sealTagLen),
	}, nil
}

func (s *sealWriter) Write(p []byte) (int, error) {
	total := len(p)
	for len(p) > 0 {
		room := sealFrameSize - len(s.buf)
		if room > len(p) {
			room = len(p)
		}
		s.buf = append(s.buf, p[:room]...)
		p = p[room:]
		if len(s.buf) == sealFrameSize {
			if err := s.flush(sealFlagMore); err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

// Close seals the final frame, which may be empty.
func (s *sealWriter) Close() error {
	err := s.flush(sealFlagLast)
	clear(s.buf[:cap(s.buf)])
	return err
}

func (s *sealWriter) flush(flag byte) error {
	var nonce [sealNonceLen]byte
	binary.LittleEndian.PutUint64(nonce[4:], s.n)
	aad := append(append([]byte(nil), s.header...), flag)
	s.out = s.aead.Seal(s.out[:0], nonce[:], s.buf, aad)
	var prefix [5]byte
	prefix[0] = flag
	binary.LittleEndian.PutUint32(prefix[1:], uint32(len(s.out)))
	if _, err := s.w.Write(prefix[:]); err != nil {
		return err
	}
	if _, err := s.w.Write(s.out); err != nil {
		return err
	}
	s.n++
	clear(s.buf)
	s.buf = s.buf[:0]
	return nil
}

// errSealTruncated and errSealAuth are the two ways a sealed body can fail;
// both are reported to the caller as corruption.
var (
	errSealTruncated = errors.New("sealed body ends before its last frame")
	errSealAuth      = errors.New("sealed frame failed to authenticate (corrupt, or sealed under a different customer key)")
)

// sealReader opens frames as they are read. After the last frame it reports
// io.EOF; a short read before that is errSealTruncated.
type sealReader struct {
	r      io.Reader
	aead   cipher.AEAD
	header []byte
	plain  []byte
	n      uint64
	done   bool
	err    error
}

func newSealReader(r io.Reader, snapKey [32]byte, header []byte) (*sealReader, error) {
	aead, err := deriveFileAEAD(snapKey, header[len(header)-sealFileIDLen:])
	if err != nil {
		return nil, err
	}
	return &sealReader{r: r, aead: aead, header: append([]byte(nil), header...)}, nil
}

func (s *sealReader) Read(p []byte) (int, error) {
	for len(s.plain) == 0 {
		if s.err != nil {
			return 0, s.err
		}
		if s.done {
			s.err = io.EOF
			return 0, io.EOF
		}
		if err := s.next(); err != nil {
			s.err = err
			return 0, err
		}
	}
	n := copy(p, s.plain)
	clear(s.plain[:n])
	s.plain = s.plain[n:]
	return n, nil
}

func (s *sealReader) next() error {
	var prefix [5]byte
	if _, err := io.ReadFull(s.r, prefix[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return errSealTruncated
		}
		return err
	}
	flag := prefix[0]
	if flag != sealFlagMore && flag != sealFlagLast {
		return fmt.Errorf("sealed frame %d has flag %#x", s.n, flag)
	}
	n := binary.LittleEndian.Uint32(prefix[1:])
	if n < sealTagLen || n > sealFrameSize+sealTagLen {
		return fmt.Errorf("sealed frame %d has implausible length %d", s.n, n)
	}
	ct := make([]byte, n)
	if _, err := io.ReadFull(s.r, ct); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return errSealTruncated
		}
		return err
	}
	var nonce [sealNonceLen]byte
	binary.LittleEndian.PutUint64(nonce[4:], s.n)
	aad := append(append([]byte(nil), s.header...), flag)
	plain, err := s.aead.Open(ct[:0], nonce[:], ct, aad)
	if err != nil {
		return errSealAuth
	}
	s.plain = plain
	s.n++
	s.done = flag == sealFlagLast
	return nil
}

// atEnd reports whether the sealed body has been fully consumed, i.e. the
// last frame was opened and nothing of it remains unread.
func (s *sealReader) atEnd() bool { return s.done && len(s.plain) == 0 }

func newFileID() ([]byte, error) {
	id := make([]byte, sealFileIDLen)
	if _, err := rand.Read(id); err != nil {
		return nil, err
	}
	return id, nil
}
