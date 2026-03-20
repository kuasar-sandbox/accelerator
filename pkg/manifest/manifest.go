// Package manifest implements the binary manifest format for mapping
// virtual disk images to their encrypted chunks.
package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/fullof-work/container-accelerator-research/pkg/chunker"
	"github.com/fullof-work/container-accelerator-research/pkg/crypto"
)

const (
	// Magic bytes "MANI" identifying the manifest format.
	Magic uint32 = 0x4D414E49
	// Version is the current manifest format version.
	Version uint32 = 2
	// HeaderSize is the fixed header size in bytes.
	HeaderSize = 64
	// EntrySize is the size of each chunk entry in bytes.
	EntrySize = 56

	// ChunkMode values.
	ChunkModeFastCDC uint32 = 1

	flagIsZero uint32 = 1 << 0
)

var (
	ErrBadMagic   = errors.New("manifest: invalid magic bytes")
	ErrBadVersion = errors.New("manifest: unsupported version")
	ErrTruncated  = errors.New("manifest: data truncated")
)

// ChunkEntry represents one chunk in the manifest.
type ChunkEntry struct {
	Offset         uint64
	Size           uint32
	IsZero         bool
	CiphertextHash [32]byte
}

// Manifest maps a virtual disk image to its encrypted chunks.
type Manifest struct {
	ImageSize    uint64
	ChunkMode    uint32 // 1 = FastCDC
	MinChunkSize uint32
	MaxChunkSize uint32
	Entries      []ChunkEntry
	Keys         [][32]byte // decrypted keys; available after Unseal()

	// Raw encrypted key table, retained for Unseal().
	encryptedKeyTable []byte
	// AAD used for key table authentication.
	aad []byte
}

// Marshal serializes the manifest to binary format and encrypts the key table
// using the customer key.
func (m *Manifest) Marshal(customerKey [32]byte) ([]byte, error) {
	chunkCount := uint32(len(m.Entries))
	keyTablePlaintextSize := int(chunkCount) * crypto.KeySize
	// Encrypted key table: nonce + ciphertext + tag.
	keyTableEncSize := crypto.GCMNonceSize + keyTablePlaintextSize + crypto.GCMTagSize

	totalSize := HeaderSize + int(chunkCount)*EntrySize + keyTableEncSize
	buf := make([]byte, totalSize)

	// Set defaults if not specified.
	chunkMode := m.ChunkMode
	if chunkMode == 0 {
		chunkMode = ChunkModeFastCDC
	}
	minChunkSize := m.MinChunkSize
	if minChunkSize == 0 {
		minChunkSize = chunker.MinChunkSize
	}
	maxChunkSize := m.MaxChunkSize
	if maxChunkSize == 0 {
		maxChunkSize = chunker.MaxChunkSize
	}

	// Header.
	binary.LittleEndian.PutUint32(buf[0:4], Magic)
	binary.LittleEndian.PutUint32(buf[4:8], Version)
	binary.LittleEndian.PutUint64(buf[8:16], m.ImageSize)
	binary.LittleEndian.PutUint32(buf[16:20], chunkCount)
	keyTableOffset := uint32(HeaderSize + int(chunkCount)*EntrySize)
	binary.LittleEndian.PutUint32(buf[20:24], keyTableOffset)
	binary.LittleEndian.PutUint32(buf[24:28], uint32(keyTableEncSize))
	binary.LittleEndian.PutUint32(buf[28:32], chunkMode)
	binary.LittleEndian.PutUint32(buf[32:36], minChunkSize)
	binary.LittleEndian.PutUint32(buf[36:40], maxChunkSize)
	// bytes 40-63 reserved (zero-filled)

	// Chunk entries.
	for i, e := range m.Entries {
		off := HeaderSize + i*EntrySize
		binary.LittleEndian.PutUint64(buf[off:off+8], e.Offset)
		binary.LittleEndian.PutUint32(buf[off+8:off+12], e.Size)
		var flags uint32
		if e.IsZero {
			flags |= flagIsZero
		}
		binary.LittleEndian.PutUint32(buf[off+12:off+16], flags)
		// off+16..off+24 reserved
		copy(buf[off+24:off+56], e.CiphertextHash[:])
	}

	// AAD = header + chunk entries (everything before the key table).
	aad := buf[:keyTableOffset]

	// Encrypt key table.
	encrypted, err := crypto.EncryptKeyTable(customerKey, m.Keys, aad)
	if err != nil {
		return nil, fmt.Errorf("manifest: encrypt key table: %w", err)
	}
	copy(buf[keyTableOffset:], encrypted)

	return buf, nil
}

// Unmarshal deserializes a manifest from binary format.
// Keys remain encrypted until Unseal() is called.
func Unmarshal(data []byte) (*Manifest, error) {
	if len(data) < HeaderSize {
		return nil, ErrTruncated
	}

	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != Magic {
		return nil, ErrBadMagic
	}
	version := binary.LittleEndian.Uint32(data[4:8])
	if version != Version {
		return nil, ErrBadVersion
	}

	m := &Manifest{
		ImageSize:    binary.LittleEndian.Uint64(data[8:16]),
		ChunkMode:    binary.LittleEndian.Uint32(data[28:32]),
		MinChunkSize: binary.LittleEndian.Uint32(data[32:36]),
		MaxChunkSize: binary.LittleEndian.Uint32(data[36:40]),
	}
	chunkCount := binary.LittleEndian.Uint32(data[16:20])
	keyTableOffset := binary.LittleEndian.Uint32(data[20:24])
	keyTableSize := binary.LittleEndian.Uint32(data[24:28])

	entriesEnd := HeaderSize + int(chunkCount)*EntrySize
	if len(data) < entriesEnd {
		return nil, ErrTruncated
	}
	if len(data) < int(keyTableOffset)+int(keyTableSize) {
		return nil, ErrTruncated
	}

	m.Entries = make([]ChunkEntry, chunkCount)
	for i := range m.Entries {
		off := HeaderSize + i*EntrySize
		m.Entries[i].Offset = binary.LittleEndian.Uint64(data[off : off+8])
		m.Entries[i].Size = binary.LittleEndian.Uint32(data[off+8 : off+12])
		flags := binary.LittleEndian.Uint32(data[off+12 : off+16])
		m.Entries[i].IsZero = flags&flagIsZero != 0
		copy(m.Entries[i].CiphertextHash[:], data[off+24:off+56])
	}

	// Retain encrypted key table and AAD for later Unseal().
	m.encryptedKeyTable = make([]byte, keyTableSize)
	copy(m.encryptedKeyTable, data[keyTableOffset:keyTableOffset+keyTableSize])
	m.aad = make([]byte, keyTableOffset)
	copy(m.aad, data[:keyTableOffset])

	return m, nil
}

// Unseal decrypts the key table using the customer key.
func (m *Manifest) Unseal(customerKey [32]byte) error {
	if m.encryptedKeyTable == nil {
		return errors.New("manifest: no encrypted key table")
	}
	keys, err := crypto.DecryptKeyTable(customerKey, m.encryptedKeyTable, m.aad, len(m.Entries))
	if err != nil {
		return fmt.Errorf("manifest: unseal: %w", err)
	}
	m.Keys = keys
	return nil
}

// ChunkIndexForOffset returns the chunk index containing the given byte offset.
// Uses binary search for O(log n) lookup with variable-size chunks.
func (m *Manifest) ChunkIndexForOffset(offset uint64) uint32 {
	if len(m.Entries) == 0 {
		return 0
	}

	lo, hi := 0, len(m.Entries)-1
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if m.Entries[mid].Offset <= offset {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return uint32(lo)
}

// IsSealed returns true if the key table has not been decrypted yet.
func (m *Manifest) IsSealed() bool {
	return m.Keys == nil
}
