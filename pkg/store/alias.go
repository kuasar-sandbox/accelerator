package store

import (
	"errors"
	"fmt"
)

// PartitionAlias holds the write-once alias objects used for cross-node delta
// compression. "Cross-node" means between nodes: separate writers (processes or
// hosts) that share one store but not their in-memory delta state, such as their
// base index and base pool. They cannot see each other's bases, so the store is
// the only place they can agree on which delta object represents a chunk. The
// same chunk on two nodes may have different delta objects as the bases differ.
// PartitionAlias helps with maintaining consistency between nodes.
//
// It is internal to the store: it is not a client-addressable
// partition (Get, Put and Exists never accept it) and exists as a Partition
// value only so the backends lay it out, count it, and drop it with its
// generation exactly like the data partitions.
//
// An alias is named by H_ind, the content key a chunk would have as an
// independent object, and its body names the delta object that currently
// represents that chunk:
//
//	{root}/alias/{generation}/{hex[:2]}/{hex[2:4]}/{H_ind}  ->  [0x01][32-byte delta key]
//
// It is created at most once. Whoever creates it decides the representation; a
// later writer adopts it instead of publishing a different one.
const PartitionAlias Partition = "alias"

// ErrUnsupported reports that the store cannot perform an optional operation.
// A caller that gets it should behave as it would against a store that never
// had the capability, not retry.
var ErrUnsupported = errors.New("store: operation not supported")

// Resolution is how a chunk is stored in one generation, as seen by H_ind.
type Resolution uint8

const (
	// ResolutionAbsent: neither an independent object nor an alias exists.
	ResolutionAbsent Resolution = iota
	// ResolutionIndependent: the independent object exists at H_ind.
	ResolutionIndependent
	// ResolutionDelta: an alias exists and names a delta object. The store has
	// not checked that the delta object exists or is correct.
	ResolutionDelta
)

func (r Resolution) String() string {
	switch r {
	case ResolutionAbsent:
		return "absent"
	case ResolutionIndependent:
		return "independent"
	case ResolutionDelta:
		return "delta"
	}
	return fmt.Sprintf("resolution(%d)", uint8(r))
}

// Resolved is the answer to a Resolve.
type Resolved struct {
	Resolution Resolution
	// DeltaKey is the delta object's key; set only for ResolutionDelta.
	DeltaKey ContentKey
}

// Represented is the outcome of asking the store to record a delta object as a
// chunk's representation in the same call that stores the object.
type Represented uint8

const (
	// RepresentedNone: nothing was recorded (no request, no alias support, or the
	// alias step failed). The object itself was stored normally.
	RepresentedNone Represented = iota
	// RepresentedWon: this object is now the chunk's representation.
	RepresentedWon
	// RepresentedLost: another delta object already represents the chunk; its key
	// is in PutResult.Key. If this Put stored the object, the store may have
	// deleted it again (PutResult.Discarded).
	RepresentedLost
	// RepresentedIndependent: the independent object already exists, so there is
	// nothing to represent and the stored delta object is unreferenced. Only
	// servers that predate the discard rule answer this; current servers create
	// the alias first and answer Won or Lost.
	RepresentedIndependent
)

func (r Represented) String() string {
	switch r {
	case RepresentedNone:
		return "none"
	case RepresentedWon:
		return "won"
	case RepresentedLost:
		return "lost"
	case RepresentedIndependent:
		return "independent"
	}
	return fmt.Sprintf("represented(%d)", uint8(r))
}

// PutResult is the outcome of a Put that asked for representation.
//
// The rule a caller must follow: record the object it sent only on
// RepresentedWon. On RepresentedLost it adopts Key (after verifying it) or
// stores the chunk independently; on RepresentedNone or RepresentedIndependent
// it stores the chunk independently or records the independent object. An
// object the alias does not name may be deleted by the store at any time
// (Discarded), so a manifest must never point at one.
type PutResult struct {
	// IsNew is false when the object itself already existed (dedup hit).
	IsNew       bool
	Represented Represented
	// Key is the winning delta object's key; set only for RepresentedLost.
	Key ContentKey
	// Discarded reports that the object this Put newly stored was deleted
	// again because it lost (RepresentedLost): nothing can reference it. The
	// bytes were uploaded but do not occupy the store.
	Discarded bool
}

const (
	aliasBodyVersion = 0x01
	// AliasBodyLen is the size of an encoded alias body.
	AliasBodyLen = 1 + len(ContentKey{})
)

// EncodeAlias returns the on-disk body of an alias that names delta.
func EncodeAlias(delta ContentKey) []byte {
	b := make([]byte, AliasBodyLen)
	b[0] = aliasBodyVersion
	copy(b[1:], delta[:])
	return b
}

// DecodeAlias parses an alias body. A body that is not exactly the current
// version is an error: the caller must not guess at another version's meaning.
func DecodeAlias(body []byte) (ContentKey, error) {
	if len(body) != AliasBodyLen {
		return ContentKey{}, fmt.Errorf("store: alias body is %d bytes, want %d", len(body), AliasBodyLen)
	}
	if body[0] != aliasBodyVersion {
		return ContentKey{}, fmt.Errorf("store: alias body version %#x, want %#x", body[0], aliasBodyVersion)
	}
	var k ContentKey
	copy(k[:], body[1:])
	return k, nil
}
