package store

import "time"

// ChunkFormatDelta is the first byte of a delta-compressed chunk object. It is
// defined by pkg/manifest/crypto (ChunkFormatAESDelta); the store does not
// interpret chunk objects, but an audit needs to tell a delta object from an
// independent one, and the format byte is the only thing that distinguishes
// them at this layer.
const ChunkFormatDelta byte = 0x03

// DeltaAudit is what a scan of one generation's delta objects found.
//
// The invariant that makes the audit meaningful: a manifest references a delta
// object only if the generation's alias for the chunk names that object (see
// PutResult). So a delta object no alias names is referenced by nothing and is
// safe to delete, with one caveat: a writer may have just stored it and not yet
// created its alias. A collector therefore leaves orphans younger than a grace
// period alone (see OrphanDelta.ModTime). The reverse case, an alias naming an
// object that is absent, is a dangling alias; writers that Resolve to it fail to
// adopt it and store independently, which supersedes it, so it is harmless, but
// it is reported.
type DeltaAudit struct {
	Aliases      int    // alias records in the generation
	Named        int    // distinct delta objects those aliases name
	Dangling     int    // aliases naming an object the generation does not hold
	Chunks       int    // chunk objects scanned
	DeltaObjects int    // of those, delta objects
	DeltaBytes   uint64 // their stored size
	Orphans      []OrphanDelta
	OrphanBytes  uint64
}

// OrphanDelta is a delta object no alias names.
type OrphanDelta struct {
	Key  ContentKey
	Size uint64
	// ModTime is when the object was written, when the backend knows; zero
	// otherwise. A collector uses it to skip objects a writer may still be
	// about to alias.
	ModTime time.Time
}
