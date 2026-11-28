// Package delta holds the pure, crypto-free building blocks for delta
// compression of unique chunks: similarity sketches, the similarity index, the
// representation index, the in-memory base pool, and VCDIFF encode/apply.
//
// It knows nothing about encryption, object formats, stores or manifests. The
// ingest package orchestrates it together with crypto; keeping the two apart
// makes every piece here testable without a store or a key.
//
// Everything in this package is SOFT STATE. Losing any of it (a restart, an
// eviction, a rejected snapshot) can only cost compression ratio, never
// correctness: chunks simply fall back to ordinary independent objects. The one
// thing that must never happen is a stale entry (an index that points at an
// object which does not exist or means something else); entries are therefore
// partitioned by dedup domain (the salt) and persisted state(a snapshot) is
// rejected wholesale when its parameters or checksum do not match.

package delta
