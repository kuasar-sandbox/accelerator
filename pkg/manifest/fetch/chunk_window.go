package fetch

import (
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// ResolveChunkWindow expands anchor to the largest contiguous, final-visible
// range served by the same physical manifest chunk. anchor must be a ChunkRun
// returned by stream.RunAt. The lookup is metadata-only: it does not fetch,
// verify, decrypt, decompress, or read payload bytes.
//
// Expansion is attempted only when the physical chunk is no larger than
// maxBytes. A larger chunk or a package-private ChunkRun implementation whose
// physical identity is unavailable leaves anchor unchanged. In a layered
// Stream, Hole is transparent while an upper Data or Zero run clips the
// returned range. The returned Run therefore remains safe to populate into the
// final composed image; physical chunk bounds alone are never exposed.
func ResolveChunkWindow(stream Stream, anchor ChunkRun, maxBytes uint64) (ChunkRun, error) {
	if stream == nil {
		return nil, fmt.Errorf("%w: nil stream", errInvalidRun)
	}
	if anchor == nil {
		return nil, fmt.Errorf("%w: nil chunk anchor", errInvalidRun)
	}
	if maxBytes == 0 {
		return nil, fmt.Errorf("%w: zero chunk window limit", errInvalidRun)
	}

	target, ok := anchor.(*manifestDataRun)
	if !ok {
		// ChunkRun is deliberately sealed to this package. Keep package-local
		// test or future implementations useful without inventing a public
		// physical-identity interface: unsupported implementations retain the
		// already-safe forward Run.
		return anchor, nil
	}
	chunkStart, chunkEnd, err := validateChunkWindowAnchor(target)
	if err != nil {
		return nil, err
	}
	if chunkEnd-chunkStart > maxBytes {
		return anchor, nil
	}

	// A direct manifest has no overlay that could hide either side of the
	// anchor. Avoid a metadata walk on this common path.
	if stream == target.stream {
		if target.offset == chunkStart && target.end == chunkEnd {
			return anchor, nil
		}
		return newManifestDataRun(target.stream, target.chunkIndex, chunkStart, chunkEnd), nil
	}

	visibleEnd := chunkEnd
	if stream.Size() < visibleEnd {
		visibleEnd = stream.Size()
	}
	if target.end > visibleEnd {
		return nil, fmt.Errorf(
			"%w: chunk anchor [%d,%d) exceeds supplied stream size %d",
			errInvalidRun,
			target.offset,
			target.end,
			stream.Size(),
		)
	}
	windowStart, windowEnd, err := visibleChunkSegment(stream, target, chunkStart, visibleEnd)
	if err != nil {
		return nil, err
	}
	if target.offset == windowStart && target.end == windowEnd {
		return anchor, nil
	}
	return newManifestDataRun(target.stream, target.chunkIndex, windowStart, windowEnd), nil
}

func validateChunkWindowAnchor(run *manifestDataRun) (uint64, uint64, error) {
	if run == nil || run.stream == nil {
		return 0, 0, fmt.Errorf("%w: invalid chunk anchor", errInvalidRun)
	}
	if run.chunkIndex >= uint64(len(run.stream.m.Entries)) {
		return 0, 0, fmt.Errorf("%w: chunk index %d out of range", errInvalidRun, run.chunkIndex)
	}
	entry := run.stream.m.Entries[run.chunkIndex]
	if entry.IsZero || entry.Size == 0 {
		return 0, 0, fmt.Errorf("%w: chunk anchor refers to non-Data entry %d", errInvalidRun, run.chunkIndex)
	}
	chunkStart := entry.Offset
	chunkEnd := chunkStart + uint64(entry.Size)
	if chunkEnd <= chunkStart {
		return 0, 0, fmt.Errorf("%w: chunk %d range overflows", errInvalidRun, run.chunkIndex)
	}
	if run.offset < chunkStart || run.offset >= run.end || run.end > chunkEnd {
		return 0, 0, fmt.Errorf(
			"%w: chunk anchor [%d,%d) outside physical chunk [%d,%d)",
			errInvalidRun,
			run.offset,
			run.end,
			chunkStart,
			chunkEnd,
		)
	}
	return chunkStart, chunkEnd, nil
}

// visibleChunkSegment walks the final composed Stream without building a run
// plan. Adjacent final Runs that resolve to the same physical chunk are merged;
// an opaque Run splits them. Only the segment containing target.offset is
// returned, so a lower chunk that reappears after an upper overlay is never
// represented as one unsafe window.
func visibleChunkSegment(stream Stream, target *manifestDataRun, chunkStart, chunkEnd uint64) (uint64, uint64, error) {
	var (
		segmentStart uint64
		segmentEnd   uint64
		inTarget     bool
		found        bool
	)

	for offset := chunkStart; offset < chunkEnd; {
		run, err := stream.RunAt(offset, chunkEnd-offset)
		if err != nil {
			releaseChunkWindowRun(run, target)
			return 0, 0, fmt.Errorf("fetch: resolve chunk window at %d: %w", offset, err)
		}
		if err := validateRun(run, offset, chunkEnd); err != nil {
			releaseChunkWindowRun(run, target)
			return 0, 0, err
		}
		next := run.End()
		same := samePhysicalChunk(run, target)
		releaseChunkWindowRun(run, target)

		if same {
			if !inTarget {
				segmentStart = offset
				inTarget = true
			}
			segmentEnd = next
		} else if inTarget {
			if target.offset >= segmentStart && target.offset < segmentEnd {
				found = true
				break
			}
			inTarget = false
		}
		offset = next
	}
	if !found && inTarget && target.offset >= segmentStart && target.offset < segmentEnd {
		found = true
	}
	if !found {
		return 0, 0, fmt.Errorf(
			"%w: anchor chunk [%d,%d) is not visible at %d in the supplied stream",
			errInvalidRun,
			chunkStart,
			chunkEnd,
			target.offset,
		)
	}
	if segmentStart > target.offset || segmentEnd < target.end {
		return 0, 0, fmt.Errorf(
			"%w: visible chunk window [%d,%d) does not contain anchor [%d,%d)",
			errInvalidRun,
			segmentStart,
			segmentEnd,
			target.offset,
			target.end,
		)
	}
	return segmentStart, segmentEnd, nil
}

// A wrapper is allowed to preserve and return an underlying Run directly. Do
// not recycle the caller-owned anchor if such a wrapper returns the identical
// object during the metadata walk.
func releaseChunkWindowRun(run sparse.Run, target *manifestDataRun) {
	if run != target {
		releaseRun(run)
	}
}

func samePhysicalChunk(run sparse.Run, target *manifestDataRun) bool {
	data, ok := run.(*manifestDataRun)
	return ok && data.stream == target.stream && data.chunkIndex == target.chunkIndex
}
