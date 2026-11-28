# Vendored: github.com/go-deltasync/vcdiff

Source: `internal/vcdiff` of github.com/go-deltasync/vcdiff
Version: v0.0.0-20261010213333-45ef73e29b45
License: BSD 3-Clause (see LICENSE in this directory)

Copied from upstream (package `vcdiff`, standard library only) with the local
modifications listed below. It is vendored rather
than imported because that module declares `go 1.27.1`, and importing it would
raise this repository's minimum Go version (currently 1.26.1).

To switch to a normal dependency later, delete this directory and import
`github.com/go-deltasync/vcdiff` from `pkg/manifest/delta` once the repository
moves to Go 1.27.

## Local modifications

The upstream decoder trusts the lengths in the delta stream: a hostile stream can
declare a huge target window, huge sections, or a RUN/COPY instruction far larger
than its window, and the decoder allocates accordingly. Chunk objects normally
carry a content hash, but verification can be disabled, so the decoder is bounded.

* `decode.go`: added `Limits` and `DecodeLimited` (caps total target bytes and
  per-section lengths, checked before allocation). `Decode` keeps its signature
  and is unlimited, as upstream.
* `decode.go`: `runInstructions` rejects any instruction that would grow its
  window past the declared window size.
* `limits_test.go`: tests for the above (not upstream).

All other files are byte-identical to upstream.
