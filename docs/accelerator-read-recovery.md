[English](accelerator-read-recovery.md) | [简体中文](accelerator-read-recovery_zh.md)

# Read errors and recovery

## 1. Overview

`accelerator` provides single-attempt immutable reads through `fetch.Stream`, `sparse.Run`, Bundle and cache/Store clients. It leaves business retry and backoff to the consumer. In a running sandbox, `sandboxer` retains mandatory I/O synchronously until it succeeds or its owner terminates the operation/VM.

A failed read does not permanently poison a client, lazily loaded index or referenced source. Later calls can recover at the original endpoint. This does not authorize replay of writes or changing the selected data source.

## 2. CLI

Existing commands and flags are unchanged. Direct CLI reads report their single-attempt errors to the command owner. Store `Put`, Fill, ingestion, publication and snapshot capture are not replayed by this read contract. A lost response does not establish that a write was not applied.

## 3. Configuration

The existing cache/Store endpoint, pool and per-operation timeout settings remain authoritative. Pool dialing has a bounded connection timeout even when no read deadline is configured. Socket and RPC deadlines are attempt-local; the consumer's operation context determines whether another read attempt remains allowed.

No new retry setting, fallback endpoint, health mask, service restart or compatibility mode is introduced. Pool maintenance remains separate from business reads.

## 4. Error contract

`pkg/readerr.Mark(err, retryable)` preserves `Error()` and `Unwrap()` and adds only `Retryable() bool`. `Mark(nil, ...)` is nil. An unknown error is not a confirmed permanent failure. `IsPermanent` walks wrappers and joined causes, so a later permanent reason cannot disappear behind a temporary sibling. A marker at a semantic boundary is authoritative for its wrapped branch.

Normal end-of-object EOF and confirmed cache/Store miss retain their existing interfaces. Required immutable object lookup converts a confirmed absence into a permanent cause. Complete frame bounds, validated Run geometry, immutable format/range violations and confirmed integrity/authentication failures are marked where that meaning is known. Network EOF, partial frames/streams, backend-local cancellation and opaque access errors preserve their causes. A custom decrypt/decode error is not automatically proof of corruption; built-in format validation and actual authentication checks supply the classification.

A fixed-size ReaderAt request is one attempt. A short nil result or a real truncated declared field is a structural failure, not permission to append a second response. Complete ordinary EOF remains valid where the source's contract allows it. Explicit failure wrappers are preserved before EOF compatibility processing. The random-access metadata adapter prevents `io.ReadFull` from swallowing a complete buffer accompanied by a source failure. Sequential sources retain their one-pass contract and preserve diagnostics/causes without acquiring replay behavior.

## 5. Recovery and reliability

Connection capacity counts idle, borrowed and dialing connections under the same reservation limit for Acquire and refill. A failed dial returns its actual error immediately after releasing its reservation. A bad connection releases capacity and wakes a waiter even when no healthy connection was returned. A waiter that cancels or takes a healthy connection transfers a consumed notification when spare capacity remains. Only bounded maintenance workers perform health/refill work.

Close prevents new publication, wakes waiters and cancels dials; borrowed connections remain owned until their caller releases them. Release and Close serialize publication. Socket cancellation interrupts the actual read/write and its callback is stopped or joined before connection ownership returns to the pool. No per-read Ping or whole-pool invalidation is required for same-endpoint recovery.

Cache Get/GetShard and Store Get perform one business attempt. Failed payloads and streams are released/canceled. The next call reads a complete new object, without keeping or splicing an old prefix. Legal server cancellation stays retryable; confirmed miss stays distinct from inability to contact the peer.

Bundle Open retains its metadata/source-selection rules. Chunk-index preparation serializes loading, builds a validated map locally and publishes it only on success. Failure or cancellation is not cached. Reader Close still waits through existing source leases and prevents late publication. Each selected Manifest layer continues using its selected Bundle or remote source: a failed local read does not fall through to another Bundle or Store. When unavailable ordered refs leave lookup uncertain, a remote miss does not falsely prove global absence; the original causes remain inspectable.

Parallel range reads join every worker before returning, including on cancellation. The original failure is retained, derived sibling cancellation does not replace it, and another worker's permanent reason remains in the aggregate. No worker may keep writing the caller's buffer after the read returns.

Tests cover failed dialing, an actually full pool, cancellation consuming a wake, concurrent refill/Close, whole-pool old connections, original-endpoint restart, half-frame/half-stream cleanup, initialization recovery, source isolation and joined permanent errors. The consumer's CH/KVM tests validate Guest completion and snapshot behavior with the exact paired source set.

## 6. Performance

The idle Acquire path uses the existing connection channel without the publication mutex. Failed attempts create no unbounded background refill tasks. Capacity and maintenance concurrency remain bounded across outage duration. Successful Bundle preparation is reused; failed preparation consumes only attempt-local state. Existing streaming sparse iteration and Hole/Zero/Data semantics remain unchanged.

## 7. See Also

- [Cache](cache.md)
- [Store](store.md)
- [Manifest](manifest.md)
- [File artifacts](file-artifacts.md)
- [Sandboxer synchronous recovery](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandboxer-read-recovery.md)
- [Proposal and acceptance checklist](https://github.com/kuasar-sandbox/sandboxer/issues/225)
