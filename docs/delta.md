# Delta compression: similar chunks stored as patches

Use this guide to understand what delta compression changes in the Manifest write and read paths, how to enable and size it, what the Store and the tooling do with delta objects, and how to roll it out and operate it. It is the contract. Object and manifest layouts are specified in [artifacts](artifacts.md); the Store side of aliases is in [store](store.md).

Delta compression is **off by default** (`delta.enabled: false`). With it off, the pipeline is byte-for-byte what it was before the feature existed.

## What it does

The write path already removes exact duplicates: a chunk's object address is a function of its plaintext and the salt, so the second copy of a chunk costs one `Put` that the Store answers with `isNew=false`. Delta compression goes after chunks that are *not* exact duplicates but are mostly the same bytes as a chunk already stored: the same library with a different build ID, the same configuration file with one line changed, the same disk block with a few records rewritten. Such a chunk is stored as a **VCDIFF patch against the earlier chunk** (its **base**) instead of as a standalone object.

A patch is accepted only when the delta object is at least **4 KiB and 20 % smaller** than the independent object the chunk would otherwise have produced. The fallback is always the existing path. A base is always an ordinary independent object; a delta is never built against another delta (**chain depth one**), so reading a delta chunk costs at most two object fetches.

Performance estimation on a Non-Versioned 62-image VM dataset, the store shrank from 26.10 GiB to 20.33 GiB (**−22 %**); on a Versioned 142-image VM dataset, where exact dedup already removes most repeats, delta halved what remained (11.09 GiB to 5.34 GiB, **−52 %**).

## Core model

Three identifiers matter for every chunk `X` with plaintext `P`:

| Name | Definition | Role |
| --- | --- | --- |
| `K_X` | `SHA256(salt ‖ P)` | The convergent key of the independent object. |
| `H_ind(X)` | `SHA256([format] ‖ AES-CTR(K_X, RAW-or-Snappy(P)))` | The address `X` would have as an independent object. Every writer can compute it from plaintext and salt alone. It is the key of the alias and of the representation index. |
| `K_d` | `SHA256(salt ‖ "accelerator-delta-chunk/v1" ‖ baseObjectKey ‖ P)` | The key the manifest records for a delta chunk. Bound to the base so the same plaintext patched against two bases never shares a CTR keystream. |

A **delta object** has format byte `0x03`: a 45-byte cleartext header (base object key, base plaintext size, target size, VCDIFF stream size, payload encoding) and then `AES-CTR(K_d)` over the base's key `K_base` followed by the RAW or Snappy VCDIFF stream. Its address is `SHA256(object)` like any chunk. The base object key is in the clear so a reader can start fetching the base before decrypting; the base's key is inside the ciphertext so the manifest key table stays one 32-byte key per chunk.

A **manifest** that contains at least one delta chunk is **Version 2**: the entry carries `EntryFlagDelta` (flag bit 1) and a **delta table** row `{entry, base_size, base_object_key}` follows the hole table. The AAD covers the table, so a base reference cannot be swapped without the key table failing to unseal. A manifest without deltas is Version 1 and byte-identical to pre-delta output. The table is what prefetch, bundles and `manifest-ctl deps` use to know an image's full object closure without opening any object; the delta object's own header stays authoritative when the object is opened.

The **dedup domain is the delta domain**: candidates, bases and aliases are scoped to one `(generation, salt)` pair, exactly like exact-duplicate dedup. A chunk is never patched against a base in another generation or under another `extra_salt`.

### Invariant

**A manifest references a delta object only if the Store's alias for `H_ind` names that object.** Every rule below exists to keep this true. It is what makes two writers converge on one representation, what lets the Store delete a losing writer's object, and what makes `store-ctl delta-gc` safe.

## Configuration

The `delta:` block of the Manifest config is decoded **strictly** (an unknown key is an error, unlike the rest of the file). Sizes are human-readable strings (`util.ParseSize`), durations are Go duration strings. Every field has a default; `enabled: true` alone is a valid block.

```yaml
delta:
  enabled: false                 # master switch; false preserves the existing pipeline exactly
  feature_chunker:               # sub-chunking used only to compute similarity features
    min: "256"
    avg: "1KiB"
    max: "4KiB"
  sketch:
    slots: 16                    # k min-hash slots, power of two in [2, 64]
    group_size: 2                # g slots per super-feature; must divide slots
  candidates:
    min_similarity: 0.5          # estimated Jaccard threshold, (0, 1]
    max: 4                       # candidates tried with a real VCDIFF encode
    attempt_deadline: "50ms"     # wall-clock budget per chunk before falling back
    max_chunk_size: "1MiB"       # larger chunks are never delta encoded
  index:
    max_entries: 0               # similarity-index bases per domain; 0 = max(1024, base_pool.max_bytes / 64 KiB)
    max_postings: 64             # longest posting list per super-feature
    max_representations: 10000000
    known_capacity: 8000000      # Bloom filter of chunks known to be stored independently
    max_domains: 4               # (generation, salt) domains kept at once
    verify_hit: true             # confirm a representation hit with one Resolve before skipping the Put
    snapshot_path: ""            # empty = state lives only in the process
    snapshot_interval: "5m"
  base_pool:
    max_bytes: "4GiB"            # resident base plaintext
  fetch:                         # honoured by readers whether or not enabled is true
    base_cache_bytes: "64MiB"    # shared plaintext cache of decoded bases
    scratch_bytes: "128MiB"      # concurrent delta-apply memory
    max_readbacks: 4             # concurrent Store read-backs during ingest
```

### Sample: delta enabled with a persisted snapshot

This `manifest.yaml` turns delta on and persists the sealed snapshot, so a later `manifest-ctl store` process can see the earlier images. Replace the key with a generated one (`python3 -c 'import secrets; print(secrets.token_hex(32))'`). Start a Store on the socket first; the [independent roundtrip](transfer.md#independent-store-roundtrip) shows how to initialise and serve one.

```yaml
manifest:
  key: "REPLACE_WITH_64_HEX_CHARS"
  verify_content: true
store:
  endpoint: unix:///tmp/accelerator-delta/store.sock
  timeout: 5s
chunker:
  mode: cdc
  cdc:
    min: 128KiB
    avg: 512KiB
    max: 1MiB
crypto:
  chunk: aes
  manifest: aes
  local: off
delta:
  enabled: true
  index:
    snapshot_path: /tmp/accelerator-delta/delta.snap
    snapshot_interval: "5m"
```

```bash
mkdir -p /tmp/accelerator-delta
manifest-ctl store --manifest-config manifest.yaml --no-progress image1.tar
manifest-ctl store --manifest-config manifest.yaml --no-progress image2.tar
```

The second run prints a `delta:` line; for similar images, `saved` is non-zero and `attempts` shows bases from the first image being tried. The snapshot directory is created with mode `0700` if missing, the file is written with mode `0600`, and it can only be read with the same `manifest.key`.

What to size deliberately:

- **`base_pool.max_bytes`** is decrypted plaintext held in process memory. It is a memory budget and a confidentiality surface (pages are zeroed on eviction). A base that leaves the pool is still usable: the writer reads the object back from the Store, so a small pool costs read-backs, not savings. The default is large; a CLI that imports one image per process does not need 4 GiB.
- **`index.max_entries`** decides how far back a chunk can look for a base. Caps of 1 and 4 saved nothing on the phase 3 corpus; the cap has to span the distance between a base and the chunk that resembles it.
- **`index.snapshot_path`** is what lets one-image-per-process writers (`manifest-ctl store`) see earlier images. Without it every process starts empty and only intra-image similarity is found. The file is sealed (below). The Bloom filter(a fixed-size set that answers "already stored whole?" per chunk; its false positives, under 1%, only skip a delta attempt) dominates its size: about 10 MB per domain at the default `known_capacity`.
- **`candidates.attempt_deadline`** bounds the write-side CPU cost per chunk. The benchmark runs used 500 ms; the default is conservative.
- **`fetch.*`** is read by every reader. `base_cache_bytes` should cover the distinct bases of the images a host reads concurrently; the default 64 MiB holds about 128 bases of 512 KiB.

`delta.enabled: true` is also refused up front, with one log line, for writers that cannot support it: a bundle writer (bundles are self-contained and the base might live outside the bundle), or a writer or encryptor lacking the delta interfaces. Such a writer stores ordinary objects.

## Write path

For every unique chunk the ingester still computes the independent object: its hash `H_ind` is the lookup key and its size is the acceptance baseline. Delta adds, in order:

1. **Gates.** Skip chunks below `4 KiB + 78 B` (a delta cannot save 4 KiB) or above `candidates.max_chunk_size`, and skip everything if the Store turned out not to support aliases.
2. **Local belief.** Look `H_ind` up in the representation index (chunks this node knows are stored as deltas). A hit with `verify_hit: false` is used without an RPC. A miss that the Bloom filter knows as stored-whole is stored normally.
3. **Ask the Store**: `Resolve(generation, H_ind)`. `INDEPENDENT` → store normally (dedup) but still sketch and register the chunk as a base. `DELTA{k}` → use the local entry if it names `k`, otherwise **adopt** `k`. `ABSENT` → continue. A failed call means "cannot tell" and the chunk is stored independently. `Unimplemented` disables delta writes for the rest of the process.
4. **Similarity.** Cut the plaintext into ~1 KiB features, compute a 16-slot min-hash sketch folded into 8 super-features, and look up the per-domain similarity index. Candidates with estimated Jaccard ≥ `min_similarity` are ranked; up to `candidates.max` are tried with a real VCDIFF encode against their plaintext (from the base pool, or read back from the Store under `fetch.max_readbacks`), within `attempt_deadline`.
5. **Accept or reject.** The best delta object is kept only if `DeltaBeneficial(len(independent), len(delta))` holds: at least 4 KiB and 20 % smaller. Rejected chunks are stored independently and registered as bases.
6. **`Put` with `represents = H_ind`.** The Store stores the delta object and tries to create the write-once alias `H_ind → delta key`:
   - `WON`: record the representation locally; the manifest entry points at the delta.
   - `LOST{k}`: another writer's delta already represents the chunk. The Store has **deleted our object** (`discarded`) if this `Put` created it. Adopt `k`; if adoption fails definitively, store independently (which supersedes the alias for everyone).
   - `NONE` (the alias step failed): our object is valid but unnamed, and an unnamed delta must not be referenced. Store independently; the unnamed object is an orphan for `delta-gc`.

**Adoption** is mandatory and complete. A node adopting another node's delta reads the object, checks its hash, header and the acceptance rule, re-derives `K_d` from its own plaintext and opens it, takes the base from its pool or reads it, checks its size against the header and re-encrypts it under its own salt to prove `K_base` matches the base's address, applies the patch and compares every byte. Only then does it record the representation and write `K_d` into its manifest. A definitive failure stores the chunk independently; a transient one (read error, context cancelled) keeps the node's own delta if it has one.

The chunk whose delta was accepted is **not** registered as a base (depth one). The chunk is sketched regardless of the outcome, because the sketch is how it becomes a base for later chunks.

### Statistics

`manifest-ctl store` prints a `delta:` line after `compression:` when delta ran:

```text
delta:        chunks=96 logical=24.0 MiB saved=23.8 MiB repHits=0 attempts=96 rejected=0 noCandidate=0 notResident=96 readbacks=96 known=0 timeouts=0 errors=0
aliases:      resolves=96 won=96 lost=0 independent=0 adopted=0 adoptFailed=0 discarded=0 B orphan=0 B
```

`RawChunks`/`CompressedChunks` still count every non-zero chunk by the independent encoding it would have had, so `CompressionSavedBytes` keeps its meaning; `DeltaChunks`, `DeltaLogicalBytes` and `DeltaSavedBytes` describe the delta effect on top. `discarded` is bytes the Store deleted for us after a lost race; `orphan` is bytes that stayed in the Store unreferenced (an alias step that failed, or a discard that failed).

### Persisted state: the sealed snapshot

All write-side state is **soft**: losing it never makes a read wrong, because every delta object is self-sufficient and every manifest names exactly the objects it needs. Losing it only costs future savings. With `index.snapshot_path` set, the engine persists per domain the representation index, the known-independent Bloom filter and the similarity index (base keys, sizes and sketches; not the plaintext) at the end of the first ingest, then at most every `snapshot_interval`, and on `CloseDelta`.

The snapshot contains chunk decryption keys (each base's `K_base`), so everything after its cleartext header is **AES-256-GCM sealed** under a key only a holder of the customer key can derive:

```text
K_snap = HMAC-SHA256(customerKey, "accelerator-delta-snapshot/v1")
K_file = HMAC-SHA256(K_snap, "accelerator-delta-snapshot-file/v1" ‖ fileID)   # fileID: 16 random bytes in the header
```

The body is a sequence of independently sealed 1 MiB frames with the frame counter in the nonce and the header plus an end-of-file flag in the AAD, so truncation and reordering are detected without a separate checksum. A snapshot written under another customer key, another generation or other sketch parameters is refused and the engine starts empty (one log line).

## Read path

Readers need no configuration to read delta chunks: `fetch.*` sizes the buffers, `enabled` is irrelevant. When a `ReadAt` lands in a chunk whose object starts with `0x03`, the fetcher parses the header, fetches and decrypts the base (refusing a base that is itself a delta), applies the patch under the `fetch.scratch_bytes` budget and serves the range. A partial read materialises the whole chunk, since VCDIFF has no random access; the per-stream plaintext chunk cache then serves neighbouring reads.

Decoded bases live in a **shared plaintext base cache** (`fetch.base_cache_bytes`) keyed on **object address, base key and plaintext size**. Keying on the address alone would let a manifest carrying a wrong key for a base poison every other reader of that base in the process; with the key in the cache key a bad manifest only fails its own read.

Before applying, the reader checks the object against the manifest: the entry's delta flag must match the object's format byte, and the delta table must name the same base object and base size as the object header. The flag and table are authenticated by the key table's AAD and the header is not, so this binds the base to the manifest even with `verify_content` off. A mismatch fails the read before the base is fetched.

**Prefetch** of a Version 2 manifest warms deltas and their bases in parallel through the delta table.

A delta chunk costs more than an independent one: one extra object fetch when the base is not cached, one decrypt of the base and one VCDIFF apply either way.

## Store

The Store never opens a delta object (it holds no keys or plaintext). It contributes two things, specified in [store.md](store.md#delta-aliases-and-resolve):

- the **alias** partition and the `Resolve` RPC, through which writers agree on one representation per chunk; and
- the **discard**: a `Put` whose `represents` lost the alias race deletes the object it just created, so the common race leaves nothing behind. `store-ctl serve` reports `discarded` and `discardErr` on its stats line.

Readers never consult aliases: a manifest names the objects it uses and carries the keys that open them.

### Orphans and `delta-gc`

An **orphan** is a delta object no alias names. By the invariant, no manifest can reference it. Orphans arise only on error paths: the alias step failed after the object was stored (`NONE`), or a discard failed. `store-ctl delta-gc --generation G` audits one generation: it lists aliases, counts `0x03` objects in the chunk partition (fs reads the first byte of each file; S3 uses a ranged GET of the first bytes) and reports those no alias names, with their age. `--delete` removes orphans at least `--older-than` old (default one hour, so a writer between its `Put` and its alias is never raced); orphans of unknown age are never deleted. Aliases are never deleted individually: a missing alias would let two writers disagree, and they leave with their generation (`purge --generation`). An alias naming an object the generation no longer holds is reported as dangling and is harmless: a writer that resolves to it fails adoption and stores the chunk independently, which supersedes it.

A **generation purge** removes chunk, manifest, blob and alias partitions together, so delta needs nothing extra there. There is no reference-aware collector for ordinary chunks either; what `delta-gc` adds is the audit for the one kind of object that can never be referenced.

## Tooling

| Command | Delta behaviour |
| --- | --- |
| `manifest-ctl store` | Prints the `delta:` and `aliases:` lines; emits a Version 2 manifest iff at least one chunk is a delta. |
| `manifest-ctl info` | For Version 2 adds `delta chunks: N (bytes)` and `delta bases: N distinct (bytes), M outside this manifest`. |
| `manifest-ctl deps [-bases-only] [-quiet] <manifest\|manifest://hex>` | Prints the image's object closure, one line `<role> <hex> <bytes>` per distinct object: `chunk` (independent, addressed by an entry), `delta` (addressed by an entry) or `base` (needed by a delta, addressed by no entry). A collector that keeps this set for every live manifest never removes a base from under a delta. |
| `manifest-ctl verify` | Opens every delta, fetches its base, applies and checks; the summary reads `verified: N (delta: D)`. |
| `manifest-ctl load` / fetch | Transparent. |
| `store-ctl delta-gc` | Orphan audit and collection, above. |
| `store-ctl info` | `alias=N` per generation. |

## Bundles

A bundle **can carry** delta chunks: `FullVerify` and the multi-upload verifier open each delta, require its base to be present in the bundle (`ErrIncomplete` otherwise), check the manifest's delta table against the object header, reject a base that is itself a delta, and apply and compare. `CopyManifest` copies the full closure including bases.

Uploading a bundle that carries delta chunks (`Reader.Upload`, `UploadExactManifests`) stores each delta object and then records the alias that names it, under the chunk's independent address in the target's admission generation. That way `store-ctl delta-gc` does not treat it as an orphan. When the alias cannot be recorded, the upload fails with `bundle.ErrDeltaAlias` before any manifest is published. The causes are:

- the target client cannot record aliases;
- the store does not report a recorded alias (a server that predates aliases);
- the target already represents the chunk by a different delta object;
- the manifest was written with a writer extra salt, so the chunk's independent address cannot be computed from the generation salt.

Any delta object stored before the failure is unreferenced, and `delta-gc` removes it after its grace period.

Bundle **writers do not produce** deltas: the base would have to be carried in full, which is what the delta was meant to avoid, so a writer in bundle mode runs with delta off and logs it once. A delta-encoded image can still be bundled by a tool that copies an existing manifest's closure.

## Compatibility and rollout

- **Readers first.** A reader that predates the feature fails on `0x03` with "unknown chunk format" and on a Version 2 manifest with an unknown-version error. Upgrade every binary that opens chunks or manifests, then `store-ctl`, then enable `delta.enabled` on writers. All current readers accept `0x03` and Version 2 whether or not delta is enabled for them.
- **Old `store-ctl`.** A Store without `Resolve` makes the writer store independently for the rest of the process (one log line). Deployments with several Stores get delta only on upgraded ones, and manifests for the same image can differ between Stores, as they already can.
- **Determinism.** A first-ever ingest with delta on is byte-identical to the delta-off manifest when nothing in the image has a base; re-ingesting an image reproduces the same manifest key, in the same process, after a restart with the snapshot, and on a cold node (through the alias). What delta changes is that two *empty* Stores given the same images in different orders can pick different bases and so produce different manifest keys.
- `go.mod` is unchanged: the VCDIFF codec is vendored under `pkg/manifest/delta/internal/vcdiff` with a bounded decoder (see `NOTICE.md`).

## Testing

- Unit and integration tests under `pkg/manifest/delta`, `pkg/manifest/crypto`, `pkg/manifest/codec` (Version 2 round trip, Version 1 bytes and AAD unchanged, inconsistent tables rejected), `pkg/manifest/ingest` (every delta image read back through the manifest and the Store as a reader would, concurrent nodes converging on one manifest, adoption failures, discard accounting), `pkg/manifest/fetch` (delta materialisation, base-cache poisoning, prefetch warming bases), `pkg/manifest/bundle` (delta verification, tampering, missing bases, closure copy, alias recording on upload and its failure cases), `pkg/store` (aliases, discard, `AuditDeltas` on fs and S3) and `cmd/store-ctl`/`cmd/manifest-ctl`.
- Fuzzing: `FuzzEncodeApply` and `FuzzApplyUntrusted` on the VCDIFF codec.
- Product E2E: `test/e2e/cases/image.manifest-delta.sh` runs two Store sidecars and delta-on/off writers over real binaries, asserts the write-side and read-side contracts above, checks `delta-gc` finds no orphans after ingest, verifies the sealed snapshot is reloaded by a new process and does not contain a base key in the clear, and reports the full-load start-up time of a delta-encoded image on versus off. See [test/e2e/README.md](../test/e2e/README.md).

## Limitations

- A delta chunk's first read is slower than an independent chunk's (two fetches, a decrypt and an apply).
- Delta is found only within one `(generation, salt)` domain and, across processes, only with a snapshot or through the alias. The similarity index is node-local; which base the *first* writer of a chunk picks depends on its own ingest order.
- Bundle writers do not produce deltas.
- Two writers finishing the same new chunk at the same instant both upload a patch; the Store discards the loser's. Only error paths leave orphans, and `delta-gc` collects them.
- An alias is never repaired in place; a bad one is superseded when any writer stores the chunk independently.

## Related contracts

- [Store](store.md): aliases, `Resolve`, discard, `delta-gc`.
- [Artifacts](artifacts.md): format `0x03` and the Version 2 manifest layout.
- [Data transfer](transfer.md): `manifest-ctl` commands and configuration.
