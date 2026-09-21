[English](README.md) | [简体中文](README_zh.md)

# Accelerator E2E tests

This is the accelerator-owned suite for an assembled platform binary set. It exercises real flatten, manifest, store and cache APIs, including real EROFS creation. Keep the entire `test/e2e` tree, including `lib/`, when assembling tests; the fixtures need no private source files.

Run from the accelerator checkout:

```sh
make test-e2e-scripts                          # offline script/fixture regressions; no builds
make test-e2e E2E_BIN=/path/to/assembled/bin/x86_64
# Or run the assembled suite directly:
BIN=/path/to/assembled/bin/x86_64 bash test/e2e/run_all.sh
# Run only flatten/manifest coverage:
BIN=/path/to/assembled/bin/x86_64 bash test/e2e/e2e_manifest.sh
```

`E2E_BIN` defaults to the sibling platform checkout's `bin/$(TARGET_ARCH)`; direct `run_all.sh` requires `BIN`. Use native Linux binaries for the runner architecture. `make test-e2e-scripts` requires Python 3.9+, Bash and the ordinary tools listed below, but no assembled binaries, Docker, Redis, sudo privileges, Go or native compilation. It checks port leases and runs fixture/lifecycle tests with explicitly stubbed CLI boundaries. Those boundary tests never complete the numbered E2E assertions and are not evidence of real E2E success. `make test` also includes these regressions and retains its existing RocksDB/unit-test build requirements.

The real suite requires:

- Linux, Bash 4+ with `/dev/tcp`, Python 3 stdlib, OpenSSL, GNU coreutils/findutils, GNU grep (including `-P`), awk, sed, tar, unzip, and util-linux `flock`; GNU Make for the Makefile entry points. These scripts use Linux sparse-file operations and GNU command options.
- An assembled `BIN` containing executable `flatten-ctl`, `manifest-ctl`, `store-ctl` and `cache-ctl`. Use the normal RocksDB-enabled cache binary; `no_rocksdb` is not the suite default or a substitute for required coverage.
- A real `mkfs.erofs` supporting deduplication and chunked layout (`-Ededupe --chunksize=4096`). Resolution is `MKFS_EROFS_PATH`, then beside the resolved `flatten-ctl` executable, then `PATH`. The manifest test checks it before starting its store.
- Root or noninteractive `sudo -n` for flatten export to preserve layer UID/GID ownership. The rest of the suite can run unprivileged. The manifest test forwards only isolated HOME/Docker configuration, owned scratch space and the resolved EROFS tool into export; it does not use `sudo -E`.
- `redis-server` on `PATH`, or `REDIS_SERVER` naming the executable, for the required Redis local/tiered/sharded cases. The suite starts disposable local Redis processes; it needs no external Redis service.
- Writable scratch storage with real sparse-file/`SEEK_HOLE` support and room for image/store copies, plus available loopback TCP ports and Unix sockets. Existing cache/port/rolling scripts use `/tmp`; the manifest script honors `TMPDIR` for its per-run directory. Its store readiness allows up to five seconds and fails with the store log on timeout; this is a startup bound, not a performance assertion. Cleanup sends TERM, allows three seconds, then KILLs/reaps its own store and removes its work directory. INT/TERM retain exit statuses 130/143.

`run_all.sh` always runs these cases, in this order:

| Script | Required intent |
| --- | --- |
| `port_lease_test.sh` | 128 rapid plus 128 concurrent unique loopback port leases. |
| `e2e_cache.sh` | Store roundtrips; embedded and Redis local/shard/tiered caches; EC reads, peer failures, upstream fill/writeback, slow-peer cancellation, restart/refill and origin-free warm reads. |
| `e2e_store_cache_listen.sh` | Store's embedded read-only cache protocol, generation rollout, older manifest reads, blob namespace and rejected writes. |
| `e2e_cluster_rolling.sh` | Rolling EC membership via SIGHUP, successful reconstruction with surviving peers and no origin fallback. |
| `e2e_manifest.sh` | All 12 numbered flatten/manifest cases below. |

The manifest cases remain: (1) flatten, trailing config ZIP, architecture and real EROFS magic; (2) store/load byte roundtrip; (3) repeat-store dedup with zero new chunks; (4) remote manifest info; (5) verification; (6) repeatable get-manifest; (7) streamed get-manifest/info; (8) cross-image shared-chunk diff; (9) fixed versus CDC diff; (10) piped stdin store/load; (11) zero-block roundtrip, at least 100 zero chunks, matching info count and manifest size at most 12 KiB; (12) sparse-hole metadata and exact restored bytes. Existing numbered assertions and `Results: N passed, N failed` output remain the test contract.

By default, `lib/manifest_fixture.py` creates two reproducible Docker-format archives using only Python stdlib. Each has two layers: a common base with 4 MiB of deterministic high-entropy bytes, then a differing 1 MiB file that replaces the base version. Timestamps, tar metadata, config digests and layer diff IDs are fixed/consistent. The fixture generator defaults to the host architecture (`amd64` or `arm64`); `--architecture` selects either explicitly. The data images carry Linux metadata and runtime user/environment/working-directory settings, including UID/GID 1000 file ownership. They are data fixtures, with no container command to launch; image architecture is metadata. Several CDC and fixed chunk boundaries fit in the shared content, without trivial mostly-zero dedup. Archives feed directly into `flatten-ctl export`; the default path needs no Docker or registry access.

Explicit `IMAGE_A` and/or `IMAGE_B` can replace either archive with an already-cached Docker image:

```sh
BIN=/path/to/assembled/bin/x86_64 IMAGE_A=already-cached:local \
    bash test/e2e/e2e_manifest.sh
```

Only overrides require Docker and access to its default local daemon. Each requested image is inspected and saved before store startup; a cache miss or unavailable daemon fails clearly. No image is pulled, tagged or removed. The other image still uses its generated fixture. Docker runs with a fresh, disposable HOME/`DOCKER_CONFIG`, ignoring caller credentials, contexts and connection environment. Caller tags remain untouched.

`e2e_obs.sh` is excluded unless `OBS_E2E=1` is explicitly set. It is a separate credentialed cloud test requiring an authorized OBS/S3-compatible endpoint, bucket and credentials (`OBS_BUCKET`, with endpoint/region/AK/SK overrides or its documented `~/.obsconfig` discovery), and permission to create/list/delete its isolated prefix. See [the OBS script](e2e_obs.sh) for the exact inputs. An excluded OBS case is **not cloud qualification**; ordinary/offline runs need no cloud credentials or cloud APIs.

Shared runner routing, validation profiles and rollout status are maintained in the [platform CI contract](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci.md). This guide owns the accelerator suite requirements and case intent.
