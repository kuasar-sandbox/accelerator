package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"flag"
	"fmt"
	mrand "math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/client"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type stringListFlag []string

func (v *stringListFlag) String() string {
	return strings.Join(*v, ",")
}

func (v *stringListFlag) Set(value string) error {
	*v = append(*v, value)
	return nil
}

// selectKey chooses the next read key per the access pattern, optionally
// drawing a cold (L2-miss → origin/L3) key with probability missRatio. warm
// keys are prefilled AND warmed into the bench target (L2 hits); cold keys live
// only in origin, so reading one misses L2 and falls through to L3.
func selectKey(access string, warm, cold [][32]byte, missRatio float64, rng *mrand.Rand, zipf *mrand.Zipf, i int) [32]byte {
	if missRatio > 0 && len(cold) > 0 && rng.Float64() < missRatio {
		return cold[rng.Intn(len(cold))]
	}
	switch access {
	case "zipf":
		if zipf != nil {
			return warm[zipf.Uint64()]
		}
		return warm[i%len(warm)]
	case "uniform":
		return warm[rng.Intn(len(warm))]
	default: // seq
		return warm[i%len(warm)]
	}
}

// waitFirstTierWarm verifies the data-plane condition the benchmark needs: one
// complete pass over the working set is served by tier 0. Counter deltas are
// used instead of waiting for implementation goroutines, so nested tiers and
// backend retries cannot produce a false-ready result.
func waitFirstTierWarm(reader client.GetCloser, infoEndpoint string, p store.Partition, keys [][32]byte, timeout time.Duration) error {
	if len(keys) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		before, err := fetchInfo(infoEndpoint, 3*time.Second)
		if err != nil {
			return err
		}
		if before.Tiered == nil || len(before.Tiered.Tiers) == 0 {
			return fmt.Errorf("Info endpoint does not report a tiered cache")
		}
		for _, key := range keys {
			_, blob, err := reader.Get(ctx, p, key)
			if err != nil {
				return fmt.Errorf("warm verification get: %w", err)
			}
			if blob == nil {
				return fmt.Errorf("warm verification get: key %x not found", key)
			}
			blob.Release()
		}
		after, err := fetchInfo(infoEndpoint, 3*time.Second)
		if err != nil {
			return err
		}
		if after.Tiered == nil || len(after.Tiered.Tiers) == 0 {
			return fmt.Errorf("Info endpoint stopped reporting a tiered cache")
		}
		beforeHits := before.Tiered.Tiers[0].Hits
		afterHits := after.Tiered.Tiers[0].Hits
		if afterHits >= beforeHits && afterHits-beforeHits >= uint64(len(keys)) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("tier 0 did not serve a complete warm pass within %s", timeout)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// cmdBench runs a concurrency sweep + latency percentiles against a
// cache-ctl endpoint. See docs/cache.md for a full flag reference.
//
// --prefill-endpoint lets the operator split prefill writes and bench
// reads across two different cache-ctl instances, so the bench target
// can be a read-only tiered instance (which rejects ObjectPut) while
// prefill flows to an underlying local instance. When --prefill-endpoint
// is non-empty the bench is forced to --mode get — any other mode would
// try to write to the bench target and fail on the first frame.
func cmdBench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	endpoint := fs.String("endpoint", "", "cache-ctl data endpoint, bench target (overrides CACHE_ENDPOINT env)")
	prefillEndpoint := fs.String("prefill-endpoint", "", "separate prefill write endpoint (default: use --endpoint; non-empty implies --mode get)")
	var infoEndpoints stringListFlag
	fs.Var(&infoEndpoints, "info-endpoint", "Info gRPC endpoint (HealthListen), repeatable; the first endpoint is the bench target")
	concurrency := fs.Int("concurrency", 8, "number of parallel workers")
	duration := fs.Duration("duration", 10*time.Second, "benchmark duration")
	valueSize := fs.Int("value-size", 256*1024, "value size in bytes")
	mode := fs.String("mode", "", "benchmark mode: get|put|mixed (default: mixed, or get when --prefill-endpoint is set)")
	namespace := fs.String("namespace", "chunk", "namespace")
	prefill := fs.Int("prefill", 1000, "number of objects to prefill for get/mixed modes")
	cpuProfile := fs.String("cpu-profile", "", "write CPU profile to file (scoped to the bench window)")
	heapProfile := fs.String("heap-profile", "", "write heap profile to file after the bench window")
	traceFile := fs.String("trace", "", "write execution trace to file (scoped to the bench window)")
	access := fs.String("access", "seq", "get/mixed read key access pattern: seq|uniform|zipf (zipf models hot base-layer skew)")
	zipfS := fs.Float64("zipf-s", 1.1, "Zipf skew exponent s (>1; higher = more skew) when --access zipf")
	coldPrefill := fs.Int("cold-prefill", 0, "extra keys prefilled to --prefill-endpoint ONLY (not warmed into the bench target) — L2-miss → origin/L3 targets")
	missRatio := fs.Float64("miss-ratio", 0, "fraction of reads that hit cold keys → L2 miss → origin/L3 (needs --cold-prefill>0 and --prefill-endpoint)")
	timeout := fs.Duration("timeout", 10*time.Second, "per-op client TCP deadline; raise for a slow/stalling origin during large prefills (a sustained 512KiB write burst can stall RocksDB past 10s)")
	fs.Parse(args)

	// Resolve --mode default based on whether a separate prefill
	// endpoint was supplied. Empty --prefill-endpoint → mixed (today's
	// behaviour). Non-empty → get only; fatal if the user explicitly
	// asked for put/mixed against a separate target.
	if *prefillEndpoint != "" {
		if *mode == "" || *mode == "mixed" {
			*mode = "get"
		}
		if *mode != "get" {
			fatal("--prefill-endpoint is incompatible with --mode %s; only get is supported when prefilling against a separate endpoint", *mode)
		}
	} else if *mode == "" {
		*mode = "mixed"
	}

	if *access != "seq" && *access != "uniform" && *access != "zipf" {
		fatal("--access must be seq|uniform|zipf")
	}
	if *access == "zipf" && *zipfS <= 1.0 {
		fatal("--zipf-s must be > 1 for --access zipf")
	}
	if *missRatio < 0 || *missRatio > 1 {
		fatal("--miss-ratio must be in [0,1]")
	}
	if *missRatio > 0 {
		if *coldPrefill <= 0 {
			fatal("--miss-ratio requires --cold-prefill > 0")
		}
		if *prefillEndpoint == "" {
			fatal("--miss-ratio requires --prefill-endpoint (cold keys must live only in origin)")
		}
	}

	// Resolve the bench target endpoint with env fallback. prefill /
	// info endpoints stay flag-only — they target different daemons
	// and shouldn't accidentally inherit CACHE_ENDPOINT.
	resolvedEP := resolveDataEndpoint(*endpoint)
	if resolvedEP == "" {
		fatal("--endpoint or CACHE_ENDPOINT required")
	}
	*endpoint = resolvedEP

	// Use a process-global BlobPool for ReadResponse payload buffers.
	// Without this, every Get allocates a fresh `value-size` slice via
	// cache.DefaultPool → GC pressure dominates the bench client's
	// CPU (≈20% GC, ≈50% in Read→ReadResponse during baseline).
	//
	// Seed size = valueSize so Alloc(valueSize) finds cap>=size in the
	// pool and reuses instead of falling through to the "oversized"
	// fresh-make path (which would defeat pooling entirely).
	benchBlobPool := cache.NewPool(*valueSize)
	opts := client.Options{Pool: *concurrency, Timeout: *timeout, BlobPool: benchBlobPool}

	// Prefill client: writes land here. Falls back to the bench
	// endpoint when --prefill-endpoint is empty, preserving the
	// original single-endpoint behaviour.
	prefillEP := *prefillEndpoint
	if prefillEP == "" {
		prefillEP = *endpoint
	}
	prefillClient, err := client.New(prefillEP, opts)
	if err != nil {
		fatal("dial prefill: %v", err)
	}
	defer prefillClient.Close()

	// Bench reader: reads come from the bench target endpoint.
	var benchReader client.GetCloser
	if *prefillEndpoint == "" {
		// Same endpoint as prefill — share the connection pool.
		benchReader = prefillClient
	} else {
		benchReader, err = client.NewGetter(*endpoint, opts)
		if err != nil {
			fatal("dial bench reader: %v", err)
		}
		defer benchReader.Close()
	}

	// Bench writer: only created when prefill and bench target share
	// the same endpoint, i.e. the bench target accepts writes (local
	// mode). When --prefill-endpoint is set this stays nil because the
	// bench target is assumed to reject writes.
	var benchWriter client.TierCloser
	if *prefillEndpoint == "" {
		benchWriter = prefillClient
	}

	// Prefill.
	//
	// When prefill and bench endpoints differ (e.g. testing a tiered
	// target against a separate origin), the bench target's L1 must
	// also be warmed up — otherwise the first bench rounds all miss
	// L1 and fall through to origin, skewing latency percentiles.
	// The prefill pass is therefore two-phase in split-endpoint mode:
	//   1. Fill each key to the prefill endpoint (the origin).
	//   2. Get each key from the bench endpoint so the tiered target
	//      fetches from origin and caches the result in its L1.
	value := make([]byte, *valueSize)
	rand.Read(value)
	keys := make([][32]byte, *prefill)
	coldKeys := make([][32]byte, *coldPrefill)
	if *mode != "put" {
		fmt.Fprintf(os.Stderr, "Prefilling %d objects (%d bytes each) to %s ...\n", *prefill, *valueSize, prefillEP)
		for i := range keys {
			keys[i] = sha256.Sum256(fmt.Appendf(nil, "bench-key-%d", i))
			if err := prefillClient.Fill(context.Background(), store.Partition(*namespace), keys[i], value); err != nil {
				fatal("prefill put: %v", err)
			}
		}
		if *coldPrefill > 0 {
			fmt.Fprintf(os.Stderr, "Prefilling %d cold objects to %s (origin only — L2-miss targets) ...\n", *coldPrefill, prefillEP)
			for i := range coldKeys {
				coldKeys[i] = sha256.Sum256(fmt.Appendf(nil, "bench-cold-%d", i))
				if err := prefillClient.Fill(context.Background(), store.Partition(*namespace), coldKeys[i], value); err != nil {
					fatal("cold prefill put: %v", err)
				}
			}
		}
		if *prefillEndpoint != "" {
			fmt.Fprintf(os.Stderr, "Warming bench target %s L1 via Get ...\n", *endpoint)
			for i := range keys {
				_, blob, err := benchReader.Get(context.Background(), store.Partition(*namespace), keys[i])
				if err != nil {
					fatal("prefill warm get: %v", err)
				}
				if blob == nil {
					fatal("prefill warm get: key %x not found on bench target after fill to prefill endpoint", keys[i])
				}
				blob.Release()
			}
			// Verify the observable condition needed by the measurement: every
			// working-set key can complete a full pass from tier 0.
			if len(infoEndpoints) > 0 {
				if err := waitFirstTierWarm(benchReader, infoEndpoints[0], store.Partition(*namespace), keys, 30*time.Second); err != nil {
					fatal("bench target did not become warm: %v", err)
				}
			}
		}
		fmt.Fprintln(os.Stderr, "Prefill done.")
	}

	// Baseline counter snapshot. Captured after prefill and warm verification
	// so the bench window excludes the Put/Get+backfill storm that
	// populated the working set. Each endpoint soft-fails independently, so one
	// unavailable shard does not prevent the benchmark itself from running.
	baselines := make([]cache.DaemonStats, len(infoEndpoints))
	haveBaseline := make([]bool, len(infoEndpoints))
	for i, endpoint := range infoEndpoints {
		b, err := fetchInfo(endpoint, 3*time.Second)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: baseline info fetch %s failed: %v\n", endpoint, err)
			continue
		}
		baselines[i] = b
		haveBaseline[i] = true
	}

	// Benchmark.
	var (
		mu       sync.Mutex
		totalOps int64
		errors   int64
		samples  []int64
	)

	// Start CPU / trace profiles scoped to the bench window. Heap snapshot
	// is taken after workers finish (steady-state). All three are best-
	// effort: warn on failure, continue the bench.
	var cpuFile, traceF *os.File
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: cpu-profile create %s: %v\n", *cpuProfile, err)
		} else if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "warn: StartCPUProfile: %v\n", err)
			f.Close()
		} else {
			cpuFile = f
		}
	}
	if *traceFile != "" {
		f, err := os.Create(*traceFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: trace create %s: %v\n", *traceFile, err)
		} else if err := trace.Start(f); err != nil {
			fmt.Fprintf(os.Stderr, "warn: trace.Start: %v\n", err)
			f.Close()
		} else {
			traceF = f
		}
	}

	// MemStats delta: capture before the bench window opens so worker
	// allocations are attributable. runtime.GC forces any deferred sweep
	// so Mallocs/TotalAlloc deltas reflect real request work.
	runtime.GC()
	var mstatsBefore runtime.MemStats
	runtime.ReadMemStats(&mstatsBefore)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localSamples := make([]int64, 0, 10000)
			var localOps, localErrors int64
			i := 0
			rng := mrand.New(mrand.NewSource(int64(workerID) + 1))
			var zipf *mrand.Zipf
			if *access == "zipf" && len(keys) > 1 {
				zipf = mrand.NewZipf(rng, *zipfS, 1.0, uint64(len(keys)-1))
			}
			for {
				select {
				case <-ctx.Done():
					mu.Lock()
					totalOps += localOps
					errors += localErrors
					samples = append(samples, localSamples...)
					mu.Unlock()
					return
				default:
				}

				start := time.Now()
				var opErr error

				switch *mode {
				case "get":
					k := selectKey(*access, keys, coldKeys, *missRatio, rng, zipf, i)
					_, blob, err := benchReader.Get(ctx, store.Partition(*namespace), k)
					if err == nil && blob != nil {
						blob.Release()
					}
					opErr = err
				case "put":
					if benchWriter == nil {
						fatal("--mode put requires bench target to accept writes")
					}
					k := sha256.Sum256(fmt.Appendf(nil, "bench-put-%d-%d", workerID, i))
					opErr = benchWriter.Fill(ctx, store.Partition(*namespace), k, value)
				case "mixed":
					if benchWriter == nil {
						fatal("--mode mixed requires bench target to accept writes")
					}
					if i%2 == 0 {
						k := selectKey(*access, keys, coldKeys, *missRatio, rng, zipf, i)
						_, blob, err := benchReader.Get(ctx, store.Partition(*namespace), k)
						if err == nil && blob != nil {
							blob.Release()
						}
						opErr = err
					} else {
						k := sha256.Sum256(fmt.Appendf(nil, "bench-put-%d-%d", workerID, i))
						opErr = benchWriter.Fill(ctx, store.Partition(*namespace), k, value)
					}
				}

				elapsed := time.Since(start).Nanoseconds()
				if opErr != nil {
					localErrors++
				} else {
					localSamples = append(localSamples, elapsed)
					localOps++
				}
				i++
			}
		}(w)
	}

	wg.Wait()

	// Close bench-window profiles before MemStats delta so the pprof
	// goroutine accounting is flushed.
	if cpuFile != nil {
		pprof.StopCPUProfile()
		cpuFile.Close()
	}
	if traceF != nil {
		trace.Stop()
		traceF.Close()
	}

	// MemStats delta — capture right after workers finish so the window
	// covers only the bench workload.
	var mstatsAfter runtime.MemStats
	runtime.ReadMemStats(&mstatsAfter)

	// Heap profile after GC for accurate in-use snapshot.
	if *heapProfile != "" {
		runtime.GC()
		f, err := os.Create(*heapProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: heap-profile create %s: %v\n", *heapProfile, err)
		} else {
			if err := pprof.WriteHeapProfile(f); err != nil {
				fmt.Fprintf(os.Stderr, "warn: WriteHeapProfile: %v\n", err)
			}
			f.Close()
		}
	}

	// Final counter snapshot. Fill counters increment when work is initiated, so
	// the delta is independent of whether a backend write is still completing.
	finals := make([]cache.DaemonStats, len(infoEndpoints))
	haveFinal := make([]bool, len(infoEndpoints))
	for i, endpoint := range infoEndpoints {
		if !haveBaseline[i] {
			continue
		}
		f, err := fetchInfo(endpoint, 3*time.Second)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: final info fetch %s failed: %v\n", endpoint, err)
			continue
		}
		finals[i] = f
		haveFinal[i] = true
	}

	// Compute percentiles.
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p50, p99, p999 := int64(0), int64(0), int64(0)
	if n := len(samples); n > 0 {
		p50 = samples[n*50/100] / 1000
		p99 = samples[n*99/100] / 1000
		idx := n * 999 / 1000
		if idx >= n {
			idx = n - 1
		}
		p999 = samples[idx] / 1000
	}

	throughput := float64(totalOps) / duration.Seconds()
	bandwidth := throughput * float64(*valueSize) / (1024 * 1024)

	fmt.Printf("mode=%s concurrency=%d duration=%s value_size=%d\n", *mode, *concurrency, *duration, *valueSize)
	fmt.Printf("  endpoint:   %s\n", *endpoint)
	if *prefillEndpoint != "" {
		fmt.Printf("  prefill_endpoint: %s\n", *prefillEndpoint)
	}
	fmt.Printf("  ops:        %d\n", totalOps)
	fmt.Printf("  throughput: %.0f ops/sec\n", throughput)
	fmt.Printf("  bandwidth:  %.1f MiB/sec\n", bandwidth)
	fmt.Printf("  latency:\n")
	fmt.Printf("    p50:   %d us\n", p50)
	fmt.Printf("    p99:   %d us\n", p99)
	fmt.Printf("    p99.9: %d us\n", p999)
	fmt.Printf("  errors:    %d\n", errors)

	// MemStats delta over the bench window — alloc pressure is the
	// single most informative per-op diagnostic after latency.
	if totalOps > 0 {
		mallocs := mstatsAfter.Mallocs - mstatsBefore.Mallocs
		bytesAllocated := mstatsAfter.TotalAlloc - mstatsBefore.TotalAlloc
		pauseNs := mstatsAfter.PauseTotalNs - mstatsBefore.PauseTotalNs
		gcCount := mstatsAfter.NumGC - mstatsBefore.NumGC
		fmt.Printf("  allocs:\n")
		fmt.Printf("    allocs/op:    %d\n", mallocs/uint64(totalOps))
		fmt.Printf("    bytes/op:     %d\n", bytesAllocated/uint64(totalOps))
		fmt.Printf("    gc-count:     %d\n", gcCount)
		fmt.Printf("    gc-pause-ms:  %d\n", pauseNs/1_000_000)
	}

	for i, endpoint := range infoEndpoints {
		if !haveFinal[i] {
			continue
		}
		if len(infoEndpoints) > 1 {
			fmt.Printf("  info endpoint: %s\n", endpoint)
		}
		printBenchWindow(diffCounters(baselines[i], finals[i]))
	}
}

// diffCounters subtracts baseline from final for every counter field
// and returns a new DaemonStats with only delta values populated in
// the counter fields. Non-counter fields (Mode, Type, Endpoint, etc.)
// come from `after` so the printer has the right labels. Rocks is
// intentionally dropped — RocksDB CF properties are absolute snapshots
// of current storage state, not deltas, so printing them in a bench
// window is misleading.
func diffCounters(before, after cache.DaemonStats) cache.DaemonStats {
	out := cache.DaemonStats{
		Mode:        after.Mode,
		BackendType: after.BackendType,
		UptimeSec:   after.UptimeSec,
		Server: cache.ServerStats{
			Hits:   after.Server.Hits - before.Server.Hits,
			Misses: after.Server.Misses - before.Server.Misses,
			Fills:  after.Server.Fills - before.Server.Fills,
		},
		Redis: diffRedisCounters(before.Redis, after.Redis),
	}
	if after.Tiered != nil {
		tiers := make([]cache.TierStats, len(after.Tiered.Tiers))
		for i, t := range after.Tiered.Tiers {
			d := cache.TierStats{
				Type:     t.Type,
				Endpoint: t.Endpoint,
				Hits:     t.Hits,
				Misses:   t.Misses,
				Fills:    t.Fills,
				Errors:   t.Errors,
				Redis:    diffRedisCounters(nil, t.Redis),
			}
			// Subtract matching tier from baseline if present.
			if before.Tiered != nil && i < len(before.Tiered.Tiers) {
				b := before.Tiered.Tiers[i]
				d.Hits -= b.Hits
				d.Misses -= b.Misses
				d.Fills -= b.Fills
				d.Errors -= b.Errors
				d.Redis = diffRedisCounters(b.Redis, t.Redis)
			}
			if len(t.Peers) > 0 {
				d.Peers = make([]cache.PeerStats, len(t.Peers))
				for j, p := range t.Peers {
					pd := cache.PeerStats{
						ID:        p.ID,
						Endpoint:  p.Endpoint,
						Hits:      p.Hits,
						Misses:    p.Misses,
						Errors:    p.Errors,
						Cancelled: p.Cancelled,
						Fills:     p.Fills,
					}
					if before.Tiered != nil && i < len(before.Tiered.Tiers) &&
						j < len(before.Tiered.Tiers[i].Peers) {
						bp := before.Tiered.Tiers[i].Peers[j]
						pd.Hits -= bp.Hits
						pd.Misses -= bp.Misses
						pd.Errors -= bp.Errors
						pd.Cancelled -= bp.Cancelled
						pd.Fills -= bp.Fills
					}
					d.Peers[j] = pd
				}
			}
			tiers[i] = d
		}
		origin := cache.OriginStats{
			Type:      after.Tiered.Origin.Type,
			StorePath: after.Tiered.Origin.StorePath,
			Endpoint:  after.Tiered.Origin.Endpoint,
			Hits:      after.Tiered.Origin.Hits,
			Misses:    after.Tiered.Origin.Misses,
			Errors:    after.Tiered.Origin.Errors,
		}
		if before.Tiered != nil {
			origin.Hits -= before.Tiered.Origin.Hits
			origin.Misses -= before.Tiered.Origin.Misses
			origin.Errors -= before.Tiered.Origin.Errors
		}
		out.Tiered = &cache.TieredStats{Tiers: tiers, Origin: origin}
	}
	return out
}

// diffRedisCounters keeps endpoint and final pool state while subtracting only
// monotonic counters. Redis latency percentiles are cumulative histograms, so
// they are intentionally omitted from a benchmark-window delta.
func diffRedisCounters(before, after *cache.RedisStats) *cache.RedisStats {
	if after == nil {
		return nil
	}
	out := &cache.RedisStats{
		Endpoint:         after.Endpoint,
		Transport:        after.Transport,
		GetPoolSize:      after.GetPoolSize,
		SetPoolSize:      after.SetPoolSize,
		GetConnected:     after.GetConnected,
		SetConnected:     after.SetConnected,
		GetInflight:      after.GetInflight,
		SetInflight:      after.SetInflight,
		PoolWaiters:      after.PoolWaiters,
		Draining:         after.Draining,
		GetHits:          after.GetHits,
		GetMisses:        after.GetMisses,
		Sets:             after.Sets,
		Cancelled:        after.Cancelled,
		LateBytesDrained: after.LateBytesDrained,
		Reconnects:       after.Reconnects,
		ProtocolErrors:   after.ProtocolErrors,
		BackendErrors:    after.BackendErrors,
	}
	if before != nil {
		out.GetHits -= before.GetHits
		out.GetMisses -= before.GetMisses
		out.Sets -= before.Sets
		out.Cancelled -= before.Cancelled
		out.LateBytesDrained -= before.LateBytesDrained
		out.Reconnects -= before.Reconnects
		out.ProtocolErrors -= before.ProtocolErrors
		out.BackendErrors -= before.BackendErrors
	}
	return out
}

// printBenchWindow renders the delta as a compact block appended to
// the bench summary. Same layout as info.go's printInfoHuman but with
// a "bench-window" header and without the rocks section.
func printBenchWindow(d cache.DaemonStats) {
	fmt.Printf("  ── bench-window counters ──\n")
	srvTot := d.Server.Hits + d.Server.Misses
	srvHR := 0.0
	if srvTot > 0 {
		srvHR = 100.0 * float64(d.Server.Hits) / float64(srvTot)
	}
	fmt.Printf("  server: hits=%d misses=%d fills=%d hit%%=%.2f\n",
		d.Server.Hits, d.Server.Misses, d.Server.Fills, srvHR)

	if d.Redis != nil {
		printRedisBenchWindow("redis", d.Redis)
	}
	if d.Tiered == nil {
		return
	}
	fmt.Printf("  %-12s %-10s %10s %10s %10s %10s %8s\n",
		"layer", "type", "hits", "misses", "fills", "errors", "hit%")
	for i, t := range d.Tiered.Tiers {
		tot := t.Hits + t.Misses
		hr := 0.0
		if tot > 0 {
			hr = 100.0 * float64(t.Hits) / float64(tot)
		}
		fmt.Printf("  tier-%-7d %-10s %10d %10d %10d %10d %7.2f%%\n",
			i, t.Type, t.Hits, t.Misses, t.Fills, t.Errors, hr)
	}
	o := d.Tiered.Origin
	otot := o.Hits + o.Misses
	ohr := 0.0
	if otot > 0 {
		ohr = 100.0 * float64(o.Hits) / float64(otot)
	}
	fmt.Printf("  %-12s %-10s %10d %10d %10s %10d %7.2f%%\n",
		"origin", o.Type, o.Hits, o.Misses, "-", o.Errors, ohr)

	for _, t := range d.Tiered.Tiers {
		if t.Redis != nil {
			printRedisBenchWindow("redis tier", t.Redis)
		}
		if len(t.Peers) == 0 {
			continue
		}
		fmt.Println("  ec peers:")
		for _, p := range t.Peers {
			ptot := p.Hits + p.Misses
			phr := 0.0
			if ptot > 0 {
				phr = 100.0 * float64(p.Hits) / float64(ptot)
			}
			fmt.Printf("    %-10s %-22s hits=%d misses=%d errors=%d cancelled=%d fills=%d hit%%=%.2f\n",
				p.ID, p.Endpoint, p.Hits, p.Misses, p.Errors, p.Cancelled, p.Fills, phr)
		}
		break
	}
}

func printRedisBenchWindow(label string, s *cache.RedisStats) {
	fmt.Printf("  %s: endpoint=%s transport=%s get-hits=%d get-misses=%d sets=%d cancels=%d late-bytes=%d reconnects=%d backend-errors=%d protocol-errors=%d end-inflight=%d/%d end-waiters=%d end-draining=%d\n",
		label, s.Endpoint, s.Transport, s.GetHits, s.GetMisses, s.Sets,
		s.Cancelled, s.LateBytesDrained, s.Reconnects, s.BackendErrors,
		s.ProtocolErrors, s.GetInflight, s.SetInflight, s.PoolWaiters, s.Draining)
}
