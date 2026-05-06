package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/fullof-work/mass-sandbox/pkg/cache"
	cachepb "github.com/fullof-work/mass-sandbox/pkg/cache/pb"
	"github.com/fullof-work/mass-sandbox/pkg/cache/rocks"
	"github.com/fullof-work/mass-sandbox/pkg/cache/runtime"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// cmdInfo dispatches to remote gRPC (cache.v1.Info.Get against a
// running daemon on its HealthListen port) or offline rocks inspection
// (OpenReadOnly against a rocksdb directory). Exactly one of
// --endpoint / --rocks-path must be provided; both-or-neither is an
// error.
func cmdInfo(args []string) {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	endpoint := fs.String("endpoint", "",
		"gRPC Info endpoint (same port as HealthListen, e.g. 127.0.0.1:7071)")
	rocksPath := fs.String("rocks-path", "",
		"Offline: open rocksdb read-only at this path")
	asJSON := fs.Bool("json", false,
		"output raw JSON (default: human-readable table)")
	fs.Parse(args)

	switch {
	case *endpoint != "" && *rocksPath != "":
		fatal("--endpoint and --rocks-path are mutually exclusive")
	case *endpoint != "":
		cmdInfoRemote(*endpoint, *asJSON)
	case *rocksPath != "":
		cmdInfoLocal(*rocksPath)
	default:
		fatal("must specify --endpoint or --rocks-path")
	}
}

// cmdInfoRemote dials the Info service and prints the snapshot.
func cmdInfoRemote(endpoint string, asJSON bool) {
	ds, err := fetchInfo(endpoint, 5*time.Second)
	if err != nil {
		fatal("%v", err)
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(ds); err != nil {
			fatal("encode: %v", err)
		}
		return
	}
	printInfoHuman(ds)
}

// fetchInfo dials the Info service at endpoint and returns a
// point-in-time snapshot. Returns (zero, error) on any failure so
// callers can decide whether to soft-fail (bench) or fatal (info).
func fetchInfo(endpoint string, timeout time.Duration) (cache.DaemonStats, error) {
	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return cache.DaemonStats{}, fmt.Errorf("dial %s: %w", endpoint, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	reply, err := cachepb.NewInfoClient(conn).Get(ctx, &cachepb.InfoRequest{})
	if err != nil {
		return cache.DaemonStats{}, fmt.Errorf("Info.Get: %w", err)
	}
	return fromPb(reply), nil
}

// waitFills blocks until the daemon's TieredCache has drained all
// in-flight fill goroutines, or timeout fires. Non-tiered modes return
// immediately server-side. Safe to call on daemons where Info isn't
// reachable — returns err, caller decides.
func waitFills(endpoint string, timeout time.Duration) error {
	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", endpoint, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err = cachepb.NewInfoClient(conn).WaitFills(ctx, &cachepb.WaitFillsRequest{})
	if err != nil {
		return fmt.Errorf("Info.WaitFills: %w", err)
	}
	return nil
}

// cmdInfoLocal opens rocksdb read-only and prints CF properties. This
// mode is intended for post-mortem inspection — it works even when no
// daemon is running against the path.
func cmdInfoLocal(path string) {
	s, err := rocks.OpenReadOnly(runtime.RocksConfig{Path: path})
	if err != nil {
		fatal("open: %v", err)
	}
	defer s.Close()

	fmt.Fprintf(os.Stdout, "RocksDB: %s\n", path)
	for _, st := range s.AllStats() {
		fmt.Fprintln(os.Stdout, st.String())
	}
}

// fromPb is the dual of server.toPb — converts the wire proto back to
// the pure Go type so the CLI can apply JSON tags / human formatting
// without touching protobuf internals.
func fromPb(r *cachepb.InfoReply) cache.DaemonStats {
	if r == nil {
		return cache.DaemonStats{}
	}
	ds := cache.DaemonStats{
		Mode:      r.Mode,
		UptimeSec: r.UptimeSec,
	}
	if r.Server != nil {
		ds.Server = cache.ServerStats{
			Hits:   r.Server.Hits,
			Misses: r.Server.Misses,
			Fills:  r.Server.Fills,
		}
	}
	if r.Tiered != nil {
		tiers := make([]cache.TierStats, len(r.Tiered.Tiers))
		for i, t := range r.Tiered.Tiers {
			tiers[i] = cache.TierStats{
				Type:     t.Type,
				Hits:     t.Hits,
				Misses:   t.Misses,
				Fills:    t.Fills,
				Errors:   t.Errors,
				Rocks:    rocksFromPb(t.Rocks),
				Peers:    peersFromPb(t.Peers),
				Endpoint: t.Endpoint,
			}
		}
		origin := cache.OriginStats{}
		if r.Tiered.Origin != nil {
			origin = cache.OriginStats{
				Type:      r.Tiered.Origin.Type,
				Hits:      r.Tiered.Origin.Hits,
				Misses:    r.Tiered.Origin.Misses,
				Errors:    r.Tiered.Origin.Errors,
				StorePath: r.Tiered.Origin.StorePath,
				Endpoint:  r.Tiered.Origin.Endpoint,
			}
		}
		ds.Tiered = &cache.TieredStats{Tiers: tiers, Origin: origin}
	}
	if len(r.Rocks) > 0 {
		ds.Rocks = rocksFromPb(r.Rocks)
	}
	return ds
}

func rocksFromPb(in []*cachepb.RocksCFStats) []cache.RocksCFStats {
	out := make([]cache.RocksCFStats, len(in))
	for i, s := range in {
		if s == nil {
			continue
		}
		out[i] = cache.RocksCFStats{
			Name:      s.Name,
			NumKeys:   s.NumKeys,
			DiskUsage: s.DiskUsage,
			MemUsage:  s.MemUsage,
		}
	}
	return out
}

func peersFromPb(in []*cachepb.PeerStats) []cache.PeerStats {
	out := make([]cache.PeerStats, len(in))
	for i, p := range in {
		if p == nil {
			continue
		}
		out[i] = cache.PeerStats{
			ID:        p.Id,
			Endpoint:  p.Endpoint,
			Hits:      p.Hits,
			Misses:    p.Misses,
			Errors:    p.Errors,
			Cancelled: p.Cancelled,
			Fills:     p.Fills,
		}
	}
	return out
}

// printInfoHuman renders a terse human-readable table — 3 sections,
// only the ones that apply to the current mode. Designed to be
// inspected quickly during bench runs.
func printInfoHuman(ds cache.DaemonStats) {
	fmt.Printf("mode=%s uptime=%ds\n", ds.Mode, ds.UptimeSec)
	// Server (wire-handler) counters — always present.
	srvTot := ds.Server.Hits + ds.Server.Misses
	srvHR := 0.0
	if srvTot > 0 {
		srvHR = 100.0 * float64(ds.Server.Hits) / float64(srvTot)
	}
	fmt.Printf("server: hits=%d misses=%d fills=%d hit%%=%.2f\n",
		ds.Server.Hits, ds.Server.Misses, ds.Server.Fills, srvHR)

	if ds.Tiered != nil {
		fmt.Println("tiers:")
		fmt.Printf("  %-14s %-12s %10s %10s %10s %10s %8s\n",
			"layer", "type", "hits", "misses", "fills", "errors", "hit%")
		for i, t := range ds.Tiered.Tiers {
			tot := t.Hits + t.Misses
			hr := 0.0
			if tot > 0 {
				hr = 100.0 * float64(t.Hits) / float64(tot)
			}
			fmt.Printf("  tier-%-9d %-12s %10d %10d %10d %10d %7.2f%%\n",
				i, t.Type, t.Hits, t.Misses, t.Fills, t.Errors, hr)
		}
		o := ds.Tiered.Origin
		otot := o.Hits + o.Misses
		ohr := 0.0
		if otot > 0 {
			ohr = 100.0 * float64(o.Hits) / float64(otot)
		}
		fmt.Printf("  %-14s %-12s %10d %10d %10s %10d %7.2f%%\n",
			"origin", o.Type, o.Hits, o.Misses, "-", o.Errors, ohr)

		for _, t := range ds.Tiered.Tiers {
			if len(t.Peers) == 0 {
				continue
			}
			fmt.Println("ec peers:")
			for _, p := range t.Peers {
				ptot := p.Hits + p.Misses
				phr := 0.0
				if ptot > 0 {
					phr = 100.0 * float64(p.Hits) / float64(ptot)
				}
				fmt.Printf("  %-10s %-20s hits=%d misses=%d errors=%d cancelled=%d fills=%d hit%%=%.2f\n",
					p.ID, p.Endpoint, p.Hits, p.Misses, p.Errors, p.Cancelled, p.Fills, phr)
			}
			break
		}
	}

	if len(ds.Rocks) > 0 {
		fmt.Println("rocks:")
		for _, cf := range ds.Rocks {
			fmt.Printf("  %-12s keys=%s disk=%s mem=%s\n",
				cf.Name, cf.NumKeys, cf.DiskUsage, cf.MemUsage)
		}
	}
}
