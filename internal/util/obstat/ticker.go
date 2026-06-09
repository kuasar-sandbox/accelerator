package obstat

import (
	"context"
	"time"
)

// RunAdaptive drives an adaptive periodic stats printer modelled on sandbox-ctl:
// it polls fast (min(base,2s)) at first to catch startup bursts, backs off to
// base once idle, and emits a line ONLY for windows the sample reports active —
// staying silent when there is no traffic. base <= 0 disables it (returns).
//
// sample is called once per tick with the window's elapsed seconds; it diffs
// its own cumulative counters against the previous call and returns the
// formatted line plus whether the window had activity. A window reported
// inactive (or an empty line) prints nothing. logf receives each line
// (typically a stderr logger). RunAdaptive returns when ctx is cancelled.
func RunAdaptive(ctx context.Context, base time.Duration,
	sample func(elapsedSec float64) (line string, active bool),
	logf func(string, ...any)) {
	if base <= 0 {
		return
	}
	fast := min(base, 2*time.Second)
	const idleBackoff = 2 // consecutive idle polls before slowing to base
	sleep := fast
	idle := 0
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
		now := time.Now()
		elapsed := now.Sub(last).Seconds()
		if elapsed <= 0 {
			elapsed = sleep.Seconds()
		}
		last = now

		line, active := sample(elapsed)
		if !active {
			if idle++; idle >= idleBackoff {
				sleep = base
			}
			continue
		}
		idle, sleep = 0, fast
		if line != "" {
			logf("%s", line)
		}
	}
}
