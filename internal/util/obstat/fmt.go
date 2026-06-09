package obstat

import "fmt"

// FmtNs renders a nanosecond duration in a compact human unit (ns/µs/ms/s).
func FmtNs(ns uint64) string {
	switch {
	case ns < 1_000:
		return fmt.Sprintf("%dns", ns)
	case ns < 1_000_000:
		return fmt.Sprintf("%.0fµs", float64(ns)/1e3)
	case ns < 1_000_000_000:
		return fmt.Sprintf("%.1fms", float64(ns)/1e6)
	default:
		return fmt.Sprintf("%.2fs", float64(ns)/1e9)
	}
}

// FmtCount renders a rate or count compactly (950, 1.2k, 3.4M).
func FmtCount(v float64) string {
	switch {
	case v < 1_000:
		return fmt.Sprintf("%.0f", v)
	case v < 1_000_000:
		return fmt.Sprintf("%.1fk", v/1e3)
	default:
		return fmt.Sprintf("%.1fM", v/1e6)
	}
}

// FmtBytesPerSec renders a byte rate with binary units (B/KiB/MiB/GiB per s).
func FmtBytesPerSec(bps float64) string {
	const k = 1024.0
	switch {
	case bps < k:
		return fmt.Sprintf("%.0fB/s", bps)
	case bps < k*k:
		return fmt.Sprintf("%.0fKiB/s", bps/k)
	case bps < k*k*k:
		return fmt.Sprintf("%.0fMiB/s", bps/(k*k))
	default:
		return fmt.Sprintf("%.2fGiB/s", bps/(k*k*k))
	}
}
