package rocks

import "fmt"

// CFStats holds a snapshot of RocksDB properties for one column family.
type CFStats struct {
	Name        string
	NumKeys     string
	DiskUsage   string
	MemUsage    string
	Compactions string
	BlobStats   string
}

// String formats stats for human display.
func (s CFStats) String() string {
	return fmt.Sprintf("  %s:\n    estimate-num-keys:       %s\n    estimate-live-data-size: %s\n    cur-size-all-mem-tables: %s\n    num-running-compactions: %s\n    blob-stats:              %s",
		s.Name, s.NumKeys, s.DiskUsage, s.MemUsage, s.Compactions, s.BlobStats)
}
