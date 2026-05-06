package rocks

import (
	"fmt"
	"strings"

	grocksdb "github.com/linxGnu/grocksdb"
)

// CFStats holds a snapshot of RocksDB properties for one column family.
type CFStats struct {
	Name        string
	NumKeys     string
	DiskUsage   string
	MemUsage    string
	Compactions string
	BlobStats   string
}

// getCFStats reads RocksDB property strings for the given column family.
func getCFStats(db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle, name string) CFStats {
	get := func(prop string) string {
		v := db.GetPropertyCF(prop, cf)
		return strings.TrimSpace(v)
	}
	return CFStats{
		Name:        name,
		NumKeys:     get("rocksdb.estimate-num-keys"),
		DiskUsage:   get("rocksdb.estimate-live-data-size"),
		MemUsage:    get("rocksdb.cur-size-all-mem-tables"),
		Compactions: get("rocksdb.num-running-compactions"),
		BlobStats:   get("rocksdb.blob-stats"),
	}
}

// String formats stats for human display.
func (s CFStats) String() string {
	return fmt.Sprintf("  %s:\n    estimate-num-keys:       %s\n    estimate-live-data-size: %s\n    cur-size-all-mem-tables: %s\n    num-running-compactions: %s\n    blob-stats:              %s",
		s.Name, s.NumKeys, s.DiskUsage, s.MemUsage, s.Compactions, s.BlobStats)
}

// AllStats returns stats for all column families in the store.
func (s *impl) AllStats() []CFStats {
	var result []CFStats
	for name, cf := range s.cfh {
		if name == cfDefault {
			continue
		}
		result = append(result, getCFStats(s.db, cf, name))
	}
	return result
}
