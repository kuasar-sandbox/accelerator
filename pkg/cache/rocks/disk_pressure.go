//go:build !no_rocksdb

package rocks

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"math"
	"path/filepath"
	"strings"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/util"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

const (
	defaultDiskHighWatermark      = 0.85
	defaultDiskLowWatermark       = 0.70
	defaultDiskMaintenancePeriod  = 30 * time.Second
	diskPressureCompactionCooloff = 5 * time.Minute
)

type diskPressureConfig struct {
	limit    uint64
	high     uint64
	low      uint64
	interval time.Duration
	enabled  bool
	now      func() time.Time
}

func parseDiskBytes(cfg runtime.RocksConfig) uint64 {
	diskBytes, _ := util.ParseSize(cfg.DiskBytes)
	if diskBytes == 0 {
		return 1 << 40
	}
	return diskBytes
}

func resolveDiskPressureConfig(cfg runtime.RocksConfig) diskPressureConfig {
	limit := parseDiskBytes(cfg)
	highRatio := cfg.DiskHighWatermark
	if highRatio <= 0 || highRatio > 1 {
		highRatio = defaultDiskHighWatermark
	}
	lowRatio := cfg.DiskLowWatermark
	if lowRatio <= 0 || lowRatio >= highRatio {
		lowRatio = defaultDiskLowWatermark
		if lowRatio >= highRatio {
			lowRatio = highRatio * 0.8
		}
	}
	p := diskPressureConfig{
		limit:    limit,
		high:     uint64(float64(limit) * highRatio),
		low:      uint64(float64(limit) * lowRatio),
		interval: defaultDiskMaintenancePeriod,
		enabled:  true,
	}
	if p.high == 0 {
		p.high = 1
	}
	raw := strings.TrimSpace(cfg.DiskMaintenanceInterval)
	if raw == "0" || strings.EqualFold(raw, "off") {
		p.enabled = false
		return p
	}
	if raw != "" {
		if interval, err := time.ParseDuration(raw); err == nil && interval > 0 {
			p.interval = interval
		}
	}
	return p
}

func diskUsageBytes(root string) (uint64, error) {
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	var total uint64
	err = filepath.WalkDir(resolved, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		size := info.Size()
		if size <= 0 {
			return nil
		}
		if total > math.MaxUint64-uint64(size) {
			total = math.MaxUint64
			return nil
		}
		total += uint64(size)
		return nil
	})
	return total, err
}

func (s *impl) startDiskMaintenance(p diskPressureConfig) {
	if !p.enabled || s.readOnly || s.path == "" {
		return
	}
	stop := make(chan struct{})
	s.maintenanceStop = stop
	s.maintenanceWG.Add(1)
	go func() {
		defer s.maintenanceWG.Done()
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				s.runDiskPressure(p, time.Now())
			}
		}
	}()
}

func (s *impl) stopDiskMaintenance() {
	if s.maintenanceStop == nil {
		return
	}
	close(s.maintenanceStop)
	s.maintenanceStop = nil
	s.maintenanceWG.Wait()
}

func (s *impl) runDiskPressure(p diskPressureConfig, now time.Time) {
	if !p.enabled || s.path == "" {
		return
	}
	usage, err := diskUsageBytes(s.path)
	if err != nil {
		log.Printf("rocks: disk usage scan failed: %v", err)
		return
	}
	if usage < p.high {
		s.pressureMu.Lock()
		if usage <= p.low {
			s.pressureNext = time.Time{}
		}
		s.pressureMu.Unlock()
		return
	}

	s.pressureMu.Lock()
	if now.Before(s.pressureNext) {
		s.pressureMu.Unlock()
		return
	}
	s.pressureMu.Unlock()

	s.mu.RLock()
	if s.closed || s.readOnly {
		s.mu.RUnlock()
		return
	}
	if s.sketch != nil {
		s.sketch.Reset()
		log.Printf("rocks: disk pressure %.1f%% >= high watermark; decayed sketch", percent(usage, p.limit))
	}
	for _, cfName := range [...]string{cfChunk, cfManifest, cfBlob} {
		cf, ok := s.cfh[cfName]
		if !ok {
			continue
		}
		if err := s.db.Compact(cf); err != nil {
			log.Printf("rocks: disk-pressure compaction %s failed: %v", cfName, err)
		}
	}
	s.mu.RUnlock()

	after, err := diskUsageBytes(s.path)
	if err != nil {
		after = usage
	}
	s.pressureMu.Lock()
	if after <= p.low {
		s.pressureNext = time.Time{}
	} else {
		completedAt := time.Now()
		if p.now != nil {
			completedAt = p.now()
		}
		if completedAt.Before(now) {
			completedAt = now
		}
		s.pressureNext = completedAt.Add(diskPressureCompactionCooloff)
	}
	s.pressureMu.Unlock()
	log.Printf("rocks: disk pressure compaction finished; usage %s -> %s (high %s)",
		formatBytes(usage), formatBytes(after), formatBytes(p.high))
}

func percent(value, limit uint64) float64 {
	if limit == 0 {
		return 0
	}
	return 100 * float64(value) / float64(limit)
}

func formatBytes(value uint64) string {
	const unit = 1024
	if value < unit {
		return fmt.Sprintf("%d B", value)
	}
	div, exp := uint64(unit), 0
	for n := value / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(value)/float64(div), "KMGTPE"[exp])
}
