//go:build !no_rocksdb

package rocks

import (
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	grocksdb "github.com/linxGnu/grocksdb"
)

func TestBuildOptionsRestoreLatencyKnobs(t *testing.T) {
	opts := BuildOptions(runtime.RocksConfig{})
	destroyOpenOptions(t, opts)

	const wantBuf = 256 << 20
	for name, o := range map[string]*grocksdb.Options{
		"db":       opts.DBOpts,
		"chunk":    opts.ChunkCF,
		"manifest": opts.ManifestCF,
		"blob":     opts.BlobCF,
	} {
		if g := o.GetWriteBufferSize(); g != wantBuf {
			t.Errorf("%s write_buffer_size=%d want %d", name, g, wantBuf)
		}
		if g := o.GetMaxWriteBufferNumber(); g != 4 {
			t.Errorf("%s max_write_buffer_number=%d want 4", name, g)
		}
	}
	for name, o := range map[string]*grocksdb.Options{
		"chunk":    opts.ChunkCF,
		"manifest": opts.ManifestCF,
		"blob":     opts.BlobCF,
	} {
		if g := o.GetPrepopulateBlobCache(); g != grocksdb.PrepopulateBlobFlushOnly {
			t.Errorf("%s prepopulate_blob_cache=%v want FlushOnly", name, g)
		}
	}
	if !opts.DBOpts.UseDirectReads() {
		t.Error("direct_reads default: UseDirectReads=false")
	}
	if !opts.DBOpts.UseDirectIOForFlushAndCompaction() {
		t.Error("direct_reads default: UseDirectIOForFlushAndCompaction=false")
	}

	off := false
	optsOff := BuildOptions(runtime.RocksConfig{DirectReads: &off})
	destroyOpenOptions(t, optsOff)
	if optsOff.DBOpts.UseDirectReads() {
		t.Error("direct_reads=false: UseDirectReads=true")
	}
	if optsOff.DBOpts.UseDirectIOForFlushAndCompaction() {
		t.Error("direct_reads=false: UseDirectIOForFlushAndCompaction=true")
	}

	optsWB := BuildOptions(runtime.RocksConfig{WriteBufferBytes: "32MiB"})
	destroyOpenOptions(t, optsWB)
	if g := optsWB.ChunkCF.GetWriteBufferSize(); g != 32<<20 {
		t.Errorf("chunk write_buffer_size=%d want 32MiB", g)
	}
}

func destroyOpenOptions(t *testing.T, opts *OpenOptions) {
	t.Helper()
	t.Cleanup(func() {
		opts.DBOpts.Destroy()
		opts.ChunkCF.Destroy()
		opts.ManifestCF.Destroy()
		opts.BlobCF.Destroy()
		opts.BBTOpts.Destroy()
	})
}
