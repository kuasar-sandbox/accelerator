//go:build ignore

package main

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

type entryContract struct {
	typeflag byte
	mode     int64
}

var archiveContract = map[string]entryContract{
	"./":                                   {typeflag: tar.TypeDir, mode: 0o755},
	"./bin/":                               {typeflag: tar.TypeDir, mode: 0o755},
	"./test/":                              {typeflag: tar.TypeDir, mode: 0o755},
	"./test/scripts/":                      {typeflag: tar.TypeDir, mode: 0o755},
	"./bin/manifest-ctl":                   {typeflag: tar.TypeReg, mode: 0o755},
	"./bin/store-ctl":                      {typeflag: tar.TypeReg, mode: 0o755},
	"./bin/cache-ctl":                      {typeflag: tar.TypeReg, mode: 0o755},
	"./test/scripts/bench_cache.sh":        {typeflag: tar.TypeReg, mode: 0o755},
	"./test/scripts/bench_cache_remote.sh": {typeflag: tar.TypeReg, mode: 0o755},
	"./test/scripts/dedup_report.sh":       {typeflag: tar.TypeReg, mode: 0o755},
	"./test/scripts/procmon.sh":            {typeflag: tar.TypeReg, mode: 0o755},
	"./test/scripts/proc_analyze.py":       {typeflag: tar.TypeReg, mode: 0o755},
	"./share/licenses/accelerator/project/LICENSE":  {typeflag: tar.TypeReg, mode: 0o644},
	"./share/licenses/accelerator/rocksdb/LICENSE":  {typeflag: tar.TypeReg, mode: 0o644},
	"./share/sources/accelerator/GO-BUILD-INFO.tsv": {typeflag: tar.TypeReg, mode: 0o644},
	"./share/sources/accelerator/GO-MODULES.tsv":    {typeflag: tar.TypeReg, mode: 0o644},
	"./share/sources/accelerator/MATERIALS.sha256":  {typeflag: tar.TypeReg, mode: 0o644},
	"./share/sources/accelerator/SOURCES.tsv":       {typeflag: tar.TypeReg, mode: 0o644},
}

func materialContract(name string) (entryContract, bool) {
	for _, directory := range []string{
		"./share/",
		"./share/licenses/",
		"./share/licenses/accelerator/",
		"./share/sources/",
		"./share/sources/accelerator/",
	} {
		if name == directory {
			return entryContract{typeflag: tar.TypeDir, mode: 0o755}, true
		}
	}
	if !strings.HasPrefix(name, "./share/licenses/accelerator/") &&
		!strings.HasPrefix(name, "./share/sources/accelerator/") {
		return entryContract{}, false
	}
	clean := path.Clean(strings.TrimPrefix(name, "./"))
	if clean == "." || strings.HasPrefix(clean, "../") || strings.Contains(clean, "/../") {
		return entryContract{}, false
	}
	if strings.HasSuffix(name, "/") {
		if name != "./"+clean+"/" {
			return entryContract{}, false
		}
		return entryContract{typeflag: tar.TypeDir, mode: 0o755}, true
	}
	if name != "./"+clean {
		return entryContract{}, false
	}
	return entryContract{typeflag: tar.TypeReg, mode: 0o644}, true
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: release-archive-validator <archive>")
		os.Exit(2)
	}
	if err := validateArchive(os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "release archive: %v\n", err)
		os.Exit(1)
	}
}

func validateArchive(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzipReader.Close()

	seen := make(map[string]struct{}, len(archiveContract))
	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		contract, ok := archiveContract[header.Name]
		if !ok {
			contract, ok = materialContract(header.Name)
			if !ok {
				return fmt.Errorf("unexpected member %q", header.Name)
			}
		}
		if _, ok := seen[header.Name]; ok {
			return fmt.Errorf("duplicate member %q", header.Name)
		}
		seen[header.Name] = struct{}{}

		if !matchesType(header.Typeflag, contract.typeflag) {
			return fmt.Errorf("member %q has type %q, want %q", header.Name, header.Typeflag, contract.typeflag)
		}
		if header.Mode != contract.mode {
			return fmt.Errorf("member %q has mode %#o, want %#o", header.Name, header.Mode, contract.mode)
		}
		if header.Uid != 0 || header.Gid != 0 {
			return fmt.Errorf("member %q has owner %d/%d, want 0/0", header.Name, header.Uid, header.Gid)
		}
		if header.Uname != "" || header.Gname != "" {
			return fmt.Errorf("member %q stores owner names %q/%q", header.Name, header.Uname, header.Gname)
		}
		if header.Linkname != "" {
			return fmt.Errorf("member %q stores link target %q", header.Name, header.Linkname)
		}
	}

	for name := range archiveContract {
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("missing member %q", name)
		}
	}
	return nil
}

func matchesType(actual, expected byte) bool {
	if expected == tar.TypeReg {
		return actual == tar.TypeReg || actual == tar.TypeRegA
	}
	return actual == expected
}
