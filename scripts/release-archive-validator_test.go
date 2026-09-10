//go:build ignore

package main

import (
	"archive/tar"
	"compress/gzip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func archiveFixture(t *testing.T, oversizedHeader bool, memberSize int, trailing []byte) string {
	t.Helper()
	name := filepath.Join(t.TempDir(), "fixture.tar.gz")
	file, err := os.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(file)
	tw := tar.NewWriter(gz)
	var names []string
	for name := range archiveContract {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		entry := archiveContract[name]
		header := &tar.Header{Name: name, Mode: entry.mode, Typeflag: entry.typeflag}
		if entry.typeflag == tar.TypeReg {
			header.Size = int64(memberSize)
		}
		if oversizedHeader && name == "./bin/cache-ctl" {
			header.Size = (512 << 20) + 1
		}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if oversizedHeader && name == "./bin/cache-ctl" {
			// A header-only compressed bomb must fail its declared-size check,
			// before attempting to consume the missing enormous body.
			if err := gz.Close(); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			return file.Name()
		}
		if entry.typeflag == tar.TypeReg {
			if _, err := tw.Write(make([]byte, memberSize)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := gz.Write(trailing); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return name
}

func TestSourceMetadataHasNoDynamicFallback(t *testing.T) {
	for _, name := range []string{
		"./share/sources/accelerator/unexpected",
		"./share/sources/accelerator/nested/",
		"./share/sources/accelerator/nested/NOTICE",
	} {
		if _, ok := materialContract(name); ok {
			t.Fatalf("undeclared source metadata accepted: %s", name)
		}
	}
	if _, ok := materialContract("./share/licenses/accelerator/dependency/LICENSE"); !ok {
		t.Fatal("ordinary nested license material was rejected")
	}
	for _, name := range []string{"SOURCES.tsv", "GO-BUILD-INFO.tsv", "GO-MODULES.tsv", "MATERIALS.sha256"} {
		if _, ok := archiveContract["./share/sources/accelerator/"+name]; !ok {
			t.Fatalf("declared source metadata missing: %s", name)
		}
	}
}

func TestArchiveResourceLimits(t *testing.T) {
	valid := archiveFixture(t, false, 16, nil)
	if err := validateArchive(valid); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, archive, want string
		member, expanded    int64
		entries             int
	}{
		{"declared-member", archiveFixture(t, true, 0, nil), "member", 512 << 20, 1 << 30, 20000},
		{"cumulative-body", archiveFixture(t, false, 4096, nil), "expanded", 8192, 16384, 20000},
		{"padding", archiveFixture(t, false, 0, make([]byte, 65536)), "expanded", 8192, 32768, 20000},
		{"count", valid, "member count", 8192, 65536, 2},
		{"trailing-data", archiveFixture(t, false, 0, []byte("unexpected")), "after tar", 8192, 65536, 20000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateArchiveWithLimits(tc.archive, tc.member, tc.expanded, tc.entries)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %s limit, got %v", tc.want, err)
			}
		})
	}
}
