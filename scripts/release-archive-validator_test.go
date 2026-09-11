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

func archiveFixture(t *testing.T, memberSize int, trailing []byte) string {
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
		if err := tw.WriteHeader(header); err != nil {
			t.Fatal(err)
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

func TestArchiveTrailer(t *testing.T) {
	if err := validateArchive(archiveFixture(t, 16, nil)); err != nil {
		t.Fatal(err)
	}
	err := validateArchive(archiveFixture(t, 0, []byte("unexpected")))
	if err == nil || !strings.Contains(err.Error(), "after tar") {
		t.Fatalf("want trailing-data rejection, got %v", err)
	}
}
