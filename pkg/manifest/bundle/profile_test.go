package bundle

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestWriterMetadataOrderAndReaderRefsCopy(t *testing.T) {
	refs := []string{
		"file://" + strings.Repeat("a", 64) + ".bundle",
		"file://" + strings.Repeat("b", 64) + ".bundle@location:B",
	}
	salt, err := store.SaltForGeneration("G1")
	if err != nil {
		t.Fatal(err)
	}
	admission := store.WriteAdmission{Generation: "G1", Salt: salt}
	manifest := canonicalManifestEntry(t)
	manifestKey, err := parseObjectEntry(manifest.name, manifestPrefix)
	if err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	w, err := NewWriter(&output, admission, WriterOptions{Refs: refs})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put(context.Background(), admission, store.PartitionManifest, manifestKey, manifest.data); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(manifestKey); err != nil {
		t.Fatal(err)
	}

	zr, err := zip.NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{zr.File[0].Name, zr.File[1].Name, zr.File[2].Name}; !reflect.DeepEqual(got, []string{refsName, admissionName(admission), manifest.name}) {
		t.Fatalf("ZIP entry order = %v", got)
	}
	reader, err := NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got := reader.Refs()
	if !reflect.DeepEqual(got, refs) {
		t.Fatalf("Refs = %v, want %v", got, refs)
	}
	got[0] = "file://mutated.bundle"
	if reflect.DeepEqual(reader.Refs(), got) {
		t.Fatal("Refs exposed mutable Reader state")
	}
}

func TestWriterWithoutRefsStartsWithAdmission(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	zr, err := zip.NewReader(bytes.NewReader(fixture.data), int64(len(fixture.data)))
	if err != nil {
		t.Fatal(err)
	}
	if zr.File[0].Name != admissionName(fixture.admission) {
		t.Fatalf("entry 0 = %q, want admission", zr.File[0].Name)
	}
	if refs := fixture.reader.Refs(); len(refs) != 0 {
		t.Fatalf("Refs = %v, want empty", refs)
	}
}

func TestRefsCodecRejectsNonCanonicalInput(t *testing.T) {
	digest := strings.Repeat("a", 64)
	valid := "file://" + digest + ".bundle"
	located := valid + "@location:A"
	payload, err := EncodeRefs([]string{located, valid})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ParseRefs(payload); err != nil || !reflect.DeepEqual(got, []string{located, valid}) {
		t.Fatalf("ParseRefs = %v, %v", got, err)
	}

	invalidRefs := []string{
		"manifest://" + digest,
		"file:///absolute.bundle",
		"file://dir/root.bundle",
		"file://dir\\root.bundle",
		"file://root.snapshot",
		"file://root.bundle@manifest:" + digest,
		"file://root.bundle@sha256:" + digest,
		"file://root.bundle@hmac:" + digest,
		"file://root\n.bundle",
		"file://root\r.bundle",
		"file://\ufeffroot.bundle",
		string([]byte{'f', 'i', 'l', 'e', ':', '/', '/', 0xff, '.', 'b', 'u', 'n', 'd', 'l', 'e'}),
	}
	for _, ref := range invalidRefs {
		t.Run(ref, func(t *testing.T) {
			var output bytes.Buffer
			salt, err := store.SaltForGeneration("G1")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := NewWriter(&output, store.WriteAdmission{Generation: "G1", Salt: salt}, WriterOptions{Refs: []string{ref}}); err == nil {
				t.Fatalf("invalid ref %q accepted", ref)
			}
			if output.Len() != 0 {
				t.Fatalf("invalid refs wrote %d bytes before validation", output.Len())
			}
		})
	}

	invalidPayloads := [][]byte{
		{},
		[]byte(valid),
		[]byte(valid + "\r\n"),
		[]byte(valid + "\n\n"),
		[]byte(" " + valid + "\n"),
		[]byte(valid + " \n"),
		[]byte("# comment\n"),
		[]byte(valid + "\n" + valid + "\n"),
		append([]byte{0xef, 0xbb, 0xbf}, []byte(valid+"\n")...),
		append([]byte(valid+"\n"), append([]byte{0xef, 0xbb, 0xbf}, []byte(valid+"\n")...)...),
		{0xff, '\n'},
	}
	for index, payload := range invalidPayloads {
		t.Run(fmt.Sprintf("payload-%d", index), func(t *testing.T) {
			if _, err := ParseRefs(payload); err == nil {
				t.Fatalf("invalid payload %q accepted", payload)
			}
		})
	}
	if _, err := EncodeRefs(make([]string, maxBundleRefs+1)); err == nil {
		t.Fatal("refs count limit was not enforced by Writer codec")
	}
	tooMany := make([]string, maxBundleRefs+1)
	for index := range tooMany {
		tooMany[index] = fmt.Sprintf("file://%064x.bundle", index)
	}
	if _, err := ParseRefs([]byte(strings.Join(tooMany, "\n") + "\n")); err == nil {
		t.Fatal("refs count limit was not enforced by Reader codec")
	}
	if _, err := ParseRefs(bytes.Repeat([]byte{'a'}, maxRefsPayload+1)); err == nil {
		t.Fatal("refs payload size limit was not enforced")
	}
}

func TestReaderRejectsMetadataOrderLegacyAndUnknownBundleEntries(t *testing.T) {
	admission := canonicalAdmissionEntry(t)
	manifest := canonicalManifestEntry(t)
	refs := rawEntry{name: refsName, data: []byte("file://parent.bundle\n")}
	legacy := admission
	legacy.name = strings.Replace(admission.name, admissionPrefix, legacyAdmissionPrefix, 1)
	nonEmptyAdmission := admission
	nonEmptyAdmission.data = []byte("not empty")
	for _, tc := range []struct {
		name    string
		entries []rawEntry
	}{
		{name: "refs after admission", entries: []rawEntry{admission, refs, manifest}},
		{name: "object before admission", entries: []rawEntry{manifest, admission}},
		{name: "legacy admission", entries: []rawEntry{legacy, manifest}},
		{name: "non-empty admission", entries: []rawEntry{nonEmptyAdmission, manifest}},
		{name: "unknown bundle metadata", entries: []rawEntry{admission, manifest, {name: "bundle/root"}}},
		{name: "empty refs", entries: []rawEntry{{name: refsName}, admission, manifest}},
		{name: "duplicate refs", entries: []rawEntry{refs, refs, admission, manifest}},
		{name: "invalid refs", entries: []rawEntry{{name: refsName, data: []byte("file://dir/root.bundle\n")}, admission, manifest}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := rawZIP(t, tc.entries, "")
			if _, err := NewReader(bytes.NewReader(data), int64(len(data))); err == nil {
				t.Fatal("invalid metadata profile accepted")
			}
		})
	}
}

func TestReaderRejectsCentralDirectoryPhysicalOrderMismatch(t *testing.T) {
	data := rawZIP(t, []rawEntry{canonicalAdmissionEntry(t), canonicalManifestEntry(t)}, "")
	mutated := reorderFirstTwoCentralRecords(t, data)
	if _, err := NewReader(bytes.NewReader(mutated), int64(len(mutated))); err == nil {
		t.Fatal("Central Directory order differing from local-header order was accepted")
	}
}

func TestReaderRejectsFakeDirectoryMagicBeforeActualDirectory(t *testing.T) {
	data := rawZIP(t, []rawEntry{canonicalAdmissionEntry(t), canonicalManifestEntry(t)}, "")
	start, eocd, _ := centralRecords(t, data)
	mutated := append([]byte(nil), data[:start]...)
	mutated = append(mutated, zipDirectoryMagic[:]...)
	mutated = append(mutated, data[start:eocd]...)
	end := append([]byte(nil), data[eocd:]...)
	binary.LittleEndian.PutUint32(end[16:20], uint32(start+len(zipDirectoryMagic)))
	mutated = append(mutated, end...)
	if _, err := NewReader(bytes.NewReader(mutated), int64(len(mutated))); err == nil {
		t.Fatal("fake Central Directory marker hiding a physical gap was accepted")
	}
}

func TestReaderRejectsGapAndHiddenLocalEntry(t *testing.T) {
	valid := rawZIP(t, []rawEntry{canonicalAdmissionEntry(t), canonicalManifestEntry(t)}, "")
	for _, tc := range []struct {
		name string
		gap  []byte
	}{
		{name: "arbitrary gap", gap: []byte{0}},
		{name: "hidden local entry", gap: localPrefix(t, rawZIP(t, []rawEntry{{name: "hidden", data: []byte("x")}}, ""))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutated := insertBeforeSecondLocalEntry(t, valid, tc.gap)
			if _, err := NewReader(bytes.NewReader(mutated), int64(len(mutated))); err == nil {
				t.Fatal("non-contiguous physical metadata prefix was accepted")
			}
		})
	}
}

func centralRecords(t *testing.T, data []byte) (int, int, [][]byte) {
	t.Helper()
	if len(data) < zipDirectoryEndSize {
		t.Fatal("short ZIP")
	}
	eocd := len(data) - zipDirectoryEndSize
	start := int(binary.LittleEndian.Uint32(data[eocd+16 : eocd+20]))
	count := int(binary.LittleEndian.Uint16(data[eocd+10 : eocd+12]))
	position := start
	records := make([][]byte, 0, count)
	for range count {
		if position+46 > eocd || !bytes.Equal(data[position:position+4], []byte{'P', 'K', 0x01, 0x02}) {
			t.Fatal("malformed test Central Directory")
		}
		size := 46 + int(binary.LittleEndian.Uint16(data[position+28:position+30])) + int(binary.LittleEndian.Uint16(data[position+30:position+32])) + int(binary.LittleEndian.Uint16(data[position+32:position+34]))
		records = append(records, append([]byte(nil), data[position:position+size]...))
		position += size
	}
	return start, eocd, records
}

func reorderFirstTwoCentralRecords(t *testing.T, data []byte) []byte {
	t.Helper()
	start, eocd, records := centralRecords(t, data)
	if len(records) < 2 {
		t.Fatal("need two Central Directory records")
	}
	records[0], records[1] = records[1], records[0]
	mutated := append([]byte(nil), data[:start]...)
	for _, record := range records {
		mutated = append(mutated, record...)
	}
	mutated = append(mutated, data[eocd:]...)
	return mutated
}

func localPrefix(t *testing.T, data []byte) []byte {
	t.Helper()
	start, _, _ := centralRecords(t, data)
	return append([]byte(nil), data[:start]...)
}

func insertBeforeSecondLocalEntry(t *testing.T, data, inserted []byte) []byte {
	t.Helper()
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(zr.File) < 2 {
		t.Fatal("need two local entries")
	}
	secondOffset, err := zr.File[1].DataOffset()
	if err != nil {
		t.Fatal(err)
	}
	secondHeader := int(secondOffset) - 30 - len(zr.File[1].Name)
	start, eocd, records := centralRecords(t, data)
	delta := len(inserted)
	mutated := append([]byte(nil), data[:secondHeader]...)
	mutated = append(mutated, inserted...)
	mutated = append(mutated, data[secondHeader:start]...)
	for _, record := range records {
		localOffset := binary.LittleEndian.Uint32(record[42:46])
		if int(localOffset) >= secondHeader {
			binary.LittleEndian.PutUint32(record[42:46], localOffset+uint32(delta))
		}
		mutated = append(mutated, record...)
	}
	eocdRecord := append([]byte(nil), data[eocd:]...)
	binary.LittleEndian.PutUint32(eocdRecord[16:20], uint32(start+delta))
	mutated = append(mutated, eocdRecord...)
	return mutated
}

func FuzzParseRefs(f *testing.F) {
	f.Add([]byte("file://parent.bundle\n"))
	f.Add([]byte("file://parent.bundle@location:A\nfile://ancestor.bundle\n"))
	f.Add([]byte("file://parent.bundle\r\n"))
	f.Fuzz(func(t *testing.T, payload []byte) {
		refs, err := ParseRefs(payload)
		if err != nil {
			return
		}
		encoded, err := EncodeRefs(refs)
		if err != nil {
			t.Fatalf("EncodeRefs(ParseRefs(payload)): %v", err)
		}
		if !bytes.Equal(encoded, payload) {
			t.Fatalf("refs codec is not canonical: %q != %q", encoded, payload)
		}
	})
}
