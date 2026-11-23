package manifest

import (
	"strings"
	"testing"
)

func BenchmarkParseRef(b *testing.B) {
	digest := strings.Repeat("a", 64)
	for _, tc := range []struct{ name, raw string }{
		{"manifest", "manifest://" + digest},
		{"located_bundle", "file://snapshot.bundle@manifest:" + digest + "@location:node.1"},
		{"located_tarstream", "file://snapshot.tarstream@digest:" + digest + "@location:node.1"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ParseRef(tc.raw); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
