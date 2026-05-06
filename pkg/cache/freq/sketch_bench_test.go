package freq

import (
	"fmt"
	"testing"
)

func benchKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("bench-key-%d", i))
	}
	return keys
}

func BenchmarkTouch(b *testing.B) {
	s := New(Config{Counters: 1 << 20, ResetAfter: 1 << 30})
	defer s.Close()
	keys := benchKeys(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Touch(keys[i&1023])
	}
}

func BenchmarkEstimate(b *testing.B) {
	s := New(Config{Counters: 1 << 20, ResetAfter: 1 << 30})
	defer s.Close()
	keys := benchKeys(10000)
	for _, k := range keys {
		s.Touch(k)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Estimate(keys[i%10000])
	}
}

func BenchmarkTouchParallel(b *testing.B) {
	s := New(Config{Counters: 1 << 20, ResetAfter: 1 << 30})
	defer s.Close()
	keys := benchKeys(1024)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Touch(keys[i&1023])
			i++
		}
	})
}
