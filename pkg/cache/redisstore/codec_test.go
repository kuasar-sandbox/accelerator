package redisstore

import (
	"strconv"
	"testing"
)

func TestParseBulkSizeEnforcesCachePayloadLimit(t *testing.T) {
	if got, err := parseBulkSize([]byte(strconv.Itoa(maxBulkSize))); err != nil || got != maxBulkSize {
		t.Fatalf("maximum bulk size=%d err=%v, want %d", got, err, maxBulkSize)
	}
	if _, err := parseBulkSize([]byte(strconv.Itoa(maxBulkSize + 1))); err == nil {
		t.Fatal("bulk size above cache payload limit was accepted")
	}
}
