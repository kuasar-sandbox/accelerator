package readerr

import (
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestJoinedCausesPreservePermanentFailure(t *testing.T) {
	temporary := errors.New("temporary access failure")
	permanent := Mark(io.EOF, false)
	err := fmt.Errorf("read: %w", errors.Join(Mark(temporary, true), permanent))
	if !IsPermanent(err) || !errors.Is(err, io.EOF) || !errors.Is(err, temporary) {
		t.Fatalf("lost classification or cause: %v", err)
	}
	if Mark(nil, false) != nil || IsPermanent(temporary) || IsPermanent(Mark(temporary, true)) {
		t.Fatal("unknown/transient errors must not be permanent")
	}
}
