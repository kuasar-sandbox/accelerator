//go:build !linux

package tar

import (
	stdtar "archive/tar"
	"errors"
	"time"
)

var errUnsupportedNode = errors.New("device/FIFO nodes are not supported on this platform")

func mknodEntry(string, *stdtar.Header) error { return errUnsupportedNode }

func lchtimes(string, time.Time) error { return nil }
