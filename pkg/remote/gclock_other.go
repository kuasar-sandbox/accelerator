//go:build !linux

package remote

// lockFile is a no-op on non-linux platforms. flatten-ctl targets linux
// (the Makefile builds GOOS=linux); this keeps `go build` / `go test` green
// on developer machines without pulling in an OS-specific locking dependency.
func lockFile(path string) (func(), error) {
	return func() {}, nil
}
