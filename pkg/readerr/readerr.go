// Package readerr carries the nature of a single read failure. It does not
// retry, choose a source, or decide the lifetime of the consuming operation.
package readerr

type marked struct {
	err       error
	retryable bool
}

func (e *marked) Error() string   { return e.err.Error() }
func (e *marked) Unwrap() error   { return e.err }
func (e *marked) Retryable() bool { return e.retryable }

// Mark preserves err's diagnostic and error chain. A nil error stays nil.
func Mark(err error, retryable bool) error {
	if err == nil {
		return nil
	}
	return &marked{err: err, retryable: retryable}
}

// IsPermanent reports whether any cause explicitly forbids retry. Inspect
// every joined cause: an earlier transient failure must not hide corruption
// discovered by another reader. A marked semantic boundary is authoritative
// for its own causes (for example, an inconclusive multi-source lookup).
func IsPermanent(err error) bool {
	if err == nil {
		return false
	}
	if m, ok := err.(interface{ Retryable() bool }); ok {
		return !m.Retryable()
	}
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		for _, cause := range e.Unwrap() {
			if IsPermanent(cause) {
				return true
			}
		}
	case interface{ Unwrap() error }:
		return IsPermanent(e.Unwrap())
	}
	return false
}
