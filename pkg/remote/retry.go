package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"time"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
)

// Docker-style defaults for complete image reads. The SDK also retries short
// HTTP transport failures before an operation returns an error.
const defaultPullRetryAttempts = 5

// PullRetryEvent contains no raw URL/error text, since redirected URLs may
// contain credentials. Callbacks may run concurrently for different layers.
type PullRetryEvent struct {
	Operation     string
	FailedAttempt int
	MaxAttempts   int
	Delay         time.Duration
	Reason        string
}

// pullOperationError keeps the original cause for errors.Is/As while omitting
// credentials from registry/CDN URLs in the user-visible error text.
type pullOperationError struct {
	message string
	cause   error
}

func (e *pullOperationError) Error() string { return e.message }
func (e *pullOperationError) Unwrap() error { return e.cause }

func pullErrorDetail(err error) string {
	if err == nil {
		return ""
	}
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return err.Error()
	}
	u, parseErr := url.Parse(urlErr.URL)
	if parseErr != nil {
		return fmt.Sprintf("%s <remote URL>: %s", urlErr.Op, pullErrorDetail(urlErr.Err))
	}
	u.User = nil
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	return fmt.Sprintf("%s %q: %s", urlErr.Op, u.String(), pullErrorDetail(urlErr.Err))
}

func metadataRetryDelay(failedAttempt int) time.Duration {
	// Moby's image config fetch waits 250ms, 500ms, 1s, 2s.
	return (250 * time.Millisecond) << (failedAttempt - 1)
}

func layerRetryDelay(failedAttempt int) time.Duration {
	// Moby's layer transfer manager waits 5, 10, 15, 20 seconds.
	return time.Duration(failedAttempt) * 5 * time.Second
}

func (c *Config) retryPull(ctx context.Context, operation string, retryDelay func(int) time.Duration, fn func() error) error {
	for attempt := 1; attempt <= defaultPullRetryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn()
		if err == nil {
			return ctx.Err()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		reason := pullRetryReason(err)
		if reason == "" || attempt == defaultPullRetryAttempts {
			return &pullOperationError{fmt.Sprintf("remote: %s failed after %d/%d attempts: %s", operation, attempt, defaultPullRetryAttempts, pullErrorDetail(err)), err}
		}
		delay := retryDelay(attempt)
		// Don't schedule a retry that cannot start before the caller's deadline.
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= delay {
			return &pullOperationError{fmt.Sprintf("remote: %s: insufficient time for retry after attempt %d: %s", operation, attempt, pullErrorDetail(err)), err}
		}
		if c.OnPullRetry != nil {
			c.OnPullRetry(PullRetryEvent{Operation: operation, FailedAttempt: attempt, MaxAttempts: defaultPullRetryAttempts, Delay: delay, Reason: reason})
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	panic("unreachable")
}

func pullRetryReason(err error) string {
	if err == nil || errors.Is(err, context.Canceled) {
		return ""
	}
	// Cache creation, write, sync, close and rename errors are local failures.
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return ""
	}
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		return ""
	}
	var registryErr *transport.Error
	if errors.As(err, &registryErr) {
		// The SDK also classifies registry diagnostic codes as temporary, even
		// when the HTTP status alone is not in its temporary status set.
		if registryErr.Temporary() {
			return fmt.Sprintf("http_%d", registryErr.StatusCode)
		}
		// Cover rate limiting, client disconnects, and transient CDN failures
		// that the SDK does not recognize from an unstructured HTTP response.
		switch registryErr.StatusCode {
		case http.StatusRequestTimeout, http.StatusTooManyRequests,
			499, 520, 521, 522, 523, 524: // Registry/CDN-specific statuses have no net/http constants.
			return fmt.Sprintf("http_%d", registryErr.StatusCode)
		}
		return ""
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		if dnsErr.IsTimeout {
			return "dns_timeout"
		}
		if dnsErr.IsTemporary {
			return "dns_temporary"
		}
		return ""
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "network_timeout"
		}
		if netErr.Temporary() {
			return "temporary_network_error"
		}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "unexpected_eof"
	}
	for _, target := range []error{syscall.ECONNRESET, syscall.ECONNREFUSED, syscall.ECONNABORTED, syscall.ETIMEDOUT, syscall.EPIPE, syscall.ENETUNREACH, syscall.EHOSTUNREACH, net.ErrClosed} {
		if errors.Is(err, target) {
			return "connection_error"
		}
	}
	return ""
}
