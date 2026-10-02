package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
)

type temporaryOnlyError struct{}

func (temporaryOnlyError) Error() string   { return "temporary" }
func (temporaryOnlyError) Temporary() bool { return true }

type temporaryNetError struct{}

func (temporaryNetError) Error() string   { return "temporary network error" }
func (temporaryNetError) Timeout() bool   { return false }
func (temporaryNetError) Temporary() bool { return true }

func TestPullRetryClassification(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		retry bool
	}{
		{"DNS timeout", &net.DNSError{Err: "i/o timeout", IsTimeout: true}, true},
		{"DNS unavailable", &net.DNSError{Err: "temporary", IsTemporary: true}, true},
		{"temporary network error", temporaryNetError{}, true},
		{"unrelated temporary error", temporaryOnlyError{}, false},
		{"DNS not found", &net.DNSError{IsNotFound: true}, false},
		{"OBS dial timeout", &url.Error{Op: "Get", URL: "http://obs.invalid/?Signature=secret", Err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ETIMEDOUT}}, true},
		{"reset", fmt.Errorf("cache write: %w", syscall.ECONNRESET), true},
		{"short body", fmt.Errorf("cache write: %w", io.ErrUnexpectedEOF), true},
		{"deadline inside request", context.DeadlineExceeded, true},
		{"cancel", context.Canceled, false},
		{"auth", &transport.Error{StatusCode: 401}, false},
		{"missing", &transport.Error{StatusCode: 404}, false},
		{"unavailable", &transport.Error{StatusCode: 503}, true},
		{"SDK temporary diagnostic", &transport.Error{StatusCode: 501, Errors: []transport.Diagnostic{{Code: transport.UnavailableErrorCode}}}, true},
		{"SDK permanent diagnostic", &transport.Error{StatusCode: 500, Errors: []transport.Diagnostic{{Code: transport.ManifestInvalidErrorCode}}}, false},
		{"rate limited", &transport.Error{StatusCode: 429}, true},
		{"client disconnected", &transport.Error{StatusCode: 499}, true},
		{"CDN 520", &transport.Error{StatusCode: 520}, true},
		{"CDN 521", &transport.Error{StatusCode: 521}, true},
		{"CDN 522", &transport.Error{StatusCode: 522}, true},
		{"CDN 523", &transport.Error{StatusCode: 523}, true},
		{"CDN 524", &transport.Error{StatusCode: 524}, true},
		{"unrecognized server error", &transport.Error{StatusCode: 526}, false},
		{"disk full", &os.PathError{Op: "write", Path: "cache", Err: syscall.ENOSPC}, false},
		{"local timeout", &os.PathError{Op: "write", Path: "cache", Err: syscall.ETIMEDOUT}, false},
		{"digest mismatch", errors.New("digest mismatch"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pullRetryReason(tc.err) != ""; got != tc.retry {
				t.Fatalf("retry=%v want %v", got, tc.retry)
			}
		})
	}
}

func TestPullRetryAttemptsAndBackoff(t *testing.T) {
	for _, tc := range []struct {
		name  string
		delay func(int) time.Duration
		want  []time.Duration
	}{
		{"metadata", metadataRetryDelay, []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second}},
		{"layer", layerRetryDelay, []time.Duration{5 * time.Second, 10 * time.Second, 15 * time.Second, 20 * time.Second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i, want := range tc.want {
				if got := tc.delay(i + 1); got != want {
					t.Fatalf("delay %d=%s want %s", i+1, got, want)
				}
			}
		})
	}
}

func TestPullRetryStops(t *testing.T) {
	for _, tc := range []struct {
		mode  string
		calls int
	}{
		{"success", 1},
		{"permanent", 1},
		{"cancel_before", 0},
		{"cancel_wait", 1},
		{"deadline", 1},
		{"exhausted", defaultPullRetryAttempts},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			mode := tc.mode
			c := testConfig(t)
			var events []PullRetryEvent
			if mode == "exhausted" {
				c.OnPullRetry = func(e PullRetryEvent) { events = append(events, e) }
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "cancel_before":
				cancel()
			case "cancel_wait":
				c.OnPullRetry = func(PullRetryEvent) { cancel() }
			case "deadline":
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 50*time.Millisecond)
				defer stop()
			}
			calls := 0
			delay := metadataRetryDelay
			if mode == "exhausted" {
				delay = func(int) time.Duration { return 0 }
			}
			err := c.retryPull(ctx, "test", delay, func() error {
				calls++
				if mode == "success" {
					return nil
				}
				if mode == "permanent" {
					return syscall.ENOSPC
				}
				return io.ErrUnexpectedEOF
			})
			if calls != tc.calls {
				t.Fatalf("calls=%d want=%d err=%v", calls, tc.calls, err)
			}
			if (err == nil) != (mode == "success") {
				t.Fatalf("err=%v", err)
			}
			if mode == "cancel_wait" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if mode == "exhausted" {
				if !errors.Is(err, io.ErrUnexpectedEOF) || !strings.Contains(err.Error(), "failed after 5/5 attempts") {
					t.Fatalf("exhaustion error=%v", err)
				}
				if len(events) != defaultPullRetryAttempts-1 {
					t.Fatalf("retry events=%d want=%d", len(events), defaultPullRetryAttempts-1)
				}
				for i, e := range events {
					if e.FailedAttempt != i+1 || e.MaxAttempts != defaultPullRetryAttempts {
						t.Fatalf("retry event %d=%+v", i, e)
					}
				}
			}
		})
	}
}

func TestPullErrorRedactsRedirectCredentials(t *testing.T) {
	c := testConfig(t)
	cause := &url.Error{
		Op:  "Get",
		URL: "http://obs.example/blob?Signature=secret&AccessKeyId=private",
		Err: syscall.ETIMEDOUT,
	}
	err := c.retryPull(context.Background(), "config sha256:test", metadataRetryDelay, func() error {
		return &transport.Error{StatusCode: 401}
	})
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("permanent error lost status: %v", err)
	}
	// A deadline shorter than the first backoff exercises the final error
	// path without sleeping through five attempts.
	ctx, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	err = c.retryPull(ctx, "config sha256:test", metadataRetryDelay, func() error { return cause })
	if err == nil {
		t.Fatal("expected timeout")
	}
	if strings.Contains(err.Error(), "Signature=") || strings.Contains(err.Error(), "AccessKeyId=") {
		t.Fatalf("signed URL leaked: %s", err)
	}
	if !errors.Is(err, syscall.ETIMEDOUT) {
		t.Fatalf("original cause lost: %v", err)
	}
}
