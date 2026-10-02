package sdkclient

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Both GET and HEAD must return the original 301/302 without leaking the
// transport's synthetic redirect target or following a real redirect.
func TestOwnedHTTPClientRedirectHeaderCleanup(t *testing.T) {
	for _, status := range []int{http.StatusMovedPermanently, http.StatusFound} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			for _, location := range []string{"", "/target"} {
				t.Run(fmt.Sprintf("%d/%s/location=%q", status, method, location), func(t *testing.T) {
					var requests atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						requests.Add(1)
						if location != "" {
							w.Header().Set("Location", location)
						}
						w.Header().Set("X-Original-Response", "retained")
						w.Header().Set("Content-Length", "0")
						w.WriteHeader(status)
					}))
					defer server.Close()
					client := newOwnedHTTPClient(http.DefaultTransport.(*http.Transport).Clone(), 2*time.Second)
					defer client.CloseIdleConnections()
					req, err := http.NewRequest(method, server.URL, nil)
					if err != nil {
						t.Fatal(err)
					}
					resp, err := client.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					defer resp.Body.Close()
					if resp.StatusCode != status {
						t.Fatalf("status=%d, want %d", resp.StatusCode, status)
					}
					if got := resp.Header.Get("Location"); got != location {
						t.Fatalf("Location=%q, want %q (synthetic Location must not leak)", got, location)
					}
					if got := resp.Header.Get("X-Original-Response"); got != "retained" {
						t.Fatalf("original response header changed: %q", got)
					}
					if got := requests.Load(); got != 1 {
						t.Fatalf("requests=%d, redirect should not have been followed", got)
					}
				})
			}
		}
	}
}
