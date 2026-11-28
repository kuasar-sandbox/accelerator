// Package plugin adapts a secret resolver to the Secret plugin HTTP protocol.
// The caller owns the HTTP server, Unix domain socket, and access policy.
package plugin

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"strings"
	"unicode"
)

const (
	resolvePath     = "/v1/secrets/resolve"
	maxRequestSize  = 1 << 20
	maxResponseSize = 1 << 20
)

// ResolveFunc retrieves the actual bytes for an opaque key without the
// @plugin/ prefix. It must be safe for concurrent calls and respect ctx.
// Returned bytes must remain unchanged while the handler encodes them; the
// handler does not modify them. The consuming application validates their
// required length and format, so an empty result is permitted.
type ResolveFunc func(ctx context.Context, key string) ([]byte, error)

// NewHandler serves POST /v1/secrets/resolve using resolve. It accepts one JSON
// object containing only a nonempty key without colons or whitespace, and passes
// the key unchanged to resolve with the request's context. Query parameters are
// not supported. Requests and encoded responses are limited to 1 MiB each.
//
// A successful response echoes the key and encodes the returned bytes as a
// base64: value. Errors matching fs.ErrNotExist, fs.ErrPermission, or
// context.DeadlineExceeded produce HTTP 404, 403, or 504 respectively. Other
// resolver errors, including a nil resolve function, produce HTTP 500. Error
// responses never contain resolver error text or secret material.
//
// NewHandler does not listen on a socket, cache values, or authorize callers.
// Serve it through an HTTP server on a Unix domain socket whose directory and
// permissions restrict access to the intended consuming processes.
func NewHandler(resolve ResolveFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != resolvePath || r.URL.RawQuery != "" || r.URL.ForceQuery {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			http.Error(w, "expected application/json", http.StatusUnsupportedMediaType)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestSize+1))
		if len(body) > maxRequestSize {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			return
		}
		if err != nil {
			http.Error(w, "invalid resolve request", http.StatusBadRequest)
			return
		}
		key, ok := requestKey(body)
		if !ok {
			http.Error(w, "invalid resolve request", http.StatusBadRequest)
			return
		}
		if resolve == nil {
			http.Error(w, "resolve failed", http.StatusInternalServerError)
			return
		}
		value, err := resolve(r.Context(), key)
		if err != nil {
			status := http.StatusInternalServerError
			switch {
			case errors.Is(err, fs.ErrNotExist):
				status = http.StatusNotFound
			case errors.Is(err, fs.ErrPermission):
				status = http.StatusForbidden
			case errors.Is(err, context.DeadlineExceeded):
				status = http.StatusGatewayTimeout
			}
			http.Error(w, "resolve failed", status)
			return
		}
		// Reject plainly oversized values before allocating their Base64 form.
		// The full JSON size is checked below, including its escaped key.
		if len(value) > maxResponseSize {
			http.Error(w, "resolve response too large", http.StatusInternalServerError)
			return
		}
		response, err := json.Marshal(struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}{key, "base64:" + base64.StdEncoding.EncodeToString(value)})
		if err != nil || len(response) > maxResponseSize {
			http.Error(w, "resolve response too large", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(response)
	})
}

func requestKey(body []byte) (string, bool) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return "", false
	}
	var key string
	seen := false
	for decoder.More() {
		field, err := decoder.Token()
		if err != nil || field != "key" || seen || decoder.Decode(&key) != nil {
			return "", false
		}
		seen = true
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return "", false
	}
	if decoder.Decode(new(any)) != io.EOF {
		return "", false
	}
	return key, seen && key != "" && !strings.ContainsRune(key, ':') && !strings.ContainsFunc(key, unicode.IsSpace)
}
