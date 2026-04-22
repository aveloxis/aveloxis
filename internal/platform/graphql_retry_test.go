package platform

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"golang.org/x/net/http2"
)

// Fix C (v0.18.23): mid-body stream-CANCEL and unexpected-EOF errors are
// retryable in principle (a fresh stream often succeeds) but were being
// returned as terminal errors by HTTPClient.GraphQL. This test file pins
// both the helper (isRetryableGraphQLReadError) and the integrated retry
// loop behavior.

// TestIsRetryableGraphQLReadError covers the error classification used
// inside the GraphQL retry loop. Unit-tests the helper in isolation so
// adding a new retryable shape later is a single-line change plus test
// case here.
func TestIsRetryableGraphQLReadError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is not retryable", nil, false},
		{
			"io.ErrUnexpectedEOF (server aborted mid-body)",
			io.ErrUnexpectedEOF,
			true,
		},
		{
			"wrapped io.ErrUnexpectedEOF",
			nil, // replaced below with &wrappedErr{inner: io.ErrUnexpectedEOF}
			true,
		},
		{
			"http2.StreamError with CANCEL code",
			http2.StreamError{StreamID: 123, Code: http2.ErrCodeCancel},
			true,
		},
		{
			"http2.StreamError with INTERNAL_ERROR code",
			http2.StreamError{StreamID: 456, Code: http2.ErrCodeInternal},
			true,
		},
		{
			"substring match: 'stream error' in message",
			errors.New("stream error: stream ID 57621; CANCEL; received from peer"),
			true,
		},
		{
			"substring match: 'connection reset' in message",
			errors.New("read tcp 10.0.0.1->10.0.0.2: connection reset by peer"),
			true,
		},
		{
			"substring match: 'unexpected EOF' in message",
			errors.New("read wrap: unexpected EOF"),
			true,
		},
		{
			"generic non-retryable error",
			errors.New("json: cannot unmarshal string into int"),
			false,
		},
		{
			"context.Canceled is not our retry class (ctx path handles it)",
			context.Canceled,
			false,
		},
	}
	// Patch the placeholder case — can't construct a wrapped
	// ErrUnexpectedEOF inline in a struct literal.
	cases[2].err = &wrappedErr{inner: io.ErrUnexpectedEOF}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isRetryableGraphQLReadError(tc.err)
			if got != tc.want {
				t.Errorf("isRetryableGraphQLReadError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

type wrappedErr struct{ inner error }

func (w *wrappedErr) Error() string { return "wrapped: " + w.inner.Error() }
func (w *wrappedErr) Unwrap() error { return w.inner }

// TestGraphQLRetriesAfterMidBodyAbort is the integration test that would
// have caught the production bug. A fake server writes the HTTP headers +
// 200 status, then aborts the connection mid-body. On the retry, it
// serves a complete response. The GraphQL call must succeed.
//
// Without Fix C, the first mid-body error returns immediately with
// "read graphql response: ..." and the caller sees a failed query.
// With Fix C, the retry loop continues; second attempt succeeds.
func TestGraphQLRetriesAfterMidBodyAbort(t *testing.T) {
	var attempts atomic.Int32
	const goodBody = `{"data":{"hello":"world"}}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			// First call: write a response that terminates mid-body. Set
			// Content-Length larger than what we actually write, then hijack
			// the connection to forcibly close it, producing an unexpected
			// EOF on the client.
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", "1000")
			w.WriteHeader(http.StatusOK)
			// Write a partial body then hijack and close.
			_, _ = w.Write([]byte(`{"data":{"hell`))
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("response writer did not implement Hijacker; can't simulate mid-body abort")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack failed: %v", err)
				return
			}
			// Abort the connection mid-response. The client's io.ReadAll
			// returns io.ErrUnexpectedEOF (or a net error), which Fix C
			// classifies as retryable.
			_ = conn.(*net.TCPConn).SetLinger(0)
			_ = conn.Close()
			return
		}
		// Second call: normal complete response.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(goodBody))
	}))
	defer server.Close()

	keys := NewKeyPool([]string{"test-token"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c := NewHTTPClient(server.URL, keys, slog.New(slog.NewTextHandler(io.Discard, nil)), AuthGitHub)

	var got struct {
		Hello string `json:"hello"`
	}
	err := c.GraphQL(context.Background(), "{ hello }", nil, &got)
	if err != nil {
		t.Fatalf("GraphQL failed after retry: %v (Fix C should have retried the mid-body abort)", err)
	}
	if got.Hello != "world" {
		t.Errorf("got.Hello = %q, want \"world\" (data from second successful attempt did not decode)", got.Hello)
	}
	if attempts.Load() != 2 {
		t.Errorf("expected exactly 2 attempts (first aborts, second succeeds), got %d", attempts.Load())
	}
}

// TestGraphQLGivesUpAfterPersistentMidBodyAborts ensures the read-retry
// budget is respected — we don't loop forever when the server is
// permanently broken. After maxReadRetries (3) failed body reads, the
// error surfaces with a "read graphql response" prefix. Cumulative test
// wall clock: 1s + 2s + 3s = 6s under jitter-free linear backoff, well
// within the default test timeout.
func TestGraphQLGivesUpAfterPersistentMidBodyAborts(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":`))
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			return
		}
		_ = conn.(*net.TCPConn).SetLinger(0)
		_ = conn.Close()
	}))
	defer server.Close()

	keys := NewKeyPool([]string{"test-token"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c := NewHTTPClient(server.URL, keys, slog.New(slog.NewTextHandler(io.Discard, nil)), AuthGitHub)

	err := c.GraphQL(context.Background(), "{ hello }", nil, nil)
	if err == nil {
		t.Fatal("expected error after persistent mid-body aborts, got nil")
	}
	// Expected: 1 initial + 3 read-retries = 4 attempts.
	if n := attempts.Load(); n != 4 {
		t.Errorf("expected 4 attempts (1 initial + 3 read-retries), got %d", n)
	}
	msg := err.Error()
	if !strings.Contains(msg, "read graphql response") {
		t.Errorf("error message %q should indicate a read failure after the retry budget is exhausted", msg)
	}
}
