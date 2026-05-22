// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// v0.23.9: GitHub's GraphQL gateway has been observed returning HTTP 200
// with a zero-byte body when the upstream resolver times out AFTER the
// response headers have been committed. The TCP stream closes cleanly so
// io.ReadAll returns (nil, nil) on the empty body — and the pre-v0.23.9
// code fell straight into parseGraphQLResponse, which surfaced a cryptic
// "decode graphql envelope: unexpected end of JSON input (body: )" error
// that classified as ClassFatal. The v0.20.8 subdivision retry only
// kicks in on ClassTransient/ClassRateLimit, so one bad batch sank the
// entire collection (production: apache/felix, 2026-05-21).
//
// These tests pin the v0.23.9 fix: the OK branch treats an empty body
// as a body-read failure, routing through the existing Fix C retry path.

// TestGraphQLRetriesAfterEmptyBody200 is the regression test for the
// apache/felix failure mode. First attempt: 200 OK with zero bytes.
// Second attempt: complete response. The GraphQL call must succeed.
func TestGraphQLRetriesAfterEmptyBody200(t *testing.T) {
	var attempts atomic.Int32
	const goodBody = `{"data":{"hello":"world"}}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			// First call: 200 OK with Content-Length: 0 and zero body.
			// This is what GitHub's gateway returns when the upstream
			// query resolution fails after headers have been committed.
			// io.ReadAll on the client side sees (nil, nil) because the
			// stream closes cleanly — there's no transport error to
			// trigger the existing Fix C retry path.
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusOK)
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
		t.Fatalf("GraphQL failed after empty-body retry: %v (v0.23.9 should have retried)", err)
	}
	if got.Hello != "world" {
		t.Errorf("got.Hello = %q, want \"world\" (data from second successful attempt did not decode)", got.Hello)
	}
	if attempts.Load() != 2 {
		t.Errorf("expected exactly 2 attempts (first empty-body, second succeeds), got %d", attempts.Load())
	}
}

// TestGraphQLGivesUpAfterPersistentEmptyBodies pins that the read-retry
// budget is respected for empty-body failures the same way it is for
// mid-body aborts. After maxReadRetries (3) consecutive empty bodies the
// error surfaces with a "read graphql response" prefix, the call doesn't
// loop forever, and the retry count matches the budget (1 initial + 3).
func TestGraphQLGivesUpAfterPersistentEmptyBodies(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	keys := NewKeyPool([]string{"test-token"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c := NewHTTPClient(server.URL, keys, slog.New(slog.NewTextHandler(io.Discard, nil)), AuthGitHub)

	err := c.GraphQL(context.Background(), "{ hello }", nil, nil)
	if err == nil {
		t.Fatal("expected error after persistent empty bodies, got nil")
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
