// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"errors"
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
	// Exhaustion is transient, like paginate's (worklist 66): a body the
	// gateway keeps cutting off comes from a query too expensive to finish,
	// and only a transient class lets the PR-batch caller subdivide it;
	// ClassFatal failed the whole job and set force_full_recollect.
	if ClassifyError(err) != ClassTransient || !errors.Is(err, ErrTransient) {
		t.Errorf("exhausted read retries classify %v (err %v); want ClassTransient wrapping ErrTransient, as the paginate path does", ClassifyError(err), err)
	}
}

// TestGraphQLRetriesAfterTruncatedBody200 (worklist 66, the 2026-09-28 kate
// log): a NON-empty body cut off mid-JSON on HTTP 200 — `{"data":{"repository":
// {"pr0":…` — was not retried: only a zero-byte body became a read failure,
// so the truncated one reached parseGraphQLResponse, "decode graphql envelope:
// unexpected end of JSON input" classified ClassFatal, the PR batch failed,
// force_full_recollect was set, and the next attempt on nixpkgs, kibana,
// azure-powershell and atomist restarted from zero and ran longer (6–21 h).
// A body that ends inside the JSON is the same stream abort as an empty one.
func TestGraphQLRetriesAfterTruncatedBody200(t *testing.T) {
	var attempts atomic.Int32
	const goodBody = `{"data":{"hello":"world"}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if n == 1 {
			_, _ = w.Write([]byte(`{"data":{"repository":{"pr0":{"number":1,"tit`))
			return
		}
		_, _ = w.Write([]byte(goodBody))
	}))
	defer server.Close()

	keys := NewKeyPool([]string{"test-token"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c := NewHTTPClient(server.URL, keys, slog.New(slog.NewTextHandler(io.Discard, nil)), AuthGitHub)
	var got struct {
		Hello string `json:"hello"`
	}
	if err := c.GraphQL(context.Background(), "{ hello }", nil, &got); err != nil {
		t.Fatalf("GraphQL failed after a truncated 200 body: %v (a body ending inside the JSON must be retried like an empty one)", err)
	}
	if got.Hello != "world" || attempts.Load() != 2 {
		t.Errorf("got %q after %d attempts; want \"world\" after exactly 2", got.Hello, attempts.Load())
	}
}

// TestGraphQLDoesNotRetryAMalformedCompleteBody: a body that is complete but
// not JSON (an HTML error page) is not a truncation and is not retried —
// only a body that ENDS inside a JSON value is (the retry sub-budget is for
// stream aborts, not wire-format errors).
func TestGraphQLDoesNotRetryAMalformedCompleteBody(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<html>oops</html>`))
	}))
	defer server.Close()
	keys := NewKeyPool([]string{"test-token"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c := NewHTTPClient(server.URL, keys, slog.New(slog.NewTextHandler(io.Discard, nil)), AuthGitHub)
	if err := c.GraphQL(context.Background(), "{ hello }", nil, nil); err == nil {
		t.Fatal("a malformed complete body must fail")
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("a malformed complete body was attempted %d times; want 1 (not a stream abort)", n)
	}
}

// TestGraphQLDoesNotRetryATruncatedResourceLimitAnswer: GitHub sometimes
// refuses a too-expensive query with a TRUNCATED body that carries
// RESOURCE_LIMITS_EXCEEDED (the 2026-09-05 chaoss.tv shape the history
// sweep subdivides on). That is the server's answer about the query, not a
// stream abort: a retry gets the same answer and spends the read budget
// (worklist 66, found by the full suite after the truncation retry landed).
func TestGraphQLDoesNotRetryATruncatedResourceLimitAnswer(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"user":null},"errors":[{"type":"RESOURCE_LIMITS_EXCEEDED","path":["user",0,`))
	}))
	defer server.Close()
	keys := NewKeyPool([]string{"test-token"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c := NewHTTPClient(server.URL, keys, slog.New(slog.NewTextHandler(io.Discard, nil)), AuthGitHub)
	err := c.GraphQL(context.Background(), "{ hello }", nil, nil)
	if err == nil {
		t.Fatal("a truncated resource-limit answer must fail")
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("a truncated resource-limit answer was attempted %d times; want 1 (the answer, not a stream abort)", n)
	}
	// PR #218 review E1: the answer classifies exactly like the complete-body
	// RESOURCE_LIMITS_EXCEEDED answer — ClassTransient wrapping
	// ErrResourceLimits — so every subdivision caller (the PR batch as well
	// as the history sweep) halves the query instead of failing on a
	// ClassFatal decode error.
	if !errors.Is(err, ErrResourceLimits) || ClassifyError(err) != ClassTransient {
		t.Errorf("error %v classifies %v; want ClassTransient wrapping ErrResourceLimits (the complete-body RLE answer's class)", err, ClassifyError(err))
	}
}

// TestJSONTruncatedOnlyForEndOfInput (final review F2 of v0.29.69):
// encoding/json reports a bad FINAL byte at offset == len too, so an
// offset test read a complete malformed body as a stream abort and retried
// it. Only a body that runs out inside a value is truncated.
func TestJSONTruncatedOnlyForEndOfInput(t *testing.T) {
	for _, tc := range []struct {
		body string
		want bool
	}{
		{`{"data":{"repository":{"pr0":{"number":1,"tit`, true},
		{`{"a":`, true},
		{`[1,2`, true},
		{`{"a":1,}`, false},
		{`{"data":{}}}`, false},
		{`{"a":1]`, false},
		{`{"data":{}}`, false},
		{`<html>oops</html>`, false},
		{``, false},
	} {
		if got := jsonTruncated([]byte(tc.body)); got != tc.want {
			t.Errorf("jsonTruncated(%q) = %v; want %v", tc.body, got, tc.want)
		}
	}
}

// TestGraphQLDoesNotRetryABodyMalformedAtItsLastByte: the client-level
// twin — a complete body whose one bad byte is its last fails at decode,
// once.
func TestGraphQLDoesNotRetryABodyMalformedAtItsLastByte(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"hello":"world"},}`))
	}))
	defer server.Close()
	keys := NewKeyPool([]string{"test-token"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c := NewHTTPClient(server.URL, keys, slog.New(slog.NewTextHandler(io.Discard, nil)), AuthGitHub)
	if err := c.GraphQL(context.Background(), "{ hello }", nil, nil); err == nil {
		t.Fatal("a malformed complete body must fail")
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("a body malformed at its last byte was attempted %d times; want 1", n)
	}
}

// TestGraphQLRetriesATruncatedBodyThatMerelyMentionsTheMarker (final review
// F3 of v0.29.69): the resource-limit exemption keys on the errors entry's
// type field, not the word anywhere — an issue title naming
// RESOURCE_LIMITS_EXCEEDED in a cut-off PR batch is still a stream abort.
func TestGraphQLRetriesATruncatedBodyThatMerelyMentionsTheMarker(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		w.WriteHeader(http.StatusOK)
		if n == 1 {
			_, _ = w.Write([]byte(`{"data":{"repository":{"pr0":{"title":"Handle \"type\":\"RESOURCE_LIMITS_EXCEEDED\" and RESOURCE_LIMITS_EXCEEDED","bo`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"hello":"world"}}`))
	}))
	defer server.Close()
	keys := NewKeyPool([]string{"test-token"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c := NewHTTPClient(server.URL, keys, slog.New(slog.NewTextHandler(io.Discard, nil)), AuthGitHub)
	var got struct {
		Hello string `json:"hello"`
	}
	if err := c.GraphQL(context.Background(), "{ hello }", nil, &got); err != nil {
		t.Fatalf("a truncated body whose data mentions the marker was not retried: %v", err)
	}
	if attempts.Load() != 2 {
		t.Errorf("attempts = %d; want 2", attempts.Load())
	}
}

func TestCarriesResourceLimitsError(t *testing.T) {
	for _, tc := range []struct {
		body string
		want bool
	}{
		{`{"data":{"user":null},"errors":[{"type":"RESOURCE_LIMITS_EXCEEDED","path":["user",0,`, true},
		{`{"errors":[{"type" : "RESOURCE_LIMITS_EXCEEDED"}]}`, true},
		{`decode graphql envelope: unexpected end of JSON input (body: {"errors":[{"type":"RESOURCE_LIMITS_EXCEEDED","pa)`, true},
		{`{"data":{"t":"RESOURCE_LIMITS_EXCEEDED"}}`, false},
		{`{"data":{"t":"Handle \"type\":\"RESOURCE_LIMITS_EXCEEDED\""}}`, false},
		{`{"errors":[{"type":"RATE_LIMITED"}]}`, false},
	} {
		if got := CarriesResourceLimitsError([]byte(tc.body)); got != tc.want {
			t.Errorf("CarriesResourceLimitsError(%q) = %v; want %v", tc.body, got, tc.want)
		}
	}
}
