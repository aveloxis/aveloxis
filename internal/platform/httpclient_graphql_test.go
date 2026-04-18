package platform

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestGraphQL_BasicQuery verifies HTTPClient.GraphQL sends a POST with the
// query body, reads back the "data" field, and decodes into the destination.
// This is the minimum viable contract; every subsequent test exercises
// edge cases on top of it.
func TestGraphQL_BasicQuery(t *testing.T) {
	// dest's shape corresponds to the *contents* of the "data" field in
	// the response; HTTPClient.GraphQL unwraps the {"data":...} envelope.
	// Callers shouldn't have to re-declare the envelope in every dest type.
	type result struct {
		Viewer struct {
			Login string `json:"login"`
		} `json:"viewer"`
	}

	var gotMethod, gotAuth, gotCT string
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"viewer":{"login":"octocat"}}}`))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"secret-token"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	var r result
	err := client.GraphQL(context.Background(), `{ viewer { login } }`, nil, &r)
	if err != nil {
		t.Fatalf("GraphQL: %v", err)
	}

	if gotMethod != "POST" {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	// GraphQL must use "bearer" token format; classic PATs without GraphQL
	// scope fall back to REST-only, but our auth header has to be bearer
	// either way for the GraphQL endpoint to accept the token.
	if !strings.HasPrefix(strings.ToLower(gotAuth), "bearer ") {
		t.Errorf("Authorization = %q, want 'bearer <token>'", gotAuth)
	}
	if !strings.Contains(gotAuth, "secret-token") {
		t.Errorf("Authorization missing token, got %q", gotAuth)
	}
	if gotCT != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotCT)
	}
	// Body must be a JSON object with a "query" field carrying the query.
	var parsed map[string]any
	if err := json.Unmarshal(gotBody, &parsed); err != nil {
		t.Fatalf("body not valid JSON: %v\n%s", err, gotBody)
	}
	if _, ok := parsed["query"]; !ok {
		t.Errorf("body missing 'query' field: %s", gotBody)
	}
	if r.Viewer.Login != "octocat" {
		t.Errorf("Viewer.Login = %q, want octocat (GraphQL helper should unwrap the data envelope automatically)", r.Viewer.Login)
	}
}

// TestGraphQL_WithVariables verifies that GraphQL variables are marshaled
// into the POST body alongside the query. Required for any parameterized
// query (which is every useful one).
func TestGraphQL_WithVariables(t *testing.T) {
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{}}`))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	vars := map[string]any{
		"owner":   "chaoss",
		"repo":    "augur",
		"numbers": []int{123, 456, 789},
	}
	var dst map[string]any
	if err := client.GraphQL(context.Background(), `query ($owner: String!) { x }`, vars, &dst); err != nil {
		t.Fatalf("GraphQL: %v", err)
	}

	var parsed struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables"`
	}
	if err := json.Unmarshal(gotBody, &parsed); err != nil {
		t.Fatalf("body not valid JSON: %v\n%s", err, gotBody)
	}
	if parsed.Variables == nil {
		t.Fatalf("variables missing from body: %s", gotBody)
	}
	if got := parsed.Variables["owner"]; got != "chaoss" {
		t.Errorf("variables.owner = %v, want chaoss", got)
	}
	if got := parsed.Variables["repo"]; got != "augur" {
		t.Errorf("variables.repo = %v, want augur", got)
	}
	// Slices survive JSON roundtrip as []any.
	nums, ok := parsed.Variables["numbers"].([]any)
	if !ok || len(nums) != 3 {
		t.Errorf("variables.numbers = %v, want 3-element slice", parsed.Variables["numbers"])
	}
}

// TestGraphQL_RespectsBaseURL is the refactor's whole point: today the
// hardcoded "https://api.github.com/graphql" makes unit-testing GraphQL
// impossible. The refactored helper must use the HTTPClient's baseURL
// so tests can intercept via httptest.
func TestGraphQL_RespectsBaseURL(t *testing.T) {
	var hit atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/graphql" {
			hit.Store(true)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{}}`))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	var dst map[string]any
	_ = client.GraphQL(context.Background(), `{ x }`, nil, &dst)

	if !hit.Load() {
		t.Error("GraphQL helper did not call the test server — it's hardcoding a URL instead of using HTTPClient.baseURL. Tests cannot intercept.")
	}
}

// TestGraphQL_RetriesOn5xx verifies the GraphQL helper inherits HTTPClient's
// retry behavior. Without this, a one-off 502 on the GraphQL endpoint would
// fail a whole PR batch; the existing graphqlRequest had no retry at all.
func TestGraphQL_RetriesOn5xx(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := count.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"ok":true}}`))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var dst map[string]any
	if err := client.GraphQL(ctx, `{ ok }`, nil, &dst); err != nil {
		t.Fatalf("expected retry to succeed after 502, got %v", err)
	}
	if count.Load() < 2 {
		t.Errorf("expected at least 2 request attempts (retry after 5xx), got %d", count.Load())
	}
}

// TestGraphQL_SurfacesErrorsField — GraphQL replies with HTTP 200 even when
// the query fails; the "errors" array in the response is the real signal.
// The helper must surface these as Go errors so callers don't silently
// treat a partial/failed response as success. GitHub returns rate-limit
// exhaustion and permission errors this way.
func TestGraphQL_SurfacesErrorsField(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":null,"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded"}]}`))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var dst map[string]any
	err := client.GraphQL(ctx, `{ x }`, nil, &dst)
	if err == nil {
		t.Fatal("GraphQL errors array must surface as a Go error — silent success would silently drop data")
	}
	// The error message must mention something specific so the operator can
	// diagnose. Empty or generic "graphql failed" wouldn't distinguish
	// rate-limit from permission from parse error.
	if !strings.Contains(err.Error(), "RATE_LIMITED") &&
		!strings.Contains(err.Error(), "rate limit") {
		t.Errorf("GraphQL error should include the type/message from the errors array, got: %v", err)
	}
}

// TestGraphQL_RateLimitedClassifiesCorrectly — RATE_LIMITED in the errors
// array must map to platform.ClassRateLimit via ClassifyError, so callers
// can treat it consistently with HTTP-level rate limits (wait, don't fail).
func TestGraphQL_RateLimitedClassifiesCorrectly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":null,"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded"}]}`))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var dst map[string]any
	err := client.GraphQL(ctx, `{ x }`, nil, &dst)
	if err == nil {
		t.Fatal("expected error")
	}
	if got := ClassifyError(err); got != ClassRateLimit {
		t.Errorf("GraphQL RATE_LIMITED should classify as ClassRateLimit, got %v (err=%v)", got, err)
	}
}

// TestGraphQL_NotFoundClassifiesCorrectly — GitHub GraphQL returns
// `type: "NOT_FOUND"` in the errors array when a queried node (repo, PR,
// issue) doesn't exist. Must map to ClassSkip so gap-fill and refresh
// loops treat it as a routine skip instead of fatal.
func TestGraphQL_NotFoundClassifiesCorrectly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"repository":null},"errors":[{"type":"NOT_FOUND","message":"Could not resolve to a Repository"}]}`))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var dst map[string]any
	err := client.GraphQL(ctx, `{ x }`, nil, &dst)
	if err == nil {
		t.Fatal("expected error")
	}
	if got := ClassifyError(err); got != ClassSkip {
		t.Errorf("GraphQL NOT_FOUND should classify as ClassSkip, got %v (err=%v)", got, err)
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GraphQL NOT_FOUND should wrap ErrNotFound for errors.Is compatibility, got %v", err)
	}
}

// TestGraphQL_ContextCancellation — a cancelled context during a
// GraphQL retry backoff must return promptly, same as the REST Get path.
// Setup: server returns 502 so the client enters its backoff-and-retry
// loop. We cancel ctx mid-backoff and verify the client exits fast.
func TestGraphQL_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	// Backoff on first attempt is ~1s. Cancel after 500ms — we should be
	// inside the backoff sleep when ctx fires, and the ctx-aware select
	// in the retry loop should return immediately.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	var dst map[string]any
	err := client.GraphQL(ctx, `{ x }`, nil, &dst)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected cancellation error")
	}
	// Total elapsed: 1st attempt (fast, 502), backoff interrupted by ctx
	// at ~500ms, return. Allow generous upper bound for CI variance.
	if elapsed > 2*time.Second {
		t.Errorf("cancellation took %v — ctx-aware retry sleeps not effective on GraphQL path", elapsed)
	}
	if got := ClassifyError(err); got != ClassTransient {
		t.Errorf("cancellation should classify as ClassTransient, got %v (err=%v)", got, err)
	}
}
