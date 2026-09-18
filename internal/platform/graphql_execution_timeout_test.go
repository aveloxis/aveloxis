// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

// graphql_execution_timeout_test.go — v0.29.56. GitHub answers a query
// its resolver could not finish with HTTP 200 and ONE typeless
// top-level error, "Something went wrong while executing your query …
// Please include `ID` when reporting this issue." (GitHub's own text
// for it adds "This may be the result of a timeout"). It classified
// ClassFatal, so nothing retried or subdivided it: the contributor
// activity sweep failed 7 of 7 ticks on 2026-09-17 with that error and
// production had checked nobody since 2026-09-14 22:47 UTC. A live
// probe the same day ran the stuck 2,500-login batch in its production
// 25-login chunks from a separate token: 100 of 100 succeeded, so the
// failure is intermittent (load), not a property of any login.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

const githubExecutionTimeoutBody = `{"data":null,"errors":[{"message":"Something went wrong while executing your query on 2026-09-17T15:51:52Z. Please include ` +
	"`BC62:2B6E7D:4C218F4:FDB59B2:6AAC0C8E`" + ` when reporting this issue."}]}`

func TestGraphQLExecutionTimeoutClassifiesTransient(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := parseGraphQLResponse([]byte(githubExecutionTimeoutBody), nil, logger)
	if err == nil {
		t.Fatal("an execution-timeout body must produce an error")
	}
	if got := ClassifyError(err); got != ClassTransient {
		t.Errorf("execution timeout classified %v, want ClassTransient (retry, then subdivide)", got)
	}
	if !errors.Is(err, ErrGraphQLExecutionTimeout) {
		t.Errorf("execution timeout must wrap ErrGraphQLExecutionTimeout, got %v", err)
	}
	// Halving provably helps only resource limits; a timeout is not that
	// signal, so the resource-limits gate must not fire on it.
	if errors.Is(err, ErrResourceLimits) {
		t.Error("an execution timeout must not wrap ErrResourceLimits")
	}

	// Other typeless top-level errors stay fatal: a malformed query does
	// not get better on retry.
	parseErr := parseGraphQLResponse([]byte(`{"errors":[{"message":"Parse error on \"}\" (RCURLY) at [1, 9]"}]}`), nil, logger)
	if got := ClassifyError(parseErr); got != ClassFatal {
		t.Errorf("a parse error classified %v, want ClassFatal", got)
	}
	// A typed error whose message happens to contain the phrase is not the
	// timeout (the type says what it is).
	typed := parseGraphQLResponse([]byte(`{"errors":[{"type":"FORBIDDEN","message":"Something went wrong while executing your query"}]}`), nil, logger)
	if errors.Is(typed, ErrGraphQLExecutionTimeout) {
		t.Error("a typed error must not be read as the execution timeout")
	}
}

// TestGraphQLExecutionTimeoutHoistsFromPerPath: GitHub reports GLOBAL
// conditions as per-path entries (the v0.27.79 incident, where every
// contributionsCollection alias carried a per-path RESOURCE_LIMITS_EXCEEDED
// and the tolerance path stamped 216,000 contributors "checked, no data").
// The execution timeout must hoist the same way: left in the partial arm it
// would be logged and swallowed, the aliases would decode as null, and the
// sweep would read them as ABSENT and mark-stamp them for a whole cooldown.
func TestGraphQLExecutionTimeoutHoistsFromPerPath(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	body := []byte(`{"data":{"u0":null,"u1":null},"errors":[
		{"path":["u0"],"message":"Something went wrong while executing your query on 2026-09-17T15:51:52Z. Please include ` + "`ABC`" + ` when reporting this issue."},
		{"path":["u1"],"message":"Something went wrong while executing your query on 2026-09-17T15:51:52Z. Please include ` + "`ABC`" + ` when reporting this issue."}
	]}`)
	var dest map[string]json.RawMessage
	err := parseGraphQLResponse(body, &dest, logger)
	if err == nil {
		t.Fatal("a per-path execution timeout must fail the query, not decode as null nodes")
	}
	if !errors.Is(err, ErrGraphQLExecutionTimeout) || ClassifyError(err) != ClassTransient {
		t.Errorf("err = %v (class %v), want the transient execution timeout", err, ClassifyError(err))
	}
	// A genuine per-path NOT_FOUND still tolerates: the other aliases' data
	// is kept and the missing node is simply absent.
	dest = nil
	notFound := []byte(`{"data":{"u0":{"login":"a"},"u1":null},"errors":[
		{"type":"NOT_FOUND","path":["u1"],"message":"Could not resolve to a User with the login of 'gone'."}
	]}`)
	if err := parseGraphQLResponse(notFound, &dest, logger); err != nil {
		t.Errorf("a per-path NOT_FOUND must still be tolerated: %v", err)
	}
	if _, ok := dest["u0"]; !ok {
		t.Error("the surviving alias's data must be decoded")
	}
}

// TestGraphQLRetriesExecutionTimeout: one intermittent timeout is retried
// on a fresh attempt like a 5xx, so the caller never sees it.
func TestGraphQLRetriesExecutionTimeout(t *testing.T) {
	restore := SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			_, _ = io.WriteString(w, githubExecutionTimeoutBody)
			return
		}
		_, _ = io.WriteString(w, `{"data":{"hello":"world"}}`)
	}))
	defer srv.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"k"}, logger), logger, AuthGitHub)
	var got struct {
		Hello string `json:"hello"`
	}
	if err := c.GraphQL(context.Background(), "{ hello }", nil, &got); err != nil {
		t.Fatalf("an intermittent execution timeout must be retried, got %v", err)
	}
	if got.Hello != "world" || hits.Load() != 2 {
		t.Errorf("hello=%q hits=%d, want the second attempt's data", got.Hello, hits.Load())
	}
}

// TestGraphQLExecutionTimeoutExhaustionIsTransient: a timeout on every
// attempt ends as a transient error (the subdivision callers' signal),
// spending the caller's budget, never more.
func TestGraphQLExecutionTimeoutExhaustionIsTransient(t *testing.T) {
	restore := SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, githubExecutionTimeoutBody)
	}))
	defer srv.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"k"}, logger), logger, AuthGitHub)
	err := c.GraphQL(WithGraphQLFastFail(context.Background()), "{ hello }", nil, nil)
	if err == nil {
		t.Fatal("a persistent execution timeout must fail")
	}
	if got := ClassifyError(err); got != ClassTransient {
		t.Errorf("exhausted execution timeouts classified %v, want ClassTransient", got)
	}
	if !errors.Is(err, ErrGraphQLExecutionTimeout) {
		t.Errorf("the exhaustion error must still name the execution timeout, got %v", err)
	}
	if n := hits.Load(); n != graphqlFastFailRetries {
		t.Errorf("hits = %d, want the fast-fail budget (%d)", n, graphqlFastFailRetries)
	}
}

// TestExhaustedErrorNamesTheLatestCause — v0.29.57 (Copilot review round 1
// on PR #210). A rate limit on the FINAL attempt rotates to a fresh key and
// undoes the budget spend, so the replacement request reuses that same
// attempt index. If the replacement then hits an execution timeout, both
// markers equal budget-1 — and the rate-limit arm was checked first, so the
// caller was told "rate limited" when the attempt that actually exhausted
// the budget timed out. The two classes drive different behaviour:
// subdivision callers halve on a timeout and defer on a rate limit, so the
// wrong one leaves a stuck batch stuck.
func TestExhaustedErrorNamesTheLatestCause(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	defer SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })()

	const rateLimitedBody = `{"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded"}]}`
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The default budget is maxRetries, so the final attempt index is
		// maxRetries-1 and it is reached on request maxRetries. That
		// request is rate-limited, which rotates to the second key and
		// undoes the budget spend — so request maxRetries+1 REUSES the
		// final index and times out. That timeout is what exhausts the
		// budget, and it is what the caller must be told.
		if n.Add(1) == maxRetries {
			_, _ = io.WriteString(w, rateLimitedBody)
			return
		}
		_, _ = io.WriteString(w, githubExecutionTimeoutBody)
	}))
	defer srv.Close()

	// Two keys, so the rate limit can rotate rather than give up. Rotation
	// is disabled under fast-fail, so this uses the default budget.
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"k1", "k2"}, logger), logger, AuthGitHub)
	var dest map[string]json.RawMessage
	err := c.GraphQL(context.Background(), "query {viewer{login}}", nil, &dest)

	if err == nil {
		t.Fatal("an exhausted budget must return an error")
	}
	if !errors.Is(err, ErrGraphQLExecutionTimeout) {
		t.Errorf("exhausted error = %v, want the execution timeout that burned the final attempt", err)
	}
	if got := ClassifyError(err); got != ClassTransient {
		t.Errorf("ClassifyError = %v, want ClassTransient so subdivision callers halve", got)
	}
}
