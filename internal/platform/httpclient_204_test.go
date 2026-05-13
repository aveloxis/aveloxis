// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"errors"
	"iter"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
)

// v0.20.6 (Fix D): GitHub returns 204 No Content for several
// legitimate cases that the pre-v0.20.6 HTTPClient treated as
// "unexpected status" and retried 10 times before giving up with
// "exhausted 10 retries":
//
//   - /repos/{owner}/{repo}/contributors when the repo has zero
//     commits (empty / archived repo).
//   - /repos/{owner}/{repo}/contributors when the repo has 5,000+
//     contributors (GitHub gives up enumerating).
//   - Other endpoints where the semantic answer is "no content."
//
// In the May 9–12 production log, 1,430 "unexpected status 204"
// warnings burned 10 retries each across 143 unique repos, each
// adding ~110 seconds of backoff and one full retry budget of
// API requests. The fix: treat 204 as success with an empty
// body. The pagination engine and the iterator-returning
// collector helpers must observe the empty body as zero items
// and complete cleanly.

// TestGet_204IsTreatedAsSuccess pins the new HTTP-layer contract:
// a 204 response must NOT retry, must NOT return "exhausted
// retries", and must NOT return ErrNotFound or ErrForbidden.
// The simplest acceptable shape is to return either (nil, nil)
// or (resp-with-empty-body, nil) and let the caller observe the
// empty body. The Get test below pins the no-retry-and-no-error
// contract; the pagination test below pins the empty-iteration
// observability.
func TestGet_204IsTreatedAsSuccess(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	keys := NewKeyPool([]string{"test-token"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	resp, err := client.Get(context.Background(), "/repos/x/y/contributors?per_page=100")
	if err != nil && !errors.Is(err, ErrNoContent) {
		t.Errorf("Get returning 204 must succeed (or surface ErrNoContent), got err=%v — pre-v0.20.6 it returned 'exhausted 10 retries' after burning 10 retry attempts on a single legitimate empty response", err)
	}
	if resp != nil {
		resp.Body.Close()
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("204 must NOT retry: got %d hits, want exactly 1. Pre-v0.20.6 the HTTPClient burned the full %d-retry budget on each 204, wasting API quota and adding ~110s of backoff per call", got, maxRetries)
	}
}

// TestPaginate_204YieldsZeroItems pins the pagination-engine
// contract: a 204 on page 1 must end the iteration cleanly with
// zero yielded items, not with an error. Callers that range over
// ListContributors etc. observe "empty result" and continue with
// the rest of the repo collection.
func TestPaginate_204YieldsZeroItems(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	keys := NewKeyPool([]string{"test-token"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	type fakeContributor struct {
		Login string `json:"login"`
	}
	seq := PaginateGitHub[fakeContributor](context.Background(), client, "/repos/x/y/contributors")
	count := 0
	var lastErr error
	var fn func() = func() {
		next, stop := iter.Pull2(seq)
		defer stop()
		for {
			_, err, ok := next()
			if !ok {
				return
			}
			if err != nil {
				lastErr = err
				return
			}
			count++
		}
	}
	fn()

	if lastErr != nil {
		t.Errorf("PaginateGitHub on 204 must yield zero items with no error, got lastErr=%v", lastErr)
	}
	if count != 0 {
		t.Errorf("PaginateGitHub on 204 must yield 0 items, got %d", count)
	}
}

// TestGet_204DoesNotLogUnexpectedStatus is a regression pin: the
// pre-v0.20.6 code emitted a WARN line ("unexpected status
// status=204") on every 204 response, which spammed operator logs
// with thousands of false positives per cycle. After the fix, 204
// should be either silent or logged at DEBUG only. A captured-log
// assertion would be brittle across implementations; pin via the
// hits counter from above and a separate source check that the
// "unexpected status" code path doesn't include 204.
func TestGet_204IsNotLoggedAsUnexpectedStatus(t *testing.T) {
	data, err := os.ReadFile("httpclient.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// The new 204 case must appear in the status switch, syntactically
	// before the default arm that emits the "unexpected status" WARN.
	// We anchor the "default" search to the status-switch arm by
	// looking for the unique log message it emits ("unexpected status")
	// rather than the bare "default:" token (which also appears in the
	// unrelated AuthStyle switch above).
	// Anchor on the WARN call site (the actual log statement) rather
	// than the bare string literal — the literal appears in docstring
	// comments above ErrNoContent too.
	noContentIdx := indexOf(src, "case resp.StatusCode == http.StatusNoContent")
	unexpectedIdx := indexOf(src, `c.logger.Warn("unexpected status"`)
	if noContentIdx < 0 {
		t.Error("httpclient.go must handle http.StatusNoContent (204) explicitly — pre-v0.20.6 it fell into the default 'unexpected status' arm and retried")
	}
	if unexpectedIdx < 0 {
		t.Fatal(`httpclient.go missing "unexpected status" WARN — test cannot verify ordering`)
	}
	if noContentIdx > unexpectedIdx {
		t.Error("StatusNoContent case must appear BEFORE the 'unexpected status' default arm in the status switch so 204s don't fall through to the retry-and-WARN path")
	}
}

// indexOf is the source-search helper used by the regression pin
// above. Returns -1 if not found.
func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
