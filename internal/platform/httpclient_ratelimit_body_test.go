package platform

import (
	"bytes"
	"context"
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

// Response bodies observed in the wild. These are verbatim from real GitHub
// responses — preserved as constants so the detection logic stays pinned to
// the actual strings GitHub returns, not a paraphrase.
const (
	// unauthenticatedIPRateLimitBody is what GitHub returns when a request
	// hits the 60/hr anonymous limit. Observed on 2026-04-18 in the tinkerbell
	// investigation. The tell-tale phrase is "authenticated requests get a
	// higher rate limit" — this shape of body means our server is making an
	// anonymous call, which is a bug we want to detect and log loudly.
	unauthenticatedIPRateLimitBody = `{"message":"API rate limit exceeded for 161.130.189.234. (But here's the good news: Authenticated requests get a higher rate limit. Check out the documentation for more details.)","documentation_url":"https://docs.github.com/rest/overview/resources-in-the-rest-api#rate-limiting"}`

	// authenticatedUserRateLimitBody is what GitHub returns when an
	// authenticated token hits its hourly 5000 limit. The message names the
	// user instead of the IP. Still a rate-limit, must be handled by waiting
	// for the reset window, not by returning ErrForbidden.
	authenticatedUserRateLimitBody = `{"message":"API rate limit exceeded for user ID 12345.","documentation_url":"https://docs.github.com/rest/overview/resources-in-the-rest-api#rate-limiting"}`

	// secondaryRateLimitBody is GitHub's abuse-detection throttle. Typically
	// paired with a Retry-After header, but we want body-text fallback in
	// case the header is dropped by a proxy.
	secondaryRateLimitBody = `{"message":"You have exceeded a secondary rate limit. Please wait a few minutes before you try again.","documentation_url":"https://docs.github.com/rest/overview/resources-in-the-rest-api#secondary-rate-limits"}`

	// privateRepoForbiddenBody is a genuine ErrForbidden case — no rate limit
	// language, just insufficient permissions. Must not be misclassified as
	// rate-limited or we'd spin forever waiting for a non-existent reset.
	privateRepoForbiddenBody = `{"message":"Must have admin rights to Repository.","documentation_url":"https://docs.github.com/rest"}`
)

// TestGet_403WithRateLimitBodyWithoutHeaders_ClassifiedAsRateLimit covers the
// defense-in-depth case: a 403 arrives with "API rate limit exceeded" in the
// body but WITHOUT Retry-After or X-RateLimit-Remaining=0 in the headers
// (observed when a proxy strips headers, or on certain secondary-limit
// paths). Headers are still consulted FIRST — this test exercises the
// fallback path only. The current code only reads headers, so it falls
// through to ErrForbidden and the collector stops instead of waiting.
// With the body-as-fallback added, we want it to detect the rate-limit
// language and treat the response as rate-limited.
func TestGet_403WithRateLimitBodyWithoutHeaders_ClassifiedAsRateLimit(t *testing.T) {
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n == 1 {
			// First attempt: 403 with rate-limit body, no headers.
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(authenticatedUserRateLimitBody))
			return
		}
		// Subsequent attempts succeed — proves the client waited and retried
		// rather than returning ErrForbidden on the first hit.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"tok1", "tok2"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	// Use a short-lived context so the test doesn't hang if the implementation
	// waits for a minutes-long reset window. The client should recognize the
	// rate-limit quickly and rotate to tok2 (or wait a short jittered backoff
	// if no reset header is available).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Get(ctx, "/repos/o/r/pulls/457")
	if err != nil {
		t.Fatalf("expected Get to retry past the rate-limited 403, got err=%v", err)
	}
	resp.Body.Close()
	if errors.Is(err, ErrForbidden) {
		t.Error("rate-limit-body 403 must not be classified as ErrForbidden — " +
			"ErrForbidden is reserved for permission/visibility denials, not throttles")
	}
	if callCount.Load() < 2 {
		t.Errorf("expected at least 2 server hits (retry after rate-limit body), got %d", callCount.Load())
	}
}

// TestGet_403WithUnauthenticatedBodyLogsError verifies we loudly surface the
// "for <IP>" rate-limit message. That body shape only appears on anonymous
// requests, which our code path should never make — 73 API keys are loaded
// at startup and c.keys.GetKey is always called before Do. If this body
// reaches us, something has leaked an unauthenticated request into an
// authenticated client. Silent misclassification would hide the bug; the
// log must fire at ERROR level so ops can catch the regression.
func TestGet_403WithUnauthenticatedBodyLogsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(unauthenticatedIPRateLimitBody))
	}))
	defer server.Close()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	keys := NewKeyPool([]string{"tok1"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// We don't care whether Get eventually returns success or error; we care
	// that the suspicious body was logged at ERROR level.
	_, _ = client.Get(ctx, "/repos/o/r/pulls/457")

	out := logBuf.String()
	if !strings.Contains(out, "level=ERROR") {
		t.Errorf("unauthenticated-shape rate-limit body must log at ERROR " +
			"level — this is the signal of a key-leak bug. Log output was:\n%s", out)
	}
	if !strings.Contains(strings.ToLower(out), "unauthenticated") &&
		!strings.Contains(strings.ToLower(out), "anonymous") {
		t.Errorf("error log must name the condition (unauthenticated/anonymous) so " +
			"on-call can route the page. Log output was:\n%s", out)
	}
}

// TestGet_403HeadersTakePrecedenceOverBody pins the ordering contract:
// when authoritative rate-limit headers are present (Retry-After, or
// X-RateLimit-Remaining=0), the client must act on the header signal
// without needing to inspect the body. Body inspection is the fallback
// net — it must never short-circuit the header path. Specifically, the
// Retry-After handler has always slept for the header-supplied duration;
// swapping to body-first would break that precise timing and could
// misclassify a Retry-After-carrying 403 that happens to also include
// rate-limit language in the body (GitHub does this sometimes).
func TestGet_403HeadersTakePrecedenceOverBody(t *testing.T) {
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n == 1 {
			// 403 with Retry-After header AND the same body. The handler
			// must take its cue from the header (1-second wait) and retry,
			// not from the body.
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(secondaryRateLimitBody))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"tok1"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	resp, err := client.Get(ctx, "/repos/o/r")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("expected Get to succeed after Retry-After wait, got err=%v", err)
	}
	resp.Body.Close()

	// The Retry-After was "1" (second). If the body path overrode the header,
	// we would have rotated immediately with zero wait. Assert that we did
	// observe the header-directed sleep — at least several hundred ms.
	if elapsed < 500*time.Millisecond {
		t.Errorf("expected client to honor Retry-After header (waited >=500ms), got %v — "+
			"body-based detection must not short-circuit the header path", elapsed)
	}
}

// TestGet_403PrivateRepoStillErrForbidden pins the negative case: a 403 with
// no rate-limit body language must still produce ErrForbidden. Without this
// guardrail the body-detection would overfire and every private-repo
// 403 would be treated as a throttle — infinite retry loop.
func TestGet_403PrivateRepoStillErrForbidden(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(privateRepoForbiddenBody))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"tok1"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := client.Get(ctx, "/repos/o/r/pulls/457")
	if err == nil {
		t.Fatal("expected error for private-repo 403")
	}
	if !errors.Is(err, ErrForbidden) {
		t.Errorf("private-repo 403 must surface as ErrForbidden, got %v", err)
	}
}

// TestIsRateLimitBody is a unit test for the pure helper that decides whether
// a response body indicates a rate limit. Keeps the substring list small and
// testable; easier to extend when GitHub adds new message shapes.
func TestIsRateLimitBody(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{"empty", "", false},
		{"unauth-ip", unauthenticatedIPRateLimitBody, true},
		{"auth-user", authenticatedUserRateLimitBody, true},
		{"secondary", secondaryRateLimitBody, true},
		{"private-repo", privateRepoForbiddenBody, false},
		{"random-403", `{"message":"Not allowed"}`, false},
		{"case-insensitive", `{"message":"RATE LIMIT EXCEEDED"}`, true},
		{"secondary-phrase", `{"message":"secondary rate limit"}`, true},
		{"unrelated-limit", `{"message":"File size limit exceeded"}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRateLimitBody([]byte(tt.body))
			if got != tt.want {
				t.Errorf("isRateLimitBody(%q) = %v, want %v", tt.body, got, tt.want)
			}
		})
	}
}

// TestIsAnonymousRateLimitBody isolates the narrow "unauthenticated shape"
// detector. Must NOT fire on generic rate-limit messages, only on the
// unambiguous "for <IP>" / "authenticated requests get a higher" wording.
func TestIsAnonymousRateLimitBody(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{"empty", "", false},
		{"unauth-ip", unauthenticatedIPRateLimitBody, true},
		{"auth-user", authenticatedUserRateLimitBody, false},
		{"secondary", secondaryRateLimitBody, false},
		{"private-repo", privateRepoForbiddenBody, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isAnonymousRateLimitBody([]byte(tt.body))
			if got != tt.want {
				t.Errorf("isAnonymousRateLimitBody(%q) = %v, want %v", tt.body, got, tt.want)
			}
		})
	}
}
