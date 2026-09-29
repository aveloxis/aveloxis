// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

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
	"sync"
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
		t.Errorf("unauthenticated-shape rate-limit body must log at ERROR "+
			"level — this is the signal of a key-leak bug. Log output was:\n%s", out)
	}
	if !strings.Contains(strings.ToLower(out), "unauthenticated") &&
		!strings.Contains(strings.ToLower(out), "anonymous") {
		t.Errorf("error log must name the condition (unauthenticated/anonymous) so "+
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

// TestGet_403RateLimitBodyLogsNameTheServingKey — v0.29.9 (operator
// decision, 2026-09-12): the headerless rate-limit 403 marks nothing on
// the key, and whether that matters depends on a question the log could
// not answer: does the SAME key come back with another such 403 inside
// GitHub's one-minute secondary-limit floor (the pool re-selecting a
// throttled key)? The chaoss.tv log held 745 of these lines in six days,
// all secondary-limit bodies on search/commits and search/users, and none
// said which key served them. Both body-classified arms must carry
// token_prefix — the same attribute the header-classified arms already
// log — so the next release's log review can group by key and time.
func TestGet_403RateLimitBodyLogsNameTheServingKey(t *testing.T) {
	for _, tc := range []struct {
		name, body, msg string
	}{
		{"headerless secondary-limit body — the production shape (WARN)", secondaryRateLimitBody,
			"403 with rate-limit body but no rate-limit headers"},
		{"unauthenticated rate-limit body (ERROR)", unauthenticatedIPRateLimitBody,
			"403 with unauthenticated rate-limit body"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var served atomic.Value
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					served.Store(strings.TrimPrefix(r.Header.Get("Authorization"), "token "))
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(tc.body))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			defer server.Close()

			logger, buf := captureLogger()
			keys := NewKeyPool([]string{"ghp_firstkey000", "ghp_secondkey00"}, logger)
			client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resp, err := client.Get(ctx, "/search/commits?q=author-email%3Ax%40y.org&per_page=1")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			resp.Body.Close()

			tok, _ := served.Load().(string)
			if tok == "" {
				t.Fatal("the 403 was never served")
			}
			var line string
			for _, l := range strings.Split(buf.String(), "\n") {
				if strings.Contains(l, tc.msg) {
					line = l
				}
			}
			if line == "" {
				t.Fatalf("no %q line logged; log:\n%s", tc.msg, buf.String())
			}
			if want := "token_prefix=" + tokenPrefix(tok); !strings.Contains(line, want) {
				t.Errorf("the %q line must name the key that served the 403 (%s); got:\n%s", tc.msg, want, line)
			}
			// attempt separates a caller's OWN retry re-leasing the throttled
			// key (same url, attempt 2, 3, ... — sequential, never an
			// in-flight overlap) from concurrent callers' overlapping 403s.
			// Without it the review's gap heuristic filed own retries
			// (1–12 s backoff) as "maybe in flight" (review of this change).
			if !strings.Contains(line, " attempt=1 ") {
				t.Errorf("the %q line must carry the 1-based attempt (attempt=1 on the first try); got:\n%s", tc.msg, line)
			}
		})
	}
}

// TestHeaderless403RateLimitBodyRestsTheKey (worklist 67): the v0.29.9
// decision rule was met on kate — 87 of 481 retry opportunities (18.1%)
// reused the SAME key against a ~1.9% baseline at 54 keys, because a
// headerless search 403 marked nothing and least-loaded selection handed
// the throttled key straight back. Option (a): the body is read under the
// lease and a rate-limit body rests the key for GitHub's documented 60 s
// floor (parseRetryAfter's default), so the retry — and every other
// caller — lands on another key. The unauthenticated-body shape is not the
// key's throttle (a key-leak bug) and does not rest it.
func TestHeaderless403RateLimitBodyRestsTheKey(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		rests      bool
	}{
		{"headerless secondary-limit body rests the key", secondaryRateLimitBody, true},
		{"unauthenticated body does not rest a key", unauthenticatedIPRateLimitBody, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var tokens []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				n := len(tokens)
				tokens = append(tokens, strings.TrimPrefix(r.Header.Get("Authorization"), "token "))
				mu.Unlock()
				if n == 0 {
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(tc.body))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			defer server.Close()
			logger, _ := captureLogger()
			keys := NewKeyPool([]string{"ghp_firstkey000", "ghp_secondkey00"}, logger)
			client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resp, err := client.Get(ctx, "/search/users?q=x%40y.org&per_page=1")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			resp.Body.Close()
			mu.Lock()
			served := append([]string(nil), tokens...)
			mu.Unlock()
			if len(served) != 2 {
				t.Fatalf("served %d requests; want the 403 then the retry", len(served))
			}
			resting := restingKeys(keys)
			if tc.rests {
				if !resting[served[0]] {
					t.Errorf("the key that drew the headerless rate-limit 403 is not resting in the pool (resting: %v)", resting)
				}
				if served[1] == served[0] {
					t.Errorf("the retry reused the throttled key %s; want the other key", served[1])
				}
			} else if len(resting) != 0 {
				t.Errorf("an unauthenticated-body 403 rested %v; it is not the key's throttle", resting)
			}
		})
	}
}

// TestGitLabHeaderless403RateLimitBodyDoesNotRestTheKey (PR #218 review E4):
// the headerless-403 rest is GitHub's documented secondary-limit rule
// (a rate-limit body with no Retry-After means "wait at least one minute").
// GitLab documents no such rule, so a GitLab 403 whose body happens to say
// "rate limit" must not bench a GitLab key for GitHub's floor; the response
// is still paced and retried by handleResponse as before.
func TestGitLabHeaderless403RateLimitBodyDoesNotRestTheKey(t *testing.T) {
	var mu sync.Mutex
	served := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n := served
		served++
		mu.Unlock()
		if n == 0 {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(secondaryRateLimitBody))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	logger, _ := captureLogger()
	keys := NewKeyPool([]string{"glpat-firstkey00", "glpat-secondkey0"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitLab)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, "/projects/1/issues?per_page=1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()
	if resting := restingKeys(keys); len(resting) != 0 {
		t.Errorf("a GitLab 403 rested %v on GitHub's headerless-403 rule; only a GitHub client applies it", resting)
	}
}

// TestGraphQLHeaderless403RateLimitBodyRestsTheKey (PR #218 review E3): the
// GraphQL twin of TestHeaderless403RateLimitBodyRestsTheKey. A GraphQL 403
// with a secondary-limit BODY but no Retry-After and no primary refusal
// returned ErrForbidden and rested nothing, so the next caller was handed
// the throttled key. It now reads the body under the lease, rests the key
// for GitHub's floor and retries on another key. The unauthenticated shape
// rests nothing (a key-leak bug, not this key's throttle) but is still
// retried, as REST does; a genuine permission 403 stays ErrForbidden.
func TestGraphQLHeaderless403RateLimitBodyRestsTheKey(t *testing.T) {
	restore := SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	for _, tc := range []struct {
		name, body string
		rests      bool
		forbidden  bool
	}{
		{"secondary-limit body rests the key and retries elsewhere", secondaryRateLimitBody, true, false},
		{"unauthenticated body rests nothing but is retried", unauthenticatedIPRateLimitBody, false, false},
		{"permission body stays ErrForbidden", privateRepoForbiddenBody, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var tokens []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				n := len(tokens)
				tokens = append(tokens, strings.TrimPrefix(r.Header.Get("Authorization"), "bearer "))
				mu.Unlock()
				if n == 0 {
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(tc.body))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"data":{"hello":"world"}}`))
			}))
			defer server.Close()
			logger, _ := captureLogger()
			keys := NewKeyPool([]string{"ghp_firstkey000", "ghp_secondkey00"}, logger)
			client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)
			var got struct {
				Hello string `json:"hello"`
			}
			err := client.GraphQL(context.Background(), "{ hello }", nil, &got)
			mu.Lock()
			served := append([]string(nil), tokens...)
			mu.Unlock()
			resting := restingKeys(keys)
			if tc.forbidden {
				if !errors.Is(err, ErrForbidden) || len(served) != 1 || len(resting) != 0 {
					t.Errorf("err=%v served=%d resting=%v; want ErrForbidden after one request, nothing rested", err, len(served), resting)
				}
				return
			}
			if err != nil || got.Hello != "world" {
				t.Fatalf("GraphQL: err=%v hello=%q; want the retry to succeed", err, got.Hello)
			}
			if len(served) != 2 {
				t.Fatalf("served %d requests; want the 403 then one retry", len(served))
			}
			if tc.rests {
				if !resting[served[0]] {
					t.Errorf("the key that drew the headerless rate-limit 403 is not resting (resting: %v)", resting)
				}
				if served[1] == served[0] {
					t.Errorf("the retry reused the throttled key %s; want the other key", served[1])
				}
			} else if len(resting) != 0 {
				t.Errorf("an unauthenticated-body 403 rested %v; it is not the key's throttle", resting)
			}
		})
	}
}

// TestGitLabGraphQLHeaderless403RestsNothing (PR #218 fix review r1): the
// GraphQL twin of TestGitLabHeaderless403RateLimitBodyDoesNotRestTheKey — the
// headerless rate-limit 403 rule is GitHub's (E4), so a GitLab GraphQL 403
// whose body mentions a rate limit rests no key and stays ErrForbidden after
// one request.
func TestGitLabGraphQLHeaderless403RestsNothing(t *testing.T) {
	restore := SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	var mu sync.Mutex
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(secondaryRateLimitBody))
	}))
	defer server.Close()
	logger, _ := captureLogger()
	keys := NewKeyPool([]string{"glpat-firstkey00", "glpat-secondkey0"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitLab)
	err := client.GraphQL(context.Background(), "{ hello }", nil, nil)
	mu.Lock()
	n := requests
	mu.Unlock()
	if resting := restingKeys(keys); len(resting) != 0 {
		t.Errorf("a GitLab GraphQL 403 rested %v on GitHub's rule", resting)
	}
	if !errors.Is(err, ErrForbidden) || n != 1 {
		t.Errorf("err=%v after %d requests; want ErrForbidden after one", err, n)
	}
}
