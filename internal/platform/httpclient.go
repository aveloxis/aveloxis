// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrNotModified is returned by GetConditional when the server returns 304.
// Callers should use their cached copy of the data.
var ErrNotModified = errors.New("not modified (304)")

// ErrNotFound wraps 404 responses from the forge API. Callers that want to
// treat a missing optional resource (e.g. /releases on a repo that never
// cut a release) as non-fatal can check errors.Is(err, ErrNotFound).
var ErrNotFound = errors.New("not found")

// ErrForbidden wraps 403 responses that are NOT rate-limit exhaustions
// (no Retry-After, non-zero X-RateLimit-Remaining). These usually mean the
// token can't see a particular resource — private GitLab project, repo
// with restricted visibility, endpoint requiring a scope the token lacks.
// Callers can check errors.Is(err, ErrForbidden) to skip the endpoint
// without failing the whole collection.
var ErrForbidden = errors.New("forbidden")

// ErrGone wraps 410 Gone responses and unfollowable 3xx redirects (the
// Location header was missing or the redirect chain looped). Distinct from
// ErrNotFound: 404 means "never existed or cannot see it", 410 means
// "existed and was deliberately removed". Callers can check errors.Is(err,
// ErrGone) to skip the resource without failing the whole collection.
var ErrGone = errors.New("gone")

// ErrNoContent wraps 204 No Content responses (v0.20.6). GitHub
// returns 204 for /repos/{owner}/{repo}/contributors when the repo
// has zero commits (empty / archived repo) or 5,000+ contributors
// (GitHub gives up enumerating). The pre-v0.20.6 HTTPClient treated
// 204 as "unexpected status" and burned the full 10-retry budget
// before giving up — wasting ~110s of backoff and 10 API requests
// per call. The fix: return ErrNoContent so the pagination engine
// observes it as a clean end-of-iteration with zero items. Pre-fix
// production logs (May 9–12) showed 1,430 such warnings across 143
// unique repos = ~5.3h of wasted wall-clock per cycle.
var ErrNoContent = errors.New("no content (204)")

// ErrTransient marks transient-class errors that should route
// to ClassTransient via platform.ClassifyError. Originally added
// v0.20.19 (Fix J) for "exhausted N retries" wrappers in
// HTTPClient.Get and HTTPClient.GraphQL; v0.25.0 also wraps it
// around single-attempt 5xx responses from the ecosyste.ms /
// deps.dev clients (which do NOT have an inner retry loop).
//
// Message rationale (v0.25.0): the sentinel text is intentionally
// generic ("transient") rather than the pre-v0.25.0
// "transient (retries exhausted)" so single-attempt wrappers
// don't claim retries that never happened. Retry-loop callers
// (httpclient.go:exhausted-N-retries wrapper, graphql.go's
// exhausted-N-retries wrapper) already prepend an explicit
// "exhausted N retries for URL: " prefix, so they continue to
// communicate the retry-loop semantics in their error string.
// Source-contract pin in transient_retry_test.go protects the
// %w-wrapping contract that callers depend on.
//
// Production diagnostic on 2026-05-13: 6 of 7 stuck repos had
// last_error starting with "graphql PR batch: graphql:
// exhausted 10 retries for https://api.github.com/graphql" and
// were looping indefinitely via v0.20.5's force_full_collect
// path. With the sentinel, ClassifyError returns ClassTransient
// and Fix C's subdivision actually fires — halving the batch
// until it's small enough to fit inside what GitHub will serve.
//
// Subsequent diagnostic on 2026-05-23: the v0.24.0 ecosyste.ms
// client wrapped 5xx into ErrTransient on a SINGLE call (no
// retry budget). With the pre-v0.25.0 sentinel text, every
// production log line read "transient (retries exhausted)" and
// operators reasonably concluded we were over-retrying — when
// in fact the ecosyste.ms client doesn't retry at all. The
// message change makes the log line accurate.
var ErrTransient = errors.New("transient")

// ErrPaginationLimitExceeded marks GitHub's hard cap on
// certain endpoints' result count (notably /releases). Past
// roughly 1000 results, GitHub returns HTTP 422 with body
// "Only the first 1000 are available." Pre-v0.20.19 the
// HTTPClient treated 422 as a fatal "unprocessable entity"
// error and killed the entire collection job. Production
// diagnostic on 2026-05-13: Azure/azure-sdk-for-java had
// last_error = "releases: unprocessable entity:
// .../releases?per_page=100&page=101" for this reason.
// Classified as ClassSkip — same semantic as 304/204:
// "no more data to fetch here." The paginator stops iterating
// cleanly without bubbling an error.
var ErrPaginationLimitExceeded = errors.New("pagination limit exceeded (GitHub serves at most 1000 results)")

// maxRedirectHops caps how many 301/302/307/308 follows a single Get call
// will perform before giving up. GitHub's best-practices guide says to
// always follow redirects; this cap protects against pathological chains
// (loops, rename-of-rename-of-rename) that would otherwise burn minutes
// per endpoint under the old "retry unexpected status" path.
const maxRedirectHops = 5

// AuthStyle controls how API tokens are sent in HTTP requests.
// GitHub and GitLab use different authentication header formats.
type AuthStyle int

const (
	// AuthGitHub sends "Authorization: token <key>" (GitHub PAT format).
	AuthGitHub AuthStyle = iota
	// AuthGitLab sends "PRIVATE-TOKEN: <key>" (GitLab PAT format).
	AuthGitLab
)

// HTTPClient wraps http.Client with rate-limiting, key rotation, retries, and
// pagination. Used by both GitHub and GitLab implementations.
type HTTPClient struct {
	inner     *http.Client
	keys      *KeyPool
	logger    *slog.Logger
	baseURL   string // e.g. "https://api.github.com" or "https://gitlab.com/api/v4"
	authStyle AuthStyle

	// etagCache stores ETags from previous responses, keyed by URL path.
	// When a cached ETag exists, Get sends If-None-Match, which saves API quota
	// when the data hasn't changed (GitHub returns 304 without counting against
	// the rate limit). The cache is bounded by typical usage patterns (one entry
	// per unique endpoint path hit during a collection cycle).
	etagMu    sync.RWMutex
	etagCache map[string]string
	// maxETagEntries bounds etagCache (v0.27.40, summary/18 Phase 3).
	// Keys include query strings, and incremental-collection URLs carry
	// per-cycle since= values and page numbers — mostly write-once,
	// never-hit-again entries. Unbounded, a weeks-long serve over 100K
	// repos accumulates millions of dead entries. When full, the cache
	// is RESET (map re-alloc): crude but O(1), and the cost of a lost
	// ETag is one non-304 response, not correctness.

	// onPermanentRedirect is invoked whenever Get observes a 301 or 308
	// response it's about to follow. The callback receives the from URL
	// (the one Get was trying to reach) and the to URL (the Location
	// header target, resolved to an absolute URL). Intended for the
	// scheduler to detect repo renames and update repos.repo_git.
	//
	// Not invoked for 302/307 — those are temporary and must not mutate
	// durable state. Guarded against nil at each call site.
	redirectMu          sync.RWMutex
	onPermanentRedirect func(from, to string)
}

// NewHTTPClient creates a platform-aware HTTP client with the given auth style.
// AuthGitHub sends "Authorization: token <key>"; AuthGitLab sends "PRIVATE-TOKEN: <key>".
// Uses a transport tuned for high-throughput API collection: keepalives enabled,
// generous idle connection pool, and HTTP/2 support (Go's default).
func NewHTTPClient(baseURL string, keys *KeyPool, logger *slog.Logger, authStyle AuthStyle) *HTTPClient {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20, // GitHub/GitLab APIs are few hosts with many requests
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
		// ResponseHeaderTimeout caps how long we wait for the server's
		// response headers after sending the request. A stalled
		// connection (firewall drop, server hang) would otherwise hold
		// a worker slot for the full whole-request Timeout. 15s is well
		// above GitHub's normal response-header latency (~200ms) but
		// below the whole-request budget so stalls fail fast.
		ResponseHeaderTimeout: 15 * time.Second,
	}
	return &HTTPClient{
		inner: &http.Client{
			// 60s whole-request timeout. Accommodates the ~1 MB responses
			// GraphQL queries return for batches of 25 parents with full
			// nested children, and leaves a 6× margin above observed p99
			// GraphQL response times (~10s) for firewall-induced jitter.
			// Previously 30s, which left only 3× margin.
			Timeout:   60 * time.Second,
			Transport: transport,
			// Our Get loop owns redirect handling explicitly so the logic is
			// in one place (hop cap, logging, Location-absent → ErrGone).
			// ErrUseLastResponse tells Go to return the 3xx to us without
			// attempting its own follow.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		keys:      keys,
		logger:    logger,
		baseURL:   strings.TrimSuffix(baseURL, "/"),
		authStyle: authStyle,
		etagCache: make(map[string]string),
	}
}

// Keys returns the underlying key pool, allowing callers to get keys for
// non-standard requests (e.g., GraphQL via POST).
func (c *HTTPClient) Keys() *KeyPool {
	return c.keys
}

// OnPermanentRedirect installs a callback that fires whenever Get observes
// a 301 or 308 response it's about to follow. The callback receives the
// from URL (the request that received the redirect) and the to URL
// (resolved absolute target). Use case: the scheduler installs a hook per
// job that updates repos.repo_git / repo_owner / repo_name when the
// redirect is on the repo root, so the DB stays in sync with GitHub's
// rename/transfer events.
//
// Only permanent redirects fire the hook. 302/307 are temporary — the
// repo's canonical URL hasn't changed, so mutating the DB would be wrong.
//
// Passing a nil hook clears any previously-installed callback.
// Safe to call at any time; internally synchronized.
func (c *HTTPClient) OnPermanentRedirect(hook func(from, to string)) {
	c.redirectMu.Lock()
	c.onPermanentRedirect = hook
	c.redirectMu.Unlock()
}

const maxRetries = 10

// maxPageReadRetries bounds per-page BODY-read retries in paginate
// (v0.27.37, Phase 1g) — same budget as Fix C's GraphQL read retries.
const maxPageReadRetries = 3

// maxETagEntries caps the per-client ETag cache (see the etagCache
// field comment). Sized generously above the fleet's ~100K-repo hot
// set of stable (query-less) endpoint paths.
const maxETagEntries = 500_000

// ctxKeyBypassETag is a context value key used by WithoutETag to
// suppress the ETag conditional layer on a single Get call. Distinct
// type per Go context-key convention so other packages can't collide
// on the key.
type ctxKeyBypassETag struct{}

// WithoutETag returns a derived context that suppresses both the
// If-None-Match send and the ETag cache write for any Get/GetJSON
// call made with it. Use this for endpoints where 304 responses
// would silently destroy data via snapshot-replace semantics — the
// v0.24.0 DistributionWorker is the canonical case: on 304 the
// scanner gets back empty data, MarkDistributionComplete rotates
// the prior rows to history and never reinserts them, so the
// repo's distribution evidence quietly disappears even though
// GitHub politely told us "you already have this."
//
// At 180-day distribution cadence the wasted GitHub API budget
// from disabling ETag is ~1.6% of the pool — well worth the
// silent-data-loss avoidance.
func WithoutETag(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyBypassETag{}, true)
}

func bypassETag(ctx context.Context) bool {
	v, _ := ctx.Value(ctxKeyBypassETag{}).(bool)
	return v
}

// forgetETag drops the cached ETag for a path. Load-bearing for the
// v0.27.37 page-read retry: Get caches the ETag at HEADER time, before
// the body is read, so after a mid-body failure the cache holds an
// ETag for content we never stored. Replaying it on the retry (or on
// the NEXT collection cycle) would yield 304 → paginate ends cleanly →
// silent truncation. Forgetting the entry makes the re-fetch
// unconditional and lets the retry's own response repopulate the cache.
func (c *HTTPClient) forgetETag(path string) {
	c.etagMu.Lock()
	delete(c.etagCache, path)
	c.etagMu.Unlock()
}

// Get performs a single authenticated GET request with retries and rate-limit handling.
func (c *HTTPClient) Get(ctx context.Context, path string) (*http.Response, error) {
	url := c.baseURL + path
	// Redirect hops consumed by this call. Counted separately from retry
	// attempts so a rename-then-rate-limited chain doesn't prematurely
	// exhaust the retry budget, and a loop doesn't run forever.
	redirectHops := 0
	skipETag := bypassETag(ctx)

	for attempt := range maxRetries {
		key, err := c.keys.GetKey(ctx)
		if err != nil {
			return nil, fmt.Errorf("getting API key: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		// Set platform-appropriate auth header.
		// GitHub: "Authorization: token <key>" (PATs and old OAuth tokens).
		// GitLab: "PRIVATE-TOKEN: <key>" (Personal Access Tokens).
		switch c.authStyle {
		case AuthGitLab:
			req.Header.Set("PRIVATE-TOKEN", key.Token)
		default: // AuthGitHub
			req.Header.Set("Authorization", "token "+key.Token)
		}
		req.Header.Set("Accept", "application/json")

		// Conditional request: send If-None-Match when we have a cached ETag.
		// GitHub does not count 304 responses against the rate limit.
		// Skipped when the caller used WithoutETag(ctx) — see v0.25.0
		// docstring on WithoutETag for the silent-data-loss rationale.
		if !skipETag {
			c.etagMu.RLock()
			if etag, ok := c.etagCache[path]; ok {
				req.Header.Set("If-None-Match", etag)
			}
			c.etagMu.RUnlock()
		}

		resp, err := c.inner.Do(req)
		if err != nil {
			// v0.27.28: a cancelled context is not a retryable failure —
			// bail BEFORE the "retrying" WARN. Pre-fix, every request
			// in flight at `aveloxis stop` logged a retry it would
			// never make (87 misleading WARNs in one minute of the
			// 2026-07-21 shutdown). Debug, not Warn: "we were told to
			// stop" is not an error.
			if ctx.Err() != nil {
				c.logger.Debug("HTTP request aborted by context cancellation", "url", url)
				return nil, ctx.Err()
			}
			c.logger.Warn("HTTP request failed, retrying",
				"url", url, "attempt", attempt+1, "error", err)
			// Context-aware sleep: a cancelled job wakes immediately
			// instead of sitting here for 20+s across the retry chain.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 2 * time.Second):
			}
			continue
		}

		c.keys.UpdateFromResponse(key, resp)

		// Log rate limit state on every response so operators can monitor usage.
		if remaining := resp.Header.Get("X-RateLimit-Remaining"); remaining != "" {
			resource := resp.Header.Get("X-RateLimit-Resource")
			if resource == "" {
				resource = "core"
			}
			limit := resp.Header.Get("X-RateLimit-Limit")
			reset := resp.Header.Get("X-RateLimit-Reset")
			c.logger.Debug("rate limit status",
				"resource", resource, "remaining", remaining,
				"limit", limit, "reset", reset)
		}

		// Cache ETag from successful responses for future conditional requests.
		// Skipped when the caller opted into WithoutETag — we don't want to
		// poison the cache for OTHER callers that might use the same path
		// without the bypass.
		if !skipETag {
			if etag := resp.Header.Get("ETag"); etag != "" && resp.StatusCode == http.StatusOK {
				c.etagMu.Lock()
				if len(c.etagCache) >= maxETagEntries {
					c.etagCache = make(map[string]string, 1024)
				}
				c.etagCache[path] = etag
				c.etagMu.Unlock()
			}
		}

		verdict, out, err := c.handleResponse(ctx, resp, &url, path, attempt, &redirectHops, key)
		if verdict == respDone {
			return out, err
		}
	}

	// v0.20.19 (Fix J): wrap with ErrTransient so
	// platform.ClassifyError returns ClassTransient instead of
	// ClassFatal. Callers can then make informed retry/skip
	// decisions (e.g. fetchPRBatchWithSubdivide subdivides
	// the batch on transient classifications).
	return nil, fmt.Errorf("exhausted %d retries for %s: %w", maxRetries, url, ErrTransient)
}

// respAction is handleResponse's verdict for one attempt.
type respAction int

const (
	// respDone: Get returns (resp, err) as-is.
	respDone respAction = iota
	// respRetry: continue the attempt loop (backoff sleeps, key
	// rotation, and redirect URL rewrites have already happened
	// inside handleResponse).
	respRetry
)

// handleResponse classifies one HTTP response and performs the arm's
// side effects (body drain/close, backoff sleeps, key bookkeeping,
// redirect URL rewrite via urlp, redirect-hop accounting via hopsp).
// Extracted verbatim from the former 351-line Get (v0.27.42,
// summary/18 Phase 4); behavior identical — the case arms below are
// the accumulated production knowledge of five versions of retry
// hardening and every line is load-bearing.
func (c *HTTPClient) handleResponse(ctx context.Context, resp *http.Response, urlp *string, path string, attempt int, hopsp *int, key *APIKey) (respAction, *http.Response, error) {
	url := *urlp
	_ = url
	switch {
	case resp.StatusCode == http.StatusOK:
		return respDone, resp, nil
	case resp.StatusCode == http.StatusNoContent:
		// 204: legitimate "empty result" response. GitHub returns
		// 204 for /contributors on empty or 5000+-contributor
		// repos. Pre-v0.20.6 this fell into the default retry arm
		// and burned the full 10-retry budget per call. Return
		// ErrNoContent so the pagination engine completes the
		// iteration with zero items.
		resp.Body.Close()
		return respDone, nil, ErrNoContent
	case resp.StatusCode == http.StatusNotModified:
		// 304: data hasn't changed since our last request.
		// This does NOT count against GitHub's rate limit.
		resp.Body.Close()
		return respDone, nil, ErrNotModified
	case resp.StatusCode == http.StatusNotFound:
		resp.Body.Close()
		return respDone, nil, fmt.Errorf("%w: %s", ErrNotFound, url)
	case resp.StatusCode == http.StatusGone:
		// 410 — the resource existed but was deliberately removed (e.g.,
		// a deleted GitHub issue). Never retryable; distinct from 404 so
		// callers can tell "never existed / can't see it" apart from
		// "existed and was deleted". isOptionalEndpointSkip treats
		// ErrGone like ErrNotFound so the containing job continues.
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		c.logger.Warn("resource is gone (410)",
			"url", url, "body_snippet", truncateBody(string(body), 200))
		return respDone, nil, fmt.Errorf("%w: %s", ErrGone, url)
	case resp.StatusCode == http.StatusMovedPermanently ||
		resp.StatusCode == http.StatusFound ||
		resp.StatusCode == http.StatusTemporaryRedirect ||
		resp.StatusCode == http.StatusPermanentRedirect:
		// 301/302/307/308 — follow the Location header. GitHub uses 301
		// for permanent repo rename/transfer (the prelim phase updates
		// repo_git separately via resolveRedirects); 302/307 for
		// temporary redirects; 308 is the strict permanent variant. In
		// all cases the contract is: re-issue the request against the
		// URL in the Location header.
		location := resp.Header.Get("Location")
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if location == "" {
			// GitHub returns 3xx with no Location when it cannot determine
			// the target (observed for individual issues that were moved
			// during a rename where the issue numbering doesn't line up).
			// The body often contains {"message":"Moved Permanently","url":""}.
			// Nothing useful to retry — surface as ErrGone so callers skip.
			c.logger.Warn("redirect with empty Location header — treating as gone",
				"url", url, "status", resp.StatusCode,
				"body_snippet", truncateBody(string(body), 200))
			return respDone, nil, fmt.Errorf("%w: %s (redirect with empty Location)", ErrGone, url)
		}
		if *hopsp >= maxRedirectHops {
			c.logger.Warn("redirect hop cap exceeded — treating as gone",
				"url", url, "status", resp.StatusCode,
				"location", location, "hops", *hopsp)
			return respDone, nil, fmt.Errorf("%w: %s (redirect loop or chain longer than %d)",
				ErrGone, url, maxRedirectHops)
		}
		*hopsp++
		// Resolve relative Location (most GitHub Location headers are
		// absolute, but RFC 7231 permits relative).
		newURL := location
		if !strings.HasPrefix(newURL, "http://") && !strings.HasPrefix(newURL, "https://") {
			newURL = c.baseURL + location
		}
		c.logger.Info("following redirect",
			"from", url, "to", newURL,
			"status", resp.StatusCode, "hop", *hopsp)

		// Notify the permanent-redirect hook on 301/308 only. 302/307
		// are temporary and must not mutate durable state.
		if resp.StatusCode == http.StatusMovedPermanently ||
			resp.StatusCode == http.StatusPermanentRedirect {
			c.redirectMu.RLock()
			hook := c.onPermanentRedirect
			c.redirectMu.RUnlock()
			if hook != nil {
				hook(url, newURL)
			}
		}

		url = newURL
		*urlp = url
		// Do not count this iteration against the retry budget — a
		// redirect is not a retry. Decrement attempt so the outer
		// `for attempt := range maxRetries` loop gives us a fresh slot.
		// (range-int loops don't let us modify the iterator; instead we
		// just `continue` and accept at most maxRetries hops total,
		// which is fine because maxRedirectHops=5 < maxRetries=10.)
		return respRetry, nil, nil
	case resp.StatusCode == http.StatusUnauthorized:
		// 401 = bad credentials — but GitHub's auth backend returns this
		// transiently for valid tokens during incidents, so a single 401
		// must NOT kill the key. RecordAuthFailure quarantines only after
		// several consecutive failures (any success resets the count), and
		// even then the key auto-recovers after a cooldown. Either way we
		// just rotate to the next key on the next loop iteration.
		resp.Body.Close()
		c.keys.RecordAuthFailure(key)
		return respRetry, nil, nil
	case resp.StatusCode == http.StatusBadRequest:
		// 400 = malformed request. GitHub returns HTML "Whoa there!" for
		// invalid queries (e.g., bad search syntax). Not retryable.
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		c.logger.Warn("bad request (not retrying)",
			"url", url, "status", 400, "body_snippet", truncateBody(string(body), 200))
		return respDone, nil, fmt.Errorf("bad request: %s", url)
	case resp.StatusCode == http.StatusUnprocessableEntity:
		// 422 = validation failed. Not retryable for the same
		// request shape. v0.20.19 (Fix K) carves out one
		// subtype: GitHub's hard pagination cap (~1000
		// results on /releases and similar) returns 422 with
		// a body containing "Only the first 1000 are
		// available" or "Only the first 1000 results are
		// available." That's end-of-data, not a fatal
		// validation problem — the paginator should stop
		// cleanly via the ClassSkip path.
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		bodyStr := string(body)
		if strings.Contains(bodyStr, "Only the first 1000") {
			c.logger.Info("pagination limit reached (GitHub serves at most 1000 results)",
				"url", url, "body_snippet", truncateBody(bodyStr, 200))
			return respDone, nil, fmt.Errorf("%w: %s", ErrPaginationLimitExceeded, url)
		}
		c.logger.Warn("unprocessable entity (not retrying)",
			"url", url, "status", 422, "body_snippet", truncateBody(bodyStr, 200))
		return respDone, nil, fmt.Errorf("unprocessable entity: %s", url)
	case resp.StatusCode == http.StatusForbidden:
		// 403 can mean rate limit, secondary rate limit, or resource not
		// accessible. Header signals are authoritative — they carry the
		// reset timing that retry-after plumbing relies on, so they are
		// always consulted first. Body inspection is the fallback net
		// for cases where a proxy strips the headers, GitHub's response
		// shape changes, or an unauthenticated request leaks through
		// (the "for <IP>" body shape).
		if resp.Header.Get("Retry-After") != "" {
			resp.Body.Close()
			wait := parseRetryAfter(resp)
			c.logger.Info("secondary rate limit", "url", url, "wait", wait)
			select {
			case <-ctx.Done():
				return respDone, nil, ctx.Err()
			case <-time.After(wait):
			}
			return respRetry, nil, nil
		}
		if resp.Header.Get("X-RateLimit-Remaining") == "0" {
			resp.Body.Close()
			resource := resp.Header.Get("X-RateLimit-Resource")
			if resource == "" {
				resource = "core"
			}
			resetStr := resp.Header.Get("X-RateLimit-Reset")
			c.logger.Info("rate limit exhausted",
				"url", url, "resource", resource, "reset", resetStr)
			return respRetry, nil, nil
		}
		// Headers said nothing definitive. Read the body and check whether
		// the message text reveals a rate limit anyway.
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if isAnonymousRateLimitBody(body) {
			// Unauthenticated request reached us. Every code path that
			// builds an HTTPClient call goes through GetKey() — getting
			// this body shape means a key was unset, the wrong client
			// was used, or a proxy stripped the Authorization header.
			// Log at ERROR so on-call sees the regression, then back off
			// like a regular rate limit so we don't hot-loop on the bug.
			c.logger.Error("403 with unauthenticated rate-limit body — possible key-leak or unauthenticated request bug",
				"url", url,
				"body_snippet", truncateBody(string(body), 240))
			wait := jitteredBackoff(attempt)
			select {
			case <-ctx.Done():
				return respDone, nil, ctx.Err()
			case <-time.After(wait):
			}
			return respRetry, nil, nil
		}
		if isRateLimitBody(body) {
			c.logger.Warn("403 with rate-limit body but no rate-limit headers — treating as throttled",
				"url", url,
				"body_snippet", truncateBody(string(body), 240))
			wait := jitteredBackoff(attempt)
			select {
			case <-ctx.Done():
				return respDone, nil, ctx.Err()
			case <-time.After(wait):
			}
			return respRetry, nil, nil
		}
		// 403 for other reasons (private repo, no permission) — not a key problem.
		return respDone, nil, fmt.Errorf("%w: %s (not a rate limit — may be a private repo or insufficient scope)", ErrForbidden, url)
	case resp.StatusCode == http.StatusTooManyRequests:
		resp.Body.Close()
		wait := parseRetryAfter(resp)
		c.logger.Info("rate limited", "url", url, "wait", wait)
		select {
		case <-ctx.Done():
			return respDone, nil, ctx.Err()
		case <-time.After(wait):
		}
		return respRetry, nil, nil
	case resp.StatusCode == http.StatusInternalServerError ||
		resp.StatusCode == http.StatusBadGateway ||
		resp.StatusCode == http.StatusServiceUnavailable ||
		resp.StatusCode == http.StatusGatewayTimeout:
		// 500/502/503/504 — server/gateway error. These are transient.
		// 500 was added to this branch in v0.22.12 after a 2026-05-18
		// production incident where GitHub returned 500 with empty
		// body (upstream proxy hiccup) on ~1,400 /users/<login>/events
		// requests. Previously 500 fell into the default arm with
		// linear backoff and the generic "unexpected status" log line,
		// making the incident look like an uncategorized error rather
		// than a transient 5xx.
		resp.Body.Close()
		// v0.27.34: feed the fleet-level API-outage breaker — a hard
		// outage (consecutive 5xx with no success anywhere) pauses
		// new collection claims scheduler-side.
		c.keys.NoteServerError()
		backoff := time.Duration(1<<min(attempt, 6)) * time.Second // 1s, 2s, 4s, 8s, 16s, 32s, 64s
		jitter := time.Duration(rand.IntN(int(backoff/2) + 1))
		wait := backoff + jitter
		c.logger.Warn("server error, retrying with backoff",
			"url", url, "status", resp.StatusCode, "wait", wait, "attempt", attempt+1)
		select {
		case <-ctx.Done():
			return respDone, nil, ctx.Err()
		case <-time.After(wait):
		}
		return respRetry, nil, nil
	default:
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		c.logger.Warn("unexpected status",
			"url", url, "status", resp.StatusCode, "body_snippet", truncateBody(string(body), 200), "attempt", attempt+1)
		select {
		case <-ctx.Done():
			return respDone, nil, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 2 * time.Second):
		}
		return respRetry, nil, nil
	}
}

// GetJSON performs a GET and decodes the response JSON into dest.
func (c *HTTPClient) GetJSON(ctx context.Context, path string, dest any) error {
	resp, err := c.Get(ctx, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(dest)
}

// nextPageFunc determines the next page path from an HTTP response.
// Returns "" when there are no more pages.
type nextPageFunc func(resp *http.Response, basePath string) string

// nextPageGitHub extracts the next page URL from GitHub's Link header.
func nextPageGitHub(resp *http.Response, _ string) string {
	return extractNextLink(resp)
}

// nextPageGitLab checks X-Next-Page first, then falls back to Link header.
func nextPageGitLab(resp *http.Response, basePath string) string {
	if nextPage := resp.Header.Get("X-Next-Page"); nextPage != "" {
		pageNum, err := strconv.Atoi(nextPage)
		if err != nil || pageNum == 0 {
			return ""
		}
		p := setQueryParam(basePath, "page", nextPage)
		if !strings.Contains(p, "per_page=") {
			p += "&per_page=100"
		}
		return p
	}
	return extractNextLink(resp)
}

// paginate is the shared pagination engine used by both PaginateGitHub and
// PaginateGitLab. The only behavioral difference is how the next page is
// determined, which is injected via the nextPage function.
func paginate[T any](ctx context.Context, c *HTTPClient, path string, nextPage nextPageFunc) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		currentPath := ensurePerPage(path)
		basePath := currentPath
		pageReadRetries := 0

		for currentPath != "" {
			resp, err := c.Get(ctx, currentPath)
			if err != nil {
				// 304 Not Modified means the data hasn't changed since our last
				// request (ETag match). This is not an error — just means zero new items.
				if errors.Is(err, ErrNotModified) {
					if currentPath != basePath {
						// A 304 MID-pagination is unusual (per-page ETag matched
						// on page N≥2 while earlier pages changed) and ends the
						// iteration with pages 1..N-1 only — leave a trace so a
						// truncation here is diagnosable (summary/18 audit).
						c.logger.Debug("304 on a non-first page ended pagination early",
							"path", currentPath)
					}
					return // no new data, stop pagination
				}
				// 204 No Content (v0.20.6) is the legitimate empty-result
				// response GitHub returns for /contributors on empty or
				// 5,000+-contributor repos. Treat it like 304: end
				// iteration cleanly with zero items, no error surfaced
				// to the caller. Without this branch, every contributor
				// fetch on an empty repo would yield (zero, ErrNoContent)
				// to the caller, who would treat the error as a fatal
				// "exhausted retries" equivalent.
				if errors.Is(err, ErrNoContent) {
					return
				}
				// v0.20.19 (Fix K): GitHub's hard 1000-result
				// pagination cap on /releases and similar
				// endpoints returns 422 with body "Only the
				// first 1000 are available." End the iteration
				// cleanly with whatever pages we've already
				// yielded — no error surfaced to the caller.
				if errors.Is(err, ErrPaginationLimitExceeded) {
					// v0.27.39: the result set is TRUNCATED at the
					// platform's hard cap. Callers that treat the
					// listing as a complete universe (gap fill's
					// expected-numbers enumeration) would otherwise
					// silently under-detect — leave a loud trace.
					c.logger.Warn("pagination ended at the platform's hard result cap — result set is TRUNCATED",
						"path", currentPath)
					return
				}
				var zero T
				yield(zero, err)
				return
			}

			// v0.27.37 (summary/18 Phase 1g): the body decode runs
			// OUTSIDE Get's retry loop, so a mid-body RST_STREAM/
			// CANCEL from GitHub's edge used to surface here as a
			// terminal "decoding page" error and kill the whole job.
			// On force-full walks of the fleet's largest repos
			// (~10-15K sequential pages) that compounded to
			// near-certain failure per attempt — pytorch-class repos
			// could never complete. Retryable read failures now
			// re-fetch the SAME page on a fresh stream, after
			// forgetting the header-time-cached ETag (see forgetETag —
			// without that the retry gets 304 and truncates silently).
			var page []T
			decodeErr := json.NewDecoder(resp.Body).Decode(&page)
			resp.Body.Close()
			if decodeErr != nil {
				if isRetryableReadError(decodeErr) && pageReadRetries < maxPageReadRetries {
					pageReadRetries++
					c.forgetETag(currentPath)
					c.logger.Warn("page body read failed, retrying on a fresh stream",
						"path", currentPath, "attempt", pageReadRetries, "error", decodeErr)
					continue
				}
				var zero T
				if isRetryableReadError(decodeErr) {
					yield(zero, fmt.Errorf("decoding page after %d read retries: %w: %w", maxPageReadRetries, decodeErr, ErrTransient))
				} else {
					yield(zero, fmt.Errorf("decoding page: %w", decodeErr))
				}
				return
			}
			pageReadRetries = 0

			for _, item := range page {
				if !yield(item, nil) {
					return
				}
			}

			currentPath = nextPage(resp, basePath)
		}
	}
}

// ensurePerPage adds per_page=100 if not already present.
func ensurePerPage(path string) string {
	if strings.Contains(path, "per_page=") {
		return path
	}
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + "per_page=100"
}

// PaginateGitHub yields items from a paginated GitHub API endpoint.
// GitHub uses Link headers for pagination.
func PaginateGitHub[T any](ctx context.Context, c *HTTPClient, path string) iter.Seq2[T, error] {
	return paginate[T](ctx, c, path, nextPageGitHub)
}

// PaginateGitLab yields items from a paginated GitLab API endpoint.
// GitLab uses X-Next-Page or Link headers.
func PaginateGitLab[T any](ctx context.Context, c *HTTPClient, path string) iter.Seq2[T, error] {
	return paginate[T](ctx, c, path, nextPageGitLab)
}

// linkNextRE matches the "next" relation in a Link header.
var linkNextRE = regexp.MustCompile(`<([^>]+)>;\s*rel="next"`)

// extractNextLink parses the Link header for the "next" page URL.
// Returns the path portion only (strips the host to keep requests going through our client).
func extractNextLink(resp *http.Response) string {
	link := resp.Header.Get("Link")
	if link == "" {
		return ""
	}
	matches := linkNextRE.FindStringSubmatch(link)
	if len(matches) < 2 {
		return ""
	}
	nextURL := matches[1]
	// Extract just the path+query from the full URL.
	if u, err := http.NewRequest("GET", nextURL, nil); err == nil {
		return u.URL.RequestURI()
	}
	return nextURL
}

func setQueryParam(path, key, value string) string {
	base := path
	query := ""
	if idx := strings.IndexByte(path, '?'); idx >= 0 {
		base = path[:idx]
		query = path[idx+1:]
	}

	// Remove existing key= param.
	parts := strings.Split(query, "&")
	var filtered []string
	for _, p := range parts {
		if p != "" && !strings.HasPrefix(p, key+"=") {
			filtered = append(filtered, p)
		}
	}
	filtered = append(filtered, key+"="+value)
	return base + "?" + strings.Join(filtered, "&")
}

// jitteredBackoff returns a capped exponential backoff with random jitter,
// used by 403-with-rate-limit-body fallback paths that lack an authoritative
// Retry-After or X-RateLimit-Reset to honor. Caps at 64s + jitter so a
// pathological loop on a permanent 403 doesn't burn a worker for hours.
func jitteredBackoff(attempt int) time.Duration {
	base := time.Duration(1<<min(attempt, 6)) * time.Second // 1s..64s
	jitter := time.Duration(rand.IntN(int(base/2) + 1))
	return base + jitter
}

// truncateBody returns the first n bytes of a response body for logging,
// stripping HTML tags and collapsing whitespace for readability.
func truncateBody(body string, n int) string {
	// Strip HTML tags for cleaner log output.
	var clean strings.Builder
	inTag := false
	for _, r := range body {
		switch {
		case r == '<':
			inTag = true
		case r == '>':
			inTag = false
		case !inTag && r != '\r':
			if r == '\n' || r == '\t' {
				r = ' '
			}
			clean.WriteRune(r)
		}
	}
	s := strings.Join(strings.Fields(clean.String()), " ")
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func parseRetryAfter(resp *http.Response) time.Duration {
	ra := resp.Header.Get("Retry-After")
	if ra == "" {
		return 60 * time.Second
	}
	if secs, err := strconv.Atoi(ra); err == nil {
		return time.Duration(secs) * time.Second
	}
	return 60 * time.Second
}
