// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// libyear_registry_client.go — v0.29.56. The shared request path, pacing,
// answer cache and keyed GitHub lookups behind the libyear resolvers.
//
// Measured in two hours of the 2026-09-17 chaoss.tv log and in production
// the same day:
//   - crates.io answered 429 on 40 lines (3,153 deps). Its data-access
//     policy allows at most 1 request per second, and 70 workers resolved
//     with no shared pacing; a 429 failed the dependency at once.
//   - Go module licenses and SwiftPM releases were fetched from
//     api.github.com anonymously (60 requests an hour per IP): 99.5% of
//     821,230 Go libyear rows written in 7 days had no license, and the
//     license lookup failed without a log line.
//   - 168,510 libyear rows in 24 hours came from 34,434 distinct
//     (name, version) pairs, so about 80% of registry requests were
//     repeats of an answer already fetched that day.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// registryRetryBudget bounds the total time one registry request may
// spend waiting out 429/503 answers. It is the per-request timeout
// (registryHTTPClient.Timeout): a lookup may wait out a throttled
// registry about as long as it may wait on one slow response. A
// Retry-After past what is left of the budget fails the lookup (a failure
// without an answer, so it is never cached and the dependency is retried
// on the repo's next analysis).
//
// It does NOT bound the per-host PACING wait, which is separate and comes
// first: see paceRegistryHost.
var registryRetryBudget = registryHTTPClient.Timeout

// registryMinRetryStep is the smallest wait a retry may be charged, which
// bounds the number of retries at registryRetryBudget / this. One second:
// the pace the strictest registry policy we honour asks for (crates.io's
// one request per second), so a retry never re-requests faster than the
// most restrictive registry allows.
const registryMinRetryStep = time.Second

// registrySleep is the ctx-aware wait used for pacing and Retry-After.
// Package-level seam so tests assert the waits without spending them.
var registrySleep = func(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// registryHostIntervals holds the minimum spacing between requests to a
// registry host that publishes a request-rate limit. Shared by every
// worker in the process.
//   - crates.io: "A maximum of 1 request per second" for its API
//     (crates.io data-access policy, svelte/src/routes/data-access in
//     rust-lang/crates.io, read 2026-09-17).
var (
	registryPaceMu        sync.Mutex
	registryHostIntervals = map[string]time.Duration{
		"crates.io": time.Second,
	}
	registryNextSlot    = map[string]time.Time{}
	registryPaceLastLog = map[string]time.Time{}
)

// registryHostInterval returns the pacing interval for host (0 = unpaced).
func registryHostInterval(host string) time.Duration {
	registryPaceMu.Lock()
	defer registryPaceMu.Unlock()
	return registryHostIntervals[host]
}

// setRegistryHostIntervalForTest paces an httptest host; TEST-ONLY.
func setRegistryHostIntervalForTest(host string, d time.Duration) (restore func()) {
	registryPaceMu.Lock()
	old, had := registryHostIntervals[host]
	registryHostIntervals[host] = d
	registryPaceMu.Unlock()
	return func() {
		registryPaceMu.Lock()
		defer registryPaceMu.Unlock()
		if had {
			registryHostIntervals[host] = old
		} else {
			delete(registryHostIntervals, host)
		}
		delete(registryNextSlot, host)
		// The deep-queue report is throttled per host for a minute; a later
		// test reusing this httptest port would otherwise see no line.
		delete(registryPaceLastLog, host)
	}
}

// paceRegistryHost reserves the host's next request slot and waits for
// it. Reservation under the lock makes the spacing hold across workers.
//
// The wait is OUTSIDE registryRetryBudget and is not capped: honouring a
// registry's published rate is the point, and failing instead would just
// discard the dependency. It is bounded in practice by how many callers
// are queued — each holds at most one reservation, so the worst wait is
// (concurrent callers × interval), e.g. 70 workers × 1 s for crates.io.
// A wait longer than one request's whole timeout is reported (throttled
// per host) so a queue this deep is visible rather than looking like a
// hang.
func paceRegistryHost(ctx context.Context, host string, logger *slog.Logger) error {
	registryPaceMu.Lock()
	interval := registryHostIntervals[host]
	if interval <= 0 {
		registryPaceMu.Unlock()
		return nil
	}
	now := time.Now()
	slot := registryNextSlot[host]
	if slot.Before(now) {
		slot = now
	}
	registryNextSlot[host] = slot.Add(interval)
	wait := slot.Sub(now)
	report := wait > registryHTTPClient.Timeout && now.Sub(registryPaceLastLog[host]) >= registryPaceLogEvery
	if report {
		registryPaceLastLog[host] = now
	}
	registryPaceMu.Unlock()
	if report && logger != nil {
		logger.Info("registry pacing queue is deep — lookups are waiting for their turn",
			"host", host, "wait", wait.Round(time.Second).String(), "interval", interval.String())
	}
	return registrySleep(ctx, wait)
}

// registryPaceLogEvery throttles the deep-queue report to once per host
// per minute: it describes a condition that lasts as long as the queue,
// not an event.
const registryPaceLogEvery = time.Minute

// ctxKeyRegistryLogger carries the collection's logger to the registry
// layer. The resolvers are package functions with a fixed signature
// (shared with the behavioral-test table), so there is no receiver to hang
// a logger on, and slog.Default() is NOT the process logger here: nothing
// calls slog.SetDefault, so it would write unformatted lines that ignore
// `log_level`. scanLibyear seeds it; anything else falls back to the
// default logger rather than dropping the line.
type ctxKeyRegistryLogger struct{}

// withRegistryLogger returns a context carrying the logger the registry
// layer reports through.
func withRegistryLogger(ctx context.Context, logger *slog.Logger) context.Context {
	if logger == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyRegistryLogger{}, logger)
}

// registryLoggerFrom reads that logger, falling back to the default.
func registryLoggerFrom(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKeyRegistryLogger{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}

// errRegistryNotFound is what a *registryStatusError for 404/410 unwraps
// to: the registry answered that the package or version does not exist.
var errRegistryNotFound = errors.New("registry: not found")

// registryStatusError is a non-2xx registry answer. Its text keeps the
// `registry URL: HTTP n` form the libyear WARN has always carried.
type registryStatusError struct {
	url    string
	status int
}

func (e *registryStatusError) Error() string {
	return fmt.Sprintf("registry %s: HTTP %d", e.url, e.status)
}

func (e *registryStatusError) Unwrap() error {
	if e.status == http.StatusNotFound || e.status == http.StatusGone {
		return errRegistryNotFound
	}
	return nil
}

// isDefinitiveRegistryMiss reports whether err is the registry's answer
// that the thing does not exist (safe to cache), as opposed to a failure
// that says nothing (rate limit, 5xx, timeout, decode error — never
// cached, SR-5). GitHub-sourced lookups use the platform's definitive
// answers.
func isDefinitiveRegistryMiss(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, errRegistryNotFound) || platform.IsDefinitiveAnswer(err)
}

// doRegistryRequest sends one registry request through the shared path:
// identifying User-Agent, host pacing, and 429/503 retries within
// registryRetryBudget. It returns the response only for a 2xx status; the
// caller closes the body.
func doRegistryRequest(ctx context.Context, method, rawURL string, headers ...string) (*http.Response, error) {
	logger := registryLoggerFrom(ctx)
	host := ""
	if u, err := url.Parse(rawURL); err == nil {
		host = u.Host
	}
	var waited time.Duration
	for attempt := 0; ; attempt++ {
		if err := paceRegistryHost(ctx, host, logger); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, method, rawURL, nil)
		if err != nil {
			return nil, err
		}
		// crates.io REJECTS anonymous default User-Agents with HTTP 403
		// (its policy wants an identifying UA with a contact URL), which
		// silently zeroed every cargo libyear row until v0.27.19.
		req.Header.Set("User-Agent", "aveloxis/"+db.ToolVersion+" (+https://github.com/aveloxis/aveloxis)")
		for _, h := range headers {
			if k, v, ok := strings.Cut(h, ": "); ok {
				req.Header.Set(k, v)
			}
		}
		resp, err := registryHTTPClient.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, nil
		}
		_ = resp.Body.Close()
		statusErr := &registryStatusError{url: rawURL, status: resp.StatusCode}
		if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusServiceUnavailable {
			return nil, statusErr
		}
		wait, ok := registryRetryAfter(resp.Header.Get("Retry-After"), attempt)
		if !ok {
			return nil, statusErr
		}
		// Floor the wait at one step. `Retry-After: 0` is legal, and an
		// HTTP-date header can already be in the past by the time it is
		// parsed (clock skew, round trip) — both ask for a zero wait, which
		// would neither pace the retry nor advance the budget: the loop
		// re-requested at full speed forever (300,376 requests in 10 seconds
		// in the test that found this), holding a collection worker until
		// `stop serve`.
		wait = max(wait, registryMinRetryStep)
		// Compare against what is LEFT, never `waited + wait`: a Retry-After
		// date centuries out saturates time.Until at the maximum Duration,
		// and the sum overflows int64 and goes negative — it passed a
		// "> budget" check and the loop slept ~292 years, holding the worker
		// until `stop serve`. Here `registryRetryBudget` and `waited` are
		// both non-negative with waited ≤ budget (this check is what keeps
		// it so), and neither is a value the registry chose, so the
		// subtraction cannot overflow however large `wait` is.
		if wait > registryRetryBudget-waited {
			return nil, statusErr
		}
		if err := registrySleep(ctx, wait); err != nil {
			return nil, err
		}
		waited += wait
	}
}

// registryRetryAfter reads a Retry-After header (delta-seconds or an HTTP
// date). Without one it doubles from one second per attempt, which the
// budget cuts off.
func registryRetryAfter(v string, attempt int) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Second << attempt, true
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		// Refuse before multiplying: `time.Duration(secs) * time.Second`
		// wraps int64 past ~9.2e9 seconds, and a wrapped value would be
		// floored to one second — silently ignoring a registry that asked
		// for an enormous back-off, while one second smaller correctly
		// gives up. Anything past the budget fails the lookup anyway.
		if secs > int(registryRetryBudget/time.Second) {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(time.Until(t), 0), true
	}
	return 0, false
}

// fetchRegistryLastModified sends a HEAD request and returns the
// response's Last-Modified as RFC 3339 ("" when the header is absent).
// Maven Central carries a file's publication time only in this header.
func fetchRegistryLastModified(ctx context.Context, rawURL string) (string, error) {
	resp, err := doRegistryRequest(ctx, http.MethodHead, rawURL)
	if err != nil {
		return "", err
	}
	_ = resp.Body.Close()
	lm := resp.Header.Get("Last-Modified")
	if lm == "" {
		return "", nil
	}
	t, err := http.ParseTime(lm)
	if err != nil {
		return "", fmt.Errorf("registry %s: unparseable Last-Modified %q: %w", rawURL, lm, err)
	}
	return t.UTC().Format(time.RFC3339), nil
}

// registryCacheTTL is how long a registry answer is reused. One day: the
// 80% repeat rate above was measured over a 24-hour window, and a package
// released today reaches libyear by the next day's scans, well inside
// the fleet's recollection interval (days_until_recollect, 12 days on
// chaoss.tv). Memory is bounded by the TTL: entries ≈ distinct lookups
// per day (34,434 successful pairs measured), each a few hundred bytes.
const registryCacheTTL = 24 * time.Hour

// ttlCache is a process-wide map of answers with an expiry. It stores a
// value or a definitive error; callers decide what is safe to cache.
type ttlCache[V any] struct {
	mu        sync.Mutex
	ttl       time.Duration
	now       func() time.Time
	entries   map[string]ttlEntry[V]
	lastSweep time.Time
}

type ttlEntry[V any] struct {
	val     V
	err     error
	expires time.Time
}

func newTTLCache[V any](ttl time.Duration) *ttlCache[V] {
	return &ttlCache[V]{ttl: ttl, now: time.Now, entries: map[string]ttlEntry[V]{}}
}

func (c *ttlCache[V]) get(key string) (V, error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || !c.now().Before(e.expires) {
		var zero V
		return zero, nil, false
	}
	return e.val, e.err, true
}

// put stores an answer and sweeps expired entries at most once per TTL,
// so an entry lives at most two TTLs.
func (c *ttlCache[V]) put(key string, val V, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if now.Sub(c.lastSweep) >= c.ttl {
		for k, e := range c.entries {
			if !now.Before(e.expires) {
				delete(c.entries, k)
			}
		}
		c.lastSweep = now
	}
	c.entries[key] = ttlEntry[V]{val: val, err: err, expires: now.Add(c.ttl)}
}

func (c *ttlCache[V]) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// libyearRowCache and githubLicenseCache are shared by every analysis in
// the process (a collector is built per repo).
var (
	libyearRowCache    = newTTLCache[*db.LibyearRow](registryCacheTTL)
	githubLicenseCache = newTTLCache[string](registryCacheTTL)
)

// libyearCacheKey identifies one registry answer. SwiftPM resolves from
// the requirement's repository URL, not the package label, so its URL is
// part of the key.
func libyearCacheKey(dep libyearDep) string {
	key := dep.Manager + "\x00" + dep.Name + "\x00" + dep.Version
	if dep.Manager == "swiftpm" {
		key += "\x00" + dep.Requirement
	}
	return key
}

// resolveLibyearCached answers dep from the cache or the resolver. A hit
// is a copy carrying this dependency's own requirement text and scope.
// Only answers are cached: a resolved row, or a definitive miss.
func resolveLibyearCached(ctx context.Context, cache *ttlCache[*db.LibyearRow], dep libyearDep,
	resolve func(context.Context, libyearDep) (*db.LibyearRow, error)) (*db.LibyearRow, error) {
	key := libyearCacheKey(dep)
	if cached, err, ok := cache.get(key); ok {
		if err != nil {
			return nil, err
		}
		row := *cached
		row.Requirement = dep.Requirement
		row.Type = dep.Type
		return &row, nil
	}
	row, err := resolve(ctx, dep)
	switch {
	case err == nil && row != nil:
		stored := *row
		cache.put(key, &stored, nil)
	case err != nil && isDefinitiveRegistryMiss(err):
		cache.put(key, nil, err)
	}
	return row, err
}

// githubAPIGetter is the key-pooled GitHub REST client analysis uses
// (*platform.HTTPClient on https://api.github.com). Every request leases
// a key from the shared pool (SR-20); nothing reaches api.github.com
// anonymously.
type githubAPIGetter interface {
	GetJSON(ctx context.Context, path string, dest any) error
}

// errNoGitHubClient: analysis has no keyed GitHub client (no GitHub keys),
// so a GitHub-hosted lookup cannot run. Reported, never read as "none".
var errNoGitHubClient = errors.New("no GitHub API client (no GitHub keys loaded)")

// githubModuleLicense returns the SPDX license of the GitHub repository
// hosting a Go module. Non-GitHub modules have no lookup (""). A 404 or
// another definitive answer means the repository has no detectable
// license (""), cached per owner/repo. A failure without an answer is
// returned as an error for the caller to count and log.
func githubModuleLicense(ctx context.Context, gh githubAPIGetter, cache *ttlCache[string], modulePath string) (string, error) {
	if !strings.HasPrefix(modulePath, "github.com/") {
		return "", nil
	}
	parts := strings.SplitN(strings.TrimPrefix(modulePath, "github.com/"), "/", 3)
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", nil
	}
	owner, repo := parts[0], parts[1]
	key := strings.ToLower(owner + "/" + repo)
	if lic, _, ok := cache.get(key); ok {
		return lic, nil
	}
	if gh == nil {
		return "", errNoGitHubClient
	}
	var info struct {
		License struct {
			SpdxID string `json:"spdx_id"`
		} `json:"license"`
	}
	path := "/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(repo) + "/license"
	if err := gh.GetJSON(ctx, path, &info); err != nil {
		if platform.IsDefinitiveAnswer(err) {
			cache.put(key, "", nil)
			return "", nil
		}
		return "", fmt.Errorf("license for %s/%s: %w", owner, repo, err)
	}
	lic := info.License.SpdxID
	if lic == "NOASSERTION" {
		lic = ""
	}
	cache.put(key, lic, nil)
	return lic, nil
}
