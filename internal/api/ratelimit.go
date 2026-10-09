// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.0 — per-IP rate limiting + CORS for the public analytics API
// (plan: summary/api-analytics-plan-2026-07-10.md §3).
//
// Operator contract: limits apply ONLY to clients arriving from
// outside the LAN. Requests whose RESOLVED client IP falls inside an
// exempt CIDR (default: loopback + RFC1918) bypass the limiter
// entirely. When nginx fronts the API on the same box every request
// arrives from 127.0.0.1, so the client IP is resolved from
// X-Forwarded-For — but ONLY when the direct peer is the configured
// trusted proxy; otherwise XFF is attacker-controlled and ignored.
//
// Two layers per non-exempt IP:
//   - token bucket (default 1 rps, burst 10) — smooths bursts;
//   - daily quota (default 1,000/day) — the actual anti-bulk-crawl
//     control: a bucket alone only slows a patient crawler.
//
// Hand-rolled bucket (no new dependency); state is in-memory with a
// bounded visitor map. The Authorization middleware planned for the
// super-token tiers (§2 of the plan) will layer on top of this.

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/mailer"
)

// Options configures the public-API middleware chain.
type Options struct {
	RateLimitRPS   float64  // sustained requests/second per IP (default 1)
	RateLimitBurst int      // bucket capacity (default 10)
	RateLimitDaily int      // requests/day per IP (default 1000)
	ExemptCIDRs    []string // client networks that bypass limiting entirely
	CORSOrigins    []string // origins allowed to call the API from a browser
	TrustedProxy   string   // peer IP whose X-Forwarded-For is believed
	RequireAuth    bool     // v0.27.1: gate all data endpoints behind Bearer sessions

	// ResponseCacheMaxAge is the longest a per-repository answer is reused
	// within one collection generation (collection_cache.go): the
	// contributor enrichment interval, collection.enrich_interval_minutes
	// (v0.29.71, O11 option 4). Zero keeps the 60 s TTL.
	ResponseCacheMaxAge time.Duration

	// ResponseCacheBytes bounds the repository-page response cache
	// (repo_page_cache.go, v0.29.73): api.response_cache_mb in bytes. Zero
	// keeps only /timeseries and /contributors/top answers, bounded by count
	// as main kept them (0.29.78); ETags and 304s work either way.
	ResponseCacheBytes int64

	// RewarmInterval is how often the API looks for repositories whose
	// cached answers a finished collection made outdated, and recomputes
	// them (api.cache_rewarm_seconds). Zero turns the re-warm off.
	RewarmInterval time.Duration

	// RequestTimeout is http_timeout_seconds: the bound every request runs
	// under, applied to each re-warm request too (they run inside the
	// process, past the HTTP server's bound).
	RequestTimeout time.Duration

	// FrontEndSecret is api.front_end_secret: the value a front end sends in
	// X-Aveloxis-Authorized on a request it forwards after the
	// authorization route admitted the visitor, so the limiter does not
	// count that request twice. Empty: every request is counted.
	FrontEndSecret string

	// SPAURL is web.spa_url: the separate-repo front end's origin. The
	// account-email confirmation link lands on its profile page when set
	// (web.ConfirmationPolicy); empty keeps the web process's own page.
	SPAURL string

	// GitHubAPIBase is github.base_url — the host an org registered through
	// the portal must be on (db.ErrOrgOffGitHubHost); empty means public
	// GitHub (v0.29.57 round 2).
	GitHubAPIBase string

	// Mailer carries the transactional mailer for the v0.27.20
	// add-request notifications (submission → operator, decision →
	// requester). nil = notifications silently skipped, matching the
	// web process's optional-mailer semantics.
	Mailer *mailer.Mailer

	// AutoApproveAddLimit is web.auto_approve_add_limit threaded into
	// the portal's repo-add endpoint (0 = every non-admin new-repo add
	// pends for approval).
	AutoApproveAddLimit int
}

// DefaultExemptCIDRs is the "same box / same LAN" set.
var DefaultExemptCIDRs = []string{
	"127.0.0.0/8", "::1/128",
	"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
}

type bucket struct {
	tokens float64
	last   time.Time

	day      string // YYYY-MM-DD of the quota window
	dayCount int

	lastSeen time.Time

	// badTokenAt is when this address last presented a token the store
	// called invalid (v0.29.82; see deferLookup).
	badTokenAt time.Time
}

// badTokenMemory is how long an address that presented an invalid token
// has its further uncached tokens deferred while its bucket is empty: the
// token cache's own horizon (authCacheTTL).
const badTokenMemory = authCacheTTL

// deferLookup reports whether identify should skip the store lookup for an
// uncached token from r's address (L10 round 1 on 0.29.82): the address is
// over its limit (no whole token in its bucket, or its daily quota spent)
// AND it presented an invalid token within badTokenMemory. Without it, an
// address over its limit could make every request cost a lookup by sending
// made-up tokens; with the second condition, a signed-in caller behind a
// busy shared address that has sent no bad token is still resolved. A
// deferred request the limiter admits anyway is looked up by the auth layer.
func (rl *rateLimiter) deferLookup(r *http.Request) bool {
	ip := rl.clientIP(r)
	if rl.isExempt(ip) || ip == nil {
		return false
	}
	now := rl.clock()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	b, ok := rl.visitors[ip.String()]
	if !ok || b.badTokenAt.IsZero() || now.Sub(b.badTokenAt) >= badTokenMemory {
		return false
	}
	tokens := b.tokens + now.Sub(b.last).Seconds()*rl.opts.RateLimitRPS
	overQuota := b.day == now.UTC().Format("2006-01-02") && b.dayCount >= rl.opts.RateLimitDaily
	return tokens < 1 || overQuota
}

// noteBadToken records that r's address presented an invalid token, and
// logs it once per address per badTokenMemory (never the token; kind is
// "api" or "session").
func (rl *rateLimiter) noteBadToken(r *http.Request, kind string) {
	ip := rl.clientIP(r)
	if rl.isExempt(ip) || ip == nil {
		return
	}
	now := rl.clock()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	b, ok := rl.visitors[ip.String()]
	if !ok {
		if len(rl.visitors) >= maxTrackedIPs {
			rl.evictOldestLocked()
		}
		b = &bucket{tokens: float64(rl.opts.RateLimitBurst), last: now, lastSeen: now}
		rl.visitors[ip.String()] = b
	}
	if rl.logger != nil && (b.badTokenAt.IsZero() || now.Sub(b.badTokenAt) >= badTokenMemory) {
		rl.logger.Info("invalid token presented — unknown, revoked or expired; the address is counted per IP",
			"address", ip.String(), "kind", kind)
	}
	b.badTokenAt = now
}

// allow refills by elapsed×rps (capped at burst) and consumes one
// token when available.
func (b *bucket) allow(now time.Time, rps float64, burst int) bool {
	elapsed := now.Sub(b.last).Seconds()
	b.last = now
	b.tokens += elapsed * rps
	if b.tokens > float64(burst) {
		b.tokens = float64(burst)
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

const maxTrackedIPs = 10000

type rateLimiter struct {
	opts    Options
	exempt  []*net.IPNet
	origins map[string]bool

	mu       sync.Mutex
	visitors map[string]*bucket

	// uncounted reports a request the visitor already paid for
	// (Server.frontEndAuthorized, v0.29.73); nil counts everything.
	uncounted func(*http.Request) bool

	// v0.29.82: each operator-issued API token's hourly window (keyed by
	// token id), and the clock they read (a test seam; time.Now otherwise).
	tokenWindows map[int64]*tokenWindow
	now          func() time.Time

	// logger records refused tokens and exhausted allowances (ASVS V16.3,
	// the 0.29.82 review A5); nil is silent.
	logger *slog.Logger
}

// tokenWindow is one API token's current hour: its start and the calls
// counted in it.
type tokenWindow struct {
	start  time.Time
	count  int
	warned bool // the over-allowance line was logged for this window
}

// apiTokenWindow is how long an API token's allowance lasts before it
// starts over (the allowance is per hour: operator decision 2026-10-08).
const apiTokenWindow = time.Hour

// allowToken counts one call of an API token against its hourly allowance.
// It returns whether the call is allowed, the calls left in the window and
// when the window ends.
func (rl *rateLimiter) allowToken(tokenID int64, userID, limit int) (bool, int, time.Time) {
	now := rl.clock()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if rl.tokenWindows == nil {
		rl.tokenWindows = map[int64]*tokenWindow{}
	}
	w, ok := rl.tokenWindows[tokenID]
	if !ok || !now.Before(w.start.Add(apiTokenWindow)) {
		if len(rl.tokenWindows) >= maxTrackedIPs {
			// Bounded like the per-IP map: drop windows that have ended.
			for id, old := range rl.tokenWindows {
				if !now.Before(old.start.Add(apiTokenWindow)) {
					delete(rl.tokenWindows, id)
				}
			}
		}
		w = &tokenWindow{start: now}
		rl.tokenWindows[tokenID] = w
	}
	reset := w.start.Add(apiTokenWindow)
	if w.count >= limit {
		if !w.warned && rl.logger != nil {
			w.warned = true
			rl.logger.Warn("API token over its hourly allowance — refused until its window ends",
				"token_id", tokenID, "user_id", userID, "rate_limit_per_hour", limit, "window_ends", reset)
		}
		return false, 0, reset
	}
	w.count++
	return true, limit - w.count, reset
}

func (rl *rateLimiter) clock() time.Time {
	if rl.now != nil {
		return rl.now()
	}
	return time.Now()
}

func newRateLimiter(opts Options) (*rateLimiter, error) {
	if opts.RateLimitRPS <= 0 {
		opts.RateLimitRPS = 1
	}
	if opts.RateLimitBurst <= 0 {
		opts.RateLimitBurst = 10
	}
	if opts.RateLimitDaily <= 0 {
		opts.RateLimitDaily = 1000
	}
	rl := &rateLimiter{
		opts:     opts,
		origins:  map[string]bool{},
		visitors: map[string]*bucket{},
	}
	for _, c := range opts.ExemptCIDRs {
		_, ipnet, err := net.ParseCIDR(strings.TrimSpace(c))
		if err != nil {
			return nil, fmt.Errorf("api.exempt_cidrs entry %q: %w", c, err)
		}
		rl.exempt = append(rl.exempt, ipnet)
	}
	for _, o := range opts.CORSOrigins {
		rl.origins[strings.TrimSpace(o)] = true
	}
	return rl, nil
}

// clientIP resolves the real client address. X-Forwarded-For is
// honored only when the direct peer IS the trusted proxy; the
// RIGHTMOST XFF entry is the address our own proxy appended.
func (rl *rateLimiter) clientIP(r *http.Request) net.IP {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer := net.ParseIP(host)
	if rl.opts.TrustedProxy == "" || host != rl.opts.TrustedProxy {
		return peer
	}
	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return peer
	}
	parts := strings.Split(xff, ",")
	if ip := net.ParseIP(strings.TrimSpace(parts[len(parts)-1])); ip != nil {
		return ip
	}
	return peer
}

func (rl *rateLimiter) isExempt(ip net.IP) bool {
	if ip == nil {
		return false
	}
	for _, n := range rl.exempt {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// middleware enforces the bucket + daily quota for non-exempt IPs.
func (rl *rateLimiter) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := rl.clientIP(r)
		if rl.isExempt(ip) {
			next.ServeHTTP(w, r)
			return
		}
		// v0.29.82 (operator, 2026-10-08): the per-IP limit is only for
		// callers without a valid token. A valid session is not counted; an
		// API token is counted against its own hourly allowance. An unknown
		// or expired token, or one the store could not resolve, is no token.
		if res, ok := resolutionOf(r); ok && res.presented && res.err == nil {
			if res.info.APITokenID == 0 {
				next.ServeHTTP(w, r)
				return
			}
			allowed, remaining, reset := rl.allowToken(res.info.APITokenID, res.info.UserID, res.info.RateLimitPerHour)
			h := w.Header()
			h.Set("X-RateLimit-Limit", strconv.Itoa(res.info.RateLimitPerHour))
			h.Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
			h.Set("X-RateLimit-Reset", strconv.FormatInt(reset.Unix(), 10))
			if !allowed {
				retry := int(math.Ceil(reset.Sub(rl.clock()).Seconds()))
				if retry < 1 {
					retry = 1
				}
				h.Set("Retry-After", strconv.Itoa(retry))
				http.Error(w, "API token's hourly allowance exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		if rl.uncounted != nil && rl.uncounted(r) {
			next.ServeHTTP(w, r)
			return
		}
		key := ""
		if ip != nil {
			key = ip.String()
		}
		now := rl.clock()
		rl.mu.Lock()
		b, ok := rl.visitors[key]
		if !ok {
			if len(rl.visitors) >= maxTrackedIPs {
				rl.evictOldestLocked()
			}
			b = &bucket{tokens: float64(rl.opts.RateLimitBurst), last: now}
			rl.visitors[key] = b
		}
		b.lastSeen = now
		day := now.UTC().Format("2006-01-02")
		if b.day != day {
			b.day = day
			b.dayCount = 0
		}
		b.dayCount++
		overQuota := b.dayCount > rl.opts.RateLimitDaily
		allowed := !overQuota && b.allow(now, rl.opts.RateLimitRPS, rl.opts.RateLimitBurst)
		// A request whose token identify deferred may carry a valid token: it
		// is refused only until the deferral ends, so its Retry-After says
		// that, not the daily quota's day (L10 round 2 on 0.29.82).
		var deferredUntil time.Time
		if res, ok := resolutionOf(r); ok && errors.Is(res.err, errLookupDeferred) {
			deferredUntil = b.badTokenAt.Add(badTokenMemory)
		}
		rl.mu.Unlock()

		if !allowed {
			retry := "1"
			if overQuota {
				retry = "86400"
				if !deferredUntil.IsZero() {
					secs := int(math.Ceil(deferredUntil.Sub(now).Seconds()))
					if secs < 1 {
						secs = 1
					}
					retry = strconv.Itoa(secs)
				}
			}
			w.Header().Set("Retry-After", retry)
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// evictOldestLocked drops the least-recently-seen ~1% of visitors.
// Called with rl.mu held.
func (rl *rateLimiter) evictOldestLocked() {
	n := maxTrackedIPs / 100
	for i := 0; i < n; i++ {
		var oldestKey string
		var oldest time.Time
		for k, v := range rl.visitors {
			if oldestKey == "" || v.lastSeen.Before(oldest) {
				oldestKey, oldest = k, v.lastSeen
			}
		}
		if oldestKey == "" {
			return
		}
		delete(rl.visitors, oldestKey)
	}
}

// corsAllowHeaders are the request headers a cross-origin caller may send:
// its token, a JSON body, and If-None-Match, which revalidates an answer
// against the ETag the API exposes (v0.29.73).
const corsAllowHeaders = "Authorization, Content-Type, If-None-Match"

// cors is the SINGLE CORS authority (v0.27.1 removed the per-handler
// wildcard/echo headers that predated it). Empty cors_origins =
// legacy-compatible `*` (the server-rendered GUI's cross-port fetches
// rely on it); configured = strict allowlist — set it in production.
func (rl *rateLimiter) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		// Every answer depends on the request's Origin (the headers below),
		// so a shared cache in front of the API must keep one copy per
		// Origin value — also for a request without one, whose stored copy
		// carries no CORS headers (v0.29.73).
		w.Header().Set("Vary", "Origin")
		if origin != "" && len(rl.origins) == 0 {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", corsAllowHeaders)
		} else if origin != "" && rl.origins[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", corsAllowHeaders)
			w.Header().Set("Access-Control-Max-Age", "600")
		}
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
