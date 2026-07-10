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
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
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
		key := ""
		if ip != nil {
			key = ip.String()
		}
		now := time.Now()
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
		rl.mu.Unlock()

		if !allowed {
			retry := "1"
			if overQuota {
				retry = "86400"
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

// cors is the SINGLE CORS authority (v0.27.1 removed the per-handler
// wildcard/echo headers that predated it). Empty cors_origins =
// legacy-compatible `*` (the server-rendered GUI's cross-port fetches
// rely on it); configured = strict allowlist — set it in production.
func (rl *rateLimiter) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && len(rl.origins) == 0 {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		} else if origin != "" && rl.origins[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			w.Header().Set("Access-Control-Max-Age", "600")
		}
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
