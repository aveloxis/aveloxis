// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.0 — rate-limiting + CORS middleware for the public analytics
// API (plan: summary/api-analytics-plan-2026-07-10.md §3). Operator
// requirements: limits apply ONLY to clients outside the LAN
// (exempt CIDRs, default loopback+RFC1918); defaults 1 rps / burst 10
// / 1,000 per day per IP; X-Forwarded-For honored only from the
// configured trusted proxy.

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func testLimiter(t *testing.T, opts Options) http.Handler {
	t.Helper()
	rl, err := newRateLimiter(opts)
	if err != nil {
		t.Fatal(err)
	}
	return rl.middleware(okHandler())
}

func TestRateLimiterBurstThen429(t *testing.T) {
	h := testLimiter(t, Options{RateLimitRPS: 1, RateLimitBurst: 3, RateLimitDaily: 1000})
	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/health", nil)
		req.RemoteAddr = "203.0.113.5:1234" // public IP — not exempt
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d within burst should pass, got %d", i, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/health", nil)
	req.RemoteAddr = "203.0.113.5:1234"
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("burst exceeded should 429, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("429 must carry Retry-After")
	}
}

func TestRateLimiterExemptCIDRBypasses(t *testing.T) {
	h := testLimiter(t, Options{RateLimitRPS: 1, RateLimitBurst: 1, RateLimitDaily: 2,
		ExemptCIDRs: []string{"127.0.0.0/8", "10.0.0.0/8"}})
	// Far beyond both bucket and quota — every request must pass.
	for i := 0; i < 20; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/health", nil)
		req.RemoteAddr = "10.1.2.3:9999"
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("LAN request %d must bypass the limiter, got %d", i, rec.Code)
		}
	}
}

func TestRateLimiterXFFOnlyFromTrustedProxy(t *testing.T) {
	// Trusted proxy set: XFF resolves the client, so a PUBLIC XFF
	// address behind the proxy is limited even though RemoteAddr is
	// loopback (the nginx-on-same-box layout).
	h := testLimiter(t, Options{RateLimitRPS: 1, RateLimitBurst: 1, RateLimitDaily: 1000,
		ExemptCIDRs: []string{"127.0.0.0/8"}, TrustedProxy: "127.0.0.1"})
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/health", nil)
		req.RemoteAddr = "127.0.0.1:5555"
		req.Header.Set("X-Forwarded-For", "203.0.113.9")
		h.ServeHTTP(rec, req)
		if i == 0 && rec.Code != http.StatusOK {
			t.Fatalf("first proxied public request should pass, got %d", rec.Code)
		}
		if i == 1 && rec.Code != http.StatusTooManyRequests {
			t.Fatalf("second proxied public request should 429 (burst 1), got %d", rec.Code)
		}
	}

	// NO trusted proxy: XFF is attacker-controlled and must be
	// IGNORED — a public RemoteAddr claiming a loopback XFF must
	// still be limited.
	h2 := testLimiter(t, Options{RateLimitRPS: 1, RateLimitBurst: 1, RateLimitDaily: 1000,
		ExemptCIDRs: []string{"127.0.0.0/8"}})
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/health", nil)
		req.RemoteAddr = "203.0.113.9:5555"
		req.Header.Set("X-Forwarded-For", "127.0.0.1") // spoof attempt
		h2.ServeHTTP(rec, req)
		if i == 1 && rec.Code != http.StatusTooManyRequests {
			t.Fatalf("spoofed XFF without trusted proxy must NOT exempt; got %d", rec.Code)
		}
	}
}

func TestRateLimiterDailyQuota(t *testing.T) {
	rl, err := newRateLimiter(Options{RateLimitRPS: 1000, RateLimitBurst: 1000, RateLimitDaily: 5})
	if err != nil {
		t.Fatal(err)
	}
	h := rl.middleware(okHandler())
	var got429 bool
	for i := 0; i < 7; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/health", nil)
		req.RemoteAddr = "203.0.113.7:1"
		h.ServeHTTP(rec, req)
		if i >= 5 && rec.Code == http.StatusTooManyRequests {
			got429 = true
		}
	}
	if !got429 {
		t.Error("daily quota (5) exceeded must 429 even when the token bucket allows")
	}
}

func TestBucketRefills(t *testing.T) {
	b := &bucket{tokens: 0, last: time.Now().Add(-2 * time.Second)}
	if !b.allow(time.Now(), 1, 10) {
		t.Error("bucket must refill ~2 tokens after 2s at 1 rps")
	}
}

func TestCORSMiddleware(t *testing.T) {
	rl, err := newRateLimiter(Options{RateLimitRPS: 100, RateLimitBurst: 100, RateLimitDaily: 1000,
		CORSOrigins: []string{"https://gui.example.org"}})
	if err != nil {
		t.Fatal(err)
	}
	h := rl.cors(okHandler())

	// Allowed origin: headers set; preflight short-circuits.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("OPTIONS", "/api/v1/compare", nil)
	req.Header.Set("Origin", "https://gui.example.org")
	req.Header.Set("Access-Control-Request-Method", "GET")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Errorf("preflight should 204, got %d", rec.Code)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "https://gui.example.org" {
		t.Error("allowed origin must be echoed in Access-Control-Allow-Origin")
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got == "" {
		t.Error("preflight must allow the Authorization header (Bearer auth)")
	}

	// Disallowed origin: no CORS headers.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/api/v1/health", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	h.ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("disallowed origin must not receive Access-Control-Allow-Origin")
	}
}

func TestServerNewWithOptionsWiresMiddleware(t *testing.T) {
	// Source-contract: Handler() must serve through the limiter+CORS
	// chain, and main.go must thread the config.
	srcAPI := mustReadFile(t, "server.go")
	if !contains(srcAPI, "NewWithOptions") || !contains(srcAPI, "middleware(") {
		t.Error("api.Server must expose NewWithOptions and route Handler() through the rate-limit middleware")
	}
	srcMain := mustReadFile(t, "../../cmd/aveloxis/main.go")
	if !contains(srcMain, "api.NewWithOptions(") {
		t.Error("main.go runAPI must construct the server via api.NewWithOptions with the api config block")
	}
}
