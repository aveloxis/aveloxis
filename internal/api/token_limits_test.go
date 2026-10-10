// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.29.82 (operator, 2026-10-08): the per-IP rate limit is ONLY for callers
// without a valid token. A valid session token is not counted; an
// operator-issued API token is counted against its own hourly allowance,
// whatever address it comes from; no token, an unknown token or an expired
// one stays on the per-IP bucket and daily cap. The kate incident: a
// signed-in browser opening a large repository page drained the per-IP
// burst and charts failed with 429.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

func tokenChain(t *testing.T, store sessionStore, opts Options) (http.Handler, *rateLimiter) {
	t.Helper()
	rl, err := newRateLimiter(opts)
	if err != nil {
		t.Fatal(err)
	}
	return requestChain(rl, newAuthenticator(store, false, nil), okHandler()), rl
}

func tokenReq(addr, token string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/api/v1/repos/1/timeseries", nil)
	r.RemoteAddr = addr
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r
}

var tightLimits = Options{RateLimitRPS: 0.001, RateLimitBurst: 2, RateLimitDaily: 1000}

func TestValidSessionTokenIsNeverRateLimited(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"sess": true}}
	h, _ := tokenChain(t, store, tightLimits)
	for i := 0; i < 20; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq("203.0.113.9:1", "sess"))
		if w.Code != http.StatusOK {
			t.Fatalf("request %d with a valid session token = %d; a signed-in caller is never counted", i, w.Code)
		}
	}
	// The same address without a token is still limited, and not charged
	// for the signed-in requests before it.
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq("203.0.113.9:1", ""))
		if w.Code != http.StatusOK {
			t.Fatalf("anonymous request %d within the burst = %d", i, w.Code)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq("203.0.113.9:1", ""))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("an anonymous request past the burst = %d, want 429", w.Code)
	}
}

func TestUnknownTokenIsCountedPerIP(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"sess": true}}
	h, _ := tokenChain(t, store, tightLimits)
	codes := []int{}
	for i := 0; i < 3; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq("203.0.113.10:1", "made-up"))
		codes = append(codes, w.Code)
	}
	if codes[2] != http.StatusTooManyRequests {
		t.Fatalf("an unknown token must not bypass the per-IP limit: %v", codes)
	}
}

func TestStoreFailureIsCountedPerIP(t *testing.T) {
	store := &fakeSessionStore{validateErr: errors.New("connection refused")}
	h, _ := tokenChain(t, store, tightLimits)
	codes := []int{}
	for i := 0; i < 3; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq("203.0.113.11:1", "sess"))
		codes = append(codes, w.Code)
	}
	// Within the burst the auth layer refuses (503: the token could not be
	// resolved); past it the limiter refuses first.
	if codes[0] != http.StatusServiceUnavailable || codes[2] != http.StatusTooManyRequests {
		t.Fatalf("a token the store could not resolve is counted per IP: %v", codes)
	}
}

func TestAPITokenHasItsOwnHourlyAllowance(t *testing.T) {
	store := &fakeSessionStore{userID: 7, apiValid: map[string]db.APITokenIdentity{
		db.APITokenPrefix + "good": {TokenID: 41, UserID: 7, RateLimitPerHour: 3},
	}}
	h, rl := tokenChain(t, store, tightLimits)
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	rl.now = func() time.Time { return now }
	tok := db.APITokenPrefix + "good"
	for i := 0; i < 3; i++ {
		// Different addresses: the allowance belongs to the token.
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq("198.51.100."+strconv.Itoa(i+1)+":1", tok))
		if w.Code != http.StatusOK {
			t.Fatalf("call %d of 3 = %d", i+1, w.Code)
		}
		if w.Header().Get("X-RateLimit-Limit") != "3" || w.Header().Get("X-RateLimit-Remaining") != strconv.Itoa(2-i) {
			t.Fatalf("call %d headers: limit=%q remaining=%q", i+1, w.Header().Get("X-RateLimit-Limit"), w.Header().Get("X-RateLimit-Remaining"))
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq("198.51.100.99:1", tok))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("the fourth call in the hour = %d, want 429", w.Code)
	}
	if w.Header().Get("X-RateLimit-Remaining") != "0" || w.Header().Get("Retry-After") != "3600" {
		t.Fatalf("429 headers: remaining=%q retry-after=%q (the window opened just now)", w.Header().Get("X-RateLimit-Remaining"), w.Header().Get("Retry-After"))
	}
	// The API token's calls did not touch the per-IP buckets.
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq("198.51.100.1:1", ""))
		if w.Code != http.StatusOK {
			t.Fatalf("an anonymous call from an address the token used = %d; the token is not charged to it", w.Code)
		}
	}
	// A new hour opens a new window.
	now = now.Add(time.Hour)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq("198.51.100.99:1", tok))
	if w.Code != http.StatusOK {
		t.Fatalf("a call in the next hour = %d", w.Code)
	}
}

// One request resolves its token once: the identify step's answer is the
// one the limiter and the auth layer both use.
func TestTokenResolvedOncePerRequest(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"sess": true}}
	h, _ := tokenChain(t, store, tightLimits)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq("203.0.113.12:1", "sess"))
	if n := store.validates.Load(); n != 1 {
		t.Fatalf("one request resolved its token %d times, want 1", n)
	}
}

// Handler serves through requestChain, so these tests drive production's
// order.
func TestHandlerUsesRequestChain(t *testing.T) {
	body := mustReadFile(t, "server.go")
	if !contains(body, "return requestChain(s.limiter, s.auth, s.mux)") {
		t.Fatal("Handler must return requestChain(s.limiter, s.auth, s.mux)")
	}
}

// L10 round 1 on 0.29.82 (MEDIUM): identify runs before the limiter, so an
// address over its limit could make every request cost a store lookup by
// sending made-up tokens (an invalid answer is never cached, and a fresh
// random token defeats any cache). Once such an address has presented an
// invalid token, further uncached tokens from it are not looked up while
// its bucket is empty: the limiter refuses them at no database cost.
func TestOverLimitAddressCannotForceLookupsWithJunkTokens(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"sess": true}}
	h, _ := tokenChain(t, store, tightLimits)
	addr := "203.0.113.20:1"
	for i := 0; i < 2; i++ { // empty the bucket
		h.ServeHTTP(httptest.NewRecorder(), tokenReq(addr, ""))
	}
	for i := 0; i < 20; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq(addr, "junk-"+strconv.Itoa(i)))
		if w.Code != http.StatusTooManyRequests {
			t.Fatalf("junk token %d from an address over its limit = %d, want 429", i, w.Code)
		}
	}
	if n := store.validates.Load(); n > 1 {
		t.Fatalf("20 junk tokens from an address over its limit cost %d store lookups; want at most 1", n)
	}
}

// The other side of the same rule: an address over its limit that has
// presented no invalid token (a busy shared address, the kate campus case)
// still gets its signed-in callers resolved and served.
func TestSignedInCallerBehindABusyAddressIsStillServed(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"sess": true}}
	h, _ := tokenChain(t, store, tightLimits)
	addr := "203.0.113.21:1"
	for i := 0; i < 5; i++ { // anonymous traffic empties the shared bucket
		h.ServeHTTP(httptest.NewRecorder(), tokenReq(addr, ""))
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq(addr, "sess"))
	if w.Code != http.StatusOK {
		t.Fatalf("a valid session behind a busy address = %d, want 200", w.Code)
	}
}

// L10 round 2 on 0.29.82: the deferral's other arms. An address past its
// DAILY quota keeps a full bucket (over-quota requests never reach allow),
// so only the quota arm defers its junk tokens.
func TestOverQuotaAddressCannotForceLookupsWithJunkTokens(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"sess": true}}
	h, _ := tokenChain(t, store, Options{RateLimitRPS: 1000, RateLimitBurst: 1000, RateLimitDaily: 2})
	addr := "203.0.113.30:1"
	for i := 0; i < 3; i++ { // spend the daily quota
		h.ServeHTTP(httptest.NewRecorder(), tokenReq(addr, ""))
	}
	for i := 0; i < 20; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq(addr, "junk-"+strconv.Itoa(i)))
		if w.Code != http.StatusTooManyRequests {
			t.Fatalf("junk token %d from an address past its daily quota = %d, want 429", i, w.Code)
		}
	}
	if n := store.validates.Load(); n > 1 {
		t.Fatalf("20 junk tokens from an address past its daily quota cost %d lookups; want at most 1", n)
	}
}

// The deferral lasts badTokenMemory: after it, a valid token from the same
// over-limit address is looked up and served. While it lasts, the 429 says
// when it ends — not the daily quota's day (round 2: a deferred, possibly
// valid caller told to wait 86,400 s).
func TestDeferralEndsAfterBadTokenMemory(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"sess": true}}
	h, rl := tokenChain(t, store, Options{RateLimitRPS: 0.0001, RateLimitBurst: 1, RateLimitDaily: 1})
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	rl.now = func() time.Time { return now }
	addr := "203.0.113.31:1"
	h.ServeHTTP(httptest.NewRecorder(), tokenReq(addr, ""))       // the quota (1) is spent
	h.ServeHTTP(httptest.NewRecorder(), tokenReq(addr, "junk-0")) // the bad token is noted
	w := httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq(addr, "sess"))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("during the deferral a valid but uncached token = %d, want 429", w.Code)
	}
	if ra := w.Header().Get("Retry-After"); ra != strconv.Itoa(int(badTokenMemory.Seconds())) {
		t.Fatalf("a deferred caller's Retry-After = %q, want when the deferral ends (%d), not the daily quota's day", ra, int(badTokenMemory.Seconds()))
	}
	now = now.Add(badTokenMemory)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq(addr, "sess"))
	if w.Code != http.StatusOK {
		t.Fatalf("after the deferral a valid token from the same address = %d, want 200 (looked up, not counted)", w.Code)
	}
}

// A request whose lookup identify deferred, but which the limiter admitted
// anyway (in production: a front-end-forwarded request the limiter does not
// count), is looked up by the auth layer — never answered as a store
// failure.
func TestDeferredButAdmittedRequestIsLookedUp(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"sess": true}}
	a := newAuthenticator(store, true, nil)
	rl, err := newRateLimiter(tightLimits)
	if err != nil {
		t.Fatal(err)
	}
	h := a.middleware(rl, okHandler())
	r := tokenReq("203.0.113.32:1", "sess")
	r = r.WithContext(context.WithValue(r.Context(), resolutionCtxKey{}, resolution{presented: true, err: errLookupDeferred}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK || store.validates.Load() != 1 {
		t.Fatalf("a deferred, admitted request = %d after %d lookups; want 200 after one lookup", w.Code, store.validates.Load())
	}
}

// Copilot review 5475865946 on PR #228 (MEDIUM): identify took a cached
// identity before the junk-token deferral only for SESSION tokens, so a
// cached, valid API token behind an address that had just sent a bad token
// and run out of its limit was deferred and given the address's 429 instead
// of being rechecked and charged to its own allowance. Only UNCACHED tokens
// are deferred: a cached one was valid within authCacheTTL, which a caller
// cannot fabricate, and its recheck is one indexed lookup.
func TestCachedAPITokenBehindADeferringAddressIsServed(t *testing.T) {
	tok := db.APITokenPrefix + "good"
	store := &fakeSessionStore{userID: 7, apiValid: map[string]db.APITokenIdentity{
		tok: {TokenID: 41, UserID: 7, RateLimitPerHour: 100},
	}}
	h, _ := tokenChain(t, store, tightLimits)
	addr := "203.0.113.40:1"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq(addr, tok)) // validated and cached
	if w.Code != http.StatusOK {
		t.Fatalf("first API-token call = %d", w.Code)
	}
	for i := 0; i < 3; i++ { // someone else on the address empties its bucket
		h.ServeHTTP(httptest.NewRecorder(), tokenReq(addr, ""))
	}
	h.ServeHTTP(httptest.NewRecorder(), tokenReq(addr, "junk")) // and sends a bad token
	w = httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq(addr, tok))
	if w.Code != http.StatusOK || w.Header().Get("X-RateLimit-Limit") != "100" {
		t.Fatalf("a cached API token behind a deferring address = %d (limit header %q), want 200 charged to the token", w.Code, w.Header().Get("X-RateLimit-Limit"))
	}
	// Junk from the same address is still deferred.
	before := store.validates.Load()
	for i := 0; i < 5; i++ {
		h.ServeHTTP(httptest.NewRecorder(), tokenReq(addr, "junk-"+strconv.Itoa(i)))
	}
	if n := store.validates.Load() - before; n > 0 {
		t.Fatalf("uncached junk tokens from the deferring address cost %d lookups, want 0", n)
	}
}

// Copilot review 5476192626 on PR #228: X-RateLimit-* and Retry-After are
// not CORS-safelisted, so a cross-origin browser client could not read the
// headers api.md promises. Every allowed cross-origin answer exposes them.
func TestRateLimitHeadersAreExposedCrossOrigin(t *testing.T) {
	tok := db.APITokenPrefix + "good"
	store := &fakeSessionStore{userID: 7, apiValid: map[string]db.APITokenIdentity{tok: {TokenID: 9, UserID: 7, RateLimitPerHour: 50}}}
	h, _ := tokenChain(t, store, tightLimits)
	r := tokenReq("198.51.100.70:1", tok)
	r.Header.Set("Origin", "https://app.example.org")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	exposed := strings.Join(w.Header().Values("Access-Control-Expose-Headers"), ",")
	names := map[string]bool{}
	for _, n := range strings.Split(exposed, ",") {
		names[strings.TrimSpace(n)] = true
	}
	// Exact names: "RateLimit" is a substring of "X-RateLimit-Limit".
	// RateLimit and RateLimit-Policy since v0.29.89 (summary/53).
	for _, name := range []string{"X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset", "Retry-After", "RateLimit", "RateLimit-Policy"} {
		if !names[name] {
			t.Fatalf("%s is not exposed to a cross-origin client (Access-Control-Expose-Headers: %q)", name, exposed)
		}
	}
	// A same-origin request (no Origin) gets no CORS headers at all.
	w = httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq("198.51.100.70:1", tok))
	if w.Header().Get("Access-Control-Expose-Headers") != "" {
		t.Fatal("no Origin, no CORS headers")
	}
}

// L10 round 1 on 0.29.85: the allowlisted-origin branch exposes them too.
func TestRateLimitHeadersAreExposedToAnAllowlistedOrigin(t *testing.T) {
	tok := db.APITokenPrefix + "good"
	store := &fakeSessionStore{userID: 7, apiValid: map[string]db.APITokenIdentity{tok: {TokenID: 9, UserID: 7, RateLimitPerHour: 50}}}
	opts := tightLimits
	opts.CORSOrigins = []string{"https://app.example.org"}
	h, _ := tokenChain(t, store, opts)
	r := tokenReq("198.51.100.71:1", tok)
	r.Header.Set("Origin", "https://app.example.org")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Header().Get("Access-Control-Allow-Origin") != "https://app.example.org" {
		t.Fatalf("the allowlisted origin was not allowed: %q", w.Header().Get("Access-Control-Allow-Origin"))
	}
	exposed := strings.Join(w.Header().Values("Access-Control-Expose-Headers"), ",")
	for _, name := range []string{"X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset", "Retry-After"} {
		if !strings.Contains(exposed, name) {
			t.Fatalf("%s is not exposed to an allowlisted origin (%q)", name, exposed)
		}
	}
}

// Copilot review 5477687920 on PR #228 (HIGH): an exempt network
// (api.exempt_cidrs) returned before an API token was charged, so a token
// used from the LAN or loopback had no hourly allowance and no headers —
// against "counted against its own allowance, whatever address". A token
// is charged everywhere; the exemption is for callers without one.
func TestAPITokenIsChargedFromAnExemptNetwork(t *testing.T) {
	tok := db.APITokenPrefix + "lan"
	store := &fakeSessionStore{userID: 7, apiValid: map[string]db.APITokenIdentity{tok: {TokenID: 12, UserID: 7, RateLimitPerHour: 2}},
		valid: map[string]bool{"sess": true}}
	opts := tightLimits
	opts.ExemptCIDRs = []string{"127.0.0.0/8"}
	h, _ := tokenChain(t, store, opts)
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq("127.0.0.1:1", tok))
		if w.Code != http.StatusOK || w.Header().Get("X-RateLimit-Limit") != "2" {
			t.Fatalf("call %d from an exempt address: %d, limit header %q", i+1, w.Code, w.Header().Get("X-RateLimit-Limit"))
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq("127.0.0.1:1", tok))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("the third call in the hour from an exempt address = %d, want 429", w.Code)
	}
	// The exemption still holds for callers without a token, and sessions.
	for i := 0; i < 5; i++ {
		for _, bearer := range []string{"", "sess"} {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, tokenReq("127.0.0.1:1", bearer))
			if w.Code != http.StatusOK {
				t.Fatalf("an exempt caller (bearer %q) was limited: %d", bearer, w.Code)
			}
		}
	}
}

// ASVS review G4: a cached API token over its hourly allowance was still
// rechecked against the store (APITokenActive) on every request before the
// limiter answered 429. Once its window is spent the 429 needs no lookup.
func TestExhaustedAPITokenCostsNoRecheck(t *testing.T) {
	tok := db.APITokenPrefix + "spent"
	store := &fakeSessionStore{userID: 7, apiValid: map[string]db.APITokenIdentity{tok: {TokenID: 21, UserID: 7, RateLimitPerHour: 2}}}
	h, _ := tokenChain(t, store, tightLimits)
	for i := 0; i < 2; i++ {
		h.ServeHTTP(httptest.NewRecorder(), tokenReq("198.51.100.90:1", tok))
	}
	before := store.actives.Load()
	for i := 0; i < 10; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq("198.51.100.90:1", tok))
		if w.Code != http.StatusTooManyRequests {
			t.Fatalf("over its allowance = %d, want 429", w.Code)
		}
	}
	if n := store.actives.Load() - before; n != 0 {
		t.Fatalf("10 refused requests of an exhausted token cost %d store rechecks, want 0", n)
	}
}
