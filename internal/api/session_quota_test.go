// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.29.89 (summary/53): a signed-in session is counted against
// requests_per_hour and requests_per_day (an account's overrides replacing
// the values) by the one capacity.Meter. Each quota runs in a mode: shadow
// (the dark launch: served, logged once per window), enforce (refused with
// the capacity_limit body until the window ends) or off. This replaces the
// 0.29.85 session observation and the 0.29.88 ceiling.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/db"
)

// fakeCapacityStore is the policy's store: quotas as applied, overrides,
// the contact address; err fails every read.
type fakeCapacityStore struct {
	mu        sync.Mutex
	quotas    map[string]capacity.EffectiveQuota
	overrides map[int]db.CapacityOverride
	contact   string
	err       error
	reads     int
	block     chan struct{} // when set, a read waits for it
	started   chan struct{}
}

func (f *fakeCapacityStore) EffectiveCapacityQuotas(context.Context) (map[string]capacity.EffectiveQuota, error) {
	if f.started != nil {
		f.started <- struct{}{}
	}
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]capacity.EffectiveQuota{}
	for _, n := range capacity.QuotaNames() {
		st := capacity.Shipped[n]
		out[n] = capacity.Effective(n, capacity.SourceWeb, &st)
	}
	for k, v := range f.quotas {
		out[k] = v
	}
	return out, nil
}

func (f *fakeCapacityStore) ListCapacityOverrides(context.Context) (map[int]db.CapacityOverride, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	out := map[int]db.CapacityOverride{}
	for k, v := range f.overrides {
		out[k] = v
	}
	return out, nil
}

func (f *fakeCapacityStore) GetCapacityContact(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.contact, f.err
}

func (f *fakeCapacityStore) CapacitySource(string) capacity.Source { return capacity.SourceWeb }

func quotaOf(name string, allowed int, mode capacity.Mode) capacity.EffectiveQuota {
	return capacity.EffectiveQuota{Name: name, Allowed: allowed, Mode: mode, Source: capacity.SourceWeb, Editable: true}
}

// sessionQuotaChain is the real request chain with a session "sess" (user
// 7), an API token (id 3, user 7) and the policy read from cs, inline.
func sessionQuotaChain(t *testing.T, cs *fakeCapacityStore) (http.Handler, *rateLimiter, *lockedBuffer, *time.Time) {
	t.Helper()
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"sess": true},
		apiValid: map[string]db.APITokenIdentity{db.APITokenPrefix + "tok": {TokenID: 3, UserID: 7, RateLimitPerHour: 1_000_000, RateLimitPerDay: 1_000_000}}}
	// Exempt the loopback: a session is counted whatever its address.
	h, rl := tokenChain(t, store, Options{RateLimitRPS: 1000, RateLimitBurst: 1000, RateLimitDaily: 1_000_000, ExemptCIDRs: []string{"127.0.0.0/8"}})
	logs := &lockedBuffer{}
	rl.logger = slog.New(slog.NewTextHandler(logs, nil))
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	rl.now = func() time.Time { return now }
	rl.policy = &capacityPolicy{store: cs, now: rl.clock, logger: rl.logger, spawn: func(f func()) { f() }}
	return h, rl, logs, &now
}

func sessionUsed(rl *rateLimiter, name string, w capacity.Window) int {
	return rl.quotaMeter().Peek(capacity.Subject{Kind: capacity.KindAccount, ID: "7"}, capacity.RateQuota{Name: name, Window: w, Allowed: 1 << 30}).Used
}

// Shadow (the shipped mode for the session quotas): every request is served,
// the first request past the value is logged once per window, and the
// quota is not advertised (a client must not slow down for it).
func TestSessionQuotaInShadowServesAndLogsOnce(t *testing.T) {
	cs := &fakeCapacityStore{quotas: map[string]capacity.EffectiveQuota{
		capacity.QuotaRequestsPerHour: quotaOf(capacity.QuotaRequestsPerHour, 10, capacity.Shadow)}}
	h, rl, logs, now := sessionQuotaChain(t, cs)
	for i := 0; i < 25; i++ {
		addr := "198.51.100." + strconv.Itoa(i%3+1) + ":1"
		if i%2 == 0 {
			addr = "127.0.0.1:1" // an exempt address still counts
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq(addr, "sess"))
		if w.Code != http.StatusOK {
			t.Fatalf("session request %d = %d: a shadow quota never refuses", i+1, w.Code)
		}
		if w.Header().Get("RateLimit-Policy") != "" || w.Header().Get("X-RateLimit-Limit") != "" {
			t.Fatalf("a shadow quota was advertised: %v", w.Header())
		}
	}
	out := logs.String()
	if n := strings.Count(out, "quota reached (shadow)"); n != 1 {
		t.Fatalf("25 requests against a shadow 10 logged %d lines, want 1:\n%s", n, out)
	}
	for _, want := range []string{"subject=account", "user_id=7", "token_id=0", "quota=requests_per_hour", "allowed=10", "used=11"} {
		if !strings.Contains(out, want) {
			t.Fatalf("the line must carry %s:\n%s", want, out)
		}
	}
	if strings.Contains(out, "sess ") || strings.Contains(out, "=sess") {
		t.Fatalf("a log line carries the token:\n%s", out)
	}
	if got := sessionUsed(rl, capacity.QuotaRequestsPerHour, capacity.Hour); got != 25 {
		t.Fatalf("counted %d, want 25", got)
	}
	*now = now.Add(time.Hour)
	for i := 0; i < 11; i++ {
		h.ServeHTTP(httptest.NewRecorder(), tokenReq("198.51.100.1:1", "sess"))
	}
	if n := strings.Count(logs.String(), "quota reached (shadow)"); n != 2 {
		t.Fatalf("a new hour must log at its own 11th request: %d lines", n)
	}
}

// Enforce: past the value the session is refused with the one capacity
// body — 429, Retry-After until the window ends, the kind message naming
// the hardware constraint and the contact address — from every address,
// logged once; the window's end starts it over.
func TestSessionQuotaEnforcedRefusesWithTheCapacityBody(t *testing.T) {
	cs := &fakeCapacityStore{contact: "help@example.org", quotas: map[string]capacity.EffectiveQuota{
		capacity.QuotaRequestsPerHour: quotaOf(capacity.QuotaRequestsPerHour, 10, capacity.Enforce)}}
	h, _, logs, now := sessionQuotaChain(t, cs)
	for i := 0; i < 10; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq("127.0.0.1:1", "sess"))
		if w.Code != http.StatusOK {
			t.Fatalf("request %d of 10 = %d", i+1, w.Code)
		}
		if got := w.Header().Get("X-RateLimit-Remaining"); got != strconv.Itoa(9-i) {
			t.Fatalf("request %d: X-RateLimit-Remaining %q, want %d", i+1, got, 9-i)
		}
		if !strings.Contains(w.Header().Get("RateLimit-Policy"), `"requests_per_hour";q=10;w=3600`) {
			t.Fatalf("RateLimit-Policy = %q", w.Header().Get("RateLimit-Policy"))
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq("127.0.0.1:1", "sess"))
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "3600" {
		t.Fatalf("past the quota = %d (Retry-After %q), want 429 with 3600", w.Code, w.Header().Get("Retry-After"))
	}
	if !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
		t.Fatal("a refusal is never stored")
	}
	var body capacityRefusalBody
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("refusal body is not JSON: %v %s", err, w.Body.String())
	}
	if body.Error != "capacity_limit" || body.Quota != capacity.QuotaRequestsPerHour || body.Window != "hour" ||
		body.Allowed != 10 || body.RetryAfter != 3600 || body.Contact != "help@example.org" ||
		!strings.Contains(body.Message, "limited hardware and infrastructure") || !strings.Contains(body.Message, "help@example.org") {
		t.Fatalf("refusal body = %+v", body)
	}
	for i := 0; i < 3; i++ {
		h.ServeHTTP(httptest.NewRecorder(), tokenReq("198.51.100.1:1", "sess"))
	}
	if n := strings.Count(logs.String(), "quota reached — refused"); n != 1 {
		t.Fatalf("the refusal is logged once per window, got %d:\n%s", n, logs.String())
	}
	*now = now.Add(time.Hour)
	if w := httptest.NewRecorder(); func() int { h.ServeHTTP(w, tokenReq("127.0.0.1:1", "sess")); return w.Code }() != http.StatusOK {
		t.Fatal("a new hour must start over")
	}
}

// The day quota is the UTC calendar day: it resets at 00:00 UTC, and the
// tighter of the enforced quotas is the one advertised.
func TestSessionDayQuotaResetsAtUTCMidnight(t *testing.T) {
	cs := &fakeCapacityStore{quotas: map[string]capacity.EffectiveQuota{
		capacity.QuotaRequestsPerHour: quotaOf(capacity.QuotaRequestsPerHour, 1000, capacity.Enforce),
		capacity.QuotaRequestsPerDay:  quotaOf(capacity.QuotaRequestsPerDay, 5, capacity.Enforce)}}
	h, _, _, now := sessionQuotaChain(t, cs)
	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq("198.51.100.1:1", "sess"))
		if w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("RateLimit"), `"requests_per_day";r=`) {
			t.Fatalf("request %d = %d, RateLimit %q; want 200 advertising the day (the tighter)", i+1, w.Code, w.Header().Get("RateLimit"))
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq("198.51.100.1:1", "sess"))
	midnight := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != strconv.Itoa(int(midnight.Sub(*now).Seconds())) {
		t.Fatalf("past the day = %d, Retry-After %q; want 429 until 00:00 UTC", w.Code, w.Header().Get("Retry-After"))
	}
	// One hour later the hour has reset but the day has not.
	*now = now.Add(time.Hour)
	if w := httptest.NewRecorder(); func() int { h.ServeHTTP(w, tokenReq("198.51.100.1:1", "sess")); return w.Code }() != http.StatusTooManyRequests {
		t.Fatal("the day quota must hold across hours")
	}
	*now = midnight
	if w := httptest.NewRecorder(); func() int { h.ServeHTTP(w, tokenReq("198.51.100.1:1", "sess")); return w.Code }() != http.StatusOK {
		t.Fatal("the day quota resets at 00:00 UTC")
	}
}

// An administrator's override raises (or lowers) one account's values.
func TestSessionQuotaFollowsTheAccountOverride(t *testing.T) {
	twenty := 20
	cs := &fakeCapacityStore{
		quotas:    map[string]capacity.EffectiveQuota{capacity.QuotaRequestsPerHour: quotaOf(capacity.QuotaRequestsPerHour, 10, capacity.Enforce)},
		overrides: map[int]db.CapacityOverride{7: {UserID: 7, RequestsPerHour: &twenty}},
	}
	h, _, _, _ := sessionQuotaChain(t, cs)
	for i := 0; i < 20; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq("198.51.100.1:1", "sess"))
		if w.Code != http.StatusOK {
			t.Fatalf("request %d under the override of 20 = %d", i+1, w.Code)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq("198.51.100.1:1", "sess"))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("request 21 = %d, want 429 at the override", w.Code)
	}
}

// Only valid sessions are counted against the session quotas: an API token
// has its own, anonymous and invalid-token callers the per-IP limit.
func TestSessionQuotasCountOnlySessions(t *testing.T) {
	h, rl, _, _ := sessionQuotaChain(t, &fakeCapacityStore{})
	for i := 0; i < 5; i++ {
		h.ServeHTTP(httptest.NewRecorder(), tokenReq("198.51.100.9:1", db.APITokenPrefix+"tok"))
		h.ServeHTTP(httptest.NewRecorder(), tokenReq("127.0.0.1:1", ""))
		h.ServeHTTP(httptest.NewRecorder(), tokenReq("198.51.100.9:1", "junk"))
	}
	if got := sessionUsed(rl, capacity.QuotaRequestsPerHour, capacity.Hour); got != 0 {
		t.Fatalf("API-token, anonymous and invalid-token requests were counted as a session: %d", got)
	}
}

// An unreadable policy is not a number (SR-5): every quota takes its shipped
// value, the session quotas' shipped mode is shadow, and nothing is refused.
func TestUnreadablePolicyRefusesNoSession(t *testing.T) {
	h, _, logs, _ := sessionQuotaChain(t, &fakeCapacityStore{err: errors.New("store down")})
	for i := 0; i < capacity.Shipped[capacity.QuotaRequestsPerHour].Allowed+5; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq("198.51.100.1:1", "sess"))
		if w.Code != http.StatusOK {
			t.Fatalf("with no readable policy request %d was refused: %d", i+1, w.Code)
		}
	}
	if !strings.Contains(logs.String(), "could not read the capacity quotas") {
		t.Fatalf("a failed read must be logged:\n%s", logs.String())
	}
}

// An administrator's session is counted but never refused (the cache warm's
// authenticated mode uses one; an administrator is unscoped anyway).
func TestAdministratorSessionIsNeverRefused(t *testing.T) {
	store := &fakeSessionStore{userID: 1, admin: true, valid: map[string]bool{"adminsess": true}}
	h, rl := tokenChain(t, store, Options{RateLimitRPS: 1000, RateLimitBurst: 1000, RateLimitDaily: 1_000_000})
	rl.policy = &capacityPolicy{store: &fakeCapacityStore{quotas: map[string]capacity.EffectiveQuota{
		capacity.QuotaRequestsPerHour: quotaOf(capacity.QuotaRequestsPerHour, 5, capacity.Enforce)}}, now: time.Now, spawn: func(f func()) { f() }}
	for i := 0; i < 20; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq("198.51.100.5:1", "adminsess"))
		if w.Code != http.StatusOK {
			t.Fatalf("an administrator's session was refused at request %d: %d", i+1, w.Code)
		}
	}
}

// L10 round 1 on 0.29.85: the request nginx forwards after an admitted authz
// subrequest (rl.uncounted) already paid; it is neither counted nor
// charged, and an API token still sees its current numbers.
func TestForwardedRequestIsNeitherObservedNorCharged(t *testing.T) {
	h, rl, _, _ := sessionQuotaChain(t, &fakeCapacityStore{})
	rl.uncounted = func(r *http.Request) bool { return r.Header.Get("X-Test-Forwarded") == "1" }
	fwd := func(token string) *http.Request {
		r := tokenReq("198.51.100.80:1", token)
		r.Header.Set("X-Test-Forwarded", "1")
		return r
	}
	h.ServeHTTP(httptest.NewRecorder(), tokenReq("198.51.100.80:1", "sess"))
	for i := 0; i < 3; i++ {
		h.ServeHTTP(httptest.NewRecorder(), fwd("sess"))
	}
	if got := sessionUsed(rl, capacity.QuotaRequestsPerHour, capacity.Hour); got != 1 {
		t.Fatalf("session counted %d requests, want 1 (forwarded requests already paid)", got)
	}
	tok := db.APITokenPrefix + "tok"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq("198.51.100.80:1", tok))
	first := w.Header().Get("X-RateLimit-Remaining")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, fwd(tok))
	if w.Code != http.StatusOK || w.Header().Get("X-RateLimit-Remaining") != first || w.Header().Get("X-RateLimit-Limit") == "" {
		t.Fatalf("forwarded API-token request: code %d remaining %q (after the charged one: %q); want 200, same remaining, headers present",
			w.Code, w.Header().Get("X-RateLimit-Remaining"), first)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq("198.51.100.80:1", tok))
	if w.Header().Get("X-RateLimit-Remaining") == first {
		t.Fatal("a direct request after the forwarded one must still be charged")
	}
}

// The policy is re-read once per authCacheTTL; a failed read is logged and
// keeps the last snapshot; with nothing read it reports unknown.
func TestCapacityPolicyRereadsOncePerTTL(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	cs := &fakeCapacityStore{quotas: map[string]capacity.EffectiveQuota{
		capacity.QuotaRequestsPerHour: quotaOf(capacity.QuotaRequestsPerHour, 5000, capacity.Shadow)}}
	logs := &lockedBuffer{}
	p := &capacityPolicy{store: cs, now: func() time.Time { return now }, logger: slog.New(slog.NewTextHandler(logs, nil)), spawn: func(f func()) { f() }}
	hourOf := func() int { return p.sessionQuotas(7, false)[0].Allowed }
	if got := hourOf(); got != 5000 || cs.reads != 1 {
		t.Fatalf("first read = %d after %d reads", got, cs.reads)
	}
	cs.quotas[capacity.QuotaRequestsPerHour] = quotaOf(capacity.QuotaRequestsPerHour, 7, capacity.Shadow)
	if got := hourOf(); got != 5000 || cs.reads != 1 {
		t.Fatalf("within the TTL: %d after %d reads, want the cached 5000 after 1", got, cs.reads)
	}
	now = now.Add(authCacheTTL)
	if got := hourOf(); got != 7 {
		t.Fatalf("after the TTL the edited quota must apply: got %d", got)
	}
	cs.err = errors.New("store down")
	now = now.Add(authCacheTTL)
	if got := hourOf(); got != 7 {
		t.Fatalf("a failed read must keep the last value: %d", got)
	}
	if !strings.Contains(logs.String(), "could not read the capacity quotas") {
		t.Fatalf("a failed read must be logged:\n%s", logs.String())
	}
	fresh := &capacityPolicy{store: &fakeCapacityStore{err: errors.New("down")}, now: time.Now, spawn: func(f func()) { f() }}
	if _, ok := fresh.get(); ok {
		t.Fatal("with nothing ever read the policy is unknown")
	}
	if got := fresh.sessionQuotas(7, false)[0]; got.Allowed != capacity.Shipped[capacity.QuotaRequestsPerHour].Allowed || got.Mode != capacity.Shadow {
		t.Fatalf("unknown policy: %+v; want the shipped shadow value", got)
	}
}

// The read never runs on the request path: while it is in flight, get
// answers the last snapshot at once and starts no second read.
func TestCapacityPolicyNeverWaitsOnTheStore(t *testing.T) {
	cs := &fakeCapacityStore{block: make(chan struct{}), started: make(chan struct{}, 4)}
	p := &capacityPolicy{store: cs, now: time.Now}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 3; i++ {
			if _, ok := p.get(); ok {
				t.Error("nothing can be known before the first read returns")
			}
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("get waited on a store read in flight")
	}
	<-cs.started
	if len(cs.started) != 0 {
		t.Fatal("a second read started while one was in flight")
	}
	close(cs.block)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := p.get(); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the snapshot read never reached get")
		}
		time.Sleep(time.Millisecond)
	}
}

// The meter is bounded like every limiter map.
func TestQuotaMeterIsBounded(t *testing.T) {
	rl := &rateLimiter{now: time.Now}
	for u := 0; u < maxTrackedIPs+20; u++ {
		rl.chargeQuotas(authInfo{UserID: u + 1})
	}
	if got := rl.quotaMeter().Len(); got > maxTrackedIPs {
		t.Fatalf("meter holds %d windows, bound is %d", got, maxTrackedIPs)
	}
}
