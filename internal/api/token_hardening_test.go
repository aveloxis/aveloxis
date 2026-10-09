// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.29.82, the OWASP ASVS 5.0 review (summary/51) — operator 2026-10-08:
// fix A1 (an API token is not an admin credential), A2 (the Shared-with-Me
// auto-add's cost for unlimited sessions), A5 (refusals logged), A6
// (revocation and expiry immediate in every api process).

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// A1 (ASVS V8.2.1): an API token owned by an admin authenticates as that
// account for data, but it never administers — a leaked token cannot grant
// itself replacements or change roles.
func TestAPITokenCannotAdminister(t *testing.T) {
	s := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	r := httptest.NewRequest(http.MethodGet, "/api/v1/admin/api-tokens", nil)
	r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: 1, IsAdmin: true, APITokenID: 9, RateLimitPerHour: 10}))
	w := httptest.NewRecorder()
	if _, ok := s.requireAdmin(w, r); ok || w.Code != http.StatusForbidden {
		t.Fatalf("an admin-owned API token passed requireAdmin (code %d); an API token never administers", w.Code)
	}
	if !strings.Contains(w.Body.String(), "API token") {
		t.Errorf("the refusal must say an API token cannot administer: %s", w.Body.String())
	}
	// A session of the same admin still administers.
	r = httptest.NewRequest(http.MethodGet, "/api/v1/admin/api-tokens", nil)
	r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: 1, IsAdmin: true}))
	if _, ok := s.requireAdmin(httptest.NewRecorder(), r); !ok {
		t.Fatal("an admin session must still administer")
	}
}

// A6 (ASVS V7.4.1): a cached API token is rechecked on every call, so a
// token revoked (or expired) through another api process stops at once,
// without that process's cache bust.
func TestRevokedAPITokenStopsAtOnceWithoutACacheBust(t *testing.T) {
	tok := db.APITokenPrefix + "live"
	store := &fakeSessionStore{userID: 7, apiValid: map[string]db.APITokenIdentity{tok: {TokenID: 3, UserID: 7, RateLimitPerHour: 100}}}
	rl, err := newRateLimiter(tightLimits)
	if err != nil {
		t.Fatal(err)
	}
	// The route needs an identity, as requireUser does: without auth on,
	// an invalid token is served as anonymous, so "stopped" means "carries
	// no identity".
	needsUser := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.Context().Value(authCtxKey{}).(authInfo); !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	h := requestChain(rl, newAuthenticator(store, false, nil), needsUser)
	call := func() int {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/me", nil)
		r.RemoteAddr = "198.51.100.40:1"
		r.Header.Set("Authorization", "Bearer "+tok)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	if c := call(); c != http.StatusOK {
		t.Fatalf("first call = %d", c)
	}
	delete(store.apiValid, tok) // revoked by another process: this one's cache was never busted
	if c := call(); c != http.StatusUnauthorized {
		t.Fatalf("a revoked token's next call = %d; it must stop at once (no identity)", c)
	}
	// A session is not rechecked per call (its cache stands for 60 s).
	sess := &fakeSessionStore{userID: 7, valid: map[string]bool{"sess": true}}
	h2, _ := tokenChain(t, sess, tightLimits)
	for i := 0; i < 3; i++ {
		h2.ServeHTTP(httptest.NewRecorder(), tokenReq("198.51.100.41:1", "sess"))
	}
	if n := sess.validates.Load(); n != 1 {
		t.Fatalf("a cached session was looked up %d times in three calls, want 1", n)
	}
}

// A2 (ASVS V2.4.1): an unlimited session cannot turn the Shared-with-Me
// auto-add into unbounded writes and a fleet-wide cache flush.

type countingSharedWithMe struct{ calls int }

func (c *countingSharedWithMe) EnsureRepoSharedWithUser(context.Context, int, int64) (bool, error) {
	c.calls++
	return true, nil
}

func TestSharedWithMeAutoAddsAreCappedPerUser(t *testing.T) {
	shared := &countingSharedWithMe{}
	s := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), sharedWithMe: shared,
		auth: newAuthenticator(&fakeSessionStore{}, false, nil)}
	s.autoAdds = newAutoAddLimiter()
	info := authInfo{UserID: 42, Scope: map[int64]bool{}}
	var last *httptest.ResponseRecorder
	for i := 0; i <= sharedWithMeAddsPerHour; i++ {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/repos/"+strconv.Itoa(1000+i)+"/stats", nil)
		r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, info))
		last = httptest.NewRecorder()
		ok := s.authorizeRepo(last, r, int64(1000+i))
		if i < sharedWithMeAddsPerHour && !ok {
			t.Fatalf("auto-add %d of %d was refused", i+1, sharedWithMeAddsPerHour)
		}
	}
	if last.Code != http.StatusTooManyRequests || last.Header().Get("Retry-After") == "" {
		t.Fatalf("the auto-add past %d an hour = %d (Retry-After %q), want 429 with Retry-After", sharedWithMeAddsPerHour, last.Code, last.Header().Get("Retry-After"))
	}
	if shared.calls != sharedWithMeAddsPerHour {
		t.Fatalf("the refused auto-add still wrote (%d store calls, want %d)", shared.calls, sharedWithMeAddsPerHour)
	}
	// Another user has their own allowance.
	r := httptest.NewRequest(http.MethodGet, "/api/v1/repos/1/stats", nil)
	r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: 43, Scope: map[int64]bool{}}))
	if !s.authorizeRepo(httptest.NewRecorder(), r, 1) {
		t.Fatal("another user's first auto-add was refused")
	}
}

// An auto-add drops only the adding user's cached validations, not every
// user's (the review: one session's id walk re-ran three queries for every
// signed-in user's next request).
func TestAutoAddInvalidatesOnlyThatUser(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"a": true}}
	a := newAuthenticator(store, false, nil)
	if _, err := a.resolveToken(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	a.invalidateUser(99) // someone else
	if _, ok := a.cached("a"); !ok {
		t.Fatal("invalidating user 99 dropped user 7's cached validation")
	}
	a.invalidateUser(7)
	if _, ok := a.cached("a"); ok {
		t.Fatal("invalidating user 7 must drop user 7's cached validation")
	}
	body := mustReadFile(t, "auth.go")
	fn := body[strings.Index(body, "func (s *Server) authorizeRepo("):]
	fn = fn[:strings.Index(fn, "\n}\n")]
	if strings.Contains(fn, "invalidateAll()") || !strings.Contains(fn, "s.auth.invalidateUser(info.UserID)") {
		t.Fatal("authorizeRepo's auto-add must invalidate only the caller (invalidateUser), never every user")
	}
}

// A5 (ASVS V16.3.1, V16.3.3): a refused token and a token over its
// allowance are logged — once per address or token per window, never the
// token itself.
func TestTokenRefusalsAreLogged(t *testing.T) {
	logs := &lockedBuffer{}
	tok := db.APITokenPrefix + "small"
	store := &fakeSessionStore{userID: 7, apiValid: map[string]db.APITokenIdentity{tok: {TokenID: 5, UserID: 7, RateLimitPerHour: 1}}}
	h, rl := tokenChain(t, store, Options{RateLimitRPS: 1000, RateLimitBurst: 1000, RateLimitDaily: 100000})
	rl.logger = slog.New(slog.NewTextHandler(logs, nil))
	for i := 0; i < 3; i++ {
		h.ServeHTTP(httptest.NewRecorder(), tokenReq("203.0.113.50:1", db.APITokenPrefix+"forged"+strconv.Itoa(i)))
	}
	for i := 0; i < 3; i++ {
		h.ServeHTTP(httptest.NewRecorder(), tokenReq("203.0.113.51:1", tok))
	}
	out := logs.String()
	if n := strings.Count(out, "invalid token presented"); n != 1 {
		t.Errorf("three invalid tokens from one address logged %d lines, want 1 (once per window):\n%s", n, out)
	}
	if !strings.Contains(out, "address=203.0.113.50") || !strings.Contains(out, "kind=api") {
		t.Errorf("the invalid-token line names the address and the token kind:\n%s", out)
	}
	if n := strings.Count(out, "API token over its hourly allowance"); n != 1 {
		t.Errorf("two refused calls of one token logged %d allowance lines, want 1 (once per window):\n%s", n, out)
	}
	if !strings.Contains(out, "token_id=5") || !strings.Contains(out, "user_id=7") {
		t.Errorf("the allowance line names the token id and owner:\n%s", out)
	}
	if strings.Contains(out, "forged") || strings.Contains(out, "small") {
		t.Errorf("a log line carries a token:\n%s", out)
	}
}

// L10 on the ASVS fixes (MEDIUM): requireAdmin was not the only admin
// privilege. The group-add path decides approval bypass from the account's
// admin flag in the STORE, so an admin-owned API token could enqueue
// repositories and register orgs with no approval. A request carrying an
// API token runs with db.WithoutAdminPrivilege: every store-level admin
// decision treats it as a non-admin; its data scope stays its owner's.
func TestAPITokenRequestsCarryNoAdminPrivilege(t *testing.T) {
	tok := db.APITokenPrefix + "adm"
	store := &fakeSessionStore{userID: 1, admin: true, valid: map[string]bool{"sess": true},
		apiValid: map[string]db.APITokenIdentity{tok: {TokenID: 2, UserID: 1, RateLimitPerHour: 100}}}
	rl, err := newRateLimiter(tightLimits)
	if err != nil {
		t.Fatal(err)
	}
	var dropped, attached bool
	probe := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dropped = db.AdminPrivilegeDropped(r.Context())
		_, attached = r.Context().Value(authCtxKey{}).(authInfo)
	})
	h := requestChain(rl, newAuthenticator(store, false, nil), probe)
	h.ServeHTTP(httptest.NewRecorder(), tokenReq("198.51.100.60:1", tok))
	if !attached || !dropped {
		t.Fatalf("an API-token request: identity attached=%v, admin privilege dropped=%v; want both", attached, dropped)
	}
	h.ServeHTTP(httptest.NewRecorder(), tokenReq("198.51.100.61:1", "sess"))
	if !attached || dropped {
		t.Fatalf("a session request keeps its admin privilege: attached=%v dropped=%v", attached, dropped)
	}
}

// L10 on the ASVS fixes (LOW): the cap reserves before the write, so
// concurrent auto-adds cannot overshoot it.
func TestAutoAddCapHoldsUnderConcurrency(t *testing.T) {
	l := newAutoAddLimiter()
	var wg sync.WaitGroup
	var mu sync.Mutex
	granted := 0
	for i := 0; i < 3*sharedWithMeAddsPerHour; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok, _ := l.reserve(7); ok {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if granted != sharedWithMeAddsPerHour {
		t.Fatalf("%d concurrent auto-adds were granted, want exactly %d", granted, sharedWithMeAddsPerHour)
	}
	l.refund(autoAddSlot{userID: 7, start: l.windows[7].start}) // nothing was added: the slot comes back
	if _, ok, _ := l.reserve(7); !ok {
		t.Fatal("a refunded slot must be reservable again")
	}
}

// L10 on the ASVS fixes (LOW): a lookup in flight when the token is signed
// out must not re-cache it.
func TestForgetBeatsALookupInFlight(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"sess": true}}
	a := newAuthenticator(store, false, nil)
	store.onValidate = func() { a.forget("sess") } // sign-out lands mid-lookup
	if _, err := a.resolveToken(context.Background(), "sess"); err != nil {
		t.Fatal(err)
	}
	if _, ok := a.cached("sess"); ok {
		t.Fatal("a token signed out while its lookup was in flight was cached afterwards")
	}
}

// L10 rounds 1-2 on the ASVS fixes (MEDIUM): every IMPLICIT link of a
// repository into a user's group — a write the user did not ask for, made
// by viewing (Shared with Me, the compare's Comparisons group, the
// comparison record, a star of an out-of-scope repository) — is capped per
// user before it writes, or touches only repositories already in scope.
// Found by scanning every non-test file for the linking store calls, so a
// new site cannot be missed. EXPLICIT adds (a group add, an org add, a
// collection copy) are user actions with the approval flow and stay
// uncapped (bulk paste) — decided in the 0.29.82 ledger.
func TestEveryImplicitGroupLinkIsCapped(t *testing.T) {
	implicit := []string{"EnsureRepoSharedWithUser(", "AddRepoToGroupByID(", "RecordComparisonRepos("}
	ents, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	sites := 0
	for _, e := range ents {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src := srctest.StripGoComments(mustReadFile(t, name))
		for _, call := range implicit {
			for i := strings.Index(src, call); i >= 0; {
				lineStart := strings.LastIndex(src[:i], "\n") + 1
				lineEnd := i + strings.Index(src[i:], "\n")
				if strings.Contains(src[lineStart:lineEnd], "(ctx context.Context") {
					// the interface method's own declaration, not a call
				} else {
					sites++
					decl := src[strings.LastIndex(src[:i], "\nfunc ")+1:]
					decl = decl[:strings.Index(decl, "\n")]
					body := srctest.FuncBody(t, src, decl[:strings.Index(decl, ") ")+2+strings.Index(decl[strings.Index(decl, ") ")+2:], "(")+1])
					capped := strings.Contains(body, "s.autoAdds.reserve(info.UserID)")
					// The comparison record links only what is already in the
					// caller's scope (out-of-scope entities go through the
					// capped resolveEntityRepos).
					scopedRecord := strings.HasPrefix(decl, "func (s *Server) recordComparison(") && strings.Contains(body, "if e.Kind == \"repo\" && (info.IsAdmin || info.Scope[e.RepoID])")
					if !capped && !scopedRecord {
						t.Errorf("%s: %s calls %s without the per-user auto-add cap (or the comparison record's scope filter)", name, decl, call)
					}
				}
				next := strings.Index(src[i+1:], call)
				if next < 0 {
					break
				}
				i += 1 + next
			}
		}
	}
	if sites < 4 {
		t.Fatalf("found %d implicit linking sites; want at least the four known (Shared with Me, Comparisons, the comparison record, a star)", sites)
	}
}

// invalidateAll is for admin mutations (they change someone else's role or
// scope); a caller's own scope change invalidates only the caller. Every
// non-test file is scanned.
func TestGlobalCacheFlushIsAdminOnly(t *testing.T) {
	ents, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		f := e.Name()
		if !strings.HasSuffix(f, ".go") || strings.HasSuffix(f, "_test.go") {
			continue
		}
		src := srctest.StripGoComments(mustReadFile(t, f))
		for i := strings.Index(src, "invalidateAll()"); i >= 0; {
			fnStart := strings.LastIndex(src[:i], "\nfunc ")
			decl := src[fnStart+1 : fnStart+1+strings.Index(src[fnStart+1:], "\n")]
			if !strings.HasPrefix(decl, "func (s *Server) handleAdmin") && !strings.HasPrefix(decl, "func (a *authenticator) invalidateAll") {
				t.Errorf("%s: invalidateAll() in %q — only admin handlers flush every user's cache", f, decl)
			}
			next := strings.Index(src[i+1:], "invalidateAll()")
			if next < 0 {
				break
			}
			i += 1 + next
		}
	}
}

// Copilot review 5472987053 on PR #228 (HIGH): the per-user auto-add map
// and the per-token window map dropped only ENDED windows when full, so
// with every window still active the map grew past maxTrackedIPs without
// bound. Both now go through one bound that also evicts the window started
// earliest (the least allowance left to give back).
func TestWindowMapsStayBoundedWhenEveryWindowIsActive(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	l := newAutoAddLimiter()
	l.now = func() time.Time { return now }
	for u := 0; u < maxTrackedIPs+50; u++ {
		l.reserve(u)
		now = now.Add(time.Millisecond) // all inside one hour: every window is active
	}
	if got := len(l.windows); got > maxTrackedIPs {
		t.Fatalf("auto-add map holds %d windows, bound is %d", got, maxTrackedIPs)
	}
	if _, kept := l.windows[maxTrackedIPs+49]; !kept {
		t.Fatal("the newest user's window must be kept")
	}
	if _, kept := l.windows[0]; kept {
		t.Fatal("the earliest-started window should be the one evicted")
	}

	rl := &rateLimiter{now: func() time.Time { return now }}
	for id := int64(0); id < maxTrackedIPs+50; id++ {
		rl.allowToken(id, 1, 5000)
		now = now.Add(time.Millisecond)
	}
	if got := len(rl.tokenWindows); got > maxTrackedIPs {
		t.Fatalf("API-token map holds %d windows, bound is %d", got, maxTrackedIPs)
	}
}

// L10 round 1 on the 0.29.83 fixes (LOW): a refund looked up the user's
// CURRENT window, so a reservation made in one hour and refunded in the
// next took a slot off the new hour's real adds, and a refund for an
// evicted window created a window (and could evict another user's). A
// refund now gives back only to the window it was charged to.
func TestAutoAddRefundReturnsToTheWindowItCameFrom(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 59, 59, 0, time.UTC)
	l := newAutoAddLimiter()
	l.now = func() time.Time { return now }
	stale, ok, _ := l.reserve(7) // charged to the window that starts now
	if !ok {
		t.Fatal("first reservation refused")
	}
	now = now.Add(autoAddWindow + time.Second) // the window (started at the first reservation) has ended
	if _, ok, _ := l.reserve(7); !ok {
		t.Fatal("first reservation of the new window refused")
	}
	l.refund(stale)
	if got := l.windows[7].count; got != 1 {
		t.Fatalf("refunding last hour's slot changed this hour's count to %d, want 1", got)
	}
	l.refund(autoAddSlot{userID: 8, start: now}) // a user with no window
	if _, made := l.windows[8]; made {
		t.Fatal("a refund must never create a window")
	}
}

// L10 round 2 on the 0.29.83 fixes (LOW): the Shared-with-Me refund was
// unpinned — a zero slot passed to refund silently refunded nothing and the
// package stayed green. A view that adds nothing (already shared behind a
// stale scope) or fails spends no slot.
func TestSharedWithMeViewThatAddsNothingSpendsNoSlot(t *testing.T) {
	for _, tc := range []struct {
		name string
		fake *fakeSharedWithMe
	}{
		{"already shared", &fakeSharedWithMe{added: false}},
		{"store error", &fakeSharedWithMe{err: io.ErrUnexpectedEOF}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := autoAddServer(tc.fake)
			s.autoAdds = newAutoAddLimiter()
			info := authInfo{UserID: 42, Scope: map[int64]bool{}}
			for i := 0; i < 2; i++ {
				r := httptest.NewRequest(http.MethodGet, "/api/v1/repos/5/stats", nil)
				r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, info))
				s.authorizeRepo(httptest.NewRecorder(), r, 5)
			}
			if len(tc.fake.calls) != 2 {
				t.Fatalf("store called %d times, want 2", len(tc.fake.calls))
			}
			if got := s.autoAdds.windows[42].count; got != 0 {
				t.Fatalf("two views that added nothing spent %d slots, want 0", got)
			}
		})
	}
}
