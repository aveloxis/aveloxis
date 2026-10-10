// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.29.82 — the admin endpoints behind aveloxis-gui's API-token page:
// grant (the token is shown once), list (never the token), revoke (at once),
// and the editable defaults (5,000 calls per hour, 30 days).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/db"
)

func apiTokenAdminServer(t *testing.T) (*Server, *db.PostgresStore, int, int) {
	t.Helper()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	user := func(login string, admin bool) int {
		var id int
		if err := store.Pool().QueryRow(ctx, `
			INSERT INTO aveloxis_ops.users (login_name, oauth_provider, admin) VALUES ($1, 'github', $2)
			ON CONFLICT (login_name) DO UPDATE SET admin = EXCLUDED.admin RETURNING user_id`, login, admin).Scan(&id); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			c := context.Background()
			_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.api_tokens WHERE user_id = $1 OR created_by = $1 OR revoked_by = $1`, id)
			_, _ = store.Pool().Exec(c, `UPDATE aveloxis_ops.api_token_settings SET updated_by = NULL WHERE updated_by = $1`, id)
			_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, id)
		})
		return id
	}
	admin := user("avx-it-apitok-admin-api", true)
	owner := user("avx-it-apitok-owner-api", false)
	before, err := store.GetAPITokenSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = store.SetAPITokenSettings(context.Background(), before, 0)
	})
	s, err := NewWithOptions(store, logger, Options{RateLimitRPS: 1, RateLimitBurst: 10, RateLimitDaily: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return s, store, admin, owner
}

func asUser(r *http.Request, userID int, admin bool) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: userID, IsAdmin: admin}))
}

func adminCall(t *testing.T, s *Server, method, path string, body any, userID int, admin bool) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	r := httptest.NewRequest(method, path, rd)
	r.RemoteAddr = "127.0.0.1:1" // exempt: these calls exercise the handlers, not the limiter
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, asUser(r, userID, admin))
	return w
}

func TestAPITokenAdminEndpoints(t *testing.T) {
	s, store, admin, owner := apiTokenAdminServer(t)
	ctx := context.Background()
	if err := store.SetAPITokenSettings(ctx, db.APITokenSettings{DefaultRateLimitPerHour: 4321, DefaultRateLimitPerDay: 43210, DefaultLifetimeDays: 12}, admin); err != nil {
		t.Fatal(err)
	}

	// Non-admins are refused on every route.
	for _, c := range []struct{ method, path string }{
		{"GET", "/api/v1/admin/api-tokens"},
		{"POST", "/api/v1/admin/api-tokens"},
		{"POST", "/api/v1/admin/api-tokens/1/revoke"},
		{"GET", "/api/v1/admin/api-token-settings"},
		{"POST", "/api/v1/admin/api-token-settings"},
	} {
		if w := adminCall(t, s, c.method, c.path, map[string]any{}, owner, false); w.Code != http.StatusForbidden {
			t.Errorf("%s %s as a non-admin = %d, want 403", c.method, c.path, w.Code)
		}
	}

	// Grant with the defaults: the token comes back once.
	w := adminCall(t, s, "POST", "/api/v1/admin/api-tokens", map[string]any{"user_id": owner, "label": "a research script"}, admin, true)
	if w.Code != http.StatusCreated {
		t.Fatalf("grant = %d %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
		t.Error("the response carrying a new token must be no-store")
	}
	var granted struct {
		Token            string    `json:"token"`
		TokenID          int64     `json:"token_id"`
		ExpiresAt        time.Time `json:"expires_at"`
		RateLimitPerHour int       `json:"rate_limit_per_hour"`
		RateLimitPerDay  int       `json:"rate_limit_per_day"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &granted); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(granted.Token, db.APITokenPrefix) || granted.RateLimitPerHour != 4321 || granted.RateLimitPerDay != 43210 {
		t.Fatalf("granted = %+v (want the default allowances 4321/h and 43210/day)", granted)
	}
	if d := time.Until(granted.ExpiresAt); d < 11*24*time.Hour || d > 13*24*time.Hour {
		t.Fatalf("expires %v: want the default 12 days", granted.ExpiresAt)
	}

	// Grant with explicit values.
	w = adminCall(t, s, "POST", "/api/v1/admin/api-tokens", map[string]any{"user_id": owner, "label": "short", "lifetime_days": 2, "rate_limit_per_hour": 50, "rate_limit_per_day": 500}, admin, true)
	if w.Code != http.StatusCreated || !strings.Contains(w.Body.String(), `"rate_limit_per_hour":50`) || !strings.Contains(w.Body.String(), `"rate_limit_per_day":500`) {
		t.Fatalf("explicit grant = %d %s", w.Code, w.Body.String())
	}

	// Refused grants.
	for name, body := range map[string]map[string]any{
		"no owner":         {"label": "x"},
		"unknown owner":    {"user_id": 999999999, "label": "x"},
		"no label":         {"user_id": owner, "label": " "},
		"zero lifetime":    {"user_id": owner, "label": "x", "lifetime_days": 0},
		"negative limit":   {"user_id": owner, "label": "x", "rate_limit_per_hour": -5},
		"zero daily":       {"user_id": owner, "label": "x", "rate_limit_per_day": 0},
		"daily too big":    {"user_id": owner, "label": "x", "rate_limit_per_day": int64(1) << 40},
		"lifetime too big": {"user_id": owner, "label": "x", "lifetime_days": 1 << 40},
	} {
		if w := adminCall(t, s, "POST", "/api/v1/admin/api-tokens", body, admin, true); w.Code != http.StatusBadRequest {
			t.Errorf("%s: grant = %d %s, want 400", name, w.Code, w.Body.String())
		}
	}

	// The list never carries a token or its hash.
	w = adminCall(t, s, "GET", "/api/v1/admin/api-tokens", nil, admin, true)
	if w.Code != http.StatusOK {
		t.Fatalf("list = %d", w.Code)
	}
	if strings.Contains(w.Body.String(), granted.Token) || strings.Contains(w.Body.String(), granted.Token[len(db.APITokenPrefix):]) || strings.Contains(w.Body.String(), "token_hash") {
		t.Fatal("the list must never carry a token or its hash")
	}
	if !strings.Contains(w.Body.String(), `"owner_login":"avx-it-apitok-owner-api"`) || !strings.Contains(w.Body.String(), `"default_rate_limit_per_hour":4321`) {
		t.Fatalf("the list names the owner and the defaults: %s", w.Body.String())
	}

	// The token works through the real chain, from a public address, and
	// revocation takes effect on the next call.
	call := func() int {
		r := httptest.NewRequest("GET", "/api/v1/me", nil)
		r.RemoteAddr = "203.0.113.77:1"
		r.Header.Set("Authorization", "Bearer "+granted.Token)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w.Code
	}
	if code := call(); code != http.StatusOK {
		t.Fatalf("the granted token's first call = %d", code)
	}
	w = adminCall(t, s, "POST", "/api/v1/admin/api-tokens/"+strconv.FormatInt(granted.TokenID, 10)+"/revoke", nil, admin, true)
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d %s", w.Code, w.Body.String())
	}
	if code := call(); code != http.StatusUnauthorized {
		t.Fatalf("a revoked token's next call = %d, want 401 at once (not after the 60 s cache)", code)
	}
	if w := adminCall(t, s, "POST", "/api/v1/admin/api-tokens/999999999/revoke", nil, admin, true); w.Code != http.StatusNotFound {
		t.Errorf("revoking an unknown token = %d, want 404", w.Code)
	}
	if w := adminCall(t, s, "POST", "/api/v1/admin/api-tokens/abc/revoke", nil, admin, true); w.Code != http.StatusBadRequest {
		t.Errorf("revoking a non-numeric id = %d, want 400", w.Code)
	}

	// Settings.
	w = adminCall(t, s, "POST", "/api/v1/admin/api-token-settings", map[string]any{"default_rate_limit_per_hour": 100, "default_rate_limit_per_day": 900, "default_lifetime_days": 7}, admin, true)
	if w.Code != http.StatusOK {
		t.Fatalf("settings update = %d %s", w.Code, w.Body.String())
	}
	w = adminCall(t, s, "GET", "/api/v1/admin/api-token-settings", nil, admin, true)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"default_rate_limit_per_hour":100`) ||
		!strings.Contains(w.Body.String(), `"default_rate_limit_per_day":900`) || !strings.Contains(w.Body.String(), `"default_lifetime_days":7`) {
		t.Fatalf("settings read = %d %s", w.Code, w.Body.String())
	}
	for name, body := range map[string]map[string]any{
		"zero allowance": {"default_rate_limit_per_hour": 0, "default_lifetime_days": 7},
		"zero lifetime":  {"default_rate_limit_per_hour": 10, "default_lifetime_days": 0},
		"zero daily":     {"default_rate_limit_per_hour": 10, "default_rate_limit_per_day": 0, "default_lifetime_days": 7},
		"huge lifetime":  {"default_rate_limit_per_hour": 10, "default_lifetime_days": 1 << 40},
		"missing":        {},
	} {
		if w := adminCall(t, s, "POST", "/api/v1/admin/api-token-settings", body, admin, true); w.Code != http.StatusBadRequest {
			t.Errorf("%s: settings update = %d, want 400", name, w.Code)
		}
	}
	// A quota aveloxis.json sets cannot be changed from the page: 409 with
	// the reason, not a 500 (v0.29.89).
	store.SetCapacitySources(map[string]capacity.Source{capacity.QuotaTokenRequestsPerHour: capacity.SourceDefault})
	t.Cleanup(func() { store.SetCapacitySources(nil) })
	w = adminCall(t, s, "POST", "/api/v1/admin/api-token-settings", map[string]any{"default_rate_limit_per_hour": 55, "default_lifetime_days": 7}, admin, true)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "aveloxis.json") {
		t.Errorf("changing a config-owned quota = %d %s; want 409 naming aveloxis.json", w.Code, w.Body.String())
	}
}

// The ASVS review's sign-out gap (V7.4.1): the GUI's sign-out ended only the
// web process's cookie session, and nothing called DeleteSessionToken, so a
// copied Bearer token stayed valid for its 30 days. POST /api/v1/auth/logout
// ends the session token it is called with.
func TestLogoutEndsTheSessionToken(t *testing.T) {
	s, store, _, owner := apiTokenAdminServer(t)
	ctx := context.Background()
	tok, err := store.CreateSessionToken(ctx, owner, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	call := func(method, path, bearer string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, nil)
		r.RemoteAddr = "203.0.113.90:1"
		if bearer != "" {
			r.Header.Set("Authorization", "Bearer "+bearer)
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	if w := call("GET", "/api/v1/me", tok); w.Code != http.StatusOK {
		t.Fatalf("before sign-out /me = %d", w.Code)
	}
	w := call("POST", "/api/v1/auth/logout", tok)
	if w.Code != http.StatusNoContent || !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
		t.Fatalf("sign-out = %d (Cache-Control %q), want 204 no-store", w.Code, w.Header().Get("Cache-Control"))
	}
	if _, err := store.ValidateSessionToken(ctx, tok); !errors.Is(err, db.ErrInvalidSessionToken) {
		t.Fatalf("the token must be deleted server-side, got %v", err)
	}
	if w := call("GET", "/api/v1/me", tok); w.Code != http.StatusUnauthorized {
		t.Fatalf("after sign-out /me = %d, want 401 at once (not after the 60 s cache)", w.Code)
	}
	if w := call("POST", "/api/v1/auth/logout", ""); w.Code != http.StatusUnauthorized {
		t.Errorf("sign-out without a token = %d, want 401", w.Code)
	}
	raw, _, err := store.CreateAPIToken(ctx, db.APITokenGrant{UserID: owner, Label: "x", Lifetime: time.Hour, RateLimitPerHour: 10, RateLimitPerDay: 10})
	if err != nil {
		t.Fatal(err)
	}
	if w := call("POST", "/api/v1/auth/logout", raw); w.Code != http.StatusBadRequest {
		t.Errorf("sign-out with an API token = %d, want 400 (an administrator revokes API tokens)", w.Code)
	}
}

// L10 round 3 on the ASVS fixes (L11): starring a repository id that does
// not exist linked it into the Starred group (user_repos has no FK on
// repo_id), spent a slot of the auto-add cap, then failed the star with a
// 500. A missing repository is a 404 before any reservation or write.
func TestStarOfAMissingRepositoryIs404AndWritesNothing(t *testing.T) {
	s, store, _, owner := apiTokenAdminServer(t)
	ctx := context.Background()
	const missing = 987654321
	r := httptest.NewRequest(http.MethodPut, "/api/v1/repos/987654321/star", nil)
	r.SetPathValue("repoID", "987654321")
	r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: owner, Scope: map[int64]bool{}}))
	w := httptest.NewRecorder()
	s.handleStarRepo(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("starring a missing repository = %d %s, want 404", w.Code, w.Body.String())
	}
	var linked int
	if err := store.Pool().QueryRow(ctx, `
		SELECT count(*) FROM aveloxis_ops.user_repos ur JOIN aveloxis_ops.user_groups g ON g.group_id = ur.group_id
		WHERE g.user_id = $1 AND ur.repo_id = $2`, owner, missing).Scan(&linked); err != nil {
		t.Fatal(err)
	}
	if linked != 0 {
		t.Fatal("a missing repository must not be linked into any of the user's groups")
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = $1)`, owner)
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, owner)
	})
	if _, ok, _ := s.autoAdds.reserve(owner); !ok {
		t.Fatal("the refused star must not have spent a slot")
	}
}

// Copilot review 5472987053 on PR #228 (MEDIUM, two sites): an implicit link
// that inserts nothing — the repository was already linked, but the
// caller's cached scope was stale or a concurrent request linked it first —
// spent a slot of the auto-add cap, and the star reported it as added. The
// slot comes back and no "added" marker is sent when nothing was linked.
func TestImplicitLinkThatAddsNothingSpendsNoSlot(t *testing.T) {
	s, store, _, owner := apiTokenAdminServer(t)
	ctx := context.Background()
	var repoID int64
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ('https://github.com/_avnooplink/r', 'r', '_avnooplink', 1) RETURNING repo_id`).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = $1)`, owner)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.user_repo_stars WHERE user_id = $1`, owner)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, owner)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	stale := authInfo{UserID: owner, Scope: map[int64]bool{}} // never sees the link it made
	slotsUsed := func() int {
		return s.autoAdds.quotaMeter().Peek(capacity.Subject{Kind: capacity.KindAccount, ID: strconv.Itoa(owner)}, autoAddQuota).Used
	}

	star := func() map[string]any {
		r := httptest.NewRequest(http.MethodPut, "/api/v1/repos/"+strconv.FormatInt(repoID, 10)+"/star", nil)
		r.SetPathValue("repoID", strconv.FormatInt(repoID, 10))
		r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, stale))
		w := httptest.NewRecorder()
		s.handleStarRepo(w, r)
		if w.Code != http.StatusOK {
			t.Fatalf("star = %d %s", w.Code, w.Body.String())
		}
		var out map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &out)
		return out
	}
	if first := star(); first["added_to_group"] != db.StarredGroupName {
		t.Fatalf("the first star of an out-of-scope repository = %v, want added_to_group", first)
	}
	if second := star(); second["added_to_group"] != nil {
		t.Fatalf("a star that linked nothing reported added_to_group: %v", second)
	}
	if got := slotsUsed(); got != 1 {
		t.Fatalf("two stars, one link: %d slots spent, want 1", got)
	}

	resolve := func() string {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/compare", nil)
		r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, stale))
		ids, added, ok := s.resolveEntityRepos(httptest.NewRecorder(), r, entity{Kind: "repo", RepoID: repoID, Label: "r"})
		if !ok || len(ids) != 1 {
			t.Fatalf("resolveEntityRepos = %v, %v", ids, ok)
		}
		return added
	}
	if added := resolve(); added != db.ComparisonsGroupName {
		t.Fatalf("the first comparison of an out-of-scope repository added %q, want %q", added, db.ComparisonsGroupName)
	}
	if added := resolve(); added != "" {
		t.Fatalf("a comparison that linked nothing reported %q as added", added)
	}
	if got := slotsUsed(); got != 2 {
		t.Fatalf("after one star link and one comparison link: %d slots spent, want 2", got)
	}
}

// v0.29.89 end to end on the real store (SR-10: config value → behavior):
// the stored quota row reaches the server's policy, an enforced day quota
// refuses a real session past its value, and two api processes share one
// day count through the store (FlushCapacity → request_counts).
func TestCapacityQuotaRowRefusesASessionEndToEnd(t *testing.T) {
	s, store, admin, owner := apiTokenAdminServer(t)
	ctx := context.Background()
	before, err := store.GetCapacityQuotas(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, n := range []string{capacity.QuotaRequestsPerHour, capacity.QuotaRequestsPerDay} {
			if q, ok := before[n]; ok {
				_ = store.SetCapacityQuota(context.Background(), n, q.Allowed, q.Mode, 0)
			}
		}
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.request_counts WHERE subject = $1`, capacity.CountKey(capacity.Subject{Kind: capacity.KindAccount, ID: strconv.Itoa(owner)}, capacity.QuotaRequestsPerDay))
	})
	if err := store.SetCapacityQuota(ctx, capacity.QuotaRequestsPerHour, 4321, capacity.Shadow, admin); err != nil {
		t.Fatal(err)
	}
	if err := store.SetCapacityQuota(ctx, capacity.QuotaRequestsPerDay, 3, capacity.Enforce, admin); err != nil {
		t.Fatal(err)
	}
	// The read runs off the request path; it lands within moments.
	deadline := time.Now().Add(10 * time.Second)
	for {
		qs := s.limiter.policy.sessionQuotas(owner, false)
		if qs[0].Allowed == 4321 && qs[1].Allowed == 3 && qs[1].Mode == capacity.Enforce {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("policy = %+v; want the stored rows (4321 shadow, 3 enforce)", qs)
		}
		time.Sleep(5 * time.Millisecond)
	}
	tok, err := store.CreateSessionToken(ctx, owner, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	call := func(srv *Server) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/api/v1/me", nil)
		r.RemoteAddr = "203.0.113.78:1"
		r.Header.Set("Authorization", "Bearer "+tok)
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, r)
		return w
	}
	for i := 0; i < 2; i++ {
		if w := call(s); w.Code != http.StatusOK {
			t.Fatalf("session request %d of 3 = %d %s", i+1, w.Code, w.Body.String())
		}
	}
	// A second api process on the same database learns the shared day
	// count at its first save (the bound is one save period, authCacheTTL):
	// request 3 is its own, and after it saves, request 4 is refused.
	s.FlushCapacity(ctx)
	other, err := NewWithOptions(store, s.logger, Options{RateLimitRPS: 1, RateLimitBurst: 10, RateLimitDaily: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); other.limiter.policy.sessionQuotas(owner, false)[1].Mode != capacity.Enforce; {
		if time.Now().After(deadline) {
			t.Fatal("the second process never read the policy")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if w := call(other); w.Code != http.StatusOK {
		t.Fatalf("request 3 of 3 (second process) = %d %s", w.Code, w.Body.String())
	}
	other.FlushCapacity(ctx)
	if w := call(other); w.Code != http.StatusTooManyRequests || !strings.Contains(w.Body.String(), `"error":"capacity_limit"`) {
		t.Fatalf("request 4 of 3 = %d %s; want 429 capacity_limit (the day count is shared)", w.Code, w.Body.String())
	}
}

// v0.29.86 (operator, 2026-10-09), end to end on the real store: an API
// token asking the compare or the star for a repository outside its owner's
// groups is refused with the way to add it, and nothing is linked.
func TestAPITokenOutOfScopeCompareAndStarAreRefused(t *testing.T) {
	s, store, _, owner := apiTokenAdminServer(t)
	ctx := context.Background()
	var repoID int64
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ('https://github.com/_avtokscope/r', 'r', '_avtokscope', 1) RETURNING repo_id`).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = $1)`, owner)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, owner)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	token := authInfo{UserID: owner, APITokenID: 77, Scope: map[int64]bool{}}
	linked := func() int {
		var n int
		if err := store.Pool().QueryRow(ctx, `
			SELECT count(*) FROM aveloxis_ops.user_repos ur JOIN aveloxis_ops.user_groups g ON g.group_id = ur.group_id
			WHERE g.user_id = $1 AND ur.repo_id = $2`, owner, repoID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	r := httptest.NewRequest(http.MethodGet, "/api/v1/compare", nil)
	r = r.WithContext(withIdentity(r.Context(), token))
	w := httptest.NewRecorder()
	if _, _, ok := s.resolveEntityRepos(w, r, entity{Kind: "repo", RepoID: repoID, Label: "r"}); ok {
		t.Fatal("an API token resolved an out-of-scope repository in compare")
	}
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), `"entity_url":"https://github.com/_avtokscope/r"`) ||
		!strings.Contains(w.Body.String(), `"add_to_existing_group"`) {
		t.Fatalf("compare refusal = %d %s; want 403 with the URL and the add call", w.Code, w.Body.String())
	}

	r = httptest.NewRequest(http.MethodPut, "/api/v1/repos/"+strconv.FormatInt(repoID, 10)+"/star", nil)
	r.SetPathValue("repoID", strconv.FormatInt(repoID, 10))
	r = r.WithContext(withIdentity(r.Context(), token))
	w = httptest.NewRecorder()
	s.handleStarRepo(w, r)
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), `"repo_url":"https://github.com/_avtokscope/r"`) {
		t.Fatalf("star refusal = %d %s; want 403 with the URL", w.Code, w.Body.String())
	}
	if n := linked(); n != 0 {
		t.Fatalf("an API token's refused requests linked the repository into %d group(s)", n)
	}
}

// L10 round 2 on 0.29.86: /api/v1/repos/stats?ids= ignored scope, so any API
// token read any repository's stats there (the single-repository route
// refused it). An API token gets only its owner's groups' ids; the others
// are left out. A session is unchanged.
func TestBatchStatsAreScopedForAPITokens(t *testing.T) {
	s, store, _, owner := apiTokenAdminServer(t)
	ctx := context.Background()
	var in, out int64
	for _, row := range []struct {
		dst  *int64
		name string
	}{{&in, "in"}, {&out, "out"}} {
		if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
			VALUES ('https://github.com/_avbatchscope/'||$1::text, $1::text, '_avbatchscope', 1) RETURNING repo_id`, row.name).Scan(row.dst); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id IN ($1, $2)`, in, out)
	})
	call := func(info authInfo) map[string]any {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/repos/stats?ids="+strconv.FormatInt(in, 10)+","+strconv.FormatInt(out, 10), nil)
		r = r.WithContext(withIdentity(r.Context(), info))
		w := httptest.NewRecorder()
		s.handleRepoStatsBatch(w, r)
		if w.Code != http.StatusOK {
			t.Fatalf("batch stats = %d %s", w.Code, w.Body.String())
		}
		var m map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &m)
		return m
	}
	token := call(authInfo{UserID: owner, APITokenID: 9, Scope: map[int64]bool{in: true}})
	if _, ok := token[strconv.FormatInt(out, 10)]; ok {
		t.Fatalf("an API token read an out-of-scope repository's stats: %v", token)
	}
	if _, ok := token[strconv.FormatInt(in, 10)]; !ok {
		t.Fatalf("the in-scope repository is missing: %v", token)
	}
	// ASVS review G1b: a non-admin session is scoped too — the batch read
	// any repository without passing the auto-add cap (500 ids a call).
	session := call(authInfo{UserID: owner, Scope: map[int64]bool{in: true}})
	if _, ok := session[strconv.FormatInt(out, 10)]; ok {
		t.Fatalf("a session read an out-of-scope repository's batch stats: %v", session)
	}
	if _, ok := session[strconv.FormatInt(in, 10)]; !ok {
		t.Fatalf("the session's in-scope repository is missing: %v", session)
	}
	// An administrator's session stays unscoped.
	if admin := call(authInfo{UserID: owner, IsAdmin: true}); len(admin) != 2 {
		t.Fatalf("an administrator's session must read both: %v", admin)
	}
}

// Copilot review 5477687920 on PR #228 (HIGH): the compare cache was keyed
// by user id, so an administrator's session answer for an out-of-scope
// repository was served to that administrator's (scoped) API token, and a
// token's scoped answer to the session. A token and a session of one user
// never share an entry: both orders.
func TestCompareCacheSeparatesSessionAndAPIToken(t *testing.T) {
	s, store, admin, _ := apiTokenAdminServer(t)
	ctx := context.Background()
	var repoID int64
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ('https://github.com/_avcmpcache/r', 'r', '_avcmpcache', 1) RETURNING repo_id`).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	path := "/api/v1/compare?metric=contributors&entities=repo:" + strconv.FormatInt(repoID, 10)
	call := func(info authInfo) int {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r = r.WithContext(withIdentity(r.Context(), info))
		w := httptest.NewRecorder()
		s.handleCompare(w, r)
		if w.Code == http.StatusBadRequest {
			t.Logf("400 body: %s", w.Body.String())
		}
		return w.Code
	}
	session := authInfo{UserID: admin, IsAdmin: true}
	token := authInfo{UserID: admin, APITokenID: 88, Scope: map[int64]bool{}}
	if code := call(session); code != http.StatusOK {
		t.Fatalf("the admin session's compare = %d", code)
	}
	if code := call(token); code != http.StatusForbidden {
		t.Fatalf("after the admin session cached it, the admin's API token got %d; want 403 (out of its scope)", code)
	}
	// The other order: a fresh server, the token first.
	s2, err := NewWithOptions(store, s.logger, Options{RateLimitRPS: 1, RateLimitBurst: 10, RateLimitDaily: 1000})
	if err != nil {
		t.Fatal(err)
	}
	s = s2
	if code := call(token); code != http.StatusForbidden {
		t.Fatalf("token first: %d, want 403", code)
	}
	if code := call(session); code != http.StatusOK {
		t.Fatalf("after the token's refusal, the admin session got %d; want 200", code)
	}
}

// ASVS review I4, end to end: a freshly started api process learns the
// shared day count within moments of its first request (RunCapacity saves
// when the meter opens a day window), not after a save period — so a
// restart does not serve a spent day for up to a minute.
func TestRestartedProcessLearnsTheDayCountPromptly(t *testing.T) {
	s, store, admin, owner := apiTokenAdminServer(t)
	ctx := context.Background()
	before, err := store.GetCapacityQuotas(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := capacity.CountKey(capacity.Subject{Kind: capacity.KindAccount, ID: strconv.Itoa(owner)}, capacity.QuotaRequestsPerDay)
	t.Cleanup(func() {
		if q, ok := before[capacity.QuotaRequestsPerDay]; ok {
			_ = store.SetCapacityQuota(context.Background(), capacity.QuotaRequestsPerDay, q.Allowed, q.Mode, 0)
		}
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.request_counts WHERE subject = $1`, key)
	})
	if err := store.SetCapacityQuota(ctx, capacity.QuotaRequestsPerDay, 3, capacity.Enforce, admin); err != nil {
		t.Fatal(err)
	}
	// Another process already counted 2 today.
	if _, err := store.AddRequestCounts(ctx, time.Now(), map[string]int64{key: 2}); err != nil {
		t.Fatal(err)
	}
	_ = s // the shared fixture's server is not used: this one is fresh
	fresh, err := NewWithOptions(store, s.logger, Options{RateLimitRPS: 1, RateLimitBurst: 10, RateLimitDaily: 1000})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { fresh.RunCapacity(runCtx); close(done) }()
	t.Cleanup(func() { stop(); <-done })
	for deadline := time.Now().Add(10 * time.Second); fresh.limiter.policy.sessionQuotas(owner, false)[1].Mode != capacity.Enforce; {
		if time.Now().After(deadline) {
			t.Fatal("the fresh process never read the policy")
		}
		time.Sleep(5 * time.Millisecond)
	}
	tok, err := store.CreateSessionToken(ctx, owner, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	call := func() int {
		r := httptest.NewRequest("GET", "/api/v1/me", nil)
		r.RemoteAddr = "203.0.113.79:1"
		r.Header.Set("Authorization", "Bearer "+tok)
		w := httptest.NewRecorder()
		fresh.Handler().ServeHTTP(w, r)
		return w.Code
	}
	if code := call(); code != http.StatusOK {
		t.Fatalf("request 3 of 3 = %d", code)
	}
	for deadline := time.Now().Add(5 * time.Second); ; {
		if fresh.limiter.quotaMeter().Peek(capacity.Subject{Kind: capacity.KindAccount, ID: strconv.Itoa(owner)},
			capacity.RateQuota{Name: capacity.QuotaRequestsPerDay, Window: capacity.UTCDay, Allowed: 3}).Used >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the fresh process did not learn the shared day count within moments")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if code := call(); code != http.StatusTooManyRequests {
		t.Fatalf("request 4 of 3 = %d; want 429 (the shared day is spent)", code)
	}
}
