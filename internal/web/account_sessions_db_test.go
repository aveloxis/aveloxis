// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
)

// 0.29.113 (OWASP ASVS 5.0, summary/62) through the Handler against the
// store: the account page lists sessions and ends them all (V7.5.2); an
// administrator ends another account's from the Users page (V7.4.5); a
// session idle past the bound is signed out (V7.3.1, the config knob end
// to end); the address change asks for a recent sign-in (V7.5.1); a sign-in
// replaces the session the browser carried (V7.2.4).
func TestAccountSessionsAreListedEndedAndBounded(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	tag := fmt.Sprintf("_avsess%d", time.Now().UnixNano())
	adminID := seedAdminAccount(t, store, tag+"-admin")
	userID, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: tag + "-user", Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, userID)
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
	})
	store.SetSessionIdle(10 * time.Minute)
	t.Cleanup(func() { store.SetSessionIdle(0) })
	s := New(store, config.WebConfig{DevMode: true, SessionIdleMinutes: 10}, "", logger)
	h := s.Handler()
	get := func(path, raw string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: raw})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	post := func(path, raw string, form url.Values) *httptest.ResponseRecorder {
		form.Set("csrf_token", "test-csrf-token")
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: raw})
		csrf(r)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	// Two real browser sessions of the user and a token minted from one.
	raw1, err := store.CreateWebSession(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	raw2, err := store.CreateWebSession(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MintTokenForWebSession(ctx, raw2, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	// The account page lists them, this browser's marked.
	w := get("/account", raw1)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "This browser") || !strings.Contains(w.Body.String(), "Another browser") || !strings.Contains(w.Body.String(), "A token minted for a browser session") {
		t.Fatalf("GET /account = %d\n%s", w.Code, w.Body.String())
	}
	if cc := w.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("the account page's Cache-Control = %q; want no-store", cc)
	}
	// The address change asks for a recent sign-in: a session signed in
	// an hour ago (and used since, so it is not idle) is sent to sign in
	// again; the fresh one goes through.
	if w := get("/account", raw2); w.Code != http.StatusOK { // the touch stamps last_used_at
		t.Fatalf("GET /account for the second session = %d", w.Code)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.user_session_tokens SET created_at = $2 WHERE token = $1`, db.HashToken(raw2), time.Now().Add(-time.Hour).Unix()); err != nil {
		t.Fatal(err)
	}
	if w := post("/account/email", raw2, url.Values{"email": {"x@example.org"}}); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "sign in again") || !strings.Contains(w.Body.String(), `href="/login?next=%2Faccount%2Femail"`) {
		t.Errorf("an old session's address change = %d; want the form asking to sign in again\n%s", w.Code, w.Body.String())
	}
	if w := post("/account/email", raw1, url.Values{"email": {"x@example.org"}}); strings.Contains(w.Body.String(), "sign in again") {
		t.Errorf("a fresh session's address change was asked to sign in again")
	}
	// Sign out everywhere needs a recent sign-in too (ASVS V7.5.2, the
	// 0.29.125 review): the hour-old session is asked to sign in again and
	// ends nothing.
	if w := post("/account/sessions/revoke", raw2, url.Values{}); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "sign in again") || !strings.Contains(w.Body.String(), `href="/login?next=%2Faccount"`) {
		t.Errorf("an old session's sign-out-everywhere = %d; want the account page asking to sign in again\n%s", w.Code, w.Body.String())
	}
	if _, err := store.LookupWebSession(ctx, raw1); err != nil {
		t.Fatalf("an old session's refused sign-out-everywhere must end nothing: %v", err)
	}
	// Sign out everywhere: every session of the account ends, this one too.
	if w := post("/account/sessions/revoke", raw1, url.Values{}); w.Code != http.StatusFound || w.Header().Get("Location") != "/login" {
		t.Fatalf("POST /account/sessions/revoke = %d → %q; want 302 to /login", w.Code, w.Header().Get("Location"))
	}
	for _, raw := range []string{raw1, raw2} {
		if _, err := store.LookupWebSession(ctx, raw); !errors.Is(err, db.ErrNoWebSession) {
			t.Errorf("after signing out everywhere a session still resolves: %v", err)
		}
	}
	if w := get("/dashboard", raw1); w.Code != http.StatusFound {
		t.Errorf("after signing out everywhere the dashboard = %d; want the login redirect", w.Code)
	}
	if !strings.Contains(logs.String(), "admin: end sessions") {
		t.Errorf("the revocation is not audited:\n%s", logs.String())
	}
	// An administrator ends another account's sessions from the Users page.
	raw3, err := store.CreateWebSession(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	adminRaw, err := store.CreateWebSession(ctx, adminID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.DeleteWebSession(context.Background(), adminRaw) })
	confirm := fmt.Sprintf("/admin/users/%d/sessions/revoke", userID)
	if w := get(confirm, adminRaw); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `action="`+confirm+`"`) {
		t.Errorf("the confirm page = %d; want 200 with the form", w.Code)
	}
	if w := get(confirm, raw3); w.Code != http.StatusForbidden {
		t.Errorf("a non-admin on the admin confirm = %d; want 403", w.Code)
	}
	if w := post(confirm, adminRaw, url.Values{}); w.Code != http.StatusFound || w.Header().Get("Location") != "/admin/users?ok=sessions_ended" {
		t.Errorf("the admin's revoke = %d → %q", w.Code, w.Header().Get("Location"))
	}
	if _, err := store.LookupWebSession(ctx, raw3); !errors.Is(err, db.ErrNoWebSession) {
		t.Errorf("the account's session survives the administrator's revoke: %v", err)
	}
	if w := get("/admin/users?ok=sessions_ended", adminRaw); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "sessions are ended") || !strings.Contains(w.Body.String(), "End sessions") {
		t.Errorf("the Users page after a revoke = %d; want the notice and the button\n%s", w.Code, w.Body.String())
	}
	// The inactivity bound, end to end: the knob's value is the store's,
	// and a session last used past it is signed out at the gate.
	raw4, err := store.CreateWebSession(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	if w := get("/dashboard", raw4); w.Code == http.StatusFound && strings.Contains(w.Header().Get("Location"), "/login") {
		t.Fatalf("a fresh session is refused: %d → %q", w.Code, w.Header().Get("Location"))
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.user_session_tokens SET last_used_at = $2 WHERE token = $1`, db.HashToken(raw4), time.Now().Add(-11*time.Minute).Unix()); err != nil {
		t.Fatal(err)
	}
	if w := get("/dashboard", raw4); w.Code != http.StatusFound || w.Header().Get("Location") != "/login" {
		t.Errorf("an idle session = %d → %q; want the login redirect (web.session_idle_minutes end to end)", w.Code, w.Header().Get("Location"))
	}
	// A sign-in replaces the session the browser carried.
	raw5, err := store.CreateWebSession(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, "/auth/github/callback", nil)
	r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: raw5})
	w = httptest.NewRecorder()
	s.completeOAuthLogin(w, r, db.OAuthUserInfo{Login: tag + "-user", Provider: "github"}, "GitHub")
	if w.Code != http.StatusFound {
		t.Fatalf("sign-in = %d\n%s", w.Code, w.Body.String())
	}
	if _, err := store.LookupWebSession(ctx, raw5); !errors.Is(err, db.ErrNoWebSession) {
		t.Errorf("the browser's earlier session survives a new sign-in: %v", err)
	}
	if !strings.Contains(logs.String(), `msg=sign-in`) || !strings.Contains(logs.String(), fmt.Sprintf("user_id=%d provider=github", userID)) {
		t.Errorf("the sign-in is not logged with the account and the forge:\n%s", logs.String())
	}
	_ = io.Discard
}
