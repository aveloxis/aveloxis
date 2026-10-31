// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// 0.29.113 (OWASP ASVS 5.0, summary/62) through the api process against the
// store: an account lists its sessions with the one presenting the request
// marked current and ends them all (V7.5.2); an administrator ends another
// account's (V7.4.5), a non-administrator is refused; the address change
// asks for a sign-in within web.session_idle_minutes, measured on the web
// session every production token is minted from (V7.5.1; review round 1
// of 0.29.113: the SPA's /auth/token token is a child too, and a token
// with no parent has no sign-in the rule can trust).
func TestAPISessionsAreListedEndedAndTheEmailChangeNeedsARecentSignIn(t *testing.T) {
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
	tag := fmt.Sprintf("_avapisess%d", time.Now().UnixNano())
	user := func(login string, admin bool) int {
		var id int
		if err := store.Pool().QueryRow(ctx, `
			INSERT INTO aveloxis_ops.users (login_name, oauth_provider, admin) VALUES ($1, 'github', $2)
			RETURNING user_id`, login, admin).Scan(&id); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			c := context.Background()
			_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, id)
			_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, id)
		})
		return id
	}
	adminID := user(tag+"-admin", true)
	userID := user(tag+"-user", false)
	const idle = 10 * time.Minute
	s, err := NewWithOptions(store, logger, Options{RateLimitRPS: 100, RateLimitBurst: 100, RateLimitDaily: 10000, RequireAuth: true, SessionIdle: idle})
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	call := func(method, path, token, body string) *httptest.ResponseRecorder {
		var rd io.Reader
		if body != "" {
			rd = strings.NewReader(body)
		}
		r := httptest.NewRequest(method, path, rd)
		r.RemoteAddr = "127.0.0.1:1"
		r.Header.Set("Authorization", "Bearer "+token)
		if body != "" {
			r.Header.Set("Content-Type", "application/json")
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	// The account: a browser session, the proxy's child of it, and a /auth/token token.
	webRaw, err := store.CreateWebSession(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	child, err := store.MintTokenForWebSession(ctx, webRaw, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	spa, err := store.MintTokenForWebSession(ctx, webRaw, time.Now().Add(db.DefaultSessionTokenLifetime)) // /auth/token's mint
	if err != nil {
		t.Fatal(err)
	}

	w := call(http.MethodGet, "/api/v1/me/sessions", spa, "")
	if w.Code != http.StatusOK {
		t.Fatalf("GET /me/sessions = %d %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
		t.Error("the session list must be no-store")
	}
	var listed struct {
		Sessions []sessionJSON `json:"sessions"`
	}
	if err := json.NewDecoder(w.Body).Decode(&listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Sessions) != 3 {
		t.Fatalf("sessions listed = %d, want 3 (web, proxy child, SPA token): %+v", len(listed.Sessions), listed.Sessions)
	}
	var current, web, fromWeb int
	for _, u := range listed.Sessions {
		if u.Current {
			current++
			if u.Kind != "api" || !u.FromWebSession {
				t.Errorf("the current session is %+v; want the SPA token, a child of the web session", u)
			}
		}
		if u.Kind == "web" {
			web++
		}
		if u.FromWebSession {
			fromWeb++
		}
		if u.CreatedAt.IsZero() || u.ExpiresAt.IsZero() {
			t.Errorf("session %+v carries no timestamps", u)
		}
	}
	if current != 1 || web != 1 || fromWeb != 2 {
		t.Errorf("current=%d web=%d from_web_session=%d; want 1/1/2", current, web, fromWeb)
	}

	// The e-mail change: a fresh sign-in passes the gate (the mailer is not
	// configured here, so the answer is the 422 the form shows, never the
	// re-authentication 403).
	const emailBody = `{"email":"someone@example.org"}`
	if w := call(http.MethodPost, "/api/v1/me/email", spa, emailBody); w.Code == http.StatusForbidden {
		t.Fatalf("a fresh token was asked to re-authenticate: %s", w.Body.String())
	}
	// Both tokens are judged by the web session they came from: a fresh
	// one passes; a backdated one refuses both with the typed reason —
	// the tokens' own (fresh) created_at is not consulted.
	if w := call(http.MethodPost, "/api/v1/me/email", child, emailBody); w.Code == http.StatusForbidden {
		t.Fatalf("a child of a fresh web session was asked to re-authenticate: %s", w.Body.String())
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.user_session_tokens SET created_at = $2 WHERE token = $1`, db.HashToken(webRaw), time.Now().Add(-idle-time.Minute).Unix()); err != nil {
		t.Fatal(err)
	}
	for name, tok := range map[string]string{"spa": spa, "child": child} {
		w := call(http.MethodPost, "/api/v1/me/email", tok, emailBody)
		if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), `"reauthentication_required"`) {
			t.Fatalf("the %s token of an old sign-in changed the address: %d %s; want 403 reauthentication_required", name, w.Code, w.Body.String())
		}
	}
	// A token with no parent (the expiry sweep took it) has no sign-in to vouch for.
	loose, err := store.CreateSessionToken(ctx, userID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if w := call(http.MethodPost, "/api/v1/me/email", loose, emailBody); w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), `"reauthentication_required"`) {
		t.Fatalf("a parentless token's address change = %d %s; want 403 reauthentication_required (its own fresh mint must not count)", w.Code, w.Body.String())
	}

	// A non-administrator cannot end another account's sessions; a bad id is 400.
	if w := call(http.MethodPost, fmt.Sprintf("/api/v1/admin/users/%d/sessions/revoke", adminID), spa, ""); w.Code != http.StatusForbidden {
		t.Errorf("non-admin ending another account's sessions = %d, want 403", w.Code)
	}
	adminTok, err := store.CreateSessionToken(ctx, adminID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if w := call(http.MethodPost, "/api/v1/admin/users/x/sessions/revoke", adminTok, ""); w.Code != http.StatusBadRequest {
		t.Errorf("bad user id = %d, want 400", w.Code)
	}

	// The account ends its own — after a recent sign-in (ASVS V7.5.2, the
	// 0.29.125 review): the web session was aged above, so the first try is
	// the re-authentication refusal and ends nothing.
	if w := call(http.MethodPost, "/api/v1/me/sessions/revoke", spa, ""); w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), `"reauthentication_required"`) {
		t.Fatalf("an old sign-in's sign-out-everywhere = %d %s; want 403 reauthentication_required", w.Code, w.Body.String())
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.user_session_tokens SET created_at = $2 WHERE token = $1`, db.HashToken(webRaw), time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	// Every token dies, this one included.
	w = call(http.MethodPost, "/api/v1/me/sessions/revoke", spa, "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"ended":4`) {
		t.Fatalf("POST /me/sessions/revoke = %d %s; want 200 ended 4 (web, its two children, the loose token)", w.Code, w.Body.String())
	}
	for name, tok := range map[string]string{"spa": spa, "child": child} {
		if w := call(http.MethodGet, "/api/v1/me/sessions", tok, ""); w.Code != http.StatusUnauthorized {
			t.Errorf("after revoke the %s token still answers %d, want 401", name, w.Code)
		}
	}
	if _, err := store.LookupWebSession(ctx, webRaw); err == nil {
		t.Error("after revoke the web session is still live")
	}

	// The administrator ends another account's.
	again, err := store.CreateSessionToken(ctx, userID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	w = call(http.MethodPost, fmt.Sprintf("/api/v1/admin/users/%d/sessions/revoke", userID), adminTok, "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"ended":1`) {
		t.Fatalf("admin revoke = %d %s; want 200 ended 1", w.Code, w.Body.String())
	}
	if w := call(http.MethodGet, "/api/v1/me/sessions", again, ""); w.Code != http.StatusUnauthorized {
		t.Errorf("after the admin's revoke the account's token still answers %d, want 401", w.Code)
	}
	if w := call(http.MethodGet, "/api/v1/me/sessions", adminTok, ""); w.Code != http.StatusOK {
		t.Errorf("the administrator's own session was ended too: %d", w.Code)
	}
}
