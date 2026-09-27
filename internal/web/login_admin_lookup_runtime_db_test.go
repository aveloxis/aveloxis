// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
)

// TestLoginWithFailedAdminLookupIsNonAdminAndNotRefused is the RUNTIME
// version of the follow-up-6 login contract (batch-2 review round 13, after
// twelve rounds of a structural pin each escaped by a respelling; the
// round-11 record "a runtime version needs a store-side fault the fixtures
// cannot inject" was wrong). The fault is an AFTER INSERT OR UPDATE trigger
// on aveloxis_ops.users that deletes the probe's own row and records its
// user_id: UpsertOAuthUser's write succeeds, then IsUserAdmin's SELECT finds
// no row and returns (false, pgx.ErrNoRows) — the ERROR arm's exact input.
// The fixture walks the function's own axes (round 14: one point of that
// space let escapes conditioned on the others through): both providers, first signup
// AND returning user (the row seeded before the trigger exists, so the login
// takes the UPDATE branch and wasNewUser is false), under the production
// config (DevMode false). The contract: the login is NOT refused (302, and
// exactly ONE live aveloxis_session cookie — a browser keeps the LAST
// Set-Cookie, so a trailing deletion cookie is a refusal), the session is
// non-admin and belongs to the user the trigger saw, and one log line
// carries both ERROR and the message. Round 15: the session is read the way
// production reads it (getSession, which honours ExpiresAt — an expired
// session is a refusal in effect), the sessions map holds exactly the one
// session (a second admin session under a known token is not), and the
// response sets exactly the login's two cookies. TestLoginLogsAFailedAdminLookup
// is the structural twin the DB-less test.yml job runs (integration.yml
// runs both); deferred writes (a scheduled func literal) are only that
// pin's to catch.
func TestLoginWithFailedAdminLookupIsNonAdminAndNotRefused(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	for _, provider := range []string{"github", "gitlab"} {
		for _, returning := range []bool{false, true} {
			name := provider + "/new"
			if returning {
				name = provider + "/returning"
			}
			t.Run(name, func(t *testing.T) { loginWithFailedAdminLookup(t, dsn, provider, returning) })
		}
	}
}

func loginWithFailedAdminLookup(t *testing.T, dsn, provider string, returning bool) {
	ctx := context.Background()
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	pool := store.Pool()
	const login = "_avweb_admin_lookup_fault"
	const trigger = "avweb_admin_lookup_fault_r13"
	clean := func() {
		for _, q := range []string{
			`DROP TRIGGER IF EXISTS ` + trigger + ` ON aveloxis_ops.users`,
			`DROP FUNCTION IF EXISTS aveloxis_ops.` + trigger + `()`,
			`DROP TABLE IF EXISTS aveloxis_ops.` + trigger + `_ids`,
		} {
			if _, err := pool.Exec(ctx, q); err != nil {
				t.Logf("cleanup %q: %v", q, err)
			}
		}
		if _, err := pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login); err != nil {
			t.Logf("cleanup of the probe user: %v", err)
		}
	}
	clean()
	t.Cleanup(clean)
	if returning {
		// Seeded BEFORE the trigger exists, so the login under test takes
		// UpsertOAuthUser's UPDATE branch.
		if _, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: provider}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE aveloxis_ops.`+trigger+`_ids (user_id int)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE FUNCTION aveloxis_ops.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN
			IF NEW.login_name = '`+login+`' THEN
				INSERT INTO aveloxis_ops.`+trigger+`_ids VALUES (NEW.user_id);
				DELETE FROM aveloxis_ops.users WHERE user_id = NEW.user_id;
			END IF;
			RETURN NULL;
		END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER `+trigger+` AFTER INSERT OR UPDATE ON aveloxis_ops.users FOR EACH ROW EXECUTE FUNCTION aveloxis_ops.`+trigger+`()`); err != nil {
		t.Fatal(err)
	}

	s := New(store, config.WebConfig{}, nil, "", logger) // the production config: DevMode false
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/auth/"+provider+"/callback?code=x&state=y", nil)
	s.completeOAuthLogin(rec, req, db.OAuthUserInfo{Login: login, Provider: provider}, provider)

	if rec.Code != http.StatusFound {
		t.Fatalf("a failed admin lookup must not refuse the login: got %d %q", rec.Code, rec.Body.String())
	}
	var sessionCookies []*http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == "aveloxis_session" {
			sessionCookies = append(sessionCookies, c)
		}
	}
	if len(sessionCookies) != 1 {
		t.Fatalf("want exactly one aveloxis_session Set-Cookie (a browser keeps the last), got %d: %v", len(sessionCookies), rec.Header().Values("Set-Cookie"))
	}
	c := sessionCookies[0]
	if c.MaxAge <= 0 || c.Value == "" || c.Path != "/" {
		t.Fatalf("the session cookie is not a live cookie: %s", c.String())
	}
	// The response sets exactly the login's cookies: the session and the
	// oauth_next deletion (round 15: a second cookie under another name the
	// reader preferred passed).
	names := map[string]bool{}
	for _, sc := range rec.Result().Cookies() {
		names[sc.Name] = true
	}
	if len(names) != 2 || !names["aveloxis_session"] || !names["oauth_next"] {
		t.Errorf("the response sets cookies %v; want exactly aveloxis_session and the oauth_next deletion", rec.Header().Values("Set-Cookie"))
	}
	// Read the session the way production does — getSession honours
	// ExpiresAt, so a session created and then expired is the refusal it is
	// (round 15: reading the map directly passed that).
	next := httptest.NewRequest(http.MethodGet, "/", nil)
	next.AddCookie(&http.Cookie{Name: c.Name, Value: c.Value})
	sess := s.getSession(next)
	if sess == nil {
		t.Fatal("the cookie's session does not resolve through getSession: the login was refused in effect")
	}
	s.sessionMu.RLock()
	sessions := len(s.sessions)
	s.sessionMu.RUnlock()
	if sessions != 1 {
		t.Errorf("%d sessions exist after one login; want exactly 1 (a second session minted under a known token is an escalation)", sessions)
	}
	if sess.IsAdmin {
		t.Error("a failed admin lookup created an ADMIN session")
	}
	if sess.LoginName != login {
		t.Errorf("session login = %q, want %q", sess.LoginName, login)
	}
	var seen int
	if err := pool.QueryRow(ctx, `SELECT user_id FROM aveloxis_ops.`+trigger+`_ids`).Scan(&seen); err != nil {
		t.Fatal(err)
	}
	if sess.UserID != seen {
		t.Errorf("session user_id = %d, want the signed-in user's %d (the id the trigger saw)", sess.UserID, seen)
	}
	logged := false
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "level=ERROR") && strings.Contains(line, "admin flag lookup failed at login") {
			logged = true
		}
	}
	if !logged {
		t.Errorf("the failed lookup was not logged at ERROR on one line:\n%s", logs.String())
	}
}
