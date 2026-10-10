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
// on aveloxis_ops.users that sets the probe's own row's admin to NULL (the
// column is made nullable for the test) and records every INSERT and
// UPDATE with its admin value (DELETE and TRUNCATE are the statement
// trigger's): UpsertOAuthUser's write succeeds, then IsUserAdmin's Scan of the
// NULL fails — the ERROR arm's input — while the row PERSISTS. Round 13's
// trigger deleted the row instead, and a store write keyed by the user's id
// (round 17: `s.store.SetUserAdmin(…, true)` in the arm; round 18: the same
// through an alias, a method value or a helper handed the receiver, or a
// re-probe between the arm and the session) then hit nothing the test could
// see while promoting the user's NEXT login; now no recorded write may carry
// admin = TRUE and the row must not end TRUE.
// The fixture walks the function's own axes (round 14: one point of that
// space let escapes conditioned on the others through): both providers, first signup
// AND returning user (the row seeded before the trigger exists, so the login
// takes the UPDATE branch and wasNewUser is false), under the production
// config (DevMode false). The contract: the login is NOT refused (302 to
// the default destination /dashboard — the fixture carries no oauth_next
// cookie — and exactly ONE live aveloxis_session cookie — a browser keeps
// the LAST Set-Cookie, so a trailing deletion cookie is a refusal — every
// cookie of the response Secure and HttpOnly), the session is
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
			`DO $d$ DECLARE r record; BEGIN
				FOR r IN SELECT tgrelid::regclass AS rel FROM pg_trigger WHERE tgname = '` + trigger + `_all' LOOP
					EXECUTE format('DROP TRIGGER IF EXISTS ` + trigger + `_all ON %s', r.rel);
				END LOOP;
			END $d$`,
			`DROP FUNCTION IF EXISTS aveloxis_ops.` + trigger + `_all()`,
			`DROP TABLE IF EXISTS aveloxis_ops.` + trigger + `_writes`,
		} {
			if _, err := pool.Exec(ctx, q); err != nil {
				t.Logf("cleanup %q: %v", q, err)
			}
		}
		if _, err := pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login); err != nil {
			t.Logf("cleanup of the probe user: %v", err)
		}
		if _, err := pool.Exec(ctx, `ALTER TABLE aveloxis_ops.users ALTER COLUMN admin SET NOT NULL`); err != nil {
			t.Logf("restoring users.admin NOT NULL: %v", err)
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
	// The row persists (round 18); the lookup fails on a NULL flag instead.
	if _, err := pool.Exec(ctx, `ALTER TABLE aveloxis_ops.users ALTER COLUMN admin DROP NOT NULL`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE aveloxis_ops.`+trigger+`_ids (user_id int, login_name text, admin boolean)`); err != nil {
		t.Fatal(err)
	}
	// EVERY insert or update of a users row is recorded with its login and
	// the admin value it carried (round 19: recording only the probe's row
	// let an arm mint or promote ANOTHER account's admin row unseen); only
	// the probe row's flag is then set to NULL, at depth 1 (the nested
	// UPDATE fires the trigger at depth 2, which only records).
	if _, err := pool.Exec(ctx, `CREATE FUNCTION aveloxis_ops.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN
			INSERT INTO aveloxis_ops.`+trigger+`_ids VALUES (NEW.user_id, NEW.login_name, NEW.admin);
			IF NEW.login_name = '`+login+`' AND pg_trigger_depth() = 1 THEN
				UPDATE aveloxis_ops.users SET admin = NULL WHERE user_id = NEW.user_id;
			END IF;
			RETURN NULL;
		END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER `+trigger+` AFTER INSERT OR UPDATE ON aveloxis_ops.users FOR EACH ROW EXECUTE FUNCTION aveloxis_ops.`+trigger+`()`); err != nil {
		t.Fatal(err)
	}
	// Every OTHER table: the login writes users and nothing else (sessions
	// live in memory, no mail is configured), so a statement-level trigger
	// on every existing table of the three data schemas records any
	// INSERT, UPDATE, DELETE or TRUNCATE there, and on users any DELETE or
	// TRUNCATE (rounds 20–22: a session token, a group takeover and an
	// approval each reached a table the fixture did not watch; one table at
	// a time was the wrong level). What this cannot see — DDL, and a write
	// a Go-side existence check keeps this empty fixture from provoking —
	// the structural pin's two allowlists (receiver members, package-level
	// names) refuse.
	if _, err := pool.Exec(ctx, `CREATE TABLE aveloxis_ops.`+trigger+`_writes (tbl text)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE FUNCTION aveloxis_ops.`+trigger+`_all() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN
			INSERT INTO aveloxis_ops.`+trigger+`_writes VALUES (TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME || ' ' || TG_OP);
			RETURN NULL;
		END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DO $d$ DECLARE r record; BEGIN
		FOR r IN SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname IN ('aveloxis_ops', 'aveloxis_data', 'aveloxis_scan') AND c.relkind IN ('r', 'p')
			AND c.relname <> 'users' AND c.relname NOT LIKE '`+trigger+`%' LOOP
			EXECUTE format('CREATE TRIGGER `+trigger+`_all AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON %I.%I FOR EACH STATEMENT EXECUTE FUNCTION aveloxis_ops.`+trigger+`_all()', r.nspname, r.relname);
		END LOOP;
		EXECUTE 'CREATE TRIGGER `+trigger+`_all AFTER DELETE OR TRUNCATE ON aveloxis_ops.users FOR EACH STATEMENT EXECUTE FUNCTION aveloxis_ops.`+trigger+`_all()';
	END $d$`); err != nil {
		t.Fatal(err)
	}

	s := New(store, config.WebConfig{}, nil, "", logger) // the production config: DevMode false
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/auth/"+provider+"/callback?code=x&state=y", nil)
	s.completeOAuthLogin(rec, req, db.OAuthUserInfo{Login: login, Provider: provider}, provider)

	if rec.Code != http.StatusFound {
		t.Fatalf("a failed admin lookup must not refuse the login: got %d %q", rec.Code, rec.Body.String())
	}
	// The destination is the login's default (round 16: a redirect to
	// /logout — an oauth_next cookie added in the arm — was a refusal both
	// tiers passed; the fixture carries no oauth_next cookie, so the default
	// IS the contract).
	if loc := rec.Header().Get("Location"); loc != "/dashboard" {
		t.Errorf("the login redirected to %q; want the default /dashboard (a destination that logs the user out is a refusal)", loc)
	}
	var sessionCookies []*http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == "aveloxis_session" {
			sessionCookies = append(sessionCookies, c)
		}
	}
	if len(sessionCookies) != 1 {
		t.Fatalf("want exactly one aveloxis_session Set-Cookie (a browser keeps the last), got %d: %v", len(sessionCookies), cookieStrings(rec))
	}
	c := sessionCookies[0]
	if c.MaxAge <= 0 || c.Value == "" || c.Path != "/" {
		t.Fatalf("the session cookie is not a live cookie: %s", c.String())
	}
	// Under the production config (DevMode false) the cookies are Secure
	// and HttpOnly (round 16: `s.cfg.DevMode = true` in the arm stripped
	// Secure for the whole process and nothing read the attribute).
	for _, sc := range rec.Result().Cookies() {
		if !sc.Secure || !sc.HttpOnly {
			t.Errorf("cookie %s is not Secure+HttpOnly under the production config: %s", sc.Name, sc.String())
		}
	}
	// The response sets exactly the login's cookies: the session and the
	// oauth_next deletion (round 15: a second cookie under another name the
	// reader preferred passed).
	names := map[string]bool{}
	for _, sc := range rec.Result().Cookies() {
		names[sc.Name] = true
	}
	if len(names) != 2 || !names["aveloxis_session"] || !names["oauth_next"] {
		// Printed from what the client receives (round 18: the live header
		// map listed a cookie the snapshot at WriteHeader had dropped).
		t.Errorf("the response sets cookies %v; want exactly aveloxis_session and the oauth_next deletion", cookieStrings(rec))
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
	var promoted bool
	if err := pool.QueryRow(ctx, `SELECT (SELECT min(user_id) FROM aveloxis_ops.`+trigger+`_ids WHERE login_name = $1), COALESCE(bool_or(admin IS TRUE), FALSE) FROM aveloxis_ops.`+trigger+`_ids`, login).Scan(&seen, &promoted); err != nil {
		t.Fatal(err)
	}
	if sess.UserID != seen {
		t.Errorf("session user_id = %d, want the signed-in user's %d (the id the trigger saw)", sess.UserID, seen)
	}
	// The escalation the deleting fixture could not see (rounds 17–19): a
	// write that sets admin = TRUE on any users row promotes a NEXT login —
	// this user's or another account's — whatever the session says now.
	if promoted {
		t.Error("a write set admin = TRUE on a users row during the login: the failed lookup promoted the user's next login, or another account's")
	}
	var elsewhere []string
	if err := pool.QueryRow(ctx, `SELECT COALESCE(array_agg(DISTINCT tbl), '{}') FROM aveloxis_ops.`+trigger+`_writes`).Scan(&elsewhere); err != nil {
		t.Fatal(err)
	}
	// v0.29.89 (summary/53): a NEW account also inserts its sign-up record
	// (the per-address quota's count; no privilege, no session). Exactly that
	// statement is admitted — an UPDATE or DELETE there is still refused.
	var other []string
	for _, w := range elsewhere {
		// v0.29.89: a NEW account also writes its day's sign-up secret (once
		// per UTC day). A returning login writes neither (review round 1 R2-8).
		if !returning && (w == "aveloxis_ops.account_signups INSERT" || w == "aveloxis_ops.signup_key_secrets INSERT") {
			continue
		}
		other = append(other, w)
	}
	if len(other) != 0 {
		t.Errorf("the login wrote %v: it writes aveloxis_ops.users (and a new account's sign-up record) and nothing else — a session token, a group or an approval written here is an escalation (rounds 20–21)", other)
	}
	var adminNow *bool
	if err := pool.QueryRow(ctx, `SELECT admin FROM aveloxis_ops.users WHERE user_id = $1`, seen).Scan(&adminNow); err != nil {
		t.Fatalf("the user's row must persist through the login: %v", err)
	}
	if adminNow != nil && *adminNow {
		t.Error("the user's row ends with admin = TRUE after a failed lookup")
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

// cookieStrings is what the client receives — rec.Result().Cookies(), the
// snapshot at WriteHeader — rendered for a failure message; the live header
// map can hold a cookie set after WriteHeader that no client ever sees.
func cookieStrings(rec *httptest.ResponseRecorder) []string {
	var out []string
	for _, c := range rec.Result().Cookies() {
		out = append(out, c.String())
	}
	return out
}
