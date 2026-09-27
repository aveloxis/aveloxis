// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
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

// TestAddNoticesAreShownToTheUser pins worklist follow-ups 10 and 11 (the
// PR #207 review): the group page says when a paste or an org registration
// is waiting for an administrator (?pending=N, ?org_pending=1 — the
// redirects existed since v0.27.20, the page rendered nothing for them), and
// an org add whose store call FAILS says so (?org_error=1) instead of
// redirecting as if it had succeeded. The fixture user is a non-admin with
// auto-approval off, so a new repository and a new org both pend; the
// failing org is injected by a trigger on the add-request row it pends on.
func TestAddNoticesAreShownToTheUser(t *testing.T) {
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
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	const login = "_avweb_notice_probe"
	const urlPrefix = "https://github.com/_avweb-notice-owner/_avweb-notice-"
	const orgPrefix = "https://github.com/_avweb-notice-org"
	const trigger = "_avtest_web_fail_org_request"
	pool := store.Pool()
	clean := func() {
		_, _ = pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+trigger+` ON aveloxis_ops.collection_add_requests`)
		_, _ = pool.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_ops.`+trigger+`()`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE org_url LIKE $1 || '%'`, orgPrefix)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%')`, urlPrefix)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%'`, urlPrefix)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if admin, err := store.IsUserAdmin(ctx, uid); err != nil || admin {
		t.Fatalf("fixture user must be a non-admin (admin=%v, err=%v)", admin, err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "web notices probe")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE FUNCTION aveloxis_ops.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN
			IF NEW.org_url LIKE '%fails' THEN
				RAISE EXCEPTION 'injected add-request failure';
			END IF;
			RETURN NEW;
		END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER `+trigger+` BEFORE INSERT ON aveloxis_ops.collection_add_requests FOR EACH ROW EXECUTE FUNCTION aveloxis_ops.`+trigger+`()`); err != nil {
		t.Fatal(err)
	}

	s := New(store, config.WebConfig{}, nil, "", logger)
	s.sessions["probe-token"] = &Session{UserID: uid, LoginName: login, ExpiresAt: time.Now().Add(time.Hour)}
	serve := func(r *http.Request) *httptest.ResponseRecorder {
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "probe-token"})
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	post := func(path string, form url.Values) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return serve(r)
	}
	const pendingNotice = "repositories are waiting for an administrator"
	const orgPendingNotice = "organization is waiting for an administrator"
	const orgErrorNotice = "That organization could not be added"
	const orgRejectedNotice = "That organization was not added: an administrator rejected this group."
	notices := []string{pendingNotice, orgPendingNotice, orgErrorNotice, orgRejectedNotice, "repository is waiting for an administrator"}
	pageAfter := func(w *httptest.ResponseRecorder, wantLocation, wantNotice string) {
		t.Helper()
		if w.Code != http.StatusFound || w.Header().Get("Location") != wantLocation {
			t.Fatalf("= %d Location %q; want 302 to %q", w.Code, w.Header().Get("Location"), wantLocation)
		}
		page := serve(httptest.NewRequest(http.MethodGet, wantLocation, nil))
		body := page.Body.String()
		if page.Code != http.StatusOK || !strings.Contains(body, wantNotice) {
			t.Errorf("GET %s = %d; want 200 and the page to say %q", wantLocation, page.Code, wantNotice)
		}
		for _, other := range notices {
			if !strings.Contains(wantNotice, other) && strings.Contains(body, other) {
				t.Errorf("GET %s also says %q", wantLocation, other)
			}
		}
	}

	// A non-admin's new repositories pend: the page says how many.
	w := post("/groups/add-repo", url.Values{"group_id": {fmt.Sprint(gid)}, "repo_urls": {urlPrefix + "one\n" + urlPrefix + "two"}})
	pageAfter(w, fmt.Sprintf("/groups/%d?pending=2", gid), "2 "+pendingNotice)

	// A non-admin's new org pends.
	w = post("/groups/add-org", url.Values{"group_id": {fmt.Sprint(gid)}, "org_url": {orgPrefix + "-pends"}})
	pageAfter(w, fmt.Sprintf("/groups/%d?org_pending=1", gid), orgPendingNotice)

	// The store call fails: the page says so (follow-up 11: it redirected as
	// a success).
	w = post("/groups/add-org", url.Values{"group_id": {fmt.Sprint(gid)}, "org_url": {orgPrefix + "-fails"}})
	pageAfter(w, fmt.Sprintf("/groups/%d?org_error=1", gid), orgErrorNotice)

	// One pending repository reads in the singular.
	one := serve(httptest.NewRequest(http.MethodGet, fmt.Sprintf("/groups/%d?pending=1", gid), nil))
	if one.Code != http.StatusOK || !strings.Contains(one.Body.String(), "1 repository is waiting for an administrator") {
		t.Errorf("GET ?pending=1 = %d; want the singular notice", one.Code)
	}

	// The count is a number the handler parsed, never the query string
	// reflected into a notice styled as the site's own (review round 1 of
	// the batch): a non-number, zero or a negative shows nothing.
	for _, q := range []string{"", "?pending=abc", "?pending=0", "?pending=-7", "?pending=URGENT:%20email%20admin@evil.example%203", "?add_error=other"} {
		plain := serve(httptest.NewRequest(http.MethodGet, fmt.Sprintf("/groups/%d%s", gid, q), nil))
		body := plain.Body.String()
		if plain.Code != http.StatusOK || strings.Contains(body, "waiting for an administrator") || strings.Contains(body, "evil.example") {
			t.Errorf("GET /groups/%d%s = %d; want 200 without a pending notice", gid, q, plain.Code)
		}
		for _, n := range notices {
			if strings.Contains(body, n) {
				t.Errorf("GET /groups/%d%s says %q", gid, q, n)
			}
		}
	}

	// An org add to a REJECTED group says so, not "try again" (the repo
	// paste and the portal already distinguish it).
	rejected, err := store.CreateUserGroup(ctx, uid, "web notices probe, rejected")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RejectGroup(ctx, rejected, uid); err != nil {
		t.Fatal(err)
	}
	w = post("/groups/add-org", url.Values{"group_id": {fmt.Sprint(rejected)}, "org_url": {orgPrefix + "-to-rejected"}})
	pageAfter(w, fmt.Sprintf("/groups/%d?org_error=rejected", rejected), orgRejectedNotice)
}
