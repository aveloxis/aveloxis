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

// TestAddRepoFailureIsShownToTheUser (AVELOXIS_TEST_DB) drives POST
// /groups/add-repo through the real handler and store with
// web.auto_approve_add_limit on. With the group link of one pasted repository
// failing, or the store failing, the user is redirected to the group page with
// add_error=1 and the page says some repositories could not be added (Copilot
// review of PR #207 on eb248eb: the handler logged the error and redirected as
// on success, so the user had no reason to retry). A rejected group gets
// add_error=rejected and says why instead, since trying again cannot work, and
// a successful add shows no notice (round-24 review).
func TestAddRepoFailureIsShownToTheUser(t *testing.T) {
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
	const login = "_avweb_add_error_probe"
	const urlPrefix = "https://github.com/_avweb-add-error-owner/_avweb-add-error-"
	const trigger = "_avtest_web_fail_user_repos_link"
	pool := store.Pool()
	clean := func() {
		_, _ = pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+trigger+` ON aveloxis_ops.user_repos`)
		_, _ = pool.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_ops.`+trigger+`()`)
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
	gid, err := store.CreateUserGroup(ctx, uid, "web add error probe")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE FUNCTION aveloxis_ops.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN
			IF EXISTS (SELECT 1 FROM aveloxis_data.repos WHERE repo_id = NEW.repo_id AND repo_git LIKE '%fails') THEN
				RAISE EXCEPTION 'injected link failure' USING ERRCODE = '40001';
			END IF;
			RETURN NEW;
		END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER `+trigger+` BEFORE INSERT ON aveloxis_ops.user_repos FOR EACH ROW EXECUTE FUNCTION aveloxis_ops.`+trigger+`()`); err != nil {
		t.Fatal(err)
	}

	s := New(store, config.WebConfig{AutoApproveAddLimit: 5}, nil, "", logger)
	s.sessions["probe-token"] = &Session{UserID: uid, LoginName: login, ExpiresAt: time.Now().Add(time.Hour)}
	serve := func(r *http.Request) *httptest.ResponseRecorder {
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "probe-token"})
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	const notice = "Some of those repositories could not be added"
	const rejectedNotice = "These repositories were not added: an administrator rejected this group."
	post := func(srv *Server, groupID int64, urls string) *httptest.ResponseRecorder {
		t.Helper()
		form := url.Values{"group_id": {fmt.Sprint(groupID)}, "repo_urls": {urls}}
		r := httptest.NewRequest(http.MethodPost, "/groups/add-repo", strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "probe-token"})
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, r)
		return w
	}

	form := url.Values{"group_id": {fmt.Sprint(gid)}, "repo_urls": {urlPrefix + "fails\n" + urlPrefix + "works"}}
	r := httptest.NewRequest(http.MethodPost, "/groups/add-repo", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := serve(r)
	if want := fmt.Sprintf("/groups/%d?add_error=1", gid); w.Code != http.StatusFound || w.Header().Get("Location") != want {
		t.Fatalf("POST /groups/add-repo with a failing repository = %d Location %q; want 302 to %q", w.Code, w.Header().Get("Location"), want)
	}

	page := serve(httptest.NewRequest(http.MethodGet, w.Header().Get("Location"), nil))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), notice) {
		t.Errorf("GET %s = %d; want 200 and the page to say %q", w.Header().Get("Location"), page.Code, notice)
	}
	// Neither notice shows without its own value (round-25 review: nothing
	// checked the rejected notice stayed off other pages).
	for _, path := range []string{fmt.Sprintf("/groups/%d", gid), fmt.Sprintf("/groups/%d?add_error=other", gid)} {
		plain := serve(httptest.NewRequest(http.MethodGet, path, nil))
		if plain.Code != http.StatusOK || strings.Contains(plain.Body.String(), notice) || strings.Contains(plain.Body.String(), rejectedNotice) {
			t.Errorf("GET %s = %d; want 200 without either add notice", path, plain.Code)
		}
	}

	// A successful add redirects without a notice.
	if w := post(s, gid, urlPrefix+"succeeds"); w.Code != http.StatusFound || w.Header().Get("Location") != fmt.Sprintf("/groups/%d", gid) {
		t.Errorf("a successful add = %d Location %q; want 302 to /groups/%d", w.Code, w.Header().Get("Location"), gid)
	}

	// A rejected group says so, not "try again".
	rejected, err := store.CreateUserGroup(ctx, uid, "web add error probe, rejected")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RejectGroup(ctx, rejected, uid); err != nil {
		t.Fatal(err)
	}
	w = post(s, rejected, urlPrefix+"to-rejected")
	if want := fmt.Sprintf("/groups/%d?add_error=rejected", rejected); w.Code != http.StatusFound || w.Header().Get("Location") != want {
		t.Fatalf("an add to a rejected group = %d Location %q; want 302 to %q", w.Code, w.Header().Get("Location"), want)
	}
	page = serve(httptest.NewRequest(http.MethodGet, w.Header().Get("Location"), nil))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), rejectedNotice) || strings.Contains(page.Body.String(), notice) {
		t.Errorf("GET %s = %d; want 200 saying %q and not %q", w.Header().Get("Location"), page.Code, rejectedNotice, notice)
	}

	// A store failure, which is not an auto-approve failure, is shown too.
	closed, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	down := New(closed, config.WebConfig{AutoApproveAddLimit: 5}, nil, "", logger)
	down.sessions["probe-token"] = &Session{UserID: uid, LoginName: login, ExpiresAt: time.Now().Add(time.Hour)}
	if w := post(down, gid, urlPrefix+"store-down"); w.Code != http.StatusFound || w.Header().Get("Location") != fmt.Sprintf("/groups/%d?add_error=1", gid) {
		t.Errorf("an add with the store down = %d Location %q; want 302 to /groups/%d?add_error=1", w.Code, w.Header().Get("Location"), gid)
	}
}
