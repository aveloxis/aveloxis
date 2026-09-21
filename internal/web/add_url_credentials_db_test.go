// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"bytes"
	"context"
	"fmt"
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

// TestAddURLWithCredentialsIsRefusedAndNotLogged (AVELOXIS_TEST_DB) —
// v0.29.57 fix-review round 1 on the Copilot 5261384568 fixes. Through the
// real handlers: a pasted repo URL carrying credentials is refused, the clean
// URL beside it is still added, and the server log names the refusal WITHOUT
// the credential (the "some URLs were invalid" WARN wrote the URL verbatim).
// An org URL carrying credentials, or an over-long one, sends the user back
// to the group page with org_error=invalid and a notice — the store's
// refusal used to fall through to a plain redirect — and registers nothing.
func TestAddURLWithCredentialsIsRefusedAndNotLogged(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	const login = "_avweb_cred_url_probe"
	const marker = "_avweb-cred-url-probe"
	const secret = "s3cret-token-value"
	pool := store.Pool()
	clean := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE '%' || $1 || '%')`, marker)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE '%' || $1 || '%'`, marker)
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
	if _, err := pool.Exec(ctx, `UPDATE aveloxis_ops.users SET admin = TRUE WHERE user_id = $1`, uid); err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "credential url probe")
	if err != nil {
		t.Fatal(err)
	}
	s := New(store, config.WebConfig{}, nil, "", logger)
	s.sessions["probe"] = &Session{UserID: uid, LoginName: login, IsAdmin: true, ExpiresAt: time.Now().Add(time.Hour)}
	post := func(path string, form url.Values) *httptest.ResponseRecorder {
		t.Helper()
		form.Set("group_id", fmt.Sprint(gid))
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "probe"})
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	get := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "probe"})
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}

	// Repos: the credentialed URL is refused, the clean one added, the log
	// names the refusal but not the secret.
	clean1 := "https://git.example.invalid/" + marker + "/clean"
	bad := "https://user:" + secret + "@git.example.invalid/" + marker + "/creds"
	logs.Reset()
	w := post("/groups/add-repo", url.Values{"repo_urls": {bad + "\n" + clean1}})
	// Round 2: the refused line is reported on the page, not only logged.
	if want := fmt.Sprintf("/groups/%d?add_error=invalid", gid); w.Code != http.StatusFound || w.Header().Get("Location") != want {
		t.Fatalf("add-repo = %d %q; want 302 to %s (the clean URL was added, the refused line reported)", w.Code, w.Header().Get("Location"), want)
	}
	const repoNotice = "Some of those lines were not valid repository URLs"
	if page := get(w.Header().Get("Location")); page.Code != http.StatusOK || !strings.Contains(page.Body.String(), repoNotice) {
		t.Errorf("GET %s = %d; want 200 and the page to say %q", w.Header().Get("Location"), page.Code, repoNotice)
	}
	// An over-long URL is the store's refusal of the whole paste; it is the
	// user's input too, not a "try again".
	if w := post("/groups/add-repo", url.Values{"repo_urls": {"https://git.example.invalid/" + marker + "/" + strings.Repeat("y", db.MaxAddURLBytes)}}); w.Code != http.StatusFound || !strings.HasSuffix(w.Header().Get("Location"), "?add_error=invalid") {
		t.Errorf("over-long add-repo = %d %q; want 302 to ?add_error=invalid", w.Code, w.Header().Get("Location"))
	}
	// A clean paste still redirects plainly.
	if w := post("/groups/add-repo", url.Values{"repo_urls": {"https://git.example.invalid/" + marker + "/clean2"}}); w.Code != http.StatusFound || w.Header().Get("Location") != fmt.Sprintf("/groups/%d", gid) {
		t.Errorf("clean add-repo = %d %q; want a plain redirect", w.Code, w.Header().Get("Location"))
	}
	// The schemeless paste the validator refuses after prepending https://:
	// the log must show the redacted ORIGINAL line, not the token (round 2).
	logs.Reset()
	if w := post("/groups/add-repo", url.Values{"repo_urls": {"tok-" + secret + "@github.com/" + marker + "-owner/schemeless"}}); w.Code != http.StatusFound || !strings.HasSuffix(w.Header().Get("Location"), "?add_error=invalid") {
		t.Errorf("schemeless credentialed add-repo = %d %q; want 302 to ?add_error=invalid", w.Code, w.Header().Get("Location"))
	}
	if strings.Contains(logs.String(), secret) || !strings.Contains(logs.String(), "***@github.com/") {
		t.Errorf("the schemeless paste's log line must be redacted: %s", logs.String())
	}
	logs.Reset()
	if w := post("/groups/add-repo", url.Values{"repo_urls": {bad + "\n" + clean1}}); !strings.HasSuffix(w.Header().Get("Location"), "?add_error=invalid") {
		t.Errorf("re-posting the mixed paste = %d %q; want ?add_error=invalid", w.Code, w.Header().Get("Location"))
	}
	if id, err := store.FindRepoByURL(ctx, clean1); err != nil || id == 0 {
		t.Errorf("the clean URL beside the refused one was not added (id %d, %v)", id, err)
	}
	if !strings.Contains(logs.String(), "some URLs were invalid") || !strings.Contains(logs.String(), "credentials") {
		t.Errorf("the refusal was not logged: %s", logs.String())
	}
	if strings.Contains(logs.String(), secret) {
		t.Errorf("the log carries the credential: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "https://***@git.example.invalid/") {
		t.Errorf("the log should carry the redacted spelling: %s", logs.String())
	}
	var stored int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.repos WHERE repo_git LIKE '%' || $1 || '%'`, secret).Scan(&stored); err != nil || stored != 0 {
		t.Errorf("a repo row carries the credential (%d rows, %v)", stored, err)
	}

	// Orgs: credentials, and an over-long URL, both fixable by the user.
	registered := func() int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, gid).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for name, orgURL := range map[string]string{
		"credentials": "https://user:" + secret + "@github.com/" + marker + "-org",
		"over-long":   "https://github.com/" + marker + "-" + strings.Repeat("x", db.MaxAddURLBytes),
	} {
		logs.Reset()
		w := post("/groups/add-org", url.Values{"org_url": {orgURL}})
		want := fmt.Sprintf("/groups/%d?org_error=invalid", gid)
		if w.Code != http.StatusFound || w.Header().Get("Location") != want {
			t.Errorf("%s org add = %d %q; want 302 to %s", name, w.Code, w.Header().Get("Location"), want)
		}
		if strings.Contains(logs.String(), secret) {
			t.Errorf("%s: the log carries the credential: %s", name, logs.String())
		}
		if !strings.Contains(logs.String(), "org not added") {
			t.Errorf("%s: the refusal was not logged: %s", name, logs.String())
		}
	}
	if n := registered(); n != 0 {
		t.Errorf("refused org adds registered %d rows", n)
	}
	const notice = "That organization was not added: its URL is invalid"
	page := get(fmt.Sprintf("/groups/%d?org_error=invalid", gid))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), notice) {
		t.Errorf("GET ?org_error=invalid = %d; want 200 and the page to say %q", page.Code, notice)
	}
	if plain := get(fmt.Sprintf("/groups/%d", gid)); plain.Code != http.StatusOK || strings.Contains(plain.Body.String(), notice) {
		t.Errorf("the notice shows without its value")
	}
}
