// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

// v0.29.57 (Copilot review 5260961848 on PR #210), two findings on the same
// field. New documented an empty ghAPIBase as public GitHub but stored it
// unchanged, so scanOrgRepos built a client whose request URLs had no host;
// and the scan's GitHub-or-GitLab gate compared the org URL's host against
// the literal "github.com", so on an Enterprise deployment the routed client
// (review 5260880711) was never reached for an org on the deployment's own
// host.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

func TestNewDefaultsAnEmptyGitHubBaseToPublic(t *testing.T) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(nil, config.WebConfig{Addr: ":0"}, platform.NewKeyPool([]string{"k"}, discard), "", discard)
	if s.ghAPIBase != platform.PublicGitHubAPIBase {
		t.Errorf("ghAPIBase = %q, want %q — the documented default of an empty base", s.ghAPIBase, platform.PublicGitHubAPIBase)
	}
	s = New(nil, config.WebConfig{Addr: ":0"}, nil, "https://ghe.example.invalid/api/v3", discard)
	if s.ghAPIBase != "https://ghe.example.invalid/api/v3" {
		t.Errorf("ghAPIBase = %q, want the configured base unchanged", s.ghAPIBase)
	}
}

// TestScanOrgReposEnumeratesTheConfiguredGitHubHost (AVELOXIS_TEST_DB): an
// org URL on the deployment's GitHub host is enumerated through the
// deployment's API base and lands in the group; an org URL on a DIFFERENT
// host is not sent to that base — the keys only ever go to their own host.
func TestScanOrgReposEnumeratesTheConfiguredGitHubHost(t *testing.T) {
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
		t.Fatal(err)
	}

	var hits atomic.Int64
	var api *httptest.Server
	api = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/orgs/acme/repos" || r.URL.Query().Get("page") != "1" {
			_, _ = io.WriteString(w, "[]")
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"id":       910011,
			"html_url": api.URL + "/acme/widgets",
			"name":     "widgets",
			"owner":    map[string]string{"login": "acme"},
		}})
	}))
	t.Cleanup(api.Close)
	repoGit := api.URL + "/acme/widgets"

	const login = "_avweb_org_scan_host_probe"
	clean := func() {
		p := store.Pool()
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1)`, repoGit)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1)`, repoGit)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git = $1`, repoGit)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "org scan host probe")
	if err != nil {
		t.Fatal(err)
	}

	// The deployment's GitHub host is the fake server; its keys belong there.
	s := New(store, config.WebConfig{}, platform.NewKeyPool([]string{"ghe-token"}, logger), api.URL, logger)

	inGroup := func() int {
		var n int
		if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_repos ur JOIN aveloxis_data.repos r ON r.repo_id = ur.repo_id WHERE ur.group_id = $1 AND r.repo_git = $2`, gid, repoGit).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// An org on public GitHub is NOT this deployment's host: nothing is
	// asked of the configured base (and nothing lands).
	s.scanOrgRepos(ctx, gid, "https://github.com/acme")
	if got := hits.Load(); got != 0 {
		t.Fatalf("an org on github.com sent %d requests to the configured Enterprise base; the keys belong to that base's host only", got)
	}
	if n := inGroup(); n != 0 {
		t.Fatalf("repo rows in group after a foreign-host org scan = %d, want 0", n)
	}

	// An org on the deployment's own host is enumerated and lands.
	s.scanOrgRepos(ctx, gid, api.URL+"/acme")
	if hits.Load() == 0 {
		t.Fatal("an org on the configured GitHub host was never enumerated — the gate still expects github.com")
	}
	if n := inGroup(); n != 1 {
		t.Errorf("repo rows in group after scanning the deployment's own host = %d, want 1", n)
	}
}

// TestHandleAddOrgRejectsAnOrgOffTheConfiguredGitHubHost (AVELOXIS_TEST_DB):
// registration is the owning layer (SR-18). A GitHub org URL whose host is
// not the deployment's GitHub host is refused with a visible error and
// registers nothing — otherwise the periodic refresh would enumerate its NAME
// on the deployment's host. An org on the deployment's host registers.
func TestHandleAddOrgRejectsAnOrgOffTheConfiguredGitHubHost(t *testing.T) {
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
		t.Fatal(err)
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "[]")
	}))
	t.Cleanup(api.Close)

	const login = "_avweb_add_org_host_probe"
	clean := func() {
		p := store.Pool()
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.users SET admin = TRUE WHERE user_id = $1`, uid); err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "add org host probe")
	if err != nil {
		t.Fatal(err)
	}
	s := New(store, config.WebConfig{}, platform.NewKeyPool([]string{"ghe-token"}, logger), api.URL, logger)
	s.sessions["admin"] = &Session{UserID: uid, LoginName: login, IsAdmin: true, ExpiresAt: time.Now().Add(time.Hour)}
	post := func(orgURL string) *httptest.ResponseRecorder {
		t.Helper()
		form := url.Values{"group_id": {strconv.FormatInt(gid, 10)}, "org_url": {orgURL}}
		r := httptest.NewRequest(http.MethodPost, "/groups/add-org", strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "admin"})
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	registered := func() int {
		var n int
		if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, gid).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	w := post("https://github.com/acme")
	if loc := w.Header().Get("Location"); w.Code != http.StatusFound || loc != "/groups/"+strconv.FormatInt(gid, 10)+"?org_error=host" {
		t.Fatalf("foreign-host org add = %d %q, want a redirect carrying org_error=host", w.Code, loc)
	}
	if n := registered(); n != 0 {
		t.Fatalf("a GitHub org on github.com registered %d rows on an Enterprise deployment; the refresh would enumerate its NAME on the wrong host", n)
	}

	w = post(api.URL + "/acme")
	if loc := w.Header().Get("Location"); w.Code != http.StatusFound || loc != "/groups/"+strconv.FormatInt(gid, 10) {
		t.Fatalf("own-host org add = %d %q, want a plain redirect", w.Code, loc)
	}
	if n := registered(); n != 1 {
		t.Errorf("an org on the deployment's own host registered %d rows, want 1", n)
	}

	// A GitLab group is not subject to the GitHub host gate (round 2: this
	// arm is what makes the platform conjunct load-bearing).
	w = post("https://gitlab.com/acme")
	if loc := w.Header().Get("Location"); w.Code != http.StatusFound || loc != "/groups/"+strconv.FormatInt(gid, 10) {
		t.Fatalf("gitlab group add = %d %q, want a plain redirect", w.Code, loc)
	}
	if n := registered(); n != 2 {
		t.Errorf("rows after adding a GitLab group = %d, want 2 — the host gate must not refuse GitLab groups", n)
	}
}

// TestHandleApproveAddRequestReportsAForeignHostOrgAsConflict
// (AVELOXIS_TEST_DB): a pending org request that predates the deployment's
// host (or came through any writer before the gate) cannot be approved; the
// admin sees WHY, as the portal's admin path reports it, not a 500.
func TestHandleApproveAddRequestReportsAForeignHostOrgAsConflict(t *testing.T) {
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
		t.Fatal(err)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	member, admin := "_avweb_approve_host_member"+suffix, "_avweb_approve_host_admin"+suffix
	clean := func() {
		p := store.Pool()
		for _, login := range []string{member, admin} {
			_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
		}
	}
	clean()
	t.Cleanup(clean)
	muid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: member, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	auid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: admin, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.users SET admin = TRUE WHERE user_id = $1`, auid); err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, muid, "approve host probe")
	if err != nil {
		t.Fatal(err)
	}
	// The request pended on a PUBLIC deployment (base ""), before this
	// deployment's Enterprise host was configured.
	out, err := store.AddOrgToGroup(ctx, muid, gid, "https://github.com/_avforeign"+suffix, "")
	if err != nil || out.Registered || out.RequestID == 0 {
		t.Fatalf("pending the request: out=%+v err=%v", out, err)
	}

	s := New(store, config.WebConfig{}, nil, "https://ghe.example.invalid/api/v3", logger)
	s.sessions["admin"] = &Session{UserID: auid, LoginName: admin, IsAdmin: true, ExpiresAt: time.Now().Add(time.Hour)}
	r := httptest.NewRequest(http.MethodPost, "/admin/add-requests/"+strconv.FormatInt(out.RequestID, 10)+"/approve", nil)
	r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "admin"})
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "ghe.example.invalid") {
		t.Fatalf("approving a foreign-host org = %d %q, want 409 naming this deployment's host", w.Code, strings.TrimSpace(w.Body.String()))
	}
	var status string
	if err := store.Pool().QueryRow(ctx, `SELECT status FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`, out.RequestID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Errorf("request status after the refused approval = %q, want pending", status)
	}
}

// TestHandleApproveAddRequestRefusesALegacyCredentialedOrg
// (AVELOXIS_TEST_DB) — Copilot reviews 5267193512/5267408933 and their round
// 2: a pending org request written before the store refused credentialed
// URLs is refused at approval with a 409 that names neither the credential
// nor this deployment's host, and the request stays pending for the reject.
func TestHandleApproveAddRequestRefusesALegacyCredentialedOrg(t *testing.T) {
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
		t.Fatal(err)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	member, admin := "_avweb_approve_cred_member"+suffix, "_avweb_approve_cred_admin"+suffix
	clean := func() {
		p := store.Pool()
		for _, login := range []string{member, admin} {
			_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
		}
	}
	clean()
	t.Cleanup(clean)
	muid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: member, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	auid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: admin, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.users SET admin = TRUE WHERE user_id = $1`, auid); err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, muid, "approve credential probe")
	if err != nil {
		t.Fatal(err)
	}
	// Planted by INSERT: every writer refuses such a URL now.
	var reqID int64
	if err := store.Pool().QueryRow(ctx, `
		INSERT INTO aveloxis_ops.collection_add_requests (user_id, group_id, kind, org_url, status, item_count)
		VALUES ($1, $2, 'org', $3, 'pending', 0) RETURNING request_id`, muid, gid, "https://user:s3cret@github.com/_avweb-cred"+suffix).Scan(&reqID); err != nil {
		t.Fatal(err)
	}
	s := New(store, config.WebConfig{}, nil, "https://ghe.example.invalid/api/v3", logger)
	s.sessions["admin"] = &Session{UserID: auid, LoginName: admin, IsAdmin: true, ExpiresAt: time.Now().Add(time.Hour)}
	r := httptest.NewRequest(http.MethodPost, "/admin/add-requests/"+strconv.FormatInt(reqID, 10)+"/approve", nil)
	r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "admin"})
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	body := w.Body.String()
	if w.Code != http.StatusConflict || !strings.Contains(body, "carries credentials") {
		t.Fatalf("approving a legacy credentialed org = %d %q, want 409 saying the URL carries credentials", w.Code, strings.TrimSpace(body))
	}
	if strings.Contains(body, "s3cret") || strings.Contains(body, "ghe.example.invalid") {
		t.Errorf("the 409 body must name neither the credential nor the host: %q", strings.TrimSpace(body))
	}
	var status string
	if err := store.Pool().QueryRow(ctx, `SELECT status FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`, reqID).Scan(&status); err != nil || status != "pending" {
		t.Errorf("request status after the refused approval = %q, %v; want pending", status, err)
	}
}
