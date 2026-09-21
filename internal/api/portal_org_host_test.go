// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.29.57 round 2 (Copilot 5260961848 fixes): the portal is one of the org
// registration writers, so the store's host gate must reach it — the
// deployment's GitHub base travels through Options.GitHubAPIBase.

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestPortalOrgAddRefusesAnOrgOffTheGitHubHost(t *testing.T) {
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
	const login = "_avapi_org_host_probe"
	pool := store.Pool()
	clean := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
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
	gid, err := store.CreateUserGroup(ctx, uid, "api org host probe")
	if err != nil {
		t.Fatal(err)
	}
	tok, err := store.CreateSessionToken(ctx, uid, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewWithOptions(store, logger, Options{ExemptCIDRs: DefaultExemptCIDRs, GitHubAPIBase: "https://ghe.example.invalid/api/v3"})
	if err != nil {
		t.Fatal(err)
	}
	post := func(orgURL string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/api/v1/groups/"+strconv.FormatInt(gid, 10)+"/repos",
			strings.NewReader(`{"urls":["`+orgURL+`"],"kind":"org"}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer "+tok)
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, r)
		return w
	}
	requests := func() int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.collection_add_requests WHERE user_id = $1`, uid).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	w := post("https://github.com/acme")
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "GitHub host") {
		t.Fatalf("foreign-host org through the portal = %d %q, want 400 naming the host", w.Code, strings.TrimSpace(w.Body.String()))
	}
	if n := requests(); n != 0 {
		t.Fatalf("foreign-host org pended %d add-requests, want 0", n)
	}
	w = post("https://ghe.example.invalid/acme")
	if w.Code != http.StatusOK {
		t.Fatalf("own-host org through the portal = %d %q, want 200", w.Code, strings.TrimSpace(w.Body.String()))
	}
	if n := requests(); n != 1 {
		t.Errorf("own-host org by a non-admin pended %d add-requests, want 1", n)
	}
}

// The portal's admin decision reports the same refusal as a 409 (round 3:
// the arm existed without a test that fired it).
func TestPortalAdminApproveReportsAForeignHostOrgAsConflict(t *testing.T) {
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
	member, admin := "_avapi_approve_host_member"+suffix, "_avapi_approve_host_admin"+suffix
	pool := store.Pool()
	clean := func() {
		for _, login := range []string{member, admin} {
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
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
	if _, err := pool.Exec(ctx, `UPDATE aveloxis_ops.users SET admin = TRUE WHERE user_id = $1`, auid); err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, muid, "api approve host probe")
	if err != nil {
		t.Fatal(err)
	}
	out, err := store.AddOrgToGroup(ctx, muid, gid, "https://github.com/_avapiforeign"+suffix, "")
	if err != nil || out.Registered || out.RequestID == 0 {
		t.Fatalf("pending the request: out=%+v err=%v", out, err)
	}
	srv, err := NewWithOptions(store, logger, Options{ExemptCIDRs: DefaultExemptCIDRs, GitHubAPIBase: "https://ghe.example.invalid/api/v3"})
	if err != nil {
		t.Fatal(err)
	}
	id := strconv.FormatInt(out.RequestID, 10)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/admin/add-requests/"+id+"/approve", nil)
	r.SetPathValue("requestID", id)
	r.SetPathValue("decision", "approve")
	r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: auid, IsAdmin: true}))
	w := httptest.NewRecorder()
	srv.handleAdminAddRequestDecision(w, r)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "ghe.example.invalid") {
		t.Fatalf("approving a foreign-host org through the portal = %d %q, want 409 naming this deployment's host", w.Code, strings.TrimSpace(w.Body.String()))
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`, out.RequestID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Errorf("request status after the refused approval = %q, want pending", status)
	}
}

// The portal's admin decision refuses a legacy credentialed org request the
// same way: 409, no credential and no host in the body, status still pending
// (Copilot reviews 5267193512/5267408933, round 2).
func TestPortalAdminApproveRefusesALegacyCredentialedOrg(t *testing.T) {
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
	member, admin := "_avapi_approve_cred_member"+suffix, "_avapi_approve_cred_admin"+suffix
	pool := store.Pool()
	clean := func() {
		for _, login := range []string{member, admin} {
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
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
	if _, err := pool.Exec(ctx, `UPDATE aveloxis_ops.users SET admin = TRUE WHERE user_id = $1`, auid); err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, muid, "api approve credential probe")
	if err != nil {
		t.Fatal(err)
	}
	var reqID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.collection_add_requests (user_id, group_id, kind, org_url, status, item_count)
		VALUES ($1, $2, 'org', $3, 'pending', 0) RETURNING request_id`, muid, gid, "https://user:s3cret@github.com/_avapi-cred"+suffix).Scan(&reqID); err != nil {
		t.Fatal(err)
	}
	srv, err := NewWithOptions(store, logger, Options{ExemptCIDRs: DefaultExemptCIDRs, GitHubAPIBase: "https://ghe.example.invalid/api/v3"})
	if err != nil {
		t.Fatal(err)
	}
	id := strconv.FormatInt(reqID, 10)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/admin/add-requests/"+id+"/approve", nil)
	r.SetPathValue("requestID", id)
	r.SetPathValue("decision", "approve")
	r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: auid, IsAdmin: true}))
	w := httptest.NewRecorder()
	srv.handleAdminAddRequestDecision(w, r)
	body := w.Body.String()
	if w.Code != http.StatusConflict || !strings.Contains(body, "carries credentials") {
		t.Fatalf("approving a legacy credentialed org through the portal = %d %q, want 409 saying the URL carries credentials", w.Code, strings.TrimSpace(body))
	}
	if strings.Contains(body, "s3cret") || strings.Contains(body, "ghe.example.invalid") {
		t.Errorf("the 409 body must name neither the credential nor the host: %q", strings.TrimSpace(body))
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`, reqID).Scan(&status); err != nil || status != "pending" {
		t.Errorf("request status after the refused approval = %q, %v; want pending", status, err)
	}
}
