// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/smtp"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailer"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestAdminGroupApprovalMailsTheRequesterOnce (AVELOXIS_TEST_DB): approving a
// pending group from the admin page mails its requester once, through the
// real route; approving it again mails nobody (round-10 review).
func TestAdminGroupApprovalMailsTheRequesterOnce(t *testing.T) {
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
	const login = "_avweb_group_approval_probe"
	clean := func() {
		_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.users SET email = 'requester@example.com' WHERE user_id = $1`, uid); err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "web approval probe")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.user_groups SET status = 'pending' WHERE group_id = $1`, gid); err != nil {
		t.Fatal(err)
	}

	sent := make(chan string, 4)
	m := mailer.New(mailer.Config{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop"}, nil).
		WithSendFunc(func(_ string, _ smtp.Auth, _ string, to []string, _ []byte) error {
			sent <- strings.Join(to, ",")
			return nil
		})
	s := New(store, config.WebConfig{}, nil, logger).WithMailer(m)
	s.sessions["admin"] = &Session{UserID: uid, LoginName: login, IsAdmin: true, ExpiresAt: time.Now().Add(time.Hour)}
	approve := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/admin/groups/"+strconv.FormatInt(gid, 10)+"/approve", nil)
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "admin"})
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}

	if w := approve(); w.Code != http.StatusFound {
		t.Fatalf("approve = %d %s", w.Code, w.Body.String())
	}
	select {
	case to := <-sent:
		if to != "requester@example.com" {
			t.Errorf("the approval email went to %q, want the requester", to)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no approval email within 10s")
	}
	if w := approve(); w.Code != http.StatusFound {
		t.Fatalf("second approve = %d %s", w.Code, w.Body.String())
	}
	select {
	case to := <-sent:
		t.Errorf("approving an already-approved group mailed %q again", to)
	case <-time.After(500 * time.Millisecond):
	}
}

// TestAdminAddRequestReapproveResumesProcessing (AVELOXIS_TEST_DB): through
// the admin route, re-approving a repos request whose first approval never
// finished processing resumes it (round-11 review).
func TestAdminAddRequestReapproveResumesProcessing(t *testing.T) {
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
	const login = "_avweb_reapprove_probe"
	const repoURL = "https://github.com/_avweb-reapprove-owner/_avweb-reapprove-repo"
	clean := func() {
		p := store.Pool()
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%')`, repoURL)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%'`, repoURL)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.users SET email = 'requester@example.com' WHERE user_id = $1`, uid); err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "web reapprove probe")
	if err != nil {
		t.Fatal(err)
	}
	out, err := store.AddReposToGroup(ctx, uid, gid, []string{repoURL}, 0)
	if err != nil || out.RequestID == 0 {
		t.Fatalf("AddReposToGroup = %+v, %v; want a pending request", out, err)
	}
	if _, changed, err := store.DecideAddRequest(ctx, out.RequestID, uid, true); err != nil || !changed {
		t.Fatalf("first approval: changed=%v err=%v", changed, err)
	}
	unprocessed := func() int {
		var n int
		if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.collection_add_request_items WHERE request_id = $1 AND repo_id IS NULL`, out.RequestID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if unprocessed() != 1 {
		t.Fatalf("fixture: %d unprocessed items, want 1", unprocessed())
	}

	sent := make(chan string, 4)
	m := mailer.New(mailer.Config{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop"}, nil).
		WithSendFunc(func(_ string, _ smtp.Auth, _ string, to []string, _ []byte) error {
			sent <- strings.Join(to, ",")
			return nil
		})
	logs := &lockedBuffer{}
	s := New(store, config.WebConfig{}, nil, slog.New(slog.NewTextHandler(logs, nil))).WithMailer(m)
	s.sessions["admin"] = &Session{UserID: uid, LoginName: login, IsAdmin: true, ExpiresAt: time.Now().Add(time.Hour)}
	r := httptest.NewRequest(http.MethodPost, "/admin/add-requests/"+strconv.FormatInt(out.RequestID, 10)+"/approve", nil)
	r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "admin"})
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusFound {
		t.Fatalf("re-approve = %d %s", w.Code, w.Body.String())
	}
	deadline := time.Now().Add(10 * time.Second)
	for unprocessed() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("re-approving did not resume processing within 10s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Resuming is not a new decision: the requester is not mailed again.
	select {
	case to := <-sent:
		t.Errorf("re-approving mailed the decision to %q again", to)
	case <-time.After(500 * time.Millisecond):
	}

	// Approving a REJECTED request starts no processing pass (which would
	// log a false "processing … failed" WARN).
	rejected, err := store.AddReposToGroup(ctx, uid, gid, []string{repoURL + "-rejected"}, 0)
	if err != nil || rejected.RequestID == 0 {
		t.Fatalf("AddReposToGroup = %+v, %v", rejected, err)
	}
	if _, changed, err := store.DecideAddRequest(ctx, rejected.RequestID, uid, false); err != nil || !changed {
		t.Fatalf("reject: changed=%v err=%v", changed, err)
	}
	rr := httptest.NewRequest(http.MethodPost, "/admin/add-requests/"+strconv.FormatInt(rejected.RequestID, 10)+"/approve", nil)
	rr.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "admin"})
	rw := httptest.NewRecorder()
	s.Handler().ServeHTTP(rw, rr)
	if rw.Code != http.StatusFound {
		t.Fatalf("approving a rejected request = %d %s", rw.Code, rw.Body.String())
	}
	time.Sleep(500 * time.Millisecond)
	if strings.Contains(logs.String(), "processing approved add-request failed") {
		t.Errorf("approving a rejected request started a processing pass; log:\n%s", logs.String())
	}
}

// TestAdminOrgReapproveDoesNotRescan (AVELOXIS_TEST_DB): an org approval scans
// the org once; re-approving it (registration present, nothing to complete)
// neither scans again nor mails again (round-12 review: nothing pinned
// "scan only on a real decision"). The group is rejected so the scan stops
// at its logged rejected-group gate, before any network use.
func TestAdminOrgReapproveDoesNotRescan(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	const login = "_avweb_org_rescan_probe"
	const orgURL = "https://github.com/_avweb-org-rescan-probe"
	p := store.Pool()
	clean := func() {
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `UPDATE aveloxis_ops.users SET email = 'requester@example.com' WHERE user_id = $1`, uid); err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "web org rescan probe")
	if err != nil {
		t.Fatal(err)
	}
	out, err := store.AddOrgToGroup(ctx, uid, gid, orgURL)
	if err != nil || out.RequestID == 0 {
		t.Fatalf("AddOrgToGroup = %+v, %v; want a pending request", out, err)
	}
	if _, err := p.Exec(ctx, `UPDATE aveloxis_ops.user_groups SET status = 'rejected' WHERE group_id = $1`, gid); err != nil {
		t.Fatal(err)
	}

	sent := make(chan string, 8)
	m := mailer.New(mailer.Config{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop"}, nil).
		WithSendFunc(func(_ string, _ smtp.Auth, _ string, to []string, _ []byte) error {
			sent <- strings.Join(to, ",")
			return nil
		})
	logs := &lockedBuffer{}
	// A key pool (empty) so scanOrgRepos gets as far as its group gate.
	s := New(store, config.WebConfig{}, platform.NewKeyPool(nil, discard), slog.New(slog.NewTextHandler(logs, nil))).WithMailer(m)
	s.sessions["admin"] = &Session{UserID: uid, LoginName: login, IsAdmin: true, ExpiresAt: time.Now().Add(time.Hour)}
	approve := func() {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/admin/add-requests/"+strconv.FormatInt(out.RequestID, 10)+"/approve", nil)
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "admin"})
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusFound {
			t.Fatalf("approve = %d %s", w.Code, w.Body.String())
		}
	}
	const scanned = "org scan skipped — owning group is rejected"

	approve()
	waitForLog(t, logs, scanned)
	select {
	case <-sent:
	case <-time.After(10 * time.Second):
		t.Fatal("the approval mailed nobody")
	}
	approve()
	select {
	case to := <-sent:
		t.Errorf("re-approving mailed %q again", to)
	case <-time.After(500 * time.Millisecond):
	}
	if n := strings.Count(logs.String(), scanned); n != 1 {
		t.Errorf("the org was scanned %d times, want once; log:\n%s", n, logs.String())
	}
}
