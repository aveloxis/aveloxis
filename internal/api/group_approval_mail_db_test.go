// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"fmt"
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

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailer"
)

// TestAdminGroupApprovalMailsTheRequesterOnce (AVELOXIS_TEST_DB): approving a
// pending group through the API mails its requester once; approving it again
// mails nobody; a store failure is a logged 500 (round-10 review: the
// requester lookup's error was discarded, so a failed lookup approved the
// group and silently mailed no one).
func TestAdminGroupApprovalMailsTheRequesterOnce(t *testing.T) {
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
	const login = "_avapi_group_approval_probe"
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
	gid, err := store.CreateUserGroup(ctx, uid, "api approval probe")
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
	logs := &lockedBuffer{}
	decide := func(st *db.PostgresStore, decision string) *httptest.ResponseRecorder {
		s := &Server{store: st, logger: slog.New(slog.NewTextHandler(logs, nil)), mailer: m, auth: newAuthenticator(st, false)}
		r := httptest.NewRequest(http.MethodPost, "/api/v1/admin/groups/"+strconv.FormatInt(gid, 10)+"/"+decision, nil)
		r.SetPathValue("groupID", strconv.FormatInt(gid, 10))
		r.SetPathValue("decision", decision)
		r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: uid, IsAdmin: true}))
		w := httptest.NewRecorder()
		s.handleAdminGroupDecision(w, r)
		return w
	}

	if w := decide(store, "approve"); w.Code != http.StatusOK {
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
	// A second approval changes nothing, so it mails nobody. (Bounded wait:
	// the send runs in its own goroutine.)
	if w := decide(store, "approve"); w.Code != http.StatusOK {
		t.Fatalf("second approve = %d %s", w.Code, w.Body.String())
	}
	select {
	case to := <-sent:
		t.Errorf("approving an already-approved group mailed %q again", to)
	case <-time.After(500 * time.Millisecond):
	}

	closed, err := db.NewPostgresStore(ctx, dsn, discard)
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	for _, decision := range []string{"approve", "reject"} {
		if w := decide(closed, decision); w.Code != http.StatusInternalServerError {
			t.Errorf("%s on a failing store = %d, want 500", decision, w.Code)
		}
		if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "decision="+decision) || !strings.Contains(logs.String(), "closed pool") {
			t.Errorf("a failed group %s must be logged at WARN with its error; log:\n%s", decision, logs.String())
		}
	}
}

// TestAdminAddRequestDecisionThroughTheHandler (AVELOXIS_TEST_DB): a failed
// add-request decision is a logged 500, and re-approving a repos request
// whose first approval never finished processing resumes it, as the API
// docs and the processing WARN promise (round-11 review: the decision
// failure was a bare 500, and re-approving did nothing).
func TestAdminAddRequestDecisionThroughTheHandler(t *testing.T) {
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
	const login = "_avapi_reapprove_probe"
	const repoURL = "https://github.com/_avapi-reapprove-owner/_avapi-reapprove-repo"
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
	gid, err := store.CreateUserGroup(ctx, uid, "api reapprove probe")
	if err != nil {
		t.Fatal(err)
	}
	out, err := store.AddReposToGroup(ctx, uid, gid, []string{repoURL}, 0)
	if err != nil || out.RequestID == 0 {
		t.Fatalf("AddReposToGroup = %+v, %v; want a pending request", out, err)
	}
	reqID := strconv.FormatInt(out.RequestID, 10)

	logs := &lockedBuffer{}
	sent := make(chan string, 4)
	m := mailer.New(mailer.Config{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop"}, nil).
		WithSendFunc(func(_ string, _ smtp.Auth, _ string, to []string, _ []byte) error {
			sent <- strings.Join(to, ",")
			return nil
		})
	decide := func(st *db.PostgresStore, decision string) *httptest.ResponseRecorder {
		s := &Server{store: st, logger: slog.New(slog.NewTextHandler(logs, nil)), mailer: m, auth: newAuthenticator(st, false)}
		r := httptest.NewRequest(http.MethodPost, "/api/v1/admin/add-requests/"+reqID+"/"+decision, nil)
		r.SetPathValue("requestID", reqID)
		r.SetPathValue("decision", decision)
		r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: uid, IsAdmin: true}))
		w := httptest.NewRecorder()
		s.handleAdminAddRequestDecision(w, r)
		return w
	}

	// The first approval flips the request but its processing pass never
	// runs (the process stopped): the item stays unprocessed.
	if _, changed, err := store.DecideAddRequest(ctx, out.RequestID, uid, true, ""); err != nil || !changed {
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
	w := decide(store, "approve")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"changed":false`) {
		t.Fatalf("re-approve = %d %s, want 200 with changed=false", w.Code, w.Body.String())
	}
	deadline := time.Now().Add(10 * time.Second)
	for unprocessed() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("re-approving did not resume processing within 10s; log:\n%s", logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Resuming is not a new decision: the requester is not mailed again.
	select {
	case to := <-sent:
		t.Errorf("re-approving mailed the decision to %q again", to)
	case <-time.After(500 * time.Millisecond):
	}

	// Approving a REJECTED repos request starts no processing pass (which
	// would log a false "processing … failed" WARN) and registers no org.
	rejected, err := store.AddReposToGroup(ctx, uid, gid, []string{repoURL + "-rejected"}, 0)
	if err != nil || rejected.RequestID == 0 {
		t.Fatalf("AddReposToGroup = %+v, %v", rejected, err)
	}
	if _, changed, err := store.DecideAddRequest(ctx, rejected.RequestID, uid, false, ""); err != nil || !changed {
		t.Fatalf("reject: changed=%v err=%v", changed, err)
	}
	s := &Server{store: store, logger: slog.New(slog.NewTextHandler(logs, nil)), mailer: m, auth: newAuthenticator(store, false)}
	rr := httptest.NewRequest(http.MethodPost, "/api/v1/admin/add-requests/x/approve", nil)
	rr.SetPathValue("requestID", strconv.FormatInt(rejected.RequestID, 10))
	rr.SetPathValue("decision", "approve")
	rr = rr.WithContext(context.WithValue(rr.Context(), authCtxKey{}, authInfo{UserID: uid, IsAdmin: true}))
	rw := httptest.NewRecorder()
	s.handleAdminAddRequestDecision(rw, rr)
	if rw.Code != http.StatusOK || !strings.Contains(rw.Body.String(), `"changed":false`) {
		t.Fatalf("approving a rejected request = %d %s, want 200 changed=false", rw.Code, rw.Body.String())
	}
	time.Sleep(500 * time.Millisecond)
	if strings.Contains(logs.String(), "processing approved add-request failed") {
		t.Errorf("approving a rejected request started a processing pass; log:\n%s", logs.String())
	}
	var orgRows int
	_ = store.Pool().QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, gid).Scan(&orgRows)
	if orgRows != 0 {
		t.Errorf("re-approving repos requests registered %d orgs, want 0", orgRows)
	}

	closed, err := db.NewPostgresStore(ctx, dsn, discard)
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	for _, decision := range []string{"approve", "reject"} {
		if w := decide(closed, decision); w.Code != http.StatusInternalServerError {
			t.Errorf("%s on a failing store = %d, want 500", decision, w.Code)
		}
		if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "decision="+decision) || !strings.Contains(logs.String(), "closed pool") {
			t.Errorf("a failed add-request %s must be logged at WARN with its error; log:\n%s", decision, logs.String())
		}
	}
}

// TestAdminOrgApprovalNotifiesWhenItRegisters (AVELOXIS_TEST_DB): an org
// approval whose registration fails is a 500 that approves nothing and mails
// nobody, so the admin's retry approves and notifies normally; an approved
// request left without its registration (by a release before v0.29.39) is completed by
// re-approving it, and the requester is notified then (round-12 review: the
// recovery registered the org and never told the requester).
func TestAdminOrgApprovalNotifiesWhenItRegisters(t *testing.T) {
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
	const login = "_avapi_org_notify_probe"
	const orgURL = "https://github.com/_avapi-org-notify-probe"
	p := store.Pool()
	dropTrigger := func() {
		_, _ = p.Exec(ctx, `DROP TRIGGER IF EXISTS _avapi_fail_org ON aveloxis_ops.user_org_requests`)
		_, _ = p.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_ops._avapi_fail_org()`)
	}
	clean := func() {
		dropTrigger()
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
	gid, err := store.CreateUserGroup(ctx, uid, "api org notify probe")
	if err != nil {
		t.Fatal(err)
	}
	out, err := store.AddOrgToGroup(ctx, uid, gid, orgURL, "")
	if err != nil || out.RequestID == 0 {
		t.Fatalf("AddOrgToGroup = %+v, %v; want a pending request", out, err)
	}

	sent := make(chan string, 8)
	m := mailer.New(mailer.Config{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop"}, nil).
		WithSendFunc(func(_ string, _ smtp.Auth, _ string, to []string, _ []byte) error {
			sent <- strings.Join(to, ",")
			return nil
		})
	s := &Server{store: store, logger: discard, mailer: m, auth: newAuthenticator(store, false)}
	approve := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/admin/add-requests/x/approve", nil)
		r.SetPathValue("requestID", strconv.FormatInt(out.RequestID, 10))
		r.SetPathValue("decision", "approve")
		r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: uid, IsAdmin: true}))
		w := httptest.NewRecorder()
		s.handleAdminAddRequestDecision(w, r)
		return w
	}
	// mails waits for exactly want emails, then briefly for an extra one.
	mails := func(want int) {
		t.Helper()
		for i := 0; i < want; i++ {
			select {
			case <-sent:
			case <-time.After(10 * time.Second):
				t.Fatalf("mailed %d times, want %d", i, want)
			}
		}
		select {
		case to := <-sent:
			t.Errorf("mailed %q beyond the %d expected", to, want)
		case <-time.After(500 * time.Millisecond):
		}
	}
	status := func() string {
		var st string
		_ = p.QueryRow(ctx, `SELECT status FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`, out.RequestID).Scan(&st)
		return st
	}

	if _, err := p.Exec(ctx, `CREATE FUNCTION aveloxis_ops._avapi_fail_org() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN IF NEW.org_url = '`+orgURL+`' THEN RAISE EXCEPTION 'injected registration failure'; END IF; RETURN NEW; END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `CREATE TRIGGER _avapi_fail_org BEFORE INSERT ON aveloxis_ops.user_org_requests FOR EACH ROW EXECUTE FUNCTION aveloxis_ops._avapi_fail_org()`); err != nil {
		t.Fatal(err)
	}
	if w := approve(); w.Code != http.StatusInternalServerError || status() != "pending" {
		t.Errorf("approval with a failing registration = %d, request %q; want 500 and still pending", w.Code, status())
	}
	mails(0)
	dropTrigger()
	if w := approve(); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"changed":true`) {
		t.Errorf("retry = %d %s, want 200 changed=true", w.Code, w.Body.String())
	}
	mails(1)

	// The pre-v0.29.38 half-state: approved, registration missing.
	if _, err := p.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, gid); err != nil {
		t.Fatal(err)
	}
	if w := approve(); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"changed":true`) {
		t.Errorf("re-approve completing the registration = %d %s, want 200 changed=true", w.Code, w.Body.String())
	}
	mails(1)
	if w := approve(); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"changed":false`) {
		t.Errorf("re-approve with the registration present = %d %s, want 200 changed=false", w.Code, w.Body.String())
	}
	mails(0)
}

// TestAdminAddRequestProcessingInvalidatesTheAuthCache (AVELOXIS_TEST_DB): the
// requester's repo scope changes as the background pass links repos, so the
// token cache is dropped again when that pass ends — after a first approval
// and after a resume — not only before it starts (Copilot review of PR #207,
// suppressed comment: a request during processing cached the old scope for
// the full TTL, and a resume never invalidated at all).
func TestAdminAddRequestProcessingInvalidatesTheAuthCache(t *testing.T) {
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
	const login = "_avapi_scope_cache_probe"
	const repoURL = "https://github.com/_avapi-scope-cache-owner/_avapi-scope-cache-repo"
	const trigger = "_avapi_slow_user_repos"
	p := store.Pool()
	clean := func() {
		_, _ = p.Exec(ctx, `DROP TRIGGER IF EXISTS `+trigger+` ON aveloxis_ops.user_repos`)
		_, _ = p.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_ops.`+trigger+`()`)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%')`, repoURL)
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%'`, repoURL)
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
	gid, err := store.CreateUserGroup(ctx, uid, "api scope cache probe")
	if err != nil {
		t.Fatal(err)
	}
	// Linking into the group sleeps, so the pass is still running after the
	// handler returns.
	if _, err := p.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION aveloxis_ops.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN IF NEW.group_id = %d THEN PERFORM pg_sleep(0.5); END IF; RETURN NEW; END $f$`, gid)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `CREATE TRIGGER `+trigger+` BEFORE INSERT ON aveloxis_ops.user_repos FOR EACH ROW EXECUTE FUNCTION aveloxis_ops.`+trigger+`()`); err != nil {
		t.Fatal(err)
	}

	s := &Server{store: store, logger: discard, auth: newAuthenticator(store, false)}
	unprocessed := func(reqID int64) int {
		t.Helper()
		var n int
		if err := p.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.collection_add_request_items WHERE request_id = $1 AND repo_id IS NULL`, reqID).Scan(&n); err != nil {
			t.Fatalf("count unprocessed items: %v", err)
		}
		return n
	}
	cached := func() int {
		s.auth.mu.Lock()
		defer s.auth.mu.Unlock()
		return len(s.auth.cache)
	}
	approveAndWatch := func(t *testing.T, reqID int64, wantChanged string) {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/api/v1/admin/add-requests/x/approve", nil)
		r.SetPathValue("requestID", strconv.FormatInt(reqID, 10))
		r.SetPathValue("decision", "approve")
		r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: uid, IsAdmin: true}))
		w := httptest.NewRecorder()
		s.handleAdminAddRequestDecision(w, r)
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), wantChanged) {
			t.Fatalf("approve = %d %s, want 200 with %s", w.Code, w.Body.String(), wantChanged)
		}
		// A request that resolves its token while the pass runs caches the
		// old scope. Wait until the pass is really mid-item (a backend in the
		// trigger's pg_sleep), so a cache drop at the START of the pass
		// cannot satisfy this test (round-21 review).
		sleeping := time.Now().Add(10 * time.Second)
		for {
			var n int
			if err := p.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event = 'PgSleep' AND query ILIKE '%user_repos%'`).Scan(&n); err != nil {
				t.Fatalf("watch for the pass: %v", err)
			}
			if n > 0 {
				break
			}
			if time.Now().After(sleeping) {
				t.Fatal("the pass never reached its slow step")
			}
			time.Sleep(10 * time.Millisecond)
		}
		s.auth.mu.Lock()
		s.auth.cache["probe-token"] = cachedAuth{info: authInfo{UserID: uid}, expires: time.Now().Add(authCacheTTL)}
		s.auth.mu.Unlock()
		if unprocessed(reqID) == 0 {
			t.Fatal("the pass finished before the cached entry was added; the probe did not exercise the window")
		}
		deadline := time.Now().Add(10 * time.Second)
		for unprocessed(reqID) != 0 || cached() != 0 {
			if time.Now().After(deadline) {
				t.Fatalf("10s after approving: %d items unprocessed, %d cached tokens; want the pass done and the cache dropped", unprocessed(reqID), cached())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	out, err := store.AddReposToGroup(ctx, uid, gid, []string{repoURL}, 0)
	if err != nil || out.RequestID == 0 {
		t.Fatalf("AddReposToGroup = %+v, %v; want a pending request", out, err)
	}
	t.Run("first approval", func(t *testing.T) { approveAndWatch(t, out.RequestID, `"changed":true`) })

	resumed, err := store.AddReposToGroup(ctx, uid, gid, []string{repoURL + "-resume"}, 0)
	if err != nil || resumed.RequestID == 0 {
		t.Fatalf("AddReposToGroup = %+v, %v; want a pending request", resumed, err)
	}
	if _, changed, err := store.DecideAddRequest(ctx, resumed.RequestID, uid, true, ""); err != nil || !changed {
		t.Fatalf("first approval (processing never ran): changed=%v err=%v", changed, err)
	}
	t.Run("resume", func(t *testing.T) { approveAndWatch(t, resumed.RequestID, `"changed":false`) })
}
