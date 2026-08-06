// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// shared_with_me_authz_test.go — TDD suite for the v0.27.82 shared-
// link auto-add in authorizeRepo. Operator decision (2026-08-04): a
// signed-in user opening a link to an out-of-scope EXISTING repo gets
// it auto-added to their implicit "Shared with Me" group and the
// request proceeds — links are shareable. The gate stays fail-closed
// for everything else: nonexistent repos, db errors, and servers with
// no store seam keep the structured 403.

import (
	"context"
	"errors"
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

type fakeSharedWithMe struct {
	added bool
	err   error
	calls []struct {
		userID int
		repoID int64
	}
}

func (f *fakeSharedWithMe) EnsureRepoSharedWithUser(_ context.Context, userID int, repoID int64) (bool, error) {
	f.calls = append(f.calls, struct {
		userID int
		repoID int64
	}{userID, repoID})
	return f.added, f.err
}

func autoAddServer(fake *fakeSharedWithMe) *Server {
	return &Server{
		sharedWithMe: fake,
		auth:         newAuthenticator(nil, false),
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func scopedReq(userID int, scope map[int64]bool) *http.Request {
	base := httptest.NewRequest("GET", "/api/v1/repos/99/stats", nil)
	ctx := context.WithValue(base.Context(), authCtxKey{}, authInfo{UserID: userID, Scope: scope})
	return base.WithContext(ctx)
}

// TestAuthorizeRepoAutoAddAllowsAndNotifies: the core flow — a fresh
// share is allowed AND carries the one-time notice header (with the
// CORS expose header so a cross-origin GUI can read it).
func TestAuthorizeRepoAutoAddAllowsAndNotifies(t *testing.T) {
	fake := &fakeSharedWithMe{added: true}
	s := autoAddServer(fake)
	rec := httptest.NewRecorder()
	if !s.authorizeRepo(rec, scopedReq(7, map[int64]bool{42: true}), 99) {
		t.Fatal("out-of-scope existing repo must be ALLOWED via the Shared with Me auto-add")
	}
	if len(fake.calls) != 1 || fake.calls[0].userID != 7 || fake.calls[0].repoID != 99 {
		t.Errorf("expected one EnsureRepoSharedWithUser(7, 99) call, got %+v", fake.calls)
	}
	if got := rec.Header().Get(sharedWithMeHeader); got != db.SharedWithMeGroupName {
		t.Errorf("fresh share must carry the notice header %s=%q, got %q",
			sharedWithMeHeader, db.SharedWithMeGroupName, got)
	}
	if !strings.Contains(rec.Header().Get("Access-Control-Expose-Headers"), sharedWithMeHeader) {
		t.Error("notice header must be CORS-exposed so a cross-origin GUI can toast it")
	}
}

// TestAuthorizeRepoAutoAddAlreadyLinkedNoNotice: within the auth-cache
// TTL after a share, the scope map is stale but the link exists —
// allowed, no repeated notice.
func TestAuthorizeRepoAutoAddAlreadyLinkedNoNotice(t *testing.T) {
	fake := &fakeSharedWithMe{added: false}
	s := autoAddServer(fake)
	rec := httptest.NewRecorder()
	if !s.authorizeRepo(rec, scopedReq(7, nil), 99) {
		t.Fatal("already-linked repo must be allowed")
	}
	if got := rec.Header().Get(sharedWithMeHeader); got != "" {
		t.Errorf("already-linked share must NOT repeat the notice header, got %q", got)
	}
}

// TestAuthorizeRepoAutoAddFailClosed: nonexistent repos and db errors
// both keep the structured 403 — the auto-add can only ever widen
// access for repos that exist.
func TestAuthorizeRepoAutoAddFailClosed(t *testing.T) {
	for name, fake := range map[string]*fakeSharedWithMe{
		"nonexistent repo": {err: db.ErrSharedRepoNotFound},
		"db error":         {err: errors.New("pool exhausted")},
	} {
		rec := httptest.NewRecorder()
		s := autoAddServer(fake)
		if s.authorizeRepo(rec, scopedReq(7, nil), 99) {
			t.Errorf("%s: must fail closed", name)
			continue
		}
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s: expected 403, got %d", name, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "repo_out_of_scope") {
			t.Errorf("%s: 403 must keep the structured shape, got %q", name, rec.Body.String())
		}
	}
}

// TestAuthorizeRepoAutoAddSkipsAdminAndInScope: the auto-add fires
// ONLY on the out-of-scope branch — admins and in-scope users must
// never accumulate a Shared with Me group.
func TestAuthorizeRepoAutoAddSkipsAdminAndInScope(t *testing.T) {
	fake := &fakeSharedWithMe{added: true}
	s := autoAddServer(fake)

	base := httptest.NewRequest("GET", "/api/v1/repos/99/stats", nil)
	adminCtx := context.WithValue(base.Context(), authCtxKey{}, authInfo{UserID: 1, IsAdmin: true})
	if !s.authorizeRepo(httptest.NewRecorder(), base.WithContext(adminCtx), 99) {
		t.Fatal("admin must be allowed")
	}
	if !s.authorizeRepo(httptest.NewRecorder(), scopedReq(7, map[int64]bool{99: true}), 99) {
		t.Fatal("in-scope must be allowed")
	}
	if len(fake.calls) != 0 {
		t.Errorf("admin/in-scope paths must never call the auto-add, got %+v", fake.calls)
	}
}

// TestAuthorizeRepoAutoAddSourceContract pins the wiring: cache
// invalidation on a fresh share (auth + per-user home cache), the
// nil-seam guard, and the negative tripwire that auth.go never grows
// collection machinery.
func TestAuthorizeRepoAutoAddSourceContract(t *testing.T) {
	src := mustReadFile(t, "auth.go")
	body := extractFuncBody(t, src, "authorizeRepo")
	for _, needle := range []string{
		"EnsureRepoSharedWithUser(",
		"invalidateAll()",
		"homeCache.invalidate(",
		"sharedWithMe != nil",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("authorizeRepo must contain %q", needle)
		}
	}
	stripped := stripLineComments(src)
	for _, forbidden := range []string{"EnqueueRepo", "AddOrgToGroup", "user_org_requests", "collection_add_requests"} {
		if strings.Contains(stripped, forbidden) {
			t.Errorf("auth.go must not reference %q — the shared-link flow links existing repos only", forbidden)
		}
	}
}

// TestSharedLinkEndToEnd is the full-stack proof: HTTP → Bearer auth →
// authorizeRepo → auto-add → data handler, against the real schema.
func TestSharedLinkEndToEnd(t *testing.T) {
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
	pool := store.Pool()
	suffix := time.Now().UnixNano()

	// Tracked out-of-scope repo WITH a live queue row (so the
	// row-unchanged invariant is provable end-to-end).
	var repoID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, $2, 'shared', 1) RETURNING repo_id`,
		fmt.Sprintf("https://github.com/_avlink%d/shared", suffix),
		fmt.Sprintf("_avlink%d", suffix)).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, status, priority, due_at)
		VALUES ($1, 'queued', 100, NOW())`, repoID); err != nil {
		t.Fatal(err)
	}

	seedUser := func(tag string, admin bool) (int, string) {
		t.Helper()
		var id int
		if err := pool.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
			VALUES ($1, 'github', '', $2) RETURNING user_id`,
			fmt.Sprintf("_avlink_%s_%d", tag, suffix), admin).Scan(&id); err != nil {
			t.Fatal(err)
		}
		tok, err := store.CreateSessionToken(ctx, id, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		return id, tok
	}
	userID, tok := seedUser("user", false)
	adminID, adminTok := seedUser("admin", true)

	t.Cleanup(func() {
		for _, id := range []int{userID, adminID} {
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, id)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = $1)`, id)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, id)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, id)
		}
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	var queueBefore string
	if err := pool.QueryRow(ctx, `
		SELECT row_to_json(q)::text FROM aveloxis_ops.collection_queue q WHERE repo_id = $1`,
		repoID).Scan(&queueBefore); err != nil {
		t.Fatal(err)
	}

	// RequireAuth ON; exempt CIDRs deliberately exclude loopback so
	// the anonymous probe genuinely exercises the 401 path.
	srv, err := NewWithOptions(store, logger, Options{
		RequireAuth:    true,
		ExemptCIDRs:    []string{"198.51.100.0/24"},
		RateLimitRPS:   100,
		RateLimitBurst: 100,
		RateLimitDaily: 10000,
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := srv.Handler()

	get := func(path, bearer string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest("GET", path, nil)
		req.RemoteAddr = "203.0.113.9:1"
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	statsPath := fmt.Sprintf("/api/v1/repos/%d/stats", repoID)

	// (1) Anonymous: 401 — sharing never opens anything to the
	// unauthenticated.
	if rec := get(statsPath, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous must stay 401, got %d", rec.Code)
	}

	// (2) Signed-in, out-of-scope: 200 + notice header + link created.
	rec := get(statsPath, tok)
	if rec.Code != http.StatusOK {
		t.Fatalf("shared link must work for a signed-in user, got %d (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(sharedWithMeHeader); got != db.SharedWithMeGroupName {
		t.Errorf("first visit must carry %s=%q, got %q", sharedWithMeHeader, db.SharedWithMeGroupName, got)
	}
	var links int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_ops.user_repos ur
		JOIN aveloxis_ops.user_groups g USING (group_id)
		WHERE g.user_id = $1 AND g.name = $2 AND ur.repo_id = $3`,
		userID, db.SharedWithMeGroupName, repoID).Scan(&links); err != nil {
		t.Fatal(err)
	}
	if links != 1 {
		t.Errorf("expected 1 Shared with Me link, got %d", links)
	}

	// (3) Second visit: still 200, no repeated notice (scope was
	// re-resolved after invalidation, so this rides the in-scope path).
	rec = get(statsPath, tok)
	if rec.Code != http.StatusOK {
		t.Fatalf("second visit must succeed, got %d", rec.Code)
	}
	if got := rec.Header().Get(sharedWithMeHeader); got != "" {
		t.Errorf("second visit must not repeat the notice header, got %q", got)
	}

	// (4) SAFETY: queue row byte-identical after everything.
	var queueAfter string
	if err := pool.QueryRow(ctx, `
		SELECT row_to_json(q)::text FROM aveloxis_ops.collection_queue q WHERE repo_id = $1`,
		repoID).Scan(&queueAfter); err != nil {
		t.Fatal(err)
	}
	if queueAfter != queueBefore {
		t.Errorf("shared-link flow must NEVER touch collection_queue.\nbefore: %s\nafter:  %s", queueBefore, queueAfter)
	}

	// (5) Nonexistent repo: structured 403 unchanged, no group residue.
	var maxRepoID int64
	_ = pool.QueryRow(ctx, `SELECT COALESCE(MAX(repo_id),0) FROM aveloxis_data.repos`).Scan(&maxRepoID)
	rec = get(fmt.Sprintf("/api/v1/repos/%d/stats", maxRepoID+1000000), tok)
	if rec.Code != http.StatusForbidden {
		t.Errorf("nonexistent repo must stay a structured 403, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "repo_out_of_scope") {
		t.Errorf("403 shape must be preserved, got %q", rec.Body.String())
	}

	// (6) Admin: allowed without any group creation.
	if rec := get(statsPath, adminTok); rec.Code != http.StatusOK {
		t.Fatalf("admin must be allowed, got %d", rec.Code)
	}
	var adminGroups int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_ops.user_groups WHERE user_id = $1`, adminID).Scan(&adminGroups); err != nil {
		t.Fatal(err)
	}
	if adminGroups != 0 {
		t.Errorf("admins are unscoped — no Shared with Me group may be created for them, got %d groups", adminGroups)
	}
}
