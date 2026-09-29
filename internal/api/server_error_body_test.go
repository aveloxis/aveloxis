// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestServerErrorsNeverEchoTheirCause is the ratchet for worklist follow-up
// 4: API handlers wrote the store's error text into a 500 body. A
// server-side failure is a logged 500 with a generic body (serverError);
// the client cannot act on database text, and it can leak schema and hosts.
func TestServerErrorsNeverEchoTheirCause(t *testing.T) {
	examined := 0
	for name, src := range srctest.PackageFiles(t, "internal/api", 5) {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		lines := strings.Split(srctest.StripGoComments(src), "\n")
		for i, line := range lines {
			if strings.Contains(line, "http.StatusInternalServerError") {
				examined++
				if strings.Contains(line, "err.Error()") && !sentinelCountsOnly(lines, i) {
					t.Errorf("%s:%d writes the error's text into a 500 body: use s.serverError (logged, generic body)", name, i+1)
				}
			}
		}
	}
	srctest.MinCount(t, "500 responses", examined, 10)
}

// sentinelCountsOnly is the one reviewed exception: db.ErrAddItemsFailed's
// text is counts ("N of M"), built by the store with no database text, and
// the client acts on it (retry the rest). The arm's case line names it.
func sentinelCountsOnly(lines []string, i int) bool {
	for j := i - 1; j >= 0 && j >= i-3; j-- {
		if strings.Contains(lines[j], "db.ErrAddItemsFailed") {
			return true
		}
	}
	return false
}

// servedStore opens a second store on dsn for the server under test, so a
// test can close the server's pool to fault its handlers while the
// fixture's own store stays open for the cleanups (PR #218 review C11:
// closing the fixture store in the body left every t.Cleanup delete to run
// on a closed pool, so the probe rows were never removed). Close is
// idempotent, so the registered Close after the body's is harmless.
func servedStore(t *testing.T, dsn string) *db.PostgresStore {
	t.Helper()
	served, err := db.NewPostgresStore(context.Background(), dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(served.Close)
	return served
}

// TestServerErrorBodyIsGeneric drives one handler into a store failure with
// the DB tier: the token is resolved while the store works (cached), then
// the pool is closed, so the handler's own query fails. The body is the
// generic message, the cause goes to the log.
func TestServerErrorBodyIsGeneric(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	const login = "_avapi_500_probe"
	pool := store.Pool()
	clean := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	token, err := store.CreateSessionToken(ctx, uid, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var logs strings.Builder
	s := New(servedStore(t, dsn), slog.New(slog.NewTextHandler(&logs, nil)))
	get := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/groups", nil)
		req.RemoteAddr = "203.0.113.5:1"
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		return rec
	}
	if rec := get(); rec.Code != http.StatusOK {
		t.Fatalf("priming GET /api/v1/groups = %d %s", rec.Code, rec.Body.String())
	}
	s.store.Close() // the token stays cached; the handler's query now fails
	rec := get()
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("GET /api/v1/groups over a closed pool = %d %s; want 500", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, "closed pool") || strings.Contains(body, "pgx") || strings.Contains(body, "aveloxis_ops") {
		t.Errorf("the 500 body carries the store's error text: %q", body)
	}
	if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "closed pool") {
		t.Errorf("the failure's cause is not in the log:\n%s", logs.String())
	}
}

// TestGroupAddRepoBustsTheTokenCache pins the first gap of worklist
// follow-up 7: the portal's add linked repositories into the caller's group
// without dropping the token cache, so the caller's scope stayed stale for
// the TTL and the newly linked repository answered 403.
func TestGroupAddRepoBustsTheTokenCache(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	const login = "_avapi_bust_probe"
	const repoURL = "https://github.com/_avapi-bust-owner/_avapi-bust-repo"
	pool := store.Pool()
	clean := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1)`, repoURL)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1)`, repoURL)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git = $1`, repoURL)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "api bust probe")
	if err != nil {
		t.Fatal(err)
	}
	// A tracked repository (a queue row exists): a non-admin's add links it.
	rid, err := store.UpsertRepo(ctx, &model.Repo{GitURL: repoURL, Owner: "_avapi-bust-owner", Name: "_avapi-bust-repo", Platform: model.PlatformGitHub})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueRepo(ctx, rid, 100); err != nil {
		t.Fatal(err)
	}
	token, err := store.CreateSessionToken(ctx, uid, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	s := New(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	do := func(method, path string, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.RemoteAddr = "203.0.113.5:1"
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		return rec
	}
	if rec := do(http.MethodGet, "/api/v1/groups", ""); rec.Code != http.StatusOK {
		t.Fatalf("priming GET = %d", rec.Code)
	}
	cached := func() int { s.auth.mu.Lock(); defer s.auth.mu.Unlock(); return len(s.auth.cache) }
	if cached() != 1 {
		t.Fatalf("%d cached tokens before the add; want 1", cached())
	}
	payload, _ := json.Marshal(map[string]any{"urls": []string{repoURL}, "kind": "repos"})
	rec := do(http.MethodPost, "/api/v1/groups/"+strconv.FormatInt(gid, 10)+"/repos", string(payload))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST add = %d %s", rec.Code, rec.Body.String())
	}
	if cached() != 0 {
		t.Errorf("%d cached tokens after an add that linked a repository; want 0 (the caller's scope changed)", cached())
	}
	var first map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &first); err != nil || first["linked"] != float64(1) {
		t.Errorf("the first add's response = %s (%v); want linked 1", rec.Body.String(), err)
	}

	// PR #218 review C2: re-posting a repository already in the group links
	// nothing, so it reports linked 0 and leaves every cached token alone
	// (it counted as linked and dropped the whole cache every time).
	if rec := do(http.MethodGet, "/api/v1/groups", ""); rec.Code != http.StatusOK {
		t.Fatalf("re-priming GET = %d", rec.Code)
	}
	if cached() != 1 {
		t.Fatalf("%d cached tokens before the re-post; want 1", cached())
	}
	rec = do(http.MethodPost, "/api/v1/groups/"+strconv.FormatInt(gid, 10)+"/repos", string(payload))
	if rec.Code != http.StatusOK {
		t.Fatalf("re-POST add = %d %s", rec.Code, rec.Body.String())
	}
	var again map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &again); err != nil || again["linked"] != float64(0) {
		t.Errorf("re-posting an already-linked repository = %s (%v); want linked 0", rec.Body.String(), err)
	}
	if cached() != 1 {
		t.Errorf("%d cached tokens after a re-post that linked nothing; want 1 (no scope changed)", cached())
	}
}

// TestServerErrorIsQuietOnACanceledRequest (batch 5 review round 1): every
// handler passes r.Context() to the store, so a client that closed the tab
// mid-query reaches serverError with context.Canceled. That is not a
// failure — nobody is listening — and its sibling refuseStoreError already
// classifies it as Debug; an ERROR per abandoned navigation is noise.
func TestServerErrorIsQuietOnACanceledRequest(t *testing.T) {
	var logs strings.Builder
	s := &Server{logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	rec := httptest.NewRecorder()
	// NET-6 review r5 F1: "cancelled" is the REQUEST's context being done.
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	s.serverError(rec, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(gone), "probe", fmt.Errorf("query: %w", context.Canceled))
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("a canceled request logged at ERROR:\n%s", logs.String())
	}
	logs.Reset()
	rec = httptest.NewRecorder()
	s.serverError(rec, httptest.NewRequest(http.MethodGet, "/", nil), "probe", errors.New("relation aveloxis_data.repos does not exist"))
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "aveloxis_data") {
		t.Errorf("a real failure: status %d body %q; want a generic 500", rec.Code, rec.Body.String())
	}
	if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "handler=probe") {
		t.Errorf("a real failure must log at ERROR with the handler's name:\n%s", logs.String())
	}
}

// TestGroupListsAreNot403OnAStoreFailure pins follow-up 12's API half after
// batch 5: the group repository and org lists answered EVERY store error
// with a 403 carrying its text ("ownership refusal, the dominant case").
// The store returns every failure but "not yours" since v0.29.68, so that
// 403 misfiled a store failure as the caller's; it is a logged 500 now.
func TestGroupListsAreNot403OnAStoreFailure(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	const login = "_avapi_grouplist_probe"
	const other = "_avapi_grouplist_other"
	pool := store.Pool()
	clean := func() {
		for _, l := range []string{login, other} {
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, l)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, l)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, l)
		}
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "group list probe")
	if err != nil {
		t.Fatal(err)
	}
	otherUID, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: other, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	otherGID, err := store.CreateUserGroup(ctx, otherUID, "someone else's group")
	if err != nil {
		t.Fatal(err)
	}
	token, err := store.CreateSessionToken(ctx, uid, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var logs strings.Builder
	s := New(servedStore(t, dsn), slog.New(slog.NewTextHandler(&logs, nil)))
	get := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.RemoteAddr = "203.0.113.6:1"
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		return rec
	}
	repos, orgs := fmt.Sprintf("/api/v1/groups/%d/repos", gid), fmt.Sprintf("/api/v1/groups/%d/orgs", gid)
	for _, p := range []string{repos, orgs} {
		if rec := get(p); rec.Code != http.StatusOK {
			t.Fatalf("priming GET %s = %d %s", p, rec.Code, rec.Body.String())
		}
	}
	// Someone else's group, and a group id nobody has, are still the
	// caller's 403 (PR #218 review C14: only the missing id was exercised,
	// under a comment naming someone else's group).
	for _, g := range []int64{otherGID, otherGID + 1000000} {
		for _, p := range []string{fmt.Sprintf("/api/v1/groups/%d/repos", g), fmt.Sprintf("/api/v1/groups/%d/orgs", g)} {
			if rec := get(p); rec.Code != http.StatusForbidden {
				t.Errorf("GET %s, a group the caller does not own = %d; want 403", p, rec.Code)
			}
		}
	}
	s.store.Close() // the token stays cached; the list queries now fail
	for _, p := range []string{repos, orgs} {
		logs.Reset()
		rec := get(p)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("GET %s over a closed pool = %d %q; want 500, not a 403 for the caller", p, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "closed pool") {
			t.Errorf("GET %s: the body carries the store's text: %q", p, rec.Body.String())
		}
		if !strings.Contains(logs.String(), "level=ERROR") {
			t.Errorf("GET %s: the failure is not logged:\n%s", p, logs.String())
		}
	}
}

// TestRepoLookupsAreNot404OnAStoreFailure pins the three metrics lookups
// batch 5b's review round 1 found beside the fixed one (by owner and name,
// a repo group by name, a repo by group and name): "no such thing" is a
// 404 through the typed sentinels, a store failure a logged 500 — they
// answered 404 for both.
func TestRepoLookupsAreNot404OnAStoreFailure(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	const login = "_avapi_lookup404_probe"
	pool := store.Pool()
	clean := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	token, err := store.CreateSessionToken(ctx, uid, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var logs strings.Builder
	s := New(servedStore(t, dsn), slog.New(slog.NewTextHandler(&logs, nil)))
	get := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.RemoteAddr = "203.0.113.7:1"
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		return rec
	}
	paths := []string{"/api/v1/owner/_avnobody/repo/_avnothing", "/api/v1/rg-name/_avnogroup", "/api/v1/rg-name/_avnogroup/repo-name/_avnothing"}
	for _, p := range paths {
		if rec := get(p); rec.Code != http.StatusNotFound {
			t.Errorf("GET %s (nothing there) = %d %q; want 404", p, rec.Code, rec.Body.String())
		}
	}
	s.store.Close() // the token stays cached; the lookups now fail
	for _, p := range paths {
		logs.Reset()
		rec := get(p)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("GET %s over a closed pool = %d %q; want 500, not \"not found\"", p, rec.Code, rec.Body.String())
		}
		if !strings.Contains(logs.String(), "level=ERROR") {
			t.Errorf("GET %s: the failure is not logged:\n%s", p, logs.String())
		}
	}
}

// TestResolveEntityReposDoesNotReadAStoreErrorAsOutOfScope (batch 5b review
// round 2): the Comparisons auto-add verified a repo entity with
// GetReposBatch and read `err != nil || repos[id] == nil` as "not a collected
// repository" — an unlogged 403 telling an entitled user their repository
// is out of scope on a transient store failure. The error arm goes to
// serverError first; only a nil row is "not collected". Structural: the
// arm sits behind a scope check and a URL-derived id that the closed-pool
// fixture cannot reach without a collected repository in the token's scope.
func TestResolveEntityReposDoesNotReadAStoreErrorAsOutOfScope(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/api/analytics.go"), "func (s *Server) resolveEntityRepos("))
	i := strings.Index(body, "s.store.GetReposBatch(r.Context(), collected)")
	if i < 0 {
		t.Fatal("the Comparisons auto-add no longer verifies the repo entity with GetReposBatch")
	}
	arm := body[i:]
	end := strings.Index(arm, "collected = nil")
	if end < 0 {
		t.Fatal("the nil-row check `collected = nil` moved; re-anchor this pin")
	}
	arm = arm[:end]
	at := strings.Index(arm, `s.serverError(w, r, "resolveEntityRepos", err)`)
	if at < 0 || strings.Contains(arm, "err != nil ||") {
		t.Fatal("a GetReposBatch error must go to serverError before the nil-row check: a store failure is not \"not a collected repository\"")
	}
	// The statement after the log is the return (review round 3: without
	// it the 500 is followed by a 403 and a JSON tail on the same body).
	if !strings.HasPrefix(strings.TrimSpace(arm[at+len(`s.serverError(w, r, "resolveEntityRepos", err)`):]), `return nil, "", false`) {
		t.Error("serverError in resolveEntityRepos must be followed by `return nil, \"\", false`")
	}
}

// TestSBOMDownloadAnswers404ForARepoNobodyHas (batch 5b review round 3): an
// admin's scope admits any id, so the generator is the first to learn the
// repository does not exist; through round 1 that was a 500 "internal
// error". A repository nobody has is a 404; a store failure a logged 500.
func TestSBOMDownloadAnswers404ForARepoNobodyHas(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	const login = "_avapi_sbom404_probe"
	pool := store.Pool()
	clean := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetUserAdmin(ctx, uid, true); err != nil {
		t.Fatal(err)
	}
	token, err := store.CreateSessionToken(ctx, uid, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var logs strings.Builder
	s := New(servedStore(t, dsn), slog.New(slog.NewTextHandler(&logs, nil)))
	get := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.RemoteAddr = "203.0.113.8:1"
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		return rec
	}
	path := "/api/v1/repos/999999999/sbom?format=cyclonedx"
	if rec := get(path); rec.Code != http.StatusNotFound || strings.Contains(rec.Body.String(), "no rows") {
		t.Errorf("SBOM of a repository nobody has = %d %q; want a plain 404", rec.Code, rec.Body.String())
	}
	s.store.Close()
	logs.Reset()
	if rec := get(path); rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "closed pool") || !strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("SBOM over a closed pool = %d %q (logs: %q); want a logged 500 without the store's text", rec.Code, rec.Body.String(), logs.String())
	}
}
