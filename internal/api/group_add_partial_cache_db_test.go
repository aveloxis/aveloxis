// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"encoding/json"
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

// TestGroupAddPartialFailureBustsTheTokenCache (AVELOXIS_TEST_DB) — PR #218
// review C1. An auto-approved add where one URL fails still links and
// enqueues the others, and the store reports that with ErrAddItemsFailed.
// The handler dropped the token cache only on a nil error, so the caller's
// cached scope missed the repository it had just added: the next read of it
// with the same token went through the "Shared with Me" auto-add (or a 403
// with that seam off) instead of reading a repository in the caller's own
// group. The cache must be dropped whenever anything was linked or enqueued.
func TestGroupAddPartialFailureBustsTheTokenCache(t *testing.T) {
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
	const login = "_avapi_partial_bust_probe"
	const urlPrefix = "https://github.com/_avapi-partial-bust-owner/_avapi-partial-bust-"
	const trigger = "_avtest_api_partial_bust_link"
	pool := store.Pool()
	clean := func() {
		_, _ = pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+trigger+` ON aveloxis_ops.user_repos`)
		_, _ = pool.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_ops.`+trigger+`()`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%')`, urlPrefix)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%'`, urlPrefix)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
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
	if admin, err := store.IsUserAdmin(ctx, uid); err != nil || admin {
		t.Fatalf("fixture user must be a non-admin (admin=%v, err=%v)", admin, err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "api partial bust probe")
	if err != nil {
		t.Fatal(err)
	}
	token, err := store.CreateSessionToken(ctx, uid, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	// One URL's link fails; the other is linked and enqueued.
	if _, err := pool.Exec(ctx, `CREATE FUNCTION aveloxis_ops.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN
			IF EXISTS (SELECT 1 FROM aveloxis_data.repos WHERE repo_id = NEW.repo_id AND repo_git LIKE '%partial-bust-fails') THEN
				RAISE EXCEPTION 'injected link failure' USING ERRCODE = '40001';
			END IF;
			RETURN NEW;
		END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER `+trigger+` BEFORE INSERT ON aveloxis_ops.user_repos FOR EACH ROW EXECUTE FUNCTION aveloxis_ops.`+trigger+`()`); err != nil {
		t.Fatal(err)
	}

	s, err := NewWithOptions(store, discard, Options{ExemptCIDRs: DefaultExemptCIDRs, AutoApproveAddLimit: 5})
	if err != nil {
		t.Fatal(err)
	}
	do := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.RemoteAddr = "203.0.113.9:1"
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		return rec
	}
	if rec := do(http.MethodGet, "/api/v1/groups", ""); rec.Code != http.StatusOK {
		t.Fatalf("priming GET = %d %s", rec.Code, rec.Body.String())
	}
	works := urlPrefix + "works"
	payload, _ := json.Marshal(map[string]any{"urls": []string{urlPrefix + "partial-bust-fails", works}, "kind": "repo"})
	rec := do(http.MethodPost, "/api/v1/groups/"+strconv.FormatInt(gid, 10)+"/repos", string(payload))
	// The partial add is reported as a failure naming the count.
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "1 of 2") {
		t.Fatalf("partial add = %d %q; want 500 naming 1 of 2", rec.Code, rec.Body.String())
	}
	rid, err := store.FindRepoByURL(ctx, works)
	if err != nil || rid == 0 {
		t.Fatalf("the URL beside the failing one was not added (id %d, %v)", rid, err)
	}
	var linked bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM aveloxis_ops.user_repos WHERE group_id = $1 AND repo_id = $2)`, gid, rid).Scan(&linked); err != nil || !linked {
		t.Fatalf("the added repository is not linked into the group (%v, %v)", linked, err)
	}

	// The same token reads the repository it just added. With the "Shared
	// with Me" auto-add seam off (the fail-closed configuration) a stale
	// cached scope is a 403...
	path := "/api/v1/repos/" + strconv.FormatInt(rid, 10) + "/stats"
	seam := s.sharedWithMe
	s.sharedWithMe = nil
	if read := do(http.MethodGet, path, ""); read.Code == http.StatusForbidden {
		t.Fatalf("reading the just-added repository = 403 %s; the cached scope is stale", read.Body.String())
	}
	// ...and with it on, the read must not auto-add the caller's own
	// group's repository to "Shared with Me".
	s.sharedWithMe = seam
	if read := do(http.MethodGet, path, ""); read.Code == http.StatusForbidden || read.Header().Get(sharedWithMeHeader) != "" {
		t.Errorf("reading the just-added repository = %d, auto-added to %q; want an in-scope read", read.Code, read.Header().Get(sharedWithMeHeader))
	}
}
