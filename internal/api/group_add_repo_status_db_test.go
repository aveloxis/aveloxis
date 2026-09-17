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
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// TestGroupAddRepoErrorStatus (AVELOXIS_TEST_DB) drives POST
// /api/v1/groups/{groupID}/repos through the real handler and store: a group
// the caller does not own, or a rejected group, is a 400 with its reason;
// repositories that could not be added and a store failure are a logged 500
// whose body carries no database text (Copilot review of PR #207 on d436880:
// every error was a 400 with the raw error, so a serialization failure or a
// lost connection read as a client mistake that retrying cannot fix).
func TestGroupAddRepoErrorStatus(t *testing.T) {
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
	const login = "_avapi_add_status_probe"
	const urlPrefix = "https://github.com/_avapi-add-status-owner/_avapi-add-status-"
	const trigger = "_avtest_api_fail_user_repos_link"
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
	gid, err := store.CreateUserGroup(ctx, uid, "api add status probe")
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := store.CreateUserGroup(ctx, uid, "api add status probe, rejected")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RejectGroup(ctx, rejected, uid); err != nil {
		t.Fatal(err)
	}
	var notOwned int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(MAX(group_id), 0) + 1000000 FROM aveloxis_ops.user_groups`).Scan(&notOwned); err != nil {
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
	closed, err := db.NewPostgresStore(ctx, dsn, discard)
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()

	for _, tc := range []struct {
		name     string
		store    *db.PostgresStore
		groupID  int64
		body     string
		code     int
		inBody   string
		logsWarn bool
	}{
		{"not owned", store, notOwned, `{"urls":["` + urlPrefix + `works"],"kind":"repo"}`, http.StatusBadRequest, "group not found or not owned by user", false},
		{"rejected group", store, rejected, `{"urls":["` + urlPrefix + `works"],"kind":"repo"}`, http.StatusBadRequest, "group has been rejected by an administrator", false},
		{"rejected group, org", store, rejected, `{"url":"https://github.com/_avapi-add-status-org","kind":"org"}`, http.StatusBadRequest, "group has been rejected by an administrator", false},
		{"repositories could not be added", store, gid, `{"urls":["` + urlPrefix + `fails","` + urlPrefix + `works"],"kind":"repo"}`, http.StatusInternalServerError, "1 of 2 repositories could not be added", true},
		{"store failure", closed, gid, `{"urls":["` + urlPrefix + `works"],"kind":"repo"}`, http.StatusInternalServerError, "could not add", true},
		{"store failure, org", closed, gid, `{"url":"https://github.com/_avapi-add-status-org","kind":"org"}`, http.StatusInternalServerError, "could not add", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := &lockedBuffer{}
			s := &Server{store: tc.store, logger: slog.New(slog.NewTextHandler(logs, nil)), auth: newAuthenticator(tc.store, false), autoApproveAddLimit: 5}
			id := strconv.FormatInt(tc.groupID, 10)
			r := httptest.NewRequest(http.MethodPost, "/api/v1/groups/"+id+"/repos", strings.NewReader(tc.body))
			r.SetPathValue("groupID", id)
			r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: uid}))
			w := httptest.NewRecorder()
			s.handleGroupAddRepo(w, r)
			if w.Code != tc.code || !strings.Contains(w.Body.String(), tc.inBody) {
				t.Errorf("= %d %q; want %d with %q", w.Code, w.Body.String(), tc.code, tc.inBody)
			}
			if strings.Contains(w.Body.String(), "SQLSTATE") || strings.Contains(w.Body.String(), "closed pool") || strings.Contains(w.Body.String(), "injected") {
				t.Errorf("the response carries database text: %q", w.Body.String())
			}
			want := fmt.Sprintf("group_id=%d", tc.groupID)
			warned := false
			for _, line := range strings.Split(logs.String(), "\n") {
				warned = warned || strings.Contains(line, "level=WARN") && strings.Contains(line, want) && strings.Contains(line, "error=")
			}
			if warned != tc.logsWarn {
				t.Errorf("WARN with %s and the error logged = %v, want %v; log:\n%s", want, warned, tc.logsWarn, logs.String())
			}
		})
	}
}
