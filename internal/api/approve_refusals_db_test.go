// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

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

func approveRefusalStore(t *testing.T) *db.PostgresStore {
	t.Helper()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	store, err := db.NewPostgresStore(context.Background(), dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store
}

// TestAPIApproveRefusalsAreNotServerErrors (PR #218 fix review r1): the API
// twin of the web test — an over-long legacy org request is a 409 telling
// the admin to reject it, and an unknown request id is a 404, not a logged
// generic 500.
func TestAPIApproveRefusalsAreNotServerErrors(t *testing.T) {
	store := approveRefusalStore(t)
	ctx := context.Background()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	member := "_avapi_overlong_member" + suffix
	t.Cleanup(func() {
		p := store.Pool()
		_, _ = p.Exec(context.Background(), `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, member)
		_, _ = p.Exec(context.Background(), `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, member)
		_, _ = p.Exec(context.Background(), `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, member)
	})
	muid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: member, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, muid, "over-long org probe")
	if err != nil {
		t.Fatal(err)
	}
	var reqID int64
	if err := store.Pool().QueryRow(ctx, `
		INSERT INTO aveloxis_ops.collection_add_requests (user_id, group_id, kind, org_url, status, item_count)
		VALUES ($1, $2, 'org', $3, 'pending', 0) RETURNING request_id`, muid, gid, "https://github.com/"+strings.Repeat("a", 2700)).Scan(&reqID); err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	s := &Server{store: store, logger: slog.New(slog.NewTextHandler(logs, nil)), auth: newAuthenticator(store, false, nil)}
	decide := func(id int64) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/admin/add-requests/x/approve", nil)
		r.SetPathValue("requestID", strconv.FormatInt(id, 10))
		r.SetPathValue("decision", "approve")
		r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: muid + 1_000_000, IsAdmin: true}))
		w := httptest.NewRecorder()
		s.handleAdminAddRequestDecision(w, r)
		return w
	}
	w := decide(reqID)
	body := strings.ToLower(w.Body.String())
	// The index's own bound (2704 − 20), not the add limit (1342): the
	// generic advice named the wrong number (PR #218 fix review r1).
	if w.Code != http.StatusConflict || !strings.Contains(body, "longer than 2684") || strings.Contains(body, "1342") || !strings.Contains(body, "reject") {
		t.Errorf("approving an over-long legacy org = %d %q; want 409 telling the admin to reject it", w.Code, strings.TrimSpace(w.Body.String()))
	}
	if w := decide(reqID + 1_000_000); w.Code != http.StatusNotFound {
		t.Errorf("approving a request that does not exist = %d %q; want 404", w.Code, strings.TrimSpace(w.Body.String()))
	}
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("a refusal or a missing request was logged as a server error:\n%s", logs.String())
	}
}

// TestAPIDemotingTheLastAdminIsRefusedNotA500 (PR #218 fix review r1): the
// API twin of the web last-admin test — the store's ErrLastAdmin is a 409
// with its reason, not a logged 500.
func TestAPIDemotingTheLastAdminIsRefusedNotA500(t *testing.T) {
	store := approveRefusalStore(t)
	ctx := context.Background()
	pool := store.Pool()
	var target int
	if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_ops.users (login_name, admin) VALUES ('_avapi_last_admin', TRUE) RETURNING user_id`).Scan(&target); err != nil {
		t.Fatal(err)
	}
	var others []int
	rows, err := pool.Query(ctx, `SELECT user_id FROM aveloxis_ops.users WHERE admin AND user_id <> $1`, target)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		others = append(others, id)
	}
	rows.Close()
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `UPDATE aveloxis_ops.users SET admin = TRUE WHERE user_id = ANY($1)`, others)
		_, _ = pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, target)
	})
	if _, err := pool.Exec(ctx, `UPDATE aveloxis_ops.users SET admin = FALSE WHERE user_id = ANY($1)`, others); err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	s := &Server{store: store, logger: slog.New(slog.NewTextHandler(logs, nil)), auth: newAuthenticator(store, false, nil)}
	r := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users/x/admin", strings.NewReader(`{"admin": false}`))
	r.SetPathValue("userID", strconv.Itoa(target))
	// A stale cached admin identity: the database no longer says so.
	r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: target + 1_000_000, IsAdmin: true}))
	w := httptest.NewRecorder()
	s.handleAdminSetUserAdmin(w, r)
	if w.Code != http.StatusConflict || !strings.Contains(strings.ToLower(w.Body.String()), "last admin") {
		t.Fatalf("demoting the last admin = %d %q; want 409 with the reason", w.Code, strings.TrimSpace(w.Body.String()))
	}
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("a refusal was logged as a server error:\n%s", logs.String())
	}
}
