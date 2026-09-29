// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

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

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
)

// TestApproveRefusalsAreNotServerErrors (AVELOXIS_TEST_DB; PR #218 fix review
// r1): after review C6 turned store failures into generic 500s, two answers
// the admin must act on became "internal error; try again" — a pending org
// request from before v0.29.54 whose URL the registration index cannot hold
// (the admin has to reject it; follow-up 14), and a request id that does not
// exist. They are a 409 naming the remedy and a 404.
func TestApproveRefusalsAreNotServerErrors(t *testing.T) {
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
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	member, admin := "_avweb_overlong_member"+suffix, "_avweb_overlong_admin"+suffix
	clean := func() {
		p := store.Pool()
		for _, login := range []string{member, admin} {
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
	gid, err := store.CreateUserGroup(ctx, muid, "over-long org probe")
	if err != nil {
		t.Fatal(err)
	}
	var reqID int64
	overlong := "https://github.com/" + strings.Repeat("a", 2700)
	if err := store.Pool().QueryRow(ctx, `
		INSERT INTO aveloxis_ops.collection_add_requests (user_id, group_id, kind, org_url, status, item_count)
		VALUES ($1, $2, 'org', $3, 'pending', 0) RETURNING request_id`, muid, gid, overlong).Scan(&reqID); err != nil {
		t.Fatal(err)
	}
	var logs strings.Builder
	s := New(store, config.WebConfig{}, nil, "", slog.New(slog.NewTextHandler(&logs, nil)))
	s.sessions["admin"] = &Session{UserID: auid, LoginName: admin, IsAdmin: true, ExpiresAt: time.Now().Add(time.Hour)}
	decide := func(id int64, decision string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/admin/add-requests/"+strconv.FormatInt(id, 10)+"/"+decision, nil)
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "admin"})
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	approve := func(id int64) *httptest.ResponseRecorder { return decide(id, "approve") }

	w := approve(reqID)
	body := strings.ToLower(w.Body.String())
	// The index's own bound (2704 − 20), not the add limit (1342): the
	// generic advice named the wrong number (PR #218 fix review r1).
	if w.Code != http.StatusConflict || !strings.Contains(body, "longer than 2684") || strings.Contains(body, "1342") || !strings.Contains(body, "reject") {
		t.Errorf("approving an over-long legacy org = %d %q; want 409 telling the admin to reject it", w.Code, strings.TrimSpace(w.Body.String()))
	}
	// PR #218 fix review r2 F6: a request approved before v0.29.39 that lost
	// its registration reaches the same refusal on re-approve, and rejecting
	// an approved row is a silent no-op, so the advice must cover that state
	// as its credentials twin does.
	if !strings.Contains(body, "already approved") {
		t.Errorf("the over-long refusal must say what happens to an already-approved request: %q", strings.TrimSpace(w.Body.String()))
	}
	if strings.Contains(w.Body.String(), strings.Repeat("a", 50)) {
		t.Errorf("the 409 body echoes the URL")
	}

	if w := approve(reqID + 1_000_000); w.Code != http.StatusNotFound {
		t.Errorf("approving a request that does not exist = %d %q; want 404", w.Code, strings.TrimSpace(w.Body.String()))
	}
	// PR #218 fix review r2 (test gap): the reject handler's 404 arm.
	if w := decide(reqID+1_000_000, "reject"); w.Code != http.StatusNotFound {
		t.Errorf("rejecting a request that does not exist = %d %q; want 404", w.Code, strings.TrimSpace(w.Body.String()))
	}
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("a refusal or a missing request was logged as a server error:\n%s", logs.String())
	}
}
