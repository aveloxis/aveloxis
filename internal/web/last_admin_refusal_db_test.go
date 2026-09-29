// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
)

// TestDemotingTheLastAdminIsRefusedNotA500 (AVELOXIS_TEST_DB; PR #218 review
// follow-up to C6): the store's last-admin refusal was untyped, so once
// store failures became logged generic 500s, a refused demotion also read as
// a server error. It is the store refusing the operation, not failing: 409
// with a message saying why. Reachable through a stale admin session (the
// caller's own demotion is refused before the store).
func TestDemotingTheLastAdminIsRefusedNotA500(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	pool := store.Pool()

	// Make a fresh user the ONLY admin, restoring the others afterwards.
	var target int
	if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_ops.users (login_name, admin) VALUES ('_avweb_last_admin', TRUE) RETURNING user_id`).Scan(&target); err != nil {
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
		if _, err := pool.Exec(context.Background(), `UPDATE aveloxis_ops.users SET admin = TRUE WHERE user_id = ANY($1)`, others); err != nil {
			t.Logf("restoring admins: %v", err)
		}
		if _, err := pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, target); err != nil {
			t.Logf("cleanup: %v", err)
		}
	})
	if _, err := pool.Exec(ctx, `UPDATE aveloxis_ops.users SET admin = FALSE WHERE user_id = ANY($1)`, others); err != nil {
		t.Fatal(err)
	}

	s := New(store, config.WebConfig{}, nil, "", logger)
	// A stale session: it says admin, but the database no longer does.
	s.sessions["stale-admin"] = &Session{UserID: target + 100000, LoginName: "_avweb_stale", IsAdmin: true, ExpiresAt: time.Now().Add(time.Hour)}
	r := httptest.NewRequest(http.MethodPost, "/admin/users/"+strconv.Itoa(target)+"/admin", strings.NewReader(url.Values{"admin": {"false"}}.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "stale-admin"})
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)

	if w.Code != http.StatusConflict {
		t.Fatalf("demoting the last admin answered %d %q; want 409 with the reason", w.Code, strings.TrimSpace(w.Body.String()))
	}
	if !strings.Contains(strings.ToLower(w.Body.String()), "last admin") {
		t.Errorf("body %q does not say why", w.Body.String())
	}
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("a refusal was logged as a server error:\n%s", logs.String())
	}
	var still bool
	if err := pool.QueryRow(ctx, `SELECT admin FROM aveloxis_ops.users WHERE user_id = $1`, target).Scan(&still); err != nil || !still {
		t.Errorf("the last admin was demoted (admin=%v, err=%v)", still, err)
	}
}
