// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
)

// consoleFixture is a store, an administrator and a plain user with
// sessions, and a request runner that carries the session's form token.
type consoleFixture struct {
	store   *db.PostgresStore
	adminID int
	userID  int
	h       http.Handler
	srv     *Server
}

func newConsoleFixture(t *testing.T) *consoleFixture {
	t.Helper()
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
	tag := fmt.Sprintf("_avconsole%d", time.Now().UnixNano())
	adminID := seedAdminAccount(t, store, tag+"-admin")
	// The settings rows the flows restore name the admin in updated_by
	// (plain foreign keys, no ON DELETE): null them before the admin row's
	// delete, which is registered above and so runs after this (LIFO;
	// whole-branch review of 0.29.109: one admin row leaked per test).
	t.Cleanup(func() {
		for _, q := range []string{
			`UPDATE aveloxis_ops.api_token_settings SET updated_by = NULL WHERE updated_by = $1`,
			`UPDATE aveloxis_ops.capacity_settings SET updated_by = NULL WHERE updated_by = $1`,
			`UPDATE aveloxis_ops.capacity_quotas SET updated_by = NULL WHERE updated_by = $1`,
			`DELETE FROM aveloxis_ops.signup_allowlist WHERE added_by = $1`,
		} {
			_, _ = store.Pool().Exec(context.Background(), q, adminID)
		}
	})
	userID, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: tag + "-user", Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
	})
	s := New(store, config.WebConfig{DevMode: true}, "", logger)
	plantSession(t, s, "admin-tok", &Session{UserID: adminID, LoginName: tag + "-admin", ExpiresAt: time.Now().Add(time.Hour)})
	plantSession(t, s, "user-tok", &Session{UserID: userID, LoginName: tag + "-user", ExpiresAt: time.Now().Add(time.Hour)})
	return &consoleFixture{store: store, adminID: adminID, userID: userID, h: s.Handler(), srv: s}
}

func (f *consoleFixture) get(path, tok string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: tok})
	w := httptest.NewRecorder()
	f.h.ServeHTTP(w, r)
	return w
}

func (f *consoleFixture) post(path, tok string, form url.Values) *httptest.ResponseRecorder {
	form.Set("csrf_token", "test-csrf-token")
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: tok})
	csrf(r)
	w := httptest.NewRecorder()
	f.h.ServeHTTP(w, r)
	return w
}

// The approvals page lists the recorded forge-ID changes and adopts one
// through the service (D-24, stage 3a): the form names the pair the
// administrator chose, the store moves the repository's forge ID, a
// second adoption of the same pair is a 404 (nothing pending), and a
// non-administrator's POST is refused before the service runs.
func TestApprovalsPageAdoptsAForgeIDChange(t *testing.T) {
	f := newConsoleFixture(t)
	ctx := context.Background()
	owner := fmt.Sprintf("_avadopt%d", time.Now().UnixNano())
	var repoID int64
	if err := f.store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id, platform_repo_id)
		VALUES ($1, $2, 'r', 1, 1, '100') RETURNING repo_id`, "https://github.com/"+owner+"/r", owner).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = f.store.Pool().Exec(c, `DELETE FROM aveloxis_data.repo_forge_id_changes WHERE repo_id = $1`, repoID)
		_, _ = f.store.Pool().Exec(c, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	if _, err := f.store.Pool().Exec(ctx, `INSERT INTO aveloxis_data.repo_forge_id_changes (repo_id, old_forge_id, new_forge_id) VALUES ($1, '100', '200')`, repoID); err != nil {
		t.Fatal(err)
	}
	adopt := fmt.Sprintf("/admin/forge-id-changes/%d/adopt", repoID)

	w := f.get("/admin/groups/pending", "admin-tok")
	if w.Code != http.StatusOK {
		t.Fatalf("GET /admin/groups/pending = %d; want 200\n%s", w.Code, w.Body.String())
	}
	page := w.Body.String()
	for _, want := range []string{owner + "/r", `name="old_forge_id" value="100"`, `name="new_forge_id" value="200"`, `action="` + adopt + `"`} {
		if !strings.Contains(page, want) {
			t.Errorf("the approvals page does not show %q", want)
		}
	}

	form := url.Values{"old_forge_id": {"100"}, "new_forge_id": {"200"}, "note": {"re-created by its owner"}}
	if w := f.post(adopt, "user-tok", form); w.Code != http.StatusForbidden {
		t.Errorf("a non-admin's adopt = %d; want 403", w.Code)
	}
	w = f.post(adopt, "admin-tok", form)
	if w.Code != http.StatusFound || w.Header().Get("Location") != "/admin/groups/pending" {
		t.Fatalf("adopt = %d %q; want 302 back to the approvals page\n%s", w.Code, w.Header().Get("Location"), w.Body.String())
	}
	var stored, adoptedBy string
	var adoptedAt *time.Time
	if err := f.store.Pool().QueryRow(ctx, `SELECT r.platform_repo_id, c.adopted_by, c.adopted_at FROM aveloxis_data.repos r
		JOIN aveloxis_data.repo_forge_id_changes c USING (repo_id) WHERE r.repo_id = $1`, repoID).Scan(&stored, &adoptedBy, &adoptedAt); err != nil {
		t.Fatal(err)
	}
	if stored != "200" || adoptedAt == nil || adoptedBy == "" {
		t.Errorf("after the adoption: stored id %q, adopted_at %v, adopted_by %q; want 200, a time, the administrator's label", stored, adoptedAt, adoptedBy)
	}
	if w := f.post(adopt, "admin-tok", form); w.Code != http.StatusNotFound {
		t.Errorf("adopting the same pair again = %d; want 404 (nothing pending)", w.Code)
	}
	if w := f.get("/admin/groups/pending", "admin-tok"); strings.Contains(w.Body.String(), `action="`+adopt+`"`) {
		t.Error("an adopted change still offers its Adopt form")
	}
}

// The users page shows when each account joined and was last seen (57
// §2.2: the dates the API's users list carries and the page lacked).
func TestUsersPageShowsJoinedAndLastSeen(t *testing.T) {
	f := newConsoleFixture(t)
	w := f.get("/admin/users", "admin-tok")
	if w.Code != http.StatusOK {
		t.Fatalf("GET /admin/users = %d; want 200", w.Code)
	}
	if cc := w.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("a console page's Cache-Control = %q; want no-store (ASVS V14.3.2)", cc)
	}
	page := w.Body.String()
	// The date the page shows is the row's, read back (not time.Now(): the
	// fixture and the page can straddle midnight UTC — round-3 review).
	var joined time.Time
	if err := f.store.Pool().QueryRow(context.Background(), `SELECT created_at FROM aveloxis_ops.users WHERE user_id = $1`, f.userID).Scan(&joined); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Joined", "Last seen", joined.UTC().Format("2006-01-02"), fmt.Sprintf(`action="/admin/users/%d/admin"`, f.userID)} {
		if !strings.Contains(page, want) {
			t.Errorf("the users page does not show %q", want)
		}
	}
	if w := f.get("/admin/users", "user-tok"); w.Code != http.StatusForbidden {
		t.Errorf("a non-admin's GET /admin/users = %d; want 403", w.Code)
	}
}
