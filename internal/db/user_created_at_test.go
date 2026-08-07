// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.89 — the admin users screen's "Joined" column was showing
// last access: aveloxis_ops.users had NO created_at column, and
// ListUsers served data_collection_date (re-stamped NOW() by
// UpsertOAuthUser's UPDATE branch on EVERY login) as CreatedAt.
// Operator report 2026-08-05: "The column says 'date joined', but
// it's actually showing the date last accessed. Show both."
//
// Fix: users.created_at via the v0.27.60 repos.added_at three-step
// (bare add → backfill from data_collection_date, an honest
// last-touch approximation → SET DEFAULT NOW()), INSERT-only; and
// ListUsers/handleAdminUsers carry BOTH created_at and last_seen.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

func readFileForUserCreatedAt(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestUsersSchemaDeclaresCreatedAt pins the fresh-install shape.
func TestUsersSchemaDeclaresCreatedAt(t *testing.T) {
	src := readFileForUserCreatedAt(t, "schema.sql")
	start := strings.Index(src, "CREATE TABLE IF NOT EXISTS aveloxis_ops.users (")
	if start < 0 {
		t.Fatal("could not find aveloxis_ops.users CREATE TABLE")
	}
	end := strings.Index(src[start:], ");")
	block := src[start : start+end]
	if !strings.Contains(block, "created_at") {
		t.Error("aveloxis_ops.users must declare created_at TIMESTAMPTZ DEFAULT NOW() — without it the admin screen's Joined column has nothing honest to show")
	}
}

// TestUsersCreatedAtMigrationThreeStep pins the v0.27.60-pattern
// ordering: bare add, THEN backfill from data_collection_date, THEN
// the NOW() default. A default at add-time would stamp every legacy
// row with the migration timestamp instead of the honest last-touch
// approximation.
func TestUsersCreatedAtMigrationThreeStep(t *testing.T) {
	src := readFileForUserCreatedAt(t, "migrate.go")
	for _, needle := range []string{
		`addColumnIfMissing(ctx, pg, logger, errs, "aveloxis_ops.users", "created_at", "TIMESTAMPTZ")`,
		"v0.27.89 backfill users.created_at from data_collection_date (last-touch approximation)",
		"SET created_at = COALESCE(data_collection_date, NOW())",
		"v0.27.89 default users.created_at to NOW() for future signups",
		"ALTER TABLE aveloxis_ops.users ALTER COLUMN created_at SET DEFAULT NOW()",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("migrate.go missing users.created_at migration piece: %q", needle)
		}
	}
	add := strings.Index(src, `"aveloxis_ops.users", "created_at"`)
	backfill := strings.Index(src, "v0.27.89 backfill users.created_at")
	def := strings.Index(src, "ALTER TABLE aveloxis_ops.users ALTER COLUMN created_at SET DEFAULT")
	if !(add >= 0 && add < backfill && backfill < def) {
		t.Error("users.created_at migration order must be add → backfill → default (the v0.27.60 load-bearing ordering)")
	}
}

// TestListUsersReturnsBothDates pins that ListUsers reads the REAL
// created_at AND data_collection_date (as last-seen) — the two are
// different columns with different semantics and the admin screen
// shows both.
func TestListUsersReturnsBothDates(t *testing.T) {
	src := readFileForUserCreatedAt(t, "web_store.go")
	fn := extractListUsersBody(t, src)
	if !strings.Contains(fn, "created_at") {
		t.Error("ListUsers must select users.created_at (the real join date)")
	}
	if !strings.Contains(fn, "data_collection_date") {
		t.Error("ListUsers must select data_collection_date (last seen — stamped on every login)")
	}
	if !strings.Contains(src, "LastSeen") {
		t.Error("AdminUser must carry LastSeen alongside CreatedAt")
	}
}

func extractListUsersBody(t *testing.T, src string) string {
	t.Helper()
	start := strings.Index(src, "func (s *PostgresStore) ListUsers(")
	if start < 0 {
		t.Fatal("ListUsers not found")
	}
	end := strings.Index(src[start:], "\n}")
	return src[start : start+end]
}

// TestUpsertOAuthUserNeverTouchesCreatedAt — insert-only contract:
// the login-time UPDATE must not re-stamp created_at, or the Joined
// column degrades right back into last-accessed.
func TestUpsertOAuthUserNeverTouchesCreatedAt(t *testing.T) {
	src := readFileForUserCreatedAt(t, "web_store.go")
	start := strings.Index(src, "func (s *PostgresStore) UpsertOAuthUser(")
	if start < 0 {
		t.Fatal("UpsertOAuthUser not found")
	}
	end := strings.Index(src[start:], "\n}")
	body := src[start : start+end]
	upd := strings.Index(body, "UPDATE aveloxis_ops.users SET")
	if upd < 0 {
		t.Fatal("UpsertOAuthUser UPDATE branch not found")
	}
	if strings.Contains(body[upd:], "created_at") {
		t.Error("UpsertOAuthUser's UPDATE branch must NOT touch created_at (insert-only — DEFAULT NOW() covers the INSERT path)")
	}
}

// TestAdminUsersAPICarriesLastSeen pins the JSON surface.
func TestAdminUsersAPICarriesLastSeen(t *testing.T) {
	src := readFileForUserCreatedAt(t, "../api/portal.go")
	start := strings.Index(src, "func (s *Server) handleAdminUsers(")
	if start < 0 {
		t.Fatal("handleAdminUsers not found")
	}
	end := strings.Index(src[start:], "\n}")
	body := src[start : start+end]
	if !strings.Contains(body, `json:"last_seen"`) {
		t.Error("/admin/users rows must carry last_seen alongside created_at")
	}
}

// TestUserCreatedAtEndToEnd (AVELOXIS_TEST_DB): first signup stamps
// created_at; a re-login advances data_collection_date but leaves
// created_at untouched.
func TestUserCreatedAtEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	const login = "_avcreated_at_probe"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.email_confirmations WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)

	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}

	var created1, seen1 time.Time
	if err := store.pool.QueryRow(ctx,
		`SELECT created_at, data_collection_date FROM aveloxis_ops.users WHERE user_id = $1`,
		uid).Scan(&created1, &seen1); err != nil {
		t.Fatal(err)
	}
	if created1.IsZero() {
		t.Fatal("first signup must stamp created_at via the column default")
	}

	// Simulate the passage of time, then re-login.
	if _, err := store.pool.Exec(ctx,
		`UPDATE aveloxis_ops.users SET data_collection_date = NOW() - INTERVAL '1 day', created_at = NOW() - INTERVAL '30 days' WHERE user_id = $1`, uid); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"}); err != nil {
		t.Fatal(err)
	}

	var created2, seen2 time.Time
	if err := store.pool.QueryRow(ctx,
		`SELECT created_at, data_collection_date FROM aveloxis_ops.users WHERE user_id = $1`,
		uid).Scan(&created2, &seen2); err != nil {
		t.Fatal(err)
	}
	if !created2.Equal(created1.Add(-30*24*time.Hour).Round(0)) && created2.After(created1) {
		t.Error("re-login must not advance created_at (insert-only contract)")
	}
	if !seen2.After(time.Now().Add(-time.Minute)) {
		t.Error("re-login must re-stamp data_collection_date (last seen)")
	}

	// ListUsers surfaces both, distinctly.
	users, err := store.ListUsers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, u := range users {
		if u.Login == login {
			found = true
			if u.LastSeen.Sub(u.CreatedAt) < 24*time.Hour {
				t.Errorf("ListUsers must return DISTINCT created_at (%v) and last_seen (%v) — the 30-day-old join date must survive the re-login", u.CreatedAt, u.LastSeen)
			}
		}
	}
	if !found {
		t.Error("probe user missing from ListUsers")
	}
}
