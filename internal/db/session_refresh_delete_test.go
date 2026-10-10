// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// Copilot review 5476192626 on PR #228 (HIGH): aveloxis_ops.refresh_tokens
// (an Augur leftover Aveloxis never writes) references
// user_session_tokens.token. A session with a refresh row could not be
// deleted — logout failed with a foreign-key error and the token stayed
// valid; the expired-token sweep failed the same way. Every session delete
// takes its refresh rows with it, in one statement.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSessionDeletesTakeTheirRefreshRows(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	pool := store.Pool()
	var userID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider) VALUES ('avx-it-refresh-del', 'github')
		ON CONFLICT (login_name) DO UPDATE SET oauth_provider = EXCLUDED.oauth_provider RETURNING user_id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.refresh_tokens WHERE id LIKE 'avx-it-refresh-%'`)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, userID)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
	})

	// Logout of a session that has a refresh row.
	tok, err := store.CreateSessionToken(ctx, userID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.refresh_tokens (id, user_session_token) VALUES ('avx-it-refresh-live', $1)`, hashToken(tok)); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSessionToken(ctx, tok); err != nil {
		t.Fatalf("logout of a session with a refresh row failed: %v", err)
	}
	if _, err := store.ValidateSessionToken(ctx, tok); err == nil {
		t.Fatal("the session is still valid after logout")
	}

	// The expired-token sweep (it runs when a token is created).
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_session_tokens (token, user_id, created_at, expiration) VALUES ('avx-it-expired-hash', $1, 1, 2)`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.refresh_tokens (id, user_session_token) VALUES ('avx-it-refresh-old', 'avx-it-expired-hash')`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSessionToken(ctx, userID, time.Hour); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_session_tokens WHERE token = 'avx-it-expired-hash'`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatal("the expired session with a refresh row survived the sweep")
	}
}

// Copilot review 5477367612 on PR #228 (HIGH): after a rollback, the older
// binary inserts RAW session tokens without naming token_hashed; the column
// defaulted to TRUE, so they were marked hashed and the next upgrade's
// one-time hash skipped them — plaintext at rest. Since 0.29.87 the default
// is FALSE, the new binary writes TRUE, and every migrate DELETES what is
// not marked (L10 round 2 on 0.29.87: hashing it instead turned a value
// read from a backup into a working credential). The session is signed out
// once — the contract a rollback already had.
func TestSessionTokensWrittenByAnOlderBinaryAreRemovedOnReupgrade(t *testing.T) {
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
	pool := store.Pool()
	var userID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider) VALUES ('avx-it-rollback-raw', 'github')
		ON CONFLICT (login_name) DO UPDATE SET oauth_provider = EXCLUDED.oauth_provider RETURNING user_id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.refresh_tokens WHERE id LIKE 'avx-it-rollback-%'`)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, userID)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
	})
	if err := RunMigrations(ctx, store, logger); err != nil { // the default is FALSE from here on
		t.Fatal(err)
	}
	fresh, err := store.CreateSessionToken(ctx, userID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var marked bool
	if err := pool.QueryRow(ctx, `SELECT token_hashed FROM aveloxis_ops.user_session_tokens WHERE token = $1`, hashToken(fresh)).Scan(&marked); err != nil || !marked {
		t.Fatalf("a token the new binary writes must be marked hashed: %v, %v", marked, err)
	}
	now := time.Now().Unix()
	raw := strings.Repeat("ab", 32)                       // what a pre-0.29.82 binary writes
	hashedUnmarked := hashToken(strings.Repeat("cd", 32)) // what 0.29.82–0.29.86 writes
	for i, tok := range []string{raw, hashedUnmarked} {
		if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.user_session_tokens (token, user_id, created_at, expiration) VALUES ($1, $2, $3, $4)`,
			tok, userID, now, now+3600); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.refresh_tokens (id, user_session_token) VALUES ($1, $2)`,
			"avx-it-rollback-"+strconv.Itoa(i), tok); err != nil {
			t.Fatal(err)
		}
	}
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_session_tokens WHERE user_id = $1 AND NOT token_hashed`, userID).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("%d unmarked session row(s) survived the migrate", left)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.refresh_tokens WHERE id LIKE 'avx-it-rollback-%'`).Scan(&left); err != nil || left != 0 {
		t.Fatalf("their refresh rows must go with them: %d, %v", left, err)
	}
	// Nothing a reader of the database or a backup saw is presentable.
	for _, seen := range []string{raw, hashedUnmarked} {
		if _, err := store.ValidateSessionToken(ctx, seen); err == nil {
			t.Fatalf("a value stored before the migrate (%.8s…) signs in after it", seen)
		}
	}
	// The new binary's own token is untouched.
	if got, err := store.ValidateSessionToken(ctx, fresh); err != nil || got != userID {
		t.Fatalf("a marked token must survive the migrate unchanged: %d, %v", got, err)
	}
}

// L10 round 1 on 0.29.87 (LOW): the column is added already FALSE and the
// one-time hash marks what it hashed, so no pre-0.29.82 binary can insert a
// raw row marked hashed between the add and a later default change. Driven
// on a table as a pre-0.29.82 deployment has it (no column).
func TestFirstUpgradeHashesAndMarksExistingSessions(t *testing.T) {
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
	pool := store.Pool()
	var userID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider) VALUES ('avx-it-first-upgrade', 'github')
		ON CONFLICT (login_name) DO UPDATE SET oauth_provider = EXCLUDED.oauth_provider RETURNING user_id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, userID)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
	})
	// A pre-0.29.82 deployment: no column, raw tokens. (Other rows in this
	// package database are hashed already; this test owns only its own.)
	if _, err := pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_session_tokens`); err != nil {
		t.Fatal(err)
	}
	// Restored whatever happens after the drop (L10 round 2 on 0.29.87): a
	// failure in between must not leave the package database without it.
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `ALTER TABLE aveloxis_ops.user_session_tokens ADD COLUMN IF NOT EXISTS token_hashed BOOLEAN NOT NULL DEFAULT FALSE`)
	})
	if _, err := pool.Exec(ctx, `ALTER TABLE aveloxis_ops.user_session_tokens DROP COLUMN token_hashed`); err != nil {
		t.Fatal(err)
	}
	raw := strings.Repeat("ef", 32)
	now := time.Now().Unix()
	if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.user_session_tokens (token, user_id, created_at, expiration) VALUES ($1, $2, $3, $4)`,
		raw, userID, now, now+3600); err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatal(err)
	}
	var def string
	if err := pool.QueryRow(ctx, `SELECT column_default FROM information_schema.columns
		WHERE table_schema = 'aveloxis_ops' AND table_name = 'user_session_tokens' AND column_name = 'token_hashed'`).Scan(&def); err != nil {
		t.Fatal(err)
	}
	if def != "false" {
		t.Fatalf("token_hashed default = %q, want false", def)
	}
	var marked bool
	if err := pool.QueryRow(ctx, `SELECT token_hashed FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, userID).Scan(&marked); err != nil || !marked {
		t.Fatalf("the first upgrade must mark what it hashed: %v, %v", marked, err)
	}
	if got, err := store.ValidateSessionToken(ctx, raw); err != nil || got != userID {
		t.Fatalf("a session from before the upgrade must keep working: %d, %v", got, err)
	}
}

// Copilot review 5477687920 on PR #228 (HIGH): on a fleet that ran
// 0.29.82–0.29.86 the column defaulted to TRUE, so a pre-0.29.82 binary run
// there (a rollback, a mixed deploy) stored RAW tokens marked hashed —
// indistinguishable from real hashes, left in plaintext. The marker is the
// default itself: still TRUE means such rows may exist, so that migrate
// signs every session out once (sessions and refresh rows) before it flips
// the default; afterwards the default is FALSE and nothing is wiped again.
func TestFleetFrom0_29_82To86SignsEverySessionOutOnce(t *testing.T) {
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
	pool := store.Pool()
	var userID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider) VALUES ('avx-it-fleet-8286', 'github')
		ON CONFLICT (login_name) DO UPDATE SET oauth_provider = EXCLUDED.oauth_provider RETURNING user_id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = pool.Exec(c, `ALTER TABLE aveloxis_ops.user_session_tokens ALTER COLUMN token_hashed SET DEFAULT FALSE`)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.refresh_tokens WHERE id = 'avx-it-fleet-ref'`)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, userID)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
	})
	// A 0.29.82–0.29.86 fleet: the default is TRUE, and a pre-0.29.82
	// binary wrote a raw token there (marked TRUE by that default).
	if _, err := pool.Exec(ctx, `ALTER TABLE aveloxis_ops.user_session_tokens ALTER COLUMN token_hashed SET DEFAULT TRUE`); err != nil {
		t.Fatal(err)
	}
	raw := strings.Repeat("9a", 32)
	now := time.Now().Unix()
	if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.user_session_tokens (token, user_id, created_at, expiration) VALUES ($1, $2, $3, $4)`,
		raw, userID, now, now+3600); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.refresh_tokens (id, user_session_token) VALUES ('avx-it-fleet-ref', $1)`, raw); err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_session_tokens WHERE token = $1`, raw).Scan(&n); err != nil || n != 0 {
		t.Fatalf("a raw token marked hashed survived the upgrade from 0.29.82–0.29.86: %d, %v", n, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.refresh_tokens WHERE id = 'avx-it-fleet-ref'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("its refresh row must go too: %d, %v", n, err)
	}
	// Once: a session created after it survives the next migrate.
	fresh, err := store.CreateSessionToken(ctx, userID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatal(err)
	}
	if got, err := store.ValidateSessionToken(ctx, fresh); err != nil || got != userID {
		t.Fatalf("the sign-out must happen once, not on every migrate: %d, %v", got, err)
	}
}

// ASVS review N5 (V11.2.2): a stored token hash said nothing about its
// form — the root of the 0.29.87/0.29.88 migration work. Both token kinds
// are stored as "sha256$<hex>"; a migrate tags any untagged hash already
// stored (session rows marked hashed, with their refresh rows; API-token
// rows, which are never raw), and the tagged token still signs in.
func TestStoredTokenHashesCarryTheirAlgorithm(t *testing.T) {
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
	pool := store.Pool()
	var userID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider) VALUES ('avx-it-hash-tag', 'github')
		ON CONFLICT (login_name) DO UPDATE SET oauth_provider = EXCLUDED.oauth_provider RETURNING user_id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.refresh_tokens WHERE id = 'avx-it-tag-ref'`)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.api_tokens WHERE user_id = $1`, userID)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, userID)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
	})
	if !strings.HasPrefix(hashToken("x"), "sha256$") {
		t.Fatalf("hashToken must tag its algorithm: %q", hashToken("x"))
	}
	// A new session is stored tagged.
	fresh, err := store.CreateSessionToken(ctx, userID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := pool.QueryRow(ctx, `SELECT token FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, userID).Scan(&stored); err != nil || !strings.HasPrefix(stored, "sha256$") {
		t.Fatalf("a new session's stored form = %q, %v; want sha256$…", stored, err)
	}
	// Untagged hashes from before (0.29.82–0.29.88): a marked session with a
	// refresh row, and an API token.
	legacy := strings.Repeat("5e", 32)
	untagged := strings.TrimPrefix(hashToken(legacy), "sha256$")
	now := time.Now().Unix()
	if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.user_session_tokens (token, user_id, created_at, expiration, token_hashed) VALUES ($1, $2, $3, $4, TRUE)`,
		untagged, userID, now, now+3600); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.refresh_tokens (id, user_session_token) VALUES ('avx-it-tag-ref', $1)`, untagged); err != nil {
		t.Fatal(err)
	}
	apiRaw := APITokenPrefix + strings.Repeat("7c", 32)
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.api_tokens (token_hash, user_id, label, created_by, expires_at, rate_limit_per_hour)
		VALUES ($1, $2, 'legacy', $2, NOW() + interval '1 day', 10)`, strings.TrimPrefix(hashToken(apiRaw), "sha256$"), userID); err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatal(err)
	}
	if got, err := store.ValidateSessionToken(ctx, legacy); err != nil || got != userID {
		t.Fatalf("an untagged session hash must work after the migrate tags it: %d, %v", got, err)
	}
	var ref string
	if err := pool.QueryRow(ctx, `SELECT user_session_token FROM aveloxis_ops.refresh_tokens WHERE id = 'avx-it-tag-ref'`).Scan(&ref); err != nil || ref != hashToken(legacy) {
		t.Fatalf("its refresh row must follow the tag: %q, %v", ref, err)
	}
	if id, err := store.ValidateAPIToken(ctx, apiRaw); err != nil || id.UserID != userID {
		t.Fatalf("an untagged API-token hash must work after the migrate tags it: %+v, %v", id, err)
	}
	if got, err := store.ValidateSessionToken(ctx, fresh); err != nil || got != userID {
		t.Fatalf("a tagged session must survive the migrate unchanged: %d, %v", got, err)
	}
	var untaggedLeft int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM aveloxis_ops.user_session_tokens WHERE token NOT LIKE 'sha256$%')
		+ (SELECT count(*) FROM aveloxis_ops.api_tokens WHERE token_hash NOT LIKE 'sha256$%')`).Scan(&untaggedLeft); err != nil || untaggedLeft != 0 {
		t.Fatalf("%d untagged stored hash(es) left, %v", untaggedLeft, err)
	}
}
