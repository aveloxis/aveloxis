// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.82 — session tokens hashed at rest; operator-issued API tokens with
// an hourly allowance and editable defaults (administered in aveloxis-gui).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

func tokenTestStore(t *testing.T) (*PostgresStore, context.Context) {
	t.Helper()
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
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatal(err)
	}
	return store, ctx
}

func tokenTestUser(t *testing.T, store *PostgresStore, ctx context.Context, login string) int {
	t.Helper()
	var id int
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider) VALUES ($1, 'github')
		ON CONFLICT (login_name) DO UPDATE SET oauth_provider = EXCLUDED.oauth_provider
		RETURNING user_id`, login).Scan(&id); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = store.pool.Exec(c, `DELETE FROM aveloxis_ops.api_tokens WHERE user_id = $1 OR created_by = $1`, id)
		_, _ = store.pool.Exec(c, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, id)
		_, _ = store.pool.Exec(c, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, id)
	})
	return id
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// A session token is stored only as its SHA-256: a read of the table (or a
// backup) yields nothing a caller can present. The raw token still
// validates and still logs out.
func TestSessionTokensAreStoredHashed(t *testing.T) {
	store, ctx := tokenTestStore(t)
	uid := tokenTestUser(t, store, ctx, "avx-it-tokhash")
	raw, err := store.CreateSessionToken(ctx, uid, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_session_tokens WHERE token = $1`, raw).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("the raw session token is stored; only its hash may be")
	}
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_session_tokens WHERE token = $1 AND user_id = $2`, "sha256$"+sha256Hex(raw), uid).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("the stored token must be the raw token's SHA-256 hex, tagged sha256$ (found %d rows)", n)
	}
	if got, err := store.ValidateSessionToken(ctx, raw); err != nil || got != uid {
		t.Fatalf("the raw token must validate: (%d, %v)", got, err)
	}
	if _, err := store.ValidateSessionToken(ctx, sha256Hex(raw)); !errors.Is(err, ErrInvalidSessionToken) {
		t.Error("presenting the stored hash must not authenticate (it is not the token)")
	}
	if err := store.DeleteSessionToken(ctx, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ValidateSessionToken(ctx, raw); !errors.Is(err, ErrInvalidSessionToken) {
		t.Errorf("a logged-out token must be invalid, got %v", err)
	}
}

// The one-time migration hashes every existing row in place (nobody is
// logged out: the browser's raw token hashes to the stored value) and
// repoints refresh_tokens, whose foreign key references the token. Run in a
// transaction that is rolled back, against the real tables.
func TestSessionTokenHashMigrationHashesInPlace(t *testing.T) {
	store, ctx := tokenTestStore(t)
	uid := tokenTestUser(t, store, ctx, "avx-it-tokmig")
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	const raw = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	now := time.Now().Unix()
	if _, err := tx.Exec(ctx, `INSERT INTO aveloxis_ops.user_session_tokens (token, user_id, created_at, expiration) VALUES ($1, $2, $3, $4)`, raw, uid, now, now+3600); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO aveloxis_ops.refresh_tokens (id, user_session_token) VALUES ('avx-it-refresh', $1)`, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, hashExistingSessionTokensSQL); err != nil {
		t.Fatal(err)
	}
	var tok, ref string
	if err := tx.QueryRow(ctx, `SELECT token FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, uid).Scan(&tok); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(ctx, `SELECT user_session_token FROM aveloxis_ops.refresh_tokens WHERE id = 'avx-it-refresh'`).Scan(&ref); err != nil {
		t.Fatal(err)
	}
	if tok != "sha256$"+sha256Hex(raw) || ref != tok {
		t.Fatalf("token=%q refresh=%q, want both %q", tok, ref, "sha256$"+sha256Hex(raw))
	}
	// The deferred foreign key is checked at commit: SET CONSTRAINTS ALL
	// IMMEDIATE proves the pair is consistent inside the transaction.
	if _, err := tx.Exec(ctx, `SET CONSTRAINTS ALL IMMEDIATE`); err != nil {
		t.Fatalf("the hashed pair violates refresh_tokens' foreign key: %v", err)
	}
}

func TestAPITokenLifecycle(t *testing.T) {
	store, ctx := tokenTestStore(t)
	owner := tokenTestUser(t, store, ctx, "avx-it-apitok-owner")
	admin := tokenTestUser(t, store, ctx, "avx-it-apitok-admin")

	raw, tok, err := store.CreateAPIToken(ctx, APITokenGrant{UserID: owner, Label: "research script", CreatedBy: admin, Lifetime: 48 * time.Hour, RateLimitPerHour: 1234})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(raw, APITokenPrefix) || len(raw) != len(APITokenPrefix)+64 {
		t.Fatalf("raw token %q: want %q + 64 hex characters", raw, APITokenPrefix)
	}
	var stored string
	if err := store.pool.QueryRow(ctx, `SELECT token_hash FROM aveloxis_ops.api_tokens WHERE token_id = $1`, tok.TokenID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != "sha256$"+sha256Hex(raw) {
		t.Fatal("an API token is stored only as its SHA-256 hex, tagged sha256$")
	}
	if tok.UserID != owner || tok.Label != "research script" || tok.RateLimitPerHour != 1234 || tok.CreatedBy != admin {
		t.Fatalf("created token = %+v", tok)
	}
	if d := time.Until(tok.ExpiresAt); d < 47*time.Hour || d > 49*time.Hour {
		t.Fatalf("expires_at %v is not about 48 h from now", tok.ExpiresAt)
	}

	id, err := store.ValidateAPIToken(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	if id.TokenID != tok.TokenID || id.UserID != owner || id.RateLimitPerHour != 1234 {
		t.Fatalf("validated identity = %+v", id)
	}
	var used *time.Time
	if err := store.pool.QueryRow(ctx, `SELECT last_used_at FROM aveloxis_ops.api_tokens WHERE token_id = $1`, tok.TokenID).Scan(&used); err != nil {
		t.Fatal(err)
	}
	if used == nil {
		t.Error("a validation stamps last_used_at")
	}
	for _, bad := range []string{"", APITokenPrefix, APITokenPrefix + strings.Repeat("0", 64), stored} {
		if _, err := store.ValidateAPIToken(ctx, bad); !errors.Is(err, ErrInvalidAPIToken) {
			t.Errorf("ValidateAPIToken(%q) = %v, want ErrInvalidAPIToken", bad, err)
		}
	}

	list, err := store.ListAPITokens(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found *APIToken
	for i := range list {
		if list[i].TokenID == tok.TokenID {
			found = &list[i]
		}
	}
	if found == nil || found.OwnerLogin != "avx-it-apitok-owner" || found.CreatedByLogin != "avx-it-apitok-admin" || found.RevokedAt != nil {
		t.Fatalf("listed token = %+v", found)
	}

	if err := store.RevokeAPIToken(ctx, tok.TokenID, admin); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ValidateAPIToken(ctx, raw); !errors.Is(err, ErrInvalidAPIToken) {
		t.Errorf("a revoked token must be invalid, got %v", err)
	}
	if err := store.RevokeAPIToken(ctx, tok.TokenID, admin); err != nil {
		t.Errorf("revoking twice is a no-op, got %v", err)
	}
	if err := store.RevokeAPIToken(ctx, -1, admin); !errors.Is(err, ErrAPITokenNotFound) {
		t.Errorf("revoking an unknown token = %v, want ErrAPITokenNotFound", err)
	}

	// Expired on arrival: a lifetime that has already passed.
	expired, _, err := store.CreateAPIToken(ctx, APITokenGrant{UserID: owner, Label: "expired", CreatedBy: admin, Lifetime: time.Nanosecond, RateLimitPerHour: 1})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := store.ValidateAPIToken(ctx, expired); !errors.Is(err, ErrInvalidAPIToken) {
		t.Errorf("an expired token must be invalid, got %v", err)
	}

	// A grant must name a real owner and positive limits.
	for name, g := range map[string]APITokenGrant{
		"no owner":       {UserID: 0, Label: "x", CreatedBy: admin, Lifetime: time.Hour, RateLimitPerHour: 1},
		"unknown owner":  {UserID: -7, Label: "x", CreatedBy: admin, Lifetime: time.Hour, RateLimitPerHour: 1},
		"zero lifetime":  {UserID: owner, Label: "x", CreatedBy: admin, Lifetime: 0, RateLimitPerHour: 1},
		"zero allowance": {UserID: owner, Label: "x", CreatedBy: admin, Lifetime: time.Hour, RateLimitPerHour: 0},
		"no label":       {UserID: owner, Label: "  ", CreatedBy: admin, Lifetime: time.Hour, RateLimitPerHour: 1},
	} {
		if _, _, err := store.CreateAPIToken(ctx, g); err == nil {
			t.Errorf("%s: the grant must be refused", name)
		}
	}
}

func TestAPITokenSettingsDefaultsAndUpdate(t *testing.T) {
	store, ctx := tokenTestStore(t)
	admin := tokenTestUser(t, store, ctx, "avx-it-apitok-settings")
	before, err := store.GetAPITokenSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = store.SetAPITokenSettings(context.Background(), APITokenSettings{DefaultRateLimitPerHour: before.DefaultRateLimitPerHour, DefaultLifetimeDays: before.DefaultLifetimeDays}, 0)
	})
	// The seeded defaults (operator decision 2026-10-08): this package's
	// database is fresh, and nothing else in it changes the settings.
	if DefaultAPITokenRateLimitPerHour != 5000 || DefaultAPITokenLifetimeDays != 30 {
		t.Fatalf("defaults %d/h, %d days; the operator chose 5,000/h and 30 days", DefaultAPITokenRateLimitPerHour, DefaultAPITokenLifetimeDays)
	}
	if before.DefaultRateLimitPerHour != DefaultAPITokenRateLimitPerHour || before.DefaultLifetimeDays != DefaultAPITokenLifetimeDays {
		t.Fatalf("the migrate seeds %+v, want %d/h and %d days", before, DefaultAPITokenRateLimitPerHour, DefaultAPITokenLifetimeDays)
	}
	if err := store.SetAPITokenSettings(ctx, APITokenSettings{DefaultRateLimitPerHour: 777, DefaultLifetimeDays: 9}, admin); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetAPITokenSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.DefaultRateLimitPerHour != 777 || got.DefaultLifetimeDays != 9 || got.UpdatedBy != admin {
		t.Fatalf("settings after update = %+v", got)
	}
	for _, bad := range []APITokenSettings{{0, 9, 0, time.Time{}}, {777, 0, 0, time.Time{}}, {-1, 9, 0, time.Time{}}} {
		if err := store.SetAPITokenSettings(ctx, bad, admin); err == nil {
			t.Errorf("settings %+v must be refused", bad)
		}
	}
}

// The 0.29.82 ASVS review: A3 (a token's lifetime is capped at
// MaxAPITokenLifetimeDays = 365, operator decision 2026-10-08; a label at
// MaxAPITokenLabelLength) and A6 (APITokenActive, the per-call recheck).
func TestAPITokenLimitsAndActiveCheck(t *testing.T) {
	store, ctx := tokenTestStore(t)
	owner := tokenTestUser(t, store, ctx, "avx-it-apitok-limits")
	if MaxAPITokenLifetimeDays != 365 {
		t.Fatalf("MaxAPITokenLifetimeDays = %d; the operator chose 365", MaxAPITokenLifetimeDays)
	}
	day := 24 * time.Hour
	if _, _, err := store.CreateAPIToken(ctx, APITokenGrant{UserID: owner, Label: "x", Lifetime: (MaxAPITokenLifetimeDays + 1) * day, RateLimitPerHour: 1}); err == nil {
		t.Error("a lifetime past the maximum must be refused by the store")
	}
	if _, _, err := store.CreateAPIToken(ctx, APITokenGrant{UserID: owner, Label: strings.Repeat("é", MaxAPITokenLabelLength+1), Lifetime: day, RateLimitPerHour: 1}); err == nil {
		t.Error("a label longer than the maximum (in characters) must be refused by the store")
	}
	raw, tok, err := store.CreateAPIToken(ctx, APITokenGrant{UserID: owner, Label: strings.Repeat("é", MaxAPITokenLabelLength), Lifetime: MaxAPITokenLifetimeDays * day, RateLimitPerHour: 1})
	if err != nil {
		t.Fatalf("the maximum lifetime and label length are allowed: %v", err)
	}
	if active, err := store.APITokenActive(ctx, raw); err != nil || !active {
		t.Fatalf("a fresh token is active: (%v, %v)", active, err)
	}
	if err := store.RevokeAPIToken(ctx, tok.TokenID, 0); err != nil {
		t.Fatal(err)
	}
	if active, err := store.APITokenActive(ctx, raw); err != nil || active {
		t.Fatalf("a revoked token is not active: (%v, %v)", active, err)
	}
	if active, err := store.APITokenActive(ctx, "not-a-token"); err != nil || active {
		t.Fatalf("an unknown token is not active: (%v, %v)", active, err)
	}
	if err := store.SetAPITokenSettings(ctx, APITokenSettings{DefaultRateLimitPerHour: 1, DefaultLifetimeDays: MaxAPITokenLifetimeDays + 1}, 0); err == nil {
		t.Error("a default lifetime past the maximum must be refused")
	}
}

// A request carrying an API token runs without admin privilege: the store's
// admin lookups answer false for it (L10 on the ASVS fixes).
func TestIsUserAdminHonoursWithoutAdminPrivilege(t *testing.T) {
	store, ctx := tokenTestStore(t)
	uid := tokenTestUser(t, store, ctx, "avx-it-noadmin")
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.users SET admin = TRUE WHERE user_id = $1`, uid); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.IsUserAdmin(ctx, uid); err != nil || !ok {
		t.Fatalf("the account is an admin: (%v, %v)", ok, err)
	}
	dropped := WithoutAdminPrivilege(ctx)
	if !AdminPrivilegeDropped(dropped) || AdminPrivilegeDropped(ctx) {
		t.Fatal("WithoutAdminPrivilege marks only the derived context")
	}
	if ok, err := store.IsUserAdmin(dropped, uid); err != nil || ok {
		t.Fatalf("IsUserAdmin under WithoutAdminPrivilege = (%v, %v), want false", ok, err)
	}
}
