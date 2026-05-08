// Tests for UpsertOAuthUser's defensive guards against blank-login OAuth
// callbacks. The motivating incident: a user logged into the production
// web GUI, the callback's `info.Login` was empty (GitHub /user response
// returned no Login field on that one call), and UpsertOAuthUser inserted
// a fresh user_id=2 with login_name=''. Subsequent logins kept matching
// that blank row, so the user's groups (tied to the real user_id=1)
// disappeared from the dashboard. Two latent bugs surfaced:
//
//  1. UpsertOAuthUser accepts an empty Login and inserts the row anyway.
//  2. The lookup-by-login SELECT treats ANY non-nil error as "user not
//     found" and proceeds to INSERT, so a transient DB error during
//     lookup also creates a duplicate row.
//
// These tests pin the fix for both. Pure-source contract tests run
// without a database; the integration cases gate on AVELOXIS_TEST_DB.

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestUpsertOAuthUserRejectsEmptyLogin is the headline behavioral test:
// pass an OAuthUserInfo with Login="" and the call must return the
// ErrEmptyLogin sentinel WITHOUT touching the database. We construct
// a zero-value *PostgresStore (s.pool is nil); if the function reaches
// any pool method before the empty-login guard, the test panics
// (caught by the test runner as a failure).
func TestUpsertOAuthUserRejectsEmptyLogin(t *testing.T) {
	s := &PostgresStore{} // no pool; any DB touch nil-panics
	_, err := s.UpsertOAuthUser(context.Background(), OAuthUserInfo{
		Login:    "",
		Provider: "github",
	})
	if err == nil {
		t.Fatal("UpsertOAuthUser with empty Login: err = nil, want ErrEmptyLogin")
	}
	if !errors.Is(err, ErrEmptyLogin) {
		t.Errorf("UpsertOAuthUser with empty Login: err = %v, want errors.Is(err, ErrEmptyLogin) == true", err)
	}
}

// TestUpsertOAuthUserRejectsWhitespaceLogin guards against the
// near-miss "blank but technically non-empty" case — a Login of just
// spaces or tabs is semantically the same as empty for our purposes
// and should be rejected the same way.
func TestUpsertOAuthUserRejectsWhitespaceLogin(t *testing.T) {
	s := &PostgresStore{}
	for _, login := range []string{" ", "\t", "  \n  "} {
		_, err := s.UpsertOAuthUser(context.Background(), OAuthUserInfo{
			Login:    login,
			Provider: "github",
		})
		if !errors.Is(err, ErrEmptyLogin) {
			t.Errorf("Login=%q: err = %v, want ErrEmptyLogin", login, err)
		}
	}
}

// TestErrEmptyLoginIsExported pins the contract that callers in
// internal/web can use errors.Is to detect this condition. If the
// sentinel goes away or gets renamed without callers updating, this
// fails before ship.
func TestErrEmptyLoginIsExported(t *testing.T) {
	if ErrEmptyLogin == nil {
		t.Fatal("ErrEmptyLogin sentinel is nil — must be a non-nil error value")
	}
	if !strings.Contains(ErrEmptyLogin.Error(), "login") {
		t.Errorf("ErrEmptyLogin.Error() = %q, want a message mentioning 'login'", ErrEmptyLogin.Error())
	}
}

// TestUpsertOAuthUserSourceUsesErrNoRows enforces that the lookup
// path distinguishes pgx.ErrNoRows from real DB errors. The
// pre-fix code was `if err != nil { /* INSERT */ }`, which on any
// transient DB failure creates a duplicate user row. After the fix
// the source must reference pgx.ErrNoRows so a real error bubbles
// up instead of triggering a spurious INSERT.
func TestUpsertOAuthUserSourceUsesErrNoRows(t *testing.T) {
	src := mustReadSource(t, "web_store.go")

	body := extractFunc(src, "UpsertOAuthUser")
	if body == "" {
		t.Fatal("could not locate UpsertOAuthUser function body")
	}
	if !strings.Contains(body, "pgx.ErrNoRows") {
		t.Error("UpsertOAuthUser must reference pgx.ErrNoRows so that real DB errors don't masquerade as not-found and trigger spurious INSERTs")
	}
	if !strings.Contains(body, "errors.Is") {
		t.Error("UpsertOAuthUser must use errors.Is to detect ErrNoRows (string match on the wrapped error is unreliable)")
	}
}

// TestUpsertOAuthUserSourceChecksEmptyLogin pins the empty-login
// guard in source so a future refactor can't silently remove it
// while leaving the runtime test passing on the constructor path.
func TestUpsertOAuthUserSourceChecksEmptyLogin(t *testing.T) {
	src := mustReadSource(t, "web_store.go")
	body := extractFunc(src, "UpsertOAuthUser")
	if body == "" {
		t.Fatal("could not locate UpsertOAuthUser function body")
	}
	if !strings.Contains(body, "ErrEmptyLogin") {
		t.Error("UpsertOAuthUser must reference ErrEmptyLogin to short-circuit on blank Login before any DB write")
	}
}

// TestUpsertOAuthUserEmptyLoginIntegration exercises the live path
// against a scratch Postgres (gated on AVELOXIS_TEST_DB). Verifies
// that a blank-login call inserts ZERO rows, regardless of whether
// other rows already exist with login_name=''.
func TestUpsertOAuthUserEmptyLoginIntegration(t *testing.T) {
	conn := os.Getenv("AVELOXIS_TEST_DB")
	if conn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, conn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Snapshot row count before.
	var before int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM aveloxis_ops.users WHERE login_name = ''`).Scan(&before); err != nil {
		t.Fatalf("snapshot before: %v", err)
	}

	_, err = store.UpsertOAuthUser(ctx, OAuthUserInfo{
		Login:    "",
		Provider: "github",
	})
	if !errors.Is(err, ErrEmptyLogin) {
		t.Fatalf("blank login: err = %v, want ErrEmptyLogin", err)
	}

	var after int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM aveloxis_ops.users WHERE login_name = ''`).Scan(&after); err != nil {
		t.Fatalf("snapshot after: %v", err)
	}
	if after != before {
		t.Errorf("blank-login row count changed: before=%d after=%d, want unchanged (rejected before INSERT)", before, after)
	}
}

// mustReadSource reads a file from the package directory and returns
// its contents. Test fails fast if the file is missing.
func mustReadSource(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}

// extractFunc returns the body of the named top-level function from
// the given Go source string. Returns "" if the function isn't found.
// Boundary detection is line-based: starts at "func ... <name>(" and
// stops at the next top-level `func ` definition.
func extractFunc(src, name string) string {
	marker := "func (s *PostgresStore) " + name + "("
	idx := strings.Index(src, marker)
	if idx < 0 {
		return ""
	}
	rest := src[idx:]
	// Find the next top-level func definition, which we treat as the
	// end-of-function boundary.
	if end := strings.Index(rest[1:], "\nfunc "); end > 0 {
		return rest[:end+1]
	}
	return rest
}
