// Source-contract tests for v0.18.29 Fix 3: ContributorResolver.Resolve
// uses ON CONFLICT (cntrb_id) when the platform user has a numeric ID,
// so concurrent inserts of the same person under different login strings
// route to DO UPDATE instead of tripping contributors_pkey.
//
// Background: at v0.18.28, the INSERT in contributors.go:92-103 specifies
// the deterministic cntrb_id (PlatformUUID(platform, userID)) as $1 but
// uses ON CONFLICT (cntrb_login) WHERE cntrb_login != ''. When two
// workers race to insert the same numeric user under different login
// strings (historical login drift across repos, GitHub renames, or just
// two workers seeing the same hot user concurrently), the cntrb_login
// conflict check fails to match (different login values) and the INSERT
// proceeds — then explodes on contributors_pkey because the cntrb_id
// already exists. The Postgres logs in production show this firing
// thousands of times per day on a 40K-repo fleet.
//
// The fix: when userID > 0 (deterministic UUID path), use ON CONFLICT
// (cntrb_id) DO UPDATE — that's the constraint actually firing. The
// userID == 0 path (random UUID for email-only contributors) keeps the
// partial unique index target since cntrb_login is the natural key
// there.

package db

import (
	"strings"
	"testing"
)

// extractResolverFunc locates a method on *ContributorResolver in the
// given source string. Boundary is the next top-level `func ` token.
func extractResolverFunc(src, name string) string {
	marker := "func (r *ContributorResolver) " + name + "("
	idx := strings.Index(src, marker)
	if idx < 0 {
		return ""
	}
	rest := src[idx:]
	if end := strings.Index(rest[1:], "\nfunc "); end > 0 {
		return rest[:end+1]
	}
	return rest
}

// TestResolveSourceUsesCntrbIdConflictForKnownUserID is the headline
// contract: the deterministic-UUID path must target cntrb_id.
func TestResolveSourceUsesCntrbIdConflictForKnownUserID(t *testing.T) {
	src := mustReadStoreSource(t, "contributors.go")
	body := extractResolverFunc(src, "Resolve")
	if body == "" {
		t.Fatal("could not locate ContributorResolver.Resolve body")
	}

	if !strings.Contains(body, "ON CONFLICT (cntrb_id)") {
		t.Error("ContributorResolver.Resolve must use ON CONFLICT (cntrb_id) for the userID > 0 path. " +
			"Currently uses ON CONFLICT (cntrb_login) WHERE cntrb_login != '', which doesn't match " +
			"the constraint that actually fires (contributors_pkey) when two workers race to insert " +
			"the same numeric platform user under different login strings.")
	}
}

// TestResolveSourceUpdatesLoginOnConflict pins the rename-handling
// behavior: when ON CONFLICT (cntrb_id) fires, the DO UPDATE clause
// must update cntrb_login to the new value. Without it, a renamed
// GitHub user's row keeps the stale login forever.
func TestResolveSourceUpdatesLoginOnConflict(t *testing.T) {
	src := mustReadStoreSource(t, "contributors.go")
	body := extractResolverFunc(src, "Resolve")
	if body == "" {
		t.Skip("Resolve not yet refactored")
	}

	// We expect the DO UPDATE clause for the cntrb_id path to mention
	// cntrb_login = ... EXCLUDED.cntrb_login or similar. The COALESCE
	// pattern is fine — newest non-empty observation wins.
	idIdx := strings.Index(body, "ON CONFLICT (cntrb_id)")
	if idIdx < 0 {
		t.Skip("cntrb_id branch not yet present; covered by sibling test")
	}
	// Slice from the cntrb_id ON CONFLICT to the end of that SQL block
	// (loosely: until a closing backtick or RETURNING).
	rest := body[idIdx:]
	endIdx := strings.Index(rest, "RETURNING")
	if endIdx < 0 {
		endIdx = len(rest)
	}
	clause := rest[:endIdx]
	if !strings.Contains(clause, "cntrb_login") {
		t.Error("the ON CONFLICT (cntrb_id) DO UPDATE clause must update cntrb_login. " +
			"Without it, a renamed user's row keeps the stale login indefinitely.")
	}
}

// TestResolveSourceKeepsLoginPathForRandomUUID pins that the userID == 0
// path still uses the partial unique index. For email-only contributors
// (no platform user) the cntrb_id is random, so two observations of the
// same person would generate different cntrb_ids — but the same login.
// The login partial unique index is the right target there.
func TestResolveSourceKeepsLoginPathForRandomUUID(t *testing.T) {
	src := mustReadStoreSource(t, "contributors.go")
	body := extractResolverFunc(src, "Resolve")
	if body == "" {
		t.Skip("Resolve not yet refactored")
	}
	if !strings.Contains(body, "ON CONFLICT (cntrb_login)") {
		t.Error("Resolve must keep the ON CONFLICT (cntrb_login) WHERE cntrb_login != '' branch " +
			"for the userID == 0 path (random UUID, login is the natural identity)")
	}
}

// TestResolveSourceBranchesOnUserID enforces that the function actually
// has a branch — without it, a single SQL statement can't target both
// constraints (Postgres has no multi-target ON CONFLICT).
func TestResolveSourceBranchesOnUserID(t *testing.T) {
	src := mustReadStoreSource(t, "contributors.go")
	body := extractResolverFunc(src, "Resolve")
	if body == "" {
		t.Skip("Resolve not yet refactored")
	}
	// Look for a branch on userID (or platformID/userID). We accept any
	// of `userID > 0`, `userID != 0`, or a switch/early-return pattern.
	if !strings.Contains(body, "userID > 0") && !strings.Contains(body, "userID != 0") {
		t.Error("Resolve must branch on userID > 0 to choose between the cntrb_id-targeted " +
			"upsert (deterministic UUID) and the cntrb_login-targeted upsert (random UUID). " +
			"Postgres has no multi-target ON CONFLICT, so two SQL statements are required.")
	}
}
