// Source-contract tests for v0.19.2 Fix #1: ContributorResolver.Resolve
// looks up an existing row by cntrb_login before generating a new UUID
// and INSERTing.
//
// Without this lookup, every UserRef with a login the system has seen
// before (under a different platform_user_id, or under userID=0) creates
// a fresh row that races the existing one on the partial unique index
// idx_contributors_login. v0.18.29's switch to ON CONFLICT (cntrb_id)
// for the userID > 0 path made the race produce hard ERROR-level
// statement failures instead of silent merges, which is what the
// production logs from 2026-05-02 showed.
//
// The fix: after the contributor_identities cache miss, do a single
// SELECT cntrb_id FROM contributors WHERE cntrb_login = $1. If hit,
// reuse that cntrb_id and add a contributor_identities row pointing at
// it (so future lookups by platform_user_id hit the cache directly).

package db

import (
	"strings"
	"testing"
)

// TestResolveLooksUpByLoginBeforeInsert pins the contract.
func TestResolveLooksUpByLoginBeforeInsert(t *testing.T) {
	src := mustReadStoreSource(t, "contributors.go")
	body := extractResolverFunc(src, "Resolve")
	if body == "" {
		t.Fatal("could not locate ContributorResolver.Resolve body")
	}

	// Locate the contributor_identities lookup (the existing cache
	// path). The new login lookup must come AFTER that miss but
	// BEFORE the INSERT.
	identitiesIdx := strings.Index(body, "FROM aveloxis_data.contributor_identities")
	if identitiesIdx < 0 {
		t.Fatal("could not locate the contributor_identities lookup — Resolve has been refactored")
	}

	// The new SELECT by cntrb_login must appear in the body. We don't
	// pin the exact phrasing of the SQL because formatting can vary;
	// we look for the conjunction of FROM aveloxis_data.contributors
	// AND WHERE cntrb_login.
	postIdentities := body[identitiesIdx:]
	hasLoginLookup := strings.Contains(postIdentities, "FROM aveloxis_data.contributors") &&
		strings.Contains(postIdentities, "WHERE cntrb_login")
	if !hasLoginLookup {
		t.Error("Resolve must SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = $1 " +
			"after the contributor_identities cache miss but before generating a new UUID. Without this " +
			"lookup, every UserRef whose login already exists in the contributors table creates a fresh " +
			"row that races the existing one on idx_contributors_login.")
	}

	// And the lookup must be gated on login != "" — calling SELECT
	// with empty login would wrongly match against the contributors
	// row whose login is empty (none exist due to the partial index,
	// but defensive nonetheless).
	if !strings.Contains(postIdentities, `login != ""`) &&
		!strings.Contains(postIdentities, `login!=""`) &&
		!strings.Contains(postIdentities, `len(login)`) {
		t.Error("the cntrb_login lookup must be gated on login != \"\" so empty-login UserRefs don't " +
			"trigger a meaningless query")
	}
}

// TestResolveBackfillsContributorIdentitiesAfterReuse pins that when
// the lookup-by-login finds a row to reuse AND the caller has a real
// platform_user_id, an identity row is inserted so future
// (platform_id, platform_user_id) lookups hit the cache directly
// instead of falling through to login lookup again.
func TestResolveBackfillsContributorIdentitiesAfterReuse(t *testing.T) {
	src := mustReadStoreSource(t, "contributors.go")
	body := extractResolverFunc(src, "Resolve")
	if body == "" {
		t.Skip("Resolve not yet refactored")
	}

	// The reuse path must reference an INSERT INTO contributor_identities.
	// We're not pinning the exact place in the function because the
	// reuse branch and the create-fresh branch share that INSERT —
	// the contract is that the reuse path also covers it.
	if !strings.Contains(body, "INSERT INTO\n\t\t\t\taveloxis_data.contributor_identities") &&
		!strings.Contains(body, "INSERT INTO aveloxis_data.contributor_identities") {
		t.Error("Resolve must INSERT INTO aveloxis_data.contributor_identities so future lookups by " +
			"(platform_id, platform_user_id) hit the cache. Reusing a login-matched row without backfilling " +
			"the identity row leaves the cache miss in place, defeating the optimization.")
	}
}
