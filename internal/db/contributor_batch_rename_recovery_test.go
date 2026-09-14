// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.22.13 — UpsertContributorBatch must handle the deterministic-
// cntrb_id rename collision without poisoning the whole batch tx.
//
// Pre-v0.22.13 failure shape (observed in production 2026-05-18):
//
//	A contributor was first observed under cntrb_login='oldname' and
//	written with cntrb_id = PlatformUUID(1, gh_user_id). Later the
//	user renamed on GitHub. The enrichment ticker re-fetches them by
//	gh_user_id, computes the same deterministic cntrb_id, and tries
//	to INSERT with cntrb_login='newname'. ON CONFLICT (cntrb_login)
//	does not match (case-sensitive btree; the existing login is
//	different anyway), the INSERT proceeds, and trips contributors_pkey.
//	The pre-v0.22.13 captureErr/continue pattern then silently failed
//	every subsequent contributor in the batch (Postgres "current
//	transaction is aborted") and tx.Commit returned "commit
//	unexpectedly resulted in rollback." 79 of 93 23505 events in the
//	2026-05-18 Postgres log hit contributors_pkey via this path.
//
// v0.22.13 fix: SAVEPOINT per contributor + rename recovery branch
// that detects 23505/contributors_pkey and routes it to an UPDATE on
// the existing row. Preserves R2 (cntrb_login is the original-
// observation audit trail and must never be mutated by a rename); only
// gh_login (the "current display name" mirror) is updated.

// TestUpsertContributorBatchUsesSavepoint pins that the batch loop
// brackets each contributor in SAVEPOINT/RELEASE so a single failure
// does not poison the rest of the batch.
func TestUpsertContributorBatchUsesSavepoint(t *testing.T) {
	body := extractContributorBatchBodies(t)

	if !strings.Contains(body, "SAVEPOINT") {
		t.Error("UpsertContributorBatch must use SAVEPOINT to isolate per-contributor " +
			"failures from the rest of the batch transaction. Pre-v0.22.13 a single " +
			"23505 poisoned the entire tx; v0.22.13 adds savepoint isolation so other " +
			"contributors in the same batch can still commit.")
	}
	if !strings.Contains(body, "ROLLBACK TO SAVEPOINT") {
		t.Error("UpsertContributorBatch must call ROLLBACK TO SAVEPOINT on a per-contributor " +
			"failure to clear the aborted-tx state before continuing to the next contributor. " +
			"Without this, subsequent statements in the same tx fail with " +
			"'current transaction is aborted, commands ignored until end of transaction block.'")
	}
	if !strings.Contains(body, "RELEASE SAVEPOINT") {
		t.Error("UpsertContributorBatch must call RELEASE SAVEPOINT on the success path. " +
			"Postgres lazily reclaims savepoint resources but explicit RELEASE keeps the " +
			"per-tx savepoint stack shallow on large batches (hundreds of contributors).")
	}
}

// TestUpsertContributorBatchRecoversFromPkeyRenameCollision pins the
// rename-recovery branch. The condition that distinguishes a rename
// (recoverable) from any other 23505 (skip + log) is:
//
//   - pgErr.Code == "23505"
//   - pgErr.ConstraintName == "contributors_pkey"
//   - desiredCntrbID was non-NULL (we computed a deterministic UUID
//     from gh_user_id, so the colliding row IS the same person)
func TestUpsertContributorBatchRecoversFromPkeyRenameCollision(t *testing.T) {
	body := extractContributorBatchBodies(t)

	if !strings.Contains(body, "pgconn.PgError") {
		t.Error("UpsertContributorBatch must inspect the typed pgconn.PgError to distinguish " +
			"a pkey collision (rename, recoverable) from a partial-unique-index collision " +
			"(handled by ON CONFLICT already) or any other SQLSTATE.")
	}
	if !regexp.MustCompile(`pgErr\.Code\s*==\s*"23505"`).MatchString(body) {
		t.Error("UpsertContributorBatch must gate the rename-recovery branch on " +
			`pgErr.Code == "23505" (unique violation). Any other SQLSTATE means the row ` +
			"truly cannot be inserted; capture the diagnostic and skip.")
	}
	if !strings.Contains(body, "contributors_pkey") {
		t.Error("UpsertContributorBatch must gate the rename-recovery branch on " +
			`pgErr.ConstraintName == "contributors_pkey" specifically. A 23505 on ` +
			"idx_contributors_login means ON CONFLICT (cntrb_login) somehow didn't catch " +
			"the conflict (e.g. cntrb_login was empty); that's not a rename, don't route to " +
			"the rename-recovery path.")
	}

	// The recovery UPDATE itself: set gh_login to the new login, match
	// on the existing cntrb_id (the deterministic UUID we tried to
	// insert). Without these two signals the recovery cannot work.
	//
	// Anchored on the named const rather than on a fixed-size window
	// after the contributors_pkey marker. The window form (2400 chars)
	// was the scan-window anti-pattern the house retired in v0.28.18:
	// it silently stopped covering the statement the moment 2026-09-11
	// (F5) hoisted it into renameRecoveryUpdateSQL so the pre-probe
	// path and the 23505 backstop could share ONE spelling. A named
	// const is an exact region, so this can neither over-reach into the
	// unrelated gh_*/gl_* backfill UPDATE nor under-reach past a
	// refactor.
	recoverySQL := srctest.ConstBody(t, srctest.Read(t, "internal/db/postgres.go"), "renameRecoveryUpdateSQL")

	if !regexp.MustCompile(`UPDATE\s+aveloxis_data\.contributors`).MatchString(recoverySQL) {
		t.Error("renameRecoveryUpdateSQL must be an UPDATE aveloxis_data.contributors ... — " +
			"this is what actually records the rename on the existing row. Mirrors " +
			"RenameContributorGhLogin (v0.22.12) but expressed here so it runs in the batch tx.")
	}
	if !regexp.MustCompile(`gh_login\s*=\s*\$\d`).MatchString(recoverySQL) {
		t.Error("renameRecoveryUpdateSQL must set gh_login = $N unconditionally (no COALESCE), " +
			"per the v0.22.12 RenameContributorGhLogin contract. The caller knows the existing " +
			"gh_login is stale by definition — that is what a rename means.")
	}
	if !regexp.MustCompile(`WHERE\s+cntrb_id\s*=\s*\$\d::uuid`).MatchString(recoverySQL) {
		t.Error("renameRecoveryUpdateSQL must target the existing row by cntrb_id (the " +
			"deterministic UUID). That's the only join key guaranteed to identify the same " +
			"person across renames.")
	}

	// Both paths that relabel a renamed row must go through that ONE
	// const (SR-17). A second inline spelling is how the two paths
	// drift into recording renames differently.
	if n := strings.Count(body, "renameRecoveryUpdateSQL"); n < 2 {
		t.Errorf("renameRecoveryUpdateSQL is referenced %d time(s) in the contributor-upsert unit, "+
			"want at least 2 — the known-rename pre-probe path and the 23505 backstop must share "+
			"one spelling of the relabel, or they can drift into doing different things to the "+
			"same row.", n)
	}
	// An occurrence BUDGET, not a ban: extractContributorBatchBodies
	// includes the const's own declaration, so the statement is
	// EXPECTED to appear exactly once. Banning it outright would fire
	// on the very const it is protecting (it did, on first run — the
	// v0.27.145 carve-out lesson). Two or more means someone inlined a
	// second copy beside the shared one.
	relabelShape := regexp.MustCompile(`SET\s+gh_login\s*=\s*\$2,\s*\n\s*cntrb_email\s*=\s*COALESCE`)
	if n := len(relabelShape.FindAllString(body, -1)); n != 1 {
		t.Errorf("the rename-relabel UPDATE appears %d times in the contributor-upsert unit, want "+
			"exactly 1 (the shared renameRecoveryUpdateSQL const). A second inline copy is how the "+
			"pre-probe path and the 23505 backstop drift into relabelling the same row differently.", n)
	}
}

// TestUpsertContributorBatchRenameRecoveryPreservesCntrbLogin pins R2:
// cntrb_login is the durable audit trail of the original observation
// and MUST NOT be updated on rename. Only gh_login changes.
func TestUpsertContributorBatchRenameRecoveryPreservesCntrbLogin(t *testing.T) {
	body := extractContributorBatchBodies(t)

	// Find the rename-recovery UPDATE block specifically. It's
	// distinguished from the gh_*/gl_* backfill UPDATE by also
	// touching gh_login (the backfill UPDATEs don't — that's only set
	// in the rename-recovery path inside the batch upsert).
	startIdx := strings.Index(body, "contributors_pkey")
	if startIdx < 0 {
		t.Fatal("rename-recovery branch not found (contributors_pkey marker missing). " +
			"Implement TestUpsertContributorBatchRecoversFromPkeyRenameCollision's contract first.")
	}
	// Slice ~1600 chars from the contributors_pkey marker — large
	// enough to contain the recovery UPDATE statement.
	end := startIdx + 1600
	if end > len(body) {
		end = len(body)
	}
	recoveryRegion := body[startIdx:end]

	// Negative pin: the rename-recovery UPDATE must NOT set cntrb_login.
	// Any of these patterns would violate R2:
	//   SET cntrb_login = $N
	//   cntrb_login = EXCLUDED.cntrb_login
	//   cntrb_login = $N  (in the middle of a SET clause)
	bad := regexp.MustCompile(`cntrb_login\s*=\s*(\$\d|EXCLUDED)`)
	if bad.MatchString(recoveryRegion) {
		t.Error("v0.22.13 rename-recovery UPDATE must NOT modify cntrb_login. " +
			"R2 (per docs/architecture/contributor-resolution.md): cntrb_login is the " +
			"durable audit trail of the contributor's first observation and is " +
			"deliberately frozen — gh_login is the 'current display name' mirror that " +
			"tracks renames. Same invariant RenameContributorGhLogin (v0.22.12) preserves.")
	}
}

// TestUpsertContributorBatchSavepointsPerIdentity pins that the inner
// per-identity loop also gets savepoint protection so a stale identity
// row failure (rare, but possible if the (platform_id, platform_user_id)
// unique constraint is violated in some corner case the ON CONFLICT
// doesn't catch) doesn't poison the rest of the batch either.
func TestUpsertContributorBatchSavepointsPerIdentity(t *testing.T) {
	body := extractContributorBatchBodies(t)

	// Find the identity insert loop. It's the for-range over the
	// identity slice that calls contributor_identities INSERT.
	identIdx := strings.Index(body, "contributor_identities")
	if identIdx < 0 {
		t.Fatal("contributor_identities insert not found in UpsertContributorBatch")
	}
	// Slice the surrounding region (3000 chars ought to cover the loop).
	start := identIdx - 1500
	if start < 0 {
		start = 0
	}
	end := identIdx + 1500
	if end > len(body) {
		end = len(body)
	}
	identRegion := body[start:end]

	// The identity loop must use a SAVEPOINT too. Distinguish from the
	// outer contributor savepoint by checking for a *second* SAVEPOINT
	// marker within the slice. The marker name itself doesn't matter
	// (sp_N counter or any other unique scheme) — only that there is
	// one.
	spCount := strings.Count(identRegion, "SAVEPOINT")
	if spCount < 2 {
		t.Errorf("UpsertContributorBatch's per-identity loop must bracket each identity "+
			"INSERT/UPDATE in its own SAVEPOINT so a single bad identity row doesn't poison "+
			"the rest of the batch. Found %d SAVEPOINT mentions in the identity region; "+
			"expected at least 2 (SAVEPOINT + ROLLBACK TO SAVEPOINT or RELEASE SAVEPOINT).",
			spCount)
	}
}

// extractFunctionBody returns the source text of a single Go function
// from a file in the same package. Used by source-contract tests to
// constrain assertions to one function's body.
func extractFunctionBody(t *testing.T, filename, funcName string) string {
	t.Helper()
	src, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("read %s: %v", filename, err)
	}
	code := string(src)
	marker := "func (s *PostgresStore) " + funcName + "("
	startIdx := strings.Index(code, marker)
	if startIdx < 0 {
		t.Fatalf("function %s not found in %s", funcName, filename)
	}
	tail := code[startIdx+1:]
	endRel := strings.Index(tail, "\nfunc ")
	if endRel < 0 {
		endRel = len(tail)
	}
	return code[startIdx : startIdx+1+endRel]
}
