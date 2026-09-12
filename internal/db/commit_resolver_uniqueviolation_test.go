// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Source-contract tests for UpsertContributorFull's "Row exists by ID"
// branch.
//
// SUPERSEDED CONTRACT (2026-09-11, F4). These two tests used to pin the
// v0.19.1 design: the branch wrote `cntrb_login = $2` and therefore
// needed a SQLSTATE 23505 fallback plus a log line when another row
// already held that login. They are REWRITTEN, not deleted, because the
// v0.19.1 rationale below is still the right history to read — what
// changed is the answer, not the question.
//
// v0.19.1's background, verbatim, because it is still the shape being
// prevented:
//
//	ERROR:  duplicate key value violates unique constraint "idx_contributors_login"
//	DETAIL: Key (cntrb_login)=(Aashish93-stack) already exists.
//	STATEMENT: UPDATE aveloxis_data.contributors
//	           SET gh_login = $2, cntrb_login = $2, gh_user_id = ..., ...
//	           WHERE cntrb_id = $1::uuid
//
// v0.19.1 RECOVERED from that collision. The 2026-09-11 analysis of the
// chaoss.tv logs showed recovery was the wrong level to fix it at:
//
//   - That statement was the source of ALL 555 idx_contributors_login
//     violations in the 2026-09-06..09-11 production log (555 of 555;
//     the ON CONFLICT arm in the same function contributed zero), so
//     "recovered" meant 555 Postgres ERROR lines per five days burying
//     real errors.
//   - The recovery UPDATE omitted gh_login as well as cntrb_login, so
//     for every one of those events the rename was lost ENTIRELY — the
//     current-display-name mirror stayed stale. Recovery was not
//     lossless.
//   - Writing cntrb_login at all contradicted R2
//     (docs/architecture/contributor-resolution.md) and the three other
//     rename paths, one of which carries a hard NEGATIVE pin against
//     the same write (contributor_batch_rename_recovery_test.go).
//
// So the write is gone, and with it the need to recover: contributors
// carries exactly two unique indexes — contributors_pkey (cntrb_id, not
// in the SET list) and idx_contributors_login (cntrb_login, no longer
// in the SET list) — so that statement can no longer raise 23505 at
// all. The pins below now enforce the ABSENCE of the collision-causing
// write rather than the presence of its recovery, which is the stronger
// contract: there is nothing left to recover from.
//
// The behavioral half lives in contributor_login_r2_test.go.

package db

import (
	"strings"
	"testing"
)

// rowExistsBranch returns the tail of UpsertContributorFull starting at
// the "Row exists by ID" comment — the branch both tests below scope
// to. Shared so a refactor that renames the branch fails once, loudly,
// instead of silently skipping both tests.
func rowExistsBranch(t *testing.T) string {
	t.Helper()
	src := mustReadStoreSource(t, "commit_resolver_store.go")
	body := extractBatchFunc(src, "UpsertContributorFull")
	if body == "" {
		t.Fatal("could not locate UpsertContributorFull body")
	}
	idx := strings.Index(body, "Row exists by ID")
	if idx < 0 {
		t.Fatal("could not locate the 'Row exists by ID' branch comment — has the function been refactored away from this naming?")
	}
	return body[idx:]
}

// TestUpsertContributorFullDoesNotWriteCntrbLoginOnRename is the
// negative pin that replaces v0.19.1's recovery pin. It is the twin of
// TestUpsertContributorBatchRenameRecoveryPreservesCntrbLogin on the
// batch path — the two rename paths now hold the same line.
func TestUpsertContributorFullDoesNotWriteCntrbLoginOnRename(t *testing.T) {
	rest := rowExistsBranch(t)

	// The UPDATE must not assign cntrb_login. Matching the assignment
	// operator (not the bare column name) so the explanatory comment
	// above the statement, which necessarily discusses cntrb_login,
	// cannot satisfy or trip this check.
	for _, banned := range []string{
		"cntrb_login = $",
		"cntrb_login=$",
		"cntrb_login = EXCLUDED",
	} {
		if strings.Contains(rest, banned) {
			t.Errorf("UpsertContributorFull's 'Row exists by ID' branch assigns cntrb_login (%q).\n"+
				"cntrb_login is the durable audit trail of the login as FIRST observed (R2, "+
				"docs/architecture/contributor-resolution.md); gh_login is the current-display-name mirror "+
				"and is where a rename belongs. Writing cntrb_login here collides with whatever row already "+
				"holds the new login — that was 555 of 555 idx_contributors_login violations in the "+
				"2026-09-11 production log, and the recovery dropped the gh_login update with it.", banned)
		}
	}

	// The rename must still reach gh_login — the fix removes the
	// collision, it does not stop tracking the current name.
	if !strings.Contains(rest, "gh_login = $2") {
		t.Error("UpsertContributorFull's 'Row exists by ID' branch no longer updates gh_login. " +
			"Dropping cntrb_login must not also drop the current-display-name mirror, or renames " +
			"become invisible (which is exactly what the v0.19.1 recovery path did by accident).")
	}
}

// TestUpsertContributorFullReportsRenames replaces v0.19.1's
// "fallback is logged" pin. The event worth surfacing is the rename
// itself, not the failed statement it used to cause.
func TestUpsertContributorFullReportsRenames(t *testing.T) {
	rest := rowExistsBranch(t)

	if !strings.Contains(rest, "logger.Info") {
		t.Error("UpsertContributorFull's 'Row exists by ID' branch must log the rename it observes at Info. " +
			"Pre-v0.29.x this event was visible only as a recovered Postgres ERROR logged at Debug — " +
			"invisible in production, where the log level is info.")
	}
	if !strings.Contains(rest, "rename") {
		t.Error("the rename log line must name the event ('rename') so operators can grep for it " +
			"alongside the batch path's 'contributor rename recovered in batch upsert'.")
	}
}
