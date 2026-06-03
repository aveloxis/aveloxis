// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

// v0.25.8 — source-contract tests for:
//   1. idx_commits_cmt_ght_author_id — the missing index that caused a
//      2+ day stalled rebuild of explorer_new_contributors on aveloxis_large
//      (2026-06-02 incident). cmt_ght_author_id is a plain UUID column with
//      no REFERENCES clause, so it was invisible to the v0.22.6 FK-index
//      audit that used pg_constraint.
//   2. Two-phase commit branch in explorer_new_contributors — GROUP BY on
//      commit-side columns only, then JOIN contributors. Eliminates the
//      381 GB seqscan + hash-aggregation of 434M joined rows that the
//      pre-v0.25.8 single-phase structure required.
//   3. stampSchemaVersion moved after the error check — prevents a partial
//      migration from falsely reporting schema_version as up-to-date when
//      a lock-blocked DDL step failed silently.

// TestCommitIndexDeclaredInSchema pins the CREATE INDEX declaration in
// schema.sql. Fresh installs pick up the index from DDL without needing
// the migration step.
func TestCommitIndexDeclaredInSchema(t *testing.T) {
	src := readSchemaSQLForV0257(t)

	if !strings.Contains(src, "idx_commits_cmt_ght_author_id") {
		t.Error("schema.sql must declare idx_commits_cmt_ght_author_id — the index on commits.cmt_ght_author_id that was missing in pre-v0.25.8 and caused a 2+ day matview rebuild on aveloxis_large")
	}
	if !strings.Contains(src, "WHERE cmt_ght_author_id IS NOT NULL") {
		t.Error("idx_commits_cmt_ght_author_id must be a partial index (WHERE cmt_ght_author_id IS NOT NULL) to mirror the matview commit branch's filter and keep the index ~8% smaller than a full index")
	}
}

// TestCommitIndexDeclaredInMigration pins the execCreateIndexConcurrently
// call in migrate.go. Existing deployments get the index via this step;
// fresh installs from schema.sql already have it so the IF NOT EXISTS is
// a no-op.
func TestCommitIndexDeclaredInMigration(t *testing.T) {
	src := readMigrateGoForV0257(t)

	if !strings.Contains(src, "idx_commits_cmt_ght_author_id") {
		t.Error("migrate.go must call execCreateIndexConcurrently for idx_commits_cmt_ght_author_id so existing deployments get the index without a fresh install")
	}
	if !strings.Contains(src, "execCreateIndexConcurrently") {
		t.Error("migrate.go must use execCreateIndexConcurrently (not a plain execMigrationStep) for the index — CONCURRENTLY prevents blocking writes on the 474M-row commits table during the build")
	}
}

// TestCommitBranchGroupsByCommitColumnsOnly pins the two-phase structure:
// the inner GROUP BY must reference only commit-table columns. Including
// contributor text fields (cntrb_full_name, cntrb_login) in the GROUP BY
// forces the contributors JOIN to happen before aggregation, requiring
// PostgreSQL to join 434M rows then sort/hash that full result.
func TestCommitBranchGroupsByCommitColumnsOnly(t *testing.T) {
	src := readMatviewsSQLForV0255(t)
	region := extractMatviewBlock(t, src, "explorer_new_contributors")

	// The GROUP BY inside the commit branch's phase-1 subquery must only
	// reference commit-side columns.
	if !strings.Contains(region, "GROUP BY co.cmt_ght_author_id, co.repo_id, co.cmt_author_date") {
		t.Error("explorer_new_contributors commit branch must GROUP BY (co.cmt_ght_author_id, co.repo_id, co.cmt_author_date) — commit-side columns only — so the aggregation is indexable and doesn't require joining 434M rows with contributors first")
	}
}

// TestCommitBranchDoesNotGroupByContributorText is the negative pin.
// If cntrb_full_name or cntrb_login appear in the GROUP BY, the JOIN
// must happen before the aggregation, which was the pre-v0.25.8 root
// cause of the 381 GB seqscan and 2+ day rebuild.
func TestCommitBranchDoesNotGroupByContributorText(t *testing.T) {
	src := readMatviewsSQLForV0255(t)
	region := extractMatviewBlock(t, src, "explorer_new_contributors")

	if strings.Contains(region, "GROUP BY co.cmt_ght_author_id, co.repo_id, co.cmt_author_date,\n                      c.cntrb_full_name") {
		t.Error("explorer_new_contributors commit branch must NOT include c.cntrb_full_name in the GROUP BY — that forces the contributors JOIN before aggregation, requiring PostgreSQL to hash-aggregate 434M joined rows (pre-v0.25.8 root cause of the 2+ day rebuild)")
	}
	if strings.Contains(region, "GROUP BY co.cmt_ght_author_id, co.repo_id, co.cmt_author_date,\n                      c.cntrb_login") {
		t.Error("explorer_new_contributors commit branch must NOT include c.cntrb_login in the GROUP BY — same root cause as cntrb_full_name")
	}
}

// TestCommitBranchJoinsContributorsAfterGroupBy pins the two-phase
// structure: the contributors JOIN must appear after (outside of) the
// subquery that contains the GROUP BY. This ensures the join input is
// the small grouped result, not the raw 434M-row commits table.
func TestCommitBranchJoinsContributorsAfterGroupBy(t *testing.T) {
	src := readMatviewsSQLForV0255(t)
	region := extractMatviewBlock(t, src, "explorer_new_contributors")

	// The phase-2 JOIN must reference the grouped subquery alias, not
	// the raw commits table alias (co).
	if !strings.Contains(region, "JOIN aveloxis_data.contributors c ON c.cntrb_id = co.cmt_ght_author_id") {
		t.Error("explorer_new_contributors commit branch must JOIN contributors after the GROUP BY subquery, using the subquery alias (co.cmt_ght_author_id) as the join key")
	}
	// The phase-1 subquery must select only commit-side columns.
	if !strings.Contains(region, "SELECT co.cmt_ght_author_id,\n                       co.repo_id,\n                       co.cmt_author_date") {
		t.Error("explorer_new_contributors commit branch phase-1 subquery must SELECT only commit-side columns (cmt_ght_author_id, repo_id, cmt_author_date) — contributor text fields belong in phase 2")
	}
}

// TestStampSchemaVersionAfterErrorCheck pins the migration reliability fix:
// stampSchemaVersion must NOT be called before the error check. A partial
// migration that calls stampSchemaVersion early can stamp schema_version as
// up-to-date even when DDL steps failed (e.g. a lock-blocked ALTER TABLE
// silently timed out). CheckSchemaVersion in `aveloxis web` and `aveloxis
// api` then suppresses its warning, hiding the incomplete migration.
func TestStampSchemaVersionAfterErrorCheck(t *testing.T) {
	src := readMigrateGoForV0257(t)

	stampPos := strings.Index(src, "stampSchemaVersion(ctx, pg, logger)")
	errorCheckPos := strings.Index(src, "if len(errs) > 0 {")

	if stampPos < 0 {
		t.Fatal("stampSchemaVersion call not found in migrate.go")
	}
	if errorCheckPos < 0 {
		t.Fatal("error check (if len(errs) > 0) not found in migrate.go")
	}
	if stampPos < errorCheckPos {
		t.Errorf("stampSchemaVersion (pos %d) must appear AFTER the error check (pos %d) — "+
			"calling it before means a partial migration stamps the schema as complete, "+
			"suppressing CheckSchemaVersion warnings in web/api (v0.25.8 fix for the "+
			"2026-06-02 incident where a lock-blocked ALTER TABLE produced a false schema_version stamp)",
			stampPos, errorCheckPos)
	}
}

func readSchemaSQLForV0257(t *testing.T) string {
	t.Helper()
	return readFileForTest(t, "schema.sql")
}

func readMigrateGoForV0257(t *testing.T) string {
	t.Helper()
	return readFileForTest(t, "migrate.go")
}
