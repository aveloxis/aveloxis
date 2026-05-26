// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.25.5 — source-contract tests for the matview rewrites + four
// new indexes. These pin the SHAPE of each fix so a future refactor
// can't accidentally revert the work that took explorer_new_contributors
// from 16h+ to 1-3h on aveloxis_large.

// TestLibyearViewsHaveProperJoinCondition pins the v0.25.5 fix to the
// libyear trio. Pre-v0.25.5 they cross-joined repos × repo_deps_libyear
// (no JOIN condition) → 33-billion-row intermediate at fleet scale.
// The fix is `JOIN ... USING (repo_id)`.
func TestLibyearViewsHaveProperJoinCondition(t *testing.T) {
	src := readMatviewsSQLForV0255(t)

	// explorer_libyear_summary must use a proper JOIN (USING).
	summaryRegion := extractMatviewBlock(t, src, "explorer_libyear_summary")
	if !strings.Contains(summaryRegion, "JOIN aveloxis_data.repo_deps_libyear b USING (repo_id)") {
		t.Error("explorer_libyear_summary must use `JOIN ... USING (repo_id)` — pre-v0.25.5 used a cross-join (comma syntax with no join condition) that produced 33-billion-row intermediates and semantically incorrect data (fleet-wide averages instead of per-repo).")
	}
	if !strings.Contains(summaryRegion, "(b.data_collection_date)::date") {
		t.Error("explorer_libyear_summary must group by b.data_collection_date (when libyear was collected), not a.data_collection_date (when the repo catalog row was last touched). Pre-v0.25.5 used the wrong column.")
	}

	// explorer_libyear_detail must use a proper JOIN.
	detailRegion := extractMatviewBlock(t, src, "explorer_libyear_detail")
	if !strings.Contains(detailRegion, "JOIN aveloxis_data.repo_deps_libyear b USING (repo_id)") {
		t.Error("explorer_libyear_detail must use `JOIN ... USING (repo_id)` — same cross-join bug as _summary, same fix.")
	}
}

// TestExplorerLibyearAllIsAViewAlias pins that explorer_libyear_all
// was converted from MATERIALIZED VIEW to a regular VIEW that selects
// from explorer_libyear_summary. The pre-v0.25.5 byte-for-byte-duplicate
// SQL is now expressed as an alias, eliminating storage + rebuild cost.
func TestExplorerLibyearAllIsAViewAlias(t *testing.T) {
	src := readMatviewsSQLForV0255(t)

	if !strings.Contains(src, "CREATE OR REPLACE VIEW aveloxis_data.explorer_libyear_all AS") {
		t.Error("explorer_libyear_all must be declared as a regular VIEW (not MATERIALIZED VIEW). Pre-v0.25.5 was byte-for-byte identical to explorer_libyear_summary; v0.25.5 made it a VIEW alias that selects from _summary.")
	}
	if !strings.Contains(src, "SELECT * FROM aveloxis_data.explorer_libyear_summary") {
		t.Error("explorer_libyear_all VIEW must SELECT * FROM aveloxis_data.explorer_libyear_summary — that's how the alias preserves the queryable name without duplicating storage.")
	}

	// Negative pin — the old MATERIALIZED VIEW declaration must be gone.
	// We allow a "DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_libyear_all" line
	// (intentionally dropping the old matview before creating the alias view).
	body := src
	if strings.Contains(body, "CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_libyear_all") {
		t.Error("explorer_libyear_all must NOT be declared as MATERIALIZED VIEW post-v0.25.5 — converted to a regular VIEW alias.")
	}
}

// TestAugurNewContributorsDropped pins that augur_new_contributors is
// dropped (not just aliased). Per operator decision 2026-05-26: drop
// the byte-for-byte duplicate of explorer_contributor_actions outright.
// 8Knot tooling that referenced augur_new_contributors needs to update
// to explorer_contributor_actions.
func TestAugurNewContributorsDropped(t *testing.T) {
	src := readMatviewsSQLForV0255(t)

	// Must NOT contain a CREATE for augur_new_contributors (matview or view).
	if strings.Contains(src, "CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.augur_new_contributors") ||
		strings.Contains(src, "CREATE OR REPLACE VIEW aveloxis_data.augur_new_contributors") {
		t.Error("augur_new_contributors must NOT be created (matview or view) post-v0.25.5 — operator dropped it as redundant with explorer_contributor_actions.")
	}

	// Must contain the DROP statement so existing deployments lose it.
	if !strings.Contains(src, "DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.augur_new_contributors") {
		t.Error("matviews.sql must DROP augur_new_contributors so existing v0.25.4 deployments lose the stale matview after running aveloxis refresh-views.")
	}
}

// TestRefreshListNoLongerIncludesDroppedViews pins that matviewNames
// in matviews.go has been updated to remove augur_new_contributors AND
// explorer_libyear_all (the latter is now a regular view, not a matview).
func TestRefreshListNoLongerIncludesDroppedViews(t *testing.T) {
	src := readSourceFile(t, "matviews.go")

	// matviewNames slice must NOT reference the dropped/aliased views
	// (the refresh loop would error every cycle if it tried).
	for _, dropped := range []string{
		`"aveloxis_data.augur_new_contributors"`,
		`"aveloxis_data.explorer_libyear_all"`,
	} {
		if strings.Contains(src, dropped) {
			t.Errorf("matviews.go matviewNames must NOT include %s (dropped/aliased in v0.25.5). Refreshing it would fail every cycle.", dropped)
		}
	}
}

// TestExplorerNewContributorsHasCanonicalCTE pins the v0.25.5 CTE hoist.
// Pre-v0.25.5 the canonical_full_names DISTINCT ON subquery was inlined
// in each of 7 UNION branches → 7× redundant ~1.7M-row scans. The CTE
// makes the planner materialize once and reuse.
func TestExplorerNewContributorsHasCanonicalCTE(t *testing.T) {
	src := readMatviewsSQLForV0255(t)
	region := extractMatviewBlock(t, src, "explorer_new_contributors")

	if !strings.Contains(region, "WITH canonical_full_names AS") {
		t.Error("explorer_new_contributors must declare canonical_full_names as a CTE (WITH ... AS) so it's materialized once and reused. Pre-v0.25.5 inlined it 7 times → 7× redundant 1.7M-row scans on aveloxis_large.")
	}
	// Pin the branched CTE that holds the per-branch rank pushdown.
	if !strings.Contains(region, "branched AS (") {
		t.Error("explorer_new_contributors must have a `branched` CTE that holds per-branch-ranked rows before the global rank. Without it, ROW_NUMBER runs over the full UNION ALL — Linus's tens of thousands of commits get ranked just to throw away all but 7.")
	}
}

// TestExplorerNewContributorsPushesRankDown pins the per-branch
// `WHERE br <= 7` filters. Without these, each branch dumps its entire
// (cntrb, repo) history into the outer ROW_NUMBER instead of capping
// at 7-per-pair-per-branch.
func TestExplorerNewContributorsPushesRankDown(t *testing.T) {
	src := readMatviewsSQLForV0255(t)
	region := extractMatviewBlock(t, src, "explorer_new_contributors")

	// Count `WHERE br <= 7` occurrences — should match the 6 branches.
	got := strings.Count(region, "WHERE br <= 7")
	if got < 6 {
		t.Errorf("explorer_new_contributors must push rank down per-branch with `WHERE br <= 7` in each — found %d occurrences, expected at least 6 (one per branch after the commit_comment drop).", got)
	}
}

// TestExplorerNewContributorsDropsCommitCommentBranch pins that the
// commit_comment branch is removed. Pre-v0.25.5 it joined
// commit_comment_ref (a table Aveloxis never writes to) so it always
// produced zero rows.
func TestExplorerNewContributorsDropsCommitCommentBranch(t *testing.T) {
	src := readMatviewsSQLForV0255(t)
	region := extractMatviewBlock(t, src, "explorer_new_contributors")

	if strings.Contains(region, "commit_comment_ref") {
		t.Error("explorer_new_contributors must NOT reference commit_comment_ref. Aveloxis never writes to that table (it's an Augur compatibility shim); the branch always produced zero rows and was dropped in v0.25.5.")
	}
	if strings.Contains(region, "'commit_comment'") {
		t.Error("explorer_new_contributors must not produce 'commit_comment' action rows — the branch was dropped in v0.25.5.")
	}
}

// TestExplorerNewContributorsUsesRealTimestamp pins the v0.25.5 switch
// from `to_timestamp(cmt_author_date::text, 'YYYY-MM-DD')` (per-row
// function call against 474M commits) to `cmt_author_timestamp` (real
// TIMESTAMPTZ column).
func TestExplorerNewContributorsUsesRealTimestamp(t *testing.T) {
	src := readMatviewsSQLForV0255(t)
	region := extractMatviewBlock(t, src, "explorer_new_contributors")

	if strings.Contains(region, "to_timestamp((commits.cmt_author_date)") {
		t.Error("explorer_new_contributors must NOT use to_timestamp(cmt_author_date) — switched to cmt_author_timestamp (real TIMESTAMPTZ column) in v0.25.5. The text-parse was per-row over 474M commits.")
	}
	if !strings.Contains(region, "cmt_author_timestamp") {
		t.Error("explorer_new_contributors commit branch must reference cmt_author_timestamp (the real timestamp column).")
	}
}

// TestRecentActionsTimeFilterInWhere pins that
// explorer_contributor_recent_actions pushes the 13-month time filter
// into each branch's WHERE clause (not the LEFT JOIN ON, which was the
// pre-v0.25.5 pattern and prevented the planner from short-circuiting
// the source-table scan).
func TestRecentActionsTimeFilterInWhere(t *testing.T) {
	src := readMatviewsSQLForV0255(t)
	region := extractMatviewBlock(t, src, "explorer_contributor_recent_actions")

	// The fix: each branch has the time filter in WHERE, not LEFT JOIN
	// ON. We can't tell positional context easily, but we can count
	// occurrences: the pre-v0.25.5 pattern had the filter
	// `>= now() - interval '13 months'` in the LEFT JOIN ON clause
	// alongside `contributors.cntrb_id = ...`. Post-v0.25.5, each branch
	// puts it in WHERE. The total count of the time filter should be 9
	// (one per branch).
	count := strings.Count(region, "now() - interval '13 months'")
	if count < 9 {
		t.Errorf("explorer_contributor_recent_actions has %d occurrences of `now() - interval '13 months'`; expected 9 (one per branch after pushing the filter into WHERE).", count)
	}
}

// TestCntrbPerFilePreAggregatesReviews pins the v0.25.5 fix to the
// LEFT-JOIN fan-out. The new SQL collapses reviews to one row per PR
// via a CTE before joining to pull_request_files; the old version
// LEFT-JOINed pull_request_reviews directly, multiplying each (PR,
// file) row by reviews-per-PR (~500M intermediate rows at scale).
func TestCntrbPerFilePreAggregatesReviews(t *testing.T) {
	src := readMatviewsSQLForV0255(t)
	region := extractMatviewBlock(t, src, "explorer_cntrb_per_file")

	if !strings.Contains(region, "WITH pr_reviewers AS") {
		t.Error("explorer_cntrb_per_file must declare `WITH pr_reviewers AS` (a CTE that aggregates reviews per PR before joining to pull_request_files). Pre-v0.25.5 LEFT-JOINed reviews directly, multiplying each (PR, file) row by reviews-per-PR — ~500M intermediate rows on aveloxis_large.")
	}
}

// TestV025_5_IndexesRegistered pins the four CONCURRENTLY index
// migrations.
func TestV025_5_IndexesRegistered(t *testing.T) {
	src := readSourceFile(t, "migrate.go")

	for _, idx := range []string{
		"idx_contributors_cntrb_canonical",
		"idx_contributors_canonical_eq_email",
		"idx_commits_author_timestamp_recent",
		"idx_repo_labor_repo_id_analysis_date",
	} {
		if !strings.Contains(src, idx) {
			t.Errorf("migrate.go must register v0.25.5 index %s via execCreateIndexConcurrently.", idx)
		}
	}

	// Pin the partial-index predicates so a future refactor doesn't
	// "helpfully" make them full indexes (3-5× larger, slower writes).
	for _, predicate := range []string{
		"WHERE cntrb_canonical != ''",
		"WHERE cntrb_canonical = cntrb_email",
		"WHERE cmt_author_timestamp >= '2024-01-01'",
	} {
		if !strings.Contains(src, predicate) {
			t.Errorf("migrate.go must declare the partial-index predicate %q to keep the v0.25.5 indexes small.", predicate)
		}
	}
}

// readMatviewsSQLForV0255 reads the embedded matviews.sql via filesystem
// (the embed.FS at runtime is the same content; we go to disk here
// for source-contract test stability).
func readMatviewsSQLForV0255(t *testing.T) string {
	return readSourceFile(t, "matviews.sql")
}

// readSourceFile loads a file from the db package directory.
func readSourceFile(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}

// extractMatviewBlock returns the SQL text for a single matview's
// CREATE statement, anchored on its name. Used by the per-view tests
// to scope their assertions without false-matching neighbors.
func extractMatviewBlock(t *testing.T, sql, viewName string) string {
	t.Helper()
	// Find the CREATE MATERIALIZED VIEW (or CREATE OR REPLACE VIEW)
	// for this view, then return text until the next CREATE statement
	// at top-level. Approximate but adequate for source-contract tests.
	needles := []string{
		"CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data." + viewName,
		"CREATE OR REPLACE VIEW aveloxis_data." + viewName,
	}
	var start int = -1
	for _, n := range needles {
		if i := strings.Index(sql, n); i >= 0 {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatalf("could not find CREATE for matview %q in matviews.sql", viewName)
	}
	// End at the next top-level DROP MATERIALIZED VIEW (the start of
	// the next view block) — generous window so the assertions can
	// see comments, CREATE UNIQUE INDEX, etc.
	end := strings.Index(sql[start+1:], "DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.")
	if end < 0 {
		return sql[start:]
	}
	return sql[start : start+1+end]
}
