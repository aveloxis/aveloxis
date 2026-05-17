// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.22.6 — every FK column pointing at
// aveloxis_data.contributors(cntrb_id) must be backed by a btree
// index on the child table. Without indexes, ON UPDATE CASCADE
// (added in v0.22.1) seq-scans every child table for every cntrb_id
// being rewritten. On the production aveloxis_large DB this kept
// `aveloxis migrate-cntrb-ids` stuck on batch 1 for 17+ hours with
// zero committed progress: 5,000 batch rows × 15 unindexed child
// tables (some 50M+ rows) = 75,000 sequential scans per batch.
//
// The list is the 15 child FK columns that were unindexed in
// production on 2026-05-17. contributor_identities.cntrb_id is
// already indexed by the legacy idx_contributor_identities_cntrb
// (schema.sql), so it does NOT appear here — adding a redundant
// second index would be churn.
//
// Each row is (child_table, child_column, index_name). Adding a
// NEW child FK column to aveloxis_data.contributors(cntrb_id) in
// the future requires extending this list AND schema.sql AND the
// v0.22.6 helper in cntrb_id_fk_indexes.go. The local fixture
// (not borrowed from cntrb_id_fk_indexes.go) means a refactor that
// renames the production list still fails this test by name.
var cntrbIDFKIndexesTestFixture = []struct {
	table     string
	column    string
	indexName string
}{
	{"contributor_repo", "cntrb_id", "idx_contributor_repo_cntrb_id"},
	{"contributors_aliases", "cntrb_id", "idx_contributors_aliases_cntrb_id"},
	{"issue_assignees", "cntrb_id", "idx_issue_assignees_cntrb_id"},
	{"issue_events", "cntrb_id", "idx_issue_events_cntrb_id"},
	{"issues", "closed_by_id", "idx_issues_closed_by_id"},
	{"issues", "reporter_id", "idx_issues_reporter_id"},
	{"messages", "cntrb_id", "idx_messages_cntrb_id"},
	{"pull_request_assignees", "cntrb_id", "idx_pull_request_assignees_cntrb_id"},
	{"pull_request_commits", "author_cntrb_id", "idx_pull_request_commits_author_cntrb_id"},
	{"pull_request_events", "cntrb_id", "idx_pull_request_events_cntrb_id"},
	{"pull_request_meta", "cntrb_id", "idx_pull_request_meta_cntrb_id"},
	{"pull_request_repo", "pr_cntrb_id", "idx_pull_request_repo_pr_cntrb_id"},
	{"pull_request_reviewers", "cntrb_id", "idx_pull_request_reviewers_cntrb_id"},
	{"pull_request_reviews", "cntrb_id", "idx_pull_request_reviews_cntrb_id"},
	{"pull_requests", "author_id", "idx_pull_requests_author_id"},
}

// TestSchemaDeclaresIndexesOnAllCntrbIDFKChildColumns pins the
// fresh-install path: schema.sql must declare a CREATE INDEX for
// each of the 15 unindexed child FK columns so a brand-new aveloxis
// install gets the correct shape without needing the v0.22.6
// migration to do the CONCURRENTLY rebuilds.
//
// schema.sql declares indexes inline as either:
//
//	CREATE INDEX IF NOT EXISTS <name> ON aveloxis_data.<table> (<col>);
//	CREATE INDEX IF NOT EXISTS <name>
//	  ON aveloxis_data.<table> (<col>);
//
// The test allows multi-line declarations (whitespace-tolerant) and
// matches each fixture row to a substring of the rendered statement.
func TestSchemaDeclaresIndexesOnAllCntrbIDFKChildColumns(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Collapse all whitespace to single spaces so multi-line CREATE
	// INDEX statements match a flat needle.
	flat := strings.Join(strings.Fields(code), " ")

	for _, idx := range cntrbIDFKIndexesTestFixture {
		// Look for the canonical declaration:
		//   CREATE INDEX IF NOT EXISTS <indexName> ON aveloxis_data.<table> (<column>)
		// Whitespace already flattened, so case+spacing are normalized.
		needle := "CREATE INDEX IF NOT EXISTS " + idx.indexName +
			" ON aveloxis_data." + idx.table + " (" + idx.column + ")"
		if !strings.Contains(flat, needle) {
			t.Errorf("schema.sql is missing index declaration:\n  %s\n"+
				"Without it, v0.22.1's ON UPDATE CASCADE on cntrb_id rewrites "+
				"seq-scans aveloxis_data.%s for every cntrb_id update — the "+
				"root cause of the 17-hour stall on aveloxis migrate-cntrb-ids "+
				"observed on the aveloxis_large production DB on 2026-05-17.",
				needle, idx.table)
		}
	}
}

// TestMigrationCreatesIndexesForAllCntrbIDFKChildColumns pins the
// upgrade path for existing deployments: migrate.go (and/or the
// v0.22.6 helper file) must invoke execCreateIndexConcurrently for
// each of the 15 missing indexes. CONCURRENTLY is required because
// the indexes are added on running production databases; a plain
// CREATE INDEX would acquire SHARE on the child table for the full
// build (minutes on 50M+-row messages), blocking the collection
// workers' INSERTs.
//
// The test reads both migrate.go and cntrb_id_fk_indexes.go so a
// future refactor can split or inline the helper without forcing a
// test rewrite — the contract is "the migration creates this index
// CONCURRENTLY", not "the SQL string lives in this specific file".
func TestMigrationCreatesIndexesForAllCntrbIDFKChildColumns(t *testing.T) {
	parts := [][]byte{}
	for _, path := range []string{"migrate.go", "cntrb_id_fk_indexes.go"} {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v (v0.22.6 helper file must exist alongside migrate.go)", path, err)
		}
		parts = append(parts, src)
	}
	code := string(parts[0]) + "\n" + string(parts[1])

	if !strings.Contains(code, "execCreateIndexConcurrently") {
		t.Error("v0.22.6 FK-index migration must use execCreateIndexConcurrently — " +
			"a plain CREATE INDEX would hold SHARE on the child table for the full " +
			"build duration (minutes on 50M+-row messages), blocking the running " +
			"collection workers' INSERTs")
	}

	for _, idx := range cntrbIDFKIndexesTestFixture {
		if !strings.Contains(code, idx.indexName) {
			t.Errorf("v0.22.6 migration source must reference index name %q "+
				"(table=%s column=%s) — the migration must touch every cntrb_id "+
				"FK child column; omitting one leaves the production cascade "+
				"fan-out stall in place for that table",
				idx.indexName, idx.table, idx.column)
		}
	}
}

// TestRunMigrationsInvokesEnsureCntrbIDFKIndexes pins the wiring:
// RunMigrations must call the v0.22.6 helper, otherwise the source
// changes in cntrb_id_fk_indexes.go never run at startup.
func TestRunMigrationsInvokesEnsureCntrbIDFKIndexes(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "ensureCntrbIDFKIndexes(") {
		t.Error("migrate.go must invoke ensureCntrbIDFKIndexes(...) — without the " +
			"call, the v0.22.6 helper never runs and the missing-index regression " +
			"persists in any existing aveloxis deployment that runs migrate")
	}
}

// TestEnsureCntrbIDFKIndexesRunsAfterCascadeStep pins the ordering:
// the v0.22.6 index step must run AFTER the v0.22.1 cascade step.
// If the indexes were created before the cascade constraints,
// PostgreSQL would still seq-scan during the cascade migration's
// VALIDATE CONSTRAINT phase (it scans the child table once); but
// more importantly the v0.22.2 data migration depends on cascade
// being in place, and the v0.22.6 indexes are what make that
// cascade tractable at all. Conceptually: cascade adds the
// behavior, indexes make the behavior fast. Cascade first, then
// indexes — same source order in migrate.go.
func TestEnsureCntrbIDFKIndexesRunsAfterCascadeStep(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	cascadePos := strings.Index(code, "ensureOnUpdateCascadeOnCntrbIDFKs(")
	indexPos := strings.Index(code, "ensureCntrbIDFKIndexes(")
	if cascadePos < 0 {
		t.Fatal("ensureOnUpdateCascadeOnCntrbIDFKs call not found — v0.22.1 step missing")
	}
	if indexPos < 0 {
		t.Fatal("ensureCntrbIDFKIndexes call not found — v0.22.6 step missing")
	}
	if indexPos < cascadePos {
		t.Errorf("ensureCntrbIDFKIndexes must be called AFTER "+
			"ensureOnUpdateCascadeOnCntrbIDFKs in migrate.go (cascade adds the "+
			"behavior, indexes make it tractable). Got cascade_pos=%d, index_pos=%d",
			cascadePos, indexPos)
	}
}
