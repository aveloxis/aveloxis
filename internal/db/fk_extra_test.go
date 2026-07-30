// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// v0.22.7 — every FK on a child table pointing at one of the five
// large-parent tables (pull_requests, issues, messages,
// pull_request_reviews, repos) must:
//
//  1. Have a btree index on the child column.
//  2. Declare ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED.
//
// 50 FKs total: 11 PR children + 4 issues children + 3 messages
// children + 2 PR-reviews children + 30 repos children (including
// issue_assignees.repo_id per operator decision on 2026-05-17).
//
// Each row is (child_table, child_column, target_table, target_column,
// index_name, constraint_name). The local fixture (not borrowed from
// fk_constraints_extra.go) means a refactor that renames or splits the
// production list still fails this test by name, not by silently
// following the rename — same pattern as cntrb_id_cascade_test.go.
type fkExtraSpec struct {
	table, column, parent, parentCol, indexName, constraintName string
}

var fkExtraFixture = []fkExtraSpec{
	// pull_requests(pull_request_id) ← 11 children
	{"pull_request_analysis", "pull_request_id", "pull_requests", "pull_request_id",
		"idx_pull_request_analysis_pull_request_id", "pull_request_analysis_pull_request_id_fkey"},
	{"pull_request_assignees", "pull_request_id", "pull_requests", "pull_request_id",
		"idx_pull_request_assignees_pull_request_id", "pull_request_assignees_pull_request_id_fkey"},
	{"pull_request_commits", "pull_request_id", "pull_requests", "pull_request_id",
		"idx_pull_request_commits_pull_request_id", "pull_request_commits_pull_request_id_fkey"},
	{"pull_request_events", "pull_request_id", "pull_requests", "pull_request_id",
		"idx_pull_request_events_pull_request_id", "pull_request_events_pull_request_id_fkey"},
	{"pull_request_files", "pull_request_id", "pull_requests", "pull_request_id",
		"idx_pull_request_files_pull_request_id", "pull_request_files_pull_request_id_fkey"},
	{"pull_request_labels", "pull_request_id", "pull_requests", "pull_request_id",
		"idx_pull_request_labels_pull_request_id", "pull_request_labels_pull_request_id_fkey"},
	{"pull_request_message_ref", "pull_request_id", "pull_requests", "pull_request_id",
		"idx_pull_request_message_ref_pull_request_id", "pull_request_message_ref_pull_request_id_fkey"},
	{"pull_request_meta", "pull_request_id", "pull_requests", "pull_request_id",
		"idx_pull_request_meta_pull_request_id", "pull_request_meta_pull_request_id_fkey"},
	{"pull_request_reviewers", "pull_request_id", "pull_requests", "pull_request_id",
		"idx_pull_request_reviewers_pull_request_id", "pull_request_reviewers_pull_request_id_fkey"},
	{"pull_request_reviews", "pull_request_id", "pull_requests", "pull_request_id",
		"idx_pull_request_reviews_pull_request_id", "pull_request_reviews_pull_request_id_fkey"},
	{"pull_request_teams", "pull_request_id", "pull_requests", "pull_request_id",
		"idx_pull_request_teams_pull_request_id", "pull_request_teams_pull_request_id_fkey"},

	// issues(issue_id) ← 4 children
	{"issue_assignees", "issue_id", "issues", "issue_id",
		"idx_issue_assignees_issue_id", "issue_assignees_issue_id_fkey"},
	{"issue_events", "issue_id", "issues", "issue_id",
		"idx_issue_events_issue_id", "issue_events_issue_id_fkey"},
	{"issue_labels", "issue_id", "issues", "issue_id",
		"idx_issue_labels_issue_id", "issue_labels_issue_id_fkey"},
	{"issue_message_ref", "issue_id", "issues", "issue_id",
		"idx_issue_message_ref_issue_id", "issue_message_ref_issue_id_fkey"},

	// messages(msg_id) ← 3 children
	{"issue_message_ref", "msg_id", "messages", "msg_id",
		"idx_issue_message_ref_msg_id", "issue_message_ref_msg_id_fkey"},
	{"pull_request_message_ref", "msg_id", "messages", "msg_id",
		"idx_pull_request_message_ref_msg_id", "pull_request_message_ref_msg_id_fkey"},
	{"review_comments", "msg_id", "messages", "msg_id",
		"idx_review_comments_msg_id", "review_comments_msg_id_fkey"},

	// pull_request_reviews(pr_review_id) ← 2 children
	{"pull_request_review_message_ref", "pr_review_id", "pull_request_reviews", "pr_review_id",
		"idx_pull_request_review_message_ref_pr_review_id", "pull_request_review_message_ref_pr_review_id_fkey"},
	{"review_comments", "pr_review_id", "pull_request_reviews", "pr_review_id",
		"idx_review_comments_pr_review_id", "review_comments_pr_review_id_fkey"},

	// repos(repo_id) ← 30 children (including issue_assignees.repo_id)
	{"commit_comment_ref", "repo_id", "repos", "repo_id",
		"idx_commit_comment_ref_repo_id", "commit_comment_ref_repo_id_fkey"},
	{"commit_messages", "repo_id", "repos", "repo_id",
		"idx_commit_messages_repo_id", "commit_messages_repo_id_fkey"},
	{"dei_badging", "repo_id", "repos", "repo_id",
		"idx_dei_badging_repo_id", "dei_badging_repo_id_fkey"},
	{"issue_assignees", "repo_id", "repos", "repo_id",
		"idx_issue_assignees_repo_id", "issue_assignees_repo_id_fkey"},
	{"issue_labels", "repo_id", "repos", "repo_id",
		"idx_issue_labels_repo_id", "issue_labels_repo_id_fkey"},
	{"issue_message_ref", "repo_id", "repos", "repo_id",
		"idx_issue_message_ref_repo_id", "issue_message_ref_repo_id_fkey"},
	{"libraries", "repo_id", "repos", "repo_id",
		"idx_libraries_repo_id", "libraries_repo_id_fkey"},
	{"lstm_anomaly_results", "repo_id", "repos", "repo_id",
		"idx_lstm_anomaly_results_repo_id", "lstm_anomaly_results_repo_id_fkey"},
	{"message_analysis_summary", "repo_id", "repos", "repo_id",
		"idx_message_analysis_summary_repo_id", "message_analysis_summary_repo_id_fkey"},
	{"message_sentiment_summary", "repo_id", "repos", "repo_id",
		"idx_message_sentiment_summary_repo_id", "message_sentiment_summary_repo_id_fkey"},
	{"pull_request_assignees", "repo_id", "repos", "repo_id",
		"idx_pull_request_assignees_repo_id", "pull_request_assignees_repo_id_fkey"},
	{"pull_request_commits", "repo_id", "repos", "repo_id",
		"idx_pull_request_commits_repo_id", "pull_request_commits_repo_id_fkey"},
	{"pull_request_files", "repo_id", "repos", "repo_id",
		"idx_pull_request_files_repo_id", "pull_request_files_repo_id_fkey"},
	{"pull_request_labels", "repo_id", "repos", "repo_id",
		"idx_pull_request_labels_repo_id", "pull_request_labels_repo_id_fkey"},
	{"pull_request_message_ref", "repo_id", "repos", "repo_id",
		"idx_pull_request_message_ref_repo_id", "pull_request_message_ref_repo_id_fkey"},
	{"pull_request_meta", "repo_id", "repos", "repo_id",
		"idx_pull_request_meta_repo_id", "pull_request_meta_repo_id_fkey"},
	{"pull_request_review_message_ref", "repo_id", "repos", "repo_id",
		"idx_pull_request_review_message_ref_repo_id", "pull_request_review_message_ref_repo_id_fkey"},
	{"pull_request_reviewers", "repo_id", "repos", "repo_id",
		"idx_pull_request_reviewers_repo_id", "pull_request_reviewers_repo_id_fkey"},
	{"pull_request_reviews", "repo_id", "repos", "repo_id",
		"idx_pull_request_reviews_repo_id", "pull_request_reviews_repo_id_fkey"},
	{"repo_badging", "repo_id", "repos", "repo_id",
		"idx_repo_badging_repo_id", "repo_badging_repo_id_fkey"},
	{"repo_clones", "repo_id", "repos", "repo_id",
		"idx_repo_clones_repo_id", "repo_clones_repo_id_fkey"},
	{"repo_cluster_messages", "repo_id", "repos", "repo_id",
		"idx_repo_cluster_messages_repo_id", "repo_cluster_messages_repo_id_fkey"},
	{"repo_insights", "repo_id", "repos", "repo_id",
		"idx_repo_insights_repo_id", "repo_insights_repo_id_fkey"},
	{"repo_insights_records", "repo_id", "repos", "repo_id",
		"idx_repo_insights_records_repo_id", "repo_insights_records_repo_id_fkey"},
	{"repo_meta", "repo_id", "repos", "repo_id",
		"idx_repo_meta_repo_id", "repo_meta_repo_id_fkey"},
	{"repo_sbom_scans", "repo_id", "repos", "repo_id",
		"idx_repo_sbom_scans_repo_id", "repo_sbom_scans_repo_id_fkey"},
	{"repo_stats", "repo_id", "repos", "repo_id",
		"idx_repo_stats_repo_id", "repo_stats_repo_id_fkey"},
	{"repo_topic", "repo_id", "repos", "repo_id",
		"idx_repo_topic_repo_id", "repo_topic_repo_id_fkey"},
	{"review_comments", "repo_id", "repos", "repo_id",
		"idx_review_comments_repo_id", "review_comments_repo_id_fkey"},
	{"topic_model_meta", "repo_id", "repos", "repo_id",
		"idx_topic_model_meta_repo_id", "topic_model_meta_repo_id_fkey"},
}

// TestSchemaDeclaresExtraFKIndexes pins fresh-install: schema.sql
// declares a CREATE INDEX for every one of the 50 child FK columns.
func TestSchemaDeclaresExtraFKIndexes(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	flat := strings.Join(strings.Fields(string(src)), " ")
	for _, fk := range fkExtraFixture {
		needle := "CREATE INDEX IF NOT EXISTS " + fk.indexName +
			" ON aveloxis_data." + fk.table + " (" + fk.column + ")"
		if !strings.Contains(flat, needle) {
			t.Errorf("schema.sql missing index declaration:\n  %s", needle)
		}
	}
}

// TestSchemaDeclaresFullBehaviorOnExtraFKs pins the inline FK
// behavior in schema.sql for fresh installs: each of the 50
// specific (table, column) FKs in fkExtraFixture must declare
// ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY
// DEFERRED.
//
// CRITICAL: The check is per-(table, column), not per-parent.
// repos(repo_id) has ~43 children total; only 30 are in our
// enumerated set. The other 13 (the "main" indexed children
// like issues.repo_id, pull_requests.repo_id, etc.) correctly
// get DEFERRABLE INITIALLY DEFERRED ONLY — not the full clause.
// A naive "all REFERENCES to repos must have CASCADE/RESTRICT"
// test would incorrectly flag those.
//
// Implementation: track CREATE TABLE blocks while scanning,
// match (curTable, fk.column) against fkExtraFixture, and verify
// only the matching FK lines have the full clause.
func TestSchemaDeclaresFullBehaviorOnExtraFKs(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	// Build a (table, column) → expected map.
	want := map[string]struct{}{}
	for _, fk := range fkExtraFixture {
		want[fk.table+"."+fk.column] = struct{}{}
	}

	createTableRE := regexp.MustCompile(`(?i)CREATE TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+aveloxis_data\.([a-z_]+)`)
	// Match a column-line FK: leading whitespace, column name, then
	// REFERENCES somewhere on the same line. The captured column is
	// what we cross-check against fkExtraFixture.
	fkLineRE := regexp.MustCompile(`(?i)^\s+([a-z_]+)\s+[A-Z][^,]*REFERENCES`)

	var curTable string
	found := map[string]bool{}
	for _, line := range strings.Split(string(src), "\n") {
		if m := createTableRE.FindStringSubmatch(line); m != nil {
			curTable = strings.ToLower(m[1])
			continue
		}
		if curTable == "" {
			continue
		}
		m := fkLineRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		col := strings.ToLower(m[1])
		key := curTable + "." + col
		if _, isEnumerated := want[key]; !isEnumerated {
			continue
		}
		// This is one of the 50. Must contain all three clauses.
		hasCascade := strings.Contains(line, "ON UPDATE CASCADE")
		hasRestrict := strings.Contains(line, "ON DELETE RESTRICT")
		hasDeferred := strings.Contains(line, "DEFERRABLE INITIALLY DEFERRED")
		if !(hasCascade && hasRestrict && hasDeferred) {
			t.Errorf("FK at %s.%s missing required behavior (cascade=%v, restrict=%v, deferred=%v)\nLine: %s",
				curTable, col, hasCascade, hasRestrict, hasDeferred, line)
		}
		found[key] = true
	}
	for key := range want {
		if !found[key] {
			t.Errorf("expected FK %s in schema.sql but did not find it as a column-level "+
				"REFERENCES line — list drift between fkExtraFixture and schema.sql", key)
		}
	}
}

// TestSchemaDeclaresDeferredOnAllFKs pins the deferral clause on
// EVERY FK in schema.sql, including the 39 small-parent FKs that
// don't get RESTRICT/CASCADE. v0.22.7's "all foreign keys are
// DEFERRABLE INITIALLY DEFERRED" consistency rule.
func TestSchemaDeclaresDeferredOnAllFKs(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Match every "REFERENCES <schema>.<table>(<col>)" occurrence,
	// then check that DEFERRABLE INITIALLY DEFERRED appears within
	// 200 chars.
	refRE := regexp.MustCompile(`REFERENCES\s+[a-z_]+\.[a-z_]+\([a-z_]+\)`)
	matches := refRE.FindAllStringIndex(code, -1)
	if len(matches) == 0 {
		t.Fatal("zero FK declarations found in schema.sql — parser bug?")
	}
	missing := 0
	for _, m := range matches {
		end := m[1] + 200
		if end > len(code) {
			end = len(code)
		}
		window := code[m[0]:end]
		if !strings.Contains(window, "DEFERRABLE INITIALLY DEFERRED") {
			t.Errorf("FK at offset %d missing DEFERRABLE INITIALLY DEFERRED. Window:\n%q",
				m[0], window)
			missing++
		}
	}
	if missing > 0 {
		t.Errorf("%d of %d FK declarations in schema.sql lack DEFERRABLE INITIALLY DEFERRED — "+
			"v0.22.7 contract requires deferral on every FK for consistency",
			missing, len(matches))
	}
}

// TestSchemaDeclaresDeferredOnCntrbIDFKs is a focused regression
// guard for the 16 v0.22.1 cntrb_id FKs — they previously had
// only ON UPDATE CASCADE; v0.22.7 must additionally add ON DELETE
// RESTRICT and DEFERRABLE INITIALLY DEFERRED.
func TestSchemaDeclaresDeferredOnCntrbIDFKs(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	refTarget := "REFERENCES aveloxis_data.contributors(cntrb_id)"
	occurrences := strings.Count(code, refTarget)
	// v0.22.1 baseline: 16. v0.23.0 added contributor_login_history
	// (17). v0.27.58 added contributor_activity_days and
	// contributor_activity_day_totals (19). If you add another
	// cntrb_id child table, bump this count AND add the entry to
	// cntrbIDChildFKs in cntrb_id_cascade.go so the migration helper
	// covers it.
	if occurrences != 19 {
		t.Fatalf("expected 19 cntrb_id FK occurrences in schema.sql, got %d — "+
			"if this changed, update both this test and the cntrb_id_cascade.go fixture", occurrences)
	}
	idx := 0
	for {
		n := strings.Index(code[idx:], refTarget)
		if n < 0 {
			break
		}
		start := idx + n
		end := start + len(refTarget) + 200
		if end > len(code) {
			end = len(code)
		}
		window := code[start:end]
		// All 16 must have CASCADE (from v0.22.1) AND RESTRICT (new in v0.22.7)
		// AND DEFERRED (new in v0.22.7).
		if !strings.Contains(window, "ON UPDATE CASCADE") ||
			!strings.Contains(window, "ON DELETE RESTRICT") ||
			!strings.Contains(window, "DEFERRABLE INITIALLY DEFERRED") {
			t.Errorf("cntrb_id FK at offset %d missing full v0.22.7 behavior. Window:\n%q",
				start, window)
		}
		idx = start + len(refTarget)
	}
}

// TestMigrationCreatesExtraFKIndexes pins the migration source for
// existing-deployment upgrades: migrate.go (and/or
// fk_indexes_extra.go) references each of the 50 index names via
// execCreateIndexConcurrently.
func TestMigrationCreatesExtraFKIndexes(t *testing.T) {
	parts := [][]byte{}
	for _, path := range []string{"migrate.go", "fk_indexes_extra.go"} {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v (v0.22.7 helper file must exist)", path, err)
		}
		parts = append(parts, src)
	}
	code := string(parts[0]) + "\n" + string(parts[1])
	if !strings.Contains(code, "execCreateIndexConcurrently") {
		t.Error("v0.22.7 FK-index migration must use execCreateIndexConcurrently")
	}
	for _, fk := range fkExtraFixture {
		if !strings.Contains(code, fk.indexName) {
			t.Errorf("v0.22.7 migration source must reference index name %q "+
				"(table=%s column=%s)", fk.indexName, fk.table, fk.column)
		}
	}
}

// TestMigrationAddsFullBehaviorToExtraFKs pins the constraint
// alteration step: migrate.go (and/or fk_constraints_extra.go)
// references each of the 50 constraint names AND the keywords for
// the full v0.22.7 clause set.
func TestMigrationAddsFullBehaviorToExtraFKs(t *testing.T) {
	parts := [][]byte{}
	for _, path := range []string{"migrate.go", "fk_constraints_extra.go"} {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v (v0.22.7 helper file must exist)", path, err)
		}
		parts = append(parts, src)
	}
	code := string(parts[0]) + "\n" + string(parts[1])
	for _, kw := range []string{"ON UPDATE CASCADE", "ON DELETE RESTRICT",
		"DEFERRABLE INITIALLY DEFERRED", "NOT VALID", "VALIDATE CONSTRAINT"} {
		if !strings.Contains(code, kw) {
			t.Errorf("v0.22.7 migration must contain keyword %q — required for "+
				"the constraint-rewrite-with-deferral pattern", kw)
		}
	}
	for _, fk := range fkExtraFixture {
		if !strings.Contains(code, fk.constraintName) {
			t.Errorf("v0.22.7 migration must reference constraint name %q "+
				"(table=%s column=%s)", fk.constraintName, fk.table, fk.column)
		}
	}
}

// TestRunMigrationsInvokesV022_7Helpers pins the wiring: all three
// new helpers must be invoked from RunMigrations or the v0.22.7
// work never runs.
func TestRunMigrationsInvokesV022_7Helpers(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	for _, fn := range []string{
		"ensureExtraFKIndexes(",
		"ensureExtraFKConstraints(",
		"ensureCntrbIDFKsFullBehavior(",
		"ensureAllFKsDeferrable(",
	} {
		if !strings.Contains(code, fn) {
			t.Errorf("migrate.go must invoke %s — without the call, the v0.22.7 "+
				"helper never runs", fn)
		}
	}
}

// TestEnsureAllFKsDeferrableUsesAlterConstraint pins the
// lightweight-ALTER pattern for the dynamic deferrable-catchall
// helper. ALTER CONSTRAINT DEFERRABLE is a metadata-only change;
// using DROP+ADD for the 39 small-parent FKs would mean specifying
// CASCADE/RESTRICT semantics on them which the operator explicitly
// scoped out of v0.22.7.
func TestEnsureAllFKsDeferrableUsesAlterConstraint(t *testing.T) {
	src, err := os.ReadFile("fk_constraints_extra.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "ensureAllFKsDeferrable") {
		t.Fatal("ensureAllFKsDeferrable helper not found in fk_constraints_extra.go")
	}
	if !strings.Contains(code, "ALTER CONSTRAINT") {
		t.Error("ensureAllFKsDeferrable must use ALTER CONSTRAINT (metadata-only) " +
			"not DROP+ADD — the catch-all path must not redefine CASCADE/RESTRICT " +
			"semantics on FKs the operator didn't scope into v0.22.7")
	}
	// And it must query pg_constraint for non-deferrable FKs
	if !strings.Contains(code, "pg_constraint") && !strings.Contains(code, "information_schema") {
		t.Error("ensureAllFKsDeferrable must self-discover FKs via pg_constraint or " +
			"information_schema to be idempotent and to cover FKs the operator hasn't " +
			"enumerated explicitly")
	}
}
