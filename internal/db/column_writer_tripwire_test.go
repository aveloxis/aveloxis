// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.105 — the class-killing tripwire from the 2026-08-19 fill
// audit (Workstream D). The audit found columns that had been declared
// in schema.sql, shipped to production, and never written for the
// product's ENTIRE LIFE (repos.platform_repo_id at 0 of 142,480 rows;
// cmt_whitespace at 0% while six live aggregates SUMmed it; ...).
//
// The contract this test enforces: every column of every AUDITED
// entity table either has a writer somewhere in internal/db/ (its name
// appears in non-test store source — INSERT column lists, SET clauses)
// or is on the explicit allowlist below WITH a documented reason. A
// column added to schema.sql without a writer and without an allowlist
// entry fails the build — the "silently dark for years" class dies
// here. (The subtler bound-but-never-set class needs model-field
// tracing; the periodic fill audit stays the net for that.)
//
// v0.27.106 (PR #184 review): "has a writer" is scoped to INSERT/UPDATE
// SQL statements that NAME the audited table, with SQL comments
// stripped — a column mentioned only in a code comment, a SELECT-only
// reader, or another table's statement no longer satisfies the check.
package db

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// auditedTables: the data-bearing entity tables the 2026-08-19 audit
// covered. Zero-row Augur-legacy tables (repo_badging, topic_words, …)
// are deliberately NOT audited — no writer is their documented state.
var auditedTables = []string{
	"repos", "repo_info", "issues", "pull_requests", "messages",
	"pull_request_reviews", "review_comments", "issue_events",
	"pull_request_events", "releases", "contributors", "commits",
	"pull_request_meta", "pull_request_repo", "pull_request_commits",
	"pull_request_files", "repo_deps_libyear", "repo_dependencies",
	"contributor_identities", "repo_labor",
}

// documentedEmpty: columns with NO writer, kept as Augur schema-parity
// ballast or awaiting a data source. Every entry needs a reason — this
// list IS the documentation contract (mirrored in
// docs/architecture/column-mapping.md "Known-empty columns").
var documentedEmpty = map[string]string{
	"issues.pull_request":                   "Augur-legacy: aveloxis stores PRs in their own table; the column is structurally always NULL",
	"issues.pull_request_id":                "Augur-legacy twin of issues.pull_request",
	"issues.due_on":                         "no writer; GitHub issues have no due date (GitLab due_date is a candidate if GitLab tracking grows)",
	"commits.cmt_ght_committer_id":          "committer-identity resolution not implemented (documented follow-up; author twin IS resolved)",
	"commits.cmt_ght_committed_at":          "Augur-legacy duplicate of cmt_committer_timestamp (which IS populated)",
	"contributors.cntrb_type":               "Augur-legacy geo/classification field with no data source",
	"contributors.cntrb_fake":               "Augur-legacy, no data source",
	"contributors.cntrb_lat":                "Augur-legacy geolocation, no data source",
	"contributors.cntrb_long":               "Augur-legacy geolocation, no data source",
	"contributors.cntrb_country_code":       "Augur-legacy geolocation, no data source",
	"contributors.cntrb_state":              "Augur-legacy geolocation, no data source",
	"contributors.cntrb_city":               "Augur-legacy geolocation, no data source",
	"contributors.cntrb_last_used":          "Augur-legacy, no data source",
	"repos.repo_path":                       "Augur-legacy local-clone bookkeeping; aveloxis derives clone paths from repo_id",
	"repo_labor.repo_url":                   "Augur-legacy denormalization; derivable via repos join",
	"repo_info.issue_contributors_count":    "bound-but-never-set: no cheap forge source matches Augur's definition (documented follow-up)",
	"repo_info.committer_count":             "bound-but-never-set: derivable from facade commits data (documented follow-up)",
	"repo_info.security_audit_file":         "bound-but-never-set: no forge community-profile source exists for audit files",
	"pull_requests.pr_augur_contributor_id": "Augur-legacy identity column; aveloxis uses author_id",
	"messages.rgls_id":                      "mailing-list list linkage lives on email_message/email_message_ref; the shared message upsert never writes it (its name only appears in OTHER tables' SQL — the exact false-positive the v0.27.106 statement-scoped check exists to catch)",
	"messages.msg_header":                   "mailing-list headers live on email_message; forge-sourced messages have no header material",
}

func TestEveryColumnHasWriterOrDocumentedEmpty(t *testing.T) {
	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}

	// Concatenate every non-test store source file — the writer corpus.
	var writers strings.Builder
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	files := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatal(err)
		}
		writers.Write(b)
		writers.WriteByte('\n')
		files++
	}
	if files < 30 {
		t.Fatalf("self-check: only %d store source files scanned — the corpus walk broke", files)
	}
	corpus := writers.String()

	// Pull every backtick-delimited SQL literal out of the corpus, strip
	// its `--` line comments, and index the INSERT/UPDATE statements per
	// audited table. Only these blocks count as writers.
	sqlLitRe := regexp.MustCompile("`[^`]*`")
	commentRe := regexp.MustCompile(`(?m)--[^\n]*`)
	writeBlocks := map[string][]string{}
	for _, lit := range sqlLitRe.FindAllString(corpus, -1) {
		clean := commentRe.ReplaceAllString(lit, "")
		for _, tbl := range auditedTables {
			tblRe := regexp.MustCompile(`(?i)(INSERT\s+INTO|UPDATE)\s+aveloxis_data\.` + regexp.QuoteMeta(tbl) + `\b`)
			if tblRe.MatchString(clean) {
				writeBlocks[tbl] = append(writeBlocks[tbl], clean)
			}
		}
	}

	// Full declaration line captured so SERIAL PKs can be skipped by
	// their type, whatever their name (issue_event_id, identity_id, ...).
	colRe := regexp.MustCompile(`(?m)^\s{4}([a-z_]+)\s+([A-Z][^,\n]*)`)
	skipCols := map[string]bool{
		// Bookkeeping with schema defaults — populated by DEFAULT even
		// when absent from an INSERT list.
		"tool_source": true, "tool_version": true, "data_source": true,
		"data_collection_date": true,
	}

	totalCols := 0
	inventoried := map[string]bool{}
	for _, tbl := range auditedTables {
		marker := "CREATE TABLE IF NOT EXISTS aveloxis_data." + tbl + " ("
		i := strings.Index(string(schema), marker)
		if i < 0 {
			t.Errorf("self-check: audited table %s not found in schema.sql", tbl)
			continue
		}
		block := string(schema)[i:]
		if j := strings.Index(block, "\n);"); j > 0 {
			block = block[:j]
		}
		cols := colRe.FindAllStringSubmatch(block, -1)
		// v0.27.113 (round-8 suppressed finding, real): columns added to
		// EXISTING fleets arrive as ALTER TABLE ... ADD COLUMN IF NOT
		// EXISTS guards (the v0.27.58 rule), not in the CREATE TABLE
		// block — and those are exactly the NEWEST columns, the ones
		// most at risk of shipping without a writer. Inventory them too.
		alterRe := regexp.MustCompile(`(?im)^ALTER TABLE aveloxis_data\.` +
			regexp.QuoteMeta(tbl) + `\s+ADD COLUMN IF NOT EXISTS\s+([a-z_]+)\s+([A-Z][^;\n]*)`)
		cols = append(cols, alterRe.FindAllStringSubmatch(string(schema), -1)...)
		for _, m := range cols {
			col := m[1]
			if skipCols[col] || strings.Contains(m[2], "SERIAL") {
				continue
			}
			totalCols++
			key := tbl + "." + col
			inventoried[key] = true
			if _, ok := documentedEmpty[key]; ok {
				continue
			}
			// Writer presence: the column must appear in a WRITER
			// POSITION — an INSERT column list or an UPDATE SET
			// left-hand side — of a statement naming THIS table.
			// v0.27.116 (round-10 suppressed finding, real): the
			// earlier check matched the name ANYWHERE in the SQL
			// literal, so a column read only in a WHERE predicate or
			// RETURNING clause of a write statement passed as
			// "written".
			found := false
			for _, block := range writeBlocks[tbl] {
				if sqlStatementWritesColumn(block, tbl, col) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%s appears in NO INSERT/UPDATE statement naming aveloxis_data.%s and has no documentedEmpty entry — either wire a writer or allowlist it WITH a reason (the 2026-08-19 fill-audit contract)", key, tbl)
			}
		}
	}
	// Self-check for the ALTER scan: whitespace_head_hash exists ONLY as
	// an ALTER guard (schema.sql line ~163), so its absence here means
	// ALTER-added columns silently dropped out of the audit again.
	if !inventoried["repos.whitespace_head_hash"] {
		t.Fatal("self-check: ALTER-added columns are not being inventoried (repos.whitespace_head_hash missing) — the round-8 ALTER coverage regressed")
	}
	if totalCols < 200 {
		t.Fatalf("self-check: only %d columns parsed across %d tables — the schema regex broke", totalCols, len(auditedTables))
	}

	// Reverse check: allowlist entries must reference real audited
	// tables (a renamed column must not leave a stale allowlist row).
	for key := range documentedEmpty {
		parts := strings.SplitN(key, ".", 2)
		if len(parts) != 2 {
			t.Errorf("malformed documentedEmpty key %q", key)
			continue
		}
		if !strings.Contains(string(schema), parts[1]) {
			t.Errorf("documentedEmpty entry %q names a column absent from schema.sql — stale allowlist row", key)
		}
	}
}

// sqlStatementWritesColumn reports whether a comment-stripped SQL
// literal that names the audited table writes the given column — i.e.
// the column appears in a WRITER POSITION:
//   - an INSERT INTO aveloxis_data.<tbl> (...) column list, or
//   - an UPDATE SET assignment's LEFT-hand side (`SET col =` or
//     `, col =` — including ON CONFLICT ... DO UPDATE SET).
//
// Appearances in WHERE predicates, RETURNING clauses, RHS expressions
// (COALESCE(col, ...), subqueries), or qualified references (v.col,
// c.col) deliberately do NOT count. Column lists never contain
// parentheses, so the [^)]* capture is safe; SQL WHERE clauses join
// with AND (never commas), so the `, col =` LHS shape cannot
// false-match a predicate.
func sqlStatementWritesColumn(block, tbl, col string) bool {
	insertListRe := regexp.MustCompile(`(?is)INSERT\s+INTO\s+aveloxis_data\.` + regexp.QuoteMeta(tbl) + `\s*\(([^)]*)\)`)
	wordRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(col) + `\b`)
	for _, m := range insertListRe.FindAllStringSubmatch(block, -1) {
		if wordRe.MatchString(m[1]) {
			return true
		}
	}
	setLHSRe := regexp.MustCompile(`(?is)(?:\bSET\s+|,\s*)` + regexp.QuoteMeta(col) + `\s*=`)
	return setLHSRe.MatchString(block)
}

// TestSQLStatementWritesColumnFixtures — the negative fixtures the
// round-10 suppressed finding asked for: predicate-only and
// RETURNING-only appearances must NOT count as writers.
func TestSQLStatementWritesColumnFixtures(t *testing.T) {
	cases := []struct {
		name  string
		block string
		tbl   string
		col   string
		want  bool
	}{
		{"set-lhs", `UPDATE aveloxis_data.repos SET repo_name = $1 WHERE repo_id = $2`, "repos", "repo_name", true},
		{"where-only", `UPDATE aveloxis_data.repos SET repo_name = $1 WHERE whitespace_head_hash = ''`, "repos", "whitespace_head_hash", false},
		{"insert-list", `INSERT INTO aveloxis_data.repos (repo_git, repo_name) VALUES ($1, $2) RETURNING repo_id`, "repos", "repo_git", true},
		{"returning-only", `INSERT INTO aveloxis_data.repos (repo_git) VALUES ($1) RETURNING repo_id`, "repos", "repo_id", false},
		{"conflict-set", `INSERT INTO aveloxis_data.repos (repo_git) VALUES ($1) ON CONFLICT (repo_git) DO UPDATE SET repo_name = EXCLUDED.repo_name`, "repos", "repo_name", true},
		{"rhs-only", `UPDATE aveloxis_data.repos SET repo_name = COALESCE(NULLIF(repo_git, ''), repo_name)`, "repos", "repo_git", false},
		{"multi-assign", `UPDATE aveloxis_data.repos SET repo_name = $1, repo_owner = $2 WHERE repo_id = $3`, "repos", "repo_owner", true},
		{"qualified-predicate", `UPDATE aveloxis_data.commits c SET cmt_added = v.added FROM (SELECT 1) v WHERE c.cmt_filename = v.filename`, "commits", "cmt_filename", false},
	}
	for _, c := range cases {
		if got := sqlStatementWritesColumn(c.block, c.tbl, c.col); got != c.want {
			t.Errorf("%s: sqlStatementWritesColumn(%q) = %v, want %v", c.name, c.col, got, c.want)
		}
	}
}
