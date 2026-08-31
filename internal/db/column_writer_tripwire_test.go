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
//
// v0.27.124 (regression-infrastructure Phase 2): the scanning machinery
// moved to internal/srctest/sqlscan — corpus via srctest.PackageFiles,
// statements via sqlscan.Statements (comment-stripped AND split at
// top-level semicolons, so a `SET col =` in an unrelated trailing
// statement of a multi-statement literal can no longer satisfy "this
// table writes col"), writer positions via Stmt.WritesColumn (the
// sqlStatementWritesColumn logic + its fixtures, moved wholesale).
// Behavior-identity verified at port time: identical zero-violation
// result on the real corpus, and a fake schema column / stale
// allowlist entry flag identically before and after.
package db

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
	"github.com/aveloxis/aveloxis/internal/srctest/sqlscan"
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
	"contributor_identities", "repo_labor", "repo_lockfile_edges",
	// v0.29.0 Part E: the identity-bearing mailing-list/Jira chain —
	// found ABSENT from this audit while carrying the columns the
	// whole attribution program hangs off (a dark column here is a
	// dark identity).
	"email_message", "email_message_ref", "issue_message_ref",
	"pull_request_message_ref", "contributors_aliases",
	"jira_identities",
}

// documentedEmpty: columns with NO writer, kept as Augur schema-parity
// ballast or awaiting a data source. Every entry needs a reason — this
// list IS the documentation contract (mirrored in
// docs/architecture/column-mapping.md "Known-empty columns").
var documentedEmpty = map[string]string{
	"contributors_aliases.cntrb_last_modified": "DEFAULT NOW() at insert; alias rows are insert-only (ON CONFLICT keeps or repoints, never edits in place), so no refresh writer exists by design",
	"jira_identities.first_seen":              "DEFAULT NOW() at first insert and deliberately never refreshed — it records the FIRST observation; last_seen is the refreshed twin",
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

	// The writer corpus: every non-test store source file, decomposed
	// into per-statement, comment-stripped, file-attributed SQL via the
	// Phase 2 engine. The ≥30-files self-check is PackageFiles' required
	// minFiles parameter.
	stmts := sqlscan.Statements(srctest.PackageFiles(t, "internal/db", 30))
	writeStmts := map[string][]sqlscan.Stmt{}
	for _, tbl := range auditedTables {
		writeStmts[tbl] = sqlscan.FindWrites(stmts, "aveloxis_data."+tbl)
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
			// "written". v0.27.124: per-STATEMENT via sqlscan, so a
			// SET in an unrelated statement of the same literal no
			// longer counts either.
			found := false
			for _, ws := range writeStmts[tbl] {
				if ws.WritesColumn("aveloxis_data."+tbl, col) {
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

// sqlStatementWritesColumn and its fixture table moved WHOLESALE to
// internal/srctest/sqlscan (Stmt.WritesColumn +
// TestWritesColumnFixtures) in v0.27.124 — Phase 2 of the
// regression-infrastructure plan. This file keeps only the audit's
// registry data (auditedTables, documentedEmpty) and the audit itself.
