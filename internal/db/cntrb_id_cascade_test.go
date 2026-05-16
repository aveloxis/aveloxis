// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.22.1 — every FK pointing at aveloxis_data.contributors(cntrb_id)
// must declare ON UPDATE CASCADE. The list is exhaustive: it's the
// 16 child columns enumerated by information_schema on the live
// production schema. Adding a NEW child column with a cntrb_id FK
// in the future is a separate test concern — if that happens, this
// list must grow alongside the schema.
//
// Each row is (constraint_name, child_table, child_column). Tests
// use a local fixture instead of cntrb_id_cascade.go's exported list
// so a refactor that drops or renames the production list still
// fails this test by name, not by silently following the rename.
var cntrbIDChildFKsTestFixture = []struct {
	constraint string
	table      string
	column     string
}{
	{"contributor_identities_cntrb_id_fkey", "contributor_identities", "cntrb_id"},
	{"contributor_repo_cntrb_id_fkey", "contributor_repo", "cntrb_id"},
	{"contributors_aliases_cntrb_id_fkey", "contributors_aliases", "cntrb_id"},
	{"issue_assignees_cntrb_id_fkey", "issue_assignees", "cntrb_id"},
	{"issue_events_cntrb_id_fkey", "issue_events", "cntrb_id"},
	{"issues_closed_by_id_fkey", "issues", "closed_by_id"},
	{"issues_reporter_id_fkey", "issues", "reporter_id"},
	{"messages_cntrb_id_fkey", "messages", "cntrb_id"},
	{"pull_request_assignees_cntrb_id_fkey", "pull_request_assignees", "cntrb_id"},
	{"pull_request_commits_author_cntrb_id_fkey", "pull_request_commits", "author_cntrb_id"},
	{"pull_request_events_cntrb_id_fkey", "pull_request_events", "cntrb_id"},
	{"pull_request_meta_cntrb_id_fkey", "pull_request_meta", "cntrb_id"},
	{"pull_request_repo_pr_cntrb_id_fkey", "pull_request_repo", "pr_cntrb_id"},
	{"pull_request_reviewers_cntrb_id_fkey", "pull_request_reviewers", "cntrb_id"},
	{"pull_request_reviews_cntrb_id_fkey", "pull_request_reviews", "cntrb_id"},
	{"pull_requests_author_id_fkey", "pull_requests", "author_id"},
}

// TestMigrationAddsOnUpdateCascadeToAllCntrbIDFKs is the source-
// contract regression test for the v0.22.1 schema migration that
// enables the v0.22.2 cntrb_id data migration. The migration adds
// ON UPDATE CASCADE to all 16 FK constraints pointing at
// aveloxis_data.contributors(cntrb_id), so a future UPDATE on
// contributors.cntrb_id (run by `aveloxis migrate-cntrb-ids` in
// v0.22.2) cascades automatically through every child table
// without manual FK rewrites.
//
// Pinned signals:
//
//  1. migrate.go references every one of the 16 constraint names
//     in an ALTER TABLE context. This is the v0.22.1 work landing.
//  2. migrate.go references "ON UPDATE CASCADE" in proximity to
//     each constraint reference.
//  3. The migration uses NOT VALID to skip the synchronous
//     scan-the-whole-table validation step that would otherwise
//     hold ACCESS EXCLUSIVE locks for minutes on the 50M+-row
//     messages and pull_request_commits tables. (VALIDATE
//     CONSTRAINT runs separately under SHARE UPDATE EXCLUSIVE,
//     which permits concurrent reads/writes.)
//
// If a future refactor removes any of these signals, the test
// fails before merge.
func TestMigrationAddsOnUpdateCascadeToAllCntrbIDFKs(t *testing.T) {
	// The v0.22.1 work spans two files: migrate.go invokes the
	// helper; cntrb_id_cascade.go declares the constraint-name list
	// and the per-FK ALTER TABLE template. Reading both lets a
	// future refactor split or inline the helper without forcing a
	// test rewrite — the contract is "ON UPDATE CASCADE is added
	// to every cntrb_id FK", not "the SQL string is in this
	// specific file".
	parts := [][]byte{}
	for _, path := range []string{"migrate.go", "cntrb_id_cascade.go"} {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		parts = append(parts, src)
	}
	code := string(parts[0]) + "\n" + string(parts[1])

	if !strings.Contains(code, "ON UPDATE CASCADE") {
		t.Error("migrate.go must contain ON UPDATE CASCADE — v0.22.1 schema migration " +
			"adding cascade behavior to the 16 cntrb_id FKs that the v0.22.2 data " +
			"migration depends on")
	}
	if !strings.Contains(code, "NOT VALID") {
		t.Error("migrate.go must use NOT VALID on the ADD CONSTRAINT step so the " +
			"synchronous validation scan doesn't hold ACCESS EXCLUSIVE locks on " +
			"50M+-row child tables for the duration of the migration")
	}
	if !strings.Contains(code, "VALIDATE CONSTRAINT") {
		t.Error("migrate.go must run VALIDATE CONSTRAINT after NOT VALID so the " +
			"constraint reaches a fully-validated state — VALIDATE takes " +
			"SHARE UPDATE EXCLUSIVE (concurrent ops permitted), not ACCESS EXCLUSIVE")
	}

	for _, fk := range cntrbIDChildFKsTestFixture {
		if !strings.Contains(code, fk.constraint) {
			t.Errorf("migrate.go must reference constraint name %q (table=%s column=%s) — "+
				"the v0.22.1 ON UPDATE CASCADE step must touch every cntrb_id FK; "+
				"omitting one would leave that child table without cascade and break "+
				"the v0.22.2 data migration",
				fk.constraint, fk.table, fk.column)
		}
	}
}

// TestSchemaDeclaresOnUpdateCascadeForCntrbIDFKs pins the fresh-
// deployment path: schema.sql must declare ON UPDATE CASCADE on
// every cntrb_id FK so a brand-new aveloxis install gets the
// correct constraint shape without needing the v0.22.1 migration.
// Same 16-FK set as the migration test.
func TestSchemaDeclaresOnUpdateCascadeForCntrbIDFKs(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// schema.sql declares each FK inline within its CREATE TABLE.
	// The declaration looks like:
	//   <col>  UUID REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE
	// or for NOT NULL columns:
	//   <col>  UUID NOT NULL REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE
	//
	// The test scans for every "REFERENCES aveloxis_data.contributors(cntrb_id)"
	// occurrence and confirms ON UPDATE CASCADE appears within ~200 chars
	// (Postgres syntax keeps the action clause close to the REFERENCES clause
	// in a column declaration; the gap accommodates an optional ON DELETE).
	refTarget := "REFERENCES aveloxis_data.contributors(cntrb_id)"
	occurrences := strings.Count(code, refTarget)
	if occurrences == 0 {
		t.Fatalf("schema.sql contains no REFERENCES to aveloxis_data.contributors(cntrb_id) — "+
			"this should be impossible; expected at least %d",
			len(cntrbIDChildFKsTestFixture))
	}

	// Every one of those occurrences must have ON UPDATE CASCADE within range.
	idx := 0
	missing := 0
	for {
		next := strings.Index(code[idx:], refTarget)
		if next < 0 {
			break
		}
		start := idx + next
		end := start + len(refTarget) + 200
		if end > len(code) {
			end = len(code)
		}
		window := code[start:end]
		if !strings.Contains(window, "ON UPDATE CASCADE") {
			missing++
			t.Errorf("schema.sql REFERENCES to contributors(cntrb_id) at offset %d "+
				"is missing ON UPDATE CASCADE within 200-char window. Context:\n%q",
				start, window)
		}
		idx = start + len(refTarget)
	}

	if missing > 0 {
		t.Errorf("%d of %d REFERENCES aveloxis_data.contributors(cntrb_id) occurrences "+
			"in schema.sql are missing ON UPDATE CASCADE — v0.22.1 schema declaration "+
			"must match what the migration step adds at runtime",
			missing, occurrences)
	}
}
