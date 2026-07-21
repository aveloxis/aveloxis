// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// v0.23.0 — contributor_login_history captures every (cntrb_id, platform_id,
// login) tuple ever observed for a contributor. Closes the rename-audit
// gap from v0.22.13's "what login has this person been known by between
// then and now?" question, which the v0.22.13 contract called out as
// "Not stored. Only the original observation (cntrb_login) and the
// current state (gh_login) survive."
//
// Design:
//   - One row per (cntrb_id, platform_id, login) triple, enforced by
//     UNIQUE constraint. first_seen preserved on re-observation;
//     last_seen advances every time the login is observed again.
//   - source TEXT column records WHY the row was created: 'observation'
//     (steady-state upsert), 'rename_recovery' (v0.22.13 batch path),
//     'rename_breadth' (v0.22.12 breadth-worker path), 'backfill'
//     (initial migration from existing data).
//   - FK on cntrb_id with v0.22.7 uniform clause: ON UPDATE CASCADE
//     ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED.
//
// Backfill: every (cntrb_id, platform_id, login) from existing
// contributor_identities rows + every historical cntrb_login per
// platform observed via identities, materialized on first
// aveloxis migrate after v0.23.0 install.

func TestContributorLoginHistorySchemaDeclared(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "CREATE TABLE IF NOT EXISTS aveloxis_data.contributor_login_history") {
		t.Error("schema.sql must declare aveloxis_data.contributor_login_history " +
			"(v0.23.0 — login-history audit trail).")
	}
	// Required columns. Each is a separate check so a missing one
	// names itself in the test failure.
	for _, col := range []string{
		"history_id",
		"cntrb_id",
		"platform_id",
		"login",
		"first_seen",
		"last_seen",
		"source",
	} {
		// Find the CREATE TABLE block specifically.
		start := strings.Index(code, "CREATE TABLE IF NOT EXISTS aveloxis_data.contributor_login_history")
		if start < 0 {
			t.Fatalf("table declaration not found — cannot verify column %q", col)
		}
		end := strings.Index(code[start:], ");")
		if end < 0 {
			t.Fatal("unterminated CREATE TABLE for contributor_login_history")
		}
		block := code[start : start+end]
		if !strings.Contains(block, col) {
			t.Errorf("contributor_login_history must declare a %q column", col)
		}
	}

	// FK on cntrb_id with v0.22.7 uniform clause.
	if !regexp.MustCompile(`cntrb_id\s+UUID[\s\S]{0,400}REFERENCES\s+aveloxis_data\.contributors`).MatchString(code) {
		t.Error("contributor_login_history.cntrb_id must REFERENCE aveloxis_data.contributors")
	}
	// Per v0.22.7 universal contract, every FK uses
	// DEFERRABLE INITIALLY DEFERRED. The cntrb_id FK also takes
	// ON UPDATE CASCADE per v0.22.1.
	start := strings.Index(code, "CREATE TABLE IF NOT EXISTS aveloxis_data.contributor_login_history")
	end := strings.Index(code[start:], ");")
	block := code[start : start+end]
	if !strings.Contains(block, "DEFERRABLE INITIALLY DEFERRED") {
		t.Error("contributor_login_history.cntrb_id FK must carry " +
			"DEFERRABLE INITIALLY DEFERRED (per v0.22.7 universal contract).")
	}
	if !strings.Contains(block, "ON UPDATE CASCADE") {
		t.Error("contributor_login_history.cntrb_id FK must carry ON UPDATE CASCADE " +
			"(per v0.22.1 — every cntrb_id child column uses CASCADE so renames propagate).")
	}

	// UNIQUE constraint on (cntrb_id, platform_id, login).
	if !regexp.MustCompile(`UNIQUE\s*\(\s*cntrb_id\s*,\s*platform_id\s*,\s*login\s*\)`).MatchString(block) {
		t.Error("contributor_login_history must declare UNIQUE (cntrb_id, platform_id, login) " +
			"— that's the natural identity-key of a login observation. Without it, the " +
			"RecordContributorLoginObservation ON CONFLICT clause has no target.")
	}
}

func TestContributorLoginHistoryMigrationCreatesTable(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "contributor_login_history") {
		t.Error("migrate.go must reference contributor_login_history. For existing " +
			"installations the schema-up-to-date check is what triggers serve to refuse " +
			"to start until the column set matches the binary's expectations.")
	}
}

func TestContributorLoginHistoryBackfillFromIdentities(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// The backfill must seed history from contributor_identities (the
	// only place that pairs cntrb_id with platform_id + login) AND
	// from contributors.cntrb_login (the historical-original login,
	// which is what differs from the current identity.login when a
	// rename has happened).
	if !regexp.MustCompile(`INSERT\s+INTO\s+aveloxis_data\.contributor_login_history[\s\S]{0,800}FROM\s+aveloxis_data\.contributor_identities`).MatchString(code) {
		t.Error("migrate.go must include a backfill that SELECTs from " +
			"contributor_identities into contributor_login_history. That table is the " +
			"only source of (cntrb_id, platform_id, login) triples in pre-v0.23.0 data.")
	}
	if !strings.Contains(code, "'backfill'") {
		t.Error("backfill INSERT must mark its rows with source='backfill' so operators " +
			"can distinguish migration-origin history rows from steady-state observations.")
	}
}

func TestRecordContributorLoginObservationExists(t *testing.T) {
	// Helper can live in any file in the package; check both likely
	// locations.
	for _, fname := range []string{"contributor_login_history.go", "postgres.go"} {
		src, err := os.ReadFile(fname)
		if err != nil {
			continue
		}
		if strings.Contains(string(src), "recordLoginObservation") ||
			strings.Contains(string(src), "RecordLoginObservation") {
			return
		}
	}
	t.Error("v0.23.0 must define a recordLoginObservation(ctx, tx, cntrbID, " +
		"platformID, login, source) helper that upserts into contributor_login_history. " +
		"Callers from UpsertContributorBatch, RenameContributorGhLogin, and ContributorResolver." +
		"Resolve all share this helper so the source-column policy stays consistent.")
}

func TestUpsertContributorBatchRecordsLoginHistory(t *testing.T) {
	body := extractContributorBatchBodies(t)
	if !strings.Contains(body, "recordLoginObservation") &&
		!strings.Contains(body, "contributor_login_history") {
		t.Error("UpsertContributorBatch must call recordLoginObservation (or " +
			"inline-insert into contributor_login_history) for every successful " +
			"per-identity write. Without this the steady-state path doesn't populate " +
			"history and we only capture rename events.")
	}
}

func TestRenameContributorGhLoginRecordsLoginHistory(t *testing.T) {
	src, err := os.ReadFile("contributor_search_resolve.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	startIdx := strings.Index(code, "func (s *PostgresStore) RenameContributorGhLogin(")
	if startIdx < 0 {
		t.Fatal("RenameContributorGhLogin not found")
	}
	tail := code[startIdx+1:]
	endRel := strings.Index(tail, "\nfunc ")
	if endRel < 0 {
		endRel = len(tail)
	}
	body := code[startIdx : startIdx+1+endRel]

	if !strings.Contains(body, "recordLoginObservation") &&
		!strings.Contains(body, "contributor_login_history") {
		t.Error("RenameContributorGhLogin must record the new login in " +
			"contributor_login_history (via recordLoginObservation). The whole point of " +
			"the history table is to preserve the chain of observed logins — the rename " +
			"function is precisely where the chain advances.")
	}
}
