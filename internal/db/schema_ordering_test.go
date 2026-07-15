// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestSchemaCreatesTablesBeforeReferencingThem enforces, file-wide,
// that schema.sql creates every table BEFORE any other statement
// references it (FK REFERENCES, LIKE ... INCLUDING, CREATE INDEX ON).
//
// Why this exists (v0.27.9): the v0.27.4 user_repo_stars block was
// added next to the vulnerability tables and carried
// `REFERENCES aveloxis_ops.users(user_id)` ~700 lines before
// aveloxis_ops.users was created. Because the base schema DDL runs as
// ONE implicit transaction, the forward reference rolled back the
// entire exec — including the CREATE SCHEMA statements — so a fresh
// database got NOTHING and the fail-closed migration collector
// reported 174 cascading errors. Populated scratch databases masked
// the bug completely: every CREATE ... IF NOT EXISTS no-ops there, so
// the local integration tier stayed green while CI's truly-empty
// postgres failed. This unit-tier check catches the whole class on
// any machine with zero database access.
//
// Rules:
//   - A CREATE TABLE's REFERENCES targets must already be created
//     (self-references are allowed — e.g. reply-to columns).
//   - LIKE <table> INCLUDING ... targets must already be created.
//   - CREATE INDEX ... ON <table> targets must already be created.
func TestSchemaCreatesTablesBeforeReferencingThem(t *testing.T) {
	raw, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("read schema.sql: %v", err)
	}
	// Strip -- line comments so prose mentioning table names or the
	// LIKE/REFERENCES keywords can't false-match (the
	// TestSchemaTableOrderingForLikeReferences lesson).
	commentRe := regexp.MustCompile(`--[^\n]*`)
	src := commentRe.ReplaceAllString(string(raw), "")

	createTableRe := regexp.MustCompile(`CREATE TABLE IF NOT EXISTS\s+([a-z_]+\.[a-z_]+)`)
	referencesRe := regexp.MustCompile(`REFERENCES\s+([a-z_]+\.[a-z_]+)`)
	likeRe := regexp.MustCompile(`\bLIKE\s+([a-z_]+\.[a-z_]+)`)
	createIndexRe := regexp.MustCompile(`CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?IF NOT EXISTS\s+(\S+)\s+ON\s+([a-z_]+\.[a-z_]+)`)

	created := map[string]bool{}
	var violations []string

	for _, stmt := range strings.Split(src, ";") {
		if m := createTableRe.FindStringSubmatch(stmt); m != nil {
			table := m[1]
			for _, ref := range referencesRe.FindAllStringSubmatch(stmt, -1) {
				if ref[1] != table && !created[ref[1]] {
					violations = append(violations,
						table+" declares REFERENCES "+ref[1]+" before that table is created")
				}
			}
			for _, lk := range likeRe.FindAllStringSubmatch(stmt, -1) {
				if !created[lk[1]] {
					violations = append(violations,
						table+" declares LIKE "+lk[1]+" before that table is created")
				}
			}
			created[table] = true
			continue
		}
		if m := createIndexRe.FindStringSubmatch(stmt); m != nil {
			if !created[m[2]] {
				violations = append(violations,
					"index "+m[1]+" is declared ON "+m[2]+" before that table is created")
			}
		}
	}

	// Self-check: if the CREATE TABLE regex rots, `created` collapses
	// and this test would trivially pass on an empty map while real
	// violations go unseen. The schema has 131 tables as of v0.27.9;
	// anything under 100 means the extraction broke, not the schema.
	if len(created) < 100 {
		t.Fatalf("schema table extraction found only %d tables — the CREATE TABLE regex has rotted; fix the test, do not trust this run", len(created))
	}

	for _, v := range violations {
		t.Errorf("schema.sql ordering violation: %s\n"+
			"  schema.sql executes as ONE implicit transaction on a fresh database —\n"+
			"  a forward reference rolls back the ENTIRE base DDL including CREATE SCHEMA.\n"+
			"  Move the dependent block below the table it references.", v)
	}
}
