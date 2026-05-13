// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.20.12 (Fix I): commits where cmt_author_platform_username
// is set but no matching contributor row exists today hold a
// NULL cmt_ght_author_id forever. Production diagnostic on the
// live DB showed 22,499 such commits across 571 distinct
// unresolvable logins (the 15,244-commit dominant cohort is
// the deleted bot `k8s-merge-robot`).
//
// Fix I creates placeholder contributor rows for every distinct
// unresolvable login observed in commits. The row carries
// gh_state='unresolved' so analysts can filter them out. The
// one-shot migration's WHERE clause filters for case-insensitive
// no-match against the live contributors table, so it composes
// with Fix H (case-insensitive backfill JOIN) — running both
// migrations together produces no double-creation.

func TestSchemaHasGhStateColumn(t *testing.T) {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Locate the CREATE TABLE for contributors.
	idx := strings.Index(src, "CREATE TABLE IF NOT EXISTS aveloxis_data.contributors")
	if idx < 0 {
		// Some schema.sql versions don't gate with IF NOT EXISTS.
		idx = strings.Index(src, "CREATE TABLE aveloxis_data.contributors")
	}
	if idx < 0 {
		t.Fatal("cannot find contributors DDL in schema.sql")
	}
	end := strings.Index(src[idx:], ");")
	if end < 0 {
		t.Fatal("cannot find end of contributors DDL")
	}
	ddl := src[idx : idx+end]

	if !strings.Contains(ddl, "gh_state") {
		t.Error("aveloxis_data.contributors schema must include gh_state column (v0.20.12 mirrors the gl_state column added in v0.20.3). gh_state='unresolved' marks placeholder rows created for logins that 404 on the API.")
	}
}

func TestMigrateAddsGhStateColumn(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	needle := `addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.contributors", "gh_state"`
	if !strings.Contains(src, needle) {
		t.Errorf("migrate.go must call addColumnIfMissing for aveloxis_data.contributors.gh_state — operators upgrading from <v0.20.12 need the column added automatically. Pattern matches the gl_state precedent.")
	}
}

func TestMigrationBackfillsUnresolvedLoginPlaceholders(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Pin the SQL shape: INSERT into contributors with
	// gh_state='unresolved' for each distinct unresolvable login
	// in commits. Composes with Fix H via the
	// LOWER-NOT-EXISTS filter.
	needles := []string{
		"v0.20.12 backfill placeholder contributors for unresolvable logins",
		"INSERT INTO aveloxis_data.contributors",
		"gh_state",
		"'unresolved'",
		"FROM aveloxis_data.commits",
		"cmt_author_platform_username",
		"NOT EXISTS",
		"LOWER(",
	}
	for _, n := range needles {
		if !strings.Contains(src, n) {
			t.Errorf("migrate.go missing v0.20.12 placeholder-backfill needle %q — the migration must INSERT placeholder rows (gh_state='unresolved') for every distinct unresolvable login in commits, filtered via case-insensitive NOT EXISTS so it composes with Fix H.", n)
		}
	}
}

// TestPlaceholderBackfillIsIdempotent pins the ON CONFLICT
// behavior so re-running the migration is a no-op once rows
// exist.
func TestPlaceholderBackfillIsIdempotent(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "v0.20.12 backfill placeholder")
	if idx < 0 {
		t.Fatal("cannot find v0.20.12 placeholder backfill block")
	}
	tail := src[idx:]
	end := strings.Index(tail, "`)")
	if end < 0 {
		t.Fatal("cannot find end of v0.20.12 backfill SQL")
	}
	block := tail[:end]

	// Idempotency comes from the case-insensitive NOT EXISTS
	// filter (which excludes already-present logins from a prior
	// run). Acceptable alternative: explicit ON CONFLICT DO NOTHING.
	hasCondition := strings.Contains(block, "NOT EXISTS") &&
		strings.Contains(block, "LOWER(")
	hasConflict := strings.Contains(block, "ON CONFLICT") &&
		strings.Contains(block, "DO NOTHING")
	if !hasCondition && !hasConflict {
		t.Error("placeholder backfill must be idempotent — either via a NOT EXISTS filter with case-insensitive comparison (so re-running on already-flagged data inserts zero new rows) or via ON CONFLICT (cntrb_login) DO NOTHING.")
	}
}
