// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.23.2 — idx_staging_repo_id closes the v0.22.4 long-jobs
// watchdog's pathological query plan.
//
// The watchdog (internal/scheduler/long_jobs_watchdog.go) polls
// every 30s per active collection job with:
//
//   SELECT COUNT(*) FROM aveloxis_ops.staging WHERE repo_id = $1
//
// Pre-v0.23.2, the only indexes on aveloxis_ops.staging were:
//   - staging_pkey (on staging_id) — irrelevant to the watchdog filter
//   - idx_staging_unprocessed (repo_id, entity_type) WHERE NOT processed
//     — partial; doesn't apply to the watchdog's unfiltered query
//
// Result: the planner chose a parallel sequential scan of the entire
// staging table (112 GB / 8.97M rows on the production fleet) for
// every poll. EXPLAIN ANALYZE on 2026-05-21 measured:
//
//   - 4.5 seconds wall time per execution
//   - 64 GB of buffer reads per execution
//   - 5 backends (1 leader + 4 parallel workers) per execution
//
// With N active collection jobs, the watchdog fires ~N times per 30s.
// On a 20-worker fleet that's ~3 watchdog scans/sec at 5 backends
// each — a meaningful slice of total DB CPU going to "diagnostic
// that detects stalled jobs."
//
// The 2026-05-21 fast-shutdown analysis surfaced 4,754 cancellations
// of this exact statement across 5 days of log — the dominant
// canceled-statement type by 470x.
//
// Fix: add a non-partial btree index on staging(repo_id). The watchdog
// COUNT now uses an index scan that completes in ~10ms.

func TestSchemaDeclaresStagingRepoIDIndex(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "idx_staging_repo_id") {
		t.Fatal("schema.sql must declare CREATE INDEX idx_staging_repo_id " +
			"ON aveloxis_ops.staging (repo_id) — v0.23.2 fix for the long-jobs " +
			"watchdog query plan. Without this, the watchdog poll falls through " +
			"to parallel seq scan (4.5s and 64 GB of buffer reads per call on the " +
			"production 8.97M-row staging table).")
	}
	// Pin the non-partial shape — the existing idx_staging_unprocessed
	// is partial and doesn't cover the watchdog query. Anchor on the
	// CREATE INDEX statement specifically; a doc-comment block that
	// describes "WHERE NOT processed" elsewhere shouldn't false-match.
	createStart := strings.Index(code, "CREATE INDEX IF NOT EXISTS idx_staging_repo_id")
	if createStart < 0 {
		t.Fatal("CREATE INDEX IF NOT EXISTS idx_staging_repo_id statement not found in schema.sql")
	}
	tail := code[createStart:]
	end := strings.Index(tail, ";")
	if end < 0 {
		t.Fatal("idx_staging_repo_id CREATE INDEX statement not terminated")
	}
	stmt := tail[:end]
	if strings.Contains(stmt, "WHERE") {
		t.Error("idx_staging_repo_id CREATE INDEX must NOT be a partial index. The " +
			"watchdog COUNT(*) WHERE repo_id = $1 has no WHERE-clause predicate the " +
			"planner could match against a partial index. Make it a plain btree on " +
			"(repo_id).")
	}
}

func TestMigrationCreatesStagingRepoIDIndexConcurrently(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "idx_staging_repo_id") {
		t.Error("migrate.go must invoke execCreateIndexConcurrently for " +
			"idx_staging_repo_id so existing v0.22.x installations pick up the index " +
			"on next migrate. Without this, the production fleet's watchdog query " +
			"keeps hitting the 4.5s parallel seq scan path indefinitely.")
	}
	// Pin CONCURRENTLY — on the 112 GB production staging table a
	// blocking CREATE INDEX would lock writes for the full build
	// window (potentially hours).
	if !strings.Contains(code, "execCreateIndexConcurrently") || !strings.Contains(code, "idx_staging_repo_id") {
		t.Error("the migration step for idx_staging_repo_id must use " +
			"execCreateIndexConcurrently so the build doesn't block staging writes. " +
			"Operators may see a multi-hour build on a fleet-scale staging table; " +
			"CONCURRENTLY keeps production writes flowing during that window.")
	}
}
