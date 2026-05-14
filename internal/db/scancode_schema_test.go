// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.21.0 — Scancode is decoupled from the main collection pipeline and
// runs in a separate ScancodeWorker pool. The schema changes that
// support the worker live on aveloxis_data.repos and are exercised by
// claim/recover SQL inside the worker. Pre-v0.21.0, scancode ran
// inline in AnalysisCollector.AnalyzeRepo gated by a 2-slot semaphore;
// the 2026-05-14 production incident showed 177 of 180 workers
// queued behind that semaphore for 7+ hours, effectively stalling the
// whole fleet. The new architecture moves the work off the per-job
// hot path entirely.
//
// Six columns are added to aveloxis_data.repos:
//   - scancode_last_run        TIMESTAMPTZ  — when the last successful scan completed
//   - scancode_version         TEXT         — version of the scancode binary that ran
//   - scancode_locked_at       TIMESTAMPTZ  — when the in-flight scan started; NULL when idle
//   - scancode_locked_pid      INTEGER      — OS PID of the scancode subprocess
//   - scancode_locked_boot_id  TEXT         — kernel boot_id at lock time (PID-reuse safety)
//   - scancode_output_path     TEXT         — where the subprocess writes results.json
//
// Plus a partial index supporting the worker's claim query, and an
// idempotent backfill from aveloxis_scan.scancode_scans so repos
// already scanned before v0.21.0 don't get re-scanned immediately.

func TestSchemaHasScancodeColumns(t *testing.T) {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	needles := []string{
		"scancode_last_run",
		"scancode_version",
		"scancode_locked_at",
		"scancode_locked_pid",
		"scancode_locked_boot_id",
		"scancode_output_path",
	}
	for _, n := range needles {
		if !strings.Contains(src, n) {
			t.Errorf("schema.sql must declare %s on aveloxis_data.repos — the v0.21.0 ScancodeWorker depends on it to claim repos, track in-flight subprocesses across restarts, and recover orphans without losing data", n)
		}
	}
}

func TestMigrateAddsScancodeColumns(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	columnTypes := map[string]string{
		"scancode_last_run":       "TIMESTAMPTZ",
		"scancode_version":        "TEXT",
		"scancode_locked_at":      "TIMESTAMPTZ",
		"scancode_locked_pid":     "INTEGER",
		"scancode_locked_boot_id": "TEXT",
		"scancode_output_path":    "TEXT",
	}
	for col, typ := range columnTypes {
		needle := `addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "` + col + `", "` + typ + `"`
		if !strings.Contains(src, needle) {
			t.Errorf("migrate.go must call addColumnIfMissing for aveloxis_data.repos.%s (%s). Operators upgrading from <v0.21.0 need the column added automatically; missing it would break ScancodeWorker startup", col, typ)
		}
	}
}

func TestMigrateCreatesScancodeDueIndex(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	// Partial index excludes archived repos; orders NULLS FIRST so
	// never-scanned repos sort to the front of the worker's claim
	// query.
	for _, needle := range []string{
		"idx_repos_scancode_due",
		"scancode_last_run NULLS FIRST",
		"WHERE COALESCE(repo_archived, FALSE) = FALSE",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("migrate.go must create idx_repos_scancode_due (partial, NULLS FIRST, excludes archived). Missing needle: %q. Without the index, the ScancodeWorker's claim query falls back to a sequential scan of repos every 90 seconds", needle)
		}
	}
}

func TestMigrationBackfillsFromScancodeScans(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	// The backfill must:
	//   1. Pull MAX(created_at) and most-recent scancode_version per repo
	//      from aveloxis_scan.scancode_scans
	//   2. Filter `WHERE r.scancode_last_run IS NULL` for idempotency
	//      (re-running migrate is a no-op once backfill completes)
	//   3. Use execMigrationStep so failures surface via the v0.19.4
	//      fail-closed contract
	for _, needle := range []string{
		"v0.21.0 backfill scancode_last_run from aveloxis_scan.scancode_scans",
		"aveloxis_scan.scancode_scans",
		// aveloxis_scan.scancode_scans uses data_collection_date,
		// NOT created_at — that's the convention across the
		// scan-related tables. A prior draft of this test pinned
		// MAX(created_at), which silently passed the source-grep
		// test but failed in production with SQLSTATE 42703. The
		// test now pins the actual column name so any future
		// refactor that re-introduces the wrong column fails CI.
		"MAX(data_collection_date)",
		"r.scancode_last_run IS NULL",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("migrate.go must backfill scancode_last_run + scancode_version from existing aveloxis_scan.scancode_scans rows so v0.20-era scanned repos don't all re-scan on first v0.21.0 startup. Missing needle: %q", needle)
		}
	}
}
