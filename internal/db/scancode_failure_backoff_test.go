// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.21.4 — scancode failure tracking + exponential backoff.
//
// 2026-05-14 production diagnostic showed 10 repos in a tight failure
// loop: each scan failed in ~45s (clone fail or scancode exit 1),
// ClearScancodeLock cleared scancode_locked_at but LEFT
// scancode_last_run = NULL. The claim query orders NULLS FIRST on
// scancode_last_run, so failed repos became the highest-priority
// candidates for the very next dispatcher tick. With ~3-min legitimate
// scans and ~45s failing scans, failing repos cycled through the
// worker pool 4× faster than productive ones, dominating visible
// activity (a small failing cohort can starve a healthy fleet).
//
// Fix shape (this file pins the source contract):
//
//  1. Two new columns on aveloxis_data.repos:
//       scancode_failed_attempts INTEGER DEFAULT 0
//       scancode_last_failed_at  TIMESTAMPTZ
//
//  2. New store method RecordScancodeFailure(ctx, repoID) replaces
//     ClearScancodeLock on failure paths. It clears the lock columns,
//     increments failed_attempts, stamps last_failed_at = NOW(), AND
//     when attempts >= ScancodeMaxFailures (default 10) also stamps
//     scancode_last_run = NOW() so the cadence gate (default 180
//     days) pushes the row out of the queue.
//
//  3. ClaimNextScancodeRepo gains a backoff gate:
//       scancode_last_failed_at IS NULL
//         OR scancode_last_failed_at < NOW() - backoff(attempts)
//     where backoff = LEAST(attempts*attempts, 168) hours.
//     attempts=1 → 1h, 2 → 4h, 3 → 9h, ..., 13+ → 7d cap.
//
//  4. MarkScancodeComplete resets failed_attempts to 0 on success.
//
//  5. Schema + migrate.go declare the new columns so a fresh DB or
//     an upgrade both end up with the same shape.

func TestSchemaHasScancodeFailureColumns(t *testing.T) {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	for _, needle := range []string{
		"scancode_failed_attempts",
		"scancode_last_failed_at",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("schema.sql must declare %s on aveloxis_data.repos so a fresh DB has the column at table-create time, not retrofitted via migrate", needle)
		}
	}
}

func TestMigrateAddsScancodeFailureColumns(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	columnTypes := map[string]string{
		"scancode_failed_attempts": "INTEGER DEFAULT 0",
		"scancode_last_failed_at":  "TIMESTAMPTZ",
	}
	for col, typ := range columnTypes {
		needle := `addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "` + col + `", "` + typ + `"`
		if !strings.Contains(src, needle) {
			t.Errorf("migrate.go must call addColumnIfMissing for aveloxis_data.repos.%s (%s). Without this, upgrading instances would crash the v0.21.4 ScancodeWorker when it tries to update the column", col, typ)
		}
	}
}

func TestRecordScancodeFailureExists(t *testing.T) {
	data, err := os.ReadFile("scancode_worker_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	if !strings.Contains(src, "func (s *PostgresStore) RecordScancodeFailure(") {
		t.Error("scancode_worker_store.go must declare RecordScancodeFailure(ctx, repoID). This replaces the v0.21.0 ClearScancodeLock call on failure paths so failures get tracked and backed off instead of looping immediately.")
	}

	idx := strings.Index(src, "func (s *PostgresStore) RecordScancodeFailure(")
	if idx < 0 {
		return
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		endRel = len(tail) - 1
	}
	body := tail[:1+endRel]

	// The UPDATE must increment scancode_failed_attempts.
	if !strings.Contains(body, "scancode_failed_attempts") {
		t.Error("RecordScancodeFailure must UPDATE scancode_failed_attempts = COALESCE(scancode_failed_attempts, 0) + 1 (or equivalent). Without this counter, backoff has no input.")
	}
	// Must stamp scancode_last_failed_at = NOW().
	if !strings.Contains(body, "scancode_last_failed_at") {
		t.Error("RecordScancodeFailure must stamp scancode_last_failed_at = NOW() so the claim-query backoff gate can compute time-since-failure.")
	}
	// Must clear the lock columns (was the whole job of the
	// pre-v0.21.4 ClearScancodeLock).
	if !strings.Contains(body, "scancode_locked_at = NULL") {
		t.Error("RecordScancodeFailure must clear scancode_locked_at = NULL so the row becomes eligible again after backoff (the claim query's lock-age gate would otherwise hold it for 12 hours).")
	}
	// Must eventually push the row out of the queue when failures
	// are unrecoverable: at attempts >= ScancodeMaxFailures, stamp
	// scancode_last_run = NOW() so the cadence gate excludes it.
	if !strings.Contains(body, "scancode_last_run") {
		t.Error("RecordScancodeFailure must reference scancode_last_run — when failed_attempts >= ScancodeMaxFailures, stamp it to NOW() so the cadence gate (default 180 days) pushes the unrecoverable row out of the queue. Otherwise a permanently-failing repo loops forever after exhausting the exponential backoff schedule.")
	}
}

func TestMarkScancodeCompleteResetsFailureCounter(t *testing.T) {
	data, err := os.ReadFile("scancode_worker_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *PostgresStore) MarkScancodeComplete(")
	if idx < 0 {
		t.Fatal("cannot find MarkScancodeComplete")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		endRel = len(tail) - 1
	}
	body := tail[:1+endRel]

	if !strings.Contains(body, "scancode_failed_attempts = 0") {
		t.Error("MarkScancodeComplete must reset scancode_failed_attempts = 0 on success. Without this, a repo that fails 5 times then succeeds carries the high failed_attempts forever — future failures would skip the lower backoff tiers and jump to maximum delay immediately.")
	}
	if !strings.Contains(body, "scancode_last_failed_at = NULL") {
		t.Error("MarkScancodeComplete must clear scancode_last_failed_at = NULL on success so the backoff gate doesn't consult a stale timestamp on the next failure.")
	}
}

func TestClaimNextScancodeRepoBackoffGate(t *testing.T) {
	data, err := os.ReadFile("scancode_worker_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *PostgresStore) ClaimNextScancodeRepo(")
	if idx < 0 {
		t.Fatal("cannot find ClaimNextScancodeRepo")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		endRel = len(tail) - 1
	}
	body := tail[:1+endRel]

	// Pin both halves of the backoff gate.
	if !strings.Contains(body, "scancode_last_failed_at IS NULL") {
		t.Error("ClaimNextScancodeRepo's WHERE clause must include scancode_last_failed_at IS NULL OR <backoff window past>. Without this gate, a freshly-failed repo is re-eligible on the very next dispatcher tick (the v0.21.4 loop bug).")
	}
	if !strings.Contains(body, "scancode_failed_attempts") {
		t.Error("ClaimNextScancodeRepo must reference scancode_failed_attempts when computing the backoff window — exponential backoff requires the attempt count as input.")
	}
	// The SQL must reference an interval expression that depends on
	// failed_attempts. Pin the simplest readable form
	// (make_interval(hours => ...)) so a future refactor that drops
	// the expression fails the test.
	if !strings.Contains(body, "make_interval") {
		t.Error("ClaimNextScancodeRepo's backoff gate must use make_interval(hours => ...) to compute the per-row backoff window from scancode_failed_attempts. Plain `NOW() - $N::interval` won't work because the interval is row-dependent.")
	}
}

func TestScancodeMaxFailuresConstant(t *testing.T) {
	// The threshold for "push the row out of the queue" lives as a
	// named constant so operators can grep for it and engineers
	// can see the policy in one place.
	data, err := os.ReadFile("scancode_worker_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "ScancodeMaxFailures") {
		t.Error("scancode_worker_store.go must declare a named constant ScancodeMaxFailures (default 10) controlling when RecordScancodeFailure stamps scancode_last_run = NOW() to push the unrecoverable repo out of the active queue.")
	}
}
