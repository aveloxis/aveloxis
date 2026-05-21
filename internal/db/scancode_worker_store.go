// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package db — scancode_worker_store.go contains the store methods
// the v0.21.0 ScancodeWorker uses to claim repos, persist lock state
// across restarts, and recover orphans after an ungraceful exit.
//
// See docs/architecture/scancode.md for the architectural rationale
// and the four-state recovery table.

package db

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// ScancodeStaleLockWindow is how long a scancode_locked_at value can
// be without forcing the recovery pass to treat the row as a silent
// corpse. Generous because Linux-kernel-sized scancode runs can
// legitimately take many hours. Recovery via the explicit
// recoverOrphans pass (PID + boot_id check) is the primary path;
// this window is just the silent-fallback safety net.
const ScancodeStaleLockWindow = 12 * time.Hour

// ScancodeMaxFailures (v0.21.4) is the consecutive-failure count
// after which RecordScancodeFailure stamps scancode_last_run = NOW()
// so the cadence gate excludes the row for the full cadence window
// (default 180 days). The exponential backoff schedule (quadratic,
// capped at 7 days) handles attempts 1 through ScancodeMaxFailures-1;
// once a repo has failed ScancodeMaxFailures times in a row it's
// presumed permanently broken (LFS-budget exhausted, malformed
// source tree, scancode-incompatible content) and shouldn't burn
// further worker time until the cadence gate naturally expires.
//
// 10 means an operator who fixes the underlying problem (top-ups
// the LFS budget, removes the broken file) gets up to ~10×7d = 70
// days × the natural Sunday-cron cadence to see the repo retry on
// its own. For faster retry, operators clear scancode_failed_attempts
// directly: `UPDATE aveloxis_data.repos SET scancode_failed_attempts
// = 0, scancode_last_failed_at = NULL, scancode_last_run = NULL
// WHERE repo_id = X`.
const ScancodeMaxFailures = 10

// ScancodeJob is the unit of work the ScancodeWorker dispatcher
// hands to a runner goroutine.
type ScancodeJob struct {
	RepoID    int64
	RepoOwner string
	RepoName  string
	RepoGit   string
	// TimeoutAttempts (v0.23.8) is the row's scancode_timeout_attempts
	// counter at the moment of claim. The runner uses it to compute
	// the effective wall-clock timeout for this job as
	// `min(base * 2^TimeoutAttempts, cap)`. Kernel-class repos that
	// have timed out before get progressively longer timeouts until
	// the scan completes (resetting the counter) or hits the cap.
	TimeoutAttempts int
}

// ScancodeLockedRow is a row observed by the recovery pass during
// ScancodeWorker startup. The (LockedPID, LockedBootID) pair drives
// the four-state recovery decision: reboot survivor, live orphan,
// recoverable corpse, lost run.
type ScancodeLockedRow struct {
	RepoID       int64
	RepoOwner    string
	RepoName     string
	RepoGit      string
	LockedPID    int
	LockedBootID string
	OutputPath   string
	LockedAt     time.Time
}

// ClaimNextScancodeRepo atomically claims the highest-priority
// eligible repo for scancode scanning. Returns (nil, nil) when no
// eligible row exists — the caller treats this as "queue empty,
// wait for the next tick".
//
// Eligibility rules (must all hold):
//
//  1. The repo has been collected at least once
//     (q.last_collected IS NOT NULL). Scancode never runs on a
//     repo until basic API metrics exist.
//
//  2. The repo is not archived
//     (COALESCE(repo_archived, FALSE) = FALSE). Matches the
//     partial index idx_repos_scancode_due predicate so the
//     planner uses the index instead of a sequential scan.
//
//  3. Cadence window has elapsed (scancode_last_run IS NULL OR
//     scancode_last_run < NOW() - cadence). Operator-configured
//     via collection.scancode_cadence_days, default 180 days.
//
//  4. The row is not actively locked (scancode_locked_at IS NULL
//     OR locked_at older than ScancodeStaleLockWindow). The
//     stale-lock window is the silent-corpse fallback — the
//     primary recovery path is the worker's startup recoverOrphans
//     scan which uses the (pid, boot_id) tuple to make precise
//     decisions.
//
// FOR UPDATE SKIP LOCKED on the candidate CTE makes the claim
// race-safe against any concurrent dispatcher (multiple aveloxis
// serve processes against the same DB, or a future shard split).
// Postgres' row-level lock + SKIP LOCKED is the standard job-queue
// idiom for this shape of problem.
func (s *PostgresStore) ClaimNextScancodeRepo(ctx context.Context, cadence time.Duration) (*ScancodeJob, error) {
	if cadence <= 0 {
		cadence = 180 * 24 * time.Hour
	}
	// v0.21.4 backoff gate: rows that recently failed are gated by
	// a per-row exponential window. The window is quadratic in
	// scancode_failed_attempts (1h → 4h → 9h → 16h → 25h → 36h →
	// 49h → 64h → 81h → 100h → 121h → 144h → 168h cap). Cap at
	// 168 hours (7 days) so even pathological cases retry weekly
	// rather than monthly. Implemented as
	// `NOW() - make_interval(hours => LEAST(attempts*attempts,168))`
	// — make_interval is the only built-in way to express a
	// row-dependent interval; concatenating into ::interval works
	// but is ugly and slower.
	row := s.pool.QueryRow(ctx, `
		WITH candidate AS (
		    SELECT r.repo_id
		    FROM aveloxis_data.repos r
		    JOIN aveloxis_ops.collection_queue q USING (repo_id)
		    WHERE q.last_collected IS NOT NULL
		      AND COALESCE(r.repo_archived, FALSE) = FALSE
		      AND (r.scancode_last_run IS NULL
		           OR r.scancode_last_run < NOW() - $1::interval)
		      AND (r.scancode_locked_at IS NULL
		           OR r.scancode_locked_at < NOW() - $2::interval)
		      AND (r.scancode_last_failed_at IS NULL
		           OR r.scancode_last_failed_at < NOW() - make_interval(
		               hours => LEAST(
		                   GREATEST(COALESCE(r.scancode_failed_attempts, 0), 1)
		                   * GREATEST(COALESCE(r.scancode_failed_attempts, 0), 1),
		                   168)))
		    ORDER BY r.scancode_last_run NULLS FIRST, r.repo_id
		    LIMIT 1
		    FOR UPDATE SKIP LOCKED
		)
		UPDATE aveloxis_data.repos
		SET scancode_locked_at = NOW()
		WHERE repo_id IN (SELECT repo_id FROM candidate)
		RETURNING repo_id, repo_owner, repo_name, repo_git,
		          COALESCE(scancode_timeout_attempts, 0)`,
		cadence.String(), ScancodeStaleLockWindow.String())

	var job ScancodeJob
	if err := row.Scan(&job.RepoID, &job.RepoOwner, &job.RepoName, &job.RepoGit, &job.TimeoutAttempts); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &job, nil
}

// RecordScancodeLockState persists the OS PID, kernel boot_id, and
// scancode output path immediately after cmd.Start() returns. The
// recovery pass on the NEXT aveloxis startup uses these to decide
// what to do with each locked row.
//
// scancode_locked_at is already set by the claim; we don't update
// it here so the lock age math (vs ScancodeStaleLockWindow) keeps
// reflecting the original claim time. Updating it would mask
// silently-stuck scans behind a constantly-fresh timestamp.
func (s *PostgresStore) RecordScancodeLockState(ctx context.Context, repoID int64, pid int, bootID, outputPath string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET scancode_locked_pid = $2,
		    scancode_locked_boot_id = $3,
		    scancode_output_path = $4
		WHERE repo_id = $1`, repoID, pid, bootID, outputPath)
	return err
}

// MarkScancodeComplete sets scancode_last_run = NOW(), captures the
// version that ran, and clears all four lock columns in a single
// UPDATE. Called from the success path of runOne, AFTER the
// scancode_scans + scancode_file_results inserts have committed.
//
// Atomic: the worker's "I finished" signal is one row write; no
// possibility of a partial state where last_run is set but a lock
// column lingers (which would confuse the next recovery pass).
//
// v0.21.4: also resets the failure counter (scancode_failed_attempts
// = 0, scancode_last_failed_at = NULL) so a repo that previously
// failed several times then succeeds doesn't carry the old attempt
// count into the next failure cycle.
func (s *PostgresStore) MarkScancodeComplete(ctx context.Context, repoID int64, version string) error {
	// v0.23.8 also resets scancode_timeout_attempts so a previously
	// stretched repo that successfully completes starts fresh on the
	// next cycle with the base timeout. If it ever times out again,
	// the counter rebuilds from 0.
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET scancode_last_run = NOW(),
		    scancode_version = $2,
		    scancode_locked_at = NULL,
		    scancode_locked_pid = NULL,
		    scancode_locked_boot_id = NULL,
		    scancode_output_path = NULL,
		    scancode_failed_attempts = 0,
		    scancode_last_failed_at = NULL,
		    scancode_timeout_attempts = 0
		WHERE repo_id = $1`, repoID, version)
	return err
}

// RecordScancodeFailure (v0.21.4) is the failure-path counterpart
// to MarkScancodeComplete. It clears the lock columns AND increments
// scancode_failed_attempts AND stamps scancode_last_failed_at = NOW()
// so the claim query's backoff gate can compute time-since-failure.
//
// When the resulting failed_attempts reaches ScancodeMaxFailures the
// UPDATE also stamps scancode_last_run = NOW(), which lets the
// cadence gate (default 180 days) push the unrecoverable row out of
// the active queue. Without that escape valve, a permanently-failing
// repo would never stop consuming worker time — even the 7-day
// backoff cap would let it retry weekly forever.
//
// Replaces ClearScancodeLock on every repo-specific failure path
// in scancode_worker.runOne. ClearScancodeLock still exists for the
// dispatcher's ctx-canceled-after-claim cleanup (which is a clean
// release, not a failure).
func (s *PostgresStore) RecordScancodeFailure(ctx context.Context, repoID int64) error {
	// One UPDATE that:
	//   - clears the four lock columns,
	//   - increments scancode_failed_attempts (COALESCE for legacy NULL),
	//   - stamps scancode_last_failed_at = NOW(),
	//   - if the post-increment count >= ScancodeMaxFailures, also
	//     stamps scancode_last_run = NOW() so the cadence gate
	//     excludes the row.
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET scancode_locked_at = NULL,
		    scancode_locked_pid = NULL,
		    scancode_locked_boot_id = NULL,
		    scancode_output_path = NULL,
		    scancode_failed_attempts = COALESCE(scancode_failed_attempts, 0) + 1,
		    scancode_last_failed_at = NOW(),
		    scancode_last_run = CASE
		        WHEN COALESCE(scancode_failed_attempts, 0) + 1 >= $2 THEN NOW()
		        ELSE scancode_last_run
		    END
		WHERE repo_id = $1`, repoID, ScancodeMaxFailures)
	return err
}

// ClearScancodeLock clears the lock columns without touching
// scancode_last_run or scancode_version. Called from the failure
// path of runOne (clone error, scancode subprocess crash, output
// parse failure). The row stays eligible for the next claim tick
// without waiting for the 12-hour stale-lock fallback.
func (s *PostgresStore) ClearScancodeLock(ctx context.Context, repoID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET scancode_locked_at = NULL,
		    scancode_locked_pid = NULL,
		    scancode_locked_boot_id = NULL,
		    scancode_output_path = NULL
		WHERE repo_id = $1`, repoID)
	return err
}

// ClearStaleNullPidLocks clears scancode locks that have
// scancode_locked_at SET but scancode_locked_pid NULL — the
// inconsistent state produced when v0.21.0's runOne hit a
// RecordScancodeLockState failure and proceeded anyway. v0.23.3
// aborts the scan in that path so new occurrences shouldn't
// happen, but existing rows from pre-v0.23.3 runs need cleanup.
//
// olderThan filters to locks older than the threshold so we don't
// race a legitimate in-flight runner whose PID write is delayed
// by tens-of-ms DB latency. 5 minutes is comfortable.
//
// Returns the number of rows cleared. Called from the worker's
// in-flight cleanup goroutine every 5 minutes.
func (s *PostgresStore) ClearStaleNullPidLocks(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET scancode_locked_at = NULL,
		    scancode_locked_pid = NULL,
		    scancode_locked_boot_id = NULL,
		    scancode_output_path = NULL
		WHERE scancode_locked_at IS NOT NULL
		  AND scancode_locked_pid IS NULL
		  AND scancode_locked_at < NOW() - make_interval(secs => $1)`,
		olderThan.Seconds())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ListLockedScancodeRows returns every row with a non-null
// scancode_locked_at. The recovery pass calls this once at worker
// startup, before the dispatcher begins claiming new rows.
//
// Each returned row goes through the four-state decision:
//  1. boot_id differs from current → reboot survivor, clear lock.
//  2. boot_id matches, PID alive → live orphan, spawn monitor.
//  3. boot_id matches, PID dead, output file present and valid →
//     recoverable corpse, ingest the orphaned output.
//  4. boot_id matches, PID dead, no usable output → lost run,
//     clear lock.
//
// See docs/architecture/scancode.md for the full decision table.
func (s *PostgresStore) ListLockedScancodeRows(ctx context.Context) ([]ScancodeLockedRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT repo_id, repo_owner, repo_name, repo_git,
		       COALESCE(scancode_locked_pid, 0),
		       COALESCE(scancode_locked_boot_id, ''),
		       COALESCE(scancode_output_path, ''),
		       scancode_locked_at
		FROM aveloxis_data.repos
		WHERE scancode_locked_at IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ScancodeLockedRow
	for rows.Next() {
		var r ScancodeLockedRow
		if err := rows.Scan(&r.RepoID, &r.RepoOwner, &r.RepoName, &r.RepoGit,
			&r.LockedPID, &r.LockedBootID, &r.OutputPath, &r.LockedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
