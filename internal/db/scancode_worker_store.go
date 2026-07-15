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
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// ScancodeStaleLockWindow is the FLOOR on how long a
// scancode_locked_at value must age before the claim query treats the
// row as a silent corpse and lets a second worker claim it.
//
// v0.27.6: this is no longer the window itself — it is the minimum.
// The effective window is DERIVED by the worker as
// runTimeoutCap + 2h and passed into ClaimNextScancodeRepo, because
// the v0.23.8 adaptive timeout legitimately runs scans up to the
// 24-hour cap while this constant was still 12h: any scan past 12h
// had its lock treated as stale and a SECOND worker claimed the same
// repo (confirmed interleaving in the June 2026 production logs —
// two workers scanning pytorch/docs concurrently). Recovery via the
// explicit recoverOrphans pass (PID + boot_id check) remains the
// primary path; the age window is just the silent-fallback safety
// net, and it must always exceed the longest legitimate scan.
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
	// Languages (v0.27.6) is the repos.languages JSONB breakdown at
	// claim time (populated by v0.23.0's repo-metadata capture).
	// GitHub rows carry BYTE counts per language; GitLab rows carry
	// percentages ×100 (see UpdateRepoMetadata). The worker's
	// generated-content skip policy consults it BEFORE cloning. Nil
	// when the column is empty or unparseable — the skip never fires
	// in that case.
	Languages map[string]int64
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
	// LockedHost (v0.27.6) is the os.Hostname() of the machine whose
	// worker recorded the lock. The recovery pass only adjudicates
	// (pid, boot_id) liveness for locks whose host matches its own —
	// a PID from another machine can trivially collide with an
	// unrelated local process. Empty on rows locked by pre-v0.27.6
	// binaries (treated as own-host).
	LockedHost string
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
//     OR locked_at older than the caller-supplied staleLockWindow).
//     The stale-lock window is the silent-corpse fallback — the
//     primary recovery path is the worker's startup recoverOrphans
//     scan which uses the (pid, boot_id, host) tuple to make precise
//     decisions.
//
// staleLockWindow (v0.27.6) is DERIVED by the worker as
// runTimeoutCap + 2h so it always exceeds the longest legitimate
// scan. It is clamped here to the ScancodeStaleLockWindow floor
// (12h) — passing zero (or anything smaller) can never re-introduce
// the June 2026 duplicate-claim bug where a 12h constant undercut
// the 24h adaptive-timeout cap and a second worker claimed a repo
// mid-scan.
//
// FOR UPDATE SKIP LOCKED on the candidate CTE makes the claim
// race-safe against any concurrent dispatcher (multiple aveloxis
// serve processes against the same DB, or the v0.27.6 dedicated
// `aveloxis scancode-worker` host). Postgres' row-level lock + SKIP
// LOCKED is the standard job-queue idiom for this shape of problem.
func (s *PostgresStore) ClaimNextScancodeRepo(ctx context.Context, cadence, staleLockWindow time.Duration) (*ScancodeJob, error) {
	if cadence <= 0 {
		cadence = 180 * 24 * time.Hour
	}
	if staleLockWindow < ScancodeStaleLockWindow {
		staleLockWindow = ScancodeStaleLockWindow
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
		          COALESCE(scancode_timeout_attempts, 0),
		          COALESCE(languages::text, '{}')`,
		cadence.String(), staleLockWindow.String())

	var job ScancodeJob
	var languagesJSON string
	if err := row.Scan(&job.RepoID, &job.RepoOwner, &job.RepoName, &job.RepoGit, &job.TimeoutAttempts, &languagesJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	// Best-effort parse of the v0.23.0 languages breakdown for the
	// generated-content skip policy. An empty or unparseable value
	// leaves Languages nil — the skip never fires on unknown data.
	if languagesJSON != "" && languagesJSON != "{}" {
		var langs map[string]int64
		if err := json.Unmarshal([]byte(languagesJSON), &langs); err == nil {
			job.Languages = langs
		}
	}
	return &job, nil
}

// RecordScancodeLockState persists the OS PID, kernel boot_id, and
// scancode output path immediately after cmd.Start() returns. The
// recovery pass on the NEXT aveloxis startup uses these to decide
// what to do with each locked row.
//
// scancode_locked_at is already set by the claim; we don't update
// it here so the lock age math (vs the derived stale-lock window)
// keeps reflecting the original claim time. Updating it would mask
// silently-stuck scans behind a constantly-fresh timestamp.
//
// host (v0.27.6) is the os.Hostname() of the recording worker so a
// dedicated scancode host and the primary server can share the
// table: liveness of (pid, boot_id) is only adjudicable on the
// machine that recorded it.
func (s *PostgresStore) RecordScancodeLockState(ctx context.Context, repoID int64, pid int, bootID, outputPath, host string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET scancode_locked_pid = $2,
		    scancode_locked_boot_id = $3,
		    scancode_output_path = $4,
		    scancode_locked_host = $5
		WHERE repo_id = $1`, repoID, pid, bootID, outputPath, host)
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
	//
	// v0.27.6 also clears scancode_skip_reason: a repo previously
	// skipped by policy that later gets a REAL successful scan (repo
	// shrank, policy changed) must stop advertising the stale skip.
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET scancode_last_run = NOW(),
		    scancode_version = $2,
		    scancode_locked_at = NULL,
		    scancode_locked_pid = NULL,
		    scancode_locked_boot_id = NULL,
		    scancode_output_path = NULL,
		    scancode_locked_host = NULL,
		    scancode_failed_attempts = 0,
		    scancode_last_failed_at = NULL,
		    scancode_timeout_attempts = 0,
		    scancode_skip_reason = ''
		WHERE repo_id = $1`, repoID, version)
	return err
}

// MarkScancodeSkipped (v0.27.6) records a policy skip: the worker
// decided — WITHOUT cloning or scanning — that the repo should not be
// scanned this cycle (currently only reason = 'generated-content':
// >= 90% HTML+CSS+JS by language bytes AND > 5 GiB total; the
// pytorch/docs / WHO/smart-html class of multi-GB generated
// documentation artifacts).
//
// Stamps scancode_last_run = NOW() so the cadence gate applies to
// skips exactly like scans (the decision is re-evaluated at normal
// cadence — repos change), records the reason for operator
// visibility, and clears the lock columns. Deliberately does NOT
// touch scancode_failed_attempts / scancode_timeout_attempts — a
// previously spinning repo keeps its diagnostic trail.
func (s *PostgresStore) MarkScancodeSkipped(ctx context.Context, repoID int64, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET scancode_last_run = NOW(),
		    scancode_skip_reason = $2,
		    scancode_locked_at = NULL,
		    scancode_locked_pid = NULL,
		    scancode_locked_boot_id = NULL,
		    scancode_output_path = NULL,
		    scancode_locked_host = NULL
		WHERE repo_id = $1`, repoID, reason)
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
// RecordScancodeTimeout (v0.23.8) is the failure-path counterpart to
// RecordScancodeFailure for the SPECIFIC case of a wall-clock-timeout
// SIGKILL (subprocess error string contains "signal: killed"). It:
//
//   - clears the four lock columns (so the row is re-claimable),
//   - increments scancode_timeout_attempts (drives the adaptive
//     timeout formula in runOne: `min(base * 2^attempts, cap)`),
//   - stamps scancode_last_failed_at = NOW() (so the v0.21.4
//     quadratic backoff gate still applies — a kernel-class repo
//     doesn't immediately re-claim a worker slot after timing out),
//   - does NOT increment scancode_failed_attempts.
//
// The last point is the critical distinction. v0.21.4's
// RecordScancodeFailure increments scancode_failed_attempts; on
// reaching ScancodeMaxFailures (10), it also stamps
// scancode_last_run so the cadence gate sidelines the row for 180
// days. That's correct for repos that fail because of broken
// content (PDF crashes, etc.). It is WRONG for repos that simply
// take longer than the wall-clock timeout — those are legitimately
// big, not broken. v0.23.8 separates the failure classes so
// kernel-class repos can stretch their timeout indefinitely
// (capped at scancode_run_timeout_cap_hours) without ever being
// sidelined by the 10-strike rule.
//
// A repo that hits both classes alternately (some timeouts, some
// real failures) increments both counters independently. The
// 10-strike rule fires on the failure counter only.
//
// sideline (v0.27.6) is the one carve-out from "timeouts never
// sideline": when the worker has counted
// scancode_timeout_cap_strikes CONSECUTIVE timeouts AT the
// adaptive-timeout cap, no bigger timeout is coming and the repo
// provably cannot be scanned within the operator's budget. Passing
// sideline=true additionally stamps scancode_last_run = NOW() so the
// cadence gate excludes the row — exactly the v0.21.4 10-strike
// mechanism. The June 2026 logs showed why this is needed:
// pytorch/docs and WHO/smart-html were each claimed 27× (24h at-cap
// timeout → re-claim → repeat), burning a full worker-day per cycle
// forever. scancode_timeout_attempts still increments either way —
// the diagnostic trail survives the sideline.
func (s *PostgresStore) RecordScancodeTimeout(ctx context.Context, repoID int64, sideline bool) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET scancode_locked_at = NULL,
		    scancode_locked_pid = NULL,
		    scancode_locked_boot_id = NULL,
		    scancode_output_path = NULL,
		    scancode_locked_host = NULL,
		    scancode_timeout_attempts = COALESCE(scancode_timeout_attempts, 0) + 1,
		    scancode_last_failed_at = NOW(),
		    scancode_last_run = CASE WHEN $2 THEN NOW() ELSE scancode_last_run END
		WHERE repo_id = $1`, repoID, sideline)
	return err
}

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
		    scancode_locked_host = NULL,
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
// scancode_last_run or scancode_version. Called from the dispatcher's
// ctx-canceled-after-claim cleanup and the recovery paths. The row
// stays eligible for the next claim tick without waiting for the
// stale-lock fallback window.
func (s *PostgresStore) ClearScancodeLock(ctx context.Context, repoID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET scancode_locked_at = NULL,
		    scancode_locked_pid = NULL,
		    scancode_locked_boot_id = NULL,
		    scancode_output_path = NULL,
		    scancode_locked_host = NULL
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
		    scancode_output_path = NULL,
		    scancode_locked_host = NULL
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
		       scancode_locked_at,
		       COALESCE(scancode_locked_host, '')
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
			&r.LockedPID, &r.LockedBootID, &r.OutputPath, &r.LockedAt, &r.LockedHost); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
