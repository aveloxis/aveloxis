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

// ScancodeJob is the unit of work the ScancodeWorker dispatcher
// hands to a runner goroutine.
type ScancodeJob struct {
	RepoID    int64
	RepoOwner string
	RepoName  string
	RepoGit   string
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
		    ORDER BY r.scancode_last_run NULLS FIRST, r.repo_id
		    LIMIT 1
		    FOR UPDATE SKIP LOCKED
		)
		UPDATE aveloxis_data.repos
		SET scancode_locked_at = NOW()
		WHERE repo_id IN (SELECT repo_id FROM candidate)
		RETURNING repo_id, repo_owner, repo_name, repo_git`,
		cadence.String(), ScancodeStaleLockWindow.String())

	var job ScancodeJob
	if err := row.Scan(&job.RepoID, &job.RepoOwner, &job.RepoName, &job.RepoGit); err != nil {
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
func (s *PostgresStore) MarkScancodeComplete(ctx context.Context, repoID int64, version string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET scancode_last_run = NOW(),
		    scancode_version = $2,
		    scancode_locked_at = NULL,
		    scancode_locked_pid = NULL,
		    scancode_locked_boot_id = NULL,
		    scancode_output_path = NULL
		WHERE repo_id = $1`, repoID, version)
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
