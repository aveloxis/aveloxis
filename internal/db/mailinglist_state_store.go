// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// MailingListWorker claim/checkpoint store (v0.26.0). The claim unit is a
// list (repo_groups_list_serve row). Unlike the distribution worker (which
// holds a tx open for the whole scan), a per-list backfill iterates many
// months and can run long, so we use explicit lock columns (mlls_locked_*)
// released by an UPDATE — the scancode crash-recovery shape. A stale lock
// (worker died mid-scan) is reclaimable after MailingListStaleLock.
const (
	mailingListBackoffBaseSeconds = 120           // 2m → 8m → 18m → ... quadratic
	MailingListMaxFailures        = 10            // sideline after this many consecutive failures
	MailingListStaleLock          = 2 * time.Hour // a lock older than this is presumed dead
	DefaultMailingListCadence     = 30 * 24 * time.Hour
)

// ListJob is a claimed list ready to scan.
type ListJob struct {
	RglsID      int64
	RepoGroupID int64
	ListAddress string // rgls_email, e.g. dev@kafka.apache.org
	System      string // mlls_system
	LastMonth   string // mlls_last_month checkpoint ("" = never scanned)
}

// ClaimNextList atomically claims the next eligible list for the given
// system and stamps the (pid, boot_id) lock. Eligibility: never scanned, or
// a partial scan (re-eligible immediately), or cadence elapsed — and not
// currently locked (or the lock is stale), and past the failure-backoff
// gate. Returns (nil, nil) when nothing is eligible.
func (s *PostgresStore) ClaimNextList(ctx context.Context, system string, cadence time.Duration, pid int, bootID string) (*ListJob, error) {
	if cadence <= 0 {
		cadence = DefaultMailingListCadence
	}
	claimSQL := fmt.Sprintf(`
		WITH candidate AS (
		    SELECT rgls_id
		    FROM aveloxis_data.repo_groups_list_serve
		    WHERE mlls_system = $1
		      AND COALESCE(rgls_email, '') <> ''
		      AND (mlls_last_run IS NULL
		           OR COALESCE(mlls_scan_complete, FALSE) = FALSE
		           OR mlls_last_run < NOW() - $2::interval)
		      AND (mlls_locked_at IS NULL OR mlls_locked_at < NOW() - $3::interval)
		      AND (mlls_last_failed_at IS NULL
		           OR mlls_last_failed_at < NOW() - make_interval(
		               secs => %d * GREATEST(COALESCE(mlls_failed_attempts, 0), 1)
		                          * GREATEST(COALESCE(mlls_failed_attempts, 0), 1)))
		    ORDER BY COALESCE(mlls_scan_complete, FALSE) ASC, mlls_last_run NULLS FIRST, rgls_id
		    LIMIT 1
		    FOR UPDATE SKIP LOCKED
		)
		UPDATE aveloxis_data.repo_groups_list_serve r
		SET mlls_locked_at = NOW(), mlls_locked_pid = $4, mlls_locked_boot_id = $5
		WHERE r.rgls_id IN (SELECT rgls_id FROM candidate)
		RETURNING r.rgls_id, r.repo_group_id, COALESCE(r.rgls_email, ''),
		          COALESCE(r.mlls_system, ''), COALESCE(r.mlls_last_month, '')`,
		mailingListBackoffBaseSeconds)

	var j ListJob
	err := s.pool.QueryRow(ctx, claimSQL, system, cadence.String(), MailingListStaleLock.String(), pid, bootID).
		Scan(&j.RglsID, &j.RepoGroupID, &j.ListAddress, &j.System, &j.LastMonth)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("claim next list: %w", err)
	}
	return &j, nil
}

// CheckpointListMonth records the most recent fully-processed yyyy-mm for a
// list WITHOUT releasing the lock — so an interrupted scan resumes here.
func (s *PostgresStore) CheckpointListMonth(ctx context.Context, rglsID int64, yyyymm string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE aveloxis_data.repo_groups_list_serve SET mlls_last_month = $2 WHERE rgls_id = $1`,
		rglsID, yyyymm)
	if err != nil {
		return fmt.Errorf("checkpoint list month: %w", err)
	}
	return nil
}

// CompleteListScan marks a scan finished: stamps mlls_last_run (when
// scanComplete), resets the failure counter, and releases the lock.
// scanComplete=false (a partial scan that nevertheless made progress) keeps
// the row re-eligible immediately via the claim's scan_complete branch.
func (s *PostgresStore) CompleteListScan(ctx context.Context, rglsID int64, scanComplete bool) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repo_groups_list_serve
		SET mlls_scan_complete = $2,
		    mlls_last_run = CASE WHEN $2 THEN NOW() ELSE mlls_last_run END,
		    mlls_failed_attempts = 0,
		    mlls_last_failed_at = NULL,
		    mlls_locked_at = NULL, mlls_locked_pid = NULL, mlls_locked_boot_id = ''
		WHERE rgls_id = $1`, rglsID, scanComplete)
	if err != nil {
		return fmt.Errorf("complete list scan: %w", err)
	}
	return nil
}

// RecordListFailure increments the consecutive-failure counter, stamps the
// failure time (driving the quadratic backoff gate), and releases the lock.
// On the MailingListMaxFailures-th failure it also stamps mlls_last_run so
// the cadence gate sidelines the list (mirrors scancode/distribution).
// mlls_last_month is preserved so a later retry resumes from the checkpoint.
func (s *PostgresStore) RecordListFailure(ctx context.Context, rglsID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repo_groups_list_serve
		SET mlls_failed_attempts = COALESCE(mlls_failed_attempts, 0) + 1,
		    mlls_last_failed_at = NOW(),
		    mlls_last_run = CASE WHEN COALESCE(mlls_failed_attempts, 0) + 1 >= $2
		                        THEN NOW() ELSE mlls_last_run END,
		    mlls_locked_at = NULL, mlls_locked_pid = NULL, mlls_locked_boot_id = ''
		WHERE rgls_id = $1`, rglsID, MailingListMaxFailures)
	if err != nil {
		return fmt.Errorf("record list failure: %w", err)
	}
	return nil
}

// ListsForSystem returns the registered lists for a system (diagnostic).
func (s *PostgresStore) ListsForSystem(ctx context.Context, system string) ([]ListJob, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT rgls_id, repo_group_id, COALESCE(rgls_email, ''), COALESCE(mlls_system, ''), COALESCE(mlls_last_month, '')
		FROM aveloxis_data.repo_groups_list_serve
		WHERE mlls_system = $1 AND COALESCE(rgls_email, '') <> ''
		ORDER BY rgls_id`, system)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ListJob
	for rows.Next() {
		var j ListJob
		if err := rows.Scan(&j.RglsID, &j.RepoGroupID, &j.ListAddress, &j.System, &j.LastMonth); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
