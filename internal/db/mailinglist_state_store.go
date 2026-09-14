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

// MailingListWorker claim/checkpoint store (v0.25.7). The claim unit is a
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
	// LockedAt is the claim's own mlls_locked_at stamp — the release key
	// (pass 39): unique per claim in every topology, where (pid, boot_id)
	// is not (PIDs are namespaced and the boot id host-global under the
	// container deployment). Set by ClaimNextList only.
	LockedAt time.Time
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
		RETURNING r.mlls_locked_at, r.rgls_id, r.repo_group_id, COALESCE(r.rgls_email, ''),
		          COALESCE(r.mlls_system, ''), COALESCE(r.mlls_last_month, '')`,
		mailingListBackoffBaseSeconds)

	var j ListJob
	err := s.pool.QueryRow(ctx, claimSQL, system, cadence.String(), MailingListStaleLock.String(), pid, bootID).
		Scan(&j.LockedAt, &j.RglsID, &j.RepoGroupID, &j.ListAddress, &j.System, &j.LastMonth)
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

// ReleaseListLock clears a list's lock WITHOUT touching the failure
// counters or checkpoints — the worker's shutdown path (pass 37): a scan
// interrupted by `stop serve` is neither a failure (RecordListFailure
// would count it toward the sideline) nor a completion, and without a
// release the list stayed unclaimable for MailingListStaleLock (2h) after
// every restart. Ownership-qualified (pass 38/39) by the claim's OWN
// mlls_locked_at stamp (ListJob.LockedAt): a scan that outlived the
// stale window and was re-claimed elsewhere cannot clear the new
// holder's lock. The (pid, boot_id) pair was the first key and is not
// unique under the container deployment (PIDs namespaced, boot id
// host-global). (CompleteListScan / RecordListFailure predate this and
// stay unqualified — a >2h scan reclaimed mid-way can still be closed
// out by its first holder; noted in docs/architecture/mailing-list.md.)
func (s *PostgresStore) ReleaseListLock(ctx context.Context, rglsID int64, lockedAt time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repo_groups_list_serve
		SET mlls_locked_at = NULL, mlls_locked_pid = NULL, mlls_locked_boot_id = ''
		WHERE rgls_id = $1 AND mlls_locked_at = $2`, rglsID, lockedAt)
	if err != nil {
		return fmt.Errorf("release list lock: %w", err)
	}
	return nil
}

// RecoverStaleListLocks clears the lock on any list whose worker died
// mid-scan (lock older than MailingListStaleLock), making it immediately
// reclaimable rather than waiting out the claim-query's stale gate. Called
// once at worker startup. mlls_last_month is preserved so the reclaim
// resumes from the checkpoint. Returns rows recovered.
func (s *PostgresStore) RecoverStaleListLocks(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repo_groups_list_serve
		SET mlls_locked_at = NULL, mlls_locked_pid = NULL, mlls_locked_boot_id = ''
		WHERE mlls_locked_at IS NOT NULL
		  AND mlls_locked_at < NOW() - $1::interval`, MailingListStaleLock.String())
	if err != nil {
		return 0, fmt.Errorf("recover stale list locks: %w", err)
	}
	return tag.RowsAffected(), nil
}

// MailingListStats is the read-only coverage rollup surfaced by
// `aveloxis mailing-list-stats` and the REST API.
type MailingListStats struct {
	Lists            int
	ScanComplete     int
	EmailMessages    int64
	Mirrors          int64
	SignaledCaptured int64 // signaled_repo_url populated
	SignaledResolved int64 // signaled_repo_id resolved
	SenderTotal      int64 // platform_id=6 message bodies
	SenderResolved   int64 // ... with cntrb_id resolved
	ByClass          map[string]int64
}

// MailingListStats returns the coverage rollup across all mailing-list rows.
func (s *PostgresStore) MailingListStats(ctx context.Context) (MailingListStats, error) {
	var st MailingListStats
	st.ByClass = map[string]int64{}

	if err := s.pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE COALESCE(mlls_scan_complete, FALSE))
		FROM aveloxis_data.repo_groups_list_serve WHERE COALESCE(mlls_system,'') <> ''`).
		Scan(&st.Lists, &st.ScanComplete); err != nil {
		return st, fmt.Errorf("mailing-list stats (lists): %w", err)
	}

	if err := s.pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE is_mirror),
		       count(*) FILTER (WHERE signaled_repo_url <> ''),
		       count(*) FILTER (WHERE signaled_repo_id IS NOT NULL)
		FROM aveloxis_data.email_message`).
		Scan(&st.EmailMessages, &st.Mirrors, &st.SignaledCaptured, &st.SignaledResolved); err != nil {
		return st, fmt.Errorf("mailing-list stats (email_message): %w", err)
	}

	if err := s.pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE cntrb_id IS NOT NULL)
		FROM aveloxis_data.messages WHERE platform_id = 6`).
		Scan(&st.SenderTotal, &st.SenderResolved); err != nil {
		return st, fmt.Errorf("mailing-list stats (senders): %w", err)
	}

	rows, err := s.pool.Query(ctx,
		`SELECT msg_class, count(*) FROM aveloxis_data.email_message GROUP BY msg_class ORDER BY 2 DESC`)
	if err != nil {
		return st, fmt.Errorf("mailing-list stats (by class): %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var cls string
		var n int64
		if err := rows.Scan(&cls, &n); err != nil {
			return st, err
		}
		st.ByClass[cls] = n
	}
	return st, rows.Err()
}

// MailingListCoverage extends MailingListStats with the per-branch signals
// the Phase 4 `verify-mailing-list` harness gates on: which archive
// backends fired, how messages routed (bridged to issue / PR / mirror vs
// list-only), and whether threading + external-key + sender resolution
// produced output. Every field is a count; the harness reads a non-zero
// count as "this logic branch fired at least once."
type MailingListCoverage struct {
	MailingListStats
	BySystem          map[string]int64 // email_message grouped by ml_system (proves both backends ran)
	BridgedToIssue    int64            // email_message.linked_issue_id IS NOT NULL
	BridgedToPR       int64            // email_message.linked_pull_request_id IS NOT NULL
	MirrorLinked      int64            // is_mirror AND linked to a local issue/PR
	ThreadRooted      int64            // thread_root_id IS NOT NULL (threading resolved a root)
	ExternalKeyIssues int64            // issues.external_key <> '' (Jira/Bugzilla key backfill)
}

// MailingListCoverage returns the extended branch-coverage rollup.
func (s *PostgresStore) MailingListCoverage(ctx context.Context) (MailingListCoverage, error) {
	var cov MailingListCoverage
	cov.BySystem = map[string]int64{}

	base, err := s.MailingListStats(ctx)
	if err != nil {
		return cov, err
	}
	cov.MailingListStats = base

	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE linked_issue_id IS NOT NULL),
		       count(*) FILTER (WHERE linked_pull_request_id IS NOT NULL),
		       count(*) FILTER (WHERE is_mirror AND (linked_issue_id IS NOT NULL OR linked_pull_request_id IS NOT NULL)),
		       count(*) FILTER (WHERE thread_root_id IS NOT NULL)
		FROM aveloxis_data.email_message`).
		Scan(&cov.BridgedToIssue, &cov.BridgedToPR, &cov.MirrorLinked, &cov.ThreadRooted); err != nil {
		return cov, fmt.Errorf("mailing-list coverage (routing): %w", err)
	}

	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM aveloxis_data.issues WHERE external_key <> ''`).
		Scan(&cov.ExternalKeyIssues); err != nil {
		return cov, fmt.Errorf("mailing-list coverage (external_key): %w", err)
	}

	rows, err := s.pool.Query(ctx,
		`SELECT COALESCE(ml_system,''), count(*) FROM aveloxis_data.email_message GROUP BY ml_system ORDER BY 2 DESC`)
	if err != nil {
		return cov, fmt.Errorf("mailing-list coverage (by system): %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sys string
		var n int64
		if err := rows.Scan(&sys, &n); err != nil {
			return cov, err
		}
		cov.BySystem[sys] = n
	}
	return cov, rows.Err()
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
