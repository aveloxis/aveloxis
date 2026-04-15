package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// QueueJob represents a repo in the collection queue.
type QueueJob struct {
	RepoID          int64
	Priority        int
	Status          string
	DueAt           time.Time
	LockedBy        *string
	LockedAt        *time.Time
	LastCollected   *time.Time
	LastError       *string
	LastIssues      int
	LastPRs         int
	LastMessages    int
	LastEvents      int
	LastReleases    int
	LastContributors int
	LastCommits     int
	LastDurationMs  int64
	UpdatedAt       time.Time
}

// EnqueueRepo adds a repo to the collection queue or updates its priority.
// On conflict (repo already in queue), resets status to 'queued' and due_at
// to NOW() so the repo is immediately eligible for collection — UNLESS it is
// currently being collected (status='collecting'), in which case we leave it
// alone to avoid duplicate collection runs.
//
// This fixes the issue where re-enqueuing a repo (e.g., on a new installation
// sharing the same database) would leave it with a future due_at from the
// previous collection cycle, causing it to sit idle until that time passed.
func (s *PostgresStore) EnqueueRepo(ctx context.Context, repoID int64, priority int) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
			VALUES ($1, $2, 'queued', NOW())
			ON CONFLICT (repo_id) DO UPDATE SET
				priority = LEAST(collection_queue.priority, EXCLUDED.priority),
				status = CASE WHEN collection_queue.status = 'collecting' THEN collection_queue.status ELSE 'queued' END,
				due_at = CASE WHEN collection_queue.status = 'collecting' THEN collection_queue.due_at ELSE NOW() END,
				updated_at = NOW()`,
			repoID, priority)
		return err
	})
}

// PrioritizeRepo pushes a repo to priority 0 (top of queue) and makes it
// immediately due. This is the "push to top of stack" operation.
func (s *PostgresStore) PrioritizeRepo(ctx context.Context, repoID int64) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tag, err := s.pool.Exec(ctx, `
			UPDATE aveloxis_ops.collection_queue
			SET priority = 0, due_at = NOW(), status = 'queued',
				locked_by = NULL, locked_at = NULL, updated_at = NOW()
			WHERE repo_id = $1`, repoID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return errors.New("repo not found in queue")
		}
		return nil
	})
}

// DequeueNext atomically claims the highest-priority due job using
// SELECT ... FOR UPDATE SKIP LOCKED. Returns nil if no job is available.
// workerID identifies this worker for observability.
func (s *PostgresStore) DequeueNext(ctx context.Context, workerID string) (*QueueJob, error) {
	var job QueueJob
	err := s.pool.QueryRow(ctx, `
		UPDATE aveloxis_ops.collection_queue
		SET status = 'collecting', locked_by = $1, locked_at = NOW(), updated_at = NOW()
		WHERE repo_id = (
			SELECT repo_id FROM aveloxis_ops.collection_queue
			WHERE status = 'queued' AND due_at <= NOW()
			ORDER BY priority, due_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING repo_id, priority, status, due_at, locked_by, locked_at`,
		workerID,
	).Scan(&job.RepoID, &job.Priority, &job.Status, &job.DueAt, &job.LockedBy, &job.LockedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // no work available
	}
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// CompleteJob marks a job as done and re-queues it for future collection.
func (s *PostgresStore) CompleteJob(ctx context.Context, repoID int64, success bool, recollectAfter time.Duration,
	issues, prs, messages, events, releases, contributors, commits int, durationMs int64, errMsg string) error {

	status := "queued" // re-queue immediately
	var lastErr *string
	if !success {
		lastErr = &errMsg
	}

	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			UPDATE aveloxis_ops.collection_queue
			SET status = $2,
				priority = 100,
				due_at = NOW() + $3::interval,
				locked_by = NULL,
				locked_at = NULL,
				last_collected = NOW(),
				last_error = $4,
				last_issues = $5,
				last_prs = $6,
				last_messages = $7,
				last_events = $8,
				last_releases = $9,
				last_contributors = $10,
				last_commits = $11,
				last_duration_ms = $12,
				updated_at = NOW()
			WHERE repo_id = $1`,
			repoID, status, recollectAfter.String(),
			lastErr, issues, prs, messages, events, releases, contributors, commits, durationMs)
		return err
	})
}

// MakeQueuedReposDue resets due_at to NOW() for all repos in 'queued' status
// that have a future due_at. Called on scheduler startup so that repos from a
// previous collection cycle (or migrated database) become immediately eligible
// instead of sitting idle until their original due_at passes. Does NOT touch
// repos that are currently being collected (status='collecting').
func (s *PostgresStore) MakeQueuedReposDue(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.collection_queue
		SET due_at = NOW(), updated_at = NOW()
		WHERE status = 'queued' AND due_at > NOW()`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// RecoverStaleLocks resets jobs that have been locked for longer than timeout
// (e.g. a worker crashed). Called periodically by the scheduler.
func (s *PostgresStore) RecoverStaleLocks(ctx context.Context, timeout time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.collection_queue
		SET status = 'queued', locked_by = NULL, locked_at = NULL, updated_at = NOW()
		WHERE status = 'collecting' AND locked_at < NOW() - $1::interval`,
		timeout.String())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// HeartbeatJob updates locked_at to prove the worker is still alive. Called
// periodically (every 30s) by workers during long-running collection jobs.
// Without heartbeats, RecoverStaleLocks (1-hour timeout) steals active jobs
// from workers collecting large repos like kubernetes/kubernetes.
func (s *PostgresStore) HeartbeatJob(ctx context.Context, repoID int64, workerID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.collection_queue
		SET locked_at = NOW()
		WHERE repo_id = $1 AND locked_by = $2 AND status = 'collecting'`,
		repoID, workerID)
	return err
}

// RecoverOtherWorkerLocks reclaims all queue locks held by worker IDs other
// than the current one. Called on startup: a fresh process cannot have any
// legitimate in-flight work, so all locks from other worker IDs are
// definitively stale regardless of age.
//
// This fixes the bug where stopping and restarting aveloxis mid-collection
// left repos stuck in 'collecting' with a dead worker ID. The normal
// RecoverStaleLocks (1-hour timeout) wouldn't fire because the lock was
// too recent, and releaseOurLocks only matches the current worker ID.
func (s *PostgresStore) RecoverOtherWorkerLocks(ctx context.Context, currentWorkerID string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.collection_queue
		SET status = 'queued', locked_by = NULL, locked_at = NULL,
			due_at = NOW(), updated_at = NOW()
		WHERE status = 'collecting'
			AND locked_by IS NOT NULL
			AND locked_by != $1`,
		currentWorkerID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ListQueue returns all jobs in the queue, ordered by priority then due_at.
func (s *PostgresStore) ListQueue(ctx context.Context) ([]QueueJob, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT repo_id, priority, status, due_at, locked_by, locked_at,
			   last_collected, last_error,
			   last_issues, last_prs, last_messages, last_events,
			   last_releases, last_contributors, COALESCE(last_commits, 0),
			   last_duration_ms, updated_at
		FROM aveloxis_ops.collection_queue
		ORDER BY
			CASE status WHEN 'collecting' THEN 0 WHEN 'queued' THEN 1 ELSE 2 END,
			priority, due_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []QueueJob
	for rows.Next() {
		var j QueueJob
		if err := rows.Scan(
			&j.RepoID, &j.Priority, &j.Status, &j.DueAt, &j.LockedBy, &j.LockedAt,
			&j.LastCollected, &j.LastError,
			&j.LastIssues, &j.LastPRs, &j.LastMessages, &j.LastEvents,
			&j.LastReleases, &j.LastContributors, &j.LastCommits, &j.LastDurationMs, &j.UpdatedAt,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// ListQueuePage returns a paginated slice of the queue for the monitor dashboard.
// If search is non-empty, filters to repos whose owner or name contains the term.
// Results are ordered: collecting first, then queued, then by priority and due_at.
func (s *PostgresStore) ListQueuePage(ctx context.Context, limit, offset int, search string) ([]QueueJob, int, error) {
	// Build WHERE clause for search.
	whereClause := ""
	args := []interface{}{}
	argIdx := 1

	if search != "" {
		whereClause = fmt.Sprintf(` WHERE q.repo_id IN (
			SELECT repo_id FROM aveloxis_data.repos
			WHERE repo_owner ILIKE '%%' || $%d || '%%'
			   OR repo_name ILIKE '%%' || $%d || '%%')`, argIdx, argIdx)
		args = append(args, search)
		argIdx++
	}

	// Get total count for pagination.
	var total int
	countQuery := `SELECT COUNT(*) FROM aveloxis_ops.collection_queue q` + whereClause
	if err := s.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	dataQuery := fmt.Sprintf(`
		SELECT q.repo_id, q.priority, q.status, q.due_at, q.locked_by, q.locked_at,
			   q.last_collected, q.last_error,
			   q.last_issues, q.last_prs, q.last_messages, q.last_events,
			   q.last_releases, q.last_contributors, COALESCE(q.last_commits, 0),
			   q.last_duration_ms, q.updated_at
		FROM aveloxis_ops.collection_queue q
		%s
		ORDER BY
			CASE q.status WHEN 'collecting' THEN 0 WHEN 'queued' THEN 1 ELSE 2 END,
			q.priority, q.due_at
		LIMIT $%d OFFSET $%d`, whereClause, argIdx, argIdx+1)
	args = append(args, limit, offset)

	rows, err := s.pool.Query(ctx, dataQuery, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var jobs []QueueJob
	for rows.Next() {
		var j QueueJob
		if err := rows.Scan(
			&j.RepoID, &j.Priority, &j.Status, &j.DueAt, &j.LockedBy, &j.LockedAt,
			&j.LastCollected, &j.LastError,
			&j.LastIssues, &j.LastPRs, &j.LastMessages, &j.LastEvents,
			&j.LastReleases, &j.LastContributors, &j.LastCommits, &j.LastDurationMs, &j.UpdatedAt,
		); err != nil {
			return nil, 0, err
		}
		jobs = append(jobs, j)
	}
	return jobs, total, rows.Err()
}

// QueueStats returns counts by status.
func (s *PostgresStore) QueueStats(ctx context.Context) (map[string]int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT status, COUNT(*) FROM aveloxis_ops.collection_queue GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := map[string]int{"queued": 0, "collecting": 0, "total": 0}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		stats[status] = count
		stats["total"] += count
	}
	return stats, rows.Err()
}
