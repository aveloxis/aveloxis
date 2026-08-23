// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// repo_gone.go — the distinct "gone" state for repos that no longer
// resolve on their forge (v0.28.1, item 9 — the
// department-of-veterans-affairs incident: the org privatized all
// repos after collection; prelim's 404 sideline archived + dequeued
// 477 of them, and with no queue row the GUI misread the state as
// "queued for first collection" over real collected data).
//
// repos.repo_gone_at is the marker: NULL = reachable (or never
// probed since the column landed); a timestamp = prelim's probe got
// a DEFINITIVE 404/410. repo_archived alone cannot express this —
// it conflates "GitHub says archived" (still public, still cycling)
// with "unreachable" (privatized or deleted).

package db

import (
	"context"
	"time"
)

// MarkRepoGone stamps the gone state in ONE statement — both
// repo_archived (so every archived-filtered surface keeps excluding
// the row) and repo_gone_at. Single-statement on purpose: prelim's
// sideline contract (v0.27.39) is archive-must-succeed-before-
// dequeue, and splitting the two column writes would mint a partial
// state the recovery reasoning can't classify.
func (s *PostgresStore) MarkRepoGone(ctx context.Context, repoID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET repo_archived = TRUE, repo_gone_at = NOW(), data_collection_date = NOW()
		WHERE repo_id = $1`, repoID)
	return err
}

// ClearRepoGone clears a stale gone stamp. Called by the SAME probe
// that sets it (prelim's healthy path, and mark-gone-repos' 200
// branch) so resurrection is symmetric: an org that re-publicizes
// gets its repos back the moment they're probed again. The
// IS NOT NULL guard makes it a 0-row no-op for the normal fleet.
func (s *PostgresStore) ClearRepoGone(ctx context.Context, repoID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET repo_gone_at = NULL
		WHERE repo_id = $1 AND repo_gone_at IS NOT NULL`, repoID)
	return err
}

// ResurrectRepo is mark-gone-repos' 2xx recovery as ONE transaction
// (v0.28.6, Copilot round; SR-18 — the atomicity lives in the owning
// layer so a wrong caller cannot half-apply): clear the gone stamp AND
// re-enqueue for collection together. The two-statement sequence had
// no recoverable ordering — clear-then-failed-enqueue stranded the
// repo un-stamped and queueless (the rerun's !GoneStamped check never
// retried), while enqueue-then-failed-clear dropped it out of
// GetGoneProbeCandidates' queueless candidate set (only prelim's next
// probe would heal it). Atomic: either both commit or the rerun sees
// the untouched gone-stamped queueless state and retries cleanly.
// The queue upsert mirrors EnqueueRepo verbatim (collecting rows are
// left alone).
func (s *PostgresStore) ResurrectRepo(ctx context.Context, repoID int64, priority int) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, `
			UPDATE aveloxis_data.repos
			SET repo_gone_at = NULL
			WHERE repo_id = $1 AND repo_gone_at IS NOT NULL`, repoID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
			VALUES ($1, $2, 'queued', NOW())
			ON CONFLICT (repo_id) DO UPDATE SET
				priority = LEAST(collection_queue.priority, EXCLUDED.priority),
				status = CASE WHEN collection_queue.status = 'collecting' THEN collection_queue.status ELSE 'queued' END,
				due_at = CASE WHEN collection_queue.status = 'collecting' THEN collection_queue.due_at ELSE NOW() END,
				updated_at = NOW()`, repoID, priority); err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
}

// GoneProbeCandidate is one row of the mark-gone-repos work list.
type GoneProbeCandidate struct {
	RepoID      int64
	GitURL      string
	GoneStamped bool
	HasData     bool
}

// GetGoneProbeCandidates returns the repos worth probing for the
// gone state: QUEUELESS rows (prelim dequeues gone repos; genuinely
// archived repos keep their queue rows and keep cycling) that either
// carry the archived sideline flag, already carry a gone stamp
// (so a 200 can resurrect them), or hold collected commit data.
// Dataless unarchived queueless rows — the pre-v0.27.39
// stranded-row residue — fall out naturally: there is nothing to
// display for them either way (reconcile-repos territory).
func (s *PostgresStore) GetGoneProbeCandidates(ctx context.Context, limit int) ([]GoneProbeCandidate, error) {
	if limit <= 0 {
		limit = 1000000
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, r.repo_git,
		       r.repo_gone_at IS NOT NULL,
		       EXISTS (SELECT 1 FROM aveloxis_data.commits c WHERE c.repo_id = r.repo_id)
		FROM aveloxis_data.repos r
		LEFT JOIN aveloxis_ops.collection_queue q ON q.repo_id = r.repo_id
		WHERE q.repo_id IS NULL
		  AND (COALESCE(r.repo_archived, FALSE)
		       OR r.repo_gone_at IS NOT NULL
		       OR EXISTS (SELECT 1 FROM aveloxis_data.commits c2 WHERE c2.repo_id = r.repo_id))
		ORDER BY r.repo_id
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GoneProbeCandidate
	for rows.Next() {
		var c GoneProbeCandidate
		if err := rows.Scan(&c.RepoID, &c.GitURL, &c.GoneStamped, &c.HasData); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// GetRepoGoneAt returns the gone stamp; nil = reachable / never
// probed.
func (s *PostgresStore) GetRepoGoneAt(ctx context.Context, repoID int64) (*time.Time, error) {
	var ts *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT repo_gone_at FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&ts)
	if err != nil {
		return nil, err
	}
	return ts, nil
}
