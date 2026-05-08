// Drain-park primitives for the background leftover-staging drain
// (v0.18.29 Fix 1). The scheduler uses these to atomically take a set
// of repos OUT of the worker-claim path before launching the drain
// goroutine, then release each one back to 'queued' as draining
// completes.
//
// Critical invariant: NEITHER function touches last_collected. A repo
// whose first collection was interrupted has last_collected = NULL,
// and the drain only processes pre-staged data — the original API
// fetch may have been incomplete. Setting last_collected during the
// drain would falsely mark a partially-collected repo as fully
// collected and skip the natural re-fetch on the next cycle. Only
// CompleteJob (after a successful end-to-end collection) ever sets
// last_collected.

package db

import (
	"context"
	"fmt"
)

// LockReposForDrain marks a set of repo_ids as status='collecting',
// locked_by='<workerID>:drain', locked_at=NOW() in a single UPDATE.
// Only rows currently in 'queued' status are claimed — a repo already
// being collected by another worker is left alone (returned set
// excludes it). Returns the actual list of repo_ids that were locked.
//
// The 'drain' suffix on locked_by lets operators distinguish drain
// parks from normal collection locks in the monitor, and matters for
// crash recovery: on restart, RecoverOtherWorkerLocks releases all
// locks not held by the current worker, so a drain lock from a prior
// crashed process gets cleaned up automatically (the dead worker ID
// won't match the new one).
//
// SQL deliberately mentions only queue-mechanics columns. last_collected
// is not touched. See queue_drain_lock_test.go for the source-contract
// and behavioral tests that pin this invariant.
func (s *PostgresStore) LockReposForDrain(ctx context.Context, repoIDs []int64, workerID string) ([]int64, error) {
	if len(repoIDs) == 0 {
		return nil, nil
	}

	lockedBy := fmt.Sprintf("%s:drain", workerID)

	rows, err := s.pool.Query(ctx, `
		UPDATE aveloxis_ops.collection_queue
		SET status = 'collecting',
		    locked_by = $1,
		    locked_at = NOW(),
		    updated_at = NOW()
		WHERE repo_id = ANY($2) AND status = 'queued'
		RETURNING repo_id`,
		lockedBy, repoIDs)
	if err != nil {
		return nil, fmt.Errorf("lock repos for drain: %w", err)
	}
	defer rows.Close()

	locked := make([]int64, 0, len(repoIDs))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		locked = append(locked, id)
	}
	return locked, rows.Err()
}

// ReleaseDrainLock releases a single repo back to status='queued'
// after its staging has been drained. Sets due_at=NOW() so the next
// fillWorkerSlots tick picks it up immediately for a fresh re-fetch
// (the drain processed pre-staged data; the original fetch may have
// been incomplete, so a normal collection cycle will re-fetch and
// reconcile via the existing ON CONFLICT idempotency).
//
// Like LockReposForDrain, this SQL deliberately does not touch
// last_collected. The drain has not produced a "successful collection"
// in the CompleteJob sense — only a clean end-to-end collection sets
// the timestamp.
//
// The locked_by check ensures we only release locks we actually hold;
// a repo whose drain was hijacked by a manual operator action (status
// changed externally) won't be silently overwritten.
func (s *PostgresStore) ReleaseDrainLock(ctx context.Context, repoID int64, workerID string) error {
	lockedBy := fmt.Sprintf("%s:drain", workerID)
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.collection_queue
		SET status = 'queued',
		    locked_by = NULL,
		    locked_at = NULL,
		    due_at = NOW(),
		    updated_at = NOW()
		WHERE repo_id = $1 AND locked_by = $2 AND status = 'collecting'`,
		repoID, lockedBy)
	if err != nil {
		return fmt.Errorf("release drain lock for repo %d: %w", repoID, err)
	}
	return nil
}
