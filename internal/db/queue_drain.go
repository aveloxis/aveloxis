// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

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
	"log/slog"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/safego"
)

// drainLockedBy is the ONE spelling of the drain-park lock owner
// (SR-17): LockReposForDrain, ReleaseDrainLock, and HeartbeatDrainLocks
// must all match the same locked_by string, or a heartbeat/release
// silently stops finding the row it is supposed to touch.
func drainLockedBy(workerID string) string {
	return fmt.Sprintf("%s:drain", workerID)
}

// drainHeartbeatInterval matches the scheduler's 30-second job
// heartbeat (HeartbeatJob) — 1/120th of the default 1-hour
// StaleLockTimeout, so a single missed beat can never cost the lock.
const drainHeartbeatInterval = 30 * time.Second

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

	lockedBy := drainLockedBy(workerID)

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
	lockedBy := drainLockedBy(workerID)
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

// HeartbeatDrainLocks refreshes locked_at on EVERY drain-parked row
// this worker holds — the drain-lock twin of HeartbeatJob.
// v0.27.147 (round 26) introduced the heartbeat; v0.27.150 (round 29)
// widened it from one repo to the worker's whole parked SET: the
// scheduler lock-parks the entire leftover-drain set up front and
// drains it sequentially, so with a per-repo beat the WAITING repos'
// locked_at never refreshed — one long drain (production has seen
// ~33h) let RecoverStaleLocks (1-hour default) reclaim the parked
// tail, and those repos were later drained without a valid lock while
// routine collection could purge their staging. One UPDATE per beat
// covers the current repo and every waiting one. The locked_by +
// status guards make a straggler beat after release (or after a
// reclaim) a harmless 0-row UPDATE.
func (s *PostgresStore) HeartbeatDrainLocks(ctx context.Context, workerID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.collection_queue
		SET locked_at = NOW()
		WHERE locked_by = $1 AND status = 'collecting'`,
		drainLockedBy(workerID))
	return err
}

// StartDrainHeartbeat spawns the 30-second heartbeat goroutine for
// ALL of a worker's drain-parked repos and returns a stop function
// that cancels AND joins it. This is the ONE implementation both
// drain-lock holders (the scheduler's leftover-staging drain and
// `aveloxis heal-collection-gaps`) wrap their work in — a second
// inline copy of the ticker loop is the C4/L3 defect shape. Start it
// ONCE for the whole drain/heal (immediately after the first
// LockReposForDrain, covering repos parked later under the same
// workerID too); call stop() after the last ReleaseDrainLock. Beats
// on an empty set are harmless 0-row UPDATEs.
func (s *PostgresStore) StartDrainHeartbeat(ctx context.Context, logger *slog.Logger, workerID string) (stop func()) {
	hbCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer safego.Recover(logger, "drain-heartbeat")
		ticker := time.NewTicker(drainHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if err := s.HeartbeatDrainLocks(hbCtx, workerID); err != nil {
					logger.Warn("drain heartbeat failed", "worker_id", workerID, "error", err)
				}
			}
		}
	}()
	return func() {
		cancel()
		wg.Wait()
	}
}
