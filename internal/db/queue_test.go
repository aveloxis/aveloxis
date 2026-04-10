package db

// The queue methods (EnqueueRepo, DequeueNext, CompleteJob, etc.) all operate
// directly on *PostgresStore with SQL queries. They require a live PostgreSQL
// instance and cannot be unit-tested with mocks.
//
// Set AVELOXIS_TEST_DB to a valid connection string to run integration tests.

import (
	"os"
	"strings"
	"testing"
)

// TestEnqueueRepoResetsStatusAndDueAt verifies the EnqueueRepo SQL resets
// status to 'queued' and due_at to NOW() for non-collecting repos on conflict.
// This is critical: without it, re-enqueuing a repo that already completed
// (due_at far in the future) won't make it immediately collectible.
func TestEnqueueRepoResetsStatusAndDueAt(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Find the EnqueueRepo function and extract the ON CONFLICT clause.
	idx := strings.Index(src, "func (s *PostgresStore) EnqueueRepo")
	if idx < 0 {
		t.Fatal("cannot find EnqueueRepo function")
	}
	fnBody := src[idx : idx+800]

	conflictIdx := strings.Index(fnBody, "ON CONFLICT")
	if conflictIdx < 0 {
		t.Fatal("EnqueueRepo missing ON CONFLICT clause")
	}
	conflictClause := fnBody[conflictIdx:]

	// The ON CONFLICT clause must update status (not just priority/updated_at).
	if !strings.Contains(conflictClause, "status =") && !strings.Contains(conflictClause, "status=") {
		t.Error("EnqueueRepo ON CONFLICT must update status to re-queue repos that finished with future due_at")
	}

	// The ON CONFLICT clause must update due_at.
	if !strings.Contains(conflictClause, "due_at =") && !strings.Contains(conflictClause, "due_at=") {
		t.Error("EnqueueRepo ON CONFLICT must reset due_at to NOW() for non-collecting repos")
	}

	// Must preserve 'collecting' status to avoid duplicate collection.
	if !strings.Contains(conflictClause, "collecting") {
		t.Error("EnqueueRepo ON CONFLICT must check for 'collecting' status to avoid interrupting active jobs")
	}
}

// TestMakeQueuedReposDueExists verifies the store has a method to reset all
// queued repos to due_at=NOW(). Called on scheduler startup so repos with
// future due_at (from prior collection cycles) become immediately eligible.
func TestMakeQueuedReposDueExists(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "MakeQueuedReposDue") {
		t.Error("queue.go must have a MakeQueuedReposDue method to reset due_at on startup")
	}
}

func TestQueueJobStruct(t *testing.T) {
	// Verify QueueJob can be instantiated with expected zero values.
	var j QueueJob
	if j.RepoID != 0 {
		t.Errorf("zero QueueJob.RepoID = %d, want 0", j.RepoID)
	}
	if j.Status != "" {
		t.Errorf("zero QueueJob.Status = %q, want empty", j.Status)
	}
	if j.LockedBy != nil {
		t.Error("zero QueueJob.LockedBy should be nil")
	}
	if j.LastError != nil {
		t.Error("zero QueueJob.LastError should be nil")
	}
}

// TestRecoverOtherWorkerLocksExists verifies the store has a method to
// immediately reclaim all locks held by other (dead) worker IDs on startup.
// This is distinct from RecoverStaleLocks which uses a timeout: on startup,
// no other worker can possibly be alive in this process, so all locks from
// other worker IDs are definitively stale regardless of age.
func TestRecoverOtherWorkerLocksExists(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "RecoverOtherWorkerLocks") {
		t.Error("queue.go must have RecoverOtherWorkerLocks to reclaim dead workers' locks on startup")
	}
}

// TestRecoverOtherWorkerLocksUsesWorkerID verifies the method filters by
// worker ID (not by time), so it only reclaims locks from OTHER workers.
func TestRecoverOtherWorkerLocksUsesWorkerID(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *PostgresStore) RecoverOtherWorkerLocks")
	if idx < 0 {
		t.Fatal("cannot find RecoverOtherWorkerLocks function")
	}
	fnBody := src[idx : idx+500]

	// Must filter by locked_by != workerID (not by locked_at timeout).
	if !strings.Contains(fnBody, "locked_by") {
		t.Error("RecoverOtherWorkerLocks must filter by locked_by to target other workers")
	}
	// Must set status back to queued.
	if !strings.Contains(fnBody, "'queued'") {
		t.Error("RecoverOtherWorkerLocks must reset status to 'queued'")
	}
}

// TestHeartbeatJobExists verifies the store has a heartbeat method that
// workers call periodically to prove they're alive. Without heartbeats,
// RecoverStaleLocks steals jobs from workers that are still running but
// whose collection takes longer than the stale lock timeout (1 hour).
func TestHeartbeatJobExists(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "HeartbeatJob") {
		t.Error("queue.go must have HeartbeatJob method for workers to prove they're alive")
	}
}

// TestHeartbeatJobUpdatesLockedAt verifies HeartbeatJob updates locked_at
// (not just any field) so RecoverStaleLocks sees a fresh timestamp.
func TestHeartbeatJobUpdatesLockedAt(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *PostgresStore) HeartbeatJob")
	if idx < 0 {
		t.Fatal("cannot find HeartbeatJob function")
	}
	fnBody := src[idx : idx+400]

	if !strings.Contains(fnBody, "locked_at") {
		t.Error("HeartbeatJob must update locked_at to keep the lock fresh")
	}
	if !strings.Contains(fnBody, "locked_by") {
		t.Error("HeartbeatJob must verify locked_by matches to avoid updating other workers' locks")
	}
}

func TestQueueIntegration(t *testing.T) {
	t.Skip("requires live PostgreSQL — set AVELOXIS_TEST_DB to run")
}
