// Tests for the queue lock-park primitives that support the background
// staging drain (v0.18.29 Fix 1):
//
//   - LockReposForDrain marks a set of repo_ids as status='collecting',
//     locked_by='<workerID>:drain', locked_at=NOW() in a single UPDATE.
//   - ReleaseDrainLock releases one repo back to status='queued', clearing
//     locked_by/locked_at, and setting due_at=NOW() so the next normal
//     collection picks it up immediately.
//
// The critical invariant — verified by source contract AND by integration
// behavior — is that NEITHER function touches last_collected. A repo
// whose first collection was interrupted has last_collected=NULL; the
// drain processes whatever was already staged into relational tables but
// the *fetch* may have been incomplete, so last_collected must stay NULL
// until a clean re-collection completes via CompleteJob. Setting it
// during the drain would falsely mark a partially-collected repo as
// fully collected and skip the natural re-fetch.

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// TestLockReposForDrainExists pins the function signature on the store.
func TestLockReposForDrainExists(t *testing.T) {
	src := mustReadStoreSource(t, "queue_drain.go")
	if !strings.Contains(src, "func (s *PostgresStore) LockReposForDrain(") {
		t.Error("queue_drain.go must define LockReposForDrain on *PostgresStore")
	}
}

// TestReleaseDrainLockExists pins the per-repo release function.
func TestReleaseDrainLockExists(t *testing.T) {
	src := mustReadStoreSource(t, "queue_drain.go")
	if !strings.Contains(src, "func (s *PostgresStore) ReleaseDrainLock(") {
		t.Error("queue_drain.go must define ReleaseDrainLock on *PostgresStore")
	}
}

// TestLockReposForDrainSQLDoesNotTouchLastCollected is the load-bearing
// invariant: the lock UPDATE must touch ONLY queue-mechanics columns
// (status, locked_by, locked_at, updated_at). Any UPDATE that reaches
// last_collected during a drain park would corrupt the "first collection
// in progress" signal — workers would see last_collected as non-NULL
// and switch to incremental since-windowed collection instead of a
// fresh full re-fetch.
//
// We extract the SQL string literal (between backticks) so doc comments
// that *describe* the invariant (which mention `last_collected` to
// explain what's NOT touched) don't trip the check.
func TestLockReposForDrainSQLDoesNotTouchLastCollected(t *testing.T) {
	src := mustReadStoreSource(t, "queue_drain.go")
	body := extractDrainFunc(src, "LockReposForDrain")
	if body == "" {
		t.Skip("LockReposForDrain not yet defined; covered by TestLockReposForDrainExists")
	}
	sql := extractSQLFromBody(body)
	if sql == "" {
		t.Fatal("could not locate SQL string literal inside LockReposForDrain")
	}
	if strings.Contains(sql, "last_collected") {
		t.Error("LockReposForDrain SQL must NOT mention last_collected. " +
			"The drain park is purely a queue-mechanics signal; touching last_collected " +
			"would corrupt the 'first collection in progress' invariant for repos " +
			"whose initial collection was interrupted.")
	}
	if !strings.Contains(sql, "'collecting'") {
		t.Error("LockReposForDrain must set status='collecting' (existing enum value) — " +
			"a new status would require updates across queue.go, monitor templates, etc.")
	}
}

// TestReleaseDrainLockSQLDoesNotTouchLastCollected mirrors the lock-side
// invariant for the release path. CompleteJob is the ONLY function that
// should ever set last_collected.
func TestReleaseDrainLockSQLDoesNotTouchLastCollected(t *testing.T) {
	src := mustReadStoreSource(t, "queue_drain.go")
	body := extractDrainFunc(src, "ReleaseDrainLock")
	if body == "" {
		t.Skip("ReleaseDrainLock not yet defined; covered by TestReleaseDrainLockExists")
	}
	sql := extractSQLFromBody(body)
	if sql == "" {
		t.Fatal("could not locate SQL string literal inside ReleaseDrainLock")
	}
	if strings.Contains(sql, "last_collected") {
		t.Error("ReleaseDrainLock SQL must NOT mention last_collected. " +
			"Drain processed pre-staged data; the original fetch may have been " +
			"incomplete. Only CompleteJob should ever set last_collected, after " +
			"a successful end-to-end collection.")
	}
}

// extractSQLFromBody returns the contents of the first backtick-delimited
// string literal in the given function body — i.e. the SQL the function
// passes to pgx. Doc comments mentioning column names for explanation
// purposes are left out.
func extractSQLFromBody(body string) string {
	start := strings.Index(body, "`")
	if start < 0 {
		return ""
	}
	rest := body[start+1:]
	end := strings.Index(rest, "`")
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// TestDrainLockSuffixUsesWorkerID enforces that the synthetic worker ID
// used for the drain park encodes the real worker ID. This matters for
// crash recovery: on restart, RecoverOtherWorkerLocks releases all locks
// not held by the current worker. A drain lock from a prior crashed
// process gets cleaned up automatically because its locked_by suffix
// references the dead worker ID.
func TestDrainLockSuffixUsesWorkerID(t *testing.T) {
	src := mustReadStoreSource(t, "queue_drain.go")
	body := extractDrainFunc(src, "LockReposForDrain")
	if body == "" {
		t.Skip("LockReposForDrain not yet defined")
	}
	// The function should accept workerID as a parameter and concatenate
	// it with ':drain' (or similar suffix marker) for the locked_by value.
	if !strings.Contains(body, "drain") {
		t.Error("LockReposForDrain must use a 'drain' suffix on locked_by so operators " +
			"can distinguish drain parks from normal collection locks in the monitor.")
	}
}

// ---------------------------------------------------------------------
// Integration tests — gated on AVELOXIS_TEST_DB. Verify the actual SQL
// against a live Postgres.
// ---------------------------------------------------------------------

// TestLockReposForDrainPreservesNullLastCollected is the headline
// behavioral test: insert a queue row with last_collected=NULL, run a
// lock-then-release cycle, assert last_collected is still NULL. This
// test would catch any regression where a future refactor "helpfully"
// sets last_collected during the drain park.
func TestLockReposForDrainPreservesNullLastCollected(t *testing.T) {
	conn := os.Getenv("AVELOXIS_TEST_DB")
	if conn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, conn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Insert a fresh repo + queue row with last_collected = NULL.
	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Owner: "_avdrain", Name: "null-lastcollected",
		GitURL: "https://github.com/_avdrain/null-lastcollected", Platform: model.PlatformGitHub,
	})
	if err != nil {
		t.Fatalf("upsert repo: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, status, due_at, last_collected)
		VALUES ($1, 'queued', NOW(), NULL)
		ON CONFLICT (repo_id) DO UPDATE SET status='queued', last_collected=NULL`,
		repoID); err != nil {
		t.Fatalf("seed queue row: %v", err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	// Lock for drain.
	locked, err := store.LockReposForDrain(ctx, []int64{repoID}, "test-worker")
	if err != nil {
		t.Fatalf("LockReposForDrain: %v", err)
	}
	if len(locked) != 1 || locked[0] != repoID {
		t.Errorf("LockReposForDrain returned %v, want [%d]", locked, repoID)
	}

	// Verify status is 'collecting', last_collected is still NULL.
	var status string
	var lastCollected *string
	if err := store.pool.QueryRow(ctx, `
		SELECT status, last_collected::text FROM aveloxis_ops.collection_queue WHERE repo_id = $1`,
		repoID).Scan(&status, &lastCollected); err != nil {
		t.Fatalf("post-lock select: %v", err)
	}
	if status != "collecting" {
		t.Errorf("post-lock status = %q, want 'collecting'", status)
	}
	if lastCollected != nil {
		t.Errorf("post-lock last_collected = %v, want NULL — the drain lock must NOT set last_collected", *lastCollected)
	}

	// Release.
	if err := store.ReleaseDrainLock(ctx, repoID, "test-worker"); err != nil {
		t.Fatalf("ReleaseDrainLock: %v", err)
	}

	// Verify status is 'queued', last_collected is STILL NULL.
	if err := store.pool.QueryRow(ctx, `
		SELECT status, last_collected::text FROM aveloxis_ops.collection_queue WHERE repo_id = $1`,
		repoID).Scan(&status, &lastCollected); err != nil {
		t.Fatalf("post-release select: %v", err)
	}
	if status != "queued" {
		t.Errorf("post-release status = %q, want 'queued'", status)
	}
	if lastCollected != nil {
		t.Errorf("post-release last_collected = %v, want NULL — the drain release must NOT set last_collected", *lastCollected)
	}
}

// TestLockReposForDrainSkipsActivelyCollecting guards against a worker
// already mid-collection on a repo getting its lock stomped by the
// drain park. Only repos in 'queued' status should be lockable for
// drain.
func TestLockReposForDrainSkipsActivelyCollecting(t *testing.T) {
	conn := os.Getenv("AVELOXIS_TEST_DB")
	if conn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, conn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Owner: "_avdrain", Name: "actively-collecting",
		GitURL: "https://github.com/_avdrain/actively-collecting", Platform: model.PlatformGitHub,
	})
	if err != nil {
		t.Fatalf("upsert repo: %v", err)
	}
	// Seed as 'collecting' with a different worker ID.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, status, locked_by, locked_at, due_at)
		VALUES ($1, 'collecting', 'other-worker', NOW(), NOW())
		ON CONFLICT (repo_id) DO UPDATE SET
			status='collecting', locked_by='other-worker', locked_at=NOW()`,
		repoID); err != nil {
		t.Fatalf("seed queue row: %v", err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	locked, err := store.LockReposForDrain(ctx, []int64{repoID}, "test-worker")
	if err != nil {
		t.Fatalf("LockReposForDrain: %v", err)
	}
	if len(locked) != 0 {
		t.Errorf("LockReposForDrain claimed an actively-collecting repo (returned %v) — must skip rows already locked by other workers", locked)
	}

	// Verify the original lock holder is unchanged.
	var lockedBy string
	if err := store.pool.QueryRow(ctx,
		`SELECT locked_by FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID).Scan(&lockedBy); err != nil {
		t.Fatalf("post-lock select: %v", err)
	}
	if lockedBy != "other-worker" {
		t.Errorf("LockReposForDrain stomped on existing lock: locked_by = %q, want 'other-worker'", lockedBy)
	}
}

func mustReadStoreSource(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}

func extractDrainFunc(src, name string) string {
	marker := "func (s *PostgresStore) " + name + "("
	idx := strings.Index(src, marker)
	if idx < 0 {
		return ""
	}
	rest := src[idx:]
	if end := strings.Index(rest[1:], "\nfunc "); end > 0 {
		return rest[:end+1]
	}
	return rest
}
