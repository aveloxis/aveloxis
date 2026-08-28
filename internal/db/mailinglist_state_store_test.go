// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestMailingListStateStoreMethodsExist — source-contract: the worker
// (Phase 2 glue) depends on these by name.
func TestMailingListStateStoreMethodsExist(t *testing.T) {
	data, err := os.ReadFile("mailinglist_state_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	for _, sig := range []string{
		"func (s *PostgresStore) ClaimNextList(",
		"func (s *PostgresStore) CheckpointListMonth(",
		"func (s *PostgresStore) CompleteListScan(",
		"func (s *PostgresStore) RecordListFailure(",
		"func (s *PostgresStore) ReleaseListLock(",
	} {
		if !strings.Contains(src, sig) {
			t.Errorf("mailinglist_state_store.go must declare %s", sig)
		}
	}
	// The claim must be concurrency-safe (FOR UPDATE SKIP LOCKED) and gate
	// on cadence + stale-lock + backoff.
	for _, needle := range []string{"FOR UPDATE SKIP LOCKED", "mlls_locked_at", "make_interval"} {
		if !strings.Contains(src, needle) {
			t.Errorf("ClaimNextList must contain %q", needle)
		}
	}
}

// seedList inserts a repo_group + a registered list for a test system.
func seedList(t *testing.T, store *PostgresStore, ctx context.Context, system, email string) int64 {
	t.Helper()
	gid, err := store.UpsertRepoGroup(ctx, "ml-test-group-"+email, "mailing_list_test", "")
	if err != nil {
		t.Fatalf("seed repo_group: %v", err)
	}
	var rglsID int64
	err = store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repo_groups_list_serve (repo_group_id, rgls_email, mlls_system)
		VALUES ($1, $2, $3)
		ON CONFLICT (repo_group_id, rgls_email) DO UPDATE SET mlls_system = EXCLUDED.mlls_system
		RETURNING rgls_id`, gid, email, system).Scan(&rglsID)
	if err != nil {
		t.Fatalf("seed list: %v", err)
	}
	return rglsID
}

func TestClaimNextListLifecycle(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	const system = "mltest_lifecycle"
	email := "dev@mltest-lifecycle.example.org"
	rglsID := seedList(t, store, ctx, system, email)
	defer store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = $1`, rglsID)

	// Claim it.
	job, err := store.ClaimNextList(ctx, system, time.Hour, 1234, "boot-x")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if job == nil || job.RglsID != rglsID {
		t.Fatalf("expected to claim rgls_id %d, got %+v", rglsID, job)
	}

	// A second immediate claim finds nothing (the row is locked, not stale).
	job2, err := store.ClaimNextList(ctx, system, time.Hour, 1234, "boot-x")
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if job2 != nil {
		t.Errorf("locked list must not be re-claimable, got %+v", job2)
	}

	// Checkpoint a month, then complete the scan.
	if err := store.CheckpointListMonth(ctx, rglsID, "2026-01"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := store.CompleteListScan(ctx, rglsID, true); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// Completed + within cadence → not eligible.
	job3, err := store.ClaimNextList(ctx, system, time.Hour, 1234, "boot-x")
	if err != nil {
		t.Fatalf("third claim: %v", err)
	}
	if job3 != nil {
		t.Errorf("completed list within cadence must not be re-claimed, got %+v", job3)
	}

	// Verify the checkpoint persisted.
	var lastMonth string
	if err := store.pool.QueryRow(ctx,
		`SELECT COALESCE(mlls_last_month,'') FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = $1`, rglsID).
		Scan(&lastMonth); err != nil {
		t.Fatal(err)
	}
	if lastMonth != "2026-01" {
		t.Errorf("mlls_last_month = %q, want 2026-01", lastMonth)
	}
}

func TestRecoverStaleListLocks(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	rglsID := seedList(t, store, ctx, "mltest_stale", "dev@mltest-stale.example.org")
	defer store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id=$1`, rglsID)

	// Simulate a dead worker: lock stamped 3h ago (> 2h stale threshold).
	if _, err := store.pool.Exec(ctx,
		`UPDATE aveloxis_data.repo_groups_list_serve SET mlls_locked_at = NOW() - interval '3 hours', mlls_locked_pid = 999 WHERE rgls_id=$1`,
		rglsID); err != nil {
		t.Fatal(err)
	}
	n, err := store.RecoverStaleListLocks(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if n < 1 {
		t.Fatalf("expected to recover at least 1 stale lock, got %d", n)
	}
	var locked *time.Time
	if err := store.pool.QueryRow(ctx,
		`SELECT mlls_locked_at FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id=$1`, rglsID).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	if locked != nil {
		t.Error("stale lock should be cleared")
	}
}

func TestMailingListStatsSmoke(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	if _, err := store.MailingListStats(ctx); err != nil {
		t.Errorf("MailingListStats should not error: %v", err)
	}
}

func TestRecordListFailureBacksOff(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	const system = "mltest_failure"
	email := "dev@mltest-failure.example.org"
	rglsID := seedList(t, store, ctx, system, email)
	defer store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = $1`, rglsID)

	if _, err := store.ClaimNextList(ctx, system, time.Hour, 1, "b"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.RecordListFailure(ctx, rglsID); err != nil {
		t.Fatalf("record failure: %v", err)
	}

	var attempts int
	if err := store.pool.QueryRow(ctx,
		`SELECT mlls_failed_attempts FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = $1`, rglsID).
		Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Errorf("failed_attempts = %d, want 1", attempts)
	}

	// Immediate re-claim is blocked by the backoff gate (last_failed_at < 120s ago).
	job, err := store.ClaimNextList(ctx, system, time.Hour, 1, "b")
	if err != nil {
		t.Fatalf("re-claim: %v", err)
	}
	if job != nil {
		t.Errorf("failure backoff must block immediate re-claim, got %+v", job)
	}
}
