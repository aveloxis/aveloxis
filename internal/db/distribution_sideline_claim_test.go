// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// TestSidelinedPartialScanIsNotReclaimedImmediately pins worklist item 22:
// under the default immediate_partial_reclaim, the claim's
// `OR distribution_scan_complete = FALSE` branch bypassed the cadence gate
// that the 10-strike sideline sets (distribution_last_run = NOW() at the
// tenth failure), so a sidelined partial scan came back every time its
// quadratic backoff elapsed (200 minutes at ten strikes) — forever. The
// branch now applies only below the strike limit; a repository under it
// (nine strikes) is still reclaimed at once.
func TestSidelinedPartialScanIsNotReclaimedImmediately(t *testing.T) {
	store, ctx, repoID := newSidelineFixture(t)
	// Sidelined: ten strikes, the backoff long elapsed, last_run stamped by
	// the tenth failure (the cadence gate), the scan partial.
	setStrikes := func(n int) {
		mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET distribution_scan_complete = FALSE,
			distribution_failed_attempts = $2, distribution_last_failed_at = NOW() - interval '2 days',
			distribution_last_run = NOW() WHERE repo_id = $1`, repoID, n)
	}
	setStrikes(DistributionMaxFailures)
	if job := claimMine(t, store, ctx, repoID); job != nil {
		_ = store.ReleaseDistributionClaim(ctx, job)
		t.Error("a sidelined partial scan (ten strikes) was claimed at once; the sideline's cadence gate must hold it")
	}
	setStrikes(DistributionMaxFailures - 1)
	if job := claimMine(t, store, ctx, repoID); job == nil {
		t.Error("a partial scan under the strike limit was not reclaimed; immediate partial reclaim must still apply below it")
	} else {
		_ = store.ReleaseDistributionClaim(ctx, job)
	}
}

// TestTenStrikesSidelineAPartialScan drives the sideline through
// RecordDistributionFailure itself (review round 1: the claim's `< 10` and
// the recorder's `+ 1 >= 10` are complementary only by a shared constant;
// a recorder that stopped stamping last_run at the tenth strike would pass
// every hand-built fixture and re-open the item-22 loop). The row's last
// scan is older than the cadence, as a repository stuck in the loop has:
// nine strikes leave it claimable as soon as its backoff elapses, the tenth
// stamps last_run and the cadence holds it.
func TestTenStrikesSidelineAPartialScan(t *testing.T) {
	store, ctx, repoID := newSidelineFixture(t)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET distribution_scan_complete = FALSE,
		distribution_failed_attempts = 0, distribution_last_failed_at = NULL,
		distribution_last_run = NOW() - interval '400 days' WHERE repo_id = $1`, repoID)
	lastRunStamped := func() bool {
		t.Helper()
		var recent bool
		if err := store.pool.QueryRow(ctx, `SELECT distribution_last_run > NOW() - interval '1 day' FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&recent); err != nil {
			t.Fatal(err)
		}
		return recent
	}
	for strike := 1; strike <= DistributionMaxFailures; strike++ {
		job := claimMine(t, store, ctx, repoID)
		if job == nil {
			t.Fatalf("before strike %d the partial scan was not claimable", strike)
		}
		if err := store.RecordDistributionFailure(ctx, job); err != nil {
			t.Fatalf("strike %d: %v", strike, err)
		}
		// The sideline stamp is the TENTH strike's, not an earlier one's
		// (review round 2: a recorder stamping at nine passed the claim
		// checks alone, since a partial row under the limit stays claimable
		// through the partial branch).
		if stamped := lastRunStamped(); stamped != (strike == DistributionMaxFailures) {
			t.Errorf("after strike %d distribution_last_run stamped = %v; want the stamp at strike %d only", strike, stamped, DistributionMaxFailures)
		}
		// The backoff elapses.
		mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET distribution_last_failed_at = NOW() - interval '2 days' WHERE repo_id = $1`, repoID)
	}
	if job := claimMine(t, store, ctx, repoID); job != nil {
		_ = store.ReleaseDistributionClaim(ctx, job)
		t.Errorf("after %d strikes recorded by RecordDistributionFailure the partial scan was claimed again; the tenth strike must stamp last_run and the cadence must hold it", DistributionMaxFailures)
	}
}

// newSidelineFixture is a collected GitHub repository with a queue row (the
// claim requires last_collected), removed after the test.
func newSidelineFixture(t *testing.T) (*PostgresStore, context.Context, int64) {
	t.Helper()
	store := openTestStore(t)
	ctx := context.Background()
	name := "sideline-" + fmt.Sprint(time.Now().UnixNano())
	repoID, err := store.UpsertRepo(ctx, &model.Repo{Owner: "_avsideline", Name: name, GitURL: "https://github.com/_avsideline/" + name, Platform: model.PlatformGitHub})
	if err != nil {
		t.Fatal(err)
	}
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.collection_queue (repo_id, status, due_at, last_collected)
		VALUES ($1, 'queued', NOW() + interval '7 days', NOW() - interval '1 day')
		ON CONFLICT (repo_id) DO UPDATE SET last_collected = EXCLUDED.last_collected`, repoID)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	return store, ctx, repoID
}

// claimMine claims until it holds repoID's job (returned, unreleased) or
// the queue has nothing eligible (nil). Another test's or package's row is
// released and held out of the way with a fresh failure stamp.
func claimMine(t *testing.T, store *PostgresStore, ctx context.Context, repoID int64) *DistributionJob {
	t.Helper()
	for {
		job, err := store.ClaimNextDistributionRepo(ctx, 180*24*time.Hour, true)
		if err != nil {
			t.Fatal(err)
		}
		if job == nil {
			return nil
		}
		if job.RepoID == repoID {
			return job
		}
		_ = store.ReleaseDistributionClaim(ctx, job)
		mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET distribution_last_failed_at = NOW() WHERE repo_id = $1`, job.RepoID)
	}
}
