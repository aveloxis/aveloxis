// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.31 (audit Phase 3, E1) — behavioral integration coverage for
// five store methods that previously had ONLY source-contract pins
// (strings.Contains on their SQL). A pin proves the code says what we
// wrote, not that what we wrote works against the real schema — the
// exact both-sides-agree failure shape of the v0.21.0 backfill
// column-name bug. Each test here seeds real rows and asserts the
// method's observable behavior against a live Postgres.
//
// Shared-scratch-DB safety: the claim test loops claim-and-restore
// (any other eligible repo it claims gets its lock cleared before the
// test ends); ClearStaleNullPidLocks only heals rows that are already
// in the stale-inconsistent state it exists to heal.

import (
	"testing"
	"time"
)

// enqueueCollected marks the seed's repo as having completed a
// collection — ClaimNextScancodeRepo's `last_collected IS NOT NULL`
// gate requires it.
func (s *retentionSeed) enqueueCollected() {
	s.t.Helper()
	_, err := s.store.pool.Exec(s.ctx, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, status, last_collected)
		VALUES ($1, 'queued', NOW())
		ON CONFLICT (repo_id) DO UPDATE SET last_collected = NOW()`, s.repoID)
	if err != nil {
		s.t.Fatalf("seed collection_queue: %v", err)
	}
}

func TestClaimNextScancodeRepoClaimsEligibleRow(t *testing.T) {
	store, ctx := retentionConnect(t)
	t.Cleanup(store.Close)
	seed := newRetentionSeed(ctx, t, store)
	seed.enqueueCollected()

	// The claim is fleet-ordered (scancode_last_run NULLS FIRST,
	// repo_id) — on a shared scratch DB other repos may be ahead of
	// ours. Claim in a bounded loop until our row surfaces, then
	// restore every other claimed row's lock.
	var others []int64
	defer func() {
		for _, id := range others {
			_, _ = store.pool.Exec(ctx,
				`UPDATE aveloxis_data.repos SET scancode_locked_at = NULL WHERE repo_id = $1`, id)
		}
	}()
	var claimed *ScancodeJob
	for range 5000 {
		job, err := store.ClaimNextScancodeRepo(ctx, 180*24*time.Hour, 26*time.Hour)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if job == nil {
			break // queue drained without reaching our repo
		}
		if job.RepoID == seed.repoID {
			claimed = job
			break
		}
		others = append(others, job.RepoID)
	}
	if claimed == nil {
		t.Fatal("eligible seeded repo was never claimed — the claim WHERE gates exclude a row that satisfies every documented condition")
	}
	if claimed.RepoOwner != "_avret" || claimed.RepoGit == "" {
		t.Errorf("claimed job fields: owner=%q git=%q — RETURNING column order drifted", claimed.RepoOwner, claimed.RepoGit)
	}
	// The claim must have stamped the lock — a second sweep can no
	// longer claim the same row.
	var lockedAt *time.Time
	if err := store.pool.QueryRow(ctx,
		`SELECT scancode_locked_at FROM aveloxis_data.repos WHERE repo_id = $1`, seed.repoID).Scan(&lockedAt); err != nil {
		t.Fatal(err)
	}
	if lockedAt == nil {
		t.Error("claim did not stamp scancode_locked_at — two workers could scan the same repo concurrently")
	}
}

func TestRecordScancodeFailureBackoffAndSideline(t *testing.T) {
	store, ctx := retentionConnect(t)
	t.Cleanup(store.Close)
	seed := newRetentionSeed(ctx, t, store)

	readState := func() (attempts int, lastFailed, lastRun, lockedAt *time.Time) {
		t.Helper()
		err := store.pool.QueryRow(ctx, `
			SELECT COALESCE(scancode_failed_attempts, 0), scancode_last_failed_at,
			       scancode_last_run, scancode_locked_at
			FROM aveloxis_data.repos WHERE repo_id = $1`, seed.repoID).
			Scan(&attempts, &lastFailed, &lastRun, &lockedAt)
		if err != nil {
			t.Fatal(err)
		}
		return
	}

	// Simulate a held lock so the failure path's lock-clear is
	// observable.
	if _, err := store.pool.Exec(ctx,
		`UPDATE aveloxis_data.repos SET scancode_locked_at = NOW() WHERE repo_id = $1`, seed.repoID); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordScancodeFailure(ctx, seed.repoID); err != nil {
		t.Fatalf("RecordScancodeFailure: %v", err)
	}
	attempts, lastFailed, lastRun, lockedAt := readState()
	if attempts != 1 {
		t.Errorf("after 1 failure: attempts = %d, want 1", attempts)
	}
	if lastFailed == nil {
		t.Error("after 1 failure: scancode_last_failed_at not stamped — the backoff gate has nothing to gate on")
	}
	if lockedAt != nil {
		t.Error("after failure: lock not cleared — the row stays invisible to future claims")
	}
	if lastRun != nil {
		t.Error("after 1 failure: scancode_last_run stamped — sideline must fire only at the 10th strike (v0.21.4)")
	}

	// Strikes 2..10: the 10th sidelines (stamps scancode_last_run so
	// the cadence gate excludes the row).
	for i := 2; i <= 10; i++ {
		if err := store.RecordScancodeFailure(ctx, seed.repoID); err != nil {
			t.Fatalf("failure %d: %v", i, err)
		}
	}
	attempts, _, lastRun, _ = readState()
	if attempts != 10 {
		t.Errorf("after 10 failures: attempts = %d, want 10", attempts)
	}
	if lastRun == nil {
		t.Error("10th consecutive failure did not sideline (scancode_last_run still NULL) — the failing repo keeps dominating the worker pool")
	}

	// Success resets the whole failure record (v0.21.4 + v0.23.8).
	if err := store.MarkScancodeComplete(ctx, seed.repoID, "32.5.0"); err != nil {
		t.Fatalf("MarkScancodeComplete: %v", err)
	}
	attempts, lastFailed, _, _ = readState()
	if attempts != 0 || lastFailed != nil {
		t.Errorf("after success: attempts=%d lastFailed=%v — success must clear the failure history", attempts, lastFailed)
	}
}

func TestClearStaleNullPidLocksHealsOnlyStaleRows(t *testing.T) {
	store, ctx := retentionConnect(t)
	t.Cleanup(store.Close)
	stale := newRetentionSeed(ctx, t, store)
	fresh := newRetentionSeed(ctx, t, store)

	if _, err := store.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET scancode_locked_at = NOW() - INTERVAL '10 minutes', scancode_locked_pid = NULL
		WHERE repo_id = $1`, stale.repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET scancode_locked_at = NOW() - INTERVAL '1 minute', scancode_locked_pid = NULL
		WHERE repo_id = $1`, fresh.repoID); err != nil {
		t.Fatal(err)
	}

	n, err := store.ClearStaleNullPidLocks(ctx, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClearStaleNullPidLocks: %v", err)
	}
	if n < 1 {
		t.Errorf("cleared %d rows, want >= 1 (the seeded stale row)", n)
	}
	var staleLock, freshLock *time.Time
	if err := store.pool.QueryRow(ctx,
		`SELECT scancode_locked_at FROM aveloxis_data.repos WHERE repo_id = $1`, stale.repoID).Scan(&staleLock); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx,
		`SELECT scancode_locked_at FROM aveloxis_data.repos WHERE repo_id = $1`, fresh.repoID).Scan(&freshLock); err != nil {
		t.Fatal(err)
	}
	if staleLock != nil {
		t.Error("10-minute-old NULL-PID lock survived a 5-minute sweep — the in-flight recovery is dead")
	}
	if freshLock == nil {
		t.Error("1-minute-old lock was cleared by a 5-minute sweep — a legitimate in-flight scan just lost its lock")
	}
}

func TestGetRepoTransitivePackagesFoldsScopeAndExcludesDirect(t *testing.T) {
	store, ctx := retentionConnect(t)
	t.Cleanup(store.Close)
	seed := newRetentionSeed(ctx, t, store)

	insert := func(name, version, lockfile string, direct bool, scope string) {
		t.Helper()
		_, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_lockfile_packages
				(repo_id, ecosystem, package_name, resolved_version, lockfile_path, direct, dependency_scope)
			VALUES ($1, 'npm', $2, $3, $4, $5, $6)`,
			seed.repoID, name, version, lockfile, direct, scope)
		if err != nil {
			t.Fatalf("seed lockfile pkg %s: %v", name, err)
		}
	}
	insert("lodash", "4.17.21", "package-lock.json", true, "")      // direct — excluded
	insert("minimist", "1.2.0", "package-lock.json", false, "dev")  // dev in one lockfile...
	insert("minimist", "1.2.0", "sub/package-lock.json", false, "") // ...runtime in another → runtime wins
	insert("tar", "6.0.0", "package-lock.json", false, "dev")       // dev-only stays dev

	got, err := store.GetRepoTransitivePackages(ctx, seed.repoID)
	if err != nil {
		t.Fatalf("GetRepoTransitivePackages: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d transitive packages, want 2 (direct rows must be excluded, duplicates folded): %+v", len(got), got)
	}
	// ORDER BY package_name: minimist, tar.
	if got[0].PackageName != "minimist" || got[0].Scope != "" {
		t.Errorf("minimist: scope=%q, want \"\" — any non-dev observation must win the fold (runtime exposure)", got[0].Scope)
	}
	if got[1].PackageName != "tar" || got[1].Scope != "dev" {
		t.Errorf("tar: name=%q scope=%q, want dev-only package to keep scope 'dev'", got[1].PackageName, got[1].Scope)
	}
}

func TestBackfillGitLabCommitCountPatchesOnlyZeroLatestRow(t *testing.T) {
	store, ctx := retentionConnect(t)
	t.Cleanup(store.Close)
	seed := newRetentionSeed(ctx, t, store)
	carol := seed.contributor("_avgt-backfill", "User", 0)

	// Zero commits → no-op regardless of repo_info state.
	ok, err := store.BackfillGitLabCommitCount(ctx, seed.repoID)
	if err != nil {
		t.Fatalf("backfill (no commits): %v", err)
	}
	if ok {
		t.Error("backfill reported true with zero gathered commits")
	}

	// 2 distinct hashes (one commit has 2 per-file rows — DISTINCT).
	seed.commit(carol, time.Date(2025, 1, 10, 12, 0, 0, 0, time.UTC), 2)
	seed.commit(carol, time.Date(2025, 1, 11, 12, 0, 0, 0, time.UTC), 1)
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_info (repo_id, commit_count, data_collection_date)
		VALUES ($1, 0, NOW() - INTERVAL '1 hour')`, seed.repoID); err != nil {
		t.Fatalf("seed repo_info: %v", err)
	}

	ok, err = store.BackfillGitLabCommitCount(ctx, seed.repoID)
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if !ok {
		t.Fatal("backfill reported false for a zero-count latest repo_info with 2 gathered commits")
	}
	var count int64
	if err := store.pool.QueryRow(ctx, `
		SELECT commit_count FROM aveloxis_data.repo_info
		WHERE repo_id = $1 ORDER BY data_collection_date DESC LIMIT 1`, seed.repoID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("patched commit_count = %d, want 2 (COUNT(DISTINCT hash) — 3 would mean per-file row counting)", count)
	}

	// Idempotent: second call is a no-op (count no longer 0).
	ok, err = store.BackfillGitLabCommitCount(ctx, seed.repoID)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("second backfill reported true — must no-op once the count is non-zero")
	}

	// A real API count must never be overwritten: newer row says 5.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_info (repo_id, commit_count, data_collection_date)
		VALUES ($1, 5, NOW())`, seed.repoID); err != nil {
		t.Fatal(err)
	}
	ok, err = store.BackfillGitLabCommitCount(ctx, seed.repoID)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("backfill overwrote a NON-zero API count — ShouldBackfillCommitCount's never-overwrite rule violated at the SQL layer")
	}
	if err := store.pool.QueryRow(ctx, `
		SELECT commit_count FROM aveloxis_data.repo_info
		WHERE repo_id = $1 ORDER BY data_collection_date DESC LIMIT 1`, seed.repoID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Errorf("latest commit_count = %d, want the API's 5 untouched", count)
	}
}

// TestCollectedRepoIDsCohort (v0.27.32) — the heal-vulnerabilities
// --all cohort: collected + non-archived repos IN, uncollected and
// archived repos OUT. Behavioral against the real schema, per the
// ground-truth rule.
func TestCollectedRepoIDsCohort(t *testing.T) {
	store, ctx := retentionConnect(t)
	t.Cleanup(store.Close)

	collected := newRetentionSeed(ctx, t, store)
	collected.enqueueCollected()
	uncollected := newRetentionSeed(ctx, t, store)
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, status)
		VALUES ($1, 'queued') ON CONFLICT (repo_id) DO NOTHING`, uncollected.repoID); err != nil {
		t.Fatal(err)
	}
	archived := newRetentionSeed(ctx, t, store)
	archived.enqueueCollected()
	if _, err := store.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos SET repo_archived = TRUE WHERE repo_id = $1`, archived.repoID); err != nil {
		t.Fatal(err)
	}

	ids, err := store.CollectedRepoIDs(ctx)
	if err != nil {
		t.Fatalf("CollectedRepoIDs: %v", err)
	}
	in := map[int64]bool{}
	for _, id := range ids {
		in[id] = true
	}
	if !in[collected.repoID] {
		t.Error("collected non-archived repo missing from the --all cohort")
	}
	if in[uncollected.repoID] {
		t.Error("never-collected repo present — scanning it wastes OSV budget on repos with no data")
	}
	if in[archived.repoID] {
		t.Error("archived repo present — dead repos are excluded from the fleet everywhere else")
	}
}
