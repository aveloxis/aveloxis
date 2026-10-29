// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// D7 (0.29.120, summary/63): the cached answers are durable until their
// repository's state moves, so a writer that can run outside a queued
// collection job, and changes what a cached answer shows, stamps
// repos.data_changed_at with its data. Each test drives the real writer
// and reads the stamp back.

import (
	"context"
	"strings"
	"testing"
	"time"
)

func dataChangedAt(ctx context.Context, t *testing.T, store *PostgresStore, repoID int64) *time.Time {
	t.Helper()
	var at *time.Time
	if err := store.pool.QueryRow(ctx, `SELECT data_changed_at FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at
}

// RecordCommitBounds runs in the facade (a job) and in the gone-recheck
// sweep (outside one: a gone repository has no queue row), and the bounds
// are the first-activity floor of every metric chart. A change stamps in
// the same statement; an unchanged record still writes nothing
// (TestRecordCommitBoundsWritesNothingWhenUnchanged).
func TestRecordCommitBoundsStampsTheRepository(t *testing.T) {
	store, repo, commit, _ := commitBoundsFixture(t, "_avcmtstamp")
	ctx := context.Background()
	at := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	id := repo("x")
	commit(id, strings.Repeat("7", 40), &at)
	if got := dataChangedAt(ctx, t, store, id); got != nil {
		t.Fatalf("premise: a new repository has no stamp, got %v", got)
	}
	if err := store.RecordCommitBounds(ctx, id, at, at); err != nil {
		t.Fatal(err)
	}
	first := dataChangedAt(ctx, t, store, id)
	if first == nil {
		t.Fatal("recording new commit bounds must stamp data_changed_at (the charts' first-activity floor moved)")
	}
	if err := store.RecordCommitBounds(ctx, id, at, at); err != nil {
		t.Fatal(err)
	}
	if again := dataChangedAt(ctx, t, store, id); again == nil || !again.Equal(*first) {
		t.Errorf("an unchanged record must not stamp again (%v → %v): it writes nothing", first, again)
	}
}

// heal-commit-daily's fill switches the commit arms of the time series, top
// contributors and the first-activity floor to the daily table: it stamps
// in its own transaction.
func TestFillCommitDailyStampsTheRepository(t *testing.T) {
	store, ctx := v0251Connect(t)
	fx := seedCommitDaily(t, store, ctx, "_avcd_stmp1")
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET commit_daily_complete_at = NULL, data_changed_at = NULL WHERE repo_id = $1`, fx.repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.FillRepoCommitDailyFromCommits(ctx, fx.repoID); err != nil {
		t.Fatal(err)
	}
	if dataChangedAt(ctx, t, store, fx.repoID) == nil {
		t.Error("the commit-daily fill must stamp data_changed_at in its transaction")
	}
}

// A merge moves the loser's messages, commits and traffic onto the winner,
// whose queue row it does not touch: the winner is stamped in the merge's
// transaction.
func TestDedupOnePairStampsTheWinner(t *testing.T) {
	ctx, store := caseConnect(t)
	const slug = "_avdedup_stamp"
	cleanupDedupRepos(ctx, t, store, slug)
	t.Cleanup(func() { cleanupDedupRepos(ctx, t, store, slug) })
	winnerURL := "https://github.com/" + slug + "_Org/Repo"
	winnerID, loserID := seedDedupPair(ctx, t, store, slug, winnerURL, strings.ToLower(winnerURL))
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET data_changed_at = NULL WHERE repo_id = $1`, winnerID); err != nil {
		t.Fatal(err)
	}
	pair := findPairByLowerGit(ctx, t, store, strings.ToLower(winnerURL))
	if pair == nil || pair.LoserID != loserID {
		t.Fatalf("candidate query did not surface the seeded pair (got %+v)", pair)
	}
	if err := dedupOnePair(ctx, store, *pair); err != nil {
		t.Fatal(err)
	}
	if dataChangedAt(ctx, t, store, winnerID) == nil {
		t.Error("the merge must stamp the winner's data_changed_at: it gained the loser's rows without a collection")
	}
}

// The one stamp of every run outside a queued job (D7): it lands on a
// context the caller's cancel cannot stop (an interrupted run has usually
// written part of its data — the reason collect stamped this way since
// 0.29.73), covers every repository given, and is a no-op for none.
func TestStampReposChangedOutsideJob(t *testing.T) {
	ctx, store := caseConnect(t)
	const slug = "_avstamp_oob"
	cleanupDedupRepos(ctx, t, store, slug)
	t.Cleanup(func() { cleanupDedupRepos(ctx, t, store, slug) })
	a, b := seedDedupPair(ctx, t, store, slug, "https://github.com/"+slug+"_Org/A", "https://github.com/"+slug+"_org/b")
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET data_changed_at = NULL WHERE repo_id IN ($1, $2)`, a, b); err != nil {
		t.Fatal(err)
	}
	if err := store.StampReposChangedOutsideJob(ctx, nil); err != nil {
		t.Fatalf("no repositories is a no-op, got %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.StampReposChangedOutsideJob(canceled, []int64{a, b, a}); err != nil {
		t.Fatalf("a canceled caller must still stamp (the run it ends wrote data): %v", err)
	}
	for _, id := range []int64{a, b} {
		if dataChangedAt(ctx, t, store, id) == nil {
			t.Errorf("repository %d was not stamped", id)
		}
	}
}

// Round 1 finding 6 (0.29.123): a record that leaves the bounds as they were
// stamps nothing — a gone repository with no plausibly dated commits was
// rewritten, and stamped, on every gone-recheck.
func TestRecordCommitBoundsStampsOnlyAChange(t *testing.T) {
	store, repo, _, _ := commitBoundsFixture(t, "_avcmtnoop")
	ctx := context.Background()
	id := repo("x") // no commits: the bounds stay NULL
	for i := 0; i < 2; i++ {
		if err := store.RecordCommitBounds(ctx, id, time.Time{}, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	if got := dataChangedAt(ctx, t, store, id); got != nil {
		t.Errorf("bounds that stayed NULL must not stamp, got %v", got)
	}
}

// Round 1 finding 1 (0.29.123): the metadata backfill fills repos.created_at,
// the last arm of every chart's first-activity floor, outside any job: it
// stamps when it fills it, and only then.
func TestUpdateRepoMetadataStampsWhenItFillsCreatedAt(t *testing.T) {
	store, repo, _, _ := commitBoundsFixture(t, "_avmetastamp")
	ctx := context.Background()
	id := repo("x")
	if err := store.UpdateRepoMetadata(ctx, id, "d", "Go", nil, false, "", "", time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if got := dataChangedAt(ctx, t, store, id); got != nil {
		t.Fatalf("no created_at to fill: no stamp, got %v", got)
	}
	created := time.Date(2015, 3, 1, 0, 0, 0, 0, time.UTC)
	if err := store.UpdateRepoMetadata(ctx, id, "d", "Go", nil, false, "", "", created, time.Time{}); err != nil {
		t.Fatal(err)
	}
	first := dataChangedAt(ctx, t, store, id)
	if first == nil {
		t.Fatal("filling created_at must stamp data_changed_at")
	}
	if err := store.UpdateRepoMetadata(ctx, id, "d2", "Go", nil, false, "", "", created.AddDate(1, 0, 0), time.Time{}); err != nil {
		t.Fatal(err)
	}
	if again := dataChangedAt(ctx, t, store, id); again == nil || !again.Equal(*first) {
		t.Errorf("created_at is fill-empty-only: a later call must not stamp again (%v → %v)", first, again)
	}
}
