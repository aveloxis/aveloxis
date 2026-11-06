// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// migrateStepSQL returns the literal SQL of the migrate step labelled label,
// read from the stage function that runs it (both steps of this release are
// stage 10's): the step-order analyzer requires each step's SQL as a literal
// at its call site, so a database-tier test of one step runs the text the
// migrate runs, not a copy.
func migrateStepSQL(t *testing.T, label string) string {
	t.Helper()
	src := srctest.PackageFunc(t, "internal/db", "func migrateStage10RecentReleases(")
	re := regexp.MustCompile("(?s)" + regexp.QuoteMeta(`"`+label+`"`) + "\\s*,\\s*`([^`]*)`")
	m := re.FindStringSubmatch(src)
	if m == nil {
		t.Fatalf("migrate.go has no literal-SQL step labelled %q", label)
	}
	return m[1]
}

// The gone HISTORY (0.29.140, the bobsql incident of 2026-11-05): a repository
// re-created upstream was probed while it was still 404, marked gone and
// dequeued; the org scan listed it under its new forge ID a day later and the
// operator adopted the change, and neither brought it back. Three rules fall
// out, each pinned below: every gone episode is kept as a row of
// repo_gone_history (opened by the one marker, closed by the clearers, shown
// on the repository page), a definitive sign of life from the forge
// resurrects a gone-stamped repository (the operator's adoption of a forge-ID
// change; an org scan listing it under the forge ID the row already stores),
// and the migrate step that mirrors the forge's archived flag leaves
// gone-stamped rows alone (it un-archived 426 of them on 2026-11-05, the 469
// "stranded" repositories reconcile-repos then walked).

func seedGoneHistoryRepo(t *testing.T, store *PostgresStore, ctx context.Context, name string) int64 {
	t.Helper()
	repoID := seedRepoForDeps(t, store, ctx, "_avgonehist", name)
	t.Cleanup(func() {
		bg := context.Background()
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_data.repo_gone_history WHERE repo_id = $1`, repoID)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_data.repo_forge_id_changes WHERE repo_id = $1`, repoID)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_data.repo_info WHERE repo_id = $1`, repoID)
	})
	return repoID
}

type goneEpisodeRow struct {
	goneAt         time.Time
	status         int
	source         string
	restoredAt     *time.Time
	restoredSource string
}

func goneEpisodes(ctx context.Context, t *testing.T, store *PostgresStore, repoID int64) []goneEpisodeRow {
	t.Helper()
	rows, err := store.pool.Query(ctx, `
		SELECT gone_at, gone_status, gone_source, restored_at, restored_source
		FROM aveloxis_data.repo_gone_history WHERE repo_id = $1 ORDER BY gone_at, episode_id`, repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []goneEpisodeRow
	for rows.Next() {
		var e goneEpisodeRow
		if err := rows.Scan(&e.goneAt, &e.status, &e.source, &e.restoredAt, &e.restoredSource); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

func repoDataChangedAt(ctx context.Context, t *testing.T, store *PostgresStore, repoID int64) *time.Time {
	t.Helper()
	var at *time.Time
	if err := store.pool.QueryRow(ctx, `SELECT data_changed_at FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at
}

func hasQueueRow(ctx context.Context, t *testing.T, store *PostgresStore, repoID int64) bool {
	t.Helper()
	var n int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// MarkRepoGone opens ONE episode (a second mark while gone changes nothing in
// the history: the first observation is the episode's start) and stamps the
// cache state; ClearRepoGone closes it with the source that cleared it.
func TestMarkRepoGoneOpensAnEpisodeAndClearingClosesIt(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	repoID := seedGoneHistoryRepo(t, store, ctx, "episode")
	if before := repoDataChangedAt(ctx, t, store, repoID); before != nil {
		t.Fatalf("fixture: a fresh repository carries no cache stamp, got %v", before)
	}

	if err := store.MarkRepoGone(ctx, repoID, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	eps := goneEpisodes(ctx, t, store, repoID)
	if len(eps) != 1 {
		t.Fatalf("one open episode, got %d: %+v", len(eps), eps)
	}
	if ga := repoGoneAt(ctx, t, store, repoID); ga == nil || !eps[0].goneAt.Equal(*ga) {
		t.Errorf("the episode's gone_at is the row's stamp (%v), got %v", ga, eps[0].goneAt)
	}
	if err := store.MarkRepoGone(ctx, repoID, 410, GoneSourceMarkGoneRepos); err != nil { // a later probe while still gone
		t.Fatal(err)
	}
	eps = goneEpisodes(ctx, t, store, repoID)
	if len(eps) != 1 {
		t.Fatalf("one open episode per gone stretch, got %d: %+v", len(eps), eps)
	}
	if eps[0].status != 404 || eps[0].source != GoneSourcePrelim || eps[0].restoredAt != nil {
		t.Errorf("the episode keeps the FIRST observation (404 by prelim) and is open: %+v", eps[0])
	}
	stampAfterMark := repoDataChangedAt(ctx, t, store, repoID)
	if stampAfterMark == nil {
		t.Fatal("MarkRepoGone changes what the repository page shows (gone_at, the history): it stamps data_changed_at in the same statement")
	}

	if err := store.ClearRepoGone(ctx, repoID, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	if err := store.ClearRepoGone(ctx, repoID, GoneSourceRecheck); err != nil { // the guarded no-op
		t.Fatal(err)
	}
	eps = goneEpisodes(ctx, t, store, repoID)
	if len(eps) != 1 || eps[0].restoredAt == nil || eps[0].restoredSource != GoneSourcePrelim {
		t.Fatalf("ClearRepoGone closes the open episode with its source, and a second clear touches nothing: %+v", eps)
	}
	if after := repoDataChangedAt(ctx, t, store, repoID); after == nil || !after.After(*stampAfterMark) {
		t.Errorf("ClearRepoGone stamps data_changed_at (was %v, now %v)", stampAfterMark, after)
	}

	// A second gone stretch is a second episode.
	if err := store.MarkRepoGone(ctx, repoID, 451, GoneSourceReconcile); err != nil {
		t.Fatal(err)
	}
	eps = goneEpisodes(ctx, t, store, repoID)
	if len(eps) != 2 || eps[1].status != 451 || eps[1].source != GoneSourceReconcile || eps[1].restoredAt != nil {
		t.Fatalf("a repository gone again opens a second episode: %+v", eps)
	}
}

// ResurrectRepo closes the episode in the same statement as the clear and the
// queue upsert, and enqueues whether or not the row was gone (reconcile-repos'
// lost-queue-row arm restores scheduler visibility through it).
func TestResurrectRepoClosesTheEpisodeAndEnqueues(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	gone := seedGoneHistoryRepo(t, store, ctx, "resurrect-gone")
	never := seedGoneHistoryRepo(t, store, ctx, "resurrect-never-gone")

	if err := store.MarkRepoGone(ctx, gone, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	if err := store.ResurrectRepo(ctx, gone, 10, GoneSourceRecheck); err != nil {
		t.Fatal(err)
	}
	eps := goneEpisodes(ctx, t, store, gone)
	if len(eps) != 1 || eps[0].restoredAt == nil || eps[0].restoredSource != GoneSourceRecheck {
		t.Fatalf("ResurrectRepo closes the open episode with its source: %+v", eps)
	}
	if repoGoneAt(ctx, t, store, gone) != nil || !hasQueueRow(ctx, t, store, gone) {
		t.Error("ResurrectRepo clears the stamp and creates the queue row")
	}

	if err := store.ResurrectRepo(ctx, never, 100, GoneSourceReconcile); err != nil {
		t.Fatal(err)
	}
	if !hasQueueRow(ctx, t, store, never) {
		t.Error("a repository that was never gone is still enqueued (the lost-queue-row arm)")
	}
	if eps := goneEpisodes(ctx, t, store, never); len(eps) != 0 {
		t.Errorf("no episode is invented for a repository that was never gone: %+v", eps)
	}
}

// An org scan that lists a gone-stamped repository under the forge ID the row
// already stores is a definitive sign of life: the repository is resurrected.
// A listing under ANOTHER id is the delete-and-recreate signal, which stays a
// pending forge-ID change (observation-only) — nothing moves.
func TestResurrectIfListedResurrectsOnlyUnderTheStoredForgeID(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	repoID := seedGoneHistoryRepo(t, store, ctx, "listed")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '95336057' WHERE repo_id = $1`, repoID)
	if err := store.MarkRepoGone(ctx, repoID, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}

	resurrected, err := store.ResurrectIfListed(ctx, repoID, "1438985131", 100)
	if err != nil {
		t.Fatal(err)
	}
	if resurrected || repoGoneAt(ctx, t, store, repoID) == nil || hasQueueRow(ctx, t, store, repoID) {
		t.Fatal("a listing under a DIFFERENT forge id must not resurrect: unrelated histories may be merging on this row")
	}
	if eps := goneEpisodes(ctx, t, store, repoID); len(eps) != 1 || eps[0].restoredAt != nil {
		t.Fatalf("the episode stays open: %+v", eps)
	}

	resurrected, err = store.ResurrectIfListed(ctx, repoID, "95336057", 100)
	if err != nil {
		t.Fatal(err)
	}
	if !resurrected || repoGoneAt(ctx, t, store, repoID) != nil || !hasQueueRow(ctx, t, store, repoID) {
		t.Fatal("a listing under the stored forge id resurrects: stamp cleared, queue row created")
	}
	eps := goneEpisodes(ctx, t, store, repoID)
	if len(eps) != 1 || eps[0].restoredAt == nil || eps[0].restoredSource != GoneSourceOrgScan {
		t.Fatalf("the episode is closed by the org scan: %+v", eps)
	}
	// Idempotent, and "false" for a repository that is not gone.
	if again, err := store.ResurrectIfListed(ctx, repoID, "95336057", 100); err != nil || again {
		t.Fatalf("a second listing of a reachable repository resurrects nothing: %v %v", again, err)
	}
}

// Adopting a forge-ID change is the operator's statement that the re-created
// repository is this row's continuation — and the re-created repository
// exists, so a gone stamp on the row is stale: the adoption clears it and
// re-enqueues in its own transaction. bobsql, 2026-11-05: adopted at 15:49,
// still "no longer publicly available" and absent from the monitor until
// reconcile-repos happened to re-enqueue it.
func TestAdoptForgeIDResurrectsAGoneRepository(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	gone := seedGoneHistoryRepo(t, store, ctx, "adopt-gone")
	alive := seedGoneHistoryRepo(t, store, ctx, "adopt-alive")
	for _, id := range []int64{gone, alive} {
		mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '95336057' WHERE repo_id = $1`, id)
	}
	if err := store.MarkRepoGone(ctx, gone, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 10, 29, 21, 38, 11, 0, time.UTC)

	if err := store.AdoptForgeID(ctx, gone, "95336057", "1438985131", &created, "sgoggins", "", true); err != nil {
		t.Fatal(err)
	}
	if repoGoneAt(ctx, t, store, gone) != nil {
		t.Error("the adoption clears the gone stamp")
	}
	if !hasQueueRow(ctx, t, store, gone) {
		t.Error("the adoption re-enqueues the repository")
	}
	eps := goneEpisodes(ctx, t, store, gone)
	if len(eps) != 1 || eps[0].restoredAt == nil || eps[0].restoredSource != GoneSourceAdoption {
		t.Fatalf("the episode is closed by the adoption: %+v", eps)
	}

	// A repository that was never gone is adopted as before: no queue row
	// is invented for it (its queue state is its own).
	if err := store.AdoptForgeID(ctx, alive, "95336057", "1438985131", &created, "sgoggins", "", true); err != nil {
		t.Fatal(err)
	}
	if hasQueueRow(ctx, t, store, alive) {
		t.Error("adopting a change on a reachable repository does not touch its queue")
	}
}

// The repository page shows every episode, the open one included.
func TestRepoStatsBatchCarriesTheGoneHistory(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	repoID := seedGoneHistoryRepo(t, store, ctx, "stats")
	if err := store.MarkRepoGone(ctx, repoID, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	if err := store.ClearRepoGone(ctx, repoID, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRepoGone(ctx, repoID, 451, GoneSourceRecheck); err != nil {
		t.Fatal(err)
	}
	stats, err := store.GetRepoStatsBatch(ctx, []int64{repoID})
	if err != nil {
		t.Fatal(err)
	}
	st := stats[repoID]
	if st == nil || len(st.GoneHistory) != 2 {
		t.Fatalf("two episodes on the page, got %+v", st)
	}
	if st.GoneHistory[0].RestoredAt == nil || st.GoneHistory[0].Status != 404 {
		t.Errorf("the first episode is closed, a 404: %+v", st.GoneHistory[0])
	}
	if st.GoneHistory[1].RestoredAt != nil || st.GoneHistory[1].Status != 451 || st.GoneAt == nil || !st.GoneHistory[1].GoneAt.Equal(*st.GoneAt) {
		t.Errorf("the second episode is open and is the row's gone_at: %+v vs %v", st.GoneHistory[1], st.GoneAt)
	}
}

// The v0.27.50 migrate step mirrors the forge's archived flag from the latest
// repo_info snapshot on EVERY migrate. A gone-stamped row's snapshot predates
// its disappearance, so the step un-archived every gone repository with a
// non-archived snapshot (426 on kate, 2026-11-05) into the stranded class.
// Gone-stamped rows are left alone; the rest still follow the snapshot.
func TestArchivedReconcileStepLeavesGoneRowsArchived(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	gone := seedGoneHistoryRepo(t, store, ctx, "step-gone")
	live := seedGoneHistoryRepo(t, store, ctx, "step-live")
	for _, id := range []int64{gone, live} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_info (repo_id, status, data_collection_date) VALUES ($1, 'Active', NOW())`, id)
	}
	if err := store.MarkRepoGone(ctx, gone, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET repo_archived = TRUE WHERE repo_id = $1`, live)

	if _, err := store.pool.Exec(ctx, migrateStepSQL(t, "v0.27.50 reconcile repo_archived from forge status")); err != nil {
		t.Fatal(err)
	}
	archived := func(id int64) bool {
		t.Helper()
		var a bool
		if err := store.pool.QueryRow(ctx, `SELECT COALESCE(repo_archived, FALSE) FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&a); err != nil {
			t.Fatal(err)
		}
		return a
	}
	if !archived(gone) {
		t.Error("a gone-stamped repository stays archived whatever its (older) snapshot says")
	}
	if archived(live) {
		t.Error("a reachable repository still follows the forge's snapshot (Active → not archived)")
	}
}

// The one-shot backfill opens an episode for every gone-stamped repository
// that has none (the fleet's 1,388 at 0.29.140): once, idempotently, with the
// row's own stamp as the start.
func TestGoneEpisodeBackfillOpensOneEpisodePerGoneRepo(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	stamped := seedGoneHistoryRepo(t, store, ctx, "backfill-stamped")
	withEpisode := seedGoneHistoryRepo(t, store, ctx, "backfill-has-episode")
	never := seedGoneHistoryRepo(t, store, ctx, "backfill-never")
	// A pre-0.29.140 gone row: the stamp without the history.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET repo_archived = TRUE, repo_gone_at = NOW() - interval '30 days' WHERE repo_id = $1`, stamped)
	if err := store.MarkRepoGone(ctx, withEpisode, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	backfill := migrateStepSQL(t, backfillGoneEpisodesLabel)
	for i := 0; i < 2; i++ {
		if _, err := store.pool.Exec(ctx, backfill); err != nil {
			t.Fatal(err)
		}
	}
	eps := goneEpisodes(ctx, t, store, stamped)
	if len(eps) != 1 || eps[0].source != GoneSourceBackfill || eps[0].status != 0 || eps[0].restoredAt != nil {
		t.Fatalf("one open episode from the stamp, status unknown (0), source the backfill: %+v", eps)
	}
	if ga := repoGoneAt(ctx, t, store, stamped); ga == nil || !eps[0].goneAt.Equal(*ga) {
		t.Errorf("the episode starts at the row's stamp: %v vs %v", eps[0].goneAt, ga)
	}
	if eps := goneEpisodes(ctx, t, store, withEpisode); len(eps) != 1 || eps[0].source != GoneSourcePrelim {
		t.Errorf("a row with an open episode gets no second one: %+v", eps)
	}
	if eps := goneEpisodes(ctx, t, store, never); len(eps) != 0 {
		t.Errorf("a reachable repository gets none: %+v", eps)
	}
}

// The migrate wires both: the predicate on the v0.27.50 step, and the
// ledgered backfill (its label is registered in migration_ledger_test.go)
// writing the source the store names.
func TestGoneHistoryMigrateWiring(t *testing.T) {
	if sql := migrateStepSQL(t, "v0.27.50 reconcile repo_archived from forge status"); !strings.Contains(sql, "AND r.repo_gone_at IS NULL") {
		t.Error("the v0.27.50 step must leave gone-stamped rows alone: `AND r.repo_gone_at IS NULL`")
	}
	src := srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", "func migrateStage10RecentReleases("))
	at := strings.Index(src, `"`+backfillGoneEpisodesLabel+`"`)
	if at < 0 || !strings.Contains(src[:at], "runOnceStep(") || strings.LastIndex(src[:at], "runOnceStep(") < at-300 {
		t.Fatal("the gone-episode backfill runs as a LEDGERED step (runOnceStep) under backfillGoneEpisodesLabel")
	}
	if sql := migrateStepSQL(t, backfillGoneEpisodesLabel); !strings.Contains(sql, "'"+GoneSourceBackfill+"'") {
		t.Errorf("the backfill records GoneSourceBackfill (%q) as the episode's source", GoneSourceBackfill)
	}
}

// A merge of two case variants (dedup-repos / reconcile-repos) carries the
// loser's gone episodes to the winner — observations about the same upstream.
// The invariant "an open episode ⇔ the row's current gone state" holds across
// the merge (round 2 F1): the loser's OPEN episode is carried closed by the
// merge onto a REACHABLE winner (the winner's page would otherwise say
// "not reachable … — current" under no gone banner, and the winner's next
// mark would find the slot taken), and dropped when the winner is gone
// itself (its own open episode is its current state). The loser's rows go
// with its repos row.
func TestDedupOnePairCarriesTheLosersGoneEpisodes(t *testing.T) {
	for _, winnerOpen := range []bool{false, true} {
		t.Run(map[bool]string{false: "winner is reachable", true: "winner is gone itself"}[winnerOpen], func(t *testing.T) {
			ctx, store := caseConnect(t)
			const slug = "_avdedup_gone"
			cleanupDedupRepos(ctx, t, store, slug)
			t.Cleanup(func() { cleanupDedupRepos(ctx, t, store, slug) })
			winnerURL := "https://github.com/" + slug + "_Org/Repo"
			loserURL := strings.ToLower(winnerURL)
			winnerID, loserID := seedDedupPair(ctx, t, store, slug, winnerURL, loserURL)
			mustExecRetry(ctx, t, store, `
				INSERT INTO aveloxis_data.repo_gone_history (repo_id, gone_at, gone_status, gone_source, restored_at, restored_source, tool_version, data_collection_date)
				VALUES ($1, NOW() - interval '10 days', 404, 'prelim', NOW() - interval '9 days', 'gone-recheck', '0.29.140', NOW() - interval '10 days'),
				       ($1, NOW() - interval '2 days', 451, 'mark-gone-repos', NULL, '', '0.29.140', NOW() - interval '2 days')`, loserID)
			if winnerOpen {
				if err := store.MarkRepoGone(ctx, winnerID, 404, GoneSourcePrelim); err != nil {
					t.Fatal(err)
				}
			}
			pair := findPairByLowerGit(ctx, t, store, strings.ToLower(winnerURL))
			if pair == nil {
				t.Fatal("candidate query did not surface the seeded pair")
			}
			if err := dedupOnePair(ctx, store, *pair); err != nil {
				t.Fatalf("dedupOnePair: %v", err)
			}
			eps := goneEpisodes(ctx, t, store, winnerID)
			if len(eps) != 2 {
				t.Fatalf("the winner carries the loser's closed episode and ONE more, got %+v", eps)
			}
			if eps[0].restoredAt == nil || eps[0].status != 404 || eps[0].restoredSource != "gone-recheck" {
				t.Errorf("the closed episode is carried whole: %+v", eps[0])
			}
			if winnerOpen {
				// The winner's own open episode stands; the loser's open one is dropped.
				if eps[1].restoredAt != nil || eps[1].status != 404 || eps[1].source != GoneSourcePrelim {
					t.Errorf("a gone winner keeps its own current state: %+v", eps[1])
				}
			} else {
				// A reachable winner: the loser's open episode arrives CLOSED by
				// the merge, so no open episode contradicts the winner's state.
				if eps[1].restoredAt == nil || eps[1].status != 451 || eps[1].restoredSource != GoneSourceDedup {
					t.Errorf("a reachable winner gets the loser's open episode closed by the merge: %+v", eps[1])
				}
				if repoGoneAt(ctx, t, store, winnerID) != nil {
					t.Error("the merge never stamps the winner gone")
				}
			}
			if left := goneEpisodes(ctx, t, store, loserID); len(left) != 0 {
				t.Errorf("the loser's rows are gone with its repos row: %+v", left)
			}
			// Provenance travels with the row (round 3 F1, the forge-ID step's
			// precedent): the merge does not restamp a carried episode as its
			// own collection.
			var oldStamps int
			if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repo_gone_history
				WHERE repo_id = $1 AND gone_source IN ('prelim', 'mark-gone-repos') AND restored_source IN ('gone-recheck', $2)
				  AND tool_version = '0.29.140' AND data_collection_date < NOW() - interval '1 day'`, winnerID, GoneSourceDedup).Scan(&oldStamps); err != nil {
				t.Fatal(err)
			}
			if want := map[bool]int{false: 2, true: 1}[winnerOpen]; oldStamps != want {
				t.Errorf("carried episodes keep their tool_version and data_collection_date: %d of %d", oldStamps, want)
			}
		})
	}
}

// The org scan resurrects a listed gone repository ONCE per gone stretch
// (round 1 F1): a repository the forge lists but whose page an unauthenticated
// probe cannot see (a private repository the scanning token is a member of;
// a blocked one the listing still carries) would otherwise flap at every
// org refresh — resurrected by the scan, sidelined again by prelim, a new
// episode each time. Once an org-scan resurrection has been reversed by a
// later gone mark, the listing is no longer a sign of life for that
// repository: the recheck (or an adoption, or prelim's own 2xx) brings it
// back.
func TestResurrectIfListedDoesNotRepeatAReversedResurrection(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	repoID := seedGoneHistoryRepo(t, store, ctx, "flap")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '95336057' WHERE repo_id = $1`, repoID)
	if err := store.MarkRepoGone(ctx, repoID, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	if back, err := store.ResurrectIfListed(ctx, repoID, "95336057", 100); err != nil || !back {
		t.Fatalf("the first listing resurrects: %v %v", back, err)
	}
	// prelim probes the re-queued repository: still 404.
	if err := store.MarkRepoGone(ctx, repoID, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	if err := store.DequeueRepo(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	back, err := store.ResurrectIfListed(ctx, repoID, "95336057", 100)
	if err != nil {
		t.Fatal(err)
	}
	if back || repoGoneAt(ctx, t, store, repoID) == nil || hasQueueRow(ctx, t, store, repoID) {
		t.Fatal("a listing after a reversed org-scan resurrection does not resurrect again")
	}
	eps := goneEpisodes(ctx, t, store, repoID)
	if len(eps) != 2 || eps[0].restoredSource != GoneSourceOrgScan || eps[1].restoredAt != nil {
		t.Fatalf("one closed (by the org scan) and one open episode, no third: %+v", eps)
	}
	// The other paths still bring it back.
	if err := store.ResurrectRepo(ctx, repoID, 10, GoneSourceRecheck); err != nil {
		t.Fatal(err)
	}
	if repoGoneAt(ctx, t, store, repoID) != nil {
		t.Fatal("the recheck's resurrection is not bounded by the org scan's rule")
	}
	// Per gone STRETCH, not per repository (round 2 F2): the last restoration
	// was the recheck's, so a later stretch is the org scan's to end again.
	if err := store.MarkRepoGone(ctx, repoID, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	if err := store.DequeueRepo(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	if back, err := store.ResurrectIfListed(ctx, repoID, "95336057", 100); err != nil || !back {
		t.Fatalf("a new gone stretch after a recheck restoration is resurrected by a listing again: %v %v", back, err)
	}
	if eps := goneEpisodes(ctx, t, store, repoID); len(eps) != 3 || eps[2].restoredSource != GoneSourceOrgScan {
		t.Fatalf("three closed episodes, the last by the org scan: %+v", eps)
	}
}

// Copilot PR #252: the dedup carry reads the winner's gone state (the
// invariant "open episode ⇔ current gone state"); a clearer running beside
// the merge could close the winner's episode after that read. The winner's
// repos row is locked FOR UPDATE in the pair transaction before the carry,
// so every gone-state writer of the winner serialises with it (they all
// UPDATE that row).
func TestDedupLocksTheWinnerRowBeforeCarryingGoneEpisodes(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", "func dedupOnePair("))
	lock, carry := strings.Index(body, `"lock winner repos row"`), strings.Index(body, `"repoint repo_gone_history"`)
	if lock < 0 || carry < 0 || lock > carry {
		t.Fatalf("the winner's row lock (%d) must be a step before the gone-history carry (%d)", lock, carry)
	}
	if !strings.Contains(body[lock:carry], "lockWinnerRowSQL") || !strings.Contains(lockWinnerRowSQL, "FOR UPDATE") {
		t.Error("the lock step must take the row FOR UPDATE (lockWinnerRowSQL)")
	}
	// The dataless rename heal carries the same way, under the same lock
	// (Copilot PR #252, second review).
	heal := srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", "func (s *PostgresStore) HealRenamedDuplicate("))
	hl, hc, del := strings.Index(heal, "lockWinnerRowSQL"), strings.Index(heal, "carryGoneHistorySQL"), strings.Index(heal, `"delete dup repos row"`)
	if hl < 0 || hc < hl || del < hc {
		t.Errorf("HealRenamedDuplicate must lock the winner (%d), carry the episodes (%d) and only then delete the duplicate's repos row (%d)", hl, hc, del)
	}
}

// Prelim's sideline is ONE statement (Copilot PR #252, second review): the
// stamp, the episode and the queue row's removal. As two statements an org
// scan's resurrection between them left the repository unstamped AND
// queueless. Either order of the two writers ends consistent.
func TestSidelineRepoIsOneStatementThatOrdersWithResurrection(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	repoID := seedGoneHistoryRepo(t, store, ctx, "sideline")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '95336057' WHERE repo_id = $1`, repoID)
	if err := store.EnqueueRepo(ctx, repoID, 100); err != nil {
		t.Fatal(err)
	}
	if err := store.SidelineRepo(ctx, repoID, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	if repoGoneAt(ctx, t, store, repoID) == nil || hasQueueRow(ctx, t, store, repoID) {
		t.Fatal("sidelined: gone-stamped and queueless in one statement")
	}
	if eps := goneEpisodes(ctx, t, store, repoID); len(eps) != 1 || eps[0].status != 404 || eps[0].source != GoneSourcePrelim || eps[0].restoredAt != nil {
		t.Fatalf("the sideline opens the episode: %+v", eps)
	}
	// Sidelined, then listed: clear and queued.
	if back, err := store.ResurrectIfListed(ctx, repoID, "95336057", 100); err != nil || !back {
		t.Fatalf("resurrect after sideline: %v %v", back, err)
	}
	if repoGoneAt(ctx, t, store, repoID) != nil || !hasQueueRow(ctx, t, store, repoID) {
		t.Fatal("sidelined then resurrected: clear and queued")
	}
	// Listed, then sidelined (prelim probed the re-queued repository and
	// saw 404 last): gone and queueless — never unstamped-and-queueless.
	if err := store.SidelineRepo(ctx, repoID, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	if repoGoneAt(ctx, t, store, repoID) == nil || hasQueueRow(ctx, t, store, repoID) {
		t.Fatal("resurrected then sidelined: gone and queueless")
	}
	if eps := goneEpisodes(ctx, t, store, repoID); len(eps) != 2 || eps[0].restoredSource != GoneSourceOrgScan || eps[1].restoredAt != nil {
		t.Fatalf("two episodes, the first closed by the org scan, the second open: %+v", eps)
	}
	// The sideline on a row that was never queued is the stamp alone — no
	// error, nothing to delete.
	other := seedGoneHistoryRepo(t, store, ctx, "sideline-unqueued")
	if err := store.SidelineRepo(ctx, other, 410, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	if repoGoneAt(ctx, t, store, other) == nil {
		t.Fatal("the stamp lands without a queue row to delete")
	}
}

// Prelim's gone path is the one statement: SidelineRepo, no separate
// MarkRepoGone + DequeueRepo pair (the window the race needed).
func TestPrelimSidelinesInOneStatement(t *testing.T) {
	src := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func RunPrelim("))
	if !strings.Contains(src, "store.SidelineRepo(ctx, repo.ID, statusCode, db.GoneSourcePrelim)") {
		t.Fatal("prelim's sideline must be store.SidelineRepo (stamp + episode + dequeue in one statement)")
	}
	if strings.Contains(src, "store.MarkRepoGone(") {
		t.Error("prelim must not mark gone and dequeue in two statements — an org scan's resurrection between them strands the repository")
	}
}

// An adoption lifts a gone stamp only for a PUBLIC re-creation (Copilot PR
// #252, second review): the org scan records private listings too, and the
// admin page adopts from that record without asking the forge. A private
// observation is adopted as the continuation and stays gone; its episode
// stays open; nothing is queued.
func TestAdoptForgeIDKeepsTheStampForAPrivateReCreation(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	private := seedGoneHistoryRepo(t, store, ctx, "adopt-private")
	public := seedGoneHistoryRepo(t, store, ctx, "adopt-public")
	for _, id := range []int64{private, public} {
		mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '95336057' WHERE repo_id = $1`, id)
		if err := store.MarkRepoGone(ctx, id, 404, GoneSourcePrelim); err != nil {
			t.Fatal(err)
		}
	}
	created := time.Date(2026, 10, 29, 21, 38, 11, 0, time.UTC)
	// The org scan's observations: one private listing, one public.
	if err := store.SetPlatformRepoIDIfEmptySeen(ctx, private, "1438985131", created, true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetPlatformRepoIDIfEmptySeen(ctx, public, "1438985131", created, false); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ListForgeIDChanges(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for _, c := range pending {
		if c.RepoID == private || c.RepoID == public {
			seen[c.RepoID] = c.ObservedPrivate
		}
	}
	if !seen[private] || seen[public] {
		t.Fatalf("the recorded change carries the listing's visibility: %v", seen)
	}
	// The admin page adopts from the record.
	for _, id := range []int64{private, public} {
		if err := store.AdoptPendingForgeIDChange(ctx, id, "95336057", "1438985131", "admin", ""); err != nil {
			t.Fatal(err)
		}
	}
	var stored string
	if err := store.pool.QueryRow(ctx, `SELECT platform_repo_id FROM aveloxis_data.repos WHERE repo_id = $1`, private).Scan(&stored); err != nil || stored != "1438985131" {
		t.Fatalf("the private re-creation IS adopted as the continuation: %q %v", stored, err)
	}
	if repoGoneAt(ctx, t, store, private) == nil || hasQueueRow(ctx, t, store, private) {
		t.Error("a private re-creation stays gone and unqueued: an unauthenticated probe cannot see it")
	}
	if eps := goneEpisodes(ctx, t, store, private); len(eps) != 1 || eps[0].restoredAt != nil {
		t.Errorf("its episode stays open: %+v", eps)
	}
	if repoGoneAt(ctx, t, store, public) != nil || !hasQueueRow(ctx, t, store, public) {
		t.Error("a public re-creation is resurrected by the adoption")
	}
	// A later PUBLIC listing of the same change flips the record (the last
	// observation wins), and the CLI passes the forge's own answer.
	if err := store.SetPlatformRepoIDIfEmptySeen(ctx, private, "1438985131", created, false); err != nil {
		t.Fatal(err) // adopted already: the stored id now equals the listed one, nothing recorded; no error
	}
	direct := seedGoneHistoryRepo(t, store, ctx, "adopt-direct")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '95336057' WHERE repo_id = $1`, direct)
	if err := store.MarkRepoGone(ctx, direct, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	if err := store.AdoptForgeID(ctx, direct, "95336057", "1438985131", &created, "operator", "", false); err != nil {
		t.Fatal(err)
	}
	if repoGoneAt(ctx, t, store, direct) == nil {
		t.Error("the CLI's adoption of a private re-creation keeps the stamp")
	}
}

// The blocked ordering (the fresh review's two-session probe): a resurrection
// holds the repos row while the sideline waits on it. The sideline's dequeue
// is a second statement, so its snapshot is taken after the wait and sees the
// queue row the resurrection inserted — the repository ends gone AND
// queueless (prelim saw the 404 last), never gone-and-queued.
func TestSidelineRepoDequeuesWhatAResurrectionItWaitedOnInserted(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	repoID := seedGoneHistoryRepo(t, store, ctx, "sideline-race")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '95336057' WHERE repo_id = $1`, repoID)
	if err := store.MarkRepoGone(ctx, repoID, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err) // the pre-state: gone and queueless
	}
	holder, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(ctx) //nolint:errcheck // committed below
	if _, err := holder.Exec(ctx, resurrectSQL(listedGuardSQL), repoID, GoneSourceOrgScan, 100, false, "95336057"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- store.SidelineRepo(ctx, repoID, 404, GoneSourcePrelim) }()
	// Wait until the sideline is blocked on the row (an ungranted lock of
	// another backend on this database), then let the resurrection commit.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%marked AS%'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the sideline never blocked on the resurrection's row lock")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if repoGoneAt(ctx, t, store, repoID) == nil || hasQueueRow(ctx, t, store, repoID) {
		t.Fatal("resurrected while the sideline waited: the sideline must still end gone AND queueless")
	}
	if eps := goneEpisodes(ctx, t, store, repoID); len(eps) != 2 || eps[0].restoredSource != GoneSourceOrgScan || eps[1].restoredAt != nil {
		t.Fatalf("the org scan's closed episode, then the sideline's open one: %+v", eps)
	}
}
