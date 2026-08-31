// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.29.0 home-activity cache: the home page's 90-day activity ranking
// moves from a per-render fleet-wide aggregation (measured on the
// production fleet: mean 8.1s, max 48.2s for a 143K-repo admin scope)
// to a queue-cached column stamped per completed collection — the
// v0.19.11/v0.21.2 cumulative-counts pattern applied to the one
// remaining live aggregate on the home path.

// TestSchemaDeclaresLastActivity90d pins the column's home:
// aveloxis_ops.collection_queue, beside the other CompleteJob-cached
// counts (NOT aveloxis_data.repos — the house split puts per-cycle
// cached counts on the queue row).
func TestSchemaDeclaresLastActivity90d(t *testing.T) {
	schema := srctest.Read(t, "internal/db/schema.sql")
	i := strings.Index(schema, "CREATE TABLE IF NOT EXISTS aveloxis_ops.collection_queue")
	if i < 0 {
		t.Fatal("collection_queue DDL not found")
	}
	block := schema[i : i+strings.Index(schema[i:], ");")]
	if !strings.Contains(block, "last_activity_90d") {
		t.Fatal("collection_queue must declare last_activity_90d (the cached home ranking)")
	}
	migrate := srctest.Read(t, "internal/db/migrate.go")
	if !srctest.ContainsNormalized(migrate, `"aveloxis_ops.collection_queue", "last_activity_90d"`) {
		t.Fatal("migrate.go must addColumnIfMissing last_activity_90d for existing fleets")
	}
}

// TestCompleteJobStampsActivity90d — the writer rides the same UPDATE
// as last_issues/last_prs, using the two composite indexes v0.27.4
// added (idx_issues_repo_created / idx_pull_requests_repo_created):
// two indexed range-counts on ONE repo per completed job.
func TestCompleteJobStampsActivity90d(t *testing.T) {
	src := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/db/queue.go"),
		"func (s *PostgresStore) CompleteJob("))
	if !srctest.ContainsNormalized(src, "last_activity_90d =") {
		t.Fatal("CompleteJob must stamp last_activity_90d")
	}
	if !srctest.ContainsNormalized(src, "INTERVAL '90 days'") {
		t.Fatal("the stamp must count the trailing-90-day window")
	}
}

func homeActivityConnect(t *testing.T) (*PostgresStore, context.Context) {
	t.Helper()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)
	return store, ctx
}

func seedHomeActivityRepo(t *testing.T, ctx context.Context, store *PostgresStore, id int64, recentIssues, oldIssues, recentPRs int) {
	t.Helper()
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, $2, '_avhome', $3, 1) ON CONFLICT (repo_id) DO NOTHING`,
		id, fmt.Sprintf("https://example.com/_avhome/r%d", id), fmt.Sprintf("r%d", id))
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
		VALUES ($1, 100, 'queued', NOW()) ON CONFLICT (repo_id) DO NOTHING`, id)
	n := int64(0)
	add := func(count int, table string, age string) {
		for i := 0; i < count; i++ {
			n++
			if table == "issues" {
				mustExecRetry(ctx, t, store, `
					INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, issue_title, issue_state, created_at)
					VALUES ($1, $2, $3, 't', 'open', NOW() - $4::interval) ON CONFLICT DO NOTHING`,
					id, id*100000+n, int(n), age)
			} else {
				mustExecRetry(ctx, t, store, `
					INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number, pr_title, pr_state, created_at)
					VALUES ($1, $2, $3, 't', 'open', NOW() - $4::interval) ON CONFLICT DO NOTHING`,
					id, id*100000+n, int(n), age)
			}
		}
	}
	add(recentIssues, "issues", "1 day")
	add(oldIssues, "issues", "200 days")
	add(recentPRs, "prs", "2 days")
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.pull_requests WHERE repo_id = $1`, id)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.issues WHERE repo_id = $1`, id)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
	})
}

// TestCompleteJobActivity90dEndToEnd: 2 recent issues + 1 old + 1
// recent PR → the stamp is 3 (old activity ages out of the window).
func TestCompleteJobActivity90dEndToEnd(t *testing.T) {
	store, ctx := homeActivityConnect(t)
	const id = int64(944_150_001)
	seedHomeActivityRepo(t, ctx, store, id, 2, 1, 1)

	if err := store.CompleteJob(ctx, id, true, time.Now(), 24*time.Hour,
		0, 0, 0, 0, 0, 0, 0, 100, "", 1); err != nil {
		t.Fatal(err)
	}
	var act *int64
	if err := store.pool.QueryRow(ctx,
		`SELECT last_activity_90d FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id).Scan(&act); err != nil {
		t.Fatal(err)
	}
	if act == nil || *act != 3 {
		t.Fatalf("last_activity_90d = %v, want 3 (2 recent issues + 1 recent PR; the 200-day issue ages out)", act)
	}
}

// TestGetHomeReposReadsCachedActivity — the home query must ORDER BY
// the cached column, never re-aggregate the fleet's 90-day window per
// render (the production 8-48s cost this release removes).
func TestGetHomeReposReadsCachedActivity(t *testing.T) {
	src := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/db/home_store.go"),
		"func (s *PostgresStore) GetHomeRepos("))
	if strings.Contains(src, "INTERVAL '90") {
		t.Fatal("GetHomeRepos must not re-aggregate the 90-day window per render — read collection_queue.last_activity_90d")
	}
	if !strings.Contains(src, "last_activity_90d") {
		t.Fatal("GetHomeRepos must rank by the cached last_activity_90d")
	}
}

// TestGetHomeReposCachedOrderingEndToEnd: starred pins first, then the
// cached activity ranks; a queueless repo still renders with 0 (LEFT
// JOIN — the gone cohort must not vanish from the user's list).
func TestGetHomeReposCachedOrderingEndToEnd(t *testing.T) {
	store, ctx := homeActivityConnect(t)
	const userID = 944_151
	base := int64(944_151_000)

	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_ops.users (user_id, login_name, email)
		VALUES ($1, '_avhome_user', 'avhome@example.org') ON CONFLICT (user_id) DO NOTHING`, userID)
	var groupID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name, status)
		VALUES ($1, '_avhome_group', 'approved') RETURNING group_id`, userID).Scan(&groupID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.user_repo_stars WHERE user_id = $1`, userID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.user_repos WHERE group_id = $1`, groupID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
	})

	// Four repos: quiet (activity 5), busy (10), starred-quiet (0),
	// queueless (no queue row at all).
	type rs struct {
		id       int64
		activity *int
		queue    bool
		starred  bool
	}
	five, ten, zero := 5, 10, 0
	seeds := []rs{
		{base + 1, &five, true, false},
		{base + 2, &ten, true, false},
		{base + 3, &zero, true, true},
		{base + 4, nil, false, false},
	}
	for _, sd := range seeds {
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, $2, '_avhome', $3, 1) ON CONFLICT (repo_id) DO NOTHING`,
			sd.id, fmt.Sprintf("https://example.com/_avhome/o%d", sd.id), fmt.Sprintf("o%d", sd.id))
		if sd.queue {
			mustExecRetry(ctx, t, store, `
				INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at, last_activity_90d)
				VALUES ($1, 100, 'queued', NOW(), $2)
				ON CONFLICT (repo_id) DO UPDATE SET last_activity_90d = EXCLUDED.last_activity_90d`,
				sd.id, *sd.activity)
		}
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			groupID, sd.id)
		if sd.starred {
			mustExecRetry(ctx, t, store, `
				INSERT INTO aveloxis_ops.user_repo_stars (user_id, repo_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
				userID, sd.id)
		}
		id := sd.id
		t.Cleanup(func() {
			cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
			cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
		})
	}

	repos, err := store.GetHomeRepos(ctx, userID, 50)
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for _, r := range repos {
		if r.RepoID > base && r.RepoID <= base+10 {
			got = append(got, r.RepoID)
		}
	}
	want := []int64{base + 3, base + 2, base + 1, base + 4}
	if len(got) != 4 {
		t.Fatalf("want 4 seeded repos, got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v (starred, then cached activity desc, queueless last with 0)", got, want)
		}
	}
}

// TestHomeActivityBackfillLedgered — the one-shot pass fills existing
// fleets on the first v0.29.0 migrate so the page is fast on deploy,
// not after a full recollect cycle (the v0.21.2 backfill precedent).
func TestHomeActivityBackfillLedgered(t *testing.T) {
	migrate := srctest.Read(t, "internal/db/migrate.go")
	const label = "v0.29.0 backfill collection_queue.last_activity_90d from the 90-day window"
	if !strings.Contains(migrate, label) {
		t.Fatalf("migrate.go must carry the ledgered backfill %q", label)
	}
	// The step must gate through runOnceStep AND run the SHARED const
	// (homeActivityBackfillSQL — the behavioral test below executes the
	// same spelling, SR-17).
	region := migrate[strings.Index(migrate, label)-200:]
	if len(region) > 1200 {
		region = region[:1200]
	}
	if !strings.Contains(region, "runOnceStep") || !strings.Contains(region, "homeActivityBackfillSQL") {
		t.Error("the backfill step must be runOnceStep(... homeActivityBackfillSQL)")
	}
	for _, needle := range []string{"INTERVAL '90 days'", "last_activity_90d", "IS NULL"} {
		if !srctest.ContainsNormalized(homeActivityBackfillSQL, needle) {
			t.Errorf("homeActivityBackfillSQL must carry %q", needle)
		}
	}
	// Ordering (review 2026-08-31 #4): the step must run AFTER the two
	// composite-index builds so even a pre-v0.27.4 fleet's one-shot
	// pass is index-served.
	if strings.Index(migrate, label) < strings.Index(migrate, "idx_pull_requests_repo_created") {
		t.Error("the backfill must run after idx_issues_repo_created / idx_pull_requests_repo_created are built")
	}
}

// TestHomeActivityBackfillBehavioral (review 2026-08-31 #8b) exercises
// homeActivityBackfillSQL — the SAME const the ledgered migrate step
// runs (SR-17: one spelling, so this test exercises what the fleet
// executes, closing the v0.21.1 "source pins verify the code SAYS what
// we wrote" gap). Asserts: activity fills, zero-activity fills with 0
// (never stays NULL), a pre-stamped row is untouched, and a rerun is a
// no-op (the IS NULL resume predicate).
func TestHomeActivityBackfillBehavioral(t *testing.T) {
	store, ctx := homeActivityConnect(t)
	base := int64(944_152_000)

	// Repo 1: 2 recent issues + 1 old + 1 recent PR, column NULL → 3.
	seedHomeActivityRepo(t, ctx, store, base+1, 2, 1, 1)
	// Repo 2: no activity at all, column NULL → 0 (filled, not skipped).
	seedHomeActivityRepo(t, ctx, store, base+2, 0, 0, 0)
	// Repo 3: recent activity but PRE-STAMPED 7 → untouched.
	seedHomeActivityRepo(t, ctx, store, base+3, 4, 0, 0)
	mustExecRetry(ctx, t, store, `
		UPDATE aveloxis_ops.collection_queue SET last_activity_90d = 7 WHERE repo_id = $1`, base+3)

	if _, err := store.pool.Exec(ctx, homeActivityBackfillSQL); err != nil {
		t.Fatal(err)
	}
	read := func(id int64) *int64 {
		var v *int64
		if err := store.pool.QueryRow(ctx,
			`SELECT last_activity_90d FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	if v := read(base + 1); v == nil || *v != 3 {
		t.Fatalf("active repo backfill = %v, want 3", v)
	}
	if v := read(base + 2); v == nil || *v != 0 {
		t.Fatalf("zero-activity repo must fill with 0, got %v", v)
	}
	if v := read(base + 3); v == nil || *v != 7 {
		t.Fatalf("pre-stamped repo must be untouched, got %v", v)
	}
	// Rerun: the IS NULL predicate makes it a no-op.
	tag, err := store.pool.Exec(ctx, homeActivityBackfillSQL)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 0 {
		t.Fatalf("rerun touched %d rows, want 0", tag.RowsAffected())
	}
}

// TestRefreshQueueGatheredCountsIncludesActivity (review 2026-08-31
// #5): the gap healer's refresh is CompleteJob's sibling writer — a
// healed repo's healed items are exactly the RECENT cohort, so leaving
// the home ranking stale there defeats the heal's visibility.
func TestRefreshQueueGatheredCountsIncludesActivity(t *testing.T) {
	store, ctx := homeActivityConnect(t)
	const id = int64(944_153_001)
	seedHomeActivityRepo(t, ctx, store, id, 2, 1, 0)

	if err := store.RefreshQueueGatheredCounts(ctx, id); err != nil {
		t.Fatal(err)
	}
	var act *int64
	if err := store.pool.QueryRow(ctx,
		`SELECT last_activity_90d FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id).Scan(&act); err != nil {
		t.Fatal(err)
	}
	if act == nil || *act != 2 {
		t.Fatalf("healer refresh must stamp last_activity_90d = 2 (recent issues only), got %v", act)
	}
}
