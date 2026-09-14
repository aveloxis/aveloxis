// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// gap_heal_test.go — v0.27.140: the isolated collection-gap healer.

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

// The routine path keeps its 5% threshold via delegation; the healer
// is the ONLY caller passing 0. A refactor collapsing the seam would
// either put threshold-0 into routine collection (full listings every
// cycle) or 5% into the healer (small gaps never heal) — both wrong.
func TestGapFillThresholdSeam(t *testing.T) {
	s := srctest.Read(t, "internal/collector/gap_fill.go")
	if !strings.Contains(s, "return gf.AssessAndFillGapsWithThreshold(ctx, repoID, owner, repo, metaIssues, metaPRs, GapThreshold)") {
		t.Error("AssessAndFillGaps must delegate with GapThreshold — routine collection keeps the 5% gate")
	}
	if !strings.Contains(s, "func gapExceedsThreshold(gathered, metadata int64, threshold float64) bool") {
		t.Error("gapExceedsThreshold must take the threshold as a parameter")
	}
	h := srctest.Read(t, "cmd/aveloxis/heal_collection_gaps.go")
	if !strings.Contains(h, "AssessAndFillGapsWithThreshold(ctx, c.RepoID, c.Owner, c.Name, c.MetaIssues, c.MetaPRs, threshold)") {
		t.Error("the healer must pass its derived threshold (0 = any gap; GapForceList for the completeness modes)")
	}
	// Round-23: --all/--repo-id run FORCE-LIST — threshold 0 requires
	// metadata > gathered and cannot see count-netting.
	if !strings.Contains(h, "threshold = collector.GapForceList") {
		t.Error("--all and --repo-id must switch to collector.GapForceList")
	}
	if !strings.Contains(h, "RefreshQueueGatheredCounts(ctx, c.RepoID)") {
		t.Error("a successful heal must refresh the queue's cached counts — otherwise healed repos never leave the candidate set and rerun-until-0 never converges")
	}
}

func TestHealCollectionGapsCommandContract(t *testing.T) {
	h := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/heal_collection_gaps.go"))
	if strings.Contains(h, "store.Migrate(") {
		t.Error("heal-collection-gaps must not migrate (v0.21.5 — only serve and migrate do)")
	}
	for _, needle := range []string{
		"LockReposForDrain(", // serve-safety: the v0.18.29 drain lock per repo
		"ReleaseDrainLock(",  // ...and its release on every heal path
		"GetGapHealCandidates(",
		`"dry-run"`, `"limit"`, `"workers"`, `"repo-id"`, `"after-repo-id"`, `"all"`,
	} {
		if !strings.Contains(h, needle) {
			t.Errorf("heal_collection_gaps.go missing %s", needle)
		}
	}
	m := srctest.Read(t, "cmd/aveloxis/main.go")
	if !strings.Contains(m, "healCollectionGapsCmd(&cfgPath)") {
		t.Error("heal-collection-gaps not registered in main.go")
	}
}

func TestGetGapHealCandidatesEndToEnd(t *testing.T) {
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

	// Three repos: a gap candidate, a fully-caught-up repo, and a
	// generic-git repo with a "gap" that must be structurally excluded.
	base := int64(944_140_000)
	type seed struct {
		id                  int64
		platform            int
		metaIssues, metaPRs int
		lastIssues, lastPRs int
	}
	seeds := []seed{
		{base + 1, 1, 100, 200, 90, 200}, // candidate: 10 missing issues
		{base + 2, 1, 50, 50, 50, 50},    // clean: no gap
		{base + 3, 3, 10, 10, 0, 0},      // generic git: excluded regardless
	}
	for _, sd := range seeds {
		name := fmt.Sprintf("r%d", sd.id)
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, $2, '_avheal', $3, $4)
			ON CONFLICT (repo_id) DO NOTHING`,
			sd.id, "https://example.com/_avheal/"+name, name, sd.platform); err != nil {
			t.Fatal(err)
		}
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at, last_collected, last_issues, last_prs)
			VALUES ($1, 100, 'queued', NOW(), NOW() - INTERVAL '1 day', $2, $3)
			ON CONFLICT (repo_id) DO UPDATE SET last_collected = EXCLUDED.last_collected,
				last_issues = EXCLUDED.last_issues, last_prs = EXCLUDED.last_prs, status = 'queued'`,
			sd.id, sd.lastIssues, sd.lastPRs); err != nil {
			t.Fatal(err)
		}
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_info (repo_id, issues_count, pr_count, data_collection_date)
			VALUES ($1, $2, $3, NOW())`, sd.id, sd.metaIssues, sd.metaPRs); err != nil {
			t.Fatal(err)
		}
	}
	// v0.27.147 (round 26): repo_info has no unique on repo_id and its
	// history rotation is warn-and-continue, so multiple live snapshots
	// can coexist. Plant STALE extra snapshots (older date, inflated
	// counts) behind both forge repos: the candidate query must resolve
	// the LATEST snapshot per repo — pre-fix, the candidate duplicated
	// with a wrong 999-gap shape, and the CLEAN repo re-entered the
	// candidate set forever off the stale 500-count (the exact
	// non-convergence SR-19 exists to prevent).
	for _, sd := range []struct {
		id          int64
		issues, prs int
	}{{base + 1, 999, 999}, {base + 2, 500, 500}} {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_info (repo_id, issues_count, pr_count, data_collection_date)
			VALUES ($1, $2, $3, NOW() - INTERVAL '2 days')`, sd.id, sd.issues, sd.prs); err != nil {
			t.Fatal(err)
		}
	}
	// v0.27.150 (round 29, suppressed): a collected forge repo with NO
	// repo_info snapshot at all (legacy stamped-but-empty cohort) —
	// excluded from gap mode, but --all is documented as "every
	// collected forge repo" and its force-list mode needs no counts.
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, 'https://example.com/_avheal/nosnap', '_avheal', 'nosnap', 1)
		ON CONFLICT (repo_id) DO NOTHING`, base+5)
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at, last_collected, last_issues, last_prs)
		VALUES ($1, 100, 'queued', NOW(), NOW() - INTERVAL '1 day', 0, 0)
		ON CONFLICT (repo_id) DO UPDATE SET last_collected = NOW() - INTERVAL '1 day', status = 'queued'`, base+5)
	// Review 2026-08-30 #3: last_issues is COUNT(*) INCLUDING the
	// mail-projected Jira synthetics (negative platform_issue_id), so an
	// Apache repo's cached count outruns the forge's meta count and a
	// genuine NATIVE gap is masked from the candidate predicate. Seed:
	// 3 synthetic issue rows + a cached last_issues=4 that MODELS
	// 1 native + 3 synthetics (only the count matters to the
	// predicate); forge meta 3 → 2 native issues genuinely missing.
	// Pre-fix: 3 > 4 is false and the repo never becomes a candidate.
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, 'https://example.com/_avheal/jirainfl', '_avheal', 'jirainfl', 1)
		ON CONFLICT (repo_id) DO NOTHING`, base+6)
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at, last_collected, last_issues, last_prs)
		VALUES ($1, 100, 'queued', NOW(), NOW() - INTERVAL '1 day', 4, 0)
		ON CONFLICT (repo_id) DO UPDATE SET last_collected = NOW() - INTERVAL '1 day',
			last_issues = 4, last_prs = 0, status = 'queued'`, base+6)
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.repo_info (repo_id, issues_count, pr_count, data_collection_date)
		VALUES ($1, 3, 0, NOW())`, base+6)
	for i := 1; i <= 3; i++ {
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, issue_title, issue_state, external_key, data_source)
			VALUES ($1, $2, $3, 't', 'open', $4, 'JIRA') ON CONFLICT DO NOTHING`,
			base+6, -int64(900000+i), 900+i, fmt.Sprintf("AVHEAL-%d", i))
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.issues WHERE repo_id >= $1 AND repo_id <= $2`, base, base+10)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repo_info WHERE repo_id >= $1 AND repo_id <= $2`, base, base+10)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id >= $1 AND repo_id <= $2`, base, base+10)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id >= $1 AND repo_id <= $2`, base, base+10)
	})

	// Keyset scoped to our seed range so fleet residue in the shared
	// scratch DB can't perturb the assertions.
	cands, err := store.GetGapHealCandidates(ctx, base, 100, false)
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for _, c := range cands {
		if c.RepoID > base && c.RepoID <= base+10 {
			got = append(got, c.RepoID)
			if c.RepoID == base+1 {
				if c.Gap != 10 || c.MetaIssues != 100 || c.MetaPRs != 200 {
					t.Errorf("candidate shape wrong: %+v", c)
				}
			}
		}
	}
	if len(got) != 2 || got[0] != base+1 || got[1] != base+6 {
		t.Fatalf("want the gap-bearing repo %d AND the synthetic-inflated repo %d, got %v", base+1, base+6, got)
	}

	// --all: the clean repo joins; generic git stays excluded.
	all, err := store.GetGapHealCandidates(ctx, base, 100, true)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for _, c := range all {
		if c.RepoID > base && c.RepoID <= base+10 {
			seen[c.RepoID] = true
		}
	}
	if !seen[base+1] || !seen[base+2] || seen[base+3] {
		t.Errorf("--all must include clean forge repos and still exclude generic git, got %v", seen)
	}
	if !seen[base+5] {
		t.Error("--all must include a collected repo with NO repo_info snapshot (round 29: the inner join dropped the stamped-but-empty cohort from the completeness sweep)")
	}

	// Drain-lock exclusion path (the healer's skip-if-collecting):
	// mark the candidate 'collecting' — LockReposForDrain must refuse it.
	if _, err := store.pool.Exec(ctx,
		`UPDATE aveloxis_ops.collection_queue SET status = 'collecting' WHERE repo_id = $1`, base+1); err != nil {
		t.Fatal(err)
	}
	locked, err := store.LockReposForDrain(ctx, []int64{base + 1}, "gap-heal-test")
	if err != nil {
		t.Fatal(err)
	}
	if len(locked) != 0 {
		t.Errorf("a mid-collection repo must not drain-lock, got %v", locked)
	}
}
