// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// verifyFinding runs the battery and returns the named probe's result.
func verifyFinding(ctx context.Context, t *testing.T, store *PostgresStore, check string) VerifyResult {
	t.Helper()
	for _, r := range store.RunDataVerification(ctx, VerifyOptions{Sample: 10}) {
		if r.Check == check {
			return r
		}
	}
	t.Fatalf("probe %q missing from the battery", check)
	return VerifyResult{}
}

// mentions reports whether a probe's example list names the repository.
func mentions(r VerifyResult, repoID int64) bool {
	for _, field := range strings.FieldsFunc(r.Detail, func(c rune) bool { return c == '{' || c == '}' || c == ',' || c == ' ' }) {
		if field == fmt.Sprint(repoID) {
			return true
		}
	}
	return false
}

// The gone history's invariant (0.29.140, the evaluation's release checks):
// a gone-stamped repository has exactly one open episode, a reachable one
// none. Both broken shapes FAIL and name the repository; the marker's own
// shape passes.
func TestDataVerifyChecksTheGoneEpisodes(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	stampedBare := seedGoneHistoryRepo(t, store, ctx, "dv-stamped-bare")
	openReachable := seedGoneHistoryRepo(t, store, ctx, "dv-open-reachable")
	marked := seedGoneHistoryRepo(t, store, ctx, "dv-marked")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET repo_gone_at = NOW(), repo_archived = TRUE WHERE repo_id = $1`, stampedBare)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_gone_history (repo_id, gone_at, gone_status, gone_source) VALUES ($1, NOW(), 404, 'prelim')`, openReachable)
	if err := store.SidelineRepo(ctx, marked, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	r := verifyFinding(ctx, t, store, "gone episodes match the stamps")
	if r.Severity != "FAIL" || !mentions(r, stampedBare) || !mentions(r, openReachable) {
		t.Errorf("a stamp without an episode and an episode without a stamp FAIL, naming both: %+v", r)
	}
	if mentions(r, marked) {
		t.Errorf("the gone marker's own shape is consistent: %+v", r)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET repo_gone_at = NULL, repo_archived = FALSE WHERE repo_id = $1`, stampedBare)
	mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_data.repo_gone_history WHERE repo_id = $1`, openReachable)
	if r := verifyFinding(ctx, t, store, "gone episodes match the stamps"); mentions(r, stampedBare) || mentions(r, openReachable) {
		t.Errorf("repaired rows are no longer named: %+v", r)
	}
}

// A queued gone-stamped repository is a pending re-probe (a re-add): a WARN
// that names it, never a FAIL.
func TestDataVerifyWarnsOnAGoneAndQueuedRepository(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	repoID := seedGoneHistoryRepo(t, store, ctx, "dv-gone-queued")
	if err := store.SidelineRepo(ctx, repoID, 404, GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueRepo(ctx, repoID, 100); err != nil {
		t.Fatal(err)
	}
	if r := verifyFinding(ctx, t, store, "gone and queued"); r.Severity != "WARN" || !mentions(r, repoID) {
		t.Errorf("a re-added gone repository is a WARN naming it: %+v", r)
	}
}

// An interrupted concurrent build leaves an INVALID index: a FAIL naming it.
// A unique build over duplicate rows fails exactly that way.
func TestDataVerifyFailsOnAnInvalidIndex(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	table := fmt.Sprintf("zz_dv_invalid_%d", time.Now().UnixNano())
	mustExecRetry(ctx, t, store, `CREATE TABLE aveloxis_data.`+table+` (a int); INSERT INTO aveloxis_data.`+table+` VALUES (1), (1)`)
	t.Cleanup(func() { cleanupExecRetry(context.Background(), store, `DROP TABLE IF EXISTS aveloxis_data.`+table) })
	if _, err := store.pool.Exec(ctx, `CREATE UNIQUE INDEX CONCURRENTLY `+table+`_u ON aveloxis_data.`+table+` (a)`); err == nil {
		t.Fatal("the unique build over duplicates must fail, leaving an INVALID index")
	}
	r := verifyFinding(ctx, t, store, "invalid indexes")
	if r.Severity != "FAIL" || !strings.Contains(r.Detail, table+"_u") {
		t.Errorf("an INVALID index FAILs and is named: %+v", r)
	}
	if r := verifyFinding(ctx, t, store, "lower(email) indexes"); r.Severity != "OK" {
		t.Errorf("a migrated database has the three lower(email) indexes: %+v", r)
	}
}

// An adopted change's earlier-era buckets hold their commits: a bucket below
// its fold FAILs and names the repository; a repository whose daily table is
// not complete is not judged.
func TestDataVerifyChecksTheEarlierEraBuckets(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avdv_era")
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_forge_id_changes WHERE repo_id = $1`, fx.repoID)
	})
	old := fx.in.AddDate(0, 0, -40)
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp)
		VALUES ($1, 'dv0000000001', 'f.go', 'n', 'x@old', $2, $3)`, fx.repoID, old.Format("2006-01-02"), old.Add(9*time.Hour))
	created := fx.in.AddDate(0, 0, -20)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '95336057' WHERE repo_id = $1`, fx.repoID)
	if err := store.AdoptForgeID(ctx, fx.repoID, "95336057", "1438985131", &created, "operator", "", publicListing()); err != nil {
		t.Fatal(err)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET commit_daily_complete_at = NOW() WHERE repo_id = $1`, fx.repoID)
	if r := verifyFinding(ctx, t, store, "earlier-era daily buckets"); mentions(r, fx.repoID) {
		t.Fatalf("the adoption restored the earlier era: %+v", r)
	}
	mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1 AND author_email = 'x@old'`, fx.repoID)
	if r := verifyFinding(ctx, t, store, "earlier-era daily buckets"); r.Severity != "FAIL" || !mentions(r, fx.repoID) {
		t.Errorf("a missing earlier-era bucket FAILs and names the repository: %+v", r)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET commit_daily_complete_at = NULL WHERE repo_id = $1`, fx.repoID)
	if r := verifyFinding(ctx, t, store, "earlier-era daily buckets"); mentions(r, fx.repoID) {
		t.Errorf("a repository whose daily table is not complete is not judged: %+v", r)
	}
}

// The sampled probes say so: data-test's snapshot baseline compares their
// findings by check only, since their detail is a random sample's.
func TestDataVerifyMarksItsSampledProbes(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	sampled := map[string]bool{}
	for _, r := range store.RunDataVerification(ctx, VerifyOptions{Sample: 10}) {
		if r.Sampled {
			sampled[r.Check] = true
		}
	}
	for _, check := range []string{"cached counts vs actual", "gathered vs metadata drift"} {
		if !sampled[check] {
			t.Errorf("%q reads a random sample and must be marked Sampled", check)
		}
	}
	if sampled["gone episodes match the stamps"] || sampled["batch vs single stats"] {
		t.Errorf("deterministic probes are not sampled: %v", sampled)
	}
}

// The deterministic sample draws the same repositories every time, so two
// runs over the same rows read alike and nothing is marked sampled
// (data-test's snapshot mode compares the two sides' findings exactly).
func TestDataVerifyDeterministicSampleReadsAlike(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	// Far more eligible rows than the sample (30), half with a stale cached
	// count: a random 30 draws a different mismatch count run to run, so
	// identical details over five runs prove the sample is fixed. 600 rows so
	// that rows other tests of the package left behind cannot crowd the stale
	// ones out of the sample (with a hundred of them, no stale row among 30
	// has odds near 1e-7).
	owner := fmt.Sprintf("_avdv_det_%d", time.Now().UnixNano())
	mustExecRetry(ctx, t, store, `
		WITH r AS (
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
			SELECT 'https://github.com/' || $1 || '/r' || g, $1, 'r' || g, 1 FROM generate_series(1, 600) g
			RETURNING repo_id)
		INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at, last_collected, last_issues, last_prs)
		SELECT repo_id, 100, 'queued', NOW() + INTERVAL '30 days', NOW() - INTERVAL '1 day',
		       CASE WHEN repo_id % 2 = 0 THEN 7 ELSE 0 END, 0
		FROM r`, owner)
	t.Cleanup(func() {
		bg := context.Background()
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = $1)`, owner)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_data.repos WHERE repo_owner = $1`, owner)
	})
	run := func() map[string]VerifyResult {
		out := map[string]VerifyResult{}
		for _, r := range store.RunDataVerification(ctx, VerifyOptions{Sample: 30, DeterministicSample: true}) {
			out[r.Check] = r
		}
		return out
	}
	first := run()
	if r := first["cached counts vs actual"]; r.Severity != "FAIL" {
		t.Fatalf("the stale cached counts are found: %+v", r)
	}
	for i := 0; i < 4; i++ {
		next := run()
		for _, check := range []string{"cached counts vs actual", "gathered vs metadata drift"} {
			a, b := first[check], next[check]
			if a.Check == "" || a.Sampled || a.Detail != b.Detail || a.Severity != b.Severity {
				t.Fatalf("%q with the deterministic sample: present, not marked sampled, identical every run: %+v vs %+v", check, a, b)
			}
		}
	}
}
