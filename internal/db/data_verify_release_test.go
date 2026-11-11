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
	// The advice holds for an index the migrate does not own (0.29.144: kate's
	// idx_contributor_repo_repo_git, a hand-run build, which no migrate
	// touches): drop it concurrently, and the migrate rebuilds only its own.
	for _, want := range []string{"DROP INDEX CONCURRENTLY", "pg_stat_progress_create_index", "rebuilds"} {
		if !strings.Contains(r.Detail, want) {
			t.Errorf("the INVALID-index advice must say %q: %s", want, r.Detail)
		}
	}
	if strings.Contains(r.Detail, "run `aveloxis migrate` to rebuild —") {
		t.Errorf("the advice must not promise that a migrate rebuilds every INVALID index (it rebuilds only its own): %s", r.Detail)
	}
	if r := verifyFinding(ctx, t, store, "lower(email) indexes"); r.Severity != "OK" {
		t.Errorf("a migrated database has the three lower(email) indexes: %+v", r)
	}
}

// A concurrent build still running is INVALID until it finishes: it is a
// WARN naming it under "index builds in progress", never an "invalid
// indexes" FAIL (an interrupted migrate leaves its build running on the
// server, and dropping that index would be the wrong advice).
func TestDataVerifyReportsABuildInProgressNotAnInvalidIndex(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	table := fmt.Sprintf("zz_dv_building_%d", time.Now().UnixNano())
	index := table + "_b"
	mustExecRetry(ctx, t, store, `CREATE TABLE aveloxis_data.`+table+` (a int)`)
	t.Cleanup(func() { cleanupExecRetry(context.Background(), store, `DROP TABLE IF EXISTS aveloxis_data.`+table) })

	// A writer holds the table, so the build creates its (INVALID) index and
	// then waits for that writer.
	holder, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, `INSERT INTO aveloxis_data.`+table+` VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	built := make(chan error, 1)
	go func() {
		_, err := store.pool.Exec(context.Background(), `CREATE INDEX CONCURRENTLY `+index+` ON aveloxis_data.`+table+` (a)`)
		built <- err
	}()
	released := false
	release := func() {
		if !released {
			released = true
			_ = holder.Rollback(context.Background())
			if err := <-built; err != nil {
				t.Errorf("the concurrent build failed once released: %v", err)
			}
		}
	}
	t.Cleanup(release)

	deadline := time.Now().Add(30 * time.Second)
	for {
		var n int
		if err := store.pool.QueryRow(ctx, `
			SELECT COUNT(*) FROM pg_stat_progress_create_index p JOIN pg_class i ON i.oid = p.index_relid
			WHERE i.relname = $1 AND p.datid = (SELECT oid FROM pg_database WHERE datname = current_database())`, index).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the concurrent build never reported progress on its index")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if r := verifyFinding(ctx, t, store, "invalid indexes"); strings.Contains(r.Detail, index) {
		t.Errorf("a build in progress is not an INVALID index to drop: %+v", r)
	}
	if r := verifyFinding(ctx, t, store, "index builds in progress"); r.Severity != "WARN" || !strings.Contains(r.Detail, index) {
		t.Errorf("a build in progress is a WARN naming it: %+v", r)
	}

	release()
	if r := verifyFinding(ctx, t, store, "index builds in progress"); strings.Contains(r.Detail, index) {
		t.Errorf("a finished build is no longer reported: %+v", r)
	}
	if r := verifyFinding(ctx, t, store, "invalid indexes"); strings.Contains(r.Detail, index) {
		t.Errorf("a finished build is valid: %+v", r)
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

// Builds by other roles are hidden from a role without pg_read_all_stats:
// their progress rows keep only pid and datid (index_relid is NULL), so the
// index they build reads INVALID here (0.29.144 review F4). The findings say
// so instead of advising a drop without the caveat, and the advice's own
// check query is scoped to this database.
func TestInvalidIndexFindingsNameBuildsThisRoleCannotSee(t *testing.T) {
	find := func(rs []VerifyResult, check string) VerifyResult {
		t.Helper()
		for _, r := range rs {
			if r.Check == check {
				return r
			}
		}
		t.Fatalf("finding %q missing: %+v", check, rs)
		return VerifyResult{}
	}

	rs := invalidIndexFindings(1, "{aveloxis_data.x}", 0, "{}", 2)
	if r := find(rs, "invalid indexes"); r.Severity != "FAIL" || !strings.Contains(r.Detail, "2 index builds") || !strings.Contains(r.Detail, "pg_read_all_stats") {
		t.Errorf("an INVALID index alongside hidden builds names the hidden builds: %+v", r)
	}
	// Only hidden builds: the detail names them alone, with no "0 builds,
	// e.g. {}" clause for the visible ones (review round 2).
	if r := find(invalidIndexFindings(0, "{}", 0, "{}", 2), "index builds in progress"); r.Severity != "WARN" ||
		r.Detail != "2 index builds of any kind are running under a role this one cannot see (it lacks pg_read_all_stats); rerun data-verify when they finish" {
		t.Errorf("hidden builds alone are named alone: %+v", r)
	}
	if r := find(rs, "invalid indexes"); !strings.Contains(r.Detail, "datid = (SELECT oid FROM pg_database WHERE datname = current_database())") {
		t.Errorf("the advice's check query is scoped to this database (index OIDs are per database): %+v", r)
	}

	rs = invalidIndexFindings(0, "{}", 1, "{aveloxis_data.y}", 0)
	if r := find(rs, "invalid indexes"); r.Severity != "OK" {
		t.Errorf("only a visible build in progress: no INVALID finding: %+v", r)
	}
	if r := find(rs, "index builds in progress"); r.Severity != "WARN" || !strings.Contains(r.Detail, "aveloxis_data.y") || strings.Contains(r.Detail, "cannot see") {
		t.Errorf("a visible build is named, with no hidden-build caveat: %+v", r)
	}

	rs = invalidIndexFindings(1, "{aveloxis_data.x}", 0, "{}", 0)
	if r := find(rs, "invalid indexes"); r.Severity != "FAIL" || strings.Contains(r.Detail, "cannot see") {
		t.Errorf("no hidden builds: no caveat: %+v", r)
	}
	if r := find(rs, "index builds in progress"); r.Severity != "OK" {
		t.Errorf("no builds: OK: %+v", r)
	}
}
