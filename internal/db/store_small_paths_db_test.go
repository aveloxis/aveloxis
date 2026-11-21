// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sort"
	"testing"
	"time"
)

// The store's small reads and purges that no DB-tier test reached
// (0.29.214). Each seeds what the statement reads, and asserts what it
// returns or leaves.

func TestAnSBOMIsStoredForItsRepository(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID, _, _ := pathsRepo(t, store, "sbom", "aveloxis_data.repo_sbom_scans")
	if err := store.InsertSBOM(ctx, repoID, []byte(`{"bomFormat":"CycloneDX","zz":"paths","components":[{"name":"left-pad","version":"1.3.0"}]}`)); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertSBOM(ctx, repoID, []byte(`{"bomFormat":"SPDX"}`)); err != nil {
		t.Fatal(err)
	}
	if n := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.repo_sbom_scans WHERE repo_id = $1`, repoID); n != 2 {
		t.Errorf("%d SBOM rows for the repository, want the 2 stored", n)
	}
	if got := pathsText(t, store, `SELECT sbom_scan->>'zz' FROM aveloxis_data.repo_sbom_scans WHERE repo_id = $1 AND sbom_scan->>'bomFormat' = 'CycloneDX'`, repoID); got != "paths" {
		t.Errorf("the stored document's zz is %q, want paths", got)
	}
	// The document is stored WHOLE: its components are what an SBOM is for
	// (round 9: a statement that stored it without them passed).
	if got := pathsText(t, store, `SELECT sbom_scan->'components'->0->>'name' || '@' || (sbom_scan->'components'->0->>'version') FROM aveloxis_data.repo_sbom_scans WHERE repo_id = $1 AND sbom_scan->>'bomFormat' = 'CycloneDX'`, repoID); got != "left-pad@1.3.0" {
		t.Errorf("the stored document's first component is %q, want left-pad@1.3.0", got)
	}
}

func TestLicenseCoverageCountsFilesWithALicense(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	a, _, _ := pathsRepo(t, store, "lic-a", "aveloxis_scan.scancode_file_results")
	b, _, _ := pathsRepo(t, store, "lic-b", "aveloxis_scan.scancode_file_results")
	for _, f := range []struct {
		repo int64
		path string
		spdx any
	}{{a, "a.go", "MIT"}, {a, "b.go", "MIT"}, {a, "c.go", ""}, {a, "d.go", nil}, {b, "e.go", "Apache-2.0"}} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_scan.scancode_file_results (repo_id, path, detected_license_expression_spdx) VALUES ($1, $2, $3)`, f.repo, f.path, f.spdx)
	}
	got, err := store.LicenseCoverageSnapshot(ctx, []int64{a})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"files_scanned": 4.0, "files_with_license": 2.0, "distinct_spdx": 1.0}
	if got.Value != 50 || !reflect.DeepEqual(got.Detail, want) {
		t.Errorf("one repository: %v%% %v; want 50%% %v", got.Value, got.Detail, want)
	}
	both, err := store.LicenseCoverageSnapshot(ctx, []int64{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if both.Value != 60 || both.Detail["distinct_spdx"] != 2.0 {
		t.Errorf("both repositories: %v%% %v; want 60%% with 2 distinct licenses", both.Value, both.Detail)
	}
	none, err := store.LicenseCoverageSnapshot(ctx, []int64{-77})
	if err != nil || none.Value != 0 || none.Detail["files_scanned"] != 0.0 {
		t.Errorf("no scanned file: %v %v (%v); want 0%% of 0 files", none.Value, none.Detail, err)
	}
}

func TestBreadthEventsAreStoredOnceAndTheNewestIsRead(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tag := pathsTag("breadth")
	cntrb := seedR6Contributor(t, store, tag, "", nil)
	other := seedR6Contributor(t, store, tag+"-other", "", nil)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributor_repo WHERE cntrb_id = ANY($1::uuid[])`, []string{cntrb, other})
	})
	if at, err := store.GetNewestContributorRepoEvent(ctx, cntrb); err != nil || !at.IsZero() {
		t.Fatalf("a contributor with no event: %v (%v); want the zero time", at, err)
	}
	base := time.Now().UnixNano() % 1_000_000_000_000
	t1 := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(48 * time.Hour)
	rows := []*ContributorRepoRow{
		{CntrbID: cntrb, RepoGit: "https://github.com/zz/one", RepoName: "one", GHRepoID: 11, Category: "PushEvent", EventID: base + 1, CreatedAt: t1},
		{CntrbID: cntrb, RepoGit: "https://github.com/zz/two", RepoName: "two", GHRepoID: 12, Category: "IssuesEvent", EventID: base + 2, CreatedAt: t2},
		{CntrbID: other, RepoGit: "https://github.com/zz/three", RepoName: "three", GHRepoID: 13, Category: "PushEvent", EventID: base + 3, CreatedAt: t2.Add(time.Hour)},
	}
	if err := store.InsertContributorRepoBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	// The same events again: an event is stored once.
	if err := store.InsertContributorRepoBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertContributorRepoBatch(ctx, nil); err != nil {
		t.Errorf("an empty batch: %v", err)
	}
	if n := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.contributor_repo WHERE cntrb_id = $1::uuid`, cntrb); n != 2 {
		t.Errorf("%d events stored for the contributor, want 2", n)
	}
	if got := pathsText(t, store, `SELECT repo_name || '|' || cntrb_category || '|' || gh_repo_id || '|' || tool_source || '|' || data_source FROM aveloxis_data.contributor_repo WHERE event_id = $1`, base+2); got != "two|IssuesEvent|12|aveloxis-breadth|GitHub API" {
		t.Errorf("the second event is stored as %q", got)
	}
	at, err := store.GetNewestContributorRepoEvent(ctx, cntrb)
	if err != nil || !at.Equal(t2) {
		t.Errorf("the contributor's newest event is %v (%v), want %v: the other contributor's later event is not theirs", at, err, t2)
	}
}

func TestOldRequestCountsArePruned(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tag := pathsTag("prune")
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.request_counts WHERE subject LIKE $1`, tag+"%")
	})
	now := time.Date(2031, 5, 10, 13, 0, 0, 0, time.UTC)
	for day, subject := range map[string]string{"2031-05-10": tag + "-today", "2031-05-09": tag + "-yesterday", "2031-05-08": tag + "-before", "2031-04-01": tag + "-long-ago"} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.request_counts (subject, day, requests) VALUES ($1, $2::date, 3)`, subject, day)
	}
	n, err := store.PruneRequestCounts(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if n < 2 {
		t.Errorf("%d rows pruned, want at least the two of this test older than yesterday", n)
	}
	var left []string
	rows, err := store.pool.Query(ctx, `SELECT subject FROM aveloxis_ops.request_counts WHERE subject LIKE $1 ORDER BY 1`, tag+"%")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		left = append(left, s)
	}
	rows.Close()
	if want := []string{tag + "-today", tag + "-yesterday"}; !reflect.DeepEqual(left, want) {
		t.Errorf("after the prune %v remain, want %v (today's and yesterday's counts are kept)", left, want)
	}
}

func TestStagingIsCountedSummarisedAndPurged(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID, owner, name := pathsRepo(t, store, "staging", "aveloxis_ops.staging")
	other, _, _ := pathsRepo(t, store, "staging-other", "aveloxis_ops.staging")
	add := func(repo int64, entity string, processed bool, age string) {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.staging (repo_id, platform_id, entity_type, payload, processed, created_at)
			VALUES ($1, 1, $2, '{"k":"vvvvvvvvvv"}', $3, NOW() - $4::interval)`, repo, entity, processed, age)
	}
	add(repoID, "issue", true, "3 hours")
	add(repoID, "issue", true, "1 minute")
	add(repoID, "issue", false, "3 hours")
	add(repoID, "pull_request", false, "1 minute")
	add(other, "issue", false, "1 minute")

	if n, err := store.StagingRowCount(ctx, repoID); err != nil || n != 4 {
		t.Errorf("the repository's staged rows: %d (%v), want 4", n, err)
	}
	if n, err := store.StagingRowCount(ctx, -77); err != nil || n != 0 {
		t.Errorf("a repository with nothing staged: %d (%v)", n, err)
	}

	stats, err := store.StagingStats(ctx, owner+"/"+name, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 2 || stats[0].EntityType != "issue" || stats[0].Rows != 3 || stats[0].Processed != 2 || stats[0].Unprocessed != 1 || stats[0].RepoID != repoID || stats[0].RepoOwner != owner || stats[0].Bytes <= 0 ||
		stats[1].EntityType != "pull_request" || stats[1].Rows != 1 || stats[1].Unprocessed != 1 {
		t.Errorf("the repository's staging by entity: %+v; want issue (3 rows, 2 processed) then pull_request (1 row)", stats)
	}
	if !stats[0].Oldest.Before(stats[0].Newest) {
		t.Errorf("the issue rows' oldest %v is not before their newest %v", stats[0].Oldest, stats[0].Newest)
	}
	top, err := store.StagingStats(ctx, "", 1)
	if err != nil || len(top) != 1 {
		t.Fatalf("the top one: %d rows (%v)", len(top), err)
	}
	all, err := store.StagingStats(ctx, "", 100000)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for i, r := range all {
		if i > 0 && all[i-1].Rows < r.Rows {
			t.Errorf("the fleet's staging is not ordered by rows, largest first: %d before %d", all[i-1].Rows, r.Rows)
		}
		if r.RepoID == repoID || r.RepoID == other {
			seen++
		}
	}
	if seen != 3 {
		t.Errorf("the fleet's staging names %d (repository, entity) pairs of this test, want 3", seen)
	}

	removed, err := store.PurgeStagedProcessed(ctx, 2*time.Hour)
	if err != nil || removed < 1 {
		t.Fatalf("purging processed rows older than two hours removed %d (%v)", removed, err)
	}
	if left := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_ops.staging WHERE repo_id = $1`, repoID); left != 3 {
		t.Errorf("%d rows left for the repository, want 3: only the processed row older than the retention goes", left)
	}
	if left := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_ops.staging WHERE repo_id = $1 AND processed`, repoID); left != 1 {
		t.Errorf("%d processed rows left, want the recent one", left)
	}
}

func TestProcessedTrackerAndMailStagingIsPurgedAfterItsRetention(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tag := pathsTag("purge")
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.jira_staging WHERE project_key = $1`, tag)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.mailing_list_staging WHERE message_id_header LIKE $1`, tag+"%")
	})
	for i, r := range []struct {
		processed bool
		age       string
		// The fourth row is the one that tells the retention's UNIT: thirty
		// minutes old is inside two hours and outside two minutes (round 8:
		// with ages of three hours and one minute only, a purge that read
		// its seconds as minutes passed).
	}{{true, "3 hours"}, {true, "1 minute"}, {false, "3 hours"}, {true, "30 minutes"}} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.jira_staging (jps_id, project_key, issue_key, issue_updated, envelope, processed, created_at)
			VALUES (-7, $1, $2, NOW(), '{}', $3, NOW() - $4::interval)`, tag, fmt.Sprintf("K-%d", i), r.processed, r.age)
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.mailing_list_staging (rgls_id, message_id_header, envelope, processed, created_at)
			VALUES (-7, $1, '{}', $2, NOW() - $3::interval)`, fmt.Sprintf("%s-%d", tag, i), r.processed, r.age)
	}
	if n, err := store.PurgeJiraStagingProcessed(ctx, 2*time.Hour); err != nil || n < 1 {
		t.Fatalf("the tracker purge removed %d (%v)", n, err)
	}
	if n, err := store.PurgeMailingListStagingProcessed(ctx, (2 * time.Hour).Seconds()); err != nil || n < 1 {
		t.Fatalf("the mail purge removed %d (%v)", n, err)
	}
	if got := pathsText(t, store, `SELECT string_agg(issue_key, ',' ORDER BY issue_key) FROM aveloxis_ops.jira_staging WHERE project_key = $1`, tag); got != "K-1,K-2,K-3" {
		t.Errorf("tracker rows left: %q; want K-1 and K-3 (processed, inside the retention) and K-2 (old, not processed)", got)
	}
	if got := pathsText(t, store, `SELECT string_agg(right(message_id_header, 1), ',' ORDER BY message_id_header) FROM aveloxis_ops.mailing_list_staging WHERE message_id_header LIKE $1`, tag+"%"); got != "1,2,3" {
		t.Errorf("mail rows left: %q; want 1 and 3 (processed, inside the retention) and 2 (old, not processed)", got)
	}
}

func TestTheRepositoryHostCheckMarkerIsStampedAndRead(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.program_markers WHERE marker = $1`, repositoryHostCheckMarker)
	})
	mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_ops.program_markers WHERE marker = $1`, repositoryHostCheckMarker)
	// Another program's marker, finished long ago: it is not this check's,
	// before the check has run or after (round 9: a read of ANY marker
	// passed, there being no other row).
	neighbour := pathsTag("marker")
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.program_markers (marker, rows_done, completed_at) VALUES ($1, 7, NOW() - INTERVAL '30 days')`, neighbour)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.program_markers WHERE marker = $1`, neighbour)
	})
	if since, ever, err := store.SinceRepositoryHostCheck(ctx); err != nil || ever || since != 0 {
		t.Fatalf("before any check: since %v, ever %v (%v)", since, ever, err)
	}
	if err := store.MarkRepositoryHostCheckDone(ctx, 12); err != nil {
		t.Fatal(err)
	}
	since, ever, err := store.SinceRepositoryHostCheck(ctx)
	if err != nil || !ever || since < 0 || since > time.Minute {
		t.Errorf("just after a check: since %v, ever %v (%v); want a moment ago", since, ever, err)
	}
	// An older stamp, then a second check: the row is updated in place.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.program_markers SET completed_at = NOW() - INTERVAL '3 hours' WHERE marker = $1`, repositoryHostCheckMarker)
	if since, _, _ := store.SinceRepositoryHostCheck(ctx); since < 170*time.Minute || since > 190*time.Minute {
		t.Errorf("three hours after a check: since %v", since)
	}
	if err := store.MarkRepositoryHostCheckDone(ctx, 40); err != nil {
		t.Fatal(err)
	}
	if got := pathsText(t, store, `SELECT COUNT(*) || ':' || MAX(rows_done) FROM aveloxis_ops.program_markers WHERE marker = $1`, repositoryHostCheckMarker); got != "1:40" {
		t.Errorf("after the second check the marker is %q, want one row with 40 hosts", got)
	}
	if since, _, _ := store.SinceRepositoryHostCheck(ctx); since > time.Minute {
		t.Errorf("after the second check: since %v", since)
	}
	if got := pathsText(t, store, `SELECT rows_done || ':' || (completed_at < NOW() - INTERVAL '29 days') FROM aveloxis_ops.program_markers WHERE marker = $1`, neighbour); got != "7:true" {
		t.Errorf("the other program's marker is now %q; the check's stamp must not touch it", got)
	}
}

func TestQueueStateReadsForARepository(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	collecting, _, _ := pathsRepo(t, store, "q-collecting")
	queued, _, _ := pathsRepo(t, store, "q-queued")
	absent, _, _ := pathsRepo(t, store, "q-absent")
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at) VALUES ($1, 100, 'collecting', NOW())`, collecting)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at, last_collected) VALUES ($1, 100, 'queued', NOW(), NOW())`, queued)
	for id, want := range map[int64]bool{collecting: true, queued: false, absent: false} {
		if got, err := store.IsRepoCollecting(ctx, id); err != nil || got != want {
			t.Errorf("IsRepoCollecting(%d) = %v (%v), want %v", id, got, err, want)
		}
	}
	for id, want := range map[int64][2]bool{collecting: {false, true}, queued: {true, false}, absent: {false, false}} {
		collected, running, err := store.RepoWhitespaceRewalkState(ctx, id)
		if err != nil || collected != want[0] || running != want[1] {
			t.Errorf("RepoWhitespaceRewalkState(%d) = collected %v, collecting %v (%v); want %v", id, collected, running, err, want)
		}
	}
	// A repository with no queue row is stranded; one with a row, and an
	// archived one, are not.
	before, err := store.CountStrandedRepos(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A queue row of ANY status ends it: one being collected is not
	// stranded (round 9: a count of the repositories with no QUEUED row
	// passed, the row here having been a queued one).
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at) VALUES ($1, 100, 'collecting', NOW())`, absent)
	afterQueued, err := store.CountStrandedRepos(ctx)
	if err != nil || afterQueued != before-1 {
		t.Errorf("stranded repositories %d -> %d (%v) once one has a queue row (being collected), want one fewer", before, afterQueued, err)
	}
	mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, absent)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET repo_archived = TRUE WHERE repo_id = $1`, absent)
	if afterArchived, err := store.CountStrandedRepos(ctx); err != nil || afterArchived != before-1 {
		t.Errorf("stranded repositories %d -> %d (%v) once one is archived, want one fewer", before, afterArchived, err)
	}
}

func TestPullRequestAndReviewLookupsAndOpenNumbers(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID, _, _ := pathsRepo(t, store, "prs", "aveloxis_data.pull_request_reviews", "aveloxis_data.pull_requests", "aveloxis_data.repo_info")
	other, _, _ := pathsRepo(t, store, "prs-other", "aveloxis_data.pull_requests")
	// The other repository's #9 is stored FIRST, so that a lookup by
	// number alone finds it and not this repository's (round 9: stored
	// second, it never did).
	pathsPR(t, store, other, 9, "open")
	open9 := pathsPR(t, store, repoID, 9, "open")
	pathsPR(t, store, repoID, 4, "open")
	pathsPR(t, store, repoID, 5, "closed")
	if id, err := store.FindPRDBID(ctx, repoID, 9); err != nil || id != open9 {
		t.Errorf("FindPRDBID(repo, 9) = %d (%v), want %d: the other repository's #9 is not it", id, err, open9)
	}
	if id, err := store.FindPRDBID(ctx, repoID, 777); err != nil || id != 0 {
		t.Errorf("a pull request that does not exist: %d (%v), want 0", id, err)
	}
	numbers, err := store.GetOpenPRNumbers(ctx, repoID)
	if err != nil || !reflect.DeepEqual(numbers, []int{4, 9}) {
		t.Errorf("open pull requests: %v (%v), want [4 9]", numbers, err)
	}
	// Another review of the same repository, stored first: the lookup is
	// by the review's forge id, not "a review of this repository".
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.pull_request_reviews (pull_request_id, repo_id, platform_id, platform_review_id) VALUES ($1, $2, 1, $3)`, open9, repoID, repoID*10+2)
	var review int64
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.pull_request_reviews (pull_request_id, repo_id, platform_id, platform_review_id)
		VALUES ($1, $2, 1, $3) RETURNING pr_review_id`, open9, repoID, repoID*10+1).Scan(&review); err != nil {
		t.Fatal(err)
	}
	if id, err := store.FindReviewDBID(ctx, repoID, repoID*10+1); err != nil || id != review {
		t.Errorf("FindReviewDBID = %d (%v), want %d", id, err, review)
	}
	if id, err := store.FindReviewDBID(ctx, other, repoID*10+1); err != nil || id != 0 {
		t.Errorf("the review looked up under another repository: %d (%v), want 0", id, err)
	}

	// The latest repo_info row's counts: by date, a NULL date last.
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_info (repo_id, issues_count, pr_count, data_collection_date) VALUES ($1, 1, 2, NULL), ($1, 30, 40, NOW() - INTERVAL '1 day'), ($1, 10, 20, NOW() - INTERVAL '5 days')`, repoID)
	issues, prs, err := store.GetRepoMetaCounts(ctx, repoID)
	if err != nil || issues != 30 || prs != 40 {
		t.Errorf("the repository's metadata counts: %d issues, %d pull requests (%v); want the newest dated row's 30 and 40", issues, prs, err)
	}
	if _, _, err := store.GetRepoMetaCounts(ctx, other); err == nil {
		t.Error("a repository with no metadata row was read without error")
	}
}

func TestVulnerabilityVectorsAndRepositoriesAreListedDistinct(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	a, _, _ := pathsRepo(t, store, "vuln-a", "aveloxis_data.repo_deps_vulnerabilities")
	b, _, _ := pathsRepo(t, store, "vuln-b", "aveloxis_data.repo_deps_vulnerabilities")
	tag := pathsTag("v")
	vecA, vecB := "CVSS:3.1/"+tag+"/A", "CVSS:3.1/"+tag+"/B"
	for i, r := range []struct {
		repo   int64
		vector string
	}{{a, vecB}, {a, vecA}, {b, vecA}, {b, ""}} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_deps_vulnerabilities (repo_id, vuln_id, package_name, cvss_vector) VALUES ($1, $2, 'zz-paths', $3)`, r.repo, fmt.Sprintf("%s-%d", tag, i), r.vector)
	}
	vectors, err := store.ListDistinctVulnVectors(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var mine []string
	for _, v := range vectors {
		if v == "" {
			t.Error("an empty vector was listed")
		}
		if v == vecA || v == vecB {
			mine = append(mine, v)
		}
	}
	if !reflect.DeepEqual(mine, []string{vecA, vecB}) || !sort.StringsAreSorted(vectors) {
		t.Errorf("this test's vectors are listed as %v (the whole list sorted: %v); want each once, in order", mine, sort.StringsAreSorted(vectors))
	}
	repos, err := store.ReposWithVulnerabilities(ctx)
	if err != nil {
		t.Fatal(err)
	}
	count := map[int64]int{}
	for i, id := range repos {
		count[id]++
		if i > 0 && repos[i-1] >= id {
			t.Errorf("the repositories are not listed once each in order: %d then %d", repos[i-1], id)
		}
	}
	if count[a] != 1 || count[b] != 1 {
		t.Errorf("the two repositories are listed %d and %d times, want once each", count[a], count[b])
	}
}

func TestScancodeLastRunIsTheNewestScan(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID, _, _ := pathsRepo(t, store, "scan", "aveloxis_scan.scancode_scans")
	// Another repository scanned later than this one ever is: its scans are
	// not this repository's (round 9: the newest scan of ANY repository
	// passed, there being no other).
	other, _, _ := pathsRepo(t, store, "scan-other", "aveloxis_scan.scancode_scans")
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_scan.scancode_scans (repo_id, data_collection_date) VALUES ($1, $2)`, other, time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC))
	if _, err := store.ScancodeLastRun(ctx, repoID); err == nil {
		t.Error("a repository never scanned was read without error")
	}
	newest := time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_scan.scancode_scans (repo_id, data_collection_date) VALUES ($1, $2), ($1, $3)`, repoID, newest.Add(-72*time.Hour), newest)
	if at, err := store.ScancodeLastRun(ctx, repoID); err != nil || !at.Equal(newest) {
		t.Errorf("the last scan is %v (%v), want %v", at, err, newest)
	}
}

func TestRepositoriesForMetadataRefreshAreKeysetPagedOverForgeRepositories(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	first, owner, name := pathsRepo(t, store, "meta-1")
	second, _, _ := pathsRepo(t, store, "meta-2")
	generic, _, _ := pathsRepo(t, store, "meta-git")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_id = 3 WHERE repo_id = $1`, generic)
	// A forge repository with no name, and one with no owner: there is
	// nothing to ask the forge for (round 9: neither kind was seeded).
	unnamed, _, _ := pathsRepo(t, store, "meta-noname")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET repo_name = '' WHERE repo_id = $1`, unnamed)
	unowned, _, _ := pathsRepo(t, store, "meta-noowner")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET repo_owner = '' WHERE repo_id = $1`, unowned)
	got, err := store.GetReposForMetadataRefresh(ctx, first-1, 100000)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]RepoMetadataBackfillTarget{}
	for i, r := range got {
		seen[r.RepoID] = r
		if r.RepoID <= first-1 || (i > 0 && got[i-1].RepoID >= r.RepoID) {
			t.Errorf("row %d is repository %d: not after the cursor, or not in order", i, r.RepoID)
		}
	}
	if seen[first] != (RepoMetadataBackfillTarget{RepoID: first, Owner: owner, Name: name, PlatformID: 1}) {
		t.Errorf("the first repository is listed as %+v", seen[first])
	}
	if _, ok := seen[second]; !ok {
		t.Error("the second repository is not listed")
	}
	if _, ok := seen[generic]; ok {
		t.Error("a generic-git repository is listed for a forge metadata refresh")
	}
	if _, ok := seen[unnamed]; ok {
		t.Error("a repository with no name is listed for a forge metadata refresh")
	}
	if _, ok := seen[unowned]; ok {
		t.Error("a repository with no owner is listed for a forge metadata refresh")
	}
	if page, err := store.GetReposForMetadataRefresh(ctx, first-1, 1); err != nil || len(page) != 1 || page[0].RepoID != first {
		t.Errorf("a page of one after the cursor: %+v (%v)", page, err)
	}
	if page, err := store.GetReposForMetadataRefresh(ctx, first, 0); err != nil || len(page) == 0 || page[0].RepoID <= first {
		t.Errorf("the page after the first repository, with the default limit: %+v (%v)", page, err)
	}
}

func TestCaseVariantDuplicateRepositoriesAreCounted(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	before, err := CountCaseVariantRepoDups(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	// The unique index on LOWER(repo_git) forbids such a pair for forge
	// repositories, so it is absent while the pair exists.
	if _, err := store.pool.Exec(ctx, `DROP INDEX IF EXISTS aveloxis_data.uq_repos_repo_git_ci`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ensureRepoGitCaseInsensitiveUnique(context.Background(), store, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if n := pathsInt(t, store, `SELECT COUNT(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'uq_repos_repo_git_ci' AND i.indisvalid`); n != 1 {
			t.Errorf("the case-insensitive unique index was not restored (%d valid)", n)
		}
	})
	tag := pathsTag("case")
	// The fifth is a forge repository with no case variant: it is no group
	// (round 8: without it, a count of every repository as a group of one
	// also rose by exactly one).
	for _, url := range []string{"https://github.com/" + tag + "/Repo", "https://github.com/" + tag + "/repo", "https://example.org/" + tag + "/Git", "https://example.org/" + tag + "/git", "https://github.com/" + tag + "/alone"} {
		platform := 1
		if url[8:15] == "example" {
			platform = 3
		}
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id) VALUES ($1, $2, 'r', $3)`, url, tag, platform)
	}
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repos WHERE repo_owner = $1`, tag)
	})
	after, err := CountCaseVariantRepoDups(ctx, store)
	if err != nil || after != before+1 {
		t.Errorf("case-variant duplicate groups %d -> %d (%v); want one more: the forge pair, and neither the generic-git pair nor the forge repository that has no variant", before, after, err)
	}
}
