// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The dm_ aggregate tables are rebuilt from the commits: what stood for a
// repository or a group before is deleted, and what its commits say now is
// written (0.29.214: the test that stood for this ran the statements on a
// repository with no rows and asserted only that no error came back).
func TestTheAggregateTablesAreRebuiltFromTheCommits(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tag := pathsTag("agg")
	var group int64
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repo_groups (rg_name, rg_type) VALUES ($1, 'manual') RETURNING repo_group_id`, tag).Scan(&group); err != nil {
		t.Fatal(err)
	}
	dm := []string{"dm_repo_annual", "dm_repo_monthly", "dm_repo_weekly"}
	dmGroup := []string{"dm_repo_group_annual", "dm_repo_group_monthly", "dm_repo_group_weekly"}
	children := []string{"aveloxis_data.commits"}
	for _, d := range dm {
		children = append(children, "aveloxis_data."+d)
	}
	repoID, _, _ := pathsRepo(t, store, "agg", children...)
	ungrouped, _, _ := pathsRepo(t, store, "agg-nogroup")
	t.Cleanup(func() {
		c := context.Background()
		for _, d := range dmGroup {
			cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.`+d+` WHERE repo_group_id = $1`, group)
		}
		cleanupExecRetry(c, store, `UPDATE aveloxis_data.repos SET repo_group_id = NULL WHERE repo_group_id = $1`, group)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.repo_groups WHERE repo_group_id = $1`, group)
	})
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET repo_group_id = $2 WHERE repo_id = $1`, repoID, group)
	// A second repository of the same group: the group is still one group.
	second, _, _ := pathsRepo(t, store, "agg-second")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET repo_group_id = $2 WHERE repo_id = $1`, second, group)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET repo_group_id = NULL WHERE repo_id = $1`, ungrouped)
	commit := func(hash, file string, at time.Time, added, removed int) {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_email, cmt_author_timestamp, cmt_added, cmt_removed, cmt_whitespace)
			VALUES ($1, $2, $3, 'a@example.org', $4, $5, $6, 0)`, repoID, hash, file, at, added, removed)
	}
	commit("h1", "a.go", time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC), 5, 1)
	commit("h1", "b.go", time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC), 3, 0)
	commit("h2", "a.go", time.Date(2025, 7, 1, 12, 0, 0, 0, time.UTC), 2, 0)
	// What a previous pass left, which the rebuild must remove.
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.dm_repo_annual (repo_id, email, year, added) VALUES ($1, 'stale@example.org', 1999, 99)`, repoID)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.dm_repo_group_annual (repo_group_id, email, year, added) VALUES ($1, 'stale@example.org', 1999, 99)`, group)

	groups, err := store.distinctRepoGroupIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, g := range groups {
		if g == group {
			seen++
		}
	}
	if seen != 1 || !sort.SliceIsSorted(groups, func(i, j int) bool { return groups[i] < groups[j] }) {
		t.Errorf("the groups that hold a repository: this test's group %d times in %v; want it once, the list in order", seen, groups)
	}

	if err := store.RefreshAllRepoAggregates(ctx, logger); err != nil {
		t.Fatal(err)
	}
	annual := func(table, keyCol string, key int64) string {
		return pathsText(t, store, `SELECT COALESCE(string_agg(year || ':' || email || ':' || affiliation || ':' || added || ':' || removed || ':' || files || ':' || patches, ',' ORDER BY year), '') FROM aveloxis_data.`+table+` WHERE `+keyCol+` = $1`, key)
	}
	const want = "2025:a@example.org:(Unknown):2:0:1:1,2026:a@example.org:(Unknown):8:1:2:1"
	if got := annual("dm_repo_annual", "repo_id", repoID); got != want {
		t.Errorf("the repository's annual aggregates: %q\nwant %q (the stale 1999 row gone)", got, want)
	}
	if got := annual("dm_repo_group_annual", "repo_group_id", group); got != want {
		t.Errorf("the group's annual aggregates: %q\nwant %q (the stale 1999 row gone)", got, want)
	}
	for table, key := range map[string]int64{"dm_repo_monthly": repoID, "dm_repo_weekly": repoID} {
		if n := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.`+table+` WHERE repo_id = $1`, key); n != 2 {
			t.Errorf("%d rows in %s for the repository, want 2 (one for each of its two months and weeks)", n, table)
		}
	}
	for _, table := range []string{"dm_repo_group_monthly", "dm_repo_group_weekly"} {
		if n := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.`+table+` WHERE repo_group_id = $1`, group); n != 2 {
			t.Errorf("%d rows in %s for the group, want 2", n, table)
		}
	}

	// One repository's group, rebuilt through the repository: the group's
	// rows are emptied by hand first, and come back.
	mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_data.dm_repo_group_annual WHERE repo_group_id = $1`, group)
	// Another group, with a repository that has a commit, whose aggregate
	// rows are replaced here by one marked row a table: the rebuild of ONE
	// repository's group neither deletes nor rebuilds another group's rows
	// (round 9: each of the six statements passed with its group predicate
	// gone, the test's group being the only one looked at).
	var otherGroup int64
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repo_groups (rg_name, rg_type) VALUES ($1, 'manual') RETURNING repo_group_id`, tag+"-other").Scan(&otherGroup); err != nil {
		t.Fatal(err)
	}
	otherRepo, _, _ := pathsRepo(t, store, "agg-other-group", "aveloxis_data.commits")
	t.Cleanup(func() {
		c := context.Background()
		for _, d := range dmGroup {
			cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.`+d+` WHERE repo_group_id = $1`, otherGroup)
		}
		cleanupExecRetry(c, store, `UPDATE aveloxis_data.repos SET repo_group_id = NULL WHERE repo_group_id = $1`, otherGroup)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.repo_groups WHERE repo_group_id = $1`, otherGroup)
	})
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET repo_group_id = $2 WHERE repo_id = $1`, otherRepo, otherGroup)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_email, cmt_author_timestamp, cmt_added, cmt_removed, cmt_whitespace)
		VALUES ($1, 'o1', 'o.go', 'other@example.org', '2026-04-04T12:00:00Z', 1, 0, 0)`, otherRepo)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.dm_repo_group_annual (repo_group_id, email, year, added) VALUES ($1, 'marked@example.org', 1998, 98)`, otherGroup)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.dm_repo_group_monthly (repo_group_id, email, year, month, added) VALUES ($1, 'marked@example.org', 1998, 1, 98)`, otherGroup)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.dm_repo_group_weekly (repo_group_id, email, year, week, added) VALUES ($1, 'marked@example.org', 1998, 1, 98)`, otherGroup)
	if err := store.RefreshRepoGroupAggregates(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	if got := annual("dm_repo_group_annual", "repo_group_id", group); got != want {
		t.Errorf("the group's annual aggregates after the rebuild through its repository: %q", got)
	}
	for _, d := range dmGroup {
		if got := pathsText(t, store, `SELECT COALESCE(string_agg(email || ':' || year || ':' || added, ',' ORDER BY email), '') FROM aveloxis_data.`+d+` WHERE repo_group_id = $1`, otherGroup); got != "marked@example.org:1998:98" {
			t.Errorf("the OTHER group's %s rows after the rebuild of this repository's group: %q; want its one marked row, untouched", d, got)
		}
	}
	// A repository in no group: nothing to do, and no error. One that does
	// not exist: an error, never "no group".
	if err := store.RefreshRepoGroupAggregates(ctx, ungrouped); err != nil {
		t.Errorf("a repository in no group: %v", err)
	}
	if err := store.RefreshRepoGroupAggregates(ctx, -6001); err == nil {
		t.Error("a repository that does not exist was read as one in no group")
	}
}

// The assignment backfill's dry run counts what it would fill, and the
// closed-by sweep lists closed GitHub issues with no closer.
func TestTheAssignmentDryRunCountsAndTheSweepLists(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID, owner, name := pathsRepo(t, store, "sweep", "aveloxis_data.issue_assignees", "aveloxis_data.issues")
	gitlab, _, _ := pathsRepo(t, store, "sweep-gl", "aveloxis_data.issues")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_id = 2 WHERE repo_id = $1`, gitlab)
	tag := pathsTag("sweep")
	who := seedR6Contributor(t, store, tag, "", nil)
	uid := pathsUserID()
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributor_identities (cntrb_id, platform_id, platform_user_id, login) VALUES ($1::uuid, 1, $2, 'x')`, who, uid)
	// The same NUMBER on another forge is another person: an identity of
	// platform 2 carrying the second assignee's id does not make that
	// GitHub assignee derivable (round 9: no identity of another platform
	// was seeded, so a count that ignored the platform passed).
	elsewhere := seedR6Contributor(t, store, tag+"-gl", "", nil)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributor_identities (cntrb_id, platform_id, platform_user_id, login) VALUES ($1::uuid, 2, $2, 'y')`, elsewhere, uid+1)

	before, err := store.BackfillAssignmentIdentities(ctx, 1000, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	issue := pathsIssue(t, store, repoID, 7)
	// Derivable: an assignee with no contributor whose platform id an
	// identity carries. Not derivable: one already linked, one with no
	// platform id, one whose id no identity carries.
	if err := store.UpsertIssueAssignees(ctx, issue, repoID, []model.IssueAssignee{
		{PlatformSrcID: uid, Origin: pathsOrigin}, {PlatformSrcID: uid + 1, Origin: pathsOrigin}}); err != nil {
		t.Fatal(err)
	}
	after, err := store.BackfillAssignmentIdentities(ctx, 1000, 0, true)
	if err != nil || after != before+1 {
		t.Errorf("the dry run counts %d -> %d (%v); want one more: the assignee an identity names", before, after, err)
	}
	if n := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.issue_assignees WHERE issue_id = $1 AND cntrb_id IS NOT NULL`, issue); n != 0 {
		t.Errorf("the dry run wrote: %d assignees have a contributor", n)
	}
	// Once that assignee is linked it is no longer one to derive.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.issue_assignees SET cntrb_id = $3::uuid WHERE issue_id = $1 AND platform_assignee_id = $2`, issue, uid, who)
	if linked, err := store.BackfillAssignmentIdentities(ctx, 1000, 0, true); err != nil || linked != before {
		t.Errorf("the dry run with the assignee already linked counts %d (%v); want %d, as before it was seeded", linked, err, before)
	}

	closedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, r := range []struct {
		repo   int64
		number int
		closed bool
		closer any
	}{{repoID, 3, true, nil}, {repoID, 2, true, nil}, {repoID, 4, false, nil}, {repoID, 5, true, who}, {gitlab, 1, true, nil}} {
		var at any
		if r.closed {
			at = closedAt
		}
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, closed_at, closed_by_id) VALUES ($1, $2, $3, $4, $5)`,
			r.repo, r.repo*1000+int64(r.number), r.number, at, r.closer)
	}
	all, err := store.IssuesNeedingClosedBySweep(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	var mine []int
	for i, s := range all {
		if i > 0 && (all[i-1].RepoID > s.RepoID || (all[i-1].RepoID == s.RepoID && all[i-1].Number > s.Number)) {
			t.Errorf("the sweep's list is not ordered by repository and number at row %d", i)
		}
		if s.RepoID == gitlab {
			t.Error("a GitLab issue is on the GitHub sweep's list")
		}
		if s.RepoID == repoID {
			mine = append(mine, s.Number)
			if s.Owner != owner || s.Repo != name || s.IssueID == 0 {
				t.Errorf("the listed issue %+v does not carry its repository", s)
			}
		}
	}
	if len(mine) != 2 || mine[0] != 2 || mine[1] != 3 {
		t.Errorf("this test's issues on the sweep's list: %v; want the two closed ones with no closer, #2 then #3", mine)
	}
	limited, err := store.IssuesNeedingClosedBySweep(ctx, 1)
	if err != nil || len(limited) != 1 || limited[0] != all[0] {
		t.Errorf("a limit of one: %+v (%v); want the first of the unlimited list", limited, err)
	}
}
