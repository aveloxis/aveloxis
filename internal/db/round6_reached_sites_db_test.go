// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// Review round 6: the write-policy judge sees a statement only when a test
// of this package sends it. These are writers of contributors and their
// identities (tables with registered write policies) and a few one-statement
// writers that no test of this package reached; other packages' tests drive
// them, where the judge is not installed. Each is run here, so that what it
// sends is judged.

func TestTheContributorResolverSendsItsStatements(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	uid := time.Now().UnixNano()%1_000_000_000 + 7_000_000_000
	login := fmt.Sprintf("r6-resolver-%d", uid)
	t.Cleanup(func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_identities WHERE login = $1`, login)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
	})
	// A new identity, with neither node id nor type.
	first, err := NewContributorResolver(store).Resolve(ctx, 1, uid, login, "R Six", "", "", "", "", "")
	if err != nil || first == "" {
		t.Fatalf("resolving a new contributor: %q, %v", first, err)
	}
	// A fresh resolver (no cache) meets the stored identity and fills the
	// two columns it now has values for: the fill the policy's Exceptions
	// name (FillEmptyOnly, written with NULLIF).
	second, err := NewContributorResolver(store).Resolve(ctx, 1, uid, login, "R Six", "", "", "", "MDQ6VXNlcjE=", "User")
	if err != nil || second != first {
		t.Fatalf("resolving it again: %q (was %q), %v", second, first, err)
	}
	var node, typ string
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(node_id, ''), COALESCE(user_type, '') FROM aveloxis_data.contributor_identities WHERE platform_id = 1 AND platform_user_id = $1`, uid).Scan(&node, &typ); err != nil {
		t.Fatal(err)
	}
	if node != "MDQ6VXNlcjE=" || typ != "User" {
		t.Errorf("the identity's node id and type after the fill: %q, %q", node, typ)
	}
	// And a third time with other values: a filled column keeps its value.
	if _, err := NewContributorResolver(store).Resolve(ctx, 1, uid, login, "R Six", "", "", "", "OTHER", "Bot"); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(node_id, ''), COALESCE(user_type, '') FROM aveloxis_data.contributor_identities WHERE platform_id = 1 AND platform_user_id = $1`, uid).Scan(&node, &typ); err != nil {
		t.Fatal(err)
	}
	if node != "MDQ6VXNlcjE=" || typ != "User" {
		t.Errorf("a filled node id or type was overwritten: %q, %q", node, typ)
	}
}

// One-statement writers: each changes the row it names, as it says, and no
// other (round 7: this test sent each on a row that did not exist and
// asserted only that no error came back).
func TestOneStatementWritersChangeTheirRowAndNoOther(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tag := fmt.Sprintf("r7-writers-%d", time.Now().UnixNano())
	mine := seedR6Contributor(t, store, tag, "", nil)
	other := seedR6Contributor(t, store, tag+"-other", "", nil)
	repoID := seedRepoForDeps(t, store, ctx, "r7-writers", tag)
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.issues WHERE repo_id = $1`, repoID)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.commits WHERE repo_id = $1`, repoID)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
	})
	text := func(sql string, args ...any) string {
		t.Helper()
		var s *string
		if err := store.pool.QueryRow(ctx, sql, args...).Scan(&s); err != nil {
			t.Fatal(err)
		}
		if s == nil {
			return ""
		}
		return *s
	}
	stamps := func() string {
		return text(`SELECT string_agg(cntrb_login || ':' || (cntrb_last_enriched_at IS NOT NULL) || ':' || (cntrb_last_search_attempted_at IS NOT NULL), ',' ORDER BY cntrb_login)
			FROM aveloxis_data.contributors WHERE cntrb_id = ANY($1::uuid[])`, []string{mine, other})
	}
	// A login that differs from the named one only by case is another
	// contributor's (cntrb_login is unique as written): the mark is by the
	// login as given (round 8: a case-folding match passed).
	variant := seedR6Contributor(t, store, strings.ToUpper(tag), "", nil)
	enriched := func(id string) string {
		return text(`SELECT (cntrb_last_enriched_at IS NOT NULL)::text FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id)
	}
	if err := store.MarkContributorEnriched(ctx, tag); err != nil {
		t.Fatal(err)
	}
	if got := stamps(); got != tag+":true:false,"+tag+"-other:false:false" {
		t.Errorf("after the enrichment mark: %q; want it on the named login alone", got)
	}
	if got := enriched(variant); got != "false" {
		t.Errorf("the contributor whose login differs only by case was marked enriched (%s)", got)
	}
	if err := store.MarkContributorSearchAttempted(ctx, other); err != nil {
		t.Fatal(err)
	}
	if got := stamps(); got != tag+":true:false,"+tag+"-other:false:true" {
		t.Errorf("after the search mark: %q; want it on the named contributor alone", got)
	}

	// The closer of an issue: set where there is none, and never replaced.
	var open, closed int64
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number) VALUES ($1, 1, 1) RETURNING issue_id`, repoID).Scan(&open); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, closed_by_id) VALUES ($1, 2, 2, $2::uuid) RETURNING issue_id`, repoID, other).Scan(&closed); err != nil {
		t.Fatal(err)
	}
	// A third issue, with no closer, that is NOT named: it keeps none
	// (round 9: every issue seeded was named, so a statement that stamped
	// every issue without a closer passed).
	var untouched int64
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number) VALUES ($1, 3, 3) RETURNING issue_id`, repoID).Scan(&untouched); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{open, closed} {
		if err := store.SetIssueClosedBy(ctx, id, mine); err != nil {
			t.Fatal(err)
		}
	}
	if got := text(`SELECT string_agg(COALESCE(closed_by_id::text, 'none'), ',' ORDER BY issue_number) FROM aveloxis_data.issues WHERE repo_id = $1`, repoID); got != mine+","+other+",none" {
		t.Errorf("the issues' closers are %q; want the one without a closer given it, the other's kept, and the issue not named left without one", got)
	}

	// A commit's author login: set on every file row of the commit that
	// has none, and on no other commit.
	for _, c := range [][3]string{{"h1", "a.go", ""}, {"h1", "b.go", "already"}, {"h2", "a.go", ""}} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_platform_username) VALUES ($1, $2, $3, $4)`, repoID, c[0], c[1], c[2])
	}
	// The same hash in another repository (a fork carries its parent's
	// commits): that row is the other repository's.
	fork, _, _ := pathsRepo(t, store, "r9-commit-neighbour", "aveloxis_data.commits")
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_platform_username) VALUES ($1, 'h1', 'a.go', '')`, fork)
	if err := store.SetCommitAuthorLogin(ctx, repoID, "h1", "octo"); err != nil {
		t.Fatal(err)
	}
	if got := text(`SELECT COALESCE(cmt_author_platform_username, '') FROM aveloxis_data.commits WHERE repo_id = $1`, fork); got != "" {
		t.Errorf("the same hash in another repository was given the author login %q", got)
	}
	if got := text(`SELECT string_agg(cmt_commit_hash || '/' || cmt_filename || '=' || COALESCE(cmt_author_platform_username, ''), ',' ORDER BY cmt_commit_hash, cmt_filename) FROM aveloxis_data.commits WHERE repo_id = $1`, repoID); got != "h1/a.go=octo,h1/b.go=already,h2/a.go=" {
		t.Errorf("the commits' author logins are %q", got)
	}

	// The full-collection flag: an error when the repository has no queue
	// row (nothing was changed), and the flag set and cleared when it has.
	if err := store.SetForceFullCollect(ctx, repoID, true); err == nil {
		t.Error("SetForceFullCollect on a repository with no queue row reported success")
	}
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at) VALUES ($1, 100, 'queued', NOW())`, repoID)
	// Another repository's queue row: its flag is its own (round 9: there
	// was one queue row, so a statement that flagged every row passed).
	neighbour, _, _ := pathsRepo(t, store, "r9-flag-neighbour")
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at) VALUES ($1, 100, 'queued', NOW())`, neighbour)
	for _, want := range []bool{true, false} {
		if err := store.SetForceFullCollect(ctx, repoID, want); err != nil {
			t.Fatal(err)
		}
		if got := text(`SELECT force_full_collect::text FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID); got != fmt.Sprint(want) {
			t.Errorf("force_full_collect is %s after it was set to %v", got, want)
		}
		if got := text(`SELECT COALESCE(force_full_collect, FALSE)::text FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, neighbour); got != "false" {
			t.Errorf("another repository's force_full_collect is %s after this one's was set to %v", got, want)
		}
	}
}

// Round 6, second stage: the writers of registered tables (contributors,
// contributor identities, messages) that no DB-tier test of the whole suite
// reached. Each is driven down the branch that sends its statements.

func seedR6Contributor(t *testing.T, store *PostgresStore, login, email string, ghUserID any) string {
	t.Helper()
	ctx := context.Background()
	var id string
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_login, cntrb_email, gh_user_id, cntrb_deleted)
		VALUES ($1, $2, $3, 0) RETURNING cntrb_id::text`, login, email, ghUserID).Scan(&id); err != nil {
		t.Fatalf("seed contributor %s: %v", login, err)
	}
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors_aliases WHERE cntrb_id = $1::uuid`, id)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id = $1::uuid`, id)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id = $1::uuid`, id)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id)
	})
	return id
}

func r6Deleted(t *testing.T, store *PostgresStore, id string) int {
	t.Helper()
	var d int
	if err := store.pool.QueryRow(context.Background(), `SELECT COALESCE(cntrb_deleted, 0) FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id).Scan(&d); err != nil {
		t.Fatal(err)
	}
	return d
}

// Linking a contributor to a GitHub user that another contributor row
// already carries: the two are merged (the loser's fields fill the
// winner's empty ones, the loser is soft-deleted) and the winner is linked
// and given its identity row.
func TestLinkingAContributorMergesTheDuplicateAndLinksTheWinner(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	uid := time.Now().UnixNano()%1_000_000_000 + 8_000_000_000
	login := fmt.Sprintf("r6-link-%d", uid)
	target := seedR6Contributor(t, store, login+"-a", "", nil)
	other := seedR6Contributor(t, store, login+"-b", login+"@example.org", uid)
	r8MergeFields(t, store, target, other)
	bystanders := r9Bystanders(t, store, login)
	if err := store.LinkContributorToGitHubUser(ctx, target, login, uid); err != nil {
		t.Fatal(err)
	}
	bystanders()
	r8MergedAsBestOf(t, store, target, other)
	if r6Deleted(t, store, target)+r6Deleted(t, store, other) != 1 {
		t.Errorf("after the link, exactly one of the two rows is soft-deleted: target %d, other %d", r6Deleted(t, store, target), r6Deleted(t, store, other))
	}
	var identities int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.contributor_identities WHERE platform_id = 1 AND platform_user_id = $1`, uid).Scan(&identities); err != nil {
		t.Fatal(err)
	}
	if identities != 1 {
		t.Errorf("%d identity rows for the linked GitHub user, want 1", identities)
	}
}

// The same merge on a rename, which also records the new login.
func TestRenamingAContributorMergesTheDuplicateAndRecordsTheLogin(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	uid := time.Now().UnixNano()%1_000_000_000 + 8_500_000_000
	login := fmt.Sprintf("r6-rename-%d", uid)
	target := seedR6Contributor(t, store, login+"-old", "", nil)
	other := seedR6Contributor(t, store, login+"-dup", login+"@example.org", uid)
	r8MergeFields(t, store, target, other)
	bystanders := r9Bystanders(t, store, login)
	if err := store.RenameContributorGhLogin(ctx, target, login+"-new", uid); err != nil {
		t.Fatal(err)
	}
	bystanders()
	r8MergedAsBestOf(t, store, target, other)
	if r6Deleted(t, store, target)+r6Deleted(t, store, other) != 1 {
		t.Errorf("after the rename, exactly one of the two rows is soft-deleted: target %d, other %d", r6Deleted(t, store, target), r6Deleted(t, store, other))
	}
	var gh string
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(gh_login, '') FROM aveloxis_data.contributors WHERE cntrb_id IN ($1::uuid, $2::uuid) AND COALESCE(cntrb_deleted, 0) = 0`, target, other).Scan(&gh); err != nil {
		t.Fatal(err)
	}
	if gh != login+"-new" {
		t.Errorf("the surviving row's gh_login is %q, want the new login", gh)
	}
}

// The commit resolver meets a login that a contributor row already has
// under another id: that row is updated, and its id is the one returned.
func TestUpsertContributorFullUpdatesTheRowThatHasTheLogin(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	uid := time.Now().UnixNano()%1_000_000_000 + 9_000_000_000
	login := fmt.Sprintf("r6-full-%d", uid)
	// A contributor stored before the one with the login, and (inside
	// r9Bystanders) one after: the row found is the one WITH the login,
	// wherever it lies in the table.
	seedR6Contributor(t, store, login+"-earlier", "", nil)
	have := seedR6Contributor(t, store, login, "", nil)
	bystanders := r9Bystanders(t, store, login)
	created, id, err := store.UpsertContributorFull(ctx, "0100000a-0000-0000-0000-000000000006", login, uid, login+"@example.org")
	if err != nil {
		t.Fatal(err)
	}
	bystanders()
	if created || id != have {
		t.Errorf("created %v, id %s; want the existing row %s, not created", created, id, have)
	}
	var gh int64
	var canonical string
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(gh_user_id, 0), COALESCE(cntrb_canonical, '') FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, have).Scan(&gh, &canonical); err != nil {
		t.Fatal(err)
	}
	if gh != uid || canonical != login+"@example.org" {
		t.Errorf("the row after the update: gh_user_id %d, canonical %q", gh, canonical)
	}
	// What the row has is kept (round 8: nothing asserted it). The same
	// login met again under another id, with another GitHub user id and
	// another address: gh_user_id and cntrb_canonical are filled when
	// empty and never replaced (SR-6: an identity is not rewritten by a
	// later, possibly mistaken, resolution).
	if _, id, err := store.UpsertContributorFull(ctx, "0100000a-0000-0000-0000-000000000007", login, uid+1, "another@example.org"); err != nil || id != have {
		t.Fatalf("the second meeting: id %s, %v", id, err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(gh_user_id, 0), COALESCE(cntrb_canonical, '') FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, have).Scan(&gh, &canonical); err != nil {
		t.Fatal(err)
	}
	if gh != uid || canonical != login+"@example.org" {
		t.Errorf("after a second meeting with other values: gh_user_id %d (want the established %d), canonical %q (want the established one)", gh, uid, canonical)
	}
	// And met under its OWN id (the row found by id): that row's update is
	// by id too.
	if _, id, err := store.UpsertContributorFull(ctx, have, login, uid, login+"@example.org"); err != nil || id != have {
		t.Fatalf("met under its own id: id %s, %v", id, err)
	}
	bystanders()
}

// The conflict arm of UpsertContributorFull, taken with two sessions: one
// holds an uncommitted row under the contributor id, so the function's two
// probes see nothing and its INSERT waits on that row; the first session
// commits, and the INSERT takes ON CONFLICT (cntrb_id) against a stored
// GitHub user id that differs from the incoming one. The stored one stands
// (SR-6; review round 9: no test took this arm, and the arm reverted to
// "the incoming id replaces it" passed the suite).
func TestUpsertContributorFullConflictArmKeepsTheStoredGitHubID(t *testing.T) {
	store := openTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	n := time.Now().UnixNano()
	id := fmt.Sprintf("01000000-0000-4000-8000-%012x", n&0xffffffffffff)
	login := fmt.Sprintf("r9-ucf-%d", n)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id)
	})
	first, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Rollback(context.Background()) })
	if _, err := first.Exec(ctx, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_user_id) VALUES ($1::uuid, $2, 111)`, id, login+"-first"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := store.UpsertContributorFull(ctx, id, login, 222, "r9@example.com")
		done <- err
	}()
	// Until the function's INSERT waits on the first session's row.
	for waiting := false; !waiting; time.Sleep(10 * time.Millisecond) {
		if err := store.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%aveloxis-commit-resolver%' AND pid <> pg_backend_pid())`).Scan(&waiting); err != nil {
			t.Fatalf("waiting for the INSERT to block on the first session's row: %v", err)
		}
	}
	if err := first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var gh int64
	var cntrbLogin, ghLogin string
	if err := store.pool.QueryRow(ctx, `SELECT gh_user_id, cntrb_login, gh_login FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id).Scan(&gh, &cntrbLogin, &ghLogin); err != nil {
		t.Fatal(err)
	}
	if gh != 111 || cntrbLogin != login+"-first" || ghLogin != login {
		t.Errorf("after the conflict arm: gh_user_id %d, cntrb_login %q, gh_login %q; want the stored 111 and the first login kept, and gh_login %q (which shows the arm ran)", gh, cntrbLogin, ghLogin, login)
	}
}

// r9Bystanders seeds two contributors that the call under test does not
// name, one with every merge field empty and one with them filled, and
// returns a check that both are as they were (round 9: a statement whose
// WHERE had lost its key passed these tests, there being no row but the
// named ones for it to change).
func r9Bystanders(t *testing.T, store *PostgresStore, tag string) func() {
	t.Helper()
	ctx := context.Background()
	ids := []string{seedR6Contributor(t, store, tag+"-bystander-empty", "", nil), seedR6Contributor(t, store, tag+"-bystander-full", tag+"-bystander@example.org", nil)}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.contributors SET gh_login = 'bystander', cntrb_canonical = 'bystander@example.org', cntrb_company = 'bystander co', cntrb_location = 'bystander place', cntrb_full_name = 'By Stander' WHERE cntrb_id = $1::uuid`, ids[1])
	state := func() string {
		return pathsText(t, store, `SELECT string_agg(concat_ws('|', cntrb_login, gh_login, gh_user_id, cntrb_canonical, cntrb_company, cntrb_location, cntrb_full_name, cntrb_deleted, cntrb_last_search_attempted_at IS NULL, data_collection_date), ' ; ' ORDER BY cntrb_login)
			FROM aveloxis_data.contributors WHERE cntrb_id = ANY($1::uuid[])`, ids)
	}
	before := state()
	return func() {
		t.Helper()
		if after := state(); after != before {
			t.Errorf("contributors the call did not name were changed:\n before %s\n after  %s", before, after)
		}
	}
}

// r8MergeFields gives the two rows of a merge test what tells a best-of
// merge from a copy: the target (made the older, so it is the winner by
// pickMergeWinner's rule when neither has the deterministic id) has a
// company and no location; the other has both.
func r8MergeFields(t *testing.T, store *PostgresStore, target, other string) {
	t.Helper()
	ctx := context.Background()
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.contributors SET cntrb_company = 'the target company', cntrb_location = '', data_collection_date = NOW() - INTERVAL '2 days' WHERE cntrb_id = $1::uuid`, target)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.contributors SET cntrb_company = 'the other company', cntrb_location = 'the other location', data_collection_date = NOW() - INTERVAL '1 day' WHERE cntrb_id = $1::uuid`, other)
}

// r8MergedAsBestOf: the surviving row keeps what it had (its company) and
// takes from the loser only what it lacked (the location).
func r8MergedAsBestOf(t *testing.T, store *PostgresStore, target, other string) {
	t.Helper()
	if r6Deleted(t, store, target) != 0 || r6Deleted(t, store, other) != 1 {
		t.Fatalf("the older row (the target) should have won the merge: target deleted %d, other deleted %d", r6Deleted(t, store, target), r6Deleted(t, store, other))
	}
	var company, location string
	if err := store.pool.QueryRow(context.Background(), `SELECT COALESCE(cntrb_company, ''), COALESCE(cntrb_location, '') FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, target).Scan(&company, &location); err != nil {
		t.Fatal(err)
	}
	if company != "the target company" {
		t.Errorf("the winner's company is %q after the merge; what the winner had is kept", company)
	}
	if location != "the other location" {
		t.Errorf("the winner's location is %q after the merge; what the winner lacked is taken from the loser", location)
	}
}

// A GitLab identity in a contributor batch fills the contributor's GitLab
// columns (the GitHub arm is reached by every other batch test).
func TestAContributorBatchWithAGitLabIdentityFillsTheGitLabColumns(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	uid := time.Now().UnixNano()%1_000_000_000 + 9_500_000_000
	login := fmt.Sprintf("r6-gitlab-%d", uid)
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributor_identities WHERE login = $1`, login)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id IN (SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = $1)`, login)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
	})
	if err := store.UpsertContributorBatch(ctx, []model.Contributor{{
		ID: PlatformUUID(int(model.PlatformGitLab), uid), Login: login,
		Identities: []model.ContributorIdentity{{Platform: model.PlatformGitLab, UserID: uid, Login: login, Type: "User"}},
	}}); err != nil {
		t.Fatal(err)
	}
	var glID int64
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(gl_id, 0) FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login).Scan(&glID); err != nil {
		t.Fatal(err)
	}
	if glID != uid {
		t.Errorf("gl_id = %d, want %d", glID, uid)
	}
	// What is established is kept (round 8: nothing asserted it). The
	// identity's address was given once; a later batch that brings none
	// does not blank it. And a contributor's gl_id, once set, is not
	// replaced by what a later batch carries.
	identity := func(email string) model.Contributor {
		return model.Contributor{ID: PlatformUUID(int(model.PlatformGitLab), uid), Login: login,
			Identities: []model.ContributorIdentity{{Platform: model.PlatformGitLab, UserID: uid, Login: login, Type: "User", Email: email}}}
	}
	if err := store.UpsertContributorBatch(ctx, []model.Contributor{identity("first@example.org")}); err != nil {
		t.Fatal(err)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.contributors SET gl_id = 4242 WHERE cntrb_login = $1`, login)
	if err := store.UpsertContributorBatch(ctx, []model.Contributor{identity("")}); err != nil {
		t.Fatal(err)
	}
	var email string
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(email, '') FROM aveloxis_data.contributor_identities WHERE platform_id = $1 AND platform_user_id = $2`, int16(model.PlatformGitLab), uid).Scan(&email); err != nil {
		t.Fatal(err)
	}
	if email != "first@example.org" {
		t.Errorf("the identity's address is %q after a batch that brought none; want the one it had", email)
	}
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(gl_id, 0) FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login).Scan(&glID); err != nil {
		t.Fatal(err)
	}
	if glID != 4242 {
		t.Errorf("gl_id = %d after a later batch; want the established 4242", glID)
	}
}

// A review with a body, and a batch of review comments: each stores its
// text as a message (a table with registered write policies).
func TestAReviewBodyAndReviewCommentsAreStoredAsMessages(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	n := time.Now().UnixNano() % 1_000_000_000
	repoID := seedRepoForDeps(t, store, ctx, "r6-reviews", fmt.Sprintf("repo-%d", n))
	t.Cleanup(func() {
		c := context.Background()
		for _, q := range []string{
			`DELETE FROM aveloxis_data.pull_request_review_message_ref WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.review_comments WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.pull_request_reviews WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.messages WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.pull_requests WHERE repo_id = $1`,
		} {
			cleanupExecRetry(c, store, q, repoID)
		}
	})
	prID, err := store.UpsertPullRequest(ctx, &model.PullRequest{RepoID: repoID, PlatformSrcID: 9_000_000 + n, Number: 1, Title: "r6", State: "open",
		CreatedAt: time.Now(), UpdatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPRReview(ctx, &model.PullRequestReview{PRID: prID, RepoID: repoID, PlatformID: model.PlatformGitHub,
		PlatformReviewID: 9_100_000 + n, State: "COMMENTED", Body: "a review body", SubmittedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertReviewCommentBatch(ctx, []platform.ReviewCommentWithRef{{
		Message: model.Message{RepoID: repoID, PlatformMsgID: 9_200_000 + n, PlatformID: model.PlatformGitHub, Text: "an inline comment", Timestamp: time.Now()},
		Comment: model.ReviewComment{RepoID: repoID, PlatformSrcID: 9_200_000 + n, Path: "a.go", UpdatedAt: time.Now()},
	}}); err != nil {
		t.Fatal(err)
	}
	var messages int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.messages WHERE repo_id = $1`, repoID).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if messages != 2 {
		t.Errorf("%d messages stored for the review body and the inline comment, want 2", messages)
	}
}
