// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

func TestOrganisationGroupsAreListedAndAGroupIsFoundByNameWhateverItsType(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tag := pathsTag("grp")
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_groups WHERE rg_name LIKE $1`, tag+"%")
	})
	gh, err := store.UpsertRepoGroup(ctx, tag+"-b", "github_org", "https://github.com/"+tag)
	if err != nil {
		t.Fatal(err)
	}
	gl, err := store.UpsertRepoGroup(ctx, tag+"-a", "gitlab_group", "https://gitlab.com/"+tag)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertRepoGroup(ctx, tag+"-c", "manual", ""); err != nil {
		t.Fatal(err)
	}
	// The same name under another type: the name is the identity, so the
	// stored group is returned and no second one is made.
	again, err := store.UpsertRepoGroup(ctx, tag+"-b", "gitlab_group", "https://gitlab.com/other")
	if err != nil || again != gh {
		t.Errorf("the group's name under another type gave %d (%v), want the stored group %d", again, err, gh)
	}
	if n := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.repo_groups WHERE rg_name = $1`, tag+"-b"); n != 1 {
		t.Errorf("%d groups of that name, want 1", n)
	}
	if got := pathsText(t, store, `SELECT rg_type || '|' || rg_website FROM aveloxis_data.repo_groups WHERE repo_group_id = $1`, gh); got != "github_org|https://github.com/"+tag {
		t.Errorf("the stored group was changed by the second call: %q", got)
	}
	if _, err := store.UpsertRepoGroup(ctx, tag+"-d", "github_org", "https://user:secret@github.com/x"); err == nil {
		t.Error("a group whose website carries a credential was stored")
	}

	groups, err := store.GetOrgRepoGroups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var mine []OrgGroup
	for i, g := range groups {
		if i > 0 && groups[i-1].Name > g.Name {
			t.Errorf("the groups are not listed by name: %q before %q", groups[i-1].Name, g.Name)
		}
		if g.Type != "github_org" && g.Type != "gitlab_group" {
			t.Errorf("a group of type %q is listed among the organisations", g.Type)
		}
		if strings.HasPrefix(g.Name, tag) {
			mine = append(mine, g)
		}
	}
	want := []OrgGroup{{ID: gl, Name: tag + "-a", Type: "gitlab_group", Website: "https://gitlab.com/" + tag}, {ID: gh, Name: tag + "-b", Type: "github_org", Website: "https://github.com/" + tag}}
	if !reflect.DeepEqual(mine, want) {
		t.Errorf("this test's organisation groups: %+v\nwant %+v", mine, want)
	}
}

func TestAUsersStarsAndTheirGitHubLoginAreRead(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tag := pathsTag("user")
	user := tokenTestUser(t, store, ctx, tag)
	other := tokenTestUser(t, store, ctx, tag+"-other")
	a, _, _ := pathsRepo(t, store, "star-a", "aveloxis_ops.user_repo_stars")
	b, _, _ := pathsRepo(t, store, "star-b", "aveloxis_ops.user_repo_stars")
	c, _, _ := pathsRepo(t, store, "star-c", "aveloxis_ops.user_repo_stars")
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.user_repo_stars (user_id, repo_id) VALUES ($1, $3), ($1, $4), ($2, $5)`, user, other, a, b, c)
	stars, err := store.GetUserStarredRepoIDs(ctx, user)
	if err != nil || !reflect.DeepEqual(stars, map[int64]bool{a: true, b: true}) {
		t.Errorf("the user's stars: %v (%v), want repositories %d and %d", stars, err, a, b)
	}
	if none, err := store.GetUserStarredRepoIDs(ctx, -5); err != nil || len(none) != 0 {
		t.Errorf("a user with no star: %v (%v)", none, err)
	}

	mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.users SET gh_login = $2 WHERE user_id = $1`, user, tag+"-gh")
	if id, err := store.GetUserIDByGHLogin(ctx, tag+"-gh"); err != nil || id != user {
		t.Errorf("the user by GitHub login: %d (%v), want %d", id, err, user)
	}
	if _, err := store.GetUserIDByGHLogin(ctx, tag+"-nobody"); err == nil {
		t.Error("a GitHub login no user has was found")
	}
}

func TestAFoundationMembershipIsUpsertedByItsKey(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tag := pathsTag("found")
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.foundation_membership WHERE foundation = $1`, tag)
	})
	repo := "https://github.com/" + tag + "/r"
	if err := store.UpsertFoundationMembership(ctx, tag, "sandbox", "proj", "https://old.example.org", repo); err != nil {
		t.Fatal(err)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.foundation_membership SET imported_at = NOW() - INTERVAL '2 days' WHERE foundation = $1`, tag)
	if err := store.UpsertFoundationMembership(ctx, tag, "graduated", "proj", "https://new.example.org", repo); err != nil {
		t.Fatal(err)
	}
	if got := pathsText(t, store, `SELECT COUNT(*) || '|' || MIN(status) || '|' || MIN(homepage_url) || '|' || (MIN(imported_at) > NOW() - INTERVAL '1 hour') FROM aveloxis_ops.foundation_membership WHERE foundation = $1`, tag); got != "1|graduated|https://new.example.org|true" {
		t.Errorf("the membership after two writes: %q; want one row with the new status and homepage and a fresh import time", got)
	}
	// Another repository of the same project is another row.
	if err := store.UpsertFoundationMembership(ctx, tag, "graduated", "proj", "", repo+"2"); err != nil {
		t.Fatal(err)
	}
	if n := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_ops.foundation_membership WHERE foundation = $1`, tag); n != 2 {
		t.Errorf("%d membership rows, want 2", n)
	}
	for _, bad := range [][2]string{{"https://u:p@example.org", repo + "3"}, {"", "https://u:p@github.com/x/y"}} {
		if err := store.UpsertFoundationMembership(ctx, tag, "sandbox", "cred", bad[0], bad[1]); err == nil {
			t.Errorf("a membership with a credential in a URL was stored (%v)", bad)
		}
	}
	if n := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_ops.foundation_membership WHERE foundation = $1 AND project_name = 'cred'`, tag); n != 0 {
		t.Errorf("%d rows written for the refused memberships", n)
	}
}

func TestTrackerProjectsAreDerivedFromSyntheticIssues(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	a, _, _ := pathsRepo(t, store, "jira-a", "aveloxis_data.issues")
	b, _, _ := pathsRepo(t, store, "jira-b", "aveloxis_data.issues")
	one, two := strings.ToUpper(strings.ReplaceAll(pathsTag("ja"), "-", "")), strings.ToUpper(strings.ReplaceAll(pathsTag("jb"), "-", ""))
	add := func(repo int64, n int, key, source string) {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, external_key, data_source) VALUES ($1, $2, $3, $4, $5)`,
			repo, repo*1000+int64(n), n, key, source)
	}
	add(a, 1, one+"-1", "JIRA")
	add(a, 2, one+"-2", "JIRA")
	add(b, 3, one+"-3", "JIRA")
	add(b, 4, two+"-9", "JIRA")
	add(b, 5, "", "JIRA")
	add(b, 6, "NOTJIRA-1", "GitHub API")
	got, err := store.DeriveJiraProjectsFromSynthetics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]JiraProjectCandidate{}
	pos := map[string]int{}
	for i, c := range got {
		byKey[c.ProjectKey] = c
		pos[c.ProjectKey] = i
		if c.ProjectKey == "" || c.ProjectKey == "NOTJIRA" {
			t.Errorf("a project %q was derived from an issue with no key, or from one that is not the tracker's", c.ProjectKey)
		}
	}
	lower := a
	if b < a {
		lower = b
	}
	if byKey[one] != (JiraProjectCandidate{ProjectKey: one, RepoID: lower, RepoCount: 2}) {
		t.Errorf("the project with three issues over two repositories: %+v", byKey[one])
	}
	if byKey[two] != (JiraProjectCandidate{ProjectKey: two, RepoID: b, RepoCount: 1}) {
		t.Errorf("the project with one issue: %+v", byKey[two])
	}
	if pos[one] > pos[two] {
		t.Errorf("the project with more issues is listed after the one with fewer (%d, %d)", pos[one], pos[two])
	}
}

func TestMailingListCoverageAndResolvableSendersAreCounted(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID, _, _ := pathsRepo(t, store, "ml", "aveloxis_data.email_message", "aveloxis_data.messages", "aveloxis_data.issues", "aveloxis_data.pull_requests")
	tag := pathsTag("ml")
	issue := pathsIssue(t, store, repoID, 1)
	pr := pathsPR(t, store, repoID, 1, "open")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.issues SET external_key = $2 WHERE issue_id = $1`, issue, "ZZ-1")
	// A second issue with no external key: it is not one of the counted.
	pathsIssue(t, store, repoID, 2)
	before, err := store.MailingListCoverage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	resolvableBefore, err := store.CountResolvableMailingListSenders(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Three contributors: one alone on its address, two sharing an address
	// (ambiguous), one reached by an alias.
	alone := seedR6Contributor(t, store, tag+"-alone", tag+"-alone@example.org", nil)
	seedR6Contributor(t, store, tag+"-twin1", tag+"-twin@example.org", nil)
	seedR6Contributor(t, store, tag+"-twin2", tag+"-twin@example.org", nil)
	aliased := seedR6Contributor(t, store, tag+"-aliased", "", nil)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors_aliases (cntrb_id, canonical_email, alias_email) VALUES ($1::uuid, $2, $2)`, aliased, tag+"-alias@example.org")
	_ = alone
	mail := func(n int, sender string, set string, args ...any) {
		header := fmt.Sprintf("<%s-%d@example.org>", tag, n)
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, node_id, msg_text) VALUES ($1, $2, 6, $3, 'body')`, repoID, repoID*100+int64(n), header)
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.email_message (repo_id, platform_id, ml_system, message_id_header, sender_email) VALUES ($1, 6, $2, $3, $4)`, repoID, tag, header, sender)
		if set != "" {
			mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.email_message SET `+set+` WHERE message_id_header = $1`, append([]any{header}, args...)...)
		}
	}
	mail(1, tag+"-alone@example.org", "linked_issue_id = $2, thread_root_id = 'root'", issue)
	mail(2, tag+"-twin@example.org", "linked_pull_request_id = $2, is_mirror = TRUE", pr)
	mail(3, tag+"-alias@example.org", "thread_root_id = NULL")
	mail(4, tag+"-nobody@example.org", "linked_pull_request_id = $2", pr)
	mail(5, "", "")
	// An automated sender whose address exactly one contributor has: it is
	// never resolved to a contributor (round 8: none was seeded, and a count
	// that included automation senders passed).
	seedR6Contributor(t, store, tag+"-bot", tag+"[bot]@example.org", nil)
	mail(6, tag+"[bot]@example.org", "")

	after, err := store.MailingListCoverage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.BySystem[tag] != 6 {
		t.Errorf("%d messages counted for this test's list system, want 6", after.BySystem[tag])
	}
	if d := after.BridgedToIssue - before.BridgedToIssue; d != 1 {
		t.Errorf("messages bridged to an issue: %+d, want +1", d)
	}
	if d := after.BridgedToPR - before.BridgedToPR; d != 2 {
		t.Errorf("messages bridged to a pull request: %+d, want +2 (one to an issue)", d)
	}
	if d := after.MirrorLinked - before.MirrorLinked; d != 1 {
		t.Errorf("mirror messages that are linked: %+d, want +1", d)
	}
	if d := after.ThreadRooted - before.ThreadRooted; d != 5 {
		t.Errorf("messages with a thread root recorded: %+d, want +5 (the third's is NULL)", d)
	}
	if d := after.ExternalKeyIssues - before.ExternalKeyIssues; d != 0 {
		t.Errorf("issues with an external key: %+d since the first reading, want 0 (the one of this test was there)", d)
	}
	if want := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.issues WHERE external_key <> ''`); after.ExternalKeyIssues != want || want < 1 || want >= pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.issues`) {
		t.Errorf("issues with an external key: %d, and the table holds %d (this test's among them)", after.ExternalKeyIssues, want)
	}

	resolvable, err := store.CountResolvableMailingListSenders(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d := resolvable - resolvableBefore; d != 2 {
		t.Errorf("resolvable senders: %+d, want +2: the address one contributor has, and the aliased one; not the address two share, the unknown one, the empty one, or the automated sender's", d)
	}
}

func TestAThreadWithNoIssueIsMarkedMailingListOnly(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID, _, _ := pathsRepo(t, store, "thread", "aveloxis_data.email_message")
	tag := pathsTag("thr")
	header := "<" + tag + "@example.org>"
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.email_message (repo_id, platform_id, message_id_header, thread_root_id) VALUES ($1, 6, $2, $3)`, repoID, header, "<root-"+tag+"@example.org>")
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.email_message (repo_id, platform_id, message_id_header, thread_root_id, is_mirror) VALUES ($1, 6, $2, 'x', TRUE)`, repoID, header+"-mirror")
	n, err := store.BackfillThreadInheritance(ctx, 1_000_000)
	if err != nil || n < 1 {
		t.Fatalf("the thread backfill: %d rows (%v)", n, err)
	}
	if got := pathsText(t, store, `SELECT projected_kind FROM aveloxis_data.email_message WHERE message_id_header = $1`, header); got != "mailing_list_only" {
		t.Errorf("a message whose thread has no issue is marked %q, want mailing_list_only", got)
	}
	if got := pathsText(t, store, `SELECT COALESCE(projected_kind, '') FROM aveloxis_data.email_message WHERE message_id_header = $1`, header+"-mirror"); got != "" {
		t.Errorf("a mirror message was marked %q: the backfill is not for mirrors", got)
	}
}

func TestContributorsNeedingASearchOrEnrichmentAreListed(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tag := pathsTag("need")
	want := seedR6Contributor(t, store, tag+"-want", tag+"-want@example.org", nil)
	linked := seedR6Contributor(t, store, tag+"-linked", tag+"-linked@example.org", time.Now().UnixNano()%1_000_000_000+5_000_000_000)
	noreply := seedR6Contributor(t, store, tag+"-noreply", tag+"@users.noreply.github.com", nil)
	// The other spelling the search excludes (round 8: only one was seeded).
	noReply := seedR6Contributor(t, store, tag+"-no-reply", tag+"@no-reply.example.org", nil)
	recent := seedR6Contributor(t, store, tag+"-recent", tag+"-recent@example.org", nil)
	old := seedR6Contributor(t, store, tag+"-old", tag+"-old@example.org", nil)
	deleted := seedR6Contributor(t, store, tag+"-deleted", tag+"-deleted@example.org", nil)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.contributors SET cntrb_last_search_attempted_at = NOW() - INTERVAL '2 days', cntrb_last_enriched_at = NOW() - INTERVAL '2 days' WHERE cntrb_id = $1::uuid`, recent)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.contributors SET cntrb_last_search_attempted_at = NOW() - INTERVAL '90 days', cntrb_last_enriched_at = NOW() - INTERVAL '90 days' WHERE cntrb_id = $1::uuid`, old)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.contributors SET cntrb_deleted = 1 WHERE cntrb_id = $1::uuid`, deleted)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.contributors SET cntrb_company = 'has one' WHERE cntrb_id = $1::uuid`, linked)

	search, err := store.GetContributorsNeedingSearch(ctx, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]string{}
	for _, c := range search {
		found[c.CntrbID] = c.Email
	}
	for id, in := range map[string]bool{want: true, old: true, linked: false, noreply: false, noReply: false, recent: false, deleted: false} {
		if _, ok := found[id]; ok != in {
			t.Errorf("contributor %s listed for a search = %v, want %v (linked to a GitHub user, a noreply address, tried within the cooldown, and deleted are not listed)", id, ok, in)
		}
	}
	if found[want] != tag+"-want@example.org" {
		t.Errorf("the candidate's address is %q", found[want])
	}
	if one, err := store.GetContributorsNeedingSearch(ctx, 1); err != nil || len(one) != 1 {
		t.Errorf("a limit of one: %d candidates (%v)", len(one), err)
	}

	thin, err := NewContributorResolver(store).GetThinContributorLogins(ctx, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]bool{}
	for _, l := range thin {
		listed[l] = true
	}
	for login, in := range map[string]bool{tag + "-want": true, tag + "-old": true, tag + "-noreply": true, tag + "-no-reply": true, tag + "-linked": false, tag + "-recent": false, tag + "-deleted": false} {
		if listed[login] != in {
			t.Errorf("contributor %s listed for enrichment = %v, want %v (one with a company, one enriched within the cooldown, and a deleted one are not listed)", login, listed[login], in)
		}
	}
	if one, err := NewContributorResolver(store).GetThinContributorLogins(ctx, 1); err != nil || len(one) != 1 {
		t.Errorf("a limit of one: %d logins (%v)", len(one), err)
	}
}

func TestMigrationCandidatesAreSampledWhereTheNewIdIsFree(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tag := pathsTag("cid")
	free := seedR6Contributor(t, store, tag+"-free", "", nil)
	taken := seedR6Contributor(t, store, tag+"-taken", "", nil)
	// (Ids below 2^31: the candidate set casts the id to a 32-bit integer.)
	uidFree, uidTaken := pathsUserID()%2_000_000_000, pathsUserID()%2_000_000_000
	for id, uid := range map[string]int64{free: uidFree, taken: uidTaken} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributor_identities (cntrb_id, platform_id, platform_user_id, login) VALUES ($1::uuid, 1, $2, 'x')`, id, uid)
	}
	// The id the second one would move to already belongs to another row.
	holder := PlatformUUID(1, uidTaken).String()
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, cntrb_deleted) VALUES ($1::uuid, $2, 0)`, holder, tag+"-holder")
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, holder)
	})
	got, err := SampleCntrbIDMigrationCandidates(ctx, store, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	byLogin := map[string]CntrbIDMigrationCandidate{}
	for i, c := range got {
		byLogin[c.Login] = c
		if i > 0 && got[i-1].Login > c.Login {
			t.Errorf("the sample is not ordered by login: %q before %q", got[i-1].Login, c.Login)
		}
	}
	if byLogin[tag+"-free"] != (CntrbIDMigrationCandidate{Login: tag + "-free", OldID: free, NewID: PlatformUUID(1, uidFree).String(), PlatformUserID: uidFree}) {
		t.Errorf("the contributor whose new id is free: %+v", byLogin[tag+"-free"])
	}
	if _, ok := byLogin[tag+"-taken"]; ok {
		t.Error("a contributor whose new id another row holds was sampled as a candidate")
	}
	if one, err := SampleCntrbIDMigrationCandidates(ctx, store, 1); err != nil || len(one) != 1 {
		t.Errorf("a limit of one: %d candidates (%v)", len(one), err)
	}
}

// The import from an Augur database reads two of ITS tables: they are made
// here, in schemas of their own, for the test.
func TestAugurRepositoriesAndKeysAreImported(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tag := pathsTag("augur")
	for _, q := range []string{
		`CREATE SCHEMA IF NOT EXISTS augur_data`,
		`CREATE TABLE augur_data.repo (repo_id bigint, repo_git text, repo_name text)`,
		`CREATE SCHEMA IF NOT EXISTS augur_operations`,
		`CREATE TABLE augur_operations.worker_oauth (name text, access_token text, platform text)`,
	} {
		mustExecRetry(ctx, t, store, q)
	}
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DROP TABLE IF EXISTS augur_data.repo`)
		cleanupExecRetry(c, store, `DROP TABLE IF EXISTS augur_operations.worker_oauth`)
		cleanupExecRetry(c, store, `DROP SCHEMA IF EXISTS augur_data`)
		cleanupExecRetry(c, store, `DROP SCHEMA IF EXISTS augur_operations`)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.worker_oauth WHERE access_token LIKE $1`, tag+"%")
	})
	mustExecRetry(ctx, t, store, `INSERT INTO augur_data.repo VALUES (20, 'https://github.com/z/b', 'b'), (10, 'https://github.com/z/a', NULL), (30, '', 'empty'), (40, NULL, 'null')`)
	repos, err := LoadAugurRepos(ctx, store.pool)
	if err != nil {
		t.Fatal(err)
	}
	if want := []AugurRepo{{RepoID: 10, RepoGit: "https://github.com/z/a"}, {RepoID: 20, RepoGit: "https://github.com/z/b", RepoName: "b"}}; !reflect.DeepEqual(repos, want) {
		t.Errorf("the repositories read from Augur: %+v\nwant %+v", repos, want)
	}

	mustExecRetry(ctx, t, store, `INSERT INTO augur_operations.worker_oauth VALUES ('one', $1, 'github'), ('two', $2, 'gitlab'), ('none', '', 'github')`, tag+"-1", tag+"-2")
	// One of the two is already stored: it is not copied again.
	if err := SaveAPIKey(ctx, store.pool, "mine", tag+"-1", "github"); err != nil {
		t.Fatal(err)
	}
	n, err := ImportKeysFromAugur(ctx, store.pool)
	if err != nil || n != 1 {
		t.Errorf("%d keys imported (%v), want the one that was not stored yet", n, err)
	}
	if got := pathsText(t, store, `SELECT string_agg(name || ':' || platform, ',' ORDER BY access_token) FROM aveloxis_ops.worker_oauth WHERE access_token LIKE $1`, tag+"%"); got != "mine:github,two:gitlab" {
		t.Errorf("the stored keys: %q", got)
	}
	// Saving a key again under another name renames it; the same token on
	// another platform is another key.
	if err := SaveAPIKey(ctx, store.pool, "renamed", tag+"-1", "github"); err != nil {
		t.Fatal(err)
	}
	if err := SaveAPIKey(ctx, store.pool, "other", tag+"-1", "gitlab"); err != nil {
		t.Fatal(err)
	}
	if got := pathsText(t, store, `SELECT string_agg(name || ':' || platform, ',' ORDER BY platform) FROM aveloxis_ops.worker_oauth WHERE access_token = $1`, tag+"-1"); got != "renamed:github,other:gitlab" {
		t.Errorf("the key after it was saved again: %q", got)
	}
}

func TestTheServerConnectionBudgetIsRead(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	max, reserved, err := ServerConnectionBudget(ctx, os.Getenv("AVELOXIS_TEST_DB"))
	if err != nil {
		t.Fatal(err)
	}
	wantMax := pathsInt(t, store, `SELECT current_setting('max_connections')::int`)
	wantReserved := pathsInt(t, store, `SELECT current_setting('superuser_reserved_connections')::int + COALESCE(current_setting('reserved_connections', TRUE)::int, 0)`)
	if int64(max) != wantMax || int64(reserved) != wantReserved {
		t.Errorf("the budget read is %d connections with %d reserved; the server says %d and %d", max, reserved, wantMax, wantReserved)
	}
	if _, _, err := ServerConnectionBudget(ctx, "postgres://nobody@127.0.0.1:1/none?connect_timeout=1"); err == nil {
		t.Error("a server that cannot be reached gave a budget")
	}
}

// A migrate that does not wait for the lock takes it when it is free, and
// refuses when another session holds it.
func TestAMigrateThatDoesNotWaitTakesTheFreeLockAndRefusesAHeldOne(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nowait, err := NewPostgresStore(ctx, os.Getenv("AVELOXIS_TEST_DB"), logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nowait.Close)
	nowait.migrateNoWait = true
	holder, err := store.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	t.Cleanup(func() {
		if !released {
			_, _ = holder.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, MigrateAdvisoryLockID)
			holder.Release()
		}
	})
	var got bool
	if err := holder.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, MigrateAdvisoryLockID).Scan(&got); err != nil || !got {
		t.Fatalf("the test could not take the migrate lock itself: %v, %v", got, err)
	}
	err = RunMigrations(ctx, nowait, logger)
	if err == nil || !strings.Contains(err.Error(), "advisory lock held") {
		t.Fatalf("a migrate that does not wait, with the lock held elsewhere: %v; want it refused", err)
	}
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock($1)`, MigrateAdvisoryLockID); err != nil {
		t.Fatal(err)
	}
	holder.Release()
	released = true
	if err := RunMigrations(ctx, nowait, logger); err != nil {
		t.Fatalf("a migrate that does not wait, with the lock free: %v", err)
	}
	// It let go of the lock when it finished.
	if err := store.pool.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, MigrateAdvisoryLockID).Scan(&got); err != nil || !got {
		t.Errorf("the lock is still held after the migrate finished (%v, %v)", got, err)
	}
	mustExecRetry(ctx, t, store, `SELECT pg_advisory_unlock_all()`)
}

// The one-time commit dedup of an old database: with the unique index gone
// and a duplicate present, the migrate step removes the newer duplicate and
// builds the index.
func TestTheCommitDedupStepRemovesDuplicatesAndBuildsTheIndex(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	repoID, _, _ := pathsRepo(t, store, "dedup", "aveloxis_data.commits")
	valid := func() int64 {
		return pathsInt(t, store, `SELECT COUNT(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'aveloxis_data' AND c.relname = 'idx_commits_repo_hash_file' AND i.indisvalid AND i.indisunique`)
	}
	if valid() != 1 {
		t.Fatal("the package's database has no valid unique commits index to begin with")
	}
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.commits WHERE repo_id = $1`, repoID)
		deduplicateCommits(c, store, logger)
		if valid() != 1 {
			t.Error("the unique commits index was not restored")
		}
	})
	mustExecRetry(ctx, t, store, `DROP INDEX aveloxis_data.idx_commits_repo_hash_file`)
	for i := 0; i < 3; i++ {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name) VALUES ($1, 'dup', 'f.go', $2)`, repoID, fmt.Sprintf("copy-%d", i))
	}
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name) VALUES ($1, 'dup', 'other.go', 'kept')`, repoID)
	deduplicateCommits(ctx, store, logger)
	if got := pathsText(t, store, `SELECT string_agg(cmt_filename || ':' || cmt_author_name, ',' ORDER BY cmt_filename) FROM aveloxis_data.commits WHERE repo_id = $1`, repoID); got != "f.go:copy-0,other.go:kept" {
		t.Errorf("after the dedup the repository's commit rows are %q; want the first copy of the duplicated file and the other file", got)
	}
	if valid() != 1 {
		t.Error("the unique index was not built after the dedup")
	}
}

// An interrupted concurrent build leaves the case-insensitive unique index
// INVALID; the migrate step drops it and builds it again.
func TestAnInvalidCaseInsensitiveIndexIsDroppedAndRebuilt(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	state := func() string {
		return pathsText(t, store, `SELECT COALESCE((SELECT CASE WHEN i.indisvalid THEN 'valid' ELSE 'invalid' END FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'aveloxis_data' AND c.relname = 'uq_repos_repo_git_ci'), 'absent')`)
	}
	tag := pathsTag("ci")
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.repos WHERE repo_owner = $1`, tag)
		ensureRepoGitCaseInsensitiveUnique(c, store, logger)
		if state() != "valid" {
			t.Errorf("the case-insensitive unique index is %s after the test", state())
		}
	})
	mustExecRetry(ctx, t, store, `DROP INDEX IF EXISTS aveloxis_data.uq_repos_repo_git_ci`)
	for _, name := range []string{"Repo", "repo"} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id) VALUES ($1, $2, 'r', 1)`, "https://github.com/"+tag+"/"+name, tag)
	}
	// The build fails on the pair and leaves the index invalid.
	if _, err := store.pool.Exec(ctx, `CREATE UNIQUE INDEX CONCURRENTLY uq_repos_repo_git_ci ON aveloxis_data.repos (LOWER(repo_git)) WHERE platform_id IN (1, 2)`); err == nil {
		t.Fatal("the unique index was built over a case-variant pair")
	}
	if state() != "invalid" {
		t.Fatalf("after the failed build the index is %s; the premise is an invalid one", state())
	}
	// With the pair still there the step does nothing (it would fail again).
	// "Nothing" is the SAME index object: a step that did not see the pair
	// drops the index and fails to build another, which also ends invalid
	// (round 8: the state alone was compared).
	object := func() string {
		return pathsText(t, store, `SELECT COALESCE((SELECT c.oid::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'aveloxis_data' AND c.relname = 'uq_repos_repo_git_ci'), 'none')`)
	}
	before := object()
	ensureRepoGitCaseInsensitiveUnique(ctx, store, logger)
	if state() != "invalid" || object() != before {
		t.Errorf("with duplicates present the step left the index %s (object %s, was %s), want it untouched", state(), object(), before)
	}
	mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_data.repos WHERE repo_owner = $1 AND repo_git LIKE '%/repo'`, tag)
	ensureRepoGitCaseInsensitiveUnique(ctx, store, logger)
	if state() != "valid" {
		t.Errorf("with the duplicate gone the step left the index %s, want the invalid one dropped and a valid one built", state())
	}
}

func TestAReviewedInlineCommentGetsItsCompatibilityRow(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID, _, _ := pathsRepo(t, store, "rc",
		"aveloxis_data.pull_request_review_message_ref", "aveloxis_data.review_comments", "aveloxis_data.pull_request_reviews", "aveloxis_data.messages", "aveloxis_data.pull_requests")
	pr := pathsPR(t, store, repoID, 1, "open")
	var review int64
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.pull_request_reviews (pull_request_id, repo_id, platform_id, platform_review_id) VALUES ($1, $2, 1, $3) RETURNING pr_review_id`, pr, repoID, repoID*10+7).Scan(&review); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	comment := func(src int64, reviewID int64, hunk string) platform.ReviewCommentWithRef {
		return platform.ReviewCommentWithRef{
			Message: model.Message{RepoID: repoID, PlatformMsgID: src, PlatformID: model.PlatformGitHub, Text: "inline " + hunk, Timestamp: at, Origin: pathsOrigin},
			Comment: model.ReviewComment{RepoID: repoID, ReviewID: reviewID, PlatformSrcID: src, NodeID: "RC", DiffHunk: hunk, Path: "a.go", UpdatedAt: at, HTMLURL: "https://example.org/c"},
		}
	}
	reviewed, unreviewed := repoID*100+1, repoID*100+2
	if err := store.UpsertReviewCommentBatch(ctx, []platform.ReviewCommentWithRef{comment(reviewed, review, "@@ one"), comment(unreviewed, 0, "@@ two")}); err != nil {
		t.Fatal(err)
	}
	if n := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.review_comments WHERE repo_id = $1`, repoID); n != 2 {
		t.Errorf("%d inline comments stored, want 2", n)
	}
	if got := pathsText(t, store, `SELECT COUNT(*) || '|' || MIN(pr_review_id) || '|' || MIN(pr_review_msg_diff_hunk) || '|' || MIN(pr_review_msg_path) FROM aveloxis_data.pull_request_review_message_ref WHERE repo_id = $1`, repoID); got != fmt.Sprintf("1|%d|@@ one|a.go", review) {
		t.Errorf("the compatibility rows: %q; want one, for the comment whose review is stored", got)
	}
	// Written again: one row still, with the new hunk.
	if err := store.UpsertReviewCommentBatch(ctx, []platform.ReviewCommentWithRef{comment(reviewed, review, "@@ changed")}); err != nil {
		t.Fatal(err)
	}
	if got := pathsText(t, store, `SELECT COUNT(*) || '|' || MIN(diff_hunk) FROM aveloxis_data.review_comments WHERE repo_id = $1 AND platform_src_id = $2`, repoID, reviewed); got != "1|@@ changed" {
		t.Errorf("the inline comment after it was written again: %q", got)
	}
	if n := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.pull_request_review_message_ref WHERE repo_id = $1`, repoID); n != 1 {
		t.Errorf("%d compatibility rows after the comment was written again, want 1", n)
	}
	// Written a third time by a pass that does not know its review: the
	// stored review is kept.
	if err := store.UpsertReviewCommentBatch(ctx, []platform.ReviewCommentWithRef{comment(reviewed, 0, "@@ third")}); err != nil {
		t.Fatal(err)
	}
	if got := pathsInt(t, store, `SELECT COALESCE(pr_review_id, 0) FROM aveloxis_data.review_comments WHERE repo_id = $1 AND platform_src_id = $2`, repoID, reviewed); got != review {
		t.Errorf("the comment's review after a write that named none: %d, want the stored %d", got, review)
	}
}
