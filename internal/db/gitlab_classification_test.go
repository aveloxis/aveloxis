// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.30.0 (multi-instance GitLab): a repository's platform_id is decided
// where it is written. UpsertRepo classifies a non-GitHub URL by the
// platforms registry — under a registered instance's web base it gets that
// instance's id whatever the caller passed; a GitLab-looking URL under no
// registered instance is recorded git-only (3), never filed under another
// instance. Adoption moves repositories onto an instance once it is
// registered; misrouted platform-2 rows that already carry API data are
// reported, never mutated; and a rename that would move a repository to
// another instance is refused.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).

package db

import (
	"context"
	"errors"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

const classifySlug = "_avclassify"

func classifyFixture(t *testing.T) (context.Context, *PostgresStore, map[string]model.Platform) {
	t.Helper()
	ctx, store := registryConnect(t)
	isolateRegistry(ctx, t, store)
	cleanupCaseRepos(ctx, t, store, classifySlug)
	t.Cleanup(func() { cleanupCaseRepos(ctx, t, store, classifySlug) })
	ids, err := store.SyncGitLabInstances(ctx, []GitLabInstanceRef{
		inst("https://gitlab.main.invalid", true),
		inst("https://code.b.invalid/gitlab", false),
		inst("https://c.invalid", false),
	})
	if err != nil {
		t.Fatal(err)
	}
	return ctx, store, ids
}

func storedPlatform(ctx context.Context, t *testing.T, store *PostgresStore, repoID int64) model.Platform {
	t.Helper()
	var p int16
	if err := store.pool.QueryRow(ctx, `SELECT platform_id FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&p); err != nil {
		t.Fatal(err)
	}
	return model.Platform(p)
}

func TestUpsertRepoClassifiesByRegistry(t *testing.T) {
	ctx, store, ids := classifyFixture(t)
	cases := []struct {
		name   string
		passed model.Platform
		url    string
		want   model.Platform
	}{
		{"platform 2 URL under a registered sub-path instance", model.PlatformGitLab, "https://code.b.invalid/gitlab/" + classifySlug + "/r1", ids["https://code.b.invalid/gitlab"]},
		{"generic-git URL under a registered instance", model.PlatformGenericGit, "https://c.invalid/" + classifySlug + "/r2", ids["https://c.invalid"]},
		{"main instance", model.PlatformGitLab, "https://gitlab.main.invalid/" + classifySlug + "/r3", model.PlatformGitLab},
		{"GitLab-looking host no instance has", model.PlatformGitLab, "https://gitlab.unregistered.invalid/" + classifySlug + "/r4", model.PlatformGenericGit},
		{"an instance id passed for a URL on another instance", ids["https://c.invalid"], "https://code.b.invalid/gitlab/" + classifySlug + "/r5", ids["https://code.b.invalid/gitlab"]},
		{"generic git elsewhere stays generic", model.PlatformGenericGit, "https://git.example.invalid/" + classifySlug + "/r6", model.PlatformGenericGit},
		{"GitHub is never reclassified", model.PlatformGitHub, "https://github.com/" + classifySlug + "/r7", model.PlatformGitHub},
	}
	for _, tc := range cases {
		id, err := store.UpsertRepo(ctx, &model.Repo{Platform: tc.passed, GitURL: tc.url, Owner: classifySlug, Name: "r"})
		if err != nil {
			t.Fatalf("%s: UpsertRepo: %v", tc.name, err)
		}
		if got := storedPlatform(ctx, t, store, id); got != tc.want {
			t.Errorf("%s: stored platform_id %d, want %d", tc.name, got, tc.want)
		}
	}

	// ON CONFLICT DO UPDATE never changes platform_id: the git-only row for
	// a host registered later keeps 3 until adoption moves it.
	late := "https://gitlab.late.invalid/" + classifySlug + "/r8"
	id, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitLab, GitURL: late, Owner: classifySlug, Name: "r8"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SyncGitLabInstances(ctx, []GitLabInstanceRef{inst("https://gitlab.main.invalid", true), inst("https://gitlab.late.invalid", false)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitLab, GitURL: late, Owner: classifySlug, Name: "r8"}); err != nil {
		t.Fatal(err)
	}
	if got := storedPlatform(ctx, t, store, id); got != model.PlatformGenericGit {
		t.Errorf("re-upsert changed platform_id to %d; only adoption may move an existing row", got)
	}
}

// Before the first registry sync (row 2 has no web base yet — a web process
// on the new binary before serve or migrate ran), a GitLab URL keeps the
// historical platform 2 rather than being demoted to git-only.
func TestUpsertRepoBeforeFirstSyncKeepsPlatform2(t *testing.T) {
	ctx, store := registryConnect(t)
	isolateRegistry(ctx, t, store)
	cleanupCaseRepos(ctx, t, store, classifySlug)
	t.Cleanup(func() { cleanupCaseRepos(ctx, t, store, classifySlug) })
	id, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitLab, GitURL: "https://gitlab.early.invalid/" + classifySlug + "/r", Owner: classifySlug, Name: "r"})
	if err != nil {
		t.Fatal(err)
	}
	if got := storedPlatform(ctx, t, store, id); got != model.PlatformGitLab {
		t.Errorf("platform_id = %d before the first sync, want 2", got)
	}
}

func TestAdoptReposForGitLabInstances(t *testing.T) {
	ctx, store := registryConnect(t)
	isolateRegistry(ctx, t, store)
	cleanupCaseRepos(ctx, t, store, classifySlug)
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_info WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE '%_avclassify%')`)
		cleanupCaseRepos(ctx, t, store, classifySlug)
	})
	gid := defaultRepoGroup(ctx, t, store)
	insert := func(p model.Platform, url string) int64 {
		var id int64
		if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_group_id, platform_id, repo_git, repo_name, repo_owner)
			VALUES ($1, $2, $3, 'r', $4) RETURNING repo_id`, gid, int16(p), url, classifySlug).Scan(&id); err != nil {
			t.Fatal(err)
		}
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.collection_queue (repo_id, last_collected) VALUES ($1, NOW())`, id)
		return id
	}
	// Seeded before the instances are registered (git-only, and a pre-v0.30.0
	// misroute onto platform 2), then the instances appear in config.
	gitOnly := insert(model.PlatformGenericGit, "https://c.invalid/"+classifySlug+"/gitonly")
	misroutedNoData := insert(model.PlatformGitLab, "https://code.b.invalid/gitlab/"+classifySlug+"/nodata")
	misroutedData := insert(model.PlatformGitLab, "https://code.b.invalid/gitlab/"+classifySlug+"/hasdata")
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_info (repo_id) VALUES ($1)`, misroutedData)
	unregistered := insert(model.PlatformGenericGit, "https://gitlab.nowhere.invalid/"+classifySlug+"/r")
	onMain := insert(model.PlatformGitLab, "https://gitlab.main.invalid/"+classifySlug+"/main")

	ids, err := store.SyncGitLabInstances(ctx, []GitLabInstanceRef{
		inst("https://gitlab.main.invalid", true),
		inst("https://code.b.invalid/gitlab", false),
		inst("https://c.invalid", false),
	})
	if err != nil {
		t.Fatal(err)
	}
	// A case variant of the git-only row already tracked on instance c
	// cannot be adopted beside it (uq_repos_repo_git_ci_gitlab_instances).
	caseTwin := insert(model.PlatformGenericGit, "https://c.invalid/"+classifySlug+"/GITONLY")

	res, err := store.AdoptReposForGitLabInstances(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := storedPlatform(ctx, t, store, gitOnly); got != ids["https://c.invalid"] {
		t.Errorf("git-only repo under instance c: platform %d, want %d", got, ids["https://c.invalid"])
	}
	if got := storedPlatform(ctx, t, store, misroutedNoData); got != ids["https://code.b.invalid/gitlab"] {
		t.Errorf("misrouted platform-2 repo without API data: platform %d, want adopted onto %d", got, ids["https://code.b.invalid/gitlab"])
	}
	if got := storedPlatform(ctx, t, store, misroutedData); got != model.PlatformGitLab {
		t.Errorf("misrouted platform-2 repo WITH API data was mutated to %d; it must be reported, not moved", got)
	}
	if got := storedPlatform(ctx, t, store, unregistered); got != model.PlatformGenericGit {
		t.Errorf("repo on an unregistered host moved to %d", got)
	}
	if got := storedPlatform(ctx, t, store, onMain); got != model.PlatformGitLab {
		t.Errorf("repo on the main instance moved to %d", got)
	}
	if got := storedPlatform(ctx, t, store, caseTwin); got != model.PlatformGenericGit {
		t.Errorf("case-variant twin moved to %d beside its tracked twin", got)
	}
	if res.Adopted != 2 || res.SkippedConflicts != 1 {
		t.Errorf("adoption result %+v, want Adopted 2, SkippedConflicts 1", res)
	}
	var force int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.collection_queue WHERE repo_id = ANY($1) AND force_full_collect`, []int64{gitOnly, misroutedNoData}).Scan(&force); err != nil || force != 2 {
		t.Errorf("adopted repos with force_full_collect = %d (%v), want 2 — their API history was never collected", force, err)
	}

	misrouted, err := store.MisroutedGitLabRepos(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range misrouted {
		if m.RepoID == misroutedData {
			found = true
		}
		if m.RepoID == onMain || m.RepoID == misroutedNoData {
			t.Errorf("repo %d wrongly reported as misrouted", m.RepoID)
		}
	}
	if !found {
		t.Errorf("the misrouted platform-2 repo with API data is not reported: %+v", misrouted)
	}
	probe := false
	for _, r := range store.RunDataVerification(ctx, VerifyOptions{Sample: 1}) {
		if r.Check == "misrouted GitLab repos" {
			probe = true
			if r.Severity != "FAIL" {
				t.Errorf("data-verify misrouted GitLab repos = %s (%s), want FAIL", r.Severity, r.Detail)
			}
		}
	}
	if !probe {
		t.Error("data-verify has no \"misrouted GitLab repos\" probe")
	}

	// Idempotent: a rerun adopts nothing new and still skips the twin.
	again, err := store.AdoptReposForGitLabInstances(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again.Adopted != 0 {
		t.Errorf("rerun adopted %d, want 0", again.Adopted)
	}
}

func TestUpdateRepoURLsRefusesCrossInstanceRename(t *testing.T) {
	ctx, store, ids := classifyFixture(t)
	b := ids["https://code.b.invalid/gitlab"]
	oldURL := "https://code.b.invalid/gitlab/" + classifySlug + "/old"
	id, err := store.UpsertRepo(ctx, &model.Repo{Platform: b, GitURL: oldURL, Owner: classifySlug, Name: "old"})
	if err != nil {
		t.Fatal(err)
	}
	err = store.UpdateRepoURLs(ctx, id, oldURL, "https://c.invalid/"+classifySlug+"/old")
	if !errors.Is(err, ErrCrossInstanceRename) {
		t.Fatalf("rename onto another GitLab instance = %v, want ErrCrossInstanceRename", err)
	}
	var stored string
	if err := store.pool.QueryRow(ctx, `SELECT repo_git FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&stored); err != nil || stored != oldURL {
		t.Errorf("refused rename changed repo_git to %q (%v)", stored, err)
	}
	if err := store.UpdateRepoURLs(ctx, id, oldURL, "https://code.b.invalid/gitlab/"+classifySlug+"/new"); err != nil {
		t.Errorf("rename within the instance: %v", err)
	}
	if err := store.UpdateRepoURL(ctx, id, "https://github.com/"+classifySlug+"/new"); !errors.Is(err, ErrCrossInstanceRename) {
		t.Errorf("rename of a GitLab repo onto GitHub = %v, want ErrCrossInstanceRename", err)
	}
}

// Review of B1–B5 (finding 1): a repository on an instance installed under a
// sub-path keeps the prefix OUT of repo_owner — every API call is built from
// owner/name — whichever door wrote it: UpsertRepo from any caller, a URL
// update, or the case-drift heal.
func TestSubPathInstanceOwnerNeverCarriesThePrefix(t *testing.T) {
	ctx, store, ids := classifyFixture(t)
	b := ids["https://code.b.invalid/gitlab"]
	owner := func(id int64) (string, string, string) {
		var o, n, g string
		if err := store.pool.QueryRow(ctx, `SELECT repo_owner, repo_name, repo_git FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&o, &n, &g); err != nil {
			t.Fatal(err)
		}
		return o, n, g
	}

	// A caller that parsed without the registry passes the prefix as owner.
	id, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGenericGit,
		GitURL: "https://code.b.invalid/gitlab/" + classifySlug + "/Sub/Repo", Owner: "gitlab/" + classifySlug + "/Sub", Name: "Repo"})
	if err != nil {
		t.Fatal(err)
	}
	if o, n, _ := owner(id); o != classifySlug+"/Sub" || n != "Repo" || storedPlatform(ctx, t, store, id) != b {
		t.Fatalf("UpsertRepo stored owner %q name %q platform %d, want %q / Repo / %d", o, n, storedPlatform(ctx, t, store, id), classifySlug+"/Sub", b)
	}

	// No owner/repo after the web base: refused, never stored with the
	// prefix as its owner (review pass 2, finding 8).
	if _, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGenericGit,
		GitURL: "https://code.b.invalid/gitlab/" + classifySlug, Owner: "gitlab", Name: classifySlug}); err == nil {
		t.Error("a URL with only one path segment after the instance's web base must be refused")
	}

	// The forge reports the canonical case; the heal keeps the prefix.
	healed, err := store.HealRepoCaseDrift(ctx, id, classifySlug+"/sub/repo")
	if err != nil || !healed {
		t.Fatalf("HealRepoCaseDrift = (%v, %v), want a heal", healed, err)
	}
	if o, n, g := owner(id); g != "https://code.b.invalid/gitlab/"+classifySlug+"/sub/repo" || o != classifySlug+"/sub" || n != "repo" {
		t.Errorf("after case heal: repo_git %q owner %q name %q — the sub-path prefix must survive and stay out of the owner", g, o, n)
	}

	// A rename within the instance.
	if err := store.UpdateRepoURLs(ctx, id, "https://code.b.invalid/gitlab/"+classifySlug+"/sub/repo", "https://code.b.invalid/gitlab/"+classifySlug+"/moved/renamed"); err != nil {
		t.Fatal(err)
	}
	if o, n, _ := owner(id); o != classifySlug+"/moved" || n != "renamed" {
		t.Errorf("after rename: owner %q name %q, want %q / renamed", o, n, classifySlug+"/moved")
	}
	// A rename to the prefix plus one segment is refused, not stored with
	// owner = the prefix (review pass 3, finding 4).
	if err := store.UpdateRepoURL(ctx, id, "https://code.b.invalid/gitlab/"+classifySlug); err == nil {
		t.Error("UpdateRepoURL to a URL with no owner/repo after the instance's web base must be refused")
	}
}
