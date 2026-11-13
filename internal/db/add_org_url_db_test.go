// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// A URL that is not a repository never becomes a repository row (0.29.146).
// The add machinery used to keep going when the URL did not parse: the row
// was stored as a git-only repository with no owner and no name, and every
// collection of it failed at the clone. An organization page did exactly
// that on the fleet (`https://github.com/<org>/`, stored as git-only). The
// service validates before it calls the store, but an item saved by an
// older release reaches the store at its approval with no such check.
func TestAURLThatIsNotARepositoryIsNeverStored(t *testing.T) {
	store, ctx := openSR5Store(t)
	suffix := time.Now().UnixNano()
	login := fmt.Sprintf("avx-orgurl-%d", suffix)
	clean := func() {
		bg := context.Background()
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1)`, "%avx-orgurl-%")
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.user_repos WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1)`, "%avx-orgurl-%")
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1`, "%avx-orgurl-%")
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "org url probe")
	if err != nil {
		t.Fatal(err)
	}
	org := fmt.Sprintf("avx-orgurl-%d", suffix)
	for _, u := range []string{
		"https://github.com/" + org + "/", // the fleet's row
		"https://github.com/" + org,
		"https://gitlab.com/" + org,
		"https://github.com/" + org + "/a/b", // GitHub is exactly owner/repo
		"https://git.example.invalid/" + org, // a generic host needs owner/repo too
		"https://github.com/",                // no path at all
		"ftp://github.com/" + org + "/repo",  // not a web URL
	} {
		id, err := store.ensureRepoCollectedInGroup(ctx, gid, u, "")
		if err == nil {
			t.Errorf("%s: stored as repository %d, want a refusal", u, id)
			continue
		}
		if !errors.Is(err, platform.ErrInvalidRepoURL) {
			t.Errorf("%s: error %v does not wrap platform.ErrInvalidRepoURL", u, err)
		}
		// Retrying cannot make it a repository: an approval pass marks the
		// item failed and goes on, it does not stop at it for ever.
		if !addItemFailurePermanent(err) {
			t.Errorf("%s: the refusal is not a permanent item failure, so one such item would block its request's approval", u)
		}
	}
	var n int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_git LIKE $1`, "%"+org+"%").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("%d repository rows were written for URLs that are not repositories", n)
	}

	// A repository URL still goes in, on a forge and on a generic host.
	for _, u := range []string{"https://github.com/" + org + "/repo", "https://git.example.invalid/" + org + "/repo"} {
		if id, err := store.ensureRepoCollectedInGroup(ctx, gid, u, ""); err != nil || id <= 0 {
			t.Errorf("%s: (%d, %v), want it stored", u, id, err)
		}
	}
}

// The store is the layer that writes the row, so it is the layer that
// refuses one without an owner or a name, whichever caller built it, and a
// rename to a URL that is not a repository's (SR-18).
func TestTheStoreRefusesARepositoryWithoutOwnerOrName(t *testing.T) {
	store, ctx := openSR5Store(t)
	suffix := time.Now().UnixNano()
	base := fmt.Sprintf("https://git.example.invalid/avx-noowner-%d", suffix)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1`, base+"%")
	})
	for _, r := range []*model.Repo{
		{GitURL: base + "/a", Platform: model.PlatformGenericGit},
		{GitURL: base + "/b", Platform: model.PlatformGenericGit, Owner: "o"},
		{GitURL: base + "/c", Platform: model.PlatformGenericGit, Name: "c"},
		{GitURL: base + "/d", Platform: model.PlatformGitHub, Owner: " ", Name: "d"},
		{GitURL: base + "/e", Platform: model.PlatformGitHub, Owner: "o", Name: ".git"}, // the name normalizes to nothing
	} {
		id, err := store.UpsertRepo(ctx, r)
		if err == nil {
			t.Errorf("UpsertRepo(owner %q, name %q) stored repository %d, want a refusal", r.Owner, r.Name, id)
			continue
		}
		if !errors.Is(err, platform.ErrInvalidRepoURL) {
			t.Errorf("UpsertRepo(owner %q, name %q): %v does not wrap platform.ErrInvalidRepoURL", r.Owner, r.Name, err)
		}
		if missing := strings.TrimSpace(r.Owner) == "" || model.NormalizeRepoName(r.Name) == ""; missing != errors.Is(err, ErrRepoWithoutOwnerOrName) {
			t.Errorf("UpsertRepo(owner %q, name %q): %v; ErrRepoWithoutOwnerOrName is for a missing owner or name, and only that", r.Owner, r.Name, err)
		}
	}
	var n int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_git LIKE $1`, base+"%").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("%d rows were written without an owner or a name", n)
	}

	// A rename: the row keeps what it has when the target is not a
	// repository's URL.
	id, err := store.UpsertRepo(ctx, &model.Repo{GitURL: base + "/ok", Platform: model.PlatformGenericGit, Owner: "o", Name: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	orgPage := fmt.Sprintf("https://github.com/avx-noowner-%d/", suffix)
	// A forge's sign-in page has two path segments and parses like a
	// repository: gitlab.com answers a project that went private with a
	// redirect to it (review round 2). It is the forge's account space.
	signIn := "https://gitlab.com/users/sign_in"
	for name, rename := range map[string]func() error{
		"UpdateRepoURL":                 func() error { return store.UpdateRepoURL(ctx, id, orgPage) },
		"UpdateRepoURLs":                func() error { return store.UpdateRepoURLs(ctx, id, base+"/ok", orgPage) },
		"UpdateRepoURL to a sign-in":    func() error { return store.UpdateRepoURL(ctx, id, signIn) },
		"UpdateRepoURLs to a sign-in":   func() error { return store.UpdateRepoURLs(ctx, id, base+"/ok", signIn) },
		"UpdateRepoURL to GitHub login": func() error { return store.UpdateRepoURL(ctx, id, "https://github.com/login/oauth") },
	} {
		if err := rename(); !errors.Is(err, platform.ErrInvalidRepoURL) {
			t.Errorf("%s to an organization page = %v, want a refusal wrapping platform.ErrInvalidRepoURL", name, err)
		}
	}
	var git, owner, repoName string
	if err := store.pool.QueryRow(ctx, `SELECT repo_git, repo_owner, repo_name FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&git, &owner, &repoName); err != nil {
		t.Fatal(err)
	}
	if git != base+"/ok" || owner != "o" || repoName != "ok" {
		t.Errorf("the refused renames changed the row: %q %q/%q", git, owner, repoName)
	}
	// A rename to a repository's URL still goes through.
	if err := store.UpdateRepoURL(ctx, id, base+"/renamed"); err != nil {
		t.Errorf("a rename to a repository URL: %v", err)
	}
}
