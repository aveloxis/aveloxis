// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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
