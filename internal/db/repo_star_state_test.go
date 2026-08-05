// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// repo_star_state_test.go — v0.27.85 IsRepoStarred: the targeted
// per-(user, repo) starred-state read backing GET /repos/{id}/star.

package db

import (
	"fmt"
	"testing"
	"time"
)

func TestIsRepoStarredEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	suffix := time.Now().UnixNano()

	var userID int
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
		VALUES ($1, 'github', '', FALSE) RETURNING user_id`,
		fmt.Sprintf("_avstarstate_%d", suffix)).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, $2, 'starstate', 1) RETURNING repo_id`,
		fmt.Sprintf("https://github.com/_avstarstate%d/starstate", suffix),
		fmt.Sprintf("_avstarstate%d", suffix)).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repo_stars WHERE user_id = $1`, userID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
	})

	starred, err := store.IsRepoStarred(ctx, userID, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if starred {
		t.Error("unstarred repo must report false")
	}
	if err := store.StarRepo(ctx, userID, repoID); err != nil {
		t.Fatal(err)
	}
	starred, err = store.IsRepoStarred(ctx, userID, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if !starred {
		t.Error("starred repo must report true")
	}
	// Another user's star state is independent.
	other, err := store.IsRepoStarred(ctx, userID+1000000, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if other {
		t.Error("star state must be scoped to the caller")
	}
	if err := store.UnstarRepo(ctx, userID, repoID); err != nil {
		t.Fatal(err)
	}
	starred, err = store.IsRepoStarred(ctx, userID, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if starred {
		t.Error("unstar must revert the state")
	}
}
