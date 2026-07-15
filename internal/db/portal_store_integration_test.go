// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.3 — integration coverage for GetPortalGroupReposForUser's
// ownership gate; v0.27.14 adds pagination (limit/offset + total).
// Gated on AVELOXIS_TEST_DB (scratch DB only).

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestGetPortalGroupReposForUserOwnership(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(store.Close)

	suffix := time.Now().UnixNano()
	var ownerID, strangerID int
	for i, login := range []string{fmt.Sprintf("_avportal_owner_%d", suffix), fmt.Sprintf("_avportal_other_%d", suffix)} {
		var id int
		err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email)
			VALUES ($1, 'github', '') RETURNING user_id`, login).Scan(&id)
		if err != nil {
			t.Fatalf("seed user: %v", err)
		}
		if i == 0 {
			ownerID = id
		} else {
			strangerID = id
		}
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id IN ($1,$2)`, ownerID, strangerID)
	})

	groupID, err := store.CreateUserGroup(ctx, ownerID, fmt.Sprintf("_avportal_grp_%d", suffix))
	if err != nil {
		t.Fatalf("create group: %v", err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id = $1`, groupID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID)
	})

	// Owner reads own (empty) group fine.
	if _, _, err := store.GetPortalGroupReposForUser(ctx, ownerID, groupID, false, 50, 0); err != nil {
		t.Errorf("owner must be able to read own group: %v", err)
	}
	// Stranger is refused.
	if _, _, err := store.GetPortalGroupReposForUser(ctx, strangerID, groupID, false, 50, 0); err == nil {
		t.Error("non-owner non-admin must NOT be able to read another user's group")
	}
	// Admin bypasses ownership.
	if _, _, err := store.GetPortalGroupReposForUser(ctx, strangerID, groupID, true, 50, 0); err != nil {
		t.Errorf("admin must be able to read any group: %v", err)
	}
}

// TestGetPortalGroupReposPagination pins the v0.27.14 envelope
// semantics: limit/offset slice the ordered listing, and total always
// reports the full group size regardless of the page requested.
func TestGetPortalGroupReposPagination(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(store.Close)

	suffix := time.Now().UnixNano()
	var userID int
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email)
		VALUES ($1, 'github', '') RETURNING user_id`,
		fmt.Sprintf("_avpage_%d", suffix)).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	groupID, err := store.CreateUserGroup(ctx, userID, fmt.Sprintf("_avpage_grp_%d", suffix))
	if err != nil {
		t.Fatal(err)
	}
	owner := fmt.Sprintf("_avpage%d", suffix)
	var repoIDs []int64
	for i := 0; i < 3; i++ {
		var id int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, $2, $3, 1) RETURNING repo_id`,
			fmt.Sprintf("https://github.com/%s/r%d", owner, i), owner, fmt.Sprintf("r%d", i)).Scan(&id); err != nil {
			t.Fatal(err)
		}
		repoIDs = append(repoIDs, id)
		if err := store.AddRepoToGroupByID(ctx, groupID, id); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id = $1`, groupID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
		for _, id := range repoIDs {
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
		}
	})

	page1, total, err := store.GetPortalGroupReposForUser(ctx, userID, groupID, false, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 || total != 3 {
		t.Errorf("page 1: want 2 repos of total 3, got %d of %d", len(page1), total)
	}
	page2, total, err := store.GetPortalGroupReposForUser(ctx, userID, groupID, false, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 1 || total != 3 {
		t.Errorf("page 2: want 1 repo of total 3, got %d of %d", len(page2), total)
	}
	// Ordered listing → pages are disjoint.
	if len(page1) == 2 && len(page2) == 1 &&
		(page1[0].RepoID == page2[0].RepoID || page1[1].RepoID == page2[0].RepoID) {
		t.Error("pages must be disjoint slices of the ordered listing")
	}
}
