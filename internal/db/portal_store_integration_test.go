// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.3 — integration coverage for GetPortalGroupReposForUser's
// ownership gate. Gated on AVELOXIS_TEST_DB (scratch DB only).

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
	if _, err := store.GetPortalGroupReposForUser(ctx, ownerID, groupID, false); err != nil {
		t.Errorf("owner must be able to read own group: %v", err)
	}
	// Stranger is refused.
	if _, err := store.GetPortalGroupReposForUser(ctx, strangerID, groupID, false); err == nil {
		t.Error("non-owner non-admin must NOT be able to read another user's group")
	}
	// Admin bypasses ownership.
	if _, err := store.GetPortalGroupReposForUser(ctx, strangerID, groupID, true); err != nil {
		t.Errorf("admin must be able to read any group: %v", err)
	}
}
