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
)

// TestUpsertRepoReportsWhyTheDefaultGroupFailed (v0.29.57, worklist 38; SR-5).
// UpsertRepo creates the 'Default' repo group with ON CONFLICT (rg_name) DO
// NOTHING and, on ANY error, ran a lookup whose error it discarded, returning
// only "failed to resolve default repo group". A fresh test database made that
// real: a test that inserted a group with an explicit repo_group_id = 1 left
// the BIGSERIAL sequence behind it, the Default insert then drew id 1 and hit
// the primary key, and the message named none of it. A failure that is not
// the rg_name conflict is now returned as itself.
func TestUpsertRepoReportsWhyTheDefaultGroupFailed(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	tag := time.Now().UnixNano()

	// No 'Default' may exist, or the insert is never attempted: move any
	// existing one aside for the duration of the test.
	held := fmt.Sprintf("_avtest_default_held_%d", tag)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repo_groups SET rg_name = $1 WHERE rg_name = 'Default'`, held)
	t.Cleanup(func() {
		c := context.Background()
		// A 'Default' the test created (only when UpsertRepo wrongly
		// succeeded) goes before the original is renamed back — the repo
		// that referenced it is deleted first (cleanups run last-in first-out).
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.repo_groups WHERE rg_name = 'Default' AND EXISTS (SELECT 1 FROM aveloxis_data.repo_groups WHERE rg_name = $1)`, held)
		cleanupExecRetry(c, store, `UPDATE aveloxis_data.repo_groups SET rg_name = 'Default' WHERE rg_name = $1`, held)
	})

	// Occupy the id the sequence hands out next — the explicit-id fixture
	// shape that broke the scheduler suite under -shuffle.
	var next int64
	mustQueryRowRetry(ctx, t, store, `SELECT nextval(pg_get_serial_sequence('aveloxis_data.repo_groups', 'repo_group_id')) + 1`, &next)
	blocker := fmt.Sprintf("_avtest_seq_blocker_%d", tag)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_groups (repo_group_id, rg_name) VALUES ($1, $2)`, next, blocker)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_groups WHERE rg_name = $1`, blocker)
	})

	url := fmt.Sprintf("https://github.com/_avtestdefgroup/r%d", tag)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repos WHERE repo_git = $1`, url)
	})
	_, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitHub, GitURL: url, Owner: "_avtestdefgroup", Name: fmt.Sprintf("r%d", tag)})
	if err == nil {
		t.Fatal("UpsertRepo must fail when the Default group cannot be created")
	}
	if !strings.Contains(err.Error(), "repo_groups_pkey") {
		t.Errorf("UpsertRepo must say why the Default group could not be created (the primary-key collision), got: %v", err)
	}
}
