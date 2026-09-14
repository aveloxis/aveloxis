// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The dm_repo_group_* rebuild is per GROUP, not per repo (v0.28.19).
//
// Every group statement is scoped `WHERE r.repo_group_id = $1`, so the
// work is proportional to the whole group. Driving it from the repo
// list rebuilt the entire group once per member — and after the
// v0.27.17 consolidation collapsed 93,912 per-repo `Default` groups
// into one, that is N identical DELETE + aggregate + INSERT passes over
// three tables for a group of N repos (measured locally: 8,766), all
// while MatviewRebuildActive pauses collection claims. Same class as
// the v0.16.5 per-repo aggregate refresh, reintroduced by
// consolidation.
func TestGroupAggregatesRefreshOncePerGroup(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/db/aggregates.go"))
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/aggregates.go"),
		"func (s *PostgresStore) RefreshAllRepoAggregates("))

	if !strings.Contains(body, "range groupIDs") {
		t.Error("RefreshAllRepoAggregates must iterate GROUP ids for the dm_repo_group_* phase — driving it from repoIDs rebuilds the whole group once per member")
	}
	if strings.Contains(body, "RefreshRepoGroupAggregates(ctx, repoID)") {
		t.Error("RefreshAllRepoAggregates still calls RefreshRepoGroupAggregates(ctx, repoID) in a loop over repos. That helper resolves the repo's group and then rebuilds the ENTIRE group, so a group of N repos does N identical rebuilds of the same rows.")
	}
	if !strings.Contains(src, "func (s *PostgresStore) RefreshGroupAggregates(ctx context.Context, groupID int64) error") {
		t.Error("RefreshGroupAggregates(ctx, groupID) must exist so the fleet pass can drive the group phase directly; RefreshRepoGroupAggregates stays for per-repo manual recalculation and should delegate to it (one definition)")
	}
	if !strings.Contains(srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/aggregates.go"),
		"func (s *PostgresStore) RefreshRepoGroupAggregates(")), "s.RefreshGroupAggregates(ctx, *rgID)") {
		t.Error("RefreshRepoGroupAggregates must delegate to RefreshGroupAggregates rather than carry a second copy of the statements — two copies of a rebuild WILL drift")
	}
}

// The group-id query must be fully consumed before the refresh loop.
// Pass 28 deleted an earlier `SELECT DISTINCT repo_group_id` because it
// held its Rows open across the loop, pinning a pool connection for the
// whole pass, and buried its own error behind ten repo failures. Those
// were the consumption pattern, not the query — re-adding the query
// must not re-add them.
func TestGroupIDQueryIsConsumedBeforeTheLoop(t *testing.T) {
	src := srctest.Read(t, "internal/db/aggregates.go")
	helper := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *PostgresStore) distinctRepoGroupIDs("))
	if !strings.Contains(helper, "defer rows.Close()") || !strings.Contains(helper, "rows.Err()") {
		t.Error("distinctRepoGroupIDs must close its Rows and check rows.Err() — a mid-stream failure would otherwise truncate the group list silently, skipping groups' aggregates without reporting anything")
	}
	if !strings.Contains(helper, "return out, rows.Err()") {
		t.Error("distinctRepoGroupIDs must return the accumulated slice, not the open Rows: holding them across the refresh loop pins a pool connection for the entire pass (pass 28)")
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *PostgresStore) RefreshAllRepoAggregates("))
	enumerate := strings.Index(body, "distinctRepoGroupIDs(ctx)")
	loop := strings.Index(body, "range groupIDs")
	if enumerate < 0 || loop < 0 || enumerate > loop {
		t.Error("the group ids must be enumerated BEFORE the refresh loop begins")
	}
	if !strings.Contains(body, "enumerating repo groups") {
		t.Error("a failure to enumerate groups must surface with its own cause, not be swallowed — pass 28's version buried it behind ten repo failures")
	}
}
