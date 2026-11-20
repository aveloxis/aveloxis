// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// NOTE: the per-site classification pin that lived here is GONE — the
// L10 pass proved it decorative (it checked the token existed, not that
// the log became unreachable). scripts/shutdown_classification_test.go
// supersedes it repo-wide with the stronger rule.

// The v0.27.17 consolidation repoints several tables' repo_group_id and
// THEN deletes the loser groups. mailing_list_staging has no FK to
// repo_groups (schema.sql declares a bare `repo_group_id BIGINT`), so a
// failed repoint followed by a successful delete leaves dangling group
// ids that wedge DrainList for the whole list. execMigrationStep
// accumulates and CONTINUES, so the deletes must be gated on every
// repoint having succeeded (Copilot round 8).
func TestRepoGroupConsolidationFailsClosedOnAFailedRepoint(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, readSourceFile(t, "migrate.go"), "func consolidateRepoGroups("))

	base := strings.Index(body, "errsBeforeRepoint := len(*errs)")
	firstRepoint := strings.Index(body, "repoint repos.repo_group_id to canonical group")
	gate := strings.Index(body, "if len(*errs) > errsBeforeRepoint {")
	// 0.29.212: the dm_ deletes are the list repoGroupLoserRowTables, run
	// through repoGroupLoserRowsDeleteSQL (so that the write-policy check
	// runs each). The anchor is the loop that runs them, and the list is
	// checked to be the dm_ tables it was.
	dmDelete := strings.Index(body, "for i, tbl := range repoGroupLoserRowTables {")
	if dmDelete >= 0 && !strings.Contains(body[dmDelete:], "repoGroupLoserRowsDeleteSQL(i))") {
		dmDelete = -1
	}
	if len(repoGroupLoserRowTables) == 0 {
		t.Fatal("repoGroupLoserRowTables is empty: the dm_ deletes this pin orders are gone")
	}
	for _, tbl := range repoGroupLoserRowTables {
		if !strings.HasPrefix(tbl, "aveloxis_data.dm_repo_group_") {
			t.Errorf("repoGroupLoserRowTables holds %s: the deletes gated here are the dm_repo_group_ aggregates", tbl)
		}
	}
	loserDelete := strings.Index(body, "delete consolidated loser repo_groups rows")

	for _, a := range []struct {
		at   int
		what string
	}{{base, "the errs baseline"}, {firstRepoint, "the repos repoint"}, {gate, "the fail-closed gate"},
		{dmDelete, "the dm_ deletes"}, {loserDelete, "the loser-group delete"}} {
		if a.at < 0 {
			t.Fatalf("could not anchor %s — re-anchor this pin", a.what)
		}
	}
	if base > firstRepoint {
		t.Error("the errs baseline must be taken BEFORE the first repoint, or a failure in the repos repoint is invisible to the gate")
	}
	if gate > dmDelete || gate > loserDelete {
		t.Error("the fail-closed gate must precede BOTH delete phases — a delete over a partial repoint orphans mailing_list_staging.repo_group_id and wedges DrainList")
	}
	if !strings.Contains(body[gate:dmDelete], "return") {
		t.Error("the gate must RETURN on a failed repoint, not merely log: the deletes are what orphan the staging rows")
	}
}
