// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Copilot round 8 on PR #191 found the shutdown-classification sweep of
// passes 28-37 had stopped at the scheduler package boundary: the
// structural analyzer (internal/scheduler/ticker_cancel_structural_test.go)
// says so itself — "Delegates in OTHER packages … are outside this pin".
// These are the internal/db sites it could not see. Each fires on every
// `stop serve` that lands mid-pass, and each is a keep-going loop, so a
// loop-top ctx.Err() guard cannot cover it: the call that OBSERVED the
// cancellation is already past the guard.
//
// The rule per site: the cancellation classification must sit BETWEEN
// the producing call and the failure log, so the log never fires.
func TestShutdownIsNotAFailureInDBKeepGoingLoops(t *testing.T) {
	for _, tc := range []struct {
		file, fn string
		// producer → the call whose error is classified;
		// classify → the classification that must follow it;
		// log      → the failure log that must NOT be reached first.
		producer, classify, logCall, why string
	}{
		{
			file: "matviews.go", fn: "func RefreshMaterializedViews(",
			// Anchored on the FALLBACK exec, not the CONCURRENTLY one:
			// the classification that actually guards the log is the
			// one after the retry. (The earlier one merely skips a
			// doomed retry — removing it is not a regression, so
			// anchoring there would make this pin decorative.)
			producer: `REFRESH MATERIALIZED VIEW %s`,
			classify: `errors.Is(err, context.Canceled)`,
			logCall:  `logger.Warn("failed to refresh materialized view"`,
			why:      "a canceled CONCURRENTLY refresh was retried non-concurrently on the same dead ctx, so the pair landed as a WARN plus a stale-view failure on every stop",
		},
		{
			file: "aggregates.go", fn: "func (s *PostgresStore) RefreshAllRepoAggregates(ctx context.Context, logger interface {",
			producer: `s.RefreshRepoAggregates(ctx, repoID)`,
			classify: `errors.Is(err, context.Canceled)`,
			logCall:  `logger.Warn("aggregate refresh failed"`,
			why:      "shutdown was recorded as a per-repo aggregate failure and a stale-row error",
		},
		{
			file: "aggregates.go", fn: "func (s *PostgresStore) RefreshAllRepoAggregates(ctx context.Context, logger interface {",
			producer: `s.RefreshGroupAggregates(ctx, groupID)`,
			classify: `errors.Is(err, context.Canceled)`,
			logCall:  `logger.Warn("group aggregate refresh failed"`,
			why:      "same misclassification in the group loop",
		},
	} {
		body := srctest.StripGoComments(srctest.FuncBody(t, readSourceFile(t, tc.file), tc.fn))
		prod := strings.Index(body, tc.producer)
		logAt := strings.Index(body, tc.logCall)
		if prod < 0 || logAt < 0 {
			t.Errorf("%s: could not anchor producer (%d) / log (%d) — re-anchor this pin", tc.file, prod, logAt)
			continue
		}
		cls := strings.Index(body[prod:logAt], tc.classify)
		if cls < 0 {
			t.Errorf("%s %s: %q must classify %s between the call and the log — %s",
				tc.file, tc.producer, tc.classify, tc.logCall, tc.why)
		}
	}
}

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
	dmDelete := strings.Index(body, "dm_repo_group_annual")
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
