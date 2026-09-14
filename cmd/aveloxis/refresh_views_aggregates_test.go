// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"
)

// TestRefreshViewsHasAggregatesFlag — v0.28.18. configuration.md
// (v0.27.56) promised that with matview_rebuild_skip_dm_aggregates on,
// the dm_ tables "update only when an operator runs aveloxis refresh-views
// or aveloxis migrate" — but RefreshAllRepoAggregates had exactly ONE
// caller, the scheduler's weekly rebuild, so with the skip on the dm_
// tables never updated at all. refresh-views now carries --aggregates
// (default off: the per-repo loop runs for days at fleet scale) that runs
// RefreshAllRepoAggregates after the matviews. The matview refresh must
// stay unconditional.
func TestRefreshViewsHasAggregatesFlag(t *testing.T) {
	body := extractFuncBody(t, mainSource(t), "func refreshViewsCmd(")
	for _, needle := range []string{`"aggregates"`, `RefreshAllRepoAggregates(`, `RefreshMaterializedViews(`, `return errors.Join(viewErr, aggErr)`} {
		if !strings.Contains(body, needle) {
			t.Errorf("refreshViewsCmd must contain %s", needle)
		}
	}
	// Pass 27: a view failure must not skip the aggregate pass (with the
	// skip knob on it is the only way dm_ tables update).
	if strings.Contains(body, "RefreshMaterializedViews(ctx, store, logger); err != nil") {
		t.Error("refresh-views must not return on a view failure before the --aggregates pass — capture viewErr and join it with the aggregate pass's error")
	}
	if strings.Index(body, `RefreshAllRepoAggregates(`) < strings.Index(body, `RefreshMaterializedViews(`) {
		t.Error("the dm_ aggregate pass must run AFTER the matview refresh (the aggregates read the base tables, the matviews are what 8Knot serves first)")
	}
}
