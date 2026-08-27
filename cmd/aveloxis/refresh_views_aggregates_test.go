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
	for _, needle := range []string{`"aggregates"`, `RefreshAllRepoAggregates(`, `RefreshMaterializedViews(`} {
		if !strings.Contains(body, needle) {
			t.Errorf("refreshViewsCmd must contain %s", needle)
		}
	}
	if strings.Index(body, `RefreshAllRepoAggregates(`) < strings.Index(body, `RefreshMaterializedViews(`) {
		t.Error("the dm_ aggregate pass must run AFTER the matview refresh (the aggregates read the base tables, the matviews are what 8Knot serves first)")
	}
}
