// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

// v0.27.96 — F8 from summary/21: contributor_affiliations measured at
// 3,151 MB for 66,573 rows (~47 KB/row) on production. The bloat engine was
// the hourly PopulateAffiliations pass upserting EVERY domain with
// `DO UPDATE SET … ca_last_used = NOW()` — every row rewritten every hour,
// ~1.6M dead tuples/day on a 66K-row table. The pass is now delta-only:
// it loads the existing map once and writes only new or changed domains.
// (ca_last_used has no readers — verified 2026-08-18 — so it advancing only
// on actual change is a documented semantic, not a regression.)

// TestPopulateAffiliationsIsDeltaOnly pins the shape: existing-map load +
// skip-unchanged gate.
func TestPopulateAffiliationsIsDeltaOnly(t *testing.T) {
	src := readFileForTest(t, "affiliations_populate.go")
	if !strings.Contains(src, "SELECT ca_domain, ca_affiliation") {
		t.Error("PopulateAffiliations must load the existing domain→company " +
			"map before upserting — without it every row is rewritten every " +
			"hour (the 47×-bloat engine, summary/21 F8)")
	}
	if !strings.Contains(src, "existing[") {
		t.Error("PopulateAffiliations must skip domains whose company is " +
			"unchanged (compare against the loaded existing map)")
	}
}

// TestPopulateAffiliationsSecondRunWritesNothing proves the delta behavior
// end-to-end: with no contributor changes between runs, the second pass
// writes zero rows.
func TestPopulateAffiliationsSecondRunWritesNothing(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.pool.Close)

	if _, err := store.PopulateAffiliations(ctx); err != nil {
		t.Fatalf("first PopulateAffiliations: %v", err)
	}
	n, err := store.PopulateAffiliations(ctx)
	if err != nil {
		t.Fatalf("second PopulateAffiliations: %v", err)
	}
	if n != 0 {
		t.Errorf("second consecutive PopulateAffiliations wrote %d rows, want 0 — "+
			"the pass must be delta-only (unchanged domains skipped), otherwise "+
			"every hourly run rewrites the whole table and regrows the 47× bloat", n)
	}
}
