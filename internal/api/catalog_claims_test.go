// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// summary/18 Phase 5c tripwire (6), v0.27.43: metric-catalog
// DEFINITIONS must match their SQL predicates. The audit found the
// catalog claiming "Soft-deleted merge-loser identities excluded"
// while the SQL performed no such filter — a presented definition
// asserting a filter that didn't exist. The claim became true in
// v0.27.37 (1f); this keeps text and SQL from drifting apart again.

package api

import (
	"os"
	"strings"
	"testing"
)

func TestCatalogSoftDeletedClaimsMatchSQL(t *testing.T) {
	catalog, err := os.ReadFile("analytics.go")
	if err != nil {
		t.Fatal(err)
	}
	sqlSrc, err := os.ReadFile("../db/analytics_store.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(catalog), "Soft-deleted") {
		// The claim was removed from the catalog — the pin's premise is
		// gone; whoever removes it should also revisit this test.
		t.Skip("catalog no longer claims soft-deleted exclusion")
	}
	// The claimed filter must exist in BOTH people metrics.
	s := string(sqlSrc)
	for _, metric := range []string{`"contributors": `, `"committers": `} {
		idx := strings.Index(s, metric)
		if idx < 0 {
			t.Fatalf("metric %s not found in analytics_store.go", metric)
		}
		end := strings.Index(s[idx:], "GROUP BY 1 ORDER BY 1")
		if end < 0 {
			t.Fatalf("cannot bound %s SQL", metric)
		}
		if !strings.Contains(s[idx:idx+end], "cntrb_deleted") {
			t.Errorf("catalog claims soft-deleted exclusion but %s SQL has no cntrb_deleted filter — the definition would be asserting a filter that does not exist (the pre-v0.27.37 drift)", metric)
		}
	}
}
