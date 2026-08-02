// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.14 — the group page's repo listing paginates and carries
// per-row counts + star state.
// v0.27.75 — the listing adopts the COLLECTIONS table grammar
// (operator request 2026-08-02): cached queue counts, last_collected,
// forge-reported last_activity, starred, and server-side sort through
// the SAME allowlist as GetCollectionRepos. The counts remain
// pre-cached values — never a per-request aggregation (the v0.27.4
// nginx-timeout class).

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParsePortalPage(t *testing.T) {
	cases := []struct {
		query    string
		wantPage int
		wantSize int
	}{
		{"", 1, 50},                      // defaults
		{"page=3", 3, 50},                // page only
		{"page_size=25", 1, 25},          // size only
		{"page=2&page_size=100", 2, 100}, // both
		{"page_size=500", 1, 100},        // cap at 100
		{"page=0&page_size=0", 1, 50},    // zero → defaults
		{"page=-4&page_size=-9", 1, 50},  // negative → defaults
		{"page=x&page_size=y", 1, 50},    // garbage → defaults
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "/api/v1/groups/1/repos?"+c.query, nil)
		page, size := parsePortalPage(r)
		if page != c.wantPage || size != c.wantSize {
			t.Errorf("parsePortalPage(%q) = (%d, %d), want (%d, %d)", c.query, page, size, c.wantPage, c.wantSize)
		}
	}
}

// TestHandleGroupReposEnvelope pins the v0.27.75 wiring: pagination
// AND sort params flow to the store, the effective sort/dir are echoed
// in the envelope alongside total/page/page_size, and the row shape is
// the collections table grammar (cached counts + dates + starred from
// the store — no per-page annotation loop in the handler).
func TestHandleGroupReposEnvelope(t *testing.T) {
	src := mustReadFile(t, "portal.go")
	body := extractFuncBody(t, src, "handleGroupRepos")
	for _, needle := range []string{
		"parsePortalPage(", `Get("sort")`, `Get("dir")`,
		"CollectionRepoSortValid(",
		`"total"`, `"page"`, `"page_size"`, `"sort"`, `"dir"`,
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("handleGroupRepos must contain %q — pagination + allowlisted sort with effective-value echo", needle)
		}
	}
	// The v0.27.14 per-page annotation loop is GONE — the store fills
	// counts and stars in the listing query itself.
	for _, banned := range []string{"GetRepoStatsBatch(", "GetUserStarredRepoIDs("} {
		if strings.Contains(body, banned) {
			t.Errorf("handleGroupRepos must NOT call %s — the store fills the whole row (v0.27.75)", banned)
		}
	}
	storeSrc := mustReadFile(t, "../db/portal_store.go")
	// The collections row shape, verbatim (v0.27.74 field names — the
	// two GUI tables render from identical JSON).
	for _, needle := range []string{
		`json:"issues"`, `json:"prs"`, `json:"commits"`,
		`json:"last_collected,omitempty"`, `json:"last_activity,omitempty"`, `json:"starred"`,
	} {
		if !strings.Contains(storeSrc, needle) {
			t.Errorf("PortalGroupRepo must declare %s — the collections table grammar", needle)
		}
	}
	// Counts come from the queue's CACHED totals + the repo_info
	// LATERAL; sort resolves through the SHARED allowlist. Never a
	// windowed per-request aggregation (the v0.27.4 timeout class).
	for _, needle := range []string{"last_issues", "last_updated", "user_repo_stars", "collectionRepoSorts"} {
		if !strings.Contains(storeSrc, needle) {
			t.Errorf("GetPortalGroupReposForUser must use %s — cached counts + shared sort allowlist", needle)
		}
	}
	if strings.Contains(storeSrc, "90 days") || strings.Contains(storeSrc, "INTERVAL '90") {
		t.Error("group-page counts must be cached totals, never the 90-day activity metric (operator decision; v0.27.4 timeout incident)")
	}
}

// TestHomeReposDefaultLimit50 pins the v0.27.14 home-list capacity
// raise (20 → 50) at both the store default and the documented shape.
func TestHomeReposDefaultLimit50(t *testing.T) {
	storeSrc := mustReadFile(t, "../db/home_store.go")
	if !strings.Contains(storeSrc, "limit = 50") {
		t.Error("GetHomeRepos default limit must be 50 (v0.27.14 raise from 20)")
	}
	docs := mustReadFile(t, "../../docs/guide/api.md")
	if !strings.Contains(docs, "limit=50") {
		t.Error("docs/guide/api.md must show the home/repos default limit as 50")
	}
}
