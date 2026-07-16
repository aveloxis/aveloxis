// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.14 — the group page's repo listing paginates and carries
// ALL-TIME counts + star state per row.
//
// OPERATOR DECISION (2026-07-15): the counts are the ALL-TIME totals
// from the latest repo_info snapshot — deliberately NOT the 90-day
// activity metric, which caused the v0.27.4 nginx-timeout incident at
// fleet scale and would need per-group caching we don't want here.
// Counts are fetched per PAGE only, via the existing batched-stats
// machinery (GetRepoStatsBatch), never for the whole group.

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

// TestHandleGroupReposEnvelope pins the wiring: pagination flows to
// the store, per-PAGE counts come from GetRepoStatsBatch, star state
// from GetUserStarredRepoIDs, and the response is an envelope with
// total/page/page_size (the GUI pager reads it).
func TestHandleGroupReposEnvelope(t *testing.T) {
	src := mustReadFile(t, "portal.go")
	body := extractFuncBody(t, src, "handleGroupRepos")
	for _, needle := range []string{
		"parsePortalPage(", "GetRepoStatsBatch(", "GetUserStarredRepoIDs(",
		`"total"`, `"page"`, `"page_size"`,
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("handleGroupRepos must contain %q — pagination envelope + per-page counts + star annotation", needle)
		}
	}
	// The all-time counts read the repo_info metadata totals — NOT any
	// 90-day activity query (the v0.27.4 nginx-timeout class).
	storeSrc := mustReadFile(t, "../db/portal_store.go")
	for _, needle := range []string{`json:"commits_all_time"`, `json:"issues_all_time"`, `json:"prs_all_time"`, `json:"starred"`} {
		if !strings.Contains(storeSrc, needle) {
			t.Errorf("PortalGroupRepo must declare %s — labeled all-time so the GUI can say so", needle)
		}
	}
	if strings.Contains(storeSrc, "90 days") || strings.Contains(storeSrc, "INTERVAL '90") {
		t.Error("group-page counts must be ALL-TIME, never the 90-day activity metric (operator decision; v0.27.4 timeout incident)")
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
