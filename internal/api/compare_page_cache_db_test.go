// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// D5 (0.29.121, summary/63 §7): the Comparisons page's repository entities
// read their series through the repository-page cache — the entry the
// repository page warms — so a repository compared and viewed shares one
// answer, durable until it is collected again.

import (
	"log/slog"
	"strconv"
	"strings"
	"testing"
)

func pageEntriesFor(s *Server, uriPart string) (n, hits int) {
	s.pageCache.mu.Lock()
	defer s.pageCache.mu.Unlock()
	for _, el := range s.pageCache.m {
		if e := el.Value.(*pageEntry); strings.Contains(e.uri, uriPart) {
			n++
			hits += e.hits
		}
	}
	return n, hits
}

func TestCompareRepoEntityFillsTheRepositoryPageEntry(t *testing.T) {
	s, _, repoID, userID := metricSeriesFixture(t)
	id := strconv.FormatInt(repoID, 10)
	if w := metricSeriesCall(t, s, "/api/v1/compare?entities=repo:"+id+"&metric=burstiness", userID); w.Code != 200 {
		t.Fatalf("compare: %d %s", w.Code, w.Body.String())
	}
	if n, _ := pageEntriesFor(s, "/repos/"+id+"/metric-series"); n != 1 {
		t.Fatalf("a comparison stored %d repository-page entries for the series, want 1", n)
	}
	if w := metricSeriesCall(t, s, "/api/v1/repos/"+id+"/metric-series?metric=burstiness", userID); w.Header().Get("X-Cache") != "hit" {
		t.Errorf("the repository page's chart after a comparison: X-Cache %q, want hit (the comparison's entry)", w.Header().Get("X-Cache"))
	}
}

func TestCompareRepoEntityReadsTheRepositoryPageEntry(t *testing.T) {
	s, _, repoID, userID := metricSeriesFixture(t)
	id := strconv.FormatInt(repoID, 10)
	if w := metricSeriesCall(t, s, "/api/v1/repos/"+id+"/metric-series?metric=committers&bucket=month", userID); w.Code != 200 {
		t.Fatalf("metric-series: %d %s", w.Code, w.Body.String())
	}
	_, before := pageEntriesFor(s, "/repos/"+id+"/metric-series")
	if w := metricSeriesCall(t, s, "/api/v1/compare?entities=repo:"+id+"&metric=committers&bucket=month", userID); w.Code != 200 {
		t.Fatalf("compare: %d %s", w.Code, w.Body.String())
	}
	n, after := pageEntriesFor(s, "/repos/"+id+"/metric-series")
	if n != 1 || after != before+1 {
		t.Errorf("the comparison must be served by the repository page's entry (entries %d, hits %d → %d)", n, before, after)
	}
}

// Round 1 finding 5: a repository entity nobody has is the page cache's
// definitive 404, not a failure: the comparison answers its empty series as
// before 0.29.121, with no WARN for an anonymous client to multiply.
func TestCompareUnknownRepoEntityLogsNoFailure(t *testing.T) {
	s, _, _, userID := metricSeriesFixture(t)
	var logs lockedBuffer
	s.logger = slog.New(slog.NewTextHandler(&logs, nil))
	w := metricSeriesCall(t, s, "/api/v1/compare?entities=repo:2147480000&metric=issues", userID)
	if w.Code != 200 {
		t.Fatalf("compare of an unknown repository: %d %s", w.Code, w.Body.String())
	}
	if strings.Contains(logs.String(), "through the page cache failed") {
		t.Errorf("an unknown repository must not log a page-cache failure:\n%s", logs.String())
	}
}
