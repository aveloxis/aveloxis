// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"net/http"
	"strconv"
)

// handleRepoMetricSeries answers one temporal metric's series for one
// repository: /compare's body for `entities=repo:{id}` (0.29.119,
// summary/63 §4). It is the repository page's chart route, served by the
// repository-page cache (pageSeriesKept, keyed by its window: pageWindows),
// so a chart is computed once per collection, or when a week completes,
// instead of on every view.
//
// Unlike /compare it links nothing: authorization is the repository
// routes' (authorizeRepo, in repoFromPath or the cache wrapper), and
// opening a repository page is not a comparison, so nothing is recorded
// in the caller's Comparisons group.
func (s *Server) handleRepoMetricSeries(w http.ResponseWriter, r *http.Request) {
	repoID, ok := s.repoFromPath(w, r)
	if !ok {
		return
	}
	a, err := parseMetricSeriesArgs(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	entities := []entity{{Kind: "repo", RepoID: repoID, Label: "repo:" + strconv.FormatInt(repoID, 10)}}
	s.labelEntities(r, entities)
	cs, err := s.entitySeriesFor(r, entities[0], []int64{repoID}, a.def.ID, a.bucket, a.since, a.until, a.retentionThreshold, s.entityFirstActivity, s.metricSeriesAndParts)
	if err != nil {
		s.serverError(w, r, "handleRepoMetricSeries", err)
		return
	}
	s.writeJSON(w, r, compareBody(a.def, a.bucket, a.since, a.until, []compareSeries{cs}))
}
