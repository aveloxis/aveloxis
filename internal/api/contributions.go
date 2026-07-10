// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// parseWindow parses ?since=YYYY-MM-DD and ?until=YYYY-MM-DD off the
// request and returns a (since, until) pair shaped exactly like the
// existing handleTimeSeries window:
//
//   - default since: 2 years ago.
//   - default until: zero time, which the store layer treats as "no
//     upper bound" via resolveWindow.
//   - until is treated as INCLUSIVE — we advance it one day before
//     handing off to the store, so the store's exclusive-upper
//     comparison includes the end-date.
//
// An invalid date in either field reverts that field to its default
// instead of erroring, matching the existing timeseries behavior so
// charts and dashboards keep rendering on malformed input.
//
// Returns ok=false if since >= until after parsing — the caller
// surfaces a 400 in that case.
func parseWindow(r *http.Request) (since, until time.Time, ok bool) {
	since = time.Now().AddDate(-2, 0, 0)
	if sinceParam := r.URL.Query().Get("since"); sinceParam != "" {
		if t, err := time.Parse("2006-01-02", sinceParam); err == nil {
			since = t
		}
	}
	var u time.Time
	if untilParam := r.URL.Query().Get("until"); untilParam != "" {
		if t, err := time.Parse("2006-01-02", untilParam); err == nil {
			// Inclusive end-date: shift by one day so the store's
			// exclusive-upper compare (< $3) covers the requested
			// final calendar day.
			u = t.AddDate(0, 0, 1)
		}
	}
	if !u.IsZero() && !since.Before(u) {
		return since, u, false
	}
	return since, u, true
}

// handleRepoContributors returns all distinct contributors who made any
// kind of contribution to the repo in the requested window. See the
// store-side GetRepoContributors docstring and docs/guide/api.md for
// the full inclusion list and known limitations.
func (s *Server) handleRepoContributors(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if !s.authorizeRepo(w, r, repoID) {
		return
	}
	since, until, ok := parseWindow(r)
	if !ok {
		http.Error(w, "since must be before until", http.StatusBadRequest)
		return
	}

	contribs, err := s.store.GetRepoContributors(r.Context(), repoID, since, until)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	setCORSIfLocalhost(r, w)
	_ = json.NewEncoder(w).Encode(contribs)
}

// handleRepoContributionsCoverage returns the enrichment-state
// snapshot for the same cohort as /identities and /affiliations: how
// many contributors have been enriched / have a canonical email / have
// a resolved gh_user_id / have an affiliation. The "are my numbers
// trustworthy?" endpoint — operators read this before drawing
// conclusions from /affiliations. See store-side
// GetRepoContributionsCoverage and docs/guide/api.md for the full
// semantics of each field.
func (s *Server) handleRepoContributionsCoverage(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if !s.authorizeRepo(w, r, repoID) {
		return
	}
	since, until, ok := parseWindow(r)
	if !ok {
		http.Error(w, "since must be before until", http.StatusBadRequest)
		return
	}

	cov, err := s.store.GetRepoContributionsCoverage(r.Context(), repoID, since, until)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	setCORSIfLocalhost(r, w)
	_ = json.NewEncoder(w).Encode(cov)
}

// handleRepoAffiliations returns the per-affiliation contributor count
// for the same window. The "(unknown)" bucket is included in the
// response so the caller can decide whether to surface or hide it.
func (s *Server) handleRepoAffiliations(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if !s.authorizeRepo(w, r, repoID) {
		return
	}
	since, until, ok := parseWindow(r)
	if !ok {
		http.Error(w, "since must be before until", http.StatusBadRequest)
		return
	}

	counts, err := s.store.GetRepoAffiliationCounts(r.Context(), repoID, since, until)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	setCORSIfLocalhost(r, w)
	_ = json.NewEncoder(w).Encode(counts)
}
