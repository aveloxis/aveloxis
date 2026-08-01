// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// handleTopContributors (v0.27.61) — GET /repos/{repoID}/contributors/top
//
// The ranked per-contributor activity breakdown behind the repo page's
// "Top contributors" card: ?since / ?until windows (parseWindow — same
// semantics as the other contributions endpoints), ?limit rows
// (default 20, capped at 100).
//
// Computed live from the base tables (repo_id-leading index slices —
// the same cost class as the per-request /contributors metric), behind
// the 60s response cache: the underlying data only changes per
// collection cycle, so a shared dashboard hitting the same repo
// repeatedly costs one query per minute. ORDER MATTERS: authorizeRepo
// runs BEFORE the cache lookup — a cached body must never leak past
// repo scope (pinned by test).
func (s *Server) handleTopContributors(w http.ResponseWriter, r *http.Request) {
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
	limit := 20
	if lp := r.URL.Query().Get("limit"); lp != "" {
		if n, err := strconv.Atoi(lp); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 100 {
		limit = 100
	}
	// v0.27.69 — the "hide bots" checkbox: ?bots=hide filters bot
	// identities (App accounts, [bot] logins, -bot/-robot machine
	// accounts — the k8s-ci-robot class).
	excludeBots := r.URL.Query().Get("bots") == "hide"

	key := fmt.Sprintf("topcontrib|%d|%s|%s|%d|%t", repoID, since.Format("2006-01-02"), until.Format("2006-01-02"), limit, excludeBots)
	if body, ok := s.respCache.get(key); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		return
	}

	rows, err := s.store.TopContributors(r.Context(), repoID, since, until, limit, excludeBots)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	envelope := map[string]any{
		"since":        since,
		"until":        until,
		"limit":        limit,
		"contributors": rows,
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.respCache.put(key, body)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}
