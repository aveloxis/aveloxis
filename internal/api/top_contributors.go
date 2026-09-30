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
// Computed from the base tables (repo_id-leading index slices), cached
// under the repository's collection generation (repoCache, v0.29.71): the
// underlying data changes per collection, so a repository page costs one
// query per collection (or per enrichment interval, the TTL). ORDER
// MATTERS: authorizeRepo
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
	// identities (App accounts, [bot] logins, logins ending in "bot" —
	// the k8s-ci-robot and pytorchmergebot class; db.displayBotLoginSQL).
	excludeBots := r.URL.Query().Get("bots") == "hide"

	// O11 option 4 (v0.29.71): cached under the repository's collection
	// generation — the heaviest API shape on kate (13 s mean, 589 s max).
	key, cacheable := s.generationKey(r.Context(), r, "handleTopContributors",
		fmt.Sprintf("topcontrib|%d|%s|%s|%d|%t", repoID, since.Format("2006-01-02"), until.Format("2006-01-02"), limit, excludeBots),
		[]int64{repoID})
	if cacheable {
		if v, ok := s.repoCache.get(key); ok {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "hit")
			_, _ = w.Write(v.([]byte))
			return
		}
	}

	rows, err := s.store.TopContributors(r.Context(), repoID, since, until, limit, excludeBots)
	if err != nil {
		s.serverError(w, r, "handleTopContributors", err)
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
		s.serverError(w, r, "handleTopContributors", err)
		return
	}
	if cacheable {
		s.repoCache.put(key, body)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}
