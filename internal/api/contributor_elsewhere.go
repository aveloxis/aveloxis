// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// v0.27.64 — the read API over the v0.27.58 contributor daily
// history. Two surfaces:
//
//   - GET /repos/{repoID}/contributors/elsewhere — "where else are
//     this repo's top contributors active?" (authorizeRepo: it hangs
//     off a repo view).
//   - GET /contributors/{cntrbID}/activity — one person's cross-repo
//     monthly view. requireUser, NOT repo scope: person-level data
//     spans repos this instance doesn't even track, so repo scope
//     cannot express it — and anonymous access can't either.
//
// Both ride the shared 60s response cache and both carry
// backfilled_at so the frontend can render "history pending" instead
// of lying with zeros.

const elsewhereWindowDays = 180 // the v0.27.58 default history window

func (s *Server) handleContributorsElsewhere(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if !s.authorizeRepo(w, r, repoID) {
		return
	}
	since := time.Now().AddDate(0, 0, -elsewhereWindowDays)
	if sp := r.URL.Query().Get("since"); sp != "" {
		if t, err := time.Parse("2006-01-02", sp); err == nil {
			since = t
		}
	}
	limit := 10
	if lp := r.URL.Query().Get("limit"); lp != "" {
		if n, err := strconv.Atoi(lp); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 25 {
		limit = 25
	}

	key := fmt.Sprintf("elsewhere|%d|%s|%d", repoID, since.Format("2006-01-02"), limit)
	if body, ok := s.respCache.get(key); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		return
	}

	rows, err := s.store.ContributorsElsewhere(r.Context(), repoID, since, limit, 10)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	body, err := json.Marshal(map[string]any{
		"since":        since.Format("2006-01-02"),
		"limit":        limit,
		"contributors": rows,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.respCache.put(key, body)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func (s *Server) handleContributorActivity(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireUser(w, r); !ok {
		return
	}
	cntrbID := r.PathValue("cntrbID")
	if _, err := uuid.Parse(cntrbID); err != nil {
		http.Error(w, "invalid cntrb_id (must be a UUID)", http.StatusBadRequest)
		return
	}
	months := 24
	if mp := r.URL.Query().Get("months"); mp != "" {
		if n, err := strconv.Atoi(mp); err == nil && n > 0 {
			months = n
		}
	}
	if months > 60 {
		months = 60
	}
	// bucket is accepted for forward compatibility; only month exists.
	if b := r.URL.Query().Get("bucket"); b != "" && b != "month" {
		http.Error(w, "unsupported bucket (only 'month')", http.StatusBadRequest)
		return
	}

	key := fmt.Sprintf("cactivity|%s|%d", cntrbID, months)
	if body, ok := s.respCache.get(key); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		return
	}

	view, err := s.store.ContributorActivity(r.Context(), cntrbID, months)
	if err != nil {
		// Unknown contributor surfaces as the lookup error — a 404 is
		// the honest mapping (the id parsed but nobody has it).
		http.Error(w, "contributor not found", http.StatusNotFound)
		return
	}
	body, err := json.Marshal(view)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.respCache.put(key, body)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}
