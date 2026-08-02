// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// handleNewRepos (v0.27.62) — GET /home/new-repos?days=30
//
// The "New Repositories" home feed: repos that entered the fleet in
// the trailing window, split into two arms — `fleet` (repos under
// orgs registered by admin users: the curated what's-new-on-this-
// instance signal) and `mine` (repos under orgs the CALLER
// registered). Window default 30 days, capped at 90 (repos.added_at
// is indexed DESC; a longer window is a browse, not a feed).
//
// requireUser (not repo scope): the feed spans the whole fleet by
// design — it is discovery surface, same posture as /repos/search.
// Responses ride the shared 60s body cache keyed per (user, days) —
// the mine arm is user-specific so the key must carry the user.
func (s *Server) handleNewRepos(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	days := 30
	if dp := r.URL.Query().Get("days"); dp != "" {
		if n, err := strconv.Atoi(dp); err == nil && n > 0 {
			days = n
		}
	}
	if days > 90 {
		days = 90
	}

	key := fmt.Sprintf("newrepos|%d|%d", info.UserID, days)
	if body, ok := s.respCache.get(key); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		return
	}

	since := time.Now().AddDate(0, 0, -days)
	fleet, mine, err := s.store.GetNewRepos(r.Context(), info.UserID, since, 200)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	body, err := json.Marshal(map[string]any{
		"days":  days,
		"fleet": fleet,
		"mine":  mine,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.respCache.put(key, body)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}
