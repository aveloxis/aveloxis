// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"net/http"
)

// handleRepoTraffic serves the repository's stored traffic in one
// answer, without paging (v0.29.118, aveloxis/aveloxis#234). A repository
// without traffic answers empty arrays. The route is cachedRepoGET, and
// every traffic writer stamps data_changed_at, so a write moves the answer.
func (s *Server) handleRepoTraffic(w http.ResponseWriter, r *http.Request) {
	repoID, ok := s.repoFromPath(w, r)
	if !ok {
		return
	}
	traffic, err := s.store.GetRepoTraffic(r.Context(), repoID)
	if err != nil {
		s.serverError(w, r, "handleRepoTraffic", err)
		return
	}
	s.writeJSON(w, r, traffic)
}
