// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// repo_star_state_test.go — v0.27.85 GET /repos/{repoID}/star. The
// repo page grew a star toggle (2026-08-05 operator ask: "probably any
// repo page should allow the user to star or unstar the repo") and no
// endpoint returned a single repo's starred state for the caller —
// only list-shaped sources (search rows, /home/repos, collection rows)
// carried it.

package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRepoStarStateRouteAndHandler(t *testing.T) {
	src := mustReadFile(t, "server.go")
	if !strings.Contains(src, `"GET /api/v1/repos/{repoID}/star"`) {
		t.Error("server.go must register GET /api/v1/repos/{repoID}/star " +
			"(the repo page's starred-state read, v0.27.85)")
	}
	portal := mustReadFile(t, "portal.go")
	if !strings.Contains(portal, "func (s *Server) handleRepoStarState(") {
		t.Error("portal.go must define handleRepoStarState")
	}
	if !strings.Contains(portal, "IsRepoStarred(") {
		t.Error("handleRepoStarState must read the state via store.IsRepoStarred " +
			"(a targeted EXISTS — not GetUserStarredRepoIDs, which loads the " +
			"caller's whole star set)")
	}
}

// TestRepoStarStateRequiresIdentity: starred state is caller-personal,
// so the handler demands identity unconditionally (requireUser — the
// same posture as PUT/DELETE star), even with require_auth off.
func TestRepoStarStateRequiresIdentity(t *testing.T) {
	store := &fakeSessionStore{userID: 7, scope: []int64{42}, valid: map[string]bool{"tok": true}}
	s := &Server{}
	s.auth = newAuthenticator(store, false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/repos/42/star", nil)
	req.SetPathValue("repoID", "42")
	s.handleRepoStarState(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("starred-state read without identity must 401, got %d", rec.Code)
	}
}
