// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// GET /api/v1/repos/{repoID}/traffic serves a repository's stored GitHub
// traffic (aveloxis/aveloxis#234). The answer depends only on the
// repository, so it goes through the repository-page cache as an exact
// answer, and it reads no query parameter.
func TestRepoTrafficRouteIsAnExactCachedAnswer(t *testing.T) {
	const pattern = "GET /api/v1/repos/{repoID}/traffic"
	found := 0
	for _, reg := range repoGETRegistrations(t) {
		if reg.handler != "handleRepoTraffic" {
			continue
		}
		found++
		if "GET "+reg.path != pattern {
			t.Errorf("handleRepoTraffic serves %q, want %q", reg.path, pattern)
		}
		if !reg.cached || reg.policy != "pageExact" {
			t.Errorf("the traffic route must be registered through s.cachedRepoGET(pageExact, ...), got cached=%t policy=%q", reg.cached, reg.policy)
		}
	}
	if found != 1 {
		t.Fatalf("handleRepoTraffic is registered %d times, want once", found)
	}
	params, listed := pageParams[pattern]
	if !listed {
		t.Fatalf("pageParams has no entry for %s: the route would be served uncached", pattern)
	}
	if len(params) != 0 {
		t.Errorf("pageParams[%q] = %v; the handler reads no query parameter", pattern, params)
	}
}

// An id that is not an integer is a 400, through the whole chain.
func TestRepoTrafficRefusesANonIntegerID(t *testing.T) {
	s, err := NewWithOptions(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"abc", "1.5", "9223372036854775808"} {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/repos/"+id+"/traffic", nil)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Errorf("GET /api/v1/repos/%s/traffic = %d %q, want 400", id, w.Code, w.Body.String())
		}
	}
}

// The handler decides scope before it reads anything. The server has no
// store, so a lookup made before the check would panic on the nil store;
// an API token outside its owner's groups must get the structured 403.
// The prelude (repoFromPath) proves the repository exists first, so the
// bare server needs a state reader that knows the id.
func TestRepoTrafficAuthorizesBeforeAnyLookup(t *testing.T) {
	s, err := NewWithOptions(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	if err != nil {
		t.Fatal(err)
	}
	s.repoStates = func(_ context.Context, ids []int64) (map[int64]db.RepoCacheState, error) {
		out := map[int64]db.RepoCacheState{}
		for _, id := range ids {
			out[id] = db.RepoCacheState{HasQueueRow: true}
		}
		return out, nil
	}
	r := httptest.NewRequest(http.MethodGet, "/api/v1/repos/42/traffic", nil)
	r = r.WithContext(withIdentity(context.Background(), authInfo{UserID: 7, APITokenID: 3, Scope: map[int64]bool{41: true}}))
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "repo_out_of_scope") {
		t.Fatalf("an out-of-scope API token = %d %q, want the structured 403", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
		t.Errorf("a refusal is about this caller and must never be stored: %v", w.Header())
	}

	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/api", "func (s *Server) handleRepoTraffic("))
	prelude := strings.Index(body, "s.repoFromPath(")
	lookup := strings.Index(body, "s.store.")
	if prelude < 0 || lookup < 0 {
		t.Fatalf("handleRepoTraffic must reach the prelude and read through s.store (repoFromPath at %d, store at %d)", prelude, lookup)
	}
	if lookup < prelude {
		t.Error("handleRepoTraffic reads the store before the prelude: a caller out of scope would trigger a lookup")
	}
}
