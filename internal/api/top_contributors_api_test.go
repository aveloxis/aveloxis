// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"os"
	"strings"
	"testing"
)

// v0.27.61 — GET /repos/{repoID}/contributors/top: route registration,
// the authz-before-cache ordering, and the limit clamp.

func TestTopContributorsRouteRegistered(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), `GET /api/v1/repos/{repoID}/contributors/top`) {
		t.Error("route GET /api/v1/repos/{repoID}/contributors/top not registered in server.go")
	}
	if !strings.Contains(string(src), "s.handleTopContributors") {
		t.Error("route must dispatch to s.handleTopContributors")
	}
}

// The response cache must NEVER be consulted before authorizeRepo: since
// v0.29.73 the route's cachedRepoGET does the lookup, pinned by
// TestCachedRepoGETAuthorizesBeforeAnyLookup; the route must go through it.
func TestTopContributorsAuthzBeforeCache(t *testing.T) {
	for _, reg := range repoGETRegistrations(t) {
		if reg.handler == "handleTopContributors" {
			if !reg.cached || reg.policy != "pageEnriched" {
				t.Errorf("contributors/top must be served through s.cachedRepoGET(pageEnriched, …), got cached=%t policy=%q", reg.cached, reg.policy)
			}
			return
		}
	}
	t.Error("contributors/top registration not found")
}

// Limit: default 20, hard cap 100 (an unbounded limit walks the whole
// contributor set through the identity join for no UI benefit). The
// handler reads ?limit= through topContributorsLimit
// (TestFoldingHandlersReadTheQueryOnlyThroughTheirParser), so this is its behavior.
func TestTopContributorsLimitClamp(t *testing.T) {
	for in, want := range map[string]int{
		"": 20, "abc": 20, "0": 20, "-3": 20, "1.5": 20,
		"1": 1, "07": 7, "20": 20, "99": 99, "100": 100, "101": 100, "100000": 100,
	} {
		if got := topContributorsLimit(in); got != want {
			t.Errorf("topContributorsLimit(%q) = %d, want %d", in, got, want)
		}
	}
}
