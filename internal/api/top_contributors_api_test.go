// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
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

// The response cache must NEVER be consulted before authorizeRepo: a
// cached body for an authorized user must not leak to an unauthorized
// one. Pin the ordering at the source level (authorizeRepo appears
// before the first cache get inside the handler body).
func TestTopContributorsAuthzBeforeCache(t *testing.T) {
	// v0.29.71: both repository-page handlers now read the collection-
	// generation cache (repoCache); the time series joined the pin then
	// (review round 1 F3 — the retargeted pin had been left failing).
	for _, h := range []struct{ file, fn string }{
		{"top_contributors.go", "handleTopContributors"},
		{"server.go", "handleTimeSeries"},
	} {
		body := srctest.StripGoComments(extractFuncBody(t, mustReadFile(t, h.file), h.fn))
		authz := strings.Index(body, "s.authorizeRepo(")
		cacheGet := strings.Index(body, "s.repoCache.get(")
		if authz < 0 || cacheGet < 0 {
			t.Errorf("%s must call s.authorizeRepo (%d) and read s.repoCache.get (%d)", h.fn, authz, cacheGet)
			continue
		}
		if cacheGet < authz {
			t.Errorf("%s: the cache lookup must come AFTER authorizeRepo — a cached body must never bypass repo scope", h.fn)
		}
	}
}

// Limit: default 20, hard cap 100 (an unbounded limit walks the whole
// contributor set through the identity join for no UI benefit).
func TestTopContributorsLimitClamp(t *testing.T) {
	src, err := os.ReadFile("top_contributors.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "limit := 20") {
		t.Error("default limit must be 20")
	}
	if !strings.Contains(string(src), "limit > 100") {
		t.Error("limit must be capped at 100")
	}
}
