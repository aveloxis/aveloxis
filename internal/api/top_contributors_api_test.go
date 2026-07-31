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

// The response cache must NEVER be consulted before authorizeRepo: a
// cached body for an authorized user must not leak to an unauthorized
// one. Pin the ordering at the source level (authorizeRepo appears
// before the first cache get inside the handler body).
func TestTopContributorsAuthzBeforeCache(t *testing.T) {
	src, err := os.ReadFile("top_contributors.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	authz := strings.Index(body, "s.authorizeRepo(")
	cacheGet := strings.Index(body, "s.respCache.get(")
	if authz < 0 {
		t.Fatal("handleTopContributors must call s.authorizeRepo")
	}
	if cacheGet < 0 {
		t.Fatal("handleTopContributors must use the 60s response cache (s.respCache.get)")
	}
	if cacheGet < authz {
		t.Error("response-cache lookup must come AFTER authorizeRepo — a cached body must never bypass repo scope")
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
