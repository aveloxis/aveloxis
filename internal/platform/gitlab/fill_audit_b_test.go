// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.104 — Workstream B captures on the GitLab client (fork-owner ref,
// project topics, project creation date). Platform parity with the GitHub
// captures per the both-platforms house rule.
package gitlab

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// B5 — REVISED in v0.27.121 (Copilot round 13, suppressed — verified
// LIVE against gitlab.com): the project payload's `owner` object is
// effectively never present for fleet collection (absent even on
// user-namespace projects unless the caller IS the owner), so the
// v0.27.109 owner-only shape was dead code. The working route: for
// namespace.kind == "user" the namespace path IS a username — one
// /users?username= lookup yields the REAL numeric GitLab user ID (the
// userID>0 deterministic path). What stays BANNED is the v0.27.109
// hazard: a LOGIN-ONLY ref minted straight from the namespace path
// (it would cross-match the GLOBAL cntrb_login table); groups stay
// honestly NULL.
func TestFetchGLProjectAsRepoCapturesOwnerRef(t *testing.T) {
	body := srctest.FuncBody(t, srctest.Read(t, "internal/platform/gitlab/client.go"), "func (c *Client) fetchGLProjectAsRepo")
	if !strings.Contains(body, "OwnerRef") {
		t.Error("fetchGLProjectAsRepo must populate OwnerRef")
	}
	if !strings.Contains(body, `Kind == "user"`) {
		t.Error("the namespace route must be gated on namespace.kind == \"user\" — groups stay unresolved")
	}
	if !strings.Contains(body, "lookupGLUserRef(") {
		t.Error("user namespaces must resolve through the /users?username= lookup (real numeric ID), never a login-only ref")
	}
	helper := srctest.FuncBody(t, srctest.Read(t, "internal/platform/gitlab/client.go"), "func (c *Client) lookupGLUserRef")
	if !strings.Contains(helper, "/users?username=") {
		t.Error("lookupGLUserRef must resolve via the users endpoint")
	}
	if !strings.Contains(helper, "model.UserRef{}, false") {
		t.Error("a failed lookup must return ok=false — the ref stays honestly empty (never fabricate identity)")
	}
}

// Behavioral fixture (the round-13 finding asked for a decoded API
// fixture): a user-namespace project WITHOUT an owner object resolves
// through /users?username= to the real numeric ID; a group-namespace
// project stays unresolved; a failed user lookup stays unresolved.
func TestFetchGLProjectAsRepoNamespaceResolution(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/projects/101", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":101,"name":"r","path_with_namespace":"alice/r","visibility":"public",
			"namespace":{"path":"alice","kind":"user"}}`)
	})
	mux.HandleFunc("/projects/102", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":102,"name":"g","path_with_namespace":"acme/g","visibility":"public",
			"namespace":{"path":"acme","kind":"group"}}`)
	})
	var userLookups atomic.Int64
	mux.HandleFunc("/users", func(w http.ResponseWriter, r *http.Request) {
		userLookups.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("username") == "alice" {
			fmt.Fprint(w, `[{"id":4242,"username":"alice"}]`)
			return
		}
		fmt.Fprint(w, `[]`)
	})
	client, _ := newTestClientWithCapture(t, mux)

	user := client.fetchGLProjectAsRepo(context.Background(), 101, "head")
	if user == nil || user.OwnerRef.PlatformID != 4242 || user.OwnerRef.Login != "alice" {
		t.Fatalf("user-namespace project must resolve the owner via /users lookup, got %+v", user)
	}
	group := client.fetchGLProjectAsRepo(context.Background(), 102, "head")
	if group == nil || group.OwnerRef.PlatformID != 0 || group.OwnerRef.Login != "" {
		t.Fatalf("group-namespace project must stay UNRESOLVED, got %+v", group.OwnerRef)
	}

	// v0.27.122 (round 14, suppressed): the username lookup is cached at
	// client scope — FetchPRRepos runs per MR against the same target
	// project, and an uncached lookup issued hundreds of identical
	// /users calls per listing page. Second fetch of the SAME
	// user-namespace project must not hit /users again.
	before := userLookups.Load()
	again := client.fetchGLProjectAsRepo(context.Background(), 101, "base")
	if again == nil || again.OwnerRef.PlatformID != 4242 {
		t.Fatalf("cached lookup must still resolve, got %+v", again)
	}
	if userLookups.Load() != before {
		t.Fatalf("second lookup for the same username re-hit /users (%d -> %d) — the cache is not working", before, userLookups.Load())
	}
}

// B7 + B8 — topics + created_at from the project payload we already fetch.
func TestGitLabRepoInfoMapsKeywordsAndCreatedAt(t *testing.T) {
	body := srctest.FuncBody(t, srctest.Read(t, "internal/platform/gitlab/client.go"), "func (c *Client) FetchRepoInfo")
	if !strings.Contains(body, "Keywords:") {
		t.Error("gitlab FetchRepoInfo must populate RepoInfo.Keywords from project topics")
	}
	if !strings.Contains(body, "CreatedAt:") {
		t.Error("gitlab FetchRepoInfo must populate RepoInfo.CreatedAt")
	}
}
