// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// 2026-07-21 — the group page's repo-add went bulk: POST
// /groups/{id}/repos accepts {"urls": [...]} so a newline-separated
// paste lands in ONE AddReposToGroup call (one approval unit per
// v0.27.20) instead of N sequential requests. These tests pin the
// body contract at the parse layer (no store needed — validation
// failures return before any store call) plus source-contract pins on
// the wiring. The array path executes against the real schema via the
// endpoint smoke recipe for the POST route.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// bulkAddRequest builds a request that already carries a resolved
// identity (the TestAdminSelfDemotionRefused pattern) so the handler
// reaches its body parsing without a real session store.
func bulkAddRequest(body string) *http.Request {
	req := httptest.NewRequest("POST", "/api/v1/groups/5/repos", strings.NewReader(body))
	req.SetPathValue("groupID", "5")
	ctx := context.WithValue(req.Context(), authCtxKey{}, authInfo{UserID: 9})
	return req.WithContext(ctx)
}

// TestGroupAddRepoRejectsEmptyBodies — neither url nor urls (or only
// blank entries) must 400 before any store call.
func TestGroupAddRepoRejectsEmptyBodies(t *testing.T) {
	s := &Server{}
	for _, body := range []string{
		`{}`,
		`{"kind":"repo"}`,
		`{"url":"  ","kind":"repo"}`,
		`{"urls":[],"kind":"repo"}`,
		`{"urls":["","  "],"kind":"repo"}`,
		`not json`,
	} {
		rec := httptest.NewRecorder()
		s.handleGroupAddRepo(rec, bulkAddRequest(body))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %q: want 400, got %d", body, rec.Code)
		}
	}
}

// TestGroupAddRepoRejectsBulkOrgs — an org is an unbounded add
// already; bulk paste is for repositories only.
func TestGroupAddRepoRejectsBulkOrgs(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	s.handleGroupAddRepo(rec, bulkAddRequest(
		`{"urls":["https://github.com/a","https://github.com/b"],"kind":"org"}`))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("multi-URL org add must 400, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "one at a time") {
		t.Errorf("multi-URL org 400 should explain orgs are one at a time, got: %s", rec.Body.String())
	}
}

// TestGroupAddRepoBulkWiring — source-contract: the handler declares
// the urls array, keeps the legacy single-url fallback (older GUIs
// send only `url`), and hands the WHOLE slice to AddReposToGroup in
// one call — never a per-URL loop of single adds, which would split
// one paste into N approval units.
func TestGroupAddRepoBulkWiring(t *testing.T) {
	src := mustReadFile(t, "portal.go")
	body := extractFuncBody(t, src, "handleGroupAddRepo")
	for _, needle := range []string{
		"URLs []string", `json:"urls"`, // the array field
		"req.URL",                     // legacy single-url fallback retained
		"AddReposToGroup(",            // one batched store call
		"urls, s.autoApproveAddLimit", // ... receiving the full slice
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("handleGroupAddRepo must contain %q — bulk body with backward-compatible single-url fallback, one AddReposToGroup call", needle)
		}
	}
}

// TestGroupOrgsHandlerWiring — source-contract: the tracked-orgs
// listing goes through the ownership-checked store method and returns
// the {"orgs": [...]} envelope the GUI renders.
func TestGroupOrgsHandlerWiring(t *testing.T) {
	src := mustReadFile(t, "portal.go")
	body := extractFuncBody(t, src, "handleGroupOrgs")
	for _, needle := range []string{
		"requireUser(", "GetPortalGroupOrgsForUser(", `"orgs"`,
		`json:"last_scanned,omitempty"`,
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("handleGroupOrgs must contain %q — Bearer-gated, ownership-checked orgs listing", needle)
		}
	}
}
