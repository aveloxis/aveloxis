// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/model"
)

// scopeHelpStore is what the refusal of an API token's out-of-scope request
// reads to say how to add the repository (the production store; a fake in
// tests).
type scopeHelpStore interface {
	GetReposBatch(ctx context.Context, repoIDs []int64) (map[int64]*model.Repo, error)
	GetUserGroups(ctx context.Context, userID int) ([]db.UserGroup, error)
}

// tokenOutOfScopeHint is what an API token's out-of-scope refusal tells its
// holder (operator decision 2026-10-09: refuse, never auto-add, and make
// the way to add it clear).
const tokenOutOfScopeHint = "This API token reads only the repositories in its owner's groups. " +
	"Add this repository to one of your groups (your_groups, with add_to_existing_group), " +
	"or create a group first (create_group, then add_to_existing_group with its group_id), then retry."

// tokenScopeInstructions is the "add it to a group" part of the refusal:
// the owner's groups that accept additions (a rejected group refuses them,
// L10 round 2 on 0.29.86) and the two calls, ready to send. addBody is the
// add's body: {"url": …, "kind": "repo"} for one repository, {"urls": […],
// "kind": "repo"} for an organization's collected repositories.
func tokenScopeInstructions(groups []db.UserGroup, addBody map[string]any) map[string]any {
	list := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		if g.Status == "rejected" {
			continue
		}
		list = append(list, map[string]any{"group_id": g.GroupID, "name": g.Name})
	}
	return map[string]any{
		"your_groups": list,
		"add_to_existing_group": map[string]any{
			"method": http.MethodPost,
			"path":   "/api/v1/groups/{group_id}/repos",
			"body":   addBody,
		},
		"create_group": map[string]any{
			"method": http.MethodPost,
			"path":   "/api/v1/groups",
			"body":   map[string]any{"name": "<a name for the new group>"},
		},
	}
}

// refuseTokenOutOfScope answers an API token's request for a repository
// outside its owner's groups: 403 repo_out_of_scope, never stored, with
// the repository's URL and the instructions when it exists. A lookup that
// fails still refuses (logged), with the plain hint.
func (s *Server) refuseTokenOutOfScope(w http.ResponseWriter, r *http.Request, info authInfo, repoID int64) {
	body := map[string]any{
		"error":   "repo_out_of_scope",
		"repo_id": repoID,
		"hint":    "add this repository to one of your groups to request access",
	}
	if s.scopeHelp != nil {
		repos, err := s.scopeHelp.GetReposBatch(r.Context(), []int64{repoID})
		var groups []db.UserGroup
		if err == nil && repos[repoID] != nil {
			groups, err = s.scopeHelp.GetUserGroups(r.Context(), info.UserID)
		}
		switch {
		case err != nil:
			httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "could not read the instructions for an API token's out-of-scope refusal — refused with the plain hint",
				"user_id", info.UserID, "repo_id", repoID, "error", err)
		case repos[repoID] == nil:
			body["hint"] = "no collected repository has this id"
		default:
			body["repo_url"] = repos[repoID].GitURL
			body["hint"] = tokenOutOfScopeHint
			for k, v := range tokenScopeInstructions(groups, map[string]any{"url": repos[repoID].GitURL, "kind": "repo"}) {
				body[k] = v
			}
		}
	}
	setNoStoreHeaders(w.Header())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	// Deliberate manual encode: non-200 status already written.
	_ = json.NewEncoder(w).Encode(body)
}

// addEntityInstructions adds the "how to add it" part to an API token's
// compare refusal. A repository is added by its stored URL. An
// organization is added as its COLLECTED repositories, one bulk repo add
// that links the queued ones at once — what a session's auto-add links (a
// repository no longer queued follows the add's approval rule). Registering
// the organization itself (kind "org") would link nothing now: it queues
// org tracking, for approval when no one tracks it yet, and a GitLab group
// is never enumerated (L10 round 2 on 0.29.86); the compare never
// registers org tracking (TestCompareAutoAddNeverTouchesOrgTracking).
// Nothing is added when the entity resolved to no collected repository or
// a lookup failed (logged).
func (s *Server) addEntityInstructions(r *http.Request, info authInfo, e entity, collected []int64, body map[string]any) {
	if s.scopeHelp == nil || len(collected) == 0 || (e.Kind != "repo" && e.Kind != "org") {
		return
	}
	logFail := func(err error) {
		httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "could not read the instructions for an API token's out-of-scope compare refusal",
			"user_id", info.UserID, "entity", e.Label, "error", err)
	}
	ids := collected
	if e.Kind == "repo" {
		ids = collected[:1]
	}
	repos, err := s.scopeHelp.GetReposBatch(r.Context(), ids)
	if err != nil {
		logFail(err)
		return
	}
	urls := make([]string, 0, len(ids))
	for _, id := range ids {
		if repo := repos[id]; repo != nil {
			urls = append(urls, repo.GitURL)
		}
	}
	if len(urls) == 0 {
		return
	}
	groups, err := s.scopeHelp.GetUserGroups(r.Context(), info.UserID)
	if err != nil {
		logFail(err)
		return
	}
	add := map[string]any{"url": urls[0], "kind": "repo"}
	body["entity_url"] = urls[0]
	body["hint"] = tokenOutOfScopeHint
	if e.Kind == "org" {
		add = map[string]any{"urls": urls, "kind": "repo"}
		delete(body, "entity_url")
		body["entity_repo_urls"] = urls
		body["hint"] = tokenOutOfScopeHint + " For an organization, add_to_existing_group adds its collected repositories (entity_repo_urls) in one call."
	}
	for k, v := range tokenScopeInstructions(groups, add) {
		body[k] = v
	}
}
