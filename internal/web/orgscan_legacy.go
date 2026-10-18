// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// scanOrgRepos fetches all repos from a GitHub org or user and adds them to the group + queue.
// Handles both orgs (/orgs/{name}/repos) and users (/users/{name}/repos).
//
// v0.27.20 gate: a 'rejected' group's orgs never scan — RejectGroup is
// the group-level abuse lever and must stop org-driven enqueue too.
// (Registration itself is already approval-gated in AddOrgToGroup;
// this is the belt for the lever. The pre-v0.27.20 claim that org
// scans checked group status was FALSE — audited 2026-07-16.)
func (s *Server) scanOrgRepos(ctx context.Context, groupID int64, orgURL string) {
	orgURL = strings.TrimSuffix(strings.TrimSpace(orgURL), "/")
	host, name, err := platform.ParseOrgURL(orgURL)
	if err != nil {
		return
	}
	// The deployment's GitHub host, not the literal "github.com": on an
	// Enterprise deployment the org lives on that host, and an org on
	// public GitHub is not one this deployment's keys can enumerate
	// (v0.29.57, Copilot review 5260961848 — the routed client below was
	// unreachable for the only orgs it could serve).
	isGitHub := platform.IsGitHubHost(host, s.ghAPIBase)

	if !isGitHub {
		return
	}

	// The rejected-group gate first: it is the abuse lever, and its log line
	// is the record of why an org did not scan, whatever the key pool holds.
	status, err := s.store.GetGroupStatus(ctx, groupID)
	if err != nil {
		// A lookup error is not "not rejected" (SR-5; worklist follow-up 2):
		// nothing scans until the status is known.
		httpserver.LogFailure(ctx, s.logger, slog.LevelError, err, "org scan skipped — group status lookup failed", "group_id", groupID, "org_url", logURL(orgURL), "error", err)
		return
	}
	if status == "rejected" {
		s.logger.Warn("org scan skipped — owning group is rejected", "group_id", groupID, "org_url", logURL(orgURL))
		return
	}
	if !s.ghKeys.HasUsableKey() {
		// web has its own log; the scheduler's startup WARN never reaches it
		// (items 40/21, review round 1).
		s.logger.Warn("org scan skipped — this web process has no usable GitHub API key", "group_id", groupID, "org_url", logURL(orgURL))
		return
	}

	httpClient := platform.NewHTTPClient(s.ghAPIBase, s.ghKeys, s.logger, platform.AuthGitHub)
	s.logger.Info("scanning repos for user group", "name", name, "group_id", groupID)

	added := 0
	newlyQueued := 0
	refusedAtQuota := 0 // links the account's repository quota refused (v0.29.89)
	alreadyExisted := 0

	// Try /orgs/ first. If that 404s, fall back to /users/ (personal accounts).
	basePaths := []string{
		fmt.Sprintf("/orgs/%s/repos", name),
		fmt.Sprintf("/users/%s/repos", name),
	}

	for _, basePath := range basePaths {
		page := 1
		foundRepos := false

		for {
			path := fmt.Sprintf("%s?per_page=100&type=all&page=%d", basePath, page)
			resp, err := httpClient.Get(platform.WithoutETag(ctx), path)
			if err != nil {
				// 404/403 means this isn't an org (or isn't visible) —
				// try the /users/ path. v0.27.36: sentinel-classified via
				// the phase-0 taxonomy instead of error-string matching
				// (summary/18 Phase 0c — the Fix C/Fix J wiring-gap class).
				if platform.ClassifyError(err) == platform.ClassSkip {
					break
				}
				httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "scan API error", "name", name, "error", err)
				break
			}
			var items []struct {
				ID        int64     `json:"id"` // v0.27.102 — rename-proof numeric identity
				CreatedAt time.Time `json:"created_at"`
				HTMLURL   string    `json:"html_url"`
				Name      string    `json:"name"`
				Owner     struct {
					Login string `json:"login"`
				} `json:"owner"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
				resp.Body.Close()
				// A decode failure must not read as "no repos found".
				httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "org scan: decoding repo page failed", "name", name, "page", page, "error", err)
				break
			}
			resp.Body.Close()

			if len(items) == 0 {
				break
			}
			foundRepos = true

			for _, item := range items {
				// Check if repo already exists in our database.
				// v0.27.36: a lookup ERROR is not "doesn't exist" — falling
				// through to the create path on a transient DB error would
				// mint duplicate rows. Skip the item; the periodic org
				// refresh retries it next scan.
				repoID, err := s.store.FindRepoByURL(ctx, item.HTMLURL)
				if err != nil {
					httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "org scan: repo lookup failed", "url", logURL(item.HTMLURL), "error", err)
					continue
				}
				if repoID > 0 {
					// Already exists — just add the user_repos reference.
					linked := true
					if _, err := s.store.AddOrgRepoToGroupByID(ctx, groupID, repoID); err != nil {
						linked = false
						if _, capped := capacity.AsExceeded(err); capped {
							refusedAtQuota++ // the store logs the refusal once a day per account
						} else {
							httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "org scan: linking existing repo failed", "repo_id", repoID, "error", err)
						}
					}
					// v0.27.102: opportunistic forge-ID backfill (fill-
					// empty-only) so the org-tracked cohort gains rename
					// protection on the next scan pass.
					if idErr := s.store.SetPlatformRepoIDIfEmptySeen(ctx, repoID, model.ForgeIDString(item.ID), item.CreatedAt); idErr != nil {
						httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, idErr, "org scan: platform_repo_id backfill failed", "repo_id", repoID, "error", idErr)
					}
					alreadyExisted++
					if linked {
						added++
					}
				} else {
					// New repo — create it and enqueue for collection.
					repoID, err = s.store.UpsertRepo(ctx, &model.Repo{
						Platform:   model.PlatformGitHub,
						GitURL:     item.HTMLURL,
						Name:       item.Name,
						Owner:      item.Owner.Login,
						PlatformID: model.ForgeIDString(item.ID), // v0.27.102 — enables the rename-heal inside UpsertRepo
					})
					if err != nil {
						httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "org scan: upserting new repo failed", "url", logURL(item.HTMLURL), "error", err)
						continue
					}
					// A silently failed enqueue strands a catalog row with no
					// queue row — a suspected origin of the reconciliation
					// gap found in the 2026-07-21 audit (summary/18 Phase 2).
					if err := s.store.EnqueueRepo(ctx, repoID, 100); err != nil {
						httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "org scan: enqueue failed", "repo_id", repoID, "url", logURL(item.HTMLURL), "error", err)
					}
					newlyQueued++
					if _, err := s.store.AddOrgRepoToGroupByID(ctx, groupID, repoID); err != nil {
						if _, capped := capacity.AsExceeded(err); capped {
							refusedAtQuota++ // queued for the approved org; not linked past the account's quota
						} else {
							httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "org scan: linking new repo failed", "repo_id", repoID, "error", err)
						}
					} else {
						added++
					}
				}
			}
			page++
		}

		if foundRepos {
			break // Found repos via this path, don't try the fallback.
		}
	}

	s.logger.Info("scan complete", "name", name,
		"total_added", added, "newly_queued", newlyQueued, "already_existed", alreadyExisted,
		"not_linked_at_quota", refusedAtQuota)
}
