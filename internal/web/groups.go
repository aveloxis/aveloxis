// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/safego"
)

func (s *Server) handleNewGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sess := s.getSession(r)
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		http.Redirect(w, r, "/dashboard", http.StatusFound)
		return
	}

	if _, err := s.store.CreateUserGroup(r.Context(), sess.UserID, name); err != nil {
		httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "failed to create group", "error", err)
	}
	http.Redirect(w, r, "/dashboard", http.StatusFound)
}

func (s *Server) handleGroup(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)
	// Extract group_id from URL: /groups/123 or /groups/123/repos/456/sbom
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/groups/"), "/")
	groupID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	// Handle SBOM download: /groups/{gid}/repos/{rid}/sbom?format=cyclonedx|spdx
	if len(parts) >= 4 && parts[1] == "repos" && parts[3] == "sbom" {
		s.handleSBOMDownload(w, r, sess, groupID, parts[2])
		return
	}

	// Handle repo detail/visualization page: /groups/{gid}/repos/{rid}
	if len(parts) >= 3 && parts[1] == "repos" {
		s.handleRepoDetail(w, r, sess, groupID, parts[2])
		return
	}

	// Pagination and search parameters.
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	perPage := 25
	query := strings.TrimSpace(r.URL.Query().Get("q"))

	group, totalRepos, err := s.store.GetGroupDetail(r.Context(), sess.UserID, groupID, page, perPage, query)
	if errors.Is(err, db.ErrGroupNotOwned) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		s.serverError(w, r, "handleGroup", err)
		return
	}

	// Enrich repos with gathered vs metadata stats.
	if len(group.Repos) > 0 {
		repoIDs := make([]int64, len(group.Repos))
		for i, r := range group.Repos {
			repoIDs[i] = r.RepoID
		}
		if stats, err := s.store.GetRepoStatsBatch(r.Context(), repoIDs); err == nil {
			for i := range group.Repos {
				if st, ok := stats[group.Repos[i].RepoID]; ok {
					group.Repos[i].GatheredIssues = st.GatheredIssues
					group.Repos[i].GatheredPRs = st.GatheredPRs
					group.Repos[i].GatheredCommits = st.GatheredCommits
					group.Repos[i].MetaIssues = st.MetadataIssues
					group.Repos[i].MetaPRs = st.MetadataPRs
					group.Repos[i].MetaCommits = st.MetadataCommits
				}
			}
		}
	}

	totalPages := (totalRepos + perPage - 1) / perPage
	if totalPages < 1 {
		totalPages = 1
	}

	// Build a sliding window of up to 5 page numbers centered on the current page.
	windowSize := 5
	winStart := page - windowSize/2
	if winStart < 1 {
		winStart = 1
	}
	winEnd := winStart + windowSize - 1
	if winEnd > totalPages {
		winEnd = totalPages
		winStart = winEnd - windowSize + 1
		if winStart < 1 {
			winStart = 1
		}
	}
	var pageWindow []int
	for i := winStart; i <= winEnd; i++ {
		pageWindow = append(pageWindow, i)
	}

	s.render(w, "group", map[string]any{
		"Session":    sess,
		"Group":      group,
		"Page":       page,
		"TotalPages": totalPages,
		"TotalRepos": totalRepos,
		"Query":      query,
		"PageWindow": pageWindow,
		"AddError":   r.URL.Query().Get("add_error"),
		"Capacity":   s.capacityNotice(r, sess.UserID),
		"OrgError":   r.URL.Query().Get("org_error"),
		// A number the handler parsed, never the query string itself: the
		// notice is styled as the site's own, so a reflected string would
		// read as the site speaking (review round 1 of the batch). Not a
		// number, zero or negative: no notice.
		"Pending":    pendingCount(r.URL.Query().Get("pending")),
		"OrgPending": r.URL.Query().Get("org_pending"),
		"GitHubHost": platform.GitHubWebHost(s.ghAPIBase),
	})
}

// pendingCount reads the ?pending= count the add redirect carries; anything
// that is not a positive integer is 0 (no notice).
func pendingCount(q string) int {
	n, err := strconv.Atoi(q)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func (s *Server) handleAddRepo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sess := s.getSession(r)
	groupID, _ := strconv.ParseInt(r.FormValue("group_id"), 10, 64)

	// Support both single URL (repo_url) and bulk paste (repo_urls, line-delimited).
	raw := r.FormValue("repo_urls")
	if raw == "" {
		raw = r.FormValue("repo_url") // backward compat with old single-URL form
	}

	var invalid []string
	if raw != "" && groupID > 0 {
		var urls []string
		for _, line := range strings.Split(raw, "\n") {
			repoURL := strings.TrimSpace(line)
			if repoURL == "" {
				continue
			}
			// Validate the URL before adding.
			v := ValidateRepoURL(repoURL)
			if !v.Valid {
				// Logged below; a refused credential must not be written to
				// the log by the line that refuses it (fix-review round 1).
				invalid = append(invalid, fmt.Sprintf("%s: %s", platform.RedactURLUserinfo(repoURL), v.Error))
				continue
			}
			urls = append(urls, v.URL)
		}

		// v0.27.20 per-add approval: the WHOLE paste is one call so any
		// not-yet-tracked URLs from a non-admin become ONE pending
		// add-request (one approval unit), while tracked repos link
		// instantly. Admins keep the direct create+enqueue path.
		if len(urls) > 0 {
			out, err := s.store.AddReposToGroup(r.Context(), sess.UserID, groupID, urls,
				s.cfg.AutoApproveAddLimitValue())
			if err != nil {
				httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "failed to add repos to group", "group_id", groupID, "error", err)
				if errors.Is(err, db.ErrURLTooLong) {
					// Name the line (follow-up 14): the store refuses the
					// whole paste and its error names none of them.
					for _, u := range urls {
						if u = strings.TrimSpace(u); len(u) > db.MaxAddURLBytes {
							s.logger.Warn("repo not added — a URL is over-long", "group_id", groupID, "bytes", len(u), "url_prefix", logURL(u))
							break
						}
					}
				}
				// Tell the user, who otherwise sees the page a success shows
				// (Copilot review of PR #207); a rejected group gets its own
				// notice, since trying again cannot work (round-24 review).
				http.Redirect(w, r, fmt.Sprintf("/groups/%d?add_error=%s", groupID, addErrorFlag(err)), http.StatusFound)
				return
			}
			s.logger.Info("repo add", "group_id", groupID,
				"linked", out.Linked, "enqueued", out.Enqueued, "pending_approval", out.Pending)
			if out.Pending > 0 {
				s.notifyAddRequestSubmitted(out.RequestID)
				http.Redirect(w, r, fmt.Sprintf("/groups/%d?pending=%d%s", groupID, out.Pending, invalidFlag(invalid)), http.StatusFound)
				return
			}
		}
		if len(invalid) > 0 {
			// The page says so too (round 2: the lines the validator refused
			// were only logged, so a refused paste looked like a success).
			s.logger.Warn("some URLs were invalid", "errors", invalid)
		}
	}
	loc := fmt.Sprintf("/groups/%d", groupID)
	if len(invalid) > 0 {
		loc += "?add_error=invalid"
	}
	http.Redirect(w, r, loc, http.StatusFound)
}

// invalidFlag is the add_error query fragment for a paste with refused
// lines, empty when every line was accepted.
func invalidFlag(invalid []string) string {
	if len(invalid) == 0 {
		return ""
	}
	return "&add_error=invalid"
}

func (s *Server) handleAddOrg(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sess := s.getSession(r)
	groupID, _ := strconv.ParseInt(r.FormValue("group_id"), 10, 64)
	orgURL := strings.TrimSpace(r.FormValue("org_url"))

	if orgURL != "" && groupID > 0 {
		// The host travels with the registration: the store refuses a
		// GitHub org that is not on this deployment's GitHub host
		// (db.ErrOrgOffGitHubHost) in every writer, and the page says so.
		out, err := s.store.AddOrgToGroup(r.Context(), sess.UserID, groupID, orgURL, s.ghAPIBase)
		switch {
		case errors.Is(err, db.ErrOrgOffGitHubHost):
			s.logger.Warn("org not added — its host is not this deployment's GitHub host",
				"group_id", groupID, "org_url", logURL(orgURL), "github_host", platform.GitHubWebHost(s.ghAPIBase))
			http.Redirect(w, r, fmt.Sprintf("/groups/%d?org_error=host", groupID), http.StatusFound)
			return
		case errors.Is(err, platform.ErrURLUserinfo), errors.Is(err, db.ErrURLTooLong), db.IsRejectedValue(err):
			// A value the database refused (SQLSTATE class 22, e.g. a NUL
			// byte) is the user's input too, as addErrorFlag reads it on the
			// repo path (PR #218 review C4).
			// The user's input, fixable by the user: say so on the page, as
			// the repo path (add_error=invalid) and the portal (400) do
			// (fix-review round 1: the store's refusal fell through to a plain
			// redirect, so a credentialed org URL was silently not added). The
			// URL is logged redacted and truncated, as the host arm logs it
			// (follow-up 14: the over-long WARN named nothing).
			s.logger.Warn("org not added — invalid URL", "group_id", groupID, "error", err,
				"org_url", logURL(orgURL), "bytes", len(orgURL))
			http.Redirect(w, r, fmt.Sprintf("/groups/%d?org_error=invalid", groupID), http.StatusFound)
			return
		case errors.Is(err, db.ErrGroupRejected):
			// Trying again cannot work: say why (the repo paste and the
			// portal already do; review round 1 of the batch).
			s.logger.Warn("org not added — the group is rejected", "group_id", groupID)
			http.Redirect(w, r, fmt.Sprintf("/groups/%d?org_error=rejected", groupID), http.StatusFound)
			return
		case err != nil:
			// Say so (worklist follow-up 11): this redirected as a success.
			httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "failed to add org to group", "group_id", groupID, "error", err)
			http.Redirect(w, r, fmt.Sprintf("/groups/%d?org_error=1", groupID), http.StatusFound)
			return
		case out.Registered:
			// Registered (an admin's add, or a non-admin's auto-approved add of
			// an org already registered in a group that is not rejected —
			// including this group, when the add inserted nothing): scan
			// immediately.
			// Use a detached context — the HTTP request context gets canceled on redirect.
			safego.Go(s.logger, "org-repo-scan", func() { s.scanOrgRepos(context.Background(), groupID, orgURL) })
		default:
			// v0.27.20: a non-admin's add of a NEW org pends on an
			// add-request — nothing scans until an admin approves.
			s.notifyAddRequestSubmitted(out.RequestID)
			http.Redirect(w, r, fmt.Sprintf("/groups/%d?org_pending=1", groupID), http.StatusFound)
			return
		}
	}
	http.Redirect(w, r, fmt.Sprintf("/groups/%d", groupID), http.StatusFound)
}

func (s *Server) handleRemoveRepo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sess := s.getSession(r)
	groupID, _ := strconv.ParseInt(r.FormValue("group_id"), 10, 64)
	repoID, _ := strconv.ParseInt(r.FormValue("repo_id"), 10, 64)

	if groupID > 0 && repoID > 0 {
		// The error was discarded and the page redirected as if the
		// repository had been removed (old problem O2).
		// A repository already gone from the group (another tab) redirects
		// to the group page like a removal: the page shows the outcome the
		// user wanted (v0.29.91 reviewed the count; nothing to say here).
		_, err := s.store.RemoveRepoFromGroup(r.Context(), sess.UserID, groupID, repoID)
		if errors.Is(err, db.ErrGroupNotOwned) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			s.serverError(w, r, "handleRemoveRepo", err)
			return
		}
	}
	http.Redirect(w, r, fmt.Sprintf("/groups/%d", groupID), http.StatusFound)
}
