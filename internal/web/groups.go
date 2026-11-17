// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/aveloxis/aveloxis/internal/admin"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/queueview"
)

func (s *Server) handleNewGroup(w http.ResponseWriter, r *http.Request, sess *Session) {
	if _, err := s.admin.CreateGroup(r.Context(), sessionActor(sess), r.FormValue("name")); err != nil {
		// An empty or over-long name is the form's input: the dashboard is
		// shown again (the page's own field bounds it); a failure is logged
		// and the dashboard shown, as before (the page cannot say more).
		if _, refused := admin.Classify(err); !refused {
			httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "failed to create group", "error", err)
		}
	}
	http.Redirect(w, r, "/dashboard", http.StatusFound)
}

// groupPageSize is the group page's page size and its maximum.
const groupPageSize = 25

// handleGroup is the group page, GET /groups/{gid} (the repository page
// and the SBOM download are their own routes since 0.29.102; an unknown
// sub-path is the mux's 404, not this page).
func (s *Server) handleGroup(w http.ResponseWriter, r *http.Request, sess *Session) {
	groupID, ok := pathID(r, "gid")
	if !ok {
		http.NotFound(w, r)
		return
	}

	// Pagination and search through the one parser (queueview, stage 3d;
	// whole-branch review of 0.29.109: a bare Atoi here let a huge page
	// overflow the store's offset into a 500, the class the parser clamps).
	p := queueview.ParseParams(r.URL.Query(), groupPageSize, groupPageSize)
	page, perPage, query := p.Page, p.PageSize, p.Search

	// The one access rule (E4, stage 3e): the owner, or an administrator
	// reading another account's group — logged by the service, and the
	// page then offers no write. A stranger gets 404 (N11).
	ref, err := s.admin.GroupAccess(r.Context(), sessionActor(sess), groupID, admin.Read)
	if err != nil {
		if !s.refusePage(w, r, err) {
			s.serverError(w, r, "handleGroup", err)
		}
		return
	}
	readOnly := ref.OwnerID != sess.UserID
	group, totalRepos, err := s.store.GetGroupDetail(r.Context(), ref.OwnerID, groupID, page, perPage, query)
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
		stats, err := s.store.GetRepoStatsBatch(r.Context(), repoIDs)
		if err != nil {
			// The page renders without the counts; said so (D-17: the error
			// was dropped without a line).
			httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "group page: repository stats unreadable — counts left empty", "group_id", groupID, "error", err)
		} else {
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

	// The pager is the shared one (queueview, stage 3d): the monitor page
	// had its own copy of this window arithmetic.
	totalPages := queueview.TotalPages(totalRepos, perPage)
	pageWindow := queueview.Window(page, totalPages)

	s.render(w, r, "group", map[string]any{
		"Session":    sess,
		"ReadOnly":   readOnly,
		"Group":      group,
		"Page":       page,
		"TotalPages": totalPages,
		"TotalRepos": totalRepos,
		"Query":      query,
		"PageSize":   pageSizeParam(p),
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
		// The configured GitLab instance, for the off-instance notice
		// (round-4 review of 0.29.100).
		"GitLabInstance": db.GitLabInstanceLabel(s.gitlabWebBase()),
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

func (s *Server) handleAddRepo(w http.ResponseWriter, r *http.Request, sess *Session) {
	groupID, _ := strconv.ParseInt(r.FormValue("group_id"), 10, 64)

	// Support both single URL (repo_url) and bulk paste (repo_urls, line-delimited).
	raw := r.FormValue("repo_urls")
	if raw == "" {
		raw = r.FormValue("repo_url") // backward compat with old single-URL form
	}
	if raw == "" || groupID <= 0 {
		http.Redirect(w, r, fmt.Sprintf("/groups/%d", groupID), http.StatusFound)
		return
	}
	// v0.27.20 per-add approval: the WHOLE paste is one call so any
	// not-yet-tracked URLs from a non-admin become ONE pending add-request
	// (one approval unit), while tracked repos link instantly. The service
	// parses every line, refuses the paste the store refuses, and notifies
	// the operator of a pending request.
	out, err := s.admin.AddToGroup(r.Context(), sessionActor(sess), groupID, strings.Split(raw, "\n"))
	if err != nil {
		if ref, ok := admin.Classify(err); ok && (ref.Kind == admin.NotOwned || ref.Kind == admin.NotFound) {
			http.NotFound(w, r)
			return
		}
		if _, refused := admin.Classify(err); !refused {
			httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "failed to add repos to group", "group_id", groupID, "error", err)
		}
		// Tell the user, who otherwise sees the page a success shows
		// (Copilot review of PR #207); a rejected group gets its own
		// notice, since trying again cannot work (round-24 review).
		flag := addErrorFlag(err)
		// Only for the service's own "no usable line" refusal: when the
		// store refused the whole paste, the general notice is the true
		// one (it says nothing from the paste was added).
		if flag == addErrorInvalid && out.AllRefused {
			flag = invalidAddFlag(out)
		}
		http.Redirect(w, r, fmt.Sprintf("/groups/%d?add_error=%s", groupID, flag), http.StatusFound)
		return
	}
	if len(out.Invalid) > 0 {
		// The page says so too (round 2: the lines the validator refused
		// were only logged, so a refused paste looked like a success).
		s.logger.Warn("some URLs were invalid", "group_id", groupID, "errors", out.Invalid)
	}
	if out.Pending > 0 {
		http.Redirect(w, r, fmt.Sprintf("/groups/%d?pending=%d%s", groupID, out.Pending, invalidFlag(out)), http.StatusFound)
		return
	}
	loc := fmt.Sprintf("/groups/%d", groupID)
	if len(out.Invalid) > 0 {
		loc += "?add_error=" + invalidAddFlag(out)
	}
	http.Redirect(w, r, loc, http.StatusFound)
}

// The group page's notices for refused paste lines: the general one, and
// the one for an owner's page (Copilot's review of PR #261: the general
// notice speaks of credentials and length, which an organization's page
// has nothing to do with).
const (
	addErrorInvalid   = "invalid"
	addErrorOwnerPage = "owner_page"
)

// invalidAddFlag names the notice for an outcome with refused lines: the
// owner-page one when any of them was an owner's page (it also says the
// other refused lines were skipped).
func invalidAddFlag(out admin.AddOutcome) string {
	if out.OwnerPages > 0 {
		return addErrorOwnerPage
	}
	return addErrorInvalid
}

// invalidFlag is the add_error query fragment for a paste with refused
// lines, empty when every line was accepted.
func invalidFlag(out admin.AddOutcome) string {
	if len(out.Invalid) == 0 {
		return ""
	}
	return "&add_error=" + invalidAddFlag(out)
}

func (s *Server) handleAddOrg(w http.ResponseWriter, r *http.Request, sess *Session) {
	groupID, _ := strconv.ParseInt(r.FormValue("group_id"), 10, 64)
	orgURL := strings.TrimSpace(r.FormValue("org_url"))
	if orgURL == "" || groupID <= 0 {
		http.Redirect(w, r, fmt.Sprintf("/groups/%d", groupID), http.StatusFound)
		return
	}
	// The hosts travel with the registration: the service refuses an
	// organization that is not on this deployment's GitHub host or GitLab
	// instance, a URL that is invalid, and a rejected group — each its own
	// notice on the page.
	out, err := s.admin.AddOrgToGroup(r.Context(), sessionActor(sess), groupID, orgURL)
	if err != nil {
		if ref, ok := admin.Classify(err); ok && (ref.Kind == admin.NotOwned || ref.Kind == admin.NotFound) {
			http.NotFound(w, r)
			return
		}
		if _, refused := admin.Classify(err); !refused {
			// Say so (worklist follow-up 11): this redirected as a success.
			httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "failed to add org to group", "group_id", groupID, "error", err)
		}
		http.Redirect(w, r, fmt.Sprintf("/groups/%d?org_error=%s", groupID, orgErrorFlag(err)), http.StatusFound)
		return
	}
	if !out.Registered {
		// v0.27.20: a non-admin's add of a NEW org pends on an add-request —
		// nothing scans until an admin approves (the service notified the
		// operator).
		http.Redirect(w, r, fmt.Sprintf("/groups/%d?org_pending=1", groupID), http.StatusFound)
		return
	}
	// Registered (an admin's add, or a non-admin's auto-approved add of an
	// org already registered in a group that is not rejected). Nothing
	// scans here (stage 1 of summary/59, C2: the web process holds no forge
	// keys): serve's demand probe finds the never-scanned registration on
	// its next poll tick.
	http.Redirect(w, r, fmt.Sprintf("/groups/%d", groupID), http.StatusFound)
}

// handleRemoveRepoConfirm shows the "Remove X from this group?" page (GET
// /groups/{gid}/repos/{rid}/remove; 0.29.102, D-19: the group page's
// Remove button used a confirm() dialog). The page's form posts the
// removal; the service's one access rule decides whether the pair exists
// for this session (404 otherwise, as the repository page answers).
func (s *Server) handleRemoveRepoConfirm(w http.ResponseWriter, r *http.Request, sess *Session) {
	groupID, okG := pathID(r, "gid")
	repoID, okR := pathID(r, "rid")
	if !okG || !okR {
		http.NotFound(w, r)
		return
	}
	group, err := s.admin.AuthorizeGroupRepo(r.Context(), sessionActor(sess), groupID, repoID)
	if err != nil {
		if !s.refusePage(w, r, err) {
			s.serverError(w, r, "handleRemoveRepoConfirm", err)
		}
		return
	}
	// A reader who is not the owner (an administrator's read-only view,
	// E4) has no Remove: the confirm page answers as a stranger's would.
	if group.OwnerID != sess.UserID {
		http.NotFound(w, r)
		return
	}
	repo, err := s.store.GetRepoByID(r.Context(), repoID)
	if errors.Is(err, db.ErrRepoNotFound) {
		http.NotFound(w, r) // the row went between the authorization and the read: the caller's 404, as on the repository page
		return
	}
	if err != nil {
		s.serverError(w, r, "handleRemoveRepoConfirm", fmt.Errorf("repository %d: %w", repoID, err))
		return
	}
	s.render(w, r, "remove_confirm", map[string]any{
		"Session":   sess,
		"GroupID":   groupID,
		"GroupName": group.Name,
		"RepoID":    repoID,
		"RepoOwner": repo.Owner,
		"RepoName":  repo.Name,
	})
}

func (s *Server) handleRemoveRepo(w http.ResponseWriter, r *http.Request, sess *Session) {
	groupID, _ := strconv.ParseInt(r.FormValue("group_id"), 10, 64)
	repoID, _ := strconv.ParseInt(r.FormValue("repo_id"), 10, 64)

	if groupID > 0 && repoID > 0 {
		// The error was discarded and the page redirected as if the
		// repository had been removed (old problem O2).
		// A repository already gone from the group (another tab) redirects
		// to the group page like a removal: the page shows the outcome the
		// user wanted (v0.29.91 reviewed the count; nothing to say here).
		if _, err := s.admin.RemoveFromGroup(r.Context(), sessionActor(sess), groupID, repoID); err != nil {
			if !s.refusePage(w, r, err) {
				s.serverError(w, r, "handleRemoveRepo", err)
			}
			return
		}
	}
	http.Redirect(w, r, fmt.Sprintf("/groups/%d", groupID), http.StatusFound)
}
