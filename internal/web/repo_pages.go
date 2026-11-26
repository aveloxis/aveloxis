// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/mailer"
)

func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	// Pattern "GET /{$}" ensures this only matches exactly "/". The one
	// public question (hasSession): a lookup failure is logged there and
	// sends the visitor to the login page.
	if s.hasSession(r) {
		http.Redirect(w, r, "/dashboard", http.StatusFound)
		return
	}
	http.Redirect(w, r, "/login", http.StatusFound)
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request, sess *Session) {

	// v0.19.10 email gate, v0.20.4 pending-aware, v0.29.29
	// confirmable-only, v0.29.30 lookup errors logged: see
	// admin.Service.EmailGate.
	needsEmailForm, pendingEmail := s.admin.EmailGate(r.Context(), sessionActor(sess), linkOrigin(r))
	if needsEmailForm {
		http.Redirect(w, r, "/account/email", http.StatusFound)
		return
	}

	groups, err := s.store.GetUserGroups(r.Context(), sess.UserID)
	if err != nil {
		// A failed lookup is not "no groups" (SR-5; batch 5b review round
		// 1): it read as an empty dashboard, indistinguishable from a fresh
		// login, and the pending-approval banner keyed off it.
		s.serverError(w, r, "handleDashboard", err)
		return
	}

	// v0.19.10 pending-approval banner: non-admin users whose groups
	// are all status='pending' get a clear signal their account is
	// awaiting administrator approval. Without this they see an empty
	// dashboard indistinguishable from a fresh admin login.
	pendingOnly := !sess.IsAdmin && len(groups) > 0
	if pendingOnly {
		for _, g := range groups {
			if g.Status != "pending" {
				pendingOnly = false
				break
			}
		}
	}

	s.render(w, r, "dashboard", map[string]any{
		"Session":      sess,
		"Groups":       groups,
		"PendingOnly":  pendingOnly,
		"PendingEmail": pendingEmail,
		// How long links are valid. The banner shows on every visit while
		// the address is pending, so it states the validity window, not
		// time left on this link.
		"ConfirmationLifetime": mailer.DurationPhrase(db.EmailConfirmationLifetime),
	})
}

// handleCompare renders the comparison page for up to 5 repos.
// Repos are specified by ID in the query string: /compare?repos=1,2,3,4,5
func (s *Server) handleCompare(w http.ResponseWriter, r *http.Request, sess *Session) {

	// Get the user's groups and their repos for the search dropdown.
	groups, err := s.store.GetUserGroups(r.Context(), sess.UserID)
	if err != nil {
		s.serverError(w, r, "handleCompare", err)
		return
	}

	s.render(w, r, "compare", map[string]any{
		"Session": sess,
		"Groups":  groups,
		"RepoIDs": compareRepoIDs(r.URL.Query().Get("repos")),
	})
}

// compareMaxRepos is the compare page's own limit (addRepo refuses a sixth;
// CHART_COLORS has five).
const compareMaxRepos = 5

// compareRepoIDs keeps the positive integer ids of a /compare?repos= list,
// in order, without repeats and at most compareMaxRepos, so the page script
// only ever sees ids it can chart (review of 0.29.92, W3 and W2-5).
func compareRepoIDs(raw string) string {
	var out []string
	seen := map[int64]bool{}
	for _, part := range strings.Split(raw, ",") {
		if len(out) == compareMaxRepos {
			break
		}
		id, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil || id <= 0 || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, strconv.FormatInt(id, 10))
	}
	return strings.Join(out, ",")
}

// handleSBOMDownload generates and returns an SBOM for a repo in a group
// the session may read, through the service's one access rule (stage 1 of
// summary/59; the repository must be in the group — PR #218 review C16's
// "not checked" decision is reversed, pinned by
// TestRepositoryPageAndSBOMRequireMembership).
func (s *Server) handleSBOMDownload(w http.ResponseWriter, r *http.Request, sess *Session) {
	groupID, okG := pathID(r, "gid")
	repoID, okR := pathID(r, "rid")
	if !okG || !okR {
		http.NotFound(w, r)
		return
	}

	// The service's one access rule: the owner or an administrator's
	// session may read the group, and the repository must be in it (N11:
	// "not yours", "no such group" and "not in that group" all answer 404,
	// SR-5: a store failure is the 500 it is).
	if _, err := s.admin.AuthorizeGroupRepo(r.Context(), sessionActor(sess), groupID, repoID); err != nil {
		if !s.refusePage(w, r, err) {
			s.serverError(w, r, "handleSBOMDownload", err)
		}
		return
	}

	format := r.URL.Query().Get("format")
	if format == "" {
		format = "cyclonedx"
	}

	var sbomFormat collector.SBOMFormat
	var filename string
	switch format {
	case "cyclonedx":
		sbomFormat = collector.FormatCycloneDX
		filename = fmt.Sprintf("sbom-repo-%d-cyclonedx.json", repoID)
	case "spdx":
		sbomFormat = collector.FormatSPDX
		filename = fmt.Sprintf("sbom-repo-%d-spdx.json", repoID)
	default:
		http.Error(w, "format must be 'cyclonedx' or 'spdx'", http.StatusBadRequest)
		return
	}

	data, err := collector.GenerateSBOM(r.Context(), s.store, repoID, sbomFormat)
	if errors.Is(err, db.ErrRepoNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		// The cause goes to the log, not the body (it carried the store's
		// text through v0.29.67; follow-up 12).
		s.serverError(w, r, "handleSBOMDownload", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	w.Write(data)
}

// handleRepoDetail shows the repo visualization/detail page.
// This is the landing page when a user clicks a repo name in the group list.
func (s *Server) handleRepoDetail(w http.ResponseWriter, r *http.Request, sess *Session) {
	groupID, okG := pathID(r, "gid")
	repoID, okR := pathID(r, "rid")
	if !okG || !okR {
		http.NotFound(w, r)
		return
	}

	// The service's one access rule (as handleSBOMDownload).
	group, err := s.admin.AuthorizeGroupRepo(r.Context(), sessionActor(sess), groupID, repoID)
	if err != nil {
		if !s.refusePage(w, r, err) {
			s.serverError(w, r, "handleRepoDetail", err)
		}
		return
	}

	// Get repo details: only "no such repository" is a 404.
	repo, err := s.store.GetRepoByID(r.Context(), repoID)
	if errors.Is(err, db.ErrRepoNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		s.serverError(w, r, "handleRepoDetail", err)
		return
	}

	// Get stats: an enrichment the page renders without, so a failure is
	// logged, not a 500 (the template handles a nil Stats).
	stats, err := s.store.GetRepoStats(r.Context(), repoID)
	if err != nil && !httpserver.RequestEnded(r.Context(), err) {
		s.logger.Warn("repo page: stats unavailable", "repo_id", repoID, "error", err)
	}

	s.render(w, r, "repo_detail", map[string]any{
		"Session": sess,
		"Group":   group,
		"Repo":    repo,
		"Stats":   stats,
		// The four tiles, decided here: a number, "unavailable" when the
		// statistics could not be read, and for vulnerabilities the count
		// or a dash with the coverage state (0.29.200).
		"Issues":  statOrUnavailable(stats, func(s *db.RepoStats) int { return s.GatheredIssues }),
		"PRs":     statOrUnavailable(stats, func(s *db.RepoStats) int { return s.GatheredPRs }),
		"Commits": statOrUnavailable(stats, func(s *db.RepoStats) int { return s.GatheredCommits }),
		"Vuln":    vulnTileOf(stats),
		"RepoID":  repoID,
		"GroupID": groupID,
	})
}
