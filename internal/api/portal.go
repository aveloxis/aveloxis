// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.3 — portal + admin JSON endpoints backing the aveloxis-gui
// pages (group.html, monitor.html, pending-groups.html, users.html).
//
// Unlike the read-analytics endpoints (gated by api.require_auth
// during rollout), EVERY endpoint here demands a valid Bearer token
// unconditionally — they carry user context (whose groups? who
// approves?) that cannot exist without identity, and admin routes
// additionally require the admin flag. Operator, 2026-07-10:
// "Obviously only admin users will see the /users urls etc."
// Reuses the v0.19.0 group-approval machinery verbatim — no new
// approval system.

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/safego"
)

// requireUser demands a validated Bearer identity regardless of the
// api.require_auth rollout flag (user-context endpoints are
// meaningless without one). Writes the 401 itself on failure.
func (s *Server) requireUser(w http.ResponseWriter, r *http.Request) (authInfo, bool) {
	if info, ok := r.Context().Value(authCtxKey{}).(authInfo); ok {
		return info, true
	}
	// The global middleware may not have resolved a token (auth off /
	// exempt LAN without a header). Resolve here so LAN callers with a
	// token still work, and everyone else gets a clean 401.
	if tok := bearerToken(r); tok != "" {
		if info, ok := s.auth.resolveToken(r.Context(), tok); ok {
			return info, true
		}
	}
	writeAuthError(w, http.StatusUnauthorized, "this endpoint requires a signed-in session (Bearer token)")
	return authInfo{}, false
}

// requireAdmin layers the admin check on requireUser.
func (s *Server) requireAdmin(w http.ResponseWriter, r *http.Request) (authInfo, bool) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return authInfo{}, false
	}
	if !info.IsAdmin {
		writeAuthError(w, http.StatusForbidden, "administrator access required")
		return authInfo{}, false
	}
	return info, true
}

// --- /api/v1/me ---

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	jsonResponse(w, map[string]any{
		"user_id":  info.UserID,
		"is_admin": info.IsAdmin,
		"scope_repo_count": func() int {
			if info.IsAdmin {
				return -1 // unscoped
			}
			return len(info.Scope)
		}(),
	})
}

// --- groups (per-user) ---

type groupJSON struct {
	GroupID   int64  `json:"group_id"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	RepoCount int    `json:"repo_count"`
	Favorited bool   `json:"favorited"`
}

func (s *Server) handleGroupsList(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	groups, err := s.store.GetUserGroups(r.Context(), info.UserID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]groupJSON, 0, len(groups))
	for _, g := range groups {
		status := g.Status
		if status == "" {
			status = "approved"
		}
		out = append(out, groupJSON{g.GroupID, g.Name, status, g.RepoCount, g.Favorited})
	}
	jsonResponse(w, map[string]any{"groups": out})
}

func (s *Server) handleGroupCreate(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Name) == "" {
		http.Error(w, "body must be {\"name\": \"...\"}", http.StatusBadRequest)
		return
	}
	id, err := s.store.CreateUserGroup(r.Context(), info.UserID, strings.TrimSpace(req.Name))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]any{"group_id": id})
}

// parsePortalPage reads the v0.27.14 pagination params: page
// (default 1) and page_size (default 50, capped at 100). Garbage,
// zero, and negative inputs collapse to the defaults.
func parsePortalPage(r *http.Request) (page, pageSize int) {
	page, _ = strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	pageSize, _ = strconv.Atoi(r.URL.Query().Get("page_size"))
	if pageSize < 1 {
		pageSize = 50
	}
	if pageSize > 100 {
		pageSize = 100
	}
	return page, pageSize
}

func (s *Server) handleGroupRepos(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	groupID, err := strconv.ParseInt(r.PathValue("groupID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid group id", http.StatusBadRequest)
		return
	}
	page, pageSize := parsePortalPage(r)
	repos, total, err := s.store.GetPortalGroupReposForUser(r.Context(), info.UserID, groupID, info.IsAdmin, pageSize, (page-1)*pageSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	// v0.27.14: annotate the PAGE (never the whole group) with ALL-TIME
	// counts from the latest repo_info snapshot via the batched-stats
	// cache, plus the caller's star state. OPERATOR DECISION: all-time
	// totals, NOT the 90-day activity metric (the v0.27.4 nginx-timeout
	// class at fleet scale).
	ids := make([]int64, 0, len(repos))
	for _, g := range repos {
		ids = append(ids, g.RepoID)
	}
	stats, _ := s.store.GetRepoStatsBatch(r.Context(), ids)
	starred, _ := s.store.GetUserStarredRepoIDs(r.Context(), info.UserID)
	for i := range repos {
		if st, ok := stats[repos[i].RepoID]; ok && st != nil {
			repos[i].CommitsAllTime = st.MetadataCommits
			repos[i].IssuesAllTime = st.MetadataIssues
			repos[i].PRsAllTime = st.MetadataPRs
		}
		repos[i].Starred = starred[repos[i].RepoID]
	}
	if repos == nil {
		repos = []db.PortalGroupRepo{}
	}
	jsonResponse(w, map[string]any{
		"repos": repos, "total": total, "page": page, "page_size": pageSize,
	})
}

// handleGroupOrgs lists the organizations tracked in a group
// (operator report 2026-07-21: the SPA group page showed only
// repositories — a group's tracked orgs were invisible even though
// the backend has carried them in aveloxis_ops.user_org_requests
// since v0.19.x). Display-only read; ownership-checked for
// non-admins exactly like the repos listing. Org REGISTRATION stays
// on POST /groups/{id}/repos with kind=org.
func (s *Server) handleGroupOrgs(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	groupID, err := strconv.ParseInt(r.PathValue("groupID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid group id", http.StatusBadRequest)
		return
	}
	orgs, err := s.store.GetPortalGroupOrgsForUser(r.Context(), info.UserID, groupID, info.IsAdmin)
	if err != nil {
		// Ownership refusal (the dominant case) — mirror handleGroupRepos.
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	type orgJSON struct {
		OrgRequestID int64      `json:"org_request_id"`
		URL          string     `json:"url"`
		Name         string     `json:"name"`
		Platform     string     `json:"platform"`
		LastScanned  *time.Time `json:"last_scanned,omitempty"`
	}
	out := make([]orgJSON, 0, len(orgs))
	for _, o := range orgs {
		out = append(out, orgJSON{o.OrgRequestID, o.OrgURL, o.OrgName, o.Platform, o.LastScanned})
	}
	jsonResponse(w, map[string]any{"orgs": out})
}

// handleGroupAddRepo is the "request access / request collection"
// affordance the compare picker's §2b classes point at. kind=repo
// (default) or org. Ownership is enforced inside the store methods.
//
// v0.27.20 (per-add approval, summary/15): already-tracked repos link
// instantly; a non-admin's NOT-yet-tracked repo or org registration
// creates a pending add-request instead of enqueueing — the response
// carries pending_approval + request_id so the GUI can say "awaiting
// administrator approval" rather than pretending collection started.
//
// 2026-07-21 bulk paste: the body also accepts {"urls": ["...", ...]}
// so a newline-separated paste lands in ONE AddReposToGroup call (one
// approval unit, per v0.27.20) instead of N sequential requests. The
// legacy single-url body stays accepted — `urls` wins when present.
// Orgs remain one per request: an org is already an unbounded add.
func (s *Server) handleGroupAddRepo(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	groupID, err := strconv.ParseInt(r.PathValue("groupID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid group id", http.StatusBadRequest)
		return
	}
	var req struct {
		URL  string   `json:"url"`
		URLs []string `json:"urls"`
		Kind string   `json:"kind"`
	}
	const bodyShape = "body must be {\"url\": \"...\"} or {\"urls\": [\"...\", ...]}, plus \"kind\": \"repo\"|\"org\""
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, bodyShape, http.StatusBadRequest)
		return
	}
	urls := make([]string, 0, len(req.URLs)+1)
	for _, u := range req.URLs {
		if u = strings.TrimSpace(u); u != "" {
			urls = append(urls, u)
		}
	}
	if len(urls) == 0 {
		if u := strings.TrimSpace(req.URL); u != "" {
			urls = append(urls, u)
		}
	}
	if len(urls) == 0 {
		http.Error(w, bodyShape, http.StatusBadRequest)
		return
	}
	resp := map[string]any{"ok": true, "submitted": len(urls)}
	if req.Kind == "org" {
		if len(urls) > 1 {
			http.Error(w, "add organizations one at a time — bulk paste is for repositories", http.StatusBadRequest)
			return
		}
		out, oerr := s.store.AddOrgToGroup(r.Context(), info.UserID, groupID, urls[0])
		err = oerr
		if err == nil && !out.Registered {
			resp["pending_approval"] = 1
			resp["request_id"] = out.RequestID
			s.notifyAddRequestSubmitted(out.RequestID)
		}
	} else {
		out, aerr := s.store.AddReposToGroup(r.Context(), info.UserID, groupID,
			urls, s.autoApproveAddLimit)
		err = aerr
		if err == nil {
			resp["linked"] = out.Linked
			resp["enqueued"] = out.Enqueued
			if out.Pending > 0 {
				resp["pending_approval"] = out.Pending
				resp["request_id"] = out.RequestID
				s.notifyAddRequestSubmitted(out.RequestID)
			}
		}
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	jsonResponse(w, resp)
}

// handleGroupPendingAdds lists the group's own awaiting-approval
// content (v0.27.20) so the GUI's group page can show what's still in
// review. Ownership-checked for non-admins.
func (s *Server) handleGroupPendingAdds(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	groupID, err := strconv.ParseInt(r.PathValue("groupID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid group id", http.StatusBadRequest)
		return
	}
	items, err := s.store.GetPendingAddItemsForUser(r.Context(), info.UserID, groupID, info.IsAdmin)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	type itemJSON struct {
		RequestID int64     `json:"request_id"`
		Kind      string    `json:"kind"`
		URL       string    `json:"url"`
		CreatedAt time.Time `json:"created_at"`
	}
	out := make([]itemJSON, 0, len(items))
	for _, it := range items {
		out = append(out, itemJSON{it.RequestID, it.Kind, it.URL, it.CreatedAt})
	}
	jsonResponse(w, map[string]any{"pending": out})
}

// notifyAddRequestSubmitted emails the operator about a new pending
// add-request (v0.27.20). Best-effort, in a goroutine; no-op without
// a mailer or operator email.
func (s *Server) notifyAddRequestSubmitted(requestID int64) {
	if s.mailer == nil || s.mailer.OperatorEmail() == "" || requestID == 0 {
		return
	}
	go func() {
		defer safego.Recover(s.logger, "add-request-submitted-email")
		pending, err := s.store.ListPendingAddRequests(context.Background())
		if err != nil {
			return
		}
		for _, req := range pending {
			if req.RequestID != requestID {
				continue
			}
			sample := req.SampleURLs
			if req.Kind == "org" {
				sample = []string{req.OrgURL}
			}
			if err := s.mailer.SendAddRequestSubmitted(s.mailer.OperatorEmail(),
				req.UserLogin, req.GroupName, req.Kind, req.ItemCount, sample, req.RequestID); err != nil {
				s.logger.Warn("failed to send add-request email", "request_id", requestID, "error", err)
			}
			return
		}
	}()
}

// handleAdminAddRequests lists the pending per-add approval queue
// (v0.27.20) for the GUI's approvals page.
func (s *Server) handleAdminAddRequests(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	pending, err := s.store.ListPendingAddRequests(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	type reqJSON struct {
		RequestID  int64     `json:"request_id"`
		UserID     int       `json:"user_id"`
		UserLogin  string    `json:"user_login"`
		UserEmail  string    `json:"user_email"`
		GroupID    int64     `json:"group_id"`
		GroupName  string    `json:"group_name"`
		Kind       string    `json:"kind"`
		OrgURL     string    `json:"org_url,omitempty"`
		ItemCount  int       `json:"item_count"`
		SampleURLs []string  `json:"sample_urls,omitempty"`
		CreatedAt  time.Time `json:"created_at"`
	}
	out := make([]reqJSON, 0, len(pending))
	for _, p := range pending {
		out = append(out, reqJSON{p.RequestID, p.UserID, p.UserLogin, p.UserEmail,
			p.GroupID, p.GroupName, p.Kind, p.OrgURL, p.ItemCount, p.SampleURLs, p.CreatedAt})
	}
	jsonResponse(w, map[string]any{"pending": out})
}

// handleAdminAddRequestDecision approves/rejects one add-request.
// Approval processing (UpsertRepo + enqueue + link per item) runs in
// the background so a large batch doesn't block the admin's request;
// re-approving resumes an interrupted pass (items are stamped as they
// process). Org approvals register tracking here; the actual repo
// scan happens on the scheduler's next refreshUserOrgs tick — the api
// process deliberately has no platform API keys.
func (s *Server) handleAdminAddRequestDecision(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	requestID, err := strconv.ParseInt(r.PathValue("requestID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid request id", http.StatusBadRequest)
		return
	}
	var approve bool
	switch r.PathValue("decision") {
	case "approve":
		approve = true
	case "reject":
		approve = false
	default:
		http.Error(w, "decision must be approve or reject", http.StatusBadRequest)
		return
	}
	req, changed, err := s.store.DecideAddRequest(r.Context(), requestID, info.UserID, approve)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if changed {
		if approve && req.Kind != "org" {
			go func() {
				defer safego.Recover(s.logger, "approved-add-request")
				n, err := s.store.ProcessApprovedAddRequest(context.Background(), req.RequestID)
				if err != nil {
					s.logger.Warn("processing approved add-request failed — re-approving resumes it",
						"request_id", req.RequestID, "processed", n, "error", err)
					return
				}
				s.logger.Info("approved add-request processed", "request_id", req.RequestID, "repos", n)
			}()
		}
		if s.mailer != nil && req.UserEmail != "" {
			req := req
			go func() {
				defer safego.Recover(s.logger, "add-request-decided-email")
				if err := s.mailer.SendAddRequestDecided(req.UserEmail, req.UserLogin,
					req.GroupName, req.Kind, approve, req.ItemCount); err != nil {
					s.logger.Warn("failed to send add-request decision email",
						"request_id", req.RequestID, "error", err)
				}
			}()
		}
		// Approval changes the requester's repo scope — drop the
		// token-validation cache so their next request sees it.
		s.auth.invalidateAll()
	}
	jsonResponse(w, map[string]any{"ok": true, "changed": changed})
}

// --- admin ---

func (s *Server) handleAdminUsers(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	users, err := s.store.ListUsers(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	type userJSON struct {
		UserID    int       `json:"user_id"`
		Login     string    `json:"login"`
		Email     string    `json:"email"`
		Provider  string    `json:"provider"`
		IsAdmin   bool      `json:"is_admin"`
		CreatedAt time.Time `json:"created_at"`
	}
	out := make([]userJSON, 0, len(users))
	for _, u := range users {
		out = append(out, userJSON{u.UserID, u.Login, u.Email, u.Provider, u.IsAdmin, u.CreatedAt})
	}
	jsonResponse(w, map[string]any{"users": out})
}

func (s *Server) handleAdminSetUserAdmin(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	targetID, err := strconv.Atoi(r.PathValue("userID"))
	if err != nil {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	var req struct {
		Admin bool `json:"admin"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "body must be {\"admin\": true|false}", http.StatusBadRequest)
		return
	}
	// Never demote yourself to zero admins — mirror the web GUI's
	// last-admin guard by refusing self-demotion outright (simplest
	// safe rule; the web GUI remains for finer cases).
	if !req.Admin && targetID == info.UserID {
		http.Error(w, "refusing self-demotion — use another admin account", http.StatusBadRequest)
		return
	}
	if err := s.store.SetUserAdmin(r.Context(), targetID, req.Admin); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Role changed — drop the token-validation cache so the target's
	// own /me reflects the new role immediately, not after the TTL.
	s.auth.invalidateAll()
	jsonResponse(w, map[string]any{"ok": true})
}

func (s *Server) handleAdminPendingGroups(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	pending, err := s.store.ListPendingGroups(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	type pendingJSON struct {
		GroupID     int64     `json:"group_id"`
		Name        string    `json:"name"`
		UserID      int       `json:"user_id"`
		UserLogin   string    `json:"user_login"`
		UserEmail   string    `json:"user_email"`
		RepoCount   int       `json:"repo_count"`
		OrgRequests int       `json:"org_requests"`
		CreatedAt   time.Time `json:"created_at"`
	}
	out := make([]pendingJSON, 0, len(pending))
	for _, p := range pending {
		out = append(out, pendingJSON{p.GroupID, p.Name, p.UserID, p.UserLogin, p.UserEmail, p.RepoCount, p.OrgRequests, p.CreatedAt})
	}
	jsonResponse(w, map[string]any{"pending": out})
}

func (s *Server) handleAdminGroupDecision(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	groupID, err := strconv.ParseInt(r.PathValue("groupID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid group id", http.StatusBadRequest)
		return
	}
	decision := r.PathValue("decision")
	switch decision {
	case "approve":
		err = s.store.ApproveGroup(r.Context(), groupID, info.UserID)
	case "reject":
		err = s.store.RejectGroup(r.Context(), groupID, info.UserID)
	default:
		http.Error(w, "decision must be approve or reject", http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// v0.27.20 parity fix: the web handler has emailed the requester
	// on approval since v0.19.0; the portal path silently didn't.
	if decision == "approve" && s.mailer != nil {
		var requesterEmail, requesterLogin, groupName string
		_ = s.store.Pool().QueryRow(r.Context(), `
			SELECT COALESCE(u.email, ''), u.login_name, g.name
			FROM aveloxis_ops.user_groups g
			JOIN aveloxis_ops.users u ON u.user_id = g.user_id
			WHERE g.group_id = $1`,
			groupID).Scan(&requesterEmail, &requesterLogin, &groupName)
		if requesterEmail != "" {
			go func() {
				defer safego.Recover(s.logger, "group-approved-email")
				if err := s.mailer.SendGroupApproved(requesterEmail, requesterLogin, groupName, groupID); err != nil {
					s.logger.Warn("failed to send group-approved email", "group_id", groupID, "error", err)
				}
			}()
		}
	}
	// Approval changes the requester's repo scope — drop the
	// token-validation cache so their next request sees it.
	s.auth.invalidateAll()
	jsonResponse(w, map[string]any{"ok": true})
}

func (s *Server) handleAdminMonitorStats(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	stats, err := s.store.QueueStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]any{"queue": stats})
}

func (s *Server) handleAdminMonitorQueue(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	const pageSize = 100
	jobs, total, err := s.store.ListQueuePage(r.Context(), pageSize, (page-1)*pageSize, r.URL.Query().Get("q"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Attach repo names — ids alone mean nothing to users (operator) —
	// and the repo_info meta counts so gathered-vs-metadata pairs render
	// side by side (operator, 2026-07-15).
	ids := make([]int64, 0, len(jobs))
	for _, j := range jobs {
		ids = append(ids, j.RepoID)
	}
	repos, _ := s.store.GetReposBatch(r.Context(), ids)
	stats, _ := s.store.GetRepoStatsBatch(r.Context(), ids)
	type jobJSON struct {
		RepoID        int64      `json:"repo_id"`
		Repo          string     `json:"repo"`
		Status        string     `json:"status"`
		Priority      int        `json:"priority"`
		DueAt         time.Time  `json:"due_at"`
		LastCollected *time.Time `json:"last_collected,omitempty"`
		LastError     *string    `json:"last_error,omitempty"`
		Issues        int        `json:"issues"`
		PRs           int        `json:"prs"`
		Commits       int        `json:"commits"`
		MetaIssues    int        `json:"meta_issues"`
		MetaPRs       int        `json:"meta_prs"`
		MetaCommits   int        `json:"meta_commits"`
	}
	out := make([]jobJSON, 0, len(jobs))
	for _, j := range jobs {
		label := strconv.FormatInt(j.RepoID, 10)
		if rp, ok := repos[j.RepoID]; ok && rp != nil {
			label = rp.Owner + "/" + rp.Name
		}
		row := jobJSON{RepoID: j.RepoID, Repo: label, Status: j.Status, Priority: j.Priority,
			DueAt: j.DueAt, LastCollected: j.LastCollected, LastError: j.LastError,
			Issues: j.LastIssues, PRs: j.LastPRs, Commits: j.LastCommits}
		if st, ok := stats[j.RepoID]; ok && st != nil {
			row.MetaIssues, row.MetaPRs, row.MetaCommits = st.MetadataIssues, st.MetadataPRs, st.MetadataCommits
		}
		out = append(out, row)
	}
	jsonResponse(w, map[string]any{
		"jobs": out, "total": total, "page": page, "page_size": pageSize,
	})
}

// handleAdminPrioritizeRepo (v0.27.14) is the SPA monitor's "Boost"
// button: pure reuse of store.PrioritizeRepo — the identical call the
// legacy :5555 monitor's POST /api/prioritize/{repoID} makes (push to
// priority 0, due now, back to 'queued').
func (s *Server) handleAdminPrioritizeRepo(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo id", http.StatusBadRequest)
		return
	}
	if err := s.store.PrioritizeRepo(r.Context(), repoID); err != nil {
		if strings.Contains(err.Error(), "not found") {
			http.Error(w, "repo not found in queue", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]any{"ok": true, "repo_id": repoID})
}

// --- v0.27.4: home tab (stars + activity) ---

// handleStarRepo stars (PUT) or unstars (DELETE) a repo for the
// signed-in user. Requires identity unconditionally — stars are
// per-user state. The repo must be in the caller's scope.
func (s *Server) handleStarRepo(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo id", http.StatusBadRequest)
		return
	}
	// v0.27.4 (operator decision): starring an out-of-scope repo
	// auto-adds it to the user's implicit "Starred" group instead of
	// 403ing. Search only surfaces already-collected repos, so this
	// can never trigger new collection — and approval exists to gate
	// new collection (the 50,000-repo bulk-add case), never to gate
	// access to data we already have. Unstar needs no scope at all.
	addedToGroup := ""
	if r.Method != http.MethodDelete && !info.IsAdmin && !info.Scope[repoID] {
		gid, gerr := s.store.FindOrCreateStarredGroup(r.Context(), info.UserID)
		if gerr == nil {
			gerr = s.store.AddRepoToGroupByID(r.Context(), gid, repoID)
		}
		if gerr != nil {
			http.Error(w, "could not add repository to your Starred group", http.StatusInternalServerError)
			return
		}
		addedToGroup = db.StarredGroupName
		// Scope changed — the user's cached token validation must
		// re-resolve so their next data request sees the repo.
		s.auth.invalidateAll()
	}
	if r.Method == http.MethodDelete {
		err = s.store.UnstarRepo(r.Context(), info.UserID, repoID)
	} else {
		err = s.store.StarRepo(r.Context(), info.UserID, repoID)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.homeCache.invalidate(info.UserID)
	resp := map[string]any{"ok": true, "starred": r.Method != http.MethodDelete}
	if addedToGroup != "" {
		resp["added_to_group"] = addedToGroup
	}
	jsonResponse(w, resp)
}

// handleHomeRepos returns the signed-in user's home-tab repo list:
// starred repos first (always shown), then the most active repos from
// their own groups over the trailing 90 days.
func (s *Server) handleHomeRepos(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if body, ok := s.homeCache.get(info.UserID); ok {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "hit")
		_, _ = w.Write(body)
		return
	}
	repos, err := s.store.GetHomeRepos(r.Context(), info.UserID, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	body, _ := json.Marshal(map[string]any{"repos": repos})
	s.homeCache.set(info.UserID, body)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// homeReposCache bounds the cost of GetHomeRepos: ~5s cold on a
// fleet-scale admin group set (86,909 repos), so navigating back to
// the home tab shouldn't re-run it. Invalidated per-user on
// star/unstar so toggles survive a reload within the TTL.
type homeReposCache struct {
	mu      sync.Mutex
	entries map[int]homeCacheEntry
}

type homeCacheEntry struct {
	body    []byte
	expires time.Time
}

const homeReposCacheTTL = 5 * time.Minute

func (c *homeReposCache) get(userID int) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[userID]
	if !ok || time.Now().After(e.expires) {
		return nil, false
	}
	return e.body, true
}

func (c *homeReposCache) set(userID int, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[int]homeCacheEntry{}
	}
	if len(c.entries) > 10000 {
		c.entries = map[int]homeCacheEntry{}
	}
	c.entries[userID] = homeCacheEntry{body: body, expires: time.Now().Add(homeReposCacheTTL)}
}

func (c *homeReposCache) invalidate(userID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, userID)
}
