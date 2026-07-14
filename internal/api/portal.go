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
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"groups": out})
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"group_id": id})
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
	repos, err := s.store.GetPortalGroupReposForUser(r.Context(), info.UserID, groupID, info.IsAdmin)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"repos": repos})
}

// handleGroupAddRepo is the "request access / request collection"
// affordance the compare picker's §2b classes point at. kind=repo
// (default) or org. Ownership + the v0.19.0 pending-approval flow are
// enforced inside the store methods.
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
		URL  string `json:"url"`
		Kind string `json:"kind"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.URL) == "" {
		http.Error(w, "body must be {\"url\": \"...\", \"kind\": \"repo\"|\"org\"}", http.StatusBadRequest)
		return
	}
	if req.Kind == "org" {
		err = s.store.AddOrgToGroup(r.Context(), info.UserID, groupID, strings.TrimSpace(req.URL))
	} else {
		err = s.store.AddRepoToGroup(r.Context(), info.UserID, groupID, strings.TrimSpace(req.URL))
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"users": out})
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"pending": out})
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
	switch r.PathValue("decision") {
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
	// Approval changes the requester's repo scope — drop the
	// token-validation cache so their next request sees it.
	s.auth.invalidateAll()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"queue": stats})
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
	// Attach repo names — ids alone mean nothing to users (operator).
	ids := make([]int64, 0, len(jobs))
	for _, j := range jobs {
		ids = append(ids, j.RepoID)
	}
	repos, _ := s.store.GetReposBatch(r.Context(), ids)
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
	}
	out := make([]jobJSON, 0, len(jobs))
	for _, j := range jobs {
		label := strconv.FormatInt(j.RepoID, 10)
		if rp, ok := repos[j.RepoID]; ok && rp != nil {
			label = rp.Owner + "/" + rp.Name
		}
		out = append(out, jobJSON{j.RepoID, label, j.Status, j.Priority, j.DueAt,
			j.LastCollected, j.LastError, j.LastIssues, j.LastPRs, j.LastCommits})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jobs": out, "total": total, "page": page, "page_size": pageSize,
	})
}
