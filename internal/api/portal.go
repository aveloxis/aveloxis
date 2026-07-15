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

	"github.com/aveloxis/aveloxis/internal/db"
	"net/http"
	"strconv"
	"strings"
	"sync"
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jobs": out, "total": total, "page": page, "page_size": pageSize,
	})
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
	w.Header().Set("Content-Type", "application/json")
	resp := map[string]any{"ok": true, "starred": r.Method != http.MethodDelete}
	if addedToGroup != "" {
		resp["added_to_group"] = addedToGroup
	}
	_ = json.NewEncoder(w).Encode(resp)
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
