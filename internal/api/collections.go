// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/aveloxis/aveloxis/internal/db"
)

// Collections API (v0.27.63). Reads are for every authenticated user
// (collections are the curated discovery surface on the home page);
// mutation is admin-only. Mutations are POST-everywhere — the CORS
// Allow-Methods header only advertises GET/POST (plus the star PUT/
// DELETE special case), so DELETE/PATCH verbs would break the SPA.

// handleCollectionsList — GET /collections: every collection with its
// live group/repo counts, position-ordered.
func (s *Server) handleCollectionsList(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireUser(w, r); !ok {
		return
	}
	cols, err := s.store.ListCollections(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]any{"collections": cols})
}

// handleCollectionDetail — GET /collections/{collectionID}?page&page_size:
// the member groups + one page of the DEDUPED repo set.
func (s *Server) handleCollectionDetail(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireUser(w, r); !ok {
		return
	}
	collID, err := strconv.ParseInt(r.PathValue("collectionID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid collection_id", http.StatusBadRequest)
		return
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))

	groups, err := s.store.GetCollectionGroups(r.Context(), collID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	repos, total, err := s.store.GetCollectionRepos(r.Context(), collID, page, pageSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Echo the EFFECTIVE paging values (mirrors the store's clamps —
	// the v0.27.65 split-clamp shape): an oversize page_size request
	// returns 100 rows and must say 100, not the default.
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 50
	}
	if pageSize > 100 {
		pageSize = 100
	}
	jsonResponse(w, map[string]any{
		"collection_id": collID,
		"groups":        groups,
		"repos":         repos,
		"total":         total,
		"page":          page,
		"page_size":     pageSize,
	})
}

// handleCollectionCopy — POST /collections/{collectionID}/copy
// {group_id} or {group_name}: link every repo of the collection into
// the caller's group. group_name find-or-creates a group for the
// caller (normal creation rules); group_id must already be theirs.
//
// NEVER enqueues collection (pinned at the store layer): collection
// repos are already tracked; approval gates NEW collection only.
// Post-copy the auth scope cache and the caller's home cache are
// invalidated so the new repos are visible on the very next request.
func (s *Server) handleCollectionCopy(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	collID, err := strconv.ParseInt(r.PathValue("collectionID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid collection_id", http.StatusBadRequest)
		return
	}
	var req struct {
		GroupID   int64  `json:"group_id"`
		GroupName string `json:"group_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	groupID := req.GroupID
	if groupID == 0 {
		if req.GroupName == "" {
			http.Error(w, "group_id or group_name required", http.StatusBadRequest)
			return
		}
		groupID, err = s.store.FindOrCreateUserGroupByName(r.Context(), info.UserID, req.GroupName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	added, err := s.store.CopyCollectionToGroup(r.Context(), collID, info.UserID, groupID)
	if err != nil {
		if errors.Is(err, db.ErrNotGroupOwner) {
			http.Error(w, "target group is not yours", http.StatusForbidden)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// The copy widened the caller's repo scope — bust both caches.
	s.auth.invalidateAll()
	s.homeCache.invalidate(info.UserID)
	jsonResponse(w, map[string]any{"added": added, "group_id": groupID})
}

// ─── Admin mutations (requireAdmin, POST-everywhere) ────────────

// handleAdminCollectionCreate — POST /admin/collections
// {name, description, position}.
func (s *Server) handleAdminCollectionCreate(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Position    int    `json:"position"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}
	id, err := s.store.CreateCollection(r.Context(), req.Name, req.Description, req.Position, info.UserID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]any{"collection_id": id})
}

// handleAdminCollectionUpdate — POST /admin/collections/{collectionID}
// {name, description, position}.
func (s *Server) handleAdminCollectionUpdate(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	collID, err := strconv.ParseInt(r.PathValue("collectionID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid collection_id", http.StatusBadRequest)
		return
	}
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Position    int    `json:"position"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}
	if err := s.store.UpdateCollection(r.Context(), collID, req.Name, req.Description, req.Position); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]any{"ok": true})
}

// handleAdminCollectionDelete — POST /admin/collections/{collectionID}/delete.
func (s *Server) handleAdminCollectionDelete(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	collID, err := strconv.ParseInt(r.PathValue("collectionID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid collection_id", http.StatusBadRequest)
		return
	}
	if err := s.store.DeleteCollection(r.Context(), collID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]any{"ok": true})
}

// handleAdminCollectionAddGroup — POST /admin/collections/{collectionID}/groups
// {group_id}: link an admin-owned group into the collection.
func (s *Server) handleAdminCollectionAddGroup(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	collID, err := strconv.ParseInt(r.PathValue("collectionID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid collection_id", http.StatusBadRequest)
		return
	}
	var req struct {
		GroupID int64 `json:"group_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.GroupID == 0 {
		http.Error(w, "group_id required", http.StatusBadRequest)
		return
	}
	if err := s.store.AddGroupToCollection(r.Context(), collID, req.GroupID); err != nil {
		if errors.Is(err, db.ErrGroupNotAdminOwned) {
			http.Error(w, "collection member groups must be admin-owned", http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]any{"ok": true})
}

// handleAdminCollectionRemoveGroup — POST
// /admin/collections/{collectionID}/groups/{groupID}/remove.
func (s *Server) handleAdminCollectionRemoveGroup(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	collID, err := strconv.ParseInt(r.PathValue("collectionID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid collection_id", http.StatusBadRequest)
		return
	}
	groupID, err := strconv.ParseInt(r.PathValue("groupID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid group_id", http.StatusBadRequest)
		return
	}
	if err := s.store.RemoveGroupFromCollection(r.Context(), collID, groupID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]any{"ok": true})
}
