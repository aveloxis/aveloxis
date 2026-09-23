// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/aveloxis/aveloxis/internal/db"
)

// Forge-ID changes, admin only (v0.29.63; 2026-09-23 operator request: an
// Adopt button instead of the terminal).
//
//	GET  /api/v1/admin/forge-id-changes?pending=1
//	POST /api/v1/admin/forge-id-changes/{repoID}/adopt   {"note": "…"} (optional)
//
// The org scan records a repository re-created upstream under the same URL
// as a PENDING change (observation-only). Adopting treats the new
// repository as a continuation: it moves the stored forge ID to the one
// the scan observed and records who adopted it, and the repository page
// shows the notice. The api process holds no forge API keys, so the button
// adopts the recorded observation; `aveloxis adopt-forge-id` asks the
// forge live.

func (s *Server) handleAdminForgeIDChanges(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	pendingOnly := r.URL.Query().Get("pending") == "1"
	changes, err := s.store.ListForgeIDChanges(r.Context(), pendingOnly)
	if err != nil {
		s.logger.Error("admin forge-ID changes: list failed", "error", err)
		http.Error(w, "forge-ID changes could not be listed", http.StatusInternalServerError)
		return
	}
	if changes == nil {
		changes = []db.ForgeIDChange{}
	}
	jsonResponse(w, map[string]any{"changes": changes})
}

// adoptNoteMaxBytes bounds the optional note (it is stored and shown to
// operators, not rendered on the public page).
const adoptNoteMaxBytes = 1000

func (s *Server) handleAdminForgeIDAdopt(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil || repoID <= 0 {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	var body struct {
		Note string `json:"note"`
	}
	if r.Body != nil {
		// Room for the JSON wrapper around a maximal note; a body past it
		// is refused as too long rather than cut into invalid JSON.
		const maxBody = adoptNoteMaxBytes + 64
		raw, rerr := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
		if rerr != nil {
			http.Error(w, "could not read the request body", http.StatusBadRequest)
			return
		}
		if len(raw) > maxBody {
			http.Error(w, "note too long", http.StatusBadRequest)
			return
		}
		if len(raw) > 0 {
			if jerr := json.Unmarshal(raw, &body); jerr != nil {
				http.Error(w, "invalid JSON body", http.StatusBadRequest)
				return
			}
		}
	}
	if len(body.Note) > adoptNoteMaxBytes {
		http.Error(w, "note too long", http.StatusBadRequest)
		return
	}
	adoptedBy, lerr := s.store.UserLabel(r.Context(), info.UserID)
	if lerr != nil {
		s.logger.Warn("admin forge-ID adopt: admin label lookup failed — recording the user id", "user_id", info.UserID, "error", lerr)
		adoptedBy = "user " + strconv.Itoa(info.UserID)
	}
	err = s.store.AdoptPendingForgeIDChange(r.Context(), repoID, adoptedBy, body.Note)
	switch {
	case errors.Is(err, db.ErrNoPendingForgeIDChange):
		http.Error(w, "no pending forge-ID change for this repository", http.StatusNotFound)
		return
	case errors.Is(err, db.ErrForgeIDNotAsExpected):
		// The stored ID moved since the scan observed the change: nothing
		// was written; the next scan records the current state.
		http.Error(w, "the stored forge ID changed since this was observed — nothing adopted; the next org scan records the current state", http.StatusConflict)
		return
	case err != nil:
		s.logger.Error("admin forge-ID adopt failed", "repo_id", repoID, "error", err)
		http.Error(w, "adoption failed", http.StatusInternalServerError)
		return
	}
	s.logger.Info("forge-ID change adopted from the admin page", "repo_id", repoID, "adopted_by", adoptedBy)
	jsonResponse(w, map[string]any{"adopted": true, "repo_id": repoID})
}
