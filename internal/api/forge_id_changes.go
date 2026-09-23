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
//	POST /api/v1/admin/forge-id-changes/{repoID}/adopt
//	     {"old_forge_id": "…", "new_forge_id": "…", "note": "…"}  (both IDs required; note optional)
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

// adoptBodyMaxBytes bounds the raw request body. JSON may spell one byte
// of the note as a six-byte \uXXXX escape, so a note at the limit can take
// six times its size on the wire; the two forge IDs and the object's keys
// fit in the fixed allowance (a forge ID is a decimal integer, at most 19
// digits for an int64). The note's own limit is checked after decoding.
const adoptBodyMaxBytes = 6*adoptNoteMaxBytes + 256

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
	// The body names the change the admin chose (review of v0.29.63:
	// adopting "the repository's newest pending change" adopted a pair the
	// admin had not clicked).
	var body struct {
		OldForgeID string `json:"old_forge_id"`
		NewForgeID string `json:"new_forge_id"`
		Note       string `json:"note"`
	}
	raw, rerr := io.ReadAll(io.LimitReader(r.Body, adoptBodyMaxBytes+1))
	if rerr != nil {
		s.logger.Warn("admin forge-ID adopt: request body unreadable", "repo_id", repoID, "error", rerr)
		http.Error(w, "could not read the request body", http.StatusBadRequest)
		return
	}
	if len(raw) > adoptBodyMaxBytes {
		http.Error(w, "request body too large", http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		http.Error(w, `invalid JSON body: want {"old_forge_id", "new_forge_id", "note"}`, http.StatusBadRequest)
		return
	}
	if body.OldForgeID == "" || body.NewForgeID == "" {
		http.Error(w, "old_forge_id and new_forge_id are required: they name the change to adopt", http.StatusBadRequest)
		return
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
	err = s.store.AdoptPendingForgeIDChange(r.Context(), repoID, body.OldForgeID, body.NewForgeID, adoptedBy, body.Note)
	switch {
	case errors.Is(err, db.ErrNoPendingForgeIDChange):
		s.logger.Warn("admin forge-ID adopt refused: no such pending change", "repo_id", repoID, "old", logSafe(body.OldForgeID), "new", logSafe(body.NewForgeID), "adopted_by", adoptedBy)
		http.Error(w, "no pending forge-ID change with that pair for this repository (already adopted, or never observed)", http.StatusNotFound)
		return
	case errors.Is(err, db.ErrForgeIDNotAsExpected):
		// The stored ID is no longer this change's old ID — usually another
		// change was adopted (the CLI, a sibling row, or a second admin's
		// click on this same change). Nothing was written; the change is
		// superseded and leaves the pending list.
		s.logger.Warn("admin forge-ID adopt refused: superseded", "repo_id", repoID, "old", logSafe(body.OldForgeID), "new", logSafe(body.NewForgeID), "adopted_by", adoptedBy)
		http.Error(w, "superseded: the repository no longer stores "+body.OldForgeID+" — nothing adopted; reload the list", http.StatusConflict)
		return
	case err != nil:
		s.logger.Error("admin forge-ID adopt failed", "repo_id", repoID, "error", err)
		http.Error(w, "adoption failed", http.StatusInternalServerError)
		return
	}
	s.logger.Info("forge-ID change adopted from the admin page", "repo_id", repoID, "old", logSafe(body.OldForgeID), "new", logSafe(body.NewForgeID), "adopted_by", adoptedBy)
	jsonResponse(w, map[string]any{"adopted": true, "repo_id": repoID})
}
