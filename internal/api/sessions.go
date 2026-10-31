// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"net/http"
	"time"

	"github.com/aveloxis/aveloxis/internal/admin"
	"github.com/aveloxis/aveloxis/internal/db"
)

// Sessions (0.29.113, OWASP ASVS 5.0 V7.5.2 and V7.4.5, summary/62): an
// account lists and ends its own sessions; an administrator ends any
// account's. The decisions are the service's (ListSessions,
// RevokeSessions); these handlers decode, call and answer. A session here
// is a web session (a browser), the SPA's /auth/token token or the web
// proxy's child; operator-issued API tokens are not sessions.

type sessionJSON struct {
	Kind           string    `json:"kind"` // "web" or "api"
	FromWebSession bool      `json:"from_web_session"`
	Current        bool      `json:"current"`
	CreatedAt      time.Time `json:"created_at"`
	LastUsedAt     time.Time `json:"last_used_at"`
	ExpiresAt      time.Time `json:"expires_at"`
}

// handleMeSessions — GET /api/v1/me/sessions: the caller's live sessions,
// the one presenting this request marked `current`.
func (s *Server) handleMeSessions(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireSession(w, r)
	if !ok {
		return
	}
	list, err := s.admin.ListSessions(r.Context(), s.actor(info), info.UserID)
	if err != nil {
		if !s.refuse(w, info, err) {
			s.serverError(w, r, "handleMeSessions", err)
		}
		return
	}
	mine := db.HashToken(bearerToken(r))
	out := make([]sessionJSON, 0, len(list))
	for _, u := range list {
		out = append(out, sessionJSON{Kind: u.Kind, FromWebSession: u.FromWebSession, Current: u.TokenHash == mine, CreatedAt: u.CreatedAt, LastUsedAt: u.LastUsedAt, ExpiresAt: u.ExpiresAt})
	}
	setNoStoreHeaders(w.Header())
	s.writeJSON(w, r, map[string]any{"sessions": out})
}

// handleMeSessionsRevoke — POST /api/v1/me/sessions/revoke: ends every
// session and token of the caller's account, this token included; the
// answer is the last one it gets.
func (s *Server) handleMeSessionsRevoke(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireSession(w, r)
	if !ok {
		return
	}
	// A sensitive change like the address (ASVS V7.5.2, the 0.29.125 review).
	if !s.requireRecentSignIn(w, r, "handleMeSessionsRevoke", "signing out everywhere") {
		return
	}
	n, err := s.admin.RevokeSessions(r.Context(), s.actor(info), info.UserID)
	if err != nil {
		if !s.refuse(w, info, err) {
			s.serverError(w, r, "handleMeSessionsRevoke", err)
		}
		return
	}
	s.auth.invalidateUser(info.UserID)
	setNoStoreHeaders(w.Header())
	s.writeJSON(w, r, map[string]any{"ok": true, "ended": n})
}

// handleAdminUserSessionsRevoke — POST /api/v1/admin/users/{userID}/sessions/revoke.
func (s *Server) handleAdminUserSessionsRevoke(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	targetID, err := admin.ParseUserID(r.PathValue("userID"))
	if err != nil {
		writeAuthError(w, http.StatusBadRequest, "invalid user id")
		return
	}
	n, err := s.admin.RevokeSessions(r.Context(), s.actor(info), targetID)
	if err != nil {
		if !s.refuse(w, info, err) {
			s.serverError(w, r, "handleAdminUserSessionsRevoke", err)
		}
		return
	}
	s.auth.invalidateUser(targetID)
	setNoStoreHeaders(w.Header())
	s.writeJSON(w, r, map[string]any{"ok": true, "ended": n})
}
