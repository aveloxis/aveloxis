// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
)

// sessionCookie builds a session cookie with security attributes set from config.
// Secure is true in production (default), false when dev_mode is enabled.
// HttpOnly is always true.
func (s *Server) sessionCookie(token string) *http.Cookie {
	return &http.Cookie{
		Name:     "aveloxis_session",
		Value:    token,
		MaxAge:   86400,
		Path:     "/",
		HttpOnly: true,
		Secure:   !s.cfg.DevMode,
		SameSite: http.SameSiteLaxMode,
	}
}

// oauthStateCookie builds the short-lived OAuth CSRF state cookie.
func (s *Server) oauthStateCookie(state string) *http.Cookie {
	return &http.Cookie{
		Name:     "oauth_state",
		Value:    state,
		MaxAge:   300,
		Path:     "/",
		HttpOnly: true,
		Secure:   !s.cfg.DevMode,
		SameSite: http.SameSiteLaxMode,
	}
}

// expireCookie builds a cookie that clears (expires) a named cookie.
func (s *Server) expireCookie(name string) *http.Cookie {
	return &http.Cookie{
		Name:     name,
		MaxAge:   -1,
		Path:     "/",
		HttpOnly: true,
		Secure:   !s.cfg.DevMode,
	}
}

func (s *Server) createSession(userID int, loginName, avatarURL, provider string, isAdmin bool) string {
	token := generateToken()
	s.sessionMu.Lock()
	s.sessions[token] = &Session{
		UserID:    userID,
		LoginName: loginName,
		AvatarURL: avatarURL,
		Provider:  provider,
		IsAdmin:   isAdmin,
		ExpiresAt: time.Now().Add(24 * time.Hour),
	}
	s.sessionMu.Unlock()
	return token
}

func (s *Server) getSession(r *http.Request) *Session {
	cookie, err := r.Cookie("aveloxis_session")
	if err != nil {
		return nil
	}
	s.sessionMu.RLock()
	sess, ok := s.sessions[cookie.Value]
	s.sessionMu.RUnlock()
	if !ok || time.Now().After(sess.ExpiresAt) {
		return nil
	}
	return sess
}

func (s *Server) requireAuth(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.getSession(r) == nil {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}
		handler(w, r)
	}
}

// requireAdmin gates a route on the session's IsAdmin flag. Returns
// 403 for authenticated non-admins so they don't even see what's
// behind the route. Unauthenticated users still get redirected to
// /login (matching requireAuth's UX).
func (s *Server) requireAdmin(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sess := s.getSession(r)
		if sess == nil {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}
		if !sess.IsAdmin {
			http.Error(w, "Administrator access required.", http.StatusForbidden)
			return
		}
		handler(w, r)
	}
}

func generateToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// handleAuthToken (v0.27.1) exchanges the caller's web session for a
// DB-backed session token the api process validates. JSON response so
// the SPA can store it (localStorage) and attach it as a Bearer.
func (s *Server) handleAuthToken(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)
	if sess == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	token, err := s.store.CreateSessionToken(r.Context(), sess.UserID, 0)
	if err != nil {
		httpserver.LogFailure(r.Context(), s.logger, slog.LevelError, err, "failed to mint session token", "user_id", sess.UserID, "error", err)
		http.Error(w, "token creation failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"token":      token,
		"expires_at": time.Now().Add(db.DefaultSessionTokenLifetime).UTC().Format(time.RFC3339),
		"user_id":    sess.UserID,
		"login":      sess.LoginName,
	})
}
