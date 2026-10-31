// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"fmt"
	"net/http"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// The account page and session termination (0.29.113, OWASP ASVS 5.0
// V7.5.2 and V7.4.5, summary/62). The decisions are the service's
// (ListSessions, RevokeSessions); these handlers render and redirect.

// sessionView is one row of the account page's session list.
type sessionView struct {
	What      string // "This browser", "Another browser", "A token this site minted" …
	Current   bool
	CreatedAt time.Time
	LastUsed  time.Time
	ExpiresAt time.Time
}

func describeSession(u db.UserSession, current bool) string {
	switch {
	case current:
		return "This browser"
	case u.Kind == "web":
		return "Another browser"
	case u.FromWebSession:
		return "A token minted for a browser session (the site's own, or aveloxis-gui's)"
	default:
		return "A token minted for this account"
	}
}

// handleAccount — GET /account: the account's live sessions, newest first,
// the caller's own marked.
func (s *Server) handleAccount(w http.ResponseWriter, r *http.Request, sess *Session) {
	s.renderAccount(w, r, sess, "")
}

// renderAccount draws the account page; errMsg, when set, is shown above
// the sessions with a "sign in again" link (the re-authentication notice).
func (s *Server) renderAccount(w http.ResponseWriter, r *http.Request, sess *Session, errMsg string) {
	list, err := s.admin.ListSessions(r.Context(), sessionActor(sess), sess.UserID)
	if err != nil {
		s.serviceErr(w, r, "handleAccount", err)
		return
	}
	mine := db.HashToken(sess.cookie)
	views := make([]sessionView, 0, len(list))
	for _, u := range list {
		current := u.TokenHash == mine
		views = append(views, sessionView{What: describeSession(u, current), Current: current, CreatedAt: u.CreatedAt, LastUsed: u.LastUsedAt, ExpiresAt: u.ExpiresAt})
	}
	s.render(w, r, "account", map[string]any{
		"Session":     sess,
		"Sessions":    views,
		"IdleMinutes": int(s.cfg.SessionIdle().Minutes()),
		"Notice":      adminNotice(r),
		"Error":       errMsg,
	})
}

// handleAccountSessionsRevoke — POST /account/sessions/revoke: ends every
// session and token of the caller's account, this browser's included, and
// sends the browser to the login page.
func (s *Server) handleAccountSessionsRevoke(w http.ResponseWriter, r *http.Request, sess *Session) {
	// Ending every session is a sensitive change like the address (ASVS
	// V7.5.2, the 0.29.125 review): a hijacked old session could end the
	// owner's others. An older sign-in is sent to sign in again first.
	if !s.recentSignIn(sess) {
		s.renderAccount(w, r, sess, fmt.Sprintf("To sign out everywhere, sign in again first: this session signed in more than %d minutes ago.", int(s.cfg.SessionIdle().Minutes())))
		return
	}
	if _, err := s.admin.RevokeSessions(r.Context(), sessionActor(sess), sess.UserID); err != nil {
		s.serviceErr(w, r, "handleAccountSessionsRevoke", err)
		return
	}
	s.clearBrowserSession(w, sess.cookie)
	http.Redirect(w, r, "/login", http.StatusFound)
}

// clearBrowserSession is the browser half of ending a session, the one
// spelling every path that signs this browser out uses (review round 1 of
// 0.29.113, L18): the proxy's held Bearer for the session is forgotten
// (raw "" when the caller already did, or there is none) and the session
// and form-token cookies are expired. The store half — which rows go — is
// each caller's own.
func (s *Server) clearBrowserSession(w http.ResponseWriter, raw string) {
	if raw != "" {
		s.proxyTokens.forget(db.HashToken(raw))
	}
	http.SetCookie(w, s.expireCookie("aveloxis_session"))
	http.SetCookie(w, s.expireCookie(csrfCookieName))
}

// handleAdminUserSessionsRevokeConfirm — GET /admin/users/{id}/sessions/revoke:
// the confirm page.
func (s *Server) handleAdminUserSessionsRevokeConfirm(w http.ResponseWriter, r *http.Request, sess *Session) {
	userID, ok := pathUserID(r, "id")
	if !ok {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	s.confirmPage(w, r, sess, "End an account's sessions",
		fmt.Sprintf("End every session of account %d? Its browsers and the tokens minted for it stop working now; the account signs in again as usual.", userID),
		fmt.Sprintf("/admin/users/%d/sessions/revoke", userID), "End sessions", "/admin/users", nil)
}

// handleAdminUserSessionsRevoke — POST /admin/users/{id}/sessions/revoke.
func (s *Server) handleAdminUserSessionsRevoke(w http.ResponseWriter, r *http.Request, sess *Session) {
	userID, ok := pathUserID(r, "id")
	if !ok {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	if _, err := s.admin.RevokeSessions(r.Context(), sessionActor(sess), userID); err != nil {
		s.serviceErr(w, r, "handleAdminUserSessionsRevoke", err)
		return
	}
	if userID == sess.UserID { // an administrator ending their own: this browser too
		s.clearBrowserSession(w, sess.cookie)
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}
	http.Redirect(w, r, "/admin/users?ok=sessions_ended", http.StatusFound)
}

// recentSignIn is the re-authentication rule for a sensitive change (ASVS
// V7.5.1, 0.29.113): the session's sign-in is no older than the
// inactivity bound (web.session_idle_minutes) — one knob, one notion of
// "recent". A session without a recorded sign-in (planted in tests) is
// never recent.
func (s *Server) recentSignIn(sess *Session) bool {
	return db.SignInIsRecent(sess.CreatedAt, s.cfg.SessionIdle())
}
