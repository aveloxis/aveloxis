// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/mailer"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// oauthCallbackTimeout bounds the OAuth callback's forge round trips AS A
// WHOLE (the code exchange, /user and, on GitHub, /user/emails share one
// context): three sequential requests to one forge × the 10 s the scorecard
// rate-limit probe grants one GitHub request (scorecard.go's client). A
// browser waiting on the callback is better served by an error than by an
// unbounded spinner. A var so the runtime test can shorten it.
var oauthCallbackTimeout = 30 * time.Second

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	// v0.20.13: surface a "Sign out" button on the login page when
	// the visitor already has an active session. Use case: switching
	// from user A to user B without manually clearing cookies in
	// DevTools. The /logout handler does the cookie/session cleanup;
	// this template flag just makes the affordance discoverable.
	s.render(w, "login", map[string]any{
		"HasGitHub":  s.ghOAuth != nil,
		"HasGitLab":  s.glOAuth != nil,
		"HasSession": s.getSession(r) != nil,
	})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("aveloxis_session")
	if err == nil {
		s.sessionMu.Lock()
		delete(s.sessions, cookie.Value)
		s.sessionMu.Unlock()
	}
	http.SetCookie(w, s.expireCookie("aveloxis_session"))
	// v0.20.13: also expire oauth_state defensively. A user who
	// reached /auth/github but never completed the callback leaves
	// this cookie behind. Clearing it alongside the session means
	// the "switch users" flow exits with no stale aveloxis-side
	// cookie state. (oauth_state has a 5-minute MaxAge so it would
	// expire on its own soon, but cleaning it on explicit logout
	// is the principled behavior.)
	http.SetCookie(w, s.expireCookie("oauth_state"))
	http.Redirect(w, r, "/login", http.StatusFound)
}

// postLoginRedirect resolves where the OAuth callback should send the
// browser. The SPA's login page passes ?next=<its own URL> when
// initiating OAuth; handleGitHubAuth/handleGitLabAuth stash it in a
// short-lived cookie, and the callback honors it ONLY when it is a
// same-site relative path or sits under the operator-configured
// web.spa_url origin — anything else (open-redirect attempts) falls
// back to the server-rendered /dashboard.
func (s *Server) postLoginRedirect(r *http.Request) string {
	c, err := r.Cookie("oauth_next")
	if err != nil || c.Value == "" {
		return "/dashboard"
	}
	if target := safeNextTarget(c.Value, s.cfg.SPAURL); target != "" {
		return target
	}
	s.logger.Warn("ignoring untrusted post-login next URL", "next", truncateForLog([]byte(c.Value), 120))
	return "/dashboard"
}

// safeNextTarget validates a post-login redirect destination and
// returns "" when it is untrusted. Extracted as a pure function
// (v0.27.10) so the open-redirect matrix — including inputs Go's own
// cookie transport would sanitize away — is directly testable.
//
// Relative paths only: browsers treat both "//host" AND "/\host" as
// protocol-relative absolute URLs (backslash is normalized to slash),
// so a bare leading-slash check is an open-redirect bypass
// (CodeQL go/bad-redirect-check, fixed v0.27.10). Absolute URLs are
// honored ONLY under the configured spa_url origin.
func safeNextTarget(next, spaURL string) string {
	if strings.HasPrefix(next, "/") && !strings.HasPrefix(next, "//") &&
		!strings.HasPrefix(next, `/\`) {
		return next
	}
	// spa_url arrives canonical (no trailing slash; config refuses one at
	// load, 2026-10-04), so the prefix test needs no trim here.
	if spa := spaURL; spa != "" &&
		(next == spa || strings.HasPrefix(next, spa+"/")) {
		return next
	}
	return ""
}

// stashNext records a login flow's ?next= destination for the callback.
func (s *Server) stashNext(w http.ResponseWriter, r *http.Request) {
	if next := r.URL.Query().Get("next"); next != "" {
		http.SetCookie(w, &http.Cookie{
			Name: "oauth_next", Value: next, Path: "/",
			MaxAge: 300, HttpOnly: true, Secure: !s.cfg.DevMode, SameSite: http.SameSiteLaxMode,
		})
	}
}

// clearNext expires the oauth_next cookie once consumed. The expiry
// cookie carries the same attributes as stashNext's original — the
// house rule (HttpOnly always; Secure unless dev_mode) applies to
// every Set-Cookie we emit, deletions included (v0.27.10).
func (s *Server) clearNext(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name: "oauth_next", Value: "", Path: "/",
		MaxAge: -1, HttpOnly: true, Secure: !s.cfg.DevMode, SameSite: http.SameSiteLaxMode,
	})
}

func (s *Server) handleGitHubAuth(w http.ResponseWriter, r *http.Request) {
	if s.ghOAuth == nil {
		http.Error(w, "GitHub OAuth not configured", http.StatusBadRequest)
		return
	}
	state := generateToken()
	http.SetCookie(w, s.oauthStateCookie(state))
	s.stashNext(w, r)
	http.Redirect(w, r, s.ghOAuth.AuthCodeURL(state), http.StatusFound)
}

func (s *Server) handleGitHubCallback(w http.ResponseWriter, r *http.Request) {
	if s.ghOAuth == nil {
		http.Error(w, "GitHub OAuth not configured", http.StatusBadRequest)
		return
	}

	// Verify state.
	stateCookie, err := r.Cookie("oauth_state")
	if err != nil || stateCookie.Value != r.URL.Query().Get("state") {
		http.Error(w, "Invalid OAuth state", http.StatusBadRequest)
		return
	}

	// Exchange code for token. Bounded (batch 7c review round 3): oauth2
	// builds on http.DefaultClient, which has no timeout, so a stalled forge
	// held the callback for as long as the browser waited.
	ctx, cancel := context.WithTimeout(r.Context(), oauthCallbackTimeout)
	defer cancel()
	token, err := s.ghOAuth.Exchange(ctx, r.URL.Query().Get("code"))
	if err != nil {
		s.logOAuthFailure(r.Context(), "github", "exchange", err)
		http.Error(w, "OAuth exchange failed", http.StatusInternalServerError)
		return
	}

	// Get user info from GitHub.
	client := s.ghOAuth.Client(ctx, token)
	userReq, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/user", nil)
	if err != nil {
		s.logger.Error("building the GitHub user request failed", "error", err)
		http.Error(w, "Failed to get user info", http.StatusInternalServerError)
		return
	}
	userReq.Header.Set("X-GitHub-Api-Version", platform.GitHubAPIVersion) // every GitHub REST request pins the version (worklist item 15)
	resp, err := client.Do(userReq)
	if err != nil {
		s.logOAuthFailure(r.Context(), "github", "user", err)
		http.Error(w, "Failed to get user info", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()
	// The body read is part of the user request: a forge that sends the
	// headers and stalls the body hits the callback bound HERE, and a
	// dropped error sent the partial body on to the decode as if the forge
	// had sent it (PR #218 review C9).
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		s.logOAuthFailure(r.Context(), "github", "user", err)
		http.Error(w, "Failed to get user info", http.StatusInternalServerError)
		return
	}

	if resp.StatusCode != http.StatusOK {
		s.logger.Error("github /user returned non-200",
			"status", resp.StatusCode, "body", truncateForLog(body, 200))
		http.Error(w, "GitHub user fetch failed", http.StatusBadGateway)
		return
	}
	// A 200 is the data answer; an error body is not a size (fix review V3).
	platform.NoteResponseSize(s.logger, "oauth-github-user", userReq.URL.String(), int64(len(body)))

	var ghUser struct {
		ID        int64  `json:"id"`
		Login     string `json:"login"`
		Name      string `json:"name"`
		Email     string `json:"email"`
		AvatarURL string `json:"avatar_url"`
	}
	if err := json.Unmarshal(body, &ghUser); err != nil {
		s.logger.Error("github /user response unmarshal failed", "error", err)
		http.Error(w, "GitHub user payload invalid", http.StatusBadGateway)
		return
	}
	if strings.TrimSpace(ghUser.Login) == "" {
		s.logger.Error("github /user response missing login field", "body", truncateForLog(body, 200))
		http.Error(w, "GitHub did not return a login. Try again, or check token scopes.", http.StatusBadGateway)
		return
	}

	// v0.19.10: when /user returns an empty email field — which happens
	// when the user has set their email to private — fall back to
	// GitHub's /user/emails endpoint. The user:email OAuth scope is
	// already requested at OAuth init (see ghOAuth.Scopes), so this
	// call works without the user re-authorizing. We pick the primary
	// verified email; if none, the first verified email; if still
	// nothing, we fall through to the email-prompt flow at
	// /account/email.
	if ghUser.Email == "" {
		if email := fetchGitHubPrimaryEmail(r.Context(), ctx, client, s.logger); email != "" {
			ghUser.Email = email
		}
	}

	// v0.19.0: SignInOAuthUser auto-promotes the first-ever user to
	// admin so a fresh deployment can review subsequent submissions.
	s.completeOAuthLogin(w, r, db.OAuthUserInfo{
		Login:     ghUser.Login,
		Email:     ghUser.Email,
		Name:      ghUser.Name,
		AvatarURL: ghUser.AvatarURL,
		GHUserID:  ghUser.ID,
		GHLogin:   ghUser.Login,
		Provider:  "github",
		// v0.29.89: the browser's address, for the sign-up quota.
		SignupAddr: s.SignupAddress(r),
	}, "GitHub")
}

// StampLegacyGitLabAccounts records this web's GitLab instance on every
// GitLab account from before the instance was recorded (final review round
// 3): such an account matches no instance at sign-in, so without the stamp
// its owner is refused. `aveloxis web` calls it once at start. A failure is
// logged and not fatal: those owners are refused (fail closed) until a
// later start stamps them.
func (s *Server) StampLegacyGitLabAccounts(ctx context.Context) {
	if s.glOAuth == nil || s.store == nil {
		return
	}
	n, err := s.store.StampLegacyGitLabHost(ctx, s.cfg.GitLabBaseURL)
	if err != nil && ctx.Err() != nil {
		return // a stop during start-up, not a failed stamp (round 4); the next start stamps
	}
	if err != nil {
		httpserver.LogFailure(ctx, s.logger, slog.LevelError, err, "recording the GitLab instance on accounts from before 0.29.69 failed — their owners cannot sign in through GitLab until a later web start stamps them", "instance", db.GitLabOAuthHost(s.cfg.GitLabBaseURL), "error", err)
		return
	}
	if n > 0 {
		s.logger.Info("recorded the GitLab instance on accounts from before 0.29.69", "instance", db.GitLabOAuthHost(s.cfg.GitLabBaseURL), "accounts", n)
	}
}

// logOAuthFailure logs a failed forge request on an OAuth callback (final
// whole-tree review F4, 2026-09-28: the exchange and user-request arms
// returned without a log line, so the callback bound expired silently). A
// browser that left mid-callback is not a failure and is not logged; an
// expired bound says so. The browser is shown a fixed message, never err.
func (s *Server) logOAuthFailure(reqCtx context.Context, provider, phase string, err error) {
	switch {
	case reqCtx.Err() != nil:
		// The request itself ended — the client left, or http_timeout_seconds
		// fired (its WARN reports it) — so this is not the callback bound's
		// expiry (NET-6 review r3 F3: with the knob under 30 s, this logged
		// ERROR naming the wrong bound).
		s.logger.Debug("oauth callback: the request ended before the forge answered", "provider", provider, "phase", phase, "error", err)
		return
	case errors.Is(err, context.DeadlineExceeded):
		s.logger.Error("oauth callback: the forge did not answer within the callback bound",
			"provider", provider, "phase", phase, "bound", oauthCallbackTimeout, "error", err)
	default:
		s.logger.Error("oauth callback: forge request failed", "provider", provider, "phase", phase, "error", err)
	}
}

// completeOAuthLogin is the shared tail of both OAuth callbacks
// (v0.27.42, summary/18 Phase 4 — the two callbacks previously
// duplicated this sequence line for line): first-signup detection,
// user upsert, welcome email, fresh admin flag, session creation, and
// the post-login redirect.
func (s *Server) completeOAuthLogin(w http.ResponseWriter, r *http.Request, info db.OAuthUserInfo, providerLabel string) {
	// The store says whether this sign-in created the account (final
	// review round 3: a separate name-keyed COUNT here disagreed with the
	// store's identity rule — a renamed user read as new, a new user on
	// another GitLab instance as returning). Only a created account gets
	// the welcome.
	userID, wasNewUser, err := s.store.SignInOAuthUser(r.Context(), info)
	if httpserver.RequestEnded(r.Context(), err) {
		return // the browser left mid-callback: nothing to serve, not a failure
	}
	if ex, capped := capacity.AsExceeded(err); capped {
		// The kind refusal (operator 2026-10-10): the limit, why, and whom to
		// write; logged once a day per address by the store.
		writeSignupRefusal(w, ex)
		return
	}
	if err != nil {
		s.logger.Error("failed to upsert OAuth user", "error", err)
		http.Error(w, "Failed to create user", http.StatusInternalServerError)
		return
	}

	// Send welcome email on first signup. No-op if mailer
	// unconfigured. Failures here don't block login — the email is a
	// nice-to-have, not a gate. Before the admin-flag lookup: the mail
	// needs no client, and the user row is already committed, so a
	// browser that leaves during the lookup below must not cost a first
	// signup its welcome (batch-2 review round 4).
	if wasNewUser && s.mailer != nil && info.Email != "" {
		if err := s.mailer.SendWelcome(info.Email, info.Login, providerLabel); err != nil && !mailer.IsSkip(err) {
			s.logger.Warn("failed to send welcome email", "login", truncateForLog([]byte(info.Login), 100), "error", err)
		}
	}

	// Read fresh admin flag — set to TRUE for the first-ever user
	// (auto-promotion in SignInOAuthUser) and stays whatever the admin
	// user-management page set it to thereafter. A failed lookup is logged
	// and the session is a non-admin one: the user can sign in again once
	// the store answers (worklist follow-up 6).
	isAdmin, err := s.store.IsUserAdmin(r.Context(), userID)
	if httpserver.RequestEnded(r.Context(), err) {
		return // the browser left mid-callback: nothing to serve, not a failure
	}
	if err != nil {
		s.logger.Error("admin flag lookup failed at login — session created as non-admin", "user_id", userID, "error", err)
	}

	sessToken := s.createSession(userID, info.Login, info.AvatarURL, info.Provider, isAdmin)
	http.SetCookie(w, s.sessionCookie(sessToken))
	dest := s.postLoginRedirect(r)
	s.clearNext(w)
	http.Redirect(w, r, dest, http.StatusFound)
}

// fetchGitHubPrimaryEmail calls GitHub's /user/emails endpoint and
// returns the user's primary verified email, or the first verified
// email if no primary is flagged, or "" if no verified email exists.
// Used as the v0.19.10 fallback when /user returned an empty email
// field (the common case for users with private email visibility).
//
// The user:email OAuth scope must be requested by ghOAuth — without
// it, /user/emails returns 404. The scope IS requested as of v0.19.0;
// see ghOAuth.Scopes in NewServer.
// reqCtx is the request's own context — it classifies a failure (NET-6
// review r6 F1: classifying by ctx, the callback's 30 s bound, hid that
// bound's expiry on a live request at Debug); ctx bounds the forge read.
func fetchGitHubPrimaryEmail(reqCtx, ctx context.Context, client *http.Client, logger *slog.Logger) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/user/emails", nil)
	if err != nil {
		httpserver.LogFailure(reqCtx, logger, slog.LevelError, err, "building the GitHub emails request failed", "error", err)
		return ""
	}
	req.Header.Set("X-GitHub-Api-Version", platform.GitHubAPIVersion)
	resp, err := client.Do(req)
	if err != nil {
		httpserver.LogFailure(reqCtx, logger, slog.LevelWarn, err, "failed to call /user/emails for OAuth fallback", "callback_bound", oauthCallbackTimeout, "error", err)
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		logger.Warn("github /user/emails returned non-200",
			"status", resp.StatusCode,
			"hint", "scope user:email may not be granted")
		return ""
	}
	var emails []struct {
		Email    string `json:"email"`
		Primary  bool   `json:"primary"`
		Verified bool   `json:"verified"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&emails); err != nil {
		httpserver.LogFailure(reqCtx, logger, slog.LevelWarn, err, "failed to decode /user/emails", "error", err)
		return ""
	}
	for _, e := range emails {
		if e.Primary && e.Verified {
			return e.Email
		}
	}
	for _, e := range emails {
		if e.Verified {
			return e.Email
		}
	}
	return ""
}

func (s *Server) handleGitLabAuth(w http.ResponseWriter, r *http.Request) {
	if s.glOAuth == nil {
		http.Error(w, "GitLab OAuth not configured", http.StatusBadRequest)
		return
	}
	state := generateToken()
	http.SetCookie(w, s.oauthStateCookie(state))
	s.stashNext(w, r)
	http.Redirect(w, r, s.glOAuth.AuthCodeURL(state), http.StatusFound)
}

func (s *Server) handleGitLabCallback(w http.ResponseWriter, r *http.Request) {
	if s.glOAuth == nil {
		http.Error(w, "GitLab OAuth not configured", http.StatusBadRequest)
		return
	}

	stateCookie, err := r.Cookie("oauth_state")
	if err != nil || stateCookie.Value != r.URL.Query().Get("state") {
		http.Error(w, "Invalid OAuth state", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), oauthCallbackTimeout) // bounded, see the GitHub callback
	defer cancel()
	token, err := s.glOAuth.Exchange(ctx, r.URL.Query().Get("code"))
	if err != nil {
		s.logOAuthFailure(r.Context(), "gitlab", "exchange", err)
		http.Error(w, "OAuth exchange failed", http.StatusInternalServerError)
		return
	}

	glBase := s.cfg.GitLabBaseURL
	if glBase == "" {
		glBase = "https://gitlab.com"
	}
	client := s.glOAuth.Client(ctx, token)
	// The bound must travel on the REQUEST: oauth2's client copies
	// http.DefaultClient's zero Timeout and uses ctx to refresh a token
	// and to select the base client (oauth2.HTTPClient), never as a
	// per-request deadline, so a plain client.Get ran unbounded (batch 7c
	// review round 4).
	userReq, err := http.NewRequestWithContext(ctx, http.MethodGet, glBase+"/api/v4/user", nil)
	if err != nil {
		s.logger.Error("building the GitLab user request failed", "error", err)
		http.Error(w, "Failed to get user info", http.StatusInternalServerError)
		return
	}
	resp, err := client.Do(userReq)
	if err != nil {
		s.logOAuthFailure(r.Context(), "gitlab", "user", err)
		http.Error(w, "Failed to get user info", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()
	// The body read is part of the user request: a forge that sends the
	// headers and stalls the body hits the callback bound HERE, and a
	// dropped error sent the partial body on to the decode as if the forge
	// had sent it (PR #218 review C9).
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		s.logOAuthFailure(r.Context(), "gitlab", "user", err)
		http.Error(w, "Failed to get user info", http.StatusInternalServerError)
		return
	}

	if resp.StatusCode != http.StatusOK {
		s.logger.Error("gitlab /user returned non-200",
			"status", resp.StatusCode, "body", truncateForLog(body, 200))
		http.Error(w, "GitLab user fetch failed", http.StatusBadGateway)
		return
	}
	// A 200 is the data answer; an error body is not a size (fix review V3).
	platform.NoteResponseSize(s.logger, "oauth-gitlab-user", glBase+"/api/v4/user", int64(len(body)))

	var glUser struct {
		ID        int64  `json:"id"`
		Username  string `json:"username"`
		Name      string `json:"name"`
		Email     string `json:"email"`
		AvatarURL string `json:"avatar_url"`
	}
	if err := json.Unmarshal(body, &glUser); err != nil {
		s.logger.Error("gitlab /user response unmarshal failed", "error", err)
		http.Error(w, "GitLab user payload invalid", http.StatusBadGateway)
		return
	}
	if strings.TrimSpace(glUser.Username) == "" {
		s.logger.Error("gitlab /user response missing username field", "body", truncateForLog(body, 200))
		http.Error(w, "GitLab did not return a username. Try again, or check token scopes.", http.StatusBadGateway)
		return
	}

	s.completeOAuthLogin(w, r, db.OAuthUserInfo{
		Login:      glUser.Username,
		Email:      glUser.Email,
		Name:       glUser.Name,
		AvatarURL:  glUser.AvatarURL,
		GLUserID:   glUser.ID,
		GLHost:     glBase, // the instance that answered; the store normalizes it (GitLabOAuthHost)
		GLUsername: glUser.Username,
		Provider:   "gitlab",
		SignupAddr: s.SignupAddress(r), // v0.29.89: for the sign-up quota
	}, "GitLab")
}
