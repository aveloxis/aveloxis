// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
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
	"golang.org/x/oauth2"
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
	// A ?next= the open-redirect rule accepts rides into the provider links,
	// where beginOAuth stashes it (0.29.113: the e-mail page's "Sign in
	// again" comes back to the form); an unsafe one is dropped here.
	s.render(w, r, "login", map[string]any{
		"HasGitHub":  s.ghOAuth != nil,
		"HasGitLab":  s.glOAuth != nil,
		"HasSession": s.hasSession(r),
		"Next":       safeNextTarget(r.URL.Query().Get("next"), s.cfg.SPAURL),
	})
}

// hasSession is the login page's one question of the request: a lookup
// failure is logged and read as "no session" HERE ONLY — the page is
// public and only decides whether to show the sign-out form.
func (s *Server) hasSession(r *http.Request) bool {
	sess, err := s.getSession(r)
	if err != nil && !httpserver.RequestEnded(r.Context(), err) {
		httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "public page: session lookup failed; treated as no session", "error", err)
	}
	return sess != nil
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := s.readCookie(r, "aveloxis_session"); err == nil {
		// The session row and every Bearer token /auth/token minted from
		// it (B4). A failure is logged; the cookies are cleared regardless
		// — the browser is signed out, and the row expires on its own.
		if err := s.store.DeleteWebSession(r.Context(), cookie.Value); err != nil && !httpserver.RequestEnded(r.Context(), err) {
			httpserver.LogFailure(r.Context(), s.logger, slog.LevelError, err, "logout: the session row could not be deleted — the cookie is cleared; the row expires on its own and the tokens minted from it stay valid until they expire", "error", err)
		}
		s.proxyTokens.forget(db.HashToken(cookie.Value)) // the proxy's Bearer for this session (0.29.110): the store revoked it with the session
	}
	s.clearBrowserSession(w, "") // the session and form-token cookies (the held Bearer was forgotten above)
	// v0.20.13: also expire oauth_state defensively. A user who
	// reached /auth/github but never completed the callback leaves
	// this cookie behind. Clearing it alongside the session means
	// the "switch users" flow exits with no stale aveloxis-side
	// cookie state. (oauth_state has a 5-minute MaxAge so it would
	// expire on its own soon, but cleaning it on explicit logout
	// is the principled behavior.)
	http.SetCookie(w, s.expireCookie("oauth_state"))
	// "Sign out & clear cookies" (the login page's control): a pending
	// post-login target goes too (0.29.106 review, W7; the form token went
	// with clearBrowserSession).
	http.SetCookie(w, s.expireCookie("oauth_next"))
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
	c, err := s.readCookie(r, "oauth_next")
	if err != nil || c.Value == "" {
		return "/dashboard"
	}
	if target := safeNextTarget(c.Value, s.cfg.SPAURL); target != "" {
		return target
	}
	s.logger.Warn("ignoring untrusted post-login next URL", "next", platform.TruncateForLog([]byte(c.Value), 120))
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
		http.SetCookie(w, s.cookie("oauth_next", next, 300))
	}
}

// clearNext expires the oauth_next cookie once consumed (the one
// constructor's deletion form; the house rule applies to deletions too,
// v0.27.10).
func (s *Server) clearNext(w http.ResponseWriter) {
	http.SetCookie(w, s.expireCookie("oauth_next"))
}

// oauthProvider is one sign-in flow's constants (0.29.102, stage 2d of
// summary/59, WEB-14): built once in New, read by beginOAuth and
// fetchOAuthUser. The two flows used to repeat the state check, the
// bounded exchange, the request build, Do, ReadAll, the non-200 arm, the
// size note and the decode line for line — every fix (the C9 body read,
// the bound, the size note) had to be made twice.
type oauthProvider struct {
	name    string            // "github" / "gitlab": log attributes, phase labels, size notes
	label   string            // "GitHub" / "GitLab": the words the browser sees
	cfg     *oauth2.Config    // nil when the provider is not configured
	userURL string            // the identity read after the exchange
	headers map[string]string // request headers the forge requires
}

// beginOAuth starts a sign-in: the state nonce in its cookie, the SPA's
// ?next= stashed, the browser sent to the provider's authorize URL.
func (s *Server) beginOAuth(w http.ResponseWriter, r *http.Request, p *oauthProvider) {
	if p.cfg == nil {
		http.Error(w, p.label+" OAuth not configured", http.StatusBadRequest)
		return
	}
	state := generateToken()
	http.SetCookie(w, s.oauthStateCookie(state))
	s.stashNext(w, r)
	http.Redirect(w, r, p.cfg.AuthCodeURL(state), http.StatusFound)
}

// fetchOAuthUser is the shared head of both callbacks: the state check,
// the code exchange, the identity read (built with the bounded ctx the
// caller owns, sent with client.Do), the body read (part of the request:
// a forge that sends the headers and stalls the body hits the bound HERE,
// and a dropped error sent the partial body on as if the forge had sent
// it — PR #218 review C9), the non-200 arm, the size note (a 200 is the
// data answer; an error body is not a size — fix review V3) and the
// decode into out. ok=false means the answer was written. The client and
// the raw body come back for the caller's own reads and logs.
func (s *Server) fetchOAuthUser(w http.ResponseWriter, r *http.Request, ctx context.Context, p *oauthProvider, out any) (client *http.Client, body []byte, ok bool) {
	if p.cfg == nil {
		http.Error(w, p.label+" OAuth not configured", http.StatusBadRequest)
		return nil, nil, false
	}
	stateCookie, err := s.readCookie(r, "oauth_state")
	if err != nil || stateCookie.Value != r.URL.Query().Get("state") {
		http.Error(w, "Invalid OAuth state", http.StatusBadRequest)
		return nil, nil, false
	}
	// Checked: the state has done its one job, so it does not live out its
	// MaxAge in the browser (ASVS review of 0.29.125, N7; logout expired it
	// already, a completed sign-in did not).
	http.SetCookie(w, s.expireCookie("oauth_state"))
	token, err := p.cfg.Exchange(ctx, r.URL.Query().Get("code"))
	if err != nil {
		s.logOAuthFailure(r.Context(), p.name, "exchange", err)
		http.Error(w, "OAuth exchange failed", http.StatusInternalServerError)
		return nil, nil, false
	}
	client = p.cfg.Client(ctx, token)
	userReq, err := http.NewRequestWithContext(ctx, http.MethodGet, p.userURL, nil)
	if err != nil {
		s.logger.Error("building the OAuth user request failed", "provider", p.name, "error", err)
		http.Error(w, "Failed to get user info", http.StatusInternalServerError)
		return nil, nil, false
	}
	for k, v := range p.headers {
		userReq.Header.Set(k, v)
	}
	resp, err := client.Do(userReq)
	if err != nil {
		s.logOAuthFailure(r.Context(), p.name, "user", err)
		http.Error(w, "Failed to get user info", http.StatusInternalServerError)
		return nil, nil, false
	}
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		s.logOAuthFailure(r.Context(), p.name, "user", err)
		http.Error(w, "Failed to get user info", http.StatusInternalServerError)
		return nil, nil, false
	}
	if resp.StatusCode != http.StatusOK {
		s.logger.Error("OAuth /user returned non-200",
			"provider", p.name, "status", resp.StatusCode, "body", platform.TruncateForLog(body, 200))
		http.Error(w, p.label+" user fetch failed", http.StatusBadGateway)
		return nil, nil, false
	}
	platform.NoteResponseSize(s.logger, "oauth-"+p.name+"-user", p.userURL, int64(len(body)))
	if err := json.Unmarshal(body, out); err != nil {
		s.logger.Error("OAuth /user response unmarshal failed", "provider", p.name, "error", err)
		http.Error(w, p.label+" user payload invalid", http.StatusBadGateway)
		return nil, nil, false
	}
	return client, body, true
}

func (s *Server) handleGitHubAuth(w http.ResponseWriter, r *http.Request) {
	s.beginOAuth(w, r, s.github)
}

func (s *Server) handleGitHubCallback(w http.ResponseWriter, r *http.Request) {
	// Bounded (batch 7c review round 3): oauth2 builds on http.DefaultClient,
	// which has no timeout, so a stalled forge held the callback for as long
	// as the browser waited. One bound for the exchange, /user and /user/emails.
	ctx, cancel := context.WithTimeout(r.Context(), oauthCallbackTimeout)
	defer cancel()
	var ghUser struct {
		ID        int64  `json:"id"`
		Login     string `json:"login"`
		Name      string `json:"name"`
		Email     string `json:"email"`
		AvatarURL string `json:"avatar_url"`
	}
	client, body, ok := s.fetchOAuthUser(w, r, ctx, s.github, &ghUser)
	if !ok {
		return
	}
	if strings.TrimSpace(ghUser.Login) == "" {
		s.logger.Error("github /user response missing login field", "body", platform.TruncateForLog(body, 200))
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
			s.logger.Warn("failed to send welcome email", "login", platform.TruncateForLog([]byte(info.Login), 100), "error", err)
		}
	}

	// A sign-in replaces the session the browser already carried (ASVS
	// V7.2.4, 0.29.113): its row and the tokens minted from it end now,
	// not at their own expiry. A failure is logged; the new session is
	// still minted (the old row expires on its own).
	if prior, err := s.readCookie(r, "aveloxis_session"); err == nil && prior.Value != "" {
		if err := s.store.DeleteWebSession(r.Context(), prior.Value); err != nil && !httpserver.RequestEnded(r.Context(), err) {
			httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "sign-in: the browser's earlier session could not be ended; it expires on its own", "user_id", userID, "error", err)
		}
		s.proxyTokens.forget(db.HashToken(prior.Value)) // its held Bearer too; the cookies are replaced just below
	}
	// No privilege is read here (0.29.102, D-1): the gate reads the users
	// row on every request, so the first user's auto-promotion and every
	// later change on the user-management page apply at once.
	sessToken, err := s.createSession(r.Context(), userID)
	if err != nil {
		if !httpserver.RequestEnded(r.Context(), err) {
			s.serverError(w, r, "completeOAuthLogin", fmt.Errorf("create web session for user %d: %w", userID, err))
		}
		return
	}
	http.SetCookie(w, s.sessionCookie(sessToken))
	http.SetCookie(w, s.cookie(csrfCookieName, rand.Text(), int(db.WebSessionLifetime.Seconds()))) // the form token (stage 2f)
	// The one line a successful sign-in writes (ASVS V16.3.1, 0.29.113):
	// the account and the forge, never the address or the token.
	s.logger.Info("sign-in", "user_id", userID, "provider", strings.ToLower(providerLabel), "new_account", wasNewUser)
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
	s.beginOAuth(w, r, s.gitlab)
}

func (s *Server) handleGitLabCallback(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), oauthCallbackTimeout) // bounded, see the GitHub callback
	defer cancel()
	var glUser struct {
		ID        int64  `json:"id"`
		Username  string `json:"username"`
		Name      string `json:"name"`
		Email     string `json:"email"`
		AvatarURL string `json:"avatar_url"`
	}
	_, body, ok := s.fetchOAuthUser(w, r, ctx, s.gitlab, &glUser)
	if !ok {
		return
	}
	if strings.TrimSpace(glUser.Username) == "" {
		s.logger.Error("gitlab /user response missing username field", "body", platform.TruncateForLog(body, 200))
		http.Error(w, "GitLab did not return a username. Try again, or check token scopes.", http.StatusBadGateway)
		return
	}

	s.completeOAuthLogin(w, r, db.OAuthUserInfo{
		Login:      glUser.Username,
		Email:      glUser.Email,
		Name:       glUser.Name,
		AvatarURL:  glUser.AvatarURL,
		GLUserID:   glUser.ID,
		GLHost:     s.cfg.GitLabBaseURL, // the instance that answered (New took the one base); the store normalizes it (GitLabOAuthHost)
		GLUsername: glUser.Username,
		Provider:   "gitlab",
		SignupAddr: s.SignupAddress(r), // v0.29.89: for the sign-up quota
	}, "GitLab")
}
