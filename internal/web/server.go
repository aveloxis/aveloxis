// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package web provides the user-facing web GUI with OAuth authentication,
// group management, and repo/org tracking.
package web

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/mailer"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/safego"
	"github.com/aveloxis/aveloxis/internal/static"
	"golang.org/x/oauth2"
)

// Server is the web GUI server.
type Server struct {
	store   *db.PostgresStore
	cfg     config.WebConfig
	logger  *slog.Logger
	ghOAuth *oauth2.Config
	glOAuth *oauth2.Config
	ghKeys  *platform.KeyPool // for immediate org scanning
	// ghAPIBase is the GitHub REST host those keys belong to, normalised by
	// New through platform.GitHubAPIBaseOrPublic so it is never empty. A
	// REQUIRED parameter of New, next to the pool, so a
	// caller cannot hand over the keys and forget the host they go with —
	// which is how the org scan below sent an Enterprise token to public
	// GitHub (v0.29.57, the last site of worklist item 34's client half).
	ghAPIBase string
	sessionMu sync.RWMutex
	sessions  map[string]*Session // session token -> session
	tmpl      *template.Template
	apiProxy  http.Handler   // reverse proxy for /api/* → cfg.APIInternalURL; nil on parse failure
	mailer    *mailer.Mailer // gmail-backed transactional mailer; safely nil if unconfigured (v0.19.0)
}

// Session tracks a logged-in user.
//
// IsAdmin (v0.19.0) is set at session-create time from the
// admin column on aveloxis_ops.users. Cached on the session so
// requireAdmin doesn't need a DB roundtrip per request. Refreshed
// only on next login — if an admin demotes a user mid-session, the
// session retains its old IsAdmin value until the next login. That's
// acceptable for the admin/non-admin distinction (worst case: user
// keeps admin access for up to one session lifetime); the
// alternative is a DB hit per request, which is what we're trying
// to avoid in v0.18.30.
type Session struct {
	UserID    int
	LoginName string
	AvatarURL string
	Provider  string
	IsAdmin   bool
	ExpiresAt time.Time
}

// New creates a web server. ghKeys is optional — if provided, org repos are
// scanned immediately when added via the GUI.
func New(store *db.PostgresStore, cfg config.WebConfig, ghKeys *platform.KeyPool, ghAPIBase string, logger *slog.Logger) *Server {
	// One spelling of the GitLab base for every consumer below (the OAuth
	// endpoints, the /api/v4/user read): a trailing slash made the token
	// URL "//oauth/token" and every GitLab sign-in failed (found by the
	// final review round 2's host test, 2026-09-28).
	cfg.GitLabBaseURL = strings.TrimRight(strings.TrimSpace(cfg.GitLabBaseURL), "/")
	s := &Server{
		store:     store,
		cfg:       cfg,
		ghKeys:    ghKeys,
		ghAPIBase: platform.GitHubAPIBaseOrPublic(ghAPIBase),
		logger:    logger,
		sessions:  make(map[string]*Session),
	}

	baseURL := strings.TrimSuffix(cfg.BaseURL, "/")
	if baseURL == "" {
		baseURL = "http://localhost" + cfg.Addr
	}

	// GitHub OAuth config. Login identity is PUBLIC GitHub by design — the
	// endpoints below and the /user fetches stay on github.com whatever
	// github.base_url says. Copilot review 5261384568 asked for them to
	// follow the base; declined (v0.29.57): under the forge-instances design
	// (public GitHub plus 1..n Enterprise hosts on one deployment) the base
	// is one instance among several and a deployment always serves
	// github.com, so login is not per instance. Enterprise SSO is a separate
	// decision, recorded, not built.
	if cfg.GitHubClientID != "" {
		s.ghOAuth = &oauth2.Config{
			ClientID:     cfg.GitHubClientID,
			ClientSecret: cfg.GitHubClientSecret,
			Scopes:       []string{"read:user", "user:email"},
			Endpoint: oauth2.Endpoint{
				AuthURL:  "https://github.com/login/oauth/authorize",
				TokenURL: "https://github.com/login/oauth/access_token",
			},
			RedirectURL: baseURL + "/auth/github/callback",
		}
	}

	// GitLab OAuth config.
	if cfg.GitLabClientID != "" {
		glBase := cfg.GitLabBaseURL
		if glBase == "" {
			glBase = "https://gitlab.com"
		}
		s.glOAuth = &oauth2.Config{
			ClientID:     cfg.GitLabClientID,
			ClientSecret: cfg.GitLabClientSecret,
			Scopes:       []string{"read_user"},
			Endpoint: oauth2.Endpoint{
				AuthURL:  glBase + "/oauth/authorize",
				TokenURL: glBase + "/oauth/token",
			},
			RedirectURL: baseURL + "/auth/gitlab/callback",
		}
	}

	// Build the internal API reverse proxy. The web server forwards /api/*
	// to cfg.APIInternalURL so the browser can fetch data using relative
	// URLs (same-origin, no CORS). This works transparently behind an nginx
	// front proxy: nginx → web(:8082) → reverse proxy → api(:8383). If an
	// operator prefers to handle /api/* in nginx directly, adding a
	// `location /api/` block pointing at the api port takes precedence and
	// the web server's built-in proxy becomes unused — no code change needed.
	apiURL := strings.TrimSpace(cfg.APIInternalURL)
	if apiURL == "" {
		apiURL = "http://127.0.0.1:8383"
	}
	if target, err := url.Parse(apiURL); err == nil && target.Host != "" {
		rp := httputil.NewSingleHostReverseProxy(target)
		// No response-header wait of its own (NET-6 review r2 F1): the
		// proxied request carries the web request's context, so the web's
		// http_timeout_seconds bound (httpserver.Bound) governs a slow api;
		// a separate wait of the same length only raced it. A dead api
		// fails fast at the dial (connection refused → 502).
		rp.Transport = &http.Transport{IdleConnTimeout: 60 * time.Second}
		rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			if r.Context().Err() != nil {
				// The web's bound fired (its WARN says so) or the client
				// left: the api is not at fault, and nobody reads a 502.
				return
			}
			// r.URL.Path is attacker-controlled — sanitize before logging.
			logger.Warn("api reverse proxy error", "path", truncateForLog([]byte(r.URL.Path), 200), "error", err)
			http.Error(w, "API backend unavailable", http.StatusBadGateway)
		}
		s.apiProxy = rp
	} else {
		logger.Warn("invalid api_internal_url; /api proxy disabled",
			"api_internal_url", logURL(apiURL), "error", err)
	}

	// Parse embedded templates.
	s.tmpl = template.Must(template.New("").Funcs(template.FuncMap{
		"truncate": func(s string, n int) string {
			if len(s) <= n {
				return s
			}
			return s[:n] + "..."
		},
		"dict": func(values ...any) map[string]any {
			m := make(map[string]any)
			for i := 0; i < len(values)-1; i += 2 {
				m[values[i].(string)] = values[i+1]
			}
			return m
		},
		"add": func(a, b int) int {
			return a + b
		},
		"subtract": func(a, b int) int {
			return a - b
		},
	}).Parse(allTemplates))

	return s
}

// WithMailer attaches a transactional mailer to the server. Returns
// the server for chaining. Optional — without one (or with a disabled
// one) the notification hooks (welcome email, group and add-request
// decisions) send nothing, the account-email form refuses new addresses,
// and users without an address reach the dashboard; see
// confirmationBaseFor.
func (s *Server) WithMailer(m *mailer.Mailer) *Server {
	s.mailer = m
	// Said once at startup, where an operator looks: the refusal it
	// predicts otherwise shows up only when a user submits the form.
	if m.Enabled() && m.SiteURL() == "" && !s.cfg.DevMode && s.logger != nil {
		s.logger.Warn("mail.site_url is not set: account-email confirmation links will be refused and users without an email address go straight to the dashboard — set mail.site_url to this site's public URL (not web.dev_mode, which is for local development)")
	}
	return s
}

// Mailer returns the mailer WithMailer attached, or nil. `aveloxis web`'s
// wiring test reads it (cmd/aveloxis TestProcessMailWiring).
func (s *Server) Mailer() *mailer.Mailer { return s.mailer }

// Handler returns the HTTP handler for the web GUI.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Static assets.
	mux.HandleFunc("GET /icon.png", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.Write(static.IconPNG)
	})

	// Public routes. "/" must NOT be a catch-all — use GET to avoid
	// swallowing static asset routes like /icon.png.
	mux.HandleFunc("GET /{$}", s.handleHome)
	mux.HandleFunc("/login", s.handleLogin)
	mux.HandleFunc("/auth/github", s.handleGitHubAuth)
	mux.HandleFunc("/auth/github/callback", s.handleGitHubCallback)
	mux.HandleFunc("/auth/gitlab", s.handleGitLabAuth)
	mux.HandleFunc("/auth/gitlab/callback", s.handleGitLabCallback)
	mux.HandleFunc("/logout", s.handleLogout)
	// v0.27.4: alias under /auth/ so the SPA can reach logout through
	// the same proxy prefix as the OAuth routes (nginx/dev-server only
	// forward /auth/* and /api/*). Clears the session + oauth cookies;
	// the SPA ignores the redirect and routes itself to login.html.
	mux.HandleFunc("/auth/logout", s.handleLogout)
	// v0.27.1: mints a DB-backed Bearer token for the separate-origin
	// SPA (aveloxis-gui). The SPA completes OAuth on this origin (the
	// session cookie), then exchanges it here for a token it sends as
	// Authorization: Bearer to the api process.
	mux.HandleFunc("/auth/token", s.requireAuth(s.handleAuthToken))

	// Authenticated routes.
	mux.HandleFunc("/dashboard", s.requireAuth(s.handleDashboard))
	mux.HandleFunc("/account/email", s.requireAuth(s.handleAccountEmail))
	mux.HandleFunc("/account/email/confirm", s.requireAuth(s.handleEmailConfirm))
	mux.HandleFunc("/groups/new", s.requireAuth(s.handleNewGroup))
	mux.HandleFunc("/groups/", s.requireAuth(s.handleGroup))
	mux.HandleFunc("/groups/add-repo", s.requireAuth(s.handleAddRepo))
	mux.HandleFunc("/groups/add-org", s.requireAuth(s.handleAddOrg))
	mux.HandleFunc("/groups/remove-repo", s.requireAuth(s.handleRemoveRepo))
	mux.HandleFunc("/compare", s.requireAuth(s.handleCompare))

	// Monitor dashboard — integrated from the standalone monitor server.
	mux.HandleFunc("/monitor", s.requireAuth(s.handleMonitor))
	mux.HandleFunc("POST /monitor/prioritize/{repoID}", s.requireAuth(s.handleMonitorPrioritize))

	// v0.19.0 admin pages. requireAdmin gates on Session.IsAdmin so
	// non-admin users get a 403 instead of seeing other people's
	// pending submissions or being able to toggle admin roles.
	mux.HandleFunc("/admin/groups/pending", s.requireAdmin(s.handleAdminPendingGroups))
	mux.HandleFunc("POST /admin/groups/{id}/approve", s.requireAdmin(s.handleApproveGroup))
	mux.HandleFunc("POST /admin/groups/{id}/reject", s.requireAdmin(s.handleRejectGroup))
	// v0.27.20 per-add approval queue (summary/15): decisions on
	// non-admin additions of not-yet-tracked repos/orgs.
	mux.HandleFunc("POST /admin/add-requests/{id}/approve", s.requireAdmin(s.handleApproveAddRequest))
	mux.HandleFunc("POST /admin/add-requests/{id}/reject", s.requireAdmin(s.handleRejectAddRequest))
	mux.HandleFunc("/admin/users", s.requireAdmin(s.handleAdminUsers))
	mux.HandleFunc("POST /admin/users/{id}/admin", s.requireAdmin(s.handleSetUserAdmin))

	// Same-origin API reverse proxy. Browser fetch calls use relative
	// /api/v1/... URLs; this handler forwards them to cfg.APIInternalURL.
	// Gated by requireAuth so an anonymous visitor can't query collected
	// data through the proxy. The browser sends the aveloxis_session cookie
	// on same-origin requests, which the auth middleware validates.
	if s.apiProxy != nil {
		mux.Handle("/api/", s.requireAuth(s.apiProxy.ServeHTTP))
	}

	return mux
}

// ============================================================
// Session management
// ============================================================

// oauthCallbackTimeout bounds the OAuth callback's forge round trips AS A
// WHOLE (the code exchange, /user and, on GitHub, /user/emails share one
// context): three sequential requests to one forge × the 10 s the scorecard
// rate-limit probe grants one GitHub request (scorecard.go's client). A
// browser waiting on the callback is better served by an error than by an
// unbounded spinner. A var so the runtime test can shorten it.
var oauthCallbackTimeout = 30 * time.Second

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

// truncateForLog returns a string-safe slice of body capped at max
// bytes, suitable for surfacing in an error log without flooding it
// when the upstream returns an HTML error page or other large body.
func truncateForLog(body []byte, max int) string {
	// The inputs here are untrusted (third-party API response bodies,
	// request paths) — strip newlines and other control characters so
	// they cannot forge log lines (CodeQL: go/log-injection), then
	// bound the length.
	s := strings.ReplaceAll(string(body), "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, s)
	if len(s) <= max {
		return s
	}
	return s[:max] + "...(truncated)"
}

// logURL is the one spelling of a caller's URL in a log attribute: userinfo
// redacted FIRST, then truncated (batch 5b review round 1: the other order
// let a userinfo longer than the truncation window survive into the WARN
// unredacted — 187 bytes of a credential). Every URL-keyed attribute in
// this package goes through it.
func logURL(u string) string {
	return truncateForLog([]byte(platform.RedactURLUserinfo(u)), 200)
}

// ============================================================
// Auth handlers
// ============================================================

func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	// Pattern "GET /{$}" ensures this only matches exactly "/".
	sess := s.getSession(r)
	if sess != nil {
		http.Redirect(w, r, "/dashboard", http.StatusFound)
		return
	}
	http.Redirect(w, r, "/login", http.StatusFound)
}

// render executes a template and logs failures. v0.27.36 (summary/18
// Phase 0a): the bare ExecuteTemplate calls discarded render errors, so
// a template failure mid-render produced a truncated page with no log
// signal. Headers are already sent by the time a body-write fails, so
// logging is the only possible action — but it must happen.
func (s *Server) render(w http.ResponseWriter, name string, data any) {
	if err := s.tmpl.ExecuteTemplate(w, name, data); err != nil {
		// No request here: a render refused after the bound is http.ErrHandlerTimeout, which RequestEnded recognises on any context.
		httpserver.LogFailure(context.Background(), s.logger, slog.LevelError, err, "template render failed", "template", name, "error", err)
	}
}

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
	}, "GitLab")
}

// ============================================================
// Dashboard & group management
// ============================================================

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)

	// v0.19.10 email gate, v0.20.4 pending-aware, v0.29.29
	// confirmable-only, v0.29.30 lookup errors logged: see
	// dashboardEmailGate.
	needsEmailForm, pendingEmail := dashboardEmailGate(r.Context(), s.store, s.confirmationPolicy(), s.logger, r, sess.UserID)
	if needsEmailForm {
		http.Redirect(w, r, "/account/email", http.StatusFound)
		return
	}

	groups, err := s.store.GetUserGroups(r.Context(), sess.UserID)
	if err != nil {
		// A failed lookup is not "no groups" (SR-5; batch 5b review round
		// 1): it read as an empty dashboard, indistinguishable from a fresh
		// login, and the pending-approval banner keyed off it.
		s.serverError(w, r, "handleDashboard", err)
		return
	}

	// v0.19.10 pending-approval banner: non-admin users whose groups
	// are all status='pending' get a clear signal their account is
	// awaiting administrator approval. Without this they see an empty
	// dashboard indistinguishable from a fresh admin login.
	pendingOnly := !sess.IsAdmin && len(groups) > 0
	if pendingOnly {
		for _, g := range groups {
			if g.Status != "pending" {
				pendingOnly = false
				break
			}
		}
	}

	s.render(w, "dashboard", map[string]any{
		"Session":      sess,
		"Groups":       groups,
		"PendingOnly":  pendingOnly,
		"PendingEmail": pendingEmail,
		// How long links are valid. The banner shows on every visit while
		// the address is pending, so it states the validity window, not
		// time left on this link.
		"ConfirmationLifetime": mailer.DurationPhrase(db.EmailConfirmationLifetime),
	})
}

// emailConfirmBase returns the base URL for the click-to-confirm link
// submitAccountEmail mails: the operator-configured site URL (which may carry
// a path; mailer.New has already trimmed it). The
// request Host header is attacker-controlled — a proxy that forwards Host
// (the usual nginx `proxy_set_header Host $host`) passes whatever the client
// sent — so deriving the link from it let an authenticated attacker submit a
// VICTIM's address with a crafted Host, have the victim receive a link to
// the attacker's server carrying the confirmation token, and replay that
// token to bind the victim's email to their own account (Copilot review on
// PR #207; CodeQL go/email-injection alert 197 traces the same request data
// into the message).
//
// So without a site URL the Host is used ONLY with web.dev_mode on AND when
// loopbackAuthority accepts it — the local-dev case that fallback existed
// for. dev_mode is required because a reverse proxy on the same host that
// does not forward Host (nginx's default for proxy_pass to 127.0.0.1) makes
// every visitor's request look loopback; the link would point at their own
// machine (round-4 review, operator decision, v0.29.30). Anywhere else this
// returns false and no link is mailed. It does not need the token, so the
// decision is made before anything is stored.
func emailConfirmBase(siteURL string, r *http.Request, devMode bool) (string, bool) {
	if siteURL != "" { // normalized once, by mailer.New
		return siteURL, true
	}
	if !devMode {
		return "", false
	}
	authority, ok := loopbackAuthority(r.Host)
	if !ok {
		return "", false
	}
	scheme := "https"
	if r.TLS == nil {
		scheme = "http"
	}
	return scheme + "://" + authority, true
}

// confirmationLink is the mailed link for a base from emailConfirmBase:
// the separate-repo front end's profile page when web.spa_url is set
// (it confirms through the API), this process's /account/email/confirm
// otherwise (2026-10-04). The token is hex, so it needs no escaping. On
// the front end it rides in the FRAGMENT: a fragment is never sent to the
// static server (so never in its access log), the front end's analytics
// tag excludes it, and the page moves it into the tab's session storage
// and drops it from the URL while its script loads, before any request
// is issued, so a signed-out click's login round trip carries a
// token-free ?next= (reviews 2026-10-04). spa_url is taken as written: config refuses a
// non-canonical value at load.
func confirmationLink(base, spaURL, token string) string {
	if spaURL != "" {
		return spaURL + "/profile.html#token=" + token
	}
	return base + "/account/email/confirm?token=" + token
}

// Why a confirmation link cannot be sent from this request.
var (
	errConfirmationMailDisabled = errors.New("mail is not configured, so no confirmation link can be sent")
	errConfirmationNoLinkBase   = errors.New("mail.site_url is not set — set it to this site's public URL to send confirmation links (web.dev_mode lets a loopback request build one for local development only; keep dev_mode off in production)")
)

// confirmationPolicy is everything that decides whether a confirmation
// link can be sent: the mailer and web.dev_mode. Both call sites — the
// account-email POST and the dashboard's email gate — take it from
// Server.confirmationPolicy, so they cannot disagree (round-5 review: the
// dashboard passed dev_mode separately, and a wrong value there brought
// back the v0.29.29 lockout with every test green).
type confirmationPolicy struct {
	mailer  confirmationMailer
	devMode bool
	spaURL  string // web.spa_url: the link lands on the front end's profile page
}

func (s *Server) confirmationPolicy() confirmationPolicy {
	return confirmationPolicy{mailer: s.mailer, devMode: s.cfg.DevMode, spaURL: s.cfg.SPAURL}
}

// ConfirmationPolicy is confirmationPolicy for the API process, which
// never builds a link from a request Host (devMode false): a configured
// mail.site_url, or no link at all.
type ConfirmationPolicy = confirmationPolicy

// NewConfirmationPolicy is the API's policy: its mailer and web.spa_url.
func NewConfirmationPolicy(m *mailer.Mailer, spaURL string) ConfirmationPolicy {
	return confirmationPolicy{mailer: m, spaURL: spaURL}
}

// AccountEmailStore is what SubmitAccountEmail needs from the store.
type AccountEmailStore = accountEmailStore

// confirmationBaseFor decides whether this request's user can be mailed a
// working confirmation link, and returns the link base when so.
// submitAccountEmail refuses on an error; the dashboard's email gate uses
// the same decision so it never sends a user to a form that would refuse
// them.
func confirmationBaseFor(p confirmationPolicy, r *http.Request) (string, error) {
	if !p.mailer.Enabled() {
		return "", errConfirmationMailDisabled
	}
	base, ok := emailConfirmBase(p.mailer.SiteURL(), r, p.devMode)
	if !ok {
		return "", errConfirmationNoLinkBase
	}
	return base, nil
}

// emailGateRedirect reports whether the dashboard must send the user to
// /account/email: they have neither a confirmed nor a pending address
// (v0.19.10; with a pending one the dashboard shows its "check your inbox"
// banner instead), AND a confirmation could actually be sent from here.
// Where it could not — mail off, or no trustworthy link base — the form
// refuses, so redirecting locked the user out of the dashboard; the
// operator chose to let them in without an address (v0.29.29).
func emailGateRedirect(confirmed, pending string, p confirmationPolicy, r *http.Request) bool {
	if strings.TrimSpace(confirmed) != "" || strings.TrimSpace(pending) != "" {
		return false
	}
	_, err := confirmationBaseFor(p, r)
	return err == nil
}

// accountEmailLookup is the store surface the dashboard's email gate reads.
type accountEmailLookup interface {
	GetUserEmail(ctx context.Context, userID int) (string, error)
	GetUserLivePendingEmail(ctx context.Context, userID int) (string, error)
}

// dashboardEmailGate reads the user's addresses, decides through
// emailGateRedirect whether the dashboard sends them to /account/email, and
// returns the pending address for the dashboard's banner. A lookup ERROR is
// not "no address" (SR-5): it is logged and the dashboard renders, rather
// than sending a user who has a confirmed address to the email form during
// a database blip.
func dashboardEmailGate(ctx context.Context, st accountEmailLookup, p confirmationPolicy, logger *slog.Logger, r *http.Request, userID int) (needsForm bool, pending string) {
	confirmed, err := st.GetUserEmail(ctx, userID)
	if err != nil {
		httpserver.LogFailure(ctx, logger, slog.LevelWarn, err, "dashboard: could not read the user's email; rendering without the email-form redirect",
			"user_id", userID, "error", err)
		return false, ""
	}
	pending, err = st.GetUserLivePendingEmail(ctx, userID)
	if err != nil {
		httpserver.LogFailure(ctx, logger, slog.LevelWarn, err, "dashboard: could not read the user's pending email; rendering without the email-form redirect",
			"user_id", userID, "error", err)
		return false, ""
	}
	return emailGateRedirect(confirmed, pending, p, r), pending
}

// accountEmailStore and confirmationMailer are the narrow surfaces
// submitAccountEmail needs, so it runs against fakes in tests.
type accountEmailStore interface {
	SetUserPendingEmail(ctx context.Context, userID int, email string) error
	ClearUserPendingEmailIf(ctx context.Context, userID int, email string) error
	CreateEmailConfirmation(ctx context.Context, userID int, email string) (string, error)
}

type confirmationMailer interface {
	SiteURL() string
	Enabled() bool
	SendEmailConfirmation(toEmail, login, confirmURL string, lifetime time.Duration) error
}

var (
	_ accountEmailStore  = (*db.PostgresStore)(nil)
	_ accountEmailLookup = (*db.PostgresStore)(nil)
	_ confirmationMailer = (*mailer.Mailer)(nil)
)

// submitAccountEmail processes a POSTed /account/email: the v0.20.4 flow that
// records the address as email_pending and mails a click-to-confirm link, so
// the address becomes users.email only when the link is followed. It
// returns "" when the address was recorded and the link handed to SMTP (the
// handler redirects), or the message the form should show.
//
// Nothing is stored until the submission is known to be mailable: the
// address passes the mailer's own recipient rule (so what is stored is the
// bare addr-spec Send will accept) and confirmationBaseFor allows it.
// Storing first left an email_pending behind on a refusal, and the
// dashboard then told the user a link had been sent. For the same reason a
// send that fails after storing clears that pending address again.
func submitAccountEmail(ctx context.Context, st accountEmailStore, p confirmationPolicy, logger *slog.Logger, r *http.Request, sess *Session) string {
	return submitAccountEmailAddress(ctx, st, p, logger, r, sess.UserID, sess.LoginName, r.FormValue("email"))
}

// SubmitAccountEmail is the API's entry to the same submission (its JSON
// body carries the address; the request supplies the Host only for a
// dev-mode web process, never here). The message is the user-facing
// reason it was refused, or "" when the confirmation was mailed.
func SubmitAccountEmail(ctx context.Context, st AccountEmailStore, p ConfirmationPolicy, logger *slog.Logger, r *http.Request, userID int, login, rawEmail string) string {
	return submitAccountEmailAddress(ctx, st, p, logger, r, userID, login, rawEmail)
}

// submitAccountEmailAddress is the one place an account email is stored
// and its confirmation mailed, for the web form and the API alike.
func submitAccountEmailAddress(ctx context.Context, st accountEmailStore, p confirmationPolicy, logger *slog.Logger, r *http.Request, userID int, login, rawEmail string) string {
	email, err := mailer.ParseRecipient(rawEmail)
	if err != nil {
		logger.Info("account email rejected: not a deliverable address",
			"user_id", userID, "error", truncateForLog([]byte(err.Error()), 200))
		return "Please enter a valid email address."
	}
	base, err := confirmationBaseFor(p, r)
	if err != nil {
		logger.Error("refusing an account email: "+err.Error(),
			"user_id", userID, "host", truncateForLog([]byte(r.Host), 200))
		return "Email confirmation is not configured on this site. Contact the operator."
	}
	if err := st.SetUserPendingEmail(ctx, userID, email); err != nil {
		httpserver.LogFailure(ctx, logger, slog.LevelWarn, err, "failed to set pending email", "user_id", userID, "error", err)
		return "Could not save email. Try again."
	}
	token, err := st.CreateEmailConfirmation(ctx, userID, email)
	if err != nil {
		httpserver.LogFailure(ctx, logger, slog.LevelWarn, err, "failed to create email confirmation", "user_id", userID, "error", err)
		return "Could not generate confirmation. Try again."
	}
	if err := p.mailer.SendEmailConfirmation(email, login, confirmationLink(base, p.spaURL, token), db.EmailConfirmationLifetime); err != nil {
		if !mailer.IsSkip(err) { // a skip was already logged by the mailer
			// The send takes no context: its failure is never the
			// request's end (NET-6 review r6 F2), so no LogFailure.
			logger.Warn("failed to send confirmation email",
				"user_id", userID, "email", platform.RedactEmail(email), "error", err)
		}
		// Don't leave the dashboard announcing "we sent a confirmation
		// link" (operator decision, v0.29.29) — but the mail may still have
		// gone out: net/smtp reports QUIT's error after the server has
		// accepted the message. So clear the pending address only while it
		// is still this submission's (a second tab may have replaced it,
		// v0.29.30), and tell the user a link that does arrive still works:
		// its token is in the database until it expires.
		//
		// The clean-up is the second half of writes that already committed,
		// so it runs detached from the request (NET-6 review r7 F1: an SMTP
		// dial can outlast nginx and the bound, and on the cancelled request
		// context the clean-up failed at once, leaving the dashboard
		// announcing a link that was never sent) and any failure is a WARN.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), pendingEmailCleanupTimeout)
		defer cleanupCancel()
		if clearErr := st.ClearUserPendingEmailIf(cleanupCtx, userID, email); clearErr != nil {
			logger.Warn("failed to clear the pending email after a failed confirmation send",
				"user_id", userID, "error", clearErr)
		}
		return "We couldn't send the confirmation email. If one does arrive, its link still works; otherwise try again later."
	}
	return ""
}

// pendingEmailCleanupTimeout bounds submitAccountEmail's detached clean-up:
// one conditional UPDATE by the user's key, which takes milliseconds unless
// the pool or the database is stalled — the case the bound exists for. The
// house precedent for such a statement is 30 s (the deploy gate's
// deployGateDialTimeout, cmd/aveloxis).
const pendingEmailCleanupTimeout = 30 * time.Second

// loopbackAuthority parses an HTTP Host header ONCE and, when it names the
// loopback interface, returns it as a URL authority for an emailed link. A
// client-set Host may be trusted for that only when it is loopback: a
// loopback Host cannot reach anyone else's machine.
//
// The same parse feeds the decision and the link, and it is strict because
// its output is mailed:
//   - a port, when present, must be a canonical decimal in 1-65535 — an
//     earlier check ignored the port, so `localhost:<any text>` passed and
//     the text rode into the link;
//   - brackets must be balanced and may hold only an IPv6 literal;
//   - every IPv6 literal, IPv4-mapped included, comes back bracketed, as a
//     URL authority requires (RFC 3986 section 3.2.2). A bare "::1" is not
//     something a browser sends, but net/http accepts it.
func loopbackAuthority(hostHeader string) (string, bool) {
	host, port := hostHeader, ""
	bracketed := strings.HasPrefix(hostHeader, "[")
	if h, p, err := net.SplitHostPort(hostHeader); err == nil {
		if !isCanonicalPort(p) {
			return "", false
		}
		host, port = h, p
	} else if bracketed {
		// "[::1]" — SplitHostPort wants a port, so strip the brackets here.
		if !strings.HasSuffix(hostHeader, "]") {
			return "", false
		}
		host = hostHeader[1 : len(hostHeader)-1]
	}
	ip := net.ParseIP(host)
	ipv6Literal := ip != nil && strings.Contains(host, ":")
	switch {
	case bracketed && !ipv6Literal:
		return "", false
	case ip != nil && !ip.IsLoopback():
		return "", false
	case ip == nil && !strings.EqualFold(host, "localhost"):
		return "", false
	}
	if port != "" {
		return net.JoinHostPort(host, port), true
	}
	if ipv6Literal {
		return "[" + host + "]", true
	}
	return host, true
}

// isCanonicalPort reports whether p is a port in 1-65535 written as a
// canonical decimal: digits only, no sign, no leading zero.
func isCanonicalPort(p string) bool {
	n, err := strconv.ParseUint(p, 10, 16)
	return err == nil && n > 0 && strconv.FormatUint(n, 10) == p
}

// handleAccountEmail renders (GET) and processes (POST) the
// email-collection form. This is the v0.19.10 fallback when both /user
// and /user/emails came back empty during OAuth callback. A submitted
// address is stored as email_pending, a click-to-confirm link is mailed
// (v0.20.4), and the user redirects to /dashboard; users.email changes
// only when the link is followed.
func (s *Server) handleAccountEmail(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)

	if r.Method == http.MethodPost {
		if msg := submitAccountEmail(r.Context(), s.store, s.confirmationPolicy(), s.logger, r, sess); msg != "" {
			s.render(w, "account_email", map[string]any{
				"Session": sess,
				"Error":   msg,
			})
			return
		}
		http.Redirect(w, r, "/dashboard", http.StatusFound)
		return
	}

	// GET: show the form. If the user already has a confirmed email,
	// they don't need to be here — bounce them back to dashboard.
	email, err := s.store.GetUserEmail(r.Context(), sess.UserID)
	if err != nil {
		// Showing the form is the safe fallback; the error is still logged.
		httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "account email form: could not read the user's email", "user_id", sess.UserID, "error", err)
	}
	if strings.TrimSpace(email) != "" {
		http.Redirect(w, r, "/dashboard", http.StatusFound)
		return
	}
	// The confirm handler sends users here with a reason; say it.
	var notice string
	switch {
	case r.URL.Query().Get("expired") == "1":
		notice = "That confirmation link has expired, was already used, or belongs to another account. Enter your email to get a new one."
	case r.URL.Query().Get("error") == "1":
		notice = "We couldn't confirm your email just now. Try the link again, or enter your email to get a new one."
	}
	s.render(w, "account_email", map[string]any{
		"Session": sess,
		"Error":   notice,
	})
}

// handleEmailConfirm confirms the v0.20.4 click-to-confirm link for the
// logged-in user and redirects to the dashboard. A link that is not a live
// token for THIS account — unknown, expired, used, or another account's —
// redirects to /account/email?expired=1 and changes nothing (the owner's
// link stays usable; another account's still-live link is logged at WARN); a database
// failure redirects to ?error=1 with the transaction rolled back, so the
// link still works. The form explains both.
func (s *Server) handleEmailConfirm(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)
	switch ConfirmAccountEmail(r.Context(), s.store, s.logger, strings.TrimSpace(r.URL.Query().Get("token")), sess.UserID) {
	case ConfirmConfirmed:
		http.Redirect(w, r, "/dashboard", http.StatusFound)
	case ConfirmError:
		http.Redirect(w, r, "/account/email?error=1", http.StatusFound)
	default:
		http.Redirect(w, r, "/account/email?expired=1", http.StatusFound)
	}
}

// ConfirmOutcome is what following a confirmation link came to.
type ConfirmOutcome int

const (
	// ConfirmConfirmed: the address is this account's confirmed email now.
	ConfirmConfirmed ConfirmOutcome = iota
	// ConfirmInvalid: not a live token for THIS account — unknown, expired,
	// used, empty, or another account's (logged at WARN); nothing changed,
	// and the owner's link stays usable.
	ConfirmInvalid
	// ConfirmError: the database failed; the transaction rolled back, so
	// the link still works.
	ConfirmError
)

// ConfirmEmailStore is what ConfirmAccountEmail needs from the store.
type ConfirmEmailStore interface {
	ConfirmEmailToken(ctx context.Context, token string, userID int) (string, error)
}

// ConfirmAccountEmail confirms the v0.20.4 click-to-confirm link for the
// signed-in user, for the web page and the API alike: one classification
// of the token's errors, so the two surfaces never disagree.
func ConfirmAccountEmail(ctx context.Context, st ConfirmEmailStore, logger *slog.Logger, token string, userID int) ConfirmOutcome {
	if token == "" {
		return ConfirmInvalid
	}
	if _, err := st.ConfirmEmailToken(ctx, token, userID); err != nil {
		var mismatch *db.TokenOwnerMismatchError
		if errors.As(err, &mismatch) {
			// The replay emailConfirmBase exists to prevent: say so.
			logger.Warn("email confirmation link belongs to another account",
				"token_user_id", mismatch.OwnerID, "session_user_id", userID)
			return ConfirmInvalid
		}
		if errors.Is(err, db.ErrConfirmationTokenInvalid) {
			logger.Info("email confirmation link rejected: unknown, expired, or already used",
				"session_user_id", userID)
			return ConfirmInvalid
		}
		httpserver.LogFailure(ctx, logger, slog.LevelWarn, err, "failed to confirm user email", "user_id", userID, "error", err)
		return ConfirmError
	}
	return ConfirmConfirmed
}

func (s *Server) handleNewGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sess := s.getSession(r)
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		http.Redirect(w, r, "/dashboard", http.StatusFound)
		return
	}

	if _, err := s.store.CreateUserGroup(r.Context(), sess.UserID, name); err != nil {
		httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "failed to create group", "error", err)
	}
	http.Redirect(w, r, "/dashboard", http.StatusFound)
}

func (s *Server) handleGroup(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)
	// Extract group_id from URL: /groups/123 or /groups/123/repos/456/sbom
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/groups/"), "/")
	groupID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	// Handle SBOM download: /groups/{gid}/repos/{rid}/sbom?format=cyclonedx|spdx
	if len(parts) >= 4 && parts[1] == "repos" && parts[3] == "sbom" {
		s.handleSBOMDownload(w, r, sess, groupID, parts[2])
		return
	}

	// Handle repo detail/visualization page: /groups/{gid}/repos/{rid}
	if len(parts) >= 3 && parts[1] == "repos" {
		s.handleRepoDetail(w, r, sess, groupID, parts[2])
		return
	}

	// Pagination and search parameters.
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	perPage := 25
	query := strings.TrimSpace(r.URL.Query().Get("q"))

	group, totalRepos, err := s.store.GetGroupDetail(r.Context(), sess.UserID, groupID, page, perPage, query)
	if errors.Is(err, db.ErrGroupNotOwned) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		s.serverError(w, r, "handleGroup", err)
		return
	}

	// Enrich repos with gathered vs metadata stats.
	if len(group.Repos) > 0 {
		repoIDs := make([]int64, len(group.Repos))
		for i, r := range group.Repos {
			repoIDs[i] = r.RepoID
		}
		if stats, err := s.store.GetRepoStatsBatch(r.Context(), repoIDs); err == nil {
			for i := range group.Repos {
				if st, ok := stats[group.Repos[i].RepoID]; ok {
					group.Repos[i].GatheredIssues = st.GatheredIssues
					group.Repos[i].GatheredPRs = st.GatheredPRs
					group.Repos[i].GatheredCommits = st.GatheredCommits
					group.Repos[i].MetaIssues = st.MetadataIssues
					group.Repos[i].MetaPRs = st.MetadataPRs
					group.Repos[i].MetaCommits = st.MetadataCommits
				}
			}
		}
	}

	totalPages := (totalRepos + perPage - 1) / perPage
	if totalPages < 1 {
		totalPages = 1
	}

	// Build a sliding window of up to 5 page numbers centered on the current page.
	windowSize := 5
	winStart := page - windowSize/2
	if winStart < 1 {
		winStart = 1
	}
	winEnd := winStart + windowSize - 1
	if winEnd > totalPages {
		winEnd = totalPages
		winStart = winEnd - windowSize + 1
		if winStart < 1 {
			winStart = 1
		}
	}
	var pageWindow []int
	for i := winStart; i <= winEnd; i++ {
		pageWindow = append(pageWindow, i)
	}

	s.render(w, "group", map[string]any{
		"Session":    sess,
		"Group":      group,
		"Page":       page,
		"TotalPages": totalPages,
		"TotalRepos": totalRepos,
		"Query":      query,
		"PageWindow": pageWindow,
		"AddError":   r.URL.Query().Get("add_error"),
		"OrgError":   r.URL.Query().Get("org_error"),
		// A number the handler parsed, never the query string itself: the
		// notice is styled as the site's own, so a reflected string would
		// read as the site speaking (review round 1 of the batch). Not a
		// number, zero or negative: no notice.
		"Pending":    pendingCount(r.URL.Query().Get("pending")),
		"OrgPending": r.URL.Query().Get("org_pending"),
		"GitHubHost": platform.GitHubWebHost(s.ghAPIBase),
	})
}

// pendingCount reads the ?pending= count the add redirect carries; anything
// that is not a positive integer is 0 (no notice).
func pendingCount(q string) int {
	n, err := strconv.Atoi(q)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func (s *Server) handleAddRepo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sess := s.getSession(r)
	groupID, _ := strconv.ParseInt(r.FormValue("group_id"), 10, 64)

	// Support both single URL (repo_url) and bulk paste (repo_urls, line-delimited).
	raw := r.FormValue("repo_urls")
	if raw == "" {
		raw = r.FormValue("repo_url") // backward compat with old single-URL form
	}

	var invalid []string
	if raw != "" && groupID > 0 {
		var urls []string
		for _, line := range strings.Split(raw, "\n") {
			repoURL := strings.TrimSpace(line)
			if repoURL == "" {
				continue
			}
			// Validate the URL before adding.
			v := ValidateRepoURL(repoURL)
			if !v.Valid {
				// Logged below; a refused credential must not be written to
				// the log by the line that refuses it (fix-review round 1).
				invalid = append(invalid, fmt.Sprintf("%s: %s", platform.RedactURLUserinfo(repoURL), v.Error))
				continue
			}
			urls = append(urls, v.URL)
		}

		// v0.27.20 per-add approval: the WHOLE paste is one call so any
		// not-yet-tracked URLs from a non-admin become ONE pending
		// add-request (one approval unit), while tracked repos link
		// instantly. Admins keep the direct create+enqueue path.
		if len(urls) > 0 {
			out, err := s.store.AddReposToGroup(r.Context(), sess.UserID, groupID, urls,
				s.cfg.AutoApproveAddLimitValue())
			if err != nil {
				httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "failed to add repos to group", "group_id", groupID, "error", err)
				if errors.Is(err, db.ErrURLTooLong) {
					// Name the line (follow-up 14): the store refuses the
					// whole paste and its error names none of them.
					for _, u := range urls {
						if u = strings.TrimSpace(u); len(u) > db.MaxAddURLBytes {
							s.logger.Warn("repo not added — a URL is over-long", "group_id", groupID, "bytes", len(u), "url_prefix", logURL(u))
							break
						}
					}
				}
				// Tell the user, who otherwise sees the page a success shows
				// (Copilot review of PR #207); a rejected group gets its own
				// notice, since trying again cannot work (round-24 review).
				http.Redirect(w, r, fmt.Sprintf("/groups/%d?add_error=%s", groupID, addErrorFlag(err)), http.StatusFound)
				return
			}
			s.logger.Info("repo add", "group_id", groupID,
				"linked", out.Linked, "enqueued", out.Enqueued, "pending_approval", out.Pending)
			if out.Pending > 0 {
				s.notifyAddRequestSubmitted(out.RequestID)
				http.Redirect(w, r, fmt.Sprintf("/groups/%d?pending=%d%s", groupID, out.Pending, invalidFlag(invalid)), http.StatusFound)
				return
			}
		}
		if len(invalid) > 0 {
			// The page says so too (round 2: the lines the validator refused
			// were only logged, so a refused paste looked like a success).
			s.logger.Warn("some URLs were invalid", "errors", invalid)
		}
	}
	loc := fmt.Sprintf("/groups/%d", groupID)
	if len(invalid) > 0 {
		loc += "?add_error=invalid"
	}
	http.Redirect(w, r, loc, http.StatusFound)
}

// invalidFlag is the add_error query fragment for a paste with refused
// lines, empty when every line was accepted.
func invalidFlag(invalid []string) string {
	if len(invalid) == 0 {
		return ""
	}
	return "&add_error=invalid"
}

func (s *Server) handleAddOrg(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sess := s.getSession(r)
	groupID, _ := strconv.ParseInt(r.FormValue("group_id"), 10, 64)
	orgURL := strings.TrimSpace(r.FormValue("org_url"))

	if orgURL != "" && groupID > 0 {
		// The host travels with the registration: the store refuses a
		// GitHub org that is not on this deployment's GitHub host
		// (db.ErrOrgOffGitHubHost) in every writer, and the page says so.
		out, err := s.store.AddOrgToGroup(r.Context(), sess.UserID, groupID, orgURL, s.ghAPIBase)
		switch {
		case errors.Is(err, db.ErrOrgOffGitHubHost):
			s.logger.Warn("org not added — its host is not this deployment's GitHub host",
				"group_id", groupID, "org_url", logURL(orgURL), "github_host", platform.GitHubWebHost(s.ghAPIBase))
			http.Redirect(w, r, fmt.Sprintf("/groups/%d?org_error=host", groupID), http.StatusFound)
			return
		case errors.Is(err, platform.ErrURLUserinfo), errors.Is(err, db.ErrURLTooLong), db.IsRejectedValue(err):
			// A value the database refused (SQLSTATE class 22, e.g. a NUL
			// byte) is the user's input too, as addErrorFlag reads it on the
			// repo path (PR #218 review C4).
			// The user's input, fixable by the user: say so on the page, as
			// the repo path (add_error=invalid) and the portal (400) do
			// (fix-review round 1: the store's refusal fell through to a plain
			// redirect, so a credentialed org URL was silently not added). The
			// URL is logged redacted and truncated, as the host arm logs it
			// (follow-up 14: the over-long WARN named nothing).
			s.logger.Warn("org not added — invalid URL", "group_id", groupID, "error", err,
				"org_url", logURL(orgURL), "bytes", len(orgURL))
			http.Redirect(w, r, fmt.Sprintf("/groups/%d?org_error=invalid", groupID), http.StatusFound)
			return
		case errors.Is(err, db.ErrGroupRejected):
			// Trying again cannot work: say why (the repo paste and the
			// portal already do; review round 1 of the batch).
			s.logger.Warn("org not added — the group is rejected", "group_id", groupID)
			http.Redirect(w, r, fmt.Sprintf("/groups/%d?org_error=rejected", groupID), http.StatusFound)
			return
		case err != nil:
			// Say so (worklist follow-up 11): this redirected as a success.
			httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "failed to add org to group", "group_id", groupID, "error", err)
			http.Redirect(w, r, fmt.Sprintf("/groups/%d?org_error=1", groupID), http.StatusFound)
			return
		case out.Registered:
			// Registered (an admin's add, or a non-admin's auto-approved add of
			// an org already registered in a group that is not rejected —
			// including this group, when the add inserted nothing): scan
			// immediately.
			// Use a detached context — the HTTP request context gets canceled on redirect.
			safego.Go(s.logger, "org-repo-scan", func() { s.scanOrgRepos(context.Background(), groupID, orgURL) })
		default:
			// v0.27.20: a non-admin's add of a NEW org pends on an
			// add-request — nothing scans until an admin approves.
			s.notifyAddRequestSubmitted(out.RequestID)
			http.Redirect(w, r, fmt.Sprintf("/groups/%d?org_pending=1", groupID), http.StatusFound)
			return
		}
	}
	http.Redirect(w, r, fmt.Sprintf("/groups/%d", groupID), http.StatusFound)
}

// scanOrgRepos fetches all repos from a GitHub org or user and adds them to the group + queue.
// Handles both orgs (/orgs/{name}/repos) and users (/users/{name}/repos).
//
// v0.27.20 gate: a 'rejected' group's orgs never scan — RejectGroup is
// the group-level abuse lever and must stop org-driven enqueue too.
// (Registration itself is already approval-gated in AddOrgToGroup;
// this is the belt for the lever. The pre-v0.27.20 claim that org
// scans checked group status was FALSE — audited 2026-07-16.)
func (s *Server) scanOrgRepos(ctx context.Context, groupID int64, orgURL string) {
	orgURL = strings.TrimSuffix(strings.TrimSpace(orgURL), "/")
	host, name, err := platform.ParseOrgURL(orgURL)
	if err != nil {
		return
	}
	// The deployment's GitHub host, not the literal "github.com": on an
	// Enterprise deployment the org lives on that host, and an org on
	// public GitHub is not one this deployment's keys can enumerate
	// (v0.29.57, Copilot review 5260961848 — the routed client below was
	// unreachable for the only orgs it could serve).
	isGitHub := platform.IsGitHubHost(host, s.ghAPIBase)

	if !isGitHub {
		return
	}

	// The rejected-group gate first: it is the abuse lever, and its log line
	// is the record of why an org did not scan, whatever the key pool holds.
	status, err := s.store.GetGroupStatus(ctx, groupID)
	if err != nil {
		// A lookup error is not "not rejected" (SR-5; worklist follow-up 2):
		// nothing scans until the status is known.
		httpserver.LogFailure(ctx, s.logger, slog.LevelError, err, "org scan skipped — group status lookup failed", "group_id", groupID, "org_url", logURL(orgURL), "error", err)
		return
	}
	if status == "rejected" {
		s.logger.Warn("org scan skipped — owning group is rejected", "group_id", groupID, "org_url", logURL(orgURL))
		return
	}
	if !s.ghKeys.HasUsableKey() {
		// web has its own log; the scheduler's startup WARN never reaches it
		// (items 40/21, review round 1).
		s.logger.Warn("org scan skipped — this web process has no usable GitHub API key", "group_id", groupID, "org_url", logURL(orgURL))
		return
	}

	httpClient := platform.NewHTTPClient(s.ghAPIBase, s.ghKeys, s.logger, platform.AuthGitHub)
	s.logger.Info("scanning repos for user group", "name", name, "group_id", groupID)

	added := 0
	newlyQueued := 0
	alreadyExisted := 0

	// Try /orgs/ first. If that 404s, fall back to /users/ (personal accounts).
	basePaths := []string{
		fmt.Sprintf("/orgs/%s/repos", name),
		fmt.Sprintf("/users/%s/repos", name),
	}

	for _, basePath := range basePaths {
		page := 1
		foundRepos := false

		for {
			path := fmt.Sprintf("%s?per_page=100&type=all&page=%d", basePath, page)
			resp, err := httpClient.Get(platform.WithoutETag(ctx), path)
			if err != nil {
				// 404/403 means this isn't an org (or isn't visible) —
				// try the /users/ path. v0.27.36: sentinel-classified via
				// the phase-0 taxonomy instead of error-string matching
				// (summary/18 Phase 0c — the Fix C/Fix J wiring-gap class).
				if platform.ClassifyError(err) == platform.ClassSkip {
					break
				}
				httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "scan API error", "name", name, "error", err)
				break
			}
			var items []struct {
				ID        int64     `json:"id"` // v0.27.102 — rename-proof numeric identity
				CreatedAt time.Time `json:"created_at"`
				HTMLURL   string    `json:"html_url"`
				Name      string    `json:"name"`
				Owner     struct {
					Login string `json:"login"`
				} `json:"owner"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
				resp.Body.Close()
				// A decode failure must not read as "no repos found".
				httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "org scan: decoding repo page failed", "name", name, "page", page, "error", err)
				break
			}
			resp.Body.Close()

			if len(items) == 0 {
				break
			}
			foundRepos = true

			for _, item := range items {
				// Check if repo already exists in our database.
				// v0.27.36: a lookup ERROR is not "doesn't exist" — falling
				// through to the create path on a transient DB error would
				// mint duplicate rows. Skip the item; the periodic org
				// refresh retries it next scan.
				repoID, err := s.store.FindRepoByURL(ctx, item.HTMLURL)
				if err != nil {
					httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "org scan: repo lookup failed", "url", logURL(item.HTMLURL), "error", err)
					continue
				}
				if repoID > 0 {
					// Already exists — just add the user_repos reference.
					if _, err := s.store.AddRepoToGroupByID(ctx, groupID, repoID); err != nil {
						httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "org scan: linking existing repo failed", "repo_id", repoID, "error", err)
					}
					// v0.27.102: opportunistic forge-ID backfill (fill-
					// empty-only) so the org-tracked cohort gains rename
					// protection on the next scan pass.
					if idErr := s.store.SetPlatformRepoIDIfEmptySeen(ctx, repoID, model.ForgeIDString(item.ID), item.CreatedAt); idErr != nil {
						httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, idErr, "org scan: platform_repo_id backfill failed", "repo_id", repoID, "error", idErr)
					}
					alreadyExisted++
					added++
				} else {
					// New repo — create it and enqueue for collection.
					repoID, err = s.store.UpsertRepo(ctx, &model.Repo{
						Platform:   model.PlatformGitHub,
						GitURL:     item.HTMLURL,
						Name:       item.Name,
						Owner:      item.Owner.Login,
						PlatformID: model.ForgeIDString(item.ID), // v0.27.102 — enables the rename-heal inside UpsertRepo
					})
					if err != nil {
						httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "org scan: upserting new repo failed", "url", logURL(item.HTMLURL), "error", err)
						continue
					}
					// A silently failed enqueue strands a catalog row with no
					// queue row — a suspected origin of the reconciliation
					// gap found in the 2026-07-21 audit (summary/18 Phase 2).
					if err := s.store.EnqueueRepo(ctx, repoID, 100); err != nil {
						httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "org scan: enqueue failed", "repo_id", repoID, "url", logURL(item.HTMLURL), "error", err)
					}
					if _, err := s.store.AddRepoToGroupByID(ctx, groupID, repoID); err != nil {
						httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "org scan: linking new repo failed", "repo_id", repoID, "error", err)
					}
					newlyQueued++
					added++
				}
			}
			page++
		}

		if foundRepos {
			break // Found repos via this path, don't try the fallback.
		}
	}

	s.logger.Info("scan complete", "name", name,
		"total_added", added, "newly_queued", newlyQueued, "already_existed", alreadyExisted)
}

func (s *Server) handleRemoveRepo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sess := s.getSession(r)
	groupID, _ := strconv.ParseInt(r.FormValue("group_id"), 10, 64)
	repoID, _ := strconv.ParseInt(r.FormValue("repo_id"), 10, 64)

	if groupID > 0 && repoID > 0 {
		// The error was discarded and the page redirected as if the
		// repository had been removed (old problem O2).
		err := s.store.RemoveRepoFromGroup(r.Context(), sess.UserID, groupID, repoID)
		if errors.Is(err, db.ErrGroupNotOwned) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			s.serverError(w, r, "handleRemoveRepo", err)
			return
		}
	}
	http.Redirect(w, r, fmt.Sprintf("/groups/%d", groupID), http.StatusFound)
}

// handleCompare renders the comparison page for up to 5 repos.
// Repos are specified by ID in the query string: /compare?repos=1,2,3,4,5
func (s *Server) handleCompare(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)

	// Get the user's groups and their repos for the search dropdown.
	groups, err := s.store.GetUserGroups(r.Context(), sess.UserID)
	if err != nil {
		s.serverError(w, r, "handleCompare", err)
		return
	}

	s.render(w, "compare", map[string]any{
		"Session": sess,
		"Groups":  groups,
		"RepoIDs": r.URL.Query().Get("repos"),
	})
}

// handleSBOMDownload generates and returns an SBOM for a repo. The caller
// must own the group named in the URL; whether the repo is a member of that
// group is not checked (PR #218 review C16: the doc claimed "the group
// containing the repo"; adding the membership check was not taken, a
// recorded decision).
func (s *Server) handleSBOMDownload(w http.ResponseWriter, r *http.Request, sess *Session, groupID int64, repoIDStr string) {
	repoID, err := strconv.ParseInt(repoIDStr, 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	// Verify the user owns this group. Only "not yours" is a 403 (SR-5,
	// follow-up 12): a store failure is the 500 it is.
	if _, _, err := s.store.GetGroupDetail(r.Context(), sess.UserID, groupID, 1, 1, ""); err != nil {
		if errors.Is(err, db.ErrGroupNotOwned) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		s.serverError(w, r, "handleSBOMDownload", err)
		return
	}

	format := r.URL.Query().Get("format")
	if format == "" {
		format = "cyclonedx"
	}

	var sbomFormat collector.SBOMFormat
	var filename string
	switch format {
	case "cyclonedx":
		sbomFormat = collector.FormatCycloneDX
		filename = fmt.Sprintf("sbom-repo-%d-cyclonedx.json", repoID)
	case "spdx":
		sbomFormat = collector.FormatSPDX
		filename = fmt.Sprintf("sbom-repo-%d-spdx.json", repoID)
	default:
		http.Error(w, "format must be 'cyclonedx' or 'spdx'", http.StatusBadRequest)
		return
	}

	data, err := collector.GenerateSBOM(r.Context(), s.store, repoID, sbomFormat)
	if errors.Is(err, db.ErrRepoNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		// The cause goes to the log, not the body (it carried the store's
		// text through v0.29.67; follow-up 12).
		s.serverError(w, r, "handleSBOMDownload", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	w.Write(data)
}

// handleRepoDetail shows the repo visualization/detail page.
// This is the landing page when a user clicks a repo name in the group list.
func (s *Server) handleRepoDetail(w http.ResponseWriter, r *http.Request, sess *Session, groupID int64, repoIDStr string) {
	repoID, err := strconv.ParseInt(repoIDStr, 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	// Verify group ownership: only "not yours" is a 403 (SR-5, follow-up 12).
	group, _, err := s.store.GetGroupDetail(r.Context(), sess.UserID, groupID, 1, 1, "")
	if errors.Is(err, db.ErrGroupNotOwned) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if err != nil {
		s.serverError(w, r, "handleRepoDetail", err)
		return
	}

	// Get repo details: only "no such repository" is a 404.
	repo, err := s.store.GetRepoByID(r.Context(), repoID)
	if errors.Is(err, db.ErrRepoNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		s.serverError(w, r, "handleRepoDetail", err)
		return
	}

	// Get stats: an enrichment the page renders without, so a failure is
	// logged, not a 500 (the template handles a nil Stats).
	stats, err := s.store.GetRepoStats(r.Context(), repoID)
	if err != nil && !httpserver.RequestEnded(r.Context(), err) {
		s.logger.Warn("repo page: stats unavailable", "repo_id", repoID, "error", err)
	}

	s.render(w, "repo_detail", map[string]any{
		"Session": sess,
		"Group":   group,
		"Repo":    repo,
		"Stats":   stats,
		"RepoID":  repoID,
		"GroupID": groupID,
	})
}

// ============================================================
// Monitor dashboard (integrated from standalone monitor)
// ============================================================

const monitorPageSize = 200

func (s *Server) handleMonitor(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)

	// Parse pagination and search.
	page := 1
	if p := r.URL.Query().Get("page"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			page = n
		}
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	offset := (page - 1) * monitorPageSize

	stats, err := s.store.QueueStats(r.Context())
	if err != nil {
		s.serverError(w, r, "handleMonitor", err)
		return
	}
	jobs, total, err := s.store.ListQueuePage(r.Context(), monitorPageSize, offset, query, "", "")
	if err != nil {
		s.serverError(w, r, "handleMonitor", err)
		return
	}

	totalPages := (total + monitorPageSize - 1) / monitorPageSize
	if totalPages < 1 {
		totalPages = 1
	}

	// Sliding window of up to 5 page numbers centered on the current
	// page, same shape as handleGroup. The shared paginationNav block
	// renders clickable links for each; combined with First/Prev/Next/
	// Last at the edges, a user is never more than one click from any
	// nearby page.
	const windowSize = 5
	winStart := page - windowSize/2
	if winStart < 1 {
		winStart = 1
	}
	winEnd := winStart + windowSize - 1
	if winEnd > totalPages {
		winEnd = totalPages
		winStart = winEnd - windowSize + 1
		if winStart < 1 {
			winStart = 1
		}
	}
	var pageWindow []int
	for i := winStart; i <= winEnd; i++ {
		pageWindow = append(pageWindow, i)
	}

	// Enrich jobs with repo details and gathered vs metadata counts.
	// Both lookups are batched (single SQL round-trip each). The prior
	// version called GetRepoByID inside the per-row loop — 200 serial
	// SELECTs per page render that made Prev/Next click navigation race
	// against the 10s auto-refresh and get cancelled.
	repoIDs := make([]int64, 0, len(jobs))
	for _, j := range jobs {
		repoIDs = append(repoIDs, j.RepoID)
	}
	// Enrichment of the rows: the page renders without it, so a failure is
	// logged, not a 500.
	repos, err := s.store.GetReposBatch(r.Context(), repoIDs)
	if err != nil && !httpserver.RequestEnded(r.Context(), err) {
		s.logger.Warn("monitor page: repository details unavailable", "rows", len(repoIDs), "error", err)
	}
	repoStats, err := s.store.GetRepoStatsBatch(r.Context(), repoIDs)
	if err != nil && !httpserver.RequestEnded(r.Context(), err) {
		s.logger.Warn("monitor page: repository stats unavailable", "rows", len(repoIDs), "error", err)
	}

	type monitorRow struct {
		RowNum          int
		RepoID          int64
		Owner           string
		Repo            string
		Plat            string
		Status          string
		Priority        int
		Due             string
		LastRun         string
		Worker          string
		ErrInfo         string
		GatheredIssues  int
		MetaIssues      int
		GatheredPRs     int
		MetaPRs         int
		GatheredCommits int
		MetaCommits     int
	}

	rows := make([]monitorRow, 0, len(jobs))
	for i, j := range jobs {
		row := monitorRow{
			RowNum:   offset + i + 1,
			RepoID:   j.RepoID,
			Status:   j.Status,
			Priority: j.Priority,
		}

		if repo, ok := repos[j.RepoID]; ok {
			row.Owner = repo.Owner
			row.Repo = repo.Name
			row.Plat = repo.Platform.String()
		}
		if st, ok := repoStats[j.RepoID]; ok {
			row.GatheredIssues = st.GatheredIssues
			row.GatheredPRs = st.GatheredPRs
			row.GatheredCommits = st.GatheredCommits
			row.MetaIssues = st.MetadataIssues
			row.MetaPRs = st.MetadataPRs
			row.MetaCommits = st.MetadataCommits
		}

		row.Due = j.DueAt.Format("Jan 2 15:04")
		if j.DueAt.Before(time.Now()) && j.Status == "queued" {
			row.Due = "now"
		}
		row.LastRun = "-"
		if j.LastCollected != nil {
			row.LastRun = j.LastCollected.Format("Jan 2 15:04")
			if j.LastDurationMs > 0 {
				row.LastRun += fmt.Sprintf(" (%ds)", j.LastDurationMs/1000)
			}
		}
		if j.LockedBy != nil {
			row.Worker = *j.LockedBy
		}
		if j.LastError != nil && *j.LastError != "" {
			row.ErrInfo = *j.LastError
		}

		rows = append(rows, row)
	}

	s.render(w, "monitor", map[string]any{
		"Session":    sess,
		"Stats":      stats,
		"Jobs":       rows,
		"Page":       page,
		"TotalPages": totalPages,
		"Total":      total,
		"Query":      query,
		"PageWindow": pageWindow,
	})
}

func (s *Server) handleMonitorPrioritize(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if err := s.store.PrioritizeRepo(r.Context(), repoID); err != nil {
		if errors.Is(err, db.ErrRepoNotInQueue) {
			http.Error(w, "repo not found in queue", http.StatusNotFound)
			return
		}
		s.serverError(w, r, "handleMonitorPrioritize", err)
		return
	}
	// Redirect back to the monitor page.
	http.Redirect(w, r, "/monitor", http.StatusFound)
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

// serverError answers a store or generation failure on a page: the cause
// goes to the log at ERROR with the handler's name and the body is generic
// (worklist follow-up 12: the group, repository and SBOM pages turned every
// lookup error into a 404 or 403, and the SBOM download wrote the error's
// text into its body). A client that left mid-request (context.Canceled) is
// Debug, as in the API's serverError.
func (s *Server) serverError(w http.ResponseWriter, r *http.Request, handler string, err error) {
	if httpserver.RequestEnded(r.Context(), err) { // the client left, or http_timeout_seconds fired (its WARN reports it)
		s.logger.Debug("request ended before its handler finished", "handler", handler, "error", err)
		return
	}
	s.logger.Error("request failed", "handler", handler, "error", err)
	http.Error(w, "internal error; try again", http.StatusInternalServerError)
}

// addErrorFlag names the notice the group page shows for a failed repository
// add: "rejected" for a group an administrator rejected (trying again cannot
// work), "invalid" for the caller's own input — a URL over-long, carrying
// credentials, or refused by the database as a value (a data exception such
// as a NUL byte; follow-up 12: it said "try adding them again") — and "1",
// the retryable failure, for everything else.
func addErrorFlag(err error) string {
	switch {
	case errors.Is(err, db.ErrGroupRejected):
		return "rejected"
	case errors.Is(err, db.ErrURLTooLong), errors.Is(err, platform.ErrURLUserinfo), db.IsRejectedValue(err):
		return "invalid"
	}
	return "1"
}
