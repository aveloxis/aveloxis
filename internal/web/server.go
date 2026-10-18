// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package web provides the user-facing web GUI with OAuth authentication,
// group management, and repo/org tracking.
package web

import (
	"context"
	"html/template"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/mailer"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/static"
	"golang.org/x/oauth2"
)

// Server is the web GUI server.
type Server struct {
	store *db.PostgresStore
	cfg   config.WebConfig
	// proxyCheck logs a missing web.trusted_proxy once (v0.29.89; the
	// shared detector since v0.29.91).
	proxyCheck httpserver.MissingTrustedProxyDetector
	logger     *slog.Logger
	ghOAuth    *oauth2.Config
	glOAuth    *oauth2.Config
	ghKeys     *platform.KeyPool // for immediate org scanning
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
		// Rewrite, not Director (v0.29.91 review round 2 R2-1): a Director
		// proxy appends its own peer (nginx) to X-Forwarded-For, so an api
		// trusting 127.0.0.1 took nginx's exempt address as the client. The
		// api is sent exactly the client this process resolved under its
		// own web.trusted_proxy (a client's own claims never pass through),
		// so one api.trusted_proxy (the web's address) is right whether
		// nginx proxies /api itself or through here. SetURL joins the path
		// and query as NewSingleHostReverseProxy did; the inbound Host and
		// X-Forwarded-Host/-Proto are kept as before (Rewrite drops them).
		rp := &httputil.ReverseProxy{Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = pr.In.Host
			for _, h := range []string{"X-Forwarded-Host", "X-Forwarded-Proto"} {
				if v := pr.In.Header.Values(h); len(v) > 0 {
					pr.Out.Header[h] = v
				}
			}
			s.warnMissingTrustedProxy(pr.In)
			if ip := httpserver.ClientIP(pr.In, s.cfg.TrustedProxy); ip != nil {
				pr.Out.Header.Set("X-Forwarded-For", ip.String())
			}
		}}
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
	s.tmpl = parseTemplates()

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
	// Administrators only (v0.29.91 whole-PR follow-up): the whole
	// collection queue, and a boost that reorders everyone's collection —
	// the API's /admin/monitor routes are administrator-only too.
	mux.HandleFunc("/monitor", s.requireAdmin(s.handleMonitor))
	mux.HandleFunc("POST /monitor/prioritize/{repoID}", s.requireAdmin(s.handleMonitorPrioritize))

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
