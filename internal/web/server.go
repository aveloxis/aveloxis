// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package web provides the user-facing web GUI with OAuth authentication,
// group management, and repo/org tracking.
package web

import (
	"context"
	"errors"
	"html/template"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/admin"
	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/mailer"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/queueview"
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
	// github and gitlab are the two sign-in flows' constants around the
	// configs above (0.29.102, WEB-14): one head for both callbacks.
	github *oauthProvider
	gitlab *oauthProvider
	// ghAPIBase is the GitHub REST host those keys belong to, normalised by
	// New through platform.GitHubAPIBaseOrPublic so it is never empty. A
	// REQUIRED parameter of New, next to the pool, so a
	// caller cannot hand over the keys and forget the host they go with —
	// which is how the org scan below sent an Enterprise token to public
	// GitHub (v0.29.57, the last site of worklist item 34's client half).
	ghAPIBase string
	// gitlabAPIBase is gitlab.base_url (WithGitLabAPIBase): the admin
	// service classifies an organisation on that instance as GitLab.
	gitlabAPIBase string
	// lookupSession is the ONE session read of a request (0.29.102, stage
	// 2c of summary/59): New sets it to storeSession (the session row
	// joined to the users row — name, avatar, provider and privilege, D-1);
	// tests without a store plant their own. db.ErrNoWebSession means no
	// session; any other error is a 500, never "no session" (SR-5).
	lookupSession func(ctx context.Context, raw string) (*Session, error)
	// mintProxyToken mints the Bearer the /api/ proxy attaches for a web
	// session (0.29.110, stage 4): New sets it to the store's
	// MintTokenForWebSession (kind "api", parent = the session); tests
	// without a store plant their own. proxyTokens holds the answers.
	mintProxyToken func(ctx context.Context, cookie string, expiresAt time.Time) (string, error)
	proxyTokens    proxyTokens
	tmpl           *template.Template
	apiProxy       http.Handler   // reverse proxy for /api/* → cfg.APIInternalURL; nil on parse failure
	mailer         *mailer.Mailer // gmail-backed transactional mailer; safely nil if unconfigured (v0.19.0)
	// admin is the administrative service this process adapts (stage 1 of
	// summary/59): groups, additions, approvals, roles and the account
	// e-mail flow. Built by New, rebuilt by WithMailer so the mailer it
	// sends through is the one attached.
	admin *admin.Service
	// queueStats caches the queue's status counts for the /monitor page
	// (stage 3d, D-22): the GROUP BY over the queue runs once per TTL, not
	// once per tab refresh.
	queueStats *queueview.StatsCache
}

// Session is a signed-in browser session as the gate resolved it for
// THIS request (0.29.102): the session row (aveloxis_ops.user_session_tokens,
// kind "web", hashed) joined to the users row, so LoginName, AvatarURL,
// Provider and IsAdmin are the row's current values — a demotion takes
// effect at the demoted account's next request (D-1; v0.19.0 cached the
// flag at login for the session's lifetime). Nothing is held in the
// process: sessions survive a restart, and a sign-out is one delete.
type Session struct {
	UserID    int
	LoginName string
	AvatarURL string
	Provider  string
	IsAdmin   bool
	ExpiresAt time.Time
	CreatedAt time.Time // the sign-in: a sensitive change asks for a recent one (ASVS V7.5.1)
	cookie    string    // the raw cookie token, for the token exchange's parent
}

// New creates a web server. The web process holds no forge keys (stage 1
// of summary/59, C2): an org added or approved here is listed by serve.
func New(store *db.PostgresStore, cfg config.WebConfig, ghAPIBase string, logger *slog.Logger) *Server {
	// One spelling of the GitLab base for every consumer below (the OAuth
	// endpoints, the /api/v4/user read, the stored instance key): the
	// config's one accessor — the trim (a trailing slash made the token
	// URL "//oauth/token" and every GitLab sign-in failed, 2026-09-28) and
	// the gitlab.com default live there (0.29.101).
	cfg.GitLabBaseURL = cfg.GitLabOAuthBase()
	s := &Server{
		store:     store,
		cfg:       cfg,
		ghAPIBase: platform.GitHubAPIBaseOrPublic(ghAPIBase),
		logger:    logger,
	}
	s.lookupSession = s.storeSession
	s.mintProxyToken = func(ctx context.Context, cookie string, expiresAt time.Time) (string, error) {
		if store == nil { // a test server without a store plants its own minter
			return "", errors.New("no store to mint a proxy token")
		}
		return store.MintTokenForWebSession(ctx, cookie, expiresAt)
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

	// The two flows' constants (nil cfg = not configured). GitHub's identity
	// read stays on api.github.com whatever github.base_url says (the
	// v0.29.57 decision above); every GitHub REST request pins the API
	// version (worklist item 15).
	s.github = &oauthProvider{name: "github", label: "GitHub", cfg: s.ghOAuth, userURL: platform.PublicGitHubAPIBase + "/user",
		headers: map[string]string{"X-GitHub-Api-Version": platform.GitHubAPIVersion}}
	s.gitlab = &oauthProvider{name: "gitlab", label: "GitLab", cfg: s.glOAuth, userURL: cfg.GitLabBaseURL + platform.GitLabAPISuffix + "/user"}

	// Build the internal API reverse proxy. The web server forwards /api/*
	// to cfg.APIInternalURL so the browser can fetch data using relative
	// URLs (same-origin, no CORS). This works transparently behind an nginx
	// front proxy: nginx → web(:8082) → reverse proxy → api(:8383). Keep it
	// that way on this GUI's host: the proxy attaches the signed-in
	// session's Bearer (0.29.110), so an nginx `location /api/` sent
	// straight to the api port answers 401 to the pages under
	// api.require_auth (0.29.115, Copilot on PR #241; the deployment guide's
	// recipe and TestDeploymentRecipeRoutesTheGUIsAPIThroughTheWeb).
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
			// The api identifies the caller by the Bearer the handler below
			// attached, never by a cookie (0.29.110, stage 4): the session
			// cookie and every other cookie stop here.
			pr.Out.Header.Del("Cookie")
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
		rp.ModifyResponse = s.modifyProxyResponse // the api's duplicate security headers dropped; a refused Bearer forgotten (0.29.110, 0.29.113)
		rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			if r.Context().Err() != nil {
				// The web's bound fired (its WARN says so) or the client
				// left: the api is not at fault, and nobody reads a 502.
				return
			}
			// r.URL.Path is attacker-controlled — sanitize before logging.
			logger.Warn("api reverse proxy error", "path", platform.TruncateForLog([]byte(r.URL.Path), 200), "error", err)
			http.Error(w, "API backend unavailable", http.StatusBadGateway)
		}
		s.apiProxy = rp
	} else {
		logger.Warn("invalid api_internal_url; /api proxy disabled",
			"api_internal_url", logURL(apiURL), "error", err)
	}

	// Parse embedded templates.
	s.tmpl = parseTemplates()
	s.queueStats = queueview.NewStatsCache(queueview.DefaultStatsTTL, s.logger)

	s.admin = s.newAdminService(nil)
	return s
}

// WithGitLabAPIBase tells the admin service which GitLab instance this
// deployment enumerates (gitlab.base_url): an organisation URL on it is
// registered as a GitLab group whatever its host name says. Returns the
// server for chaining; the service is rebuilt with the mailer it has.
func (s *Server) WithGitLabAPIBase(base string) *Server {
	s.gitlabAPIBase = base
	s.admin = s.newAdminService(s.mailer)
	return s
}

// gitlabWebBase is the configured GitLab instance's web base ("" without
// one), derived from gitlab.base_url the way the admin service derives it.
func (s *Server) gitlabWebBase() string {
	base, _ := platform.GitLabWebBaseForAPI(s.gitlabAPIBase)
	return base
}

// WithMailer attaches a transactional mailer to the server. Returns
// the server for chaining. Optional — without one (or with a disabled
// one) the notification hooks (welcome email, add-request notices and
// decisions) send nothing, the account-email form refuses new addresses,
// and users without an address reach the dashboard; see
// confirmationBaseFor.
func (s *Server) WithMailer(m *mailer.Mailer) *Server {
	s.mailer = m
	s.admin = s.newAdminService(m)
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

	// Static assets: the page assets (stylesheet, scripts, vendored
	// Chart.js) under /static/ (stage 2e, WEB-9/B6) and the icon.
	mux.Handle("GET /static/", static.Handler("/static/"))
	mux.HandleFunc("GET /icon.png", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.Write(static.IconPNG)
	})

	// Every route is method-and-path (0.29.102, stage 2a of summary/59,
	// WEB-8): the mux decides the method and the path shape, the handlers
	// read path values, and a state change never rides a GET (B3) — the
	// CSRF middleware of stage 2f sees every POST route as one set. The
	// route matrix test reads this list from the source.
	//
	// Public routes. "/" must NOT be a catch-all — use GET to avoid
	// swallowing static asset routes like /icon.png.
	mux.HandleFunc("GET /{$}", s.handleHome)
	mux.HandleFunc("GET /login", s.handleLogin)
	mux.HandleFunc("GET /auth/github", s.handleGitHubAuth)
	mux.HandleFunc("GET /auth/github/callback", s.handleGitHubCallback)
	mux.HandleFunc("GET /auth/gitlab", s.handleGitLabAuth)
	mux.HandleFunc("GET /auth/gitlab/callback", s.handleGitLabCallback)
	// Logout is a POST (B3/D-13): the pages send a form, aveloxis-gui a
	// fetch; a GET answers 405 and changes nothing.
	mux.HandleFunc("POST /logout", s.handleLogout)
	// v0.27.4: alias under /auth/ so the SPA can reach logout through
	// the same proxy prefix as the OAuth routes (nginx/dev-server only
	// forward /auth/* and /api/*). Clears the session + oauth cookies;
	// the SPA ignores the redirect and routes itself to login.html.
	mux.HandleFunc("POST /auth/logout", s.handleLogout)
	// v0.27.1: mints a DB-backed Bearer token for the separate-origin
	// SPA (aveloxis-gui). The SPA completes OAuth on this origin (the
	// session cookie), then exchanges it here for a token it sends as
	// Authorization: Bearer to the api process. POST-only from 0.29.102:
	// minting is a state change (aveloxis-gui's login page sends POST).
	mux.HandleFunc("POST /auth/token", s.requireAuth(s.handleAuthToken))

	// Authenticated routes.
	mux.HandleFunc("GET /dashboard", s.requireAuth(s.handleDashboard))
	// The account page (0.29.113, ASVS V7.5.2): the account's sessions, and
	// one form that ends them all; an administrator ends any account's from
	// the Users page (V7.4.5).
	mux.HandleFunc("GET /account", s.requireAuth(s.handleAccount))
	mux.HandleFunc("POST /account/sessions/revoke", s.requireAuth(s.handleAccountSessionsRevoke))
	mux.HandleFunc("GET /admin/users/{id}/sessions/revoke", s.requireAdmin(s.handleAdminUserSessionsRevokeConfirm))
	mux.HandleFunc("POST /admin/users/{id}/sessions/revoke", s.requireAdmin(s.handleAdminUserSessionsRevoke))
	mux.HandleFunc("GET /account/email", s.requireAuth(s.handleAccountEmailForm))
	mux.HandleFunc("POST /account/email", s.requireAuth(s.handleAccountEmailSubmit))
	mux.HandleFunc("GET /account/email/confirm", s.requireAuth(s.handleEmailConfirm))
	mux.HandleFunc("POST /groups/new", s.requireAuth(s.handleNewGroup))
	mux.HandleFunc("GET /groups/{gid}", s.requireAuth(s.handleGroup))
	mux.HandleFunc("GET /groups/{gid}/repos/{rid}", s.requireAuth(s.handleRepoDetail))
	mux.HandleFunc("GET /groups/{gid}/repos/{rid}/sbom", s.requireAuth(s.handleSBOMDownload))
	// D-19: a server-side confirmation page, not a confirm() dialog.
	mux.HandleFunc("GET /groups/{gid}/repos/{rid}/remove", s.requireAuth(s.handleRemoveRepoConfirm))
	mux.HandleFunc("POST /groups/add-repo", s.requireAuth(s.handleAddRepo))
	mux.HandleFunc("POST /groups/add-org", s.requireAuth(s.handleAddOrg))
	mux.HandleFunc("POST /groups/remove-repo", s.requireAuth(s.handleRemoveRepo))
	mux.HandleFunc("GET /compare", s.requireAuth(s.handleCompare))

	// Monitor dashboard — integrated from the standalone monitor server.
	// Administrators only (v0.29.91 whole-PR follow-up): the whole
	// collection queue, and a boost that reorders everyone's collection —
	// the API's /admin/monitor routes are administrator-only too.
	mux.HandleFunc("GET /monitor", s.requireAdmin(s.handleMonitor))
	mux.HandleFunc("POST /monitor/prioritize/{repoID}", s.requireAdmin(s.handleMonitorPrioritize))

	// v0.19.0 admin pages. requireAdmin decides privilege from the users
	// row per request (0.29.102) so non-admin users get a 403 instead of
	// seeing other people's pending submissions or toggling admin roles.
	mux.HandleFunc("GET /admin/groups/pending", s.requireAdmin(s.handleAdminPendingGroups))
	mux.HandleFunc("POST /admin/groups/{id}/reject", s.requireAdmin(s.handleRejectGroup))
	// v0.27.20 per-add approval queue (summary/15): decisions on
	// non-admin additions of not-yet-tracked repos/orgs.
	mux.HandleFunc("POST /admin/add-requests/{id}/approve", s.requireAdmin(s.handleApproveAddRequest))
	mux.HandleFunc("POST /admin/add-requests/{id}/reject", s.requireAdmin(s.handleRejectAddRequest))
	// Stage 3a (0.29.109): the Approvals page adopts a recorded forge-ID change (D-24).
	mux.HandleFunc("POST /admin/forge-id-changes/{repoID}/adopt", s.requireAdmin(s.handleAdoptForgeID))
	// Stage 3c (0.29.109): the API-tokens, Capacity and Collections pages.
	// Every destructive action has a GET confirm page and a POST (D-19).
	mux.HandleFunc("GET /admin/api-tokens", s.requireAdmin(s.handleAdminAPITokens))
	mux.HandleFunc("POST /admin/api-tokens", s.requireAdmin(s.handleAdminAPITokenGrant))
	mux.HandleFunc("GET /admin/api-tokens/{tokenID}/revoke", s.requireAdmin(s.handleAdminAPITokenRevokeConfirm))
	mux.HandleFunc("POST /admin/api-tokens/{tokenID}/revoke", s.requireAdmin(s.handleAdminAPITokenRevoke))
	mux.HandleFunc("POST /admin/api-token-settings", s.requireAdmin(s.handleAdminAPITokenDefaults))
	mux.HandleFunc("GET /admin/capacity", s.requireAdmin(s.handleAdminCapacity))
	// The API cache's view (0.29.123): a shell; its script reads the api's
	// /api/v1/admin/cache routes through the /api/ proxy (the caches live in
	// the api process).
	mux.HandleFunc("GET /admin/cache", s.requireAdmin(s.handleAdminCachePage))
	mux.HandleFunc("POST /admin/capacity/quotas/{name}", s.requireAdmin(s.handleAdminCapacityQuota))
	mux.HandleFunc("POST /admin/capacity/contact", s.requireAdmin(s.handleAdminCapacityContact))
	mux.HandleFunc("POST /admin/capacity/accounts/{userID}", s.requireAdmin(s.handleAdminCapacityOverride))
	mux.HandleFunc("GET /admin/capacity/accounts/{userID}/clear", s.requireAdmin(s.handleAdminCapacityClearConfirm))
	mux.HandleFunc("POST /admin/capacity/accounts/{userID}/clear", s.requireAdmin(s.handleAdminCapacityClear))
	mux.HandleFunc("POST /admin/capacity/signup-allowlist", s.requireAdmin(s.handleAdminAllowlistAdd))
	mux.HandleFunc("GET /admin/capacity/signup-allowlist/remove", s.requireAdmin(s.handleAdminAllowlistRemoveConfirm))
	mux.HandleFunc("POST /admin/capacity/signup-allowlist/remove", s.requireAdmin(s.handleAdminAllowlistRemove))
	mux.HandleFunc("GET /admin/collections", s.requireAdmin(s.handleAdminCollections))
	mux.HandleFunc("POST /admin/collections", s.requireAdmin(s.handleAdminCollectionCreate))
	mux.HandleFunc("GET /admin/collections/{collectionID}", s.requireAdmin(s.handleAdminCollection))
	mux.HandleFunc("POST /admin/collections/{collectionID}", s.requireAdmin(s.handleAdminCollectionUpdate))
	mux.HandleFunc("GET /admin/collections/{collectionID}/delete", s.requireAdmin(s.handleAdminCollectionDeleteConfirm))
	mux.HandleFunc("POST /admin/collections/{collectionID}/delete", s.requireAdmin(s.handleAdminCollectionDelete))
	mux.HandleFunc("POST /admin/collections/{collectionID}/groups", s.requireAdmin(s.handleAdminCollectionAddGroup))
	mux.HandleFunc("GET /admin/collections/{collectionID}/groups/{groupID}/remove", s.requireAdmin(s.handleAdminCollectionRemoveGroupConfirm))
	mux.HandleFunc("POST /admin/collections/{collectionID}/groups/{groupID}/remove", s.requireAdmin(s.handleAdminCollectionRemoveGroup))
	mux.HandleFunc("GET /admin/users", s.requireAdmin(s.handleAdminUsers))
	mux.HandleFunc("POST /admin/users/{id}/admin", s.requireAdmin(s.handleSetUserAdmin))

	// Same-origin API reverse proxy — the one method-less registration:
	// the API's own routes decide the method. Browser fetch calls use relative
	// /api/v1/... URLs; this handler forwards them to cfg.APIInternalURL.
	// Gated by requireAuth so an anonymous visitor can't query collected
	// data through the proxy. The browser sends the aveloxis_session cookie
	// on same-origin requests, which the gate validates; the api then sees
	// the session's own Bearer, attached here (0.29.110, stage 4 of
	// summary/59, D-9/C1), so it scopes the answer to the account and
	// works with api.require_auth on. A Bearer the browser sent itself is
	// replaced: the session decides the identity, nothing the page holds.
	if s.apiProxy != nil {
		mux.Handle("/api/", s.requireAuth(s.handleAPIProxy))
	}

	// Every answer carries the security headers, and every state change
	// passes the cross-site guard before its route (stage 2f, B1/D-10/D-11).
	return s.securityHeaders(s.csrfGuard(mux))
}

// render executes a template and logs failures. v0.27.36 (summary/18
// Phase 0a): the bare ExecuteTemplate calls discarded render errors, so
// a template failure mid-render produced a truncated page with no log
// signal. Headers are already sent by the time a body-write fails, so
// logging is the only possible action — but it must happen.
func (s *Server) render(w http.ResponseWriter, r *http.Request, name string, data any) {
	// The session's form token rides into every page (stage 2f): the
	// templates put it in each POST form as csrf_token.
	if m, ok := data.(map[string]any); ok {
		m["CSRF"] = s.csrfToken(w, r)
	}
	// Every rendered page is one visitor's (a session's groups, the console's
	// accounts, tokens and addresses): never stored by a browser's back
	// cache or a shared cache (ASVS V14.3.2, the 0.29.112 review). A handler
	// that set the header first keeps its value.
	if w.Header().Get("Cache-Control") == "" {
		w.Header().Set("Cache-Control", "no-store")
	}
	if err := s.tmpl.ExecuteTemplate(w, name, data); err != nil {
		// No request here: a render refused after the bound is http.ErrHandlerTimeout, which RequestEnded recognises on any context.
		httpserver.LogFailure(context.Background(), s.logger, slog.LevelError, err, "template render failed", "template", name, "error", err)
	}
}
