// Package web provides the user-facing web GUI with OAuth authentication,
// group management, and repo/org tracking.
package web

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log/slog"
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
	"github.com/aveloxis/aveloxis/internal/mailer"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/static"
	"golang.org/x/oauth2"
)

// Server is the web GUI server.
type Server struct {
	store     *db.PostgresStore
	cfg       config.WebConfig
	logger    *slog.Logger
	ghOAuth   *oauth2.Config
	glOAuth   *oauth2.Config
	ghKeys    *platform.KeyPool // for immediate org scanning
	sessionMu sync.RWMutex
	sessions  map[string]*Session // session token -> session
	tmpl      *template.Template
	apiProxy  http.Handler // reverse proxy for /api/* → cfg.APIInternalURL; nil on parse failure
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
func New(store *db.PostgresStore, cfg config.WebConfig, ghKeys *platform.KeyPool, logger *slog.Logger) *Server {
	s := &Server{
		store:    store,
		cfg:      cfg,
		ghKeys:   ghKeys,
		logger:   logger,
		sessions: make(map[string]*Session),
	}

	baseURL := strings.TrimSuffix(cfg.BaseURL, "/")
	if baseURL == "" {
		baseURL = "http://localhost" + cfg.Addr
	}

	// GitHub OAuth config.
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
		rp.Transport = &http.Transport{
			// Short timeouts so a dead api process fails fast instead of
			// blocking browser requests. The browser sees 502 Bad Gateway.
			ResponseHeaderTimeout: 15 * time.Second,
			IdleConnTimeout:       60 * time.Second,
		}
		rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			logger.Warn("api reverse proxy error", "path", r.URL.Path, "error", err)
			http.Error(w, "API backend unavailable", http.StatusBadGateway)
		}
		s.apiProxy = rp
	} else {
		logger.Warn("invalid api_internal_url; /api proxy disabled",
			"api_internal_url", apiURL, "error", err)
	}

	// Parse embedded templates.
	s.tmpl = template.Must(template.New("").Funcs(template.FuncMap{
		"truncate": func(s string, n int) string {
			if len(s) <= n {
				return s
			}
			return s[:n] + "..."
		},
		"dict": func(values ...interface{}) map[string]interface{} {
			m := make(map[string]interface{})
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
// the server for chaining. Optional — when not called, mailer-related
// hooks (welcome email on signup, group-approved notification) are
// silently skipped, matching mailer.Send's no-op-when-unconfigured
// semantics.
func (s *Server) WithMailer(m *mailer.Mailer) *Server {
	s.mailer = m
	return s
}

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
	if len(body) <= max {
		return string(body)
	}
	return string(body[:max]) + "...(truncated)"
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

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	s.tmpl.ExecuteTemplate(w, "login", map[string]interface{}{
		"HasGitHub": s.ghOAuth != nil,
		"HasGitLab": s.glOAuth != nil,
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
	http.Redirect(w, r, "/login", http.StatusFound)
}

func (s *Server) handleGitHubAuth(w http.ResponseWriter, r *http.Request) {
	if s.ghOAuth == nil {
		http.Error(w, "GitHub OAuth not configured", http.StatusBadRequest)
		return
	}
	state := generateToken()
	http.SetCookie(w, s.oauthStateCookie(state))
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

	// Exchange code for token.
	token, err := s.ghOAuth.Exchange(r.Context(), r.URL.Query().Get("code"))
	if err != nil {
		http.Error(w, "OAuth exchange failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Get user info from GitHub.
	client := s.ghOAuth.Client(r.Context(), token)
	resp, err := client.Get("https://api.github.com/user")
	if err != nil {
		http.Error(w, "Failed to get user info", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		s.logger.Error("github /user returned non-200",
			"status", resp.StatusCode, "body", truncateForLog(body, 200))
		http.Error(w, "GitHub user fetch failed", http.StatusBadGateway)
		return
	}

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
		if email := fetchGitHubPrimaryEmail(r.Context(), client, s.logger); email != "" {
			ghUser.Email = email
		}
	}

	// Create or find user. v0.19.0: UpsertOAuthUser auto-promotes the
	// first-ever user to admin so a fresh deployment can review
	// subsequent submissions.
	wasNewUser := false
	preCount := 0
	_ = s.store.Pool().QueryRow(r.Context(),
		`SELECT COUNT(*) FROM aveloxis_ops.users WHERE login_name = $1`, ghUser.Login).Scan(&preCount)
	wasNewUser = preCount == 0

	userID, err := s.store.UpsertOAuthUser(r.Context(), db.OAuthUserInfo{
		Login:     ghUser.Login,
		Email:     ghUser.Email,
		Name:      ghUser.Name,
		AvatarURL: ghUser.AvatarURL,
		GHUserID:  ghUser.ID,
		GHLogin:   ghUser.Login,
		Provider:  "github",
	})
	if err != nil {
		s.logger.Error("failed to upsert OAuth user", "error", err)
		http.Error(w, "Failed to create user", http.StatusInternalServerError)
		return
	}

	// Read fresh admin flag — set to TRUE for the first-ever user
	// (auto-promotion in UpsertOAuthUser) and stays whatever the
	// admin user-management page set it to thereafter.
	isAdmin, _ := s.store.IsUserAdmin(r.Context(), userID)

	// Send welcome email on first signup. No-op if mailer
	// unconfigured. Failures here don't block login — the email is a
	// nice-to-have, not a gate.
	if wasNewUser && s.mailer != nil && ghUser.Email != "" {
		if err := s.mailer.SendWelcome(ghUser.Email, ghUser.Login, "GitHub"); err != nil {
			s.logger.Warn("failed to send welcome email", "login", ghUser.Login, "error", err)
		}
	}

	// Create session.
	sessToken := s.createSession(userID, ghUser.Login, ghUser.AvatarURL, "github", isAdmin)
	http.SetCookie(w, s.sessionCookie(sessToken))
	http.Redirect(w, r, "/dashboard", http.StatusFound)
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
func fetchGitHubPrimaryEmail(ctx context.Context, client *http.Client, logger *slog.Logger) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/user/emails", nil)
	if err != nil {
		return ""
	}
	resp, err := client.Do(req)
	if err != nil {
		logger.Warn("failed to call /user/emails for OAuth fallback", "error", err)
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
		logger.Warn("failed to decode /user/emails", "error", err)
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

	token, err := s.glOAuth.Exchange(r.Context(), r.URL.Query().Get("code"))
	if err != nil {
		http.Error(w, "OAuth exchange failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	glBase := s.cfg.GitLabBaseURL
	if glBase == "" {
		glBase = "https://gitlab.com"
	}
	client := s.glOAuth.Client(r.Context(), token)
	resp, err := client.Get(glBase + "/api/v4/user")
	if err != nil {
		http.Error(w, "Failed to get user info", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		s.logger.Error("gitlab /user returned non-200",
			"status", resp.StatusCode, "body", truncateForLog(body, 200))
		http.Error(w, "GitLab user fetch failed", http.StatusBadGateway)
		return
	}

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

	wasNewUser := false
	preCount := 0
	_ = s.store.Pool().QueryRow(r.Context(),
		`SELECT COUNT(*) FROM aveloxis_ops.users WHERE login_name = $1`, glUser.Username).Scan(&preCount)
	wasNewUser = preCount == 0

	userID, err := s.store.UpsertOAuthUser(r.Context(), db.OAuthUserInfo{
		Login:      glUser.Username,
		Email:      glUser.Email,
		Name:       glUser.Name,
		AvatarURL:  glUser.AvatarURL,
		GLUserID:   glUser.ID,
		GLUsername: glUser.Username,
		Provider:   "gitlab",
	})
	if err != nil {
		s.logger.Error("failed to upsert OAuth user", "error", err)
		http.Error(w, "Failed to create user", http.StatusInternalServerError)
		return
	}
	isAdmin, _ := s.store.IsUserAdmin(r.Context(), userID)

	if wasNewUser && s.mailer != nil && glUser.Email != "" {
		if err := s.mailer.SendWelcome(glUser.Email, glUser.Username, "GitLab"); err != nil {
			s.logger.Warn("failed to send welcome email", "login", glUser.Username, "error", err)
		}
	}

	sessToken := s.createSession(userID, glUser.Username, glUser.AvatarURL, "gitlab", isAdmin)
	http.SetCookie(w, s.sessionCookie(sessToken))
	http.Redirect(w, r, "/dashboard", http.StatusFound)
}

// ============================================================
// Dashboard & group management
// ============================================================

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)

	// v0.19.10 email gate, v0.20.4 pending-aware: redirect to
	// /account/email ONLY when the user has neither a confirmed email
	// NOR a pending one. With a pending email the dashboard renders
	// with a "check your inbox" banner instead of redirecting (avoids
	// a loop where the user submits the form, gets redirected back,
	// and never sees the confirmation prompt).
	confirmedEmail, _ := s.store.GetUserEmail(r.Context(), sess.UserID)
	pendingEmail, _ := s.store.GetUserPendingEmail(r.Context(), sess.UserID)
	if strings.TrimSpace(confirmedEmail) == "" && strings.TrimSpace(pendingEmail) == "" {
		http.Redirect(w, r, "/account/email", http.StatusFound)
		return
	}

	groups, _ := s.store.GetUserGroups(r.Context(), sess.UserID)

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

	s.tmpl.ExecuteTemplate(w, "dashboard", map[string]any{
		"Session":      sess,
		"Groups":       groups,
		"PendingOnly":  pendingOnly,
		"PendingEmail": pendingEmail,
	})
}

// handleAccountEmail renders (GET) and processes (POST) the
// email-collection form. This is the v0.19.10 fallback when both /user
// and /user/emails came back empty during OAuth callback. After the
// form is submitted, users.email is set and the user redirects to
// /dashboard.
func (s *Server) handleAccountEmail(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)

	if r.Method == http.MethodPost {
		email := strings.TrimSpace(r.FormValue("email"))
		if email == "" || !strings.Contains(email, "@") {
			s.tmpl.ExecuteTemplate(w, "account_email", map[string]any{
				"Session": sess,
				"Error":   "Please enter a valid email address.",
			})
			return
		}
		// v0.20.4: write to email_pending (not users.email) and send
		// a click-to-confirm link. The email becomes canonical only
		// after the user clicks through.
		if err := s.store.SetUserPendingEmail(r.Context(), sess.UserID, email); err != nil {
			s.logger.Warn("failed to set pending email", "user_id", sess.UserID, "error", err)
			s.tmpl.ExecuteTemplate(w, "account_email", map[string]any{
				"Session": sess,
				"Error":   "Could not save email. Try again.",
			})
			return
		}
		token, err := s.store.CreateEmailConfirmation(r.Context(), sess.UserID, email)
		if err != nil {
			s.logger.Warn("failed to create email confirmation", "user_id", sess.UserID, "error", err)
			s.tmpl.ExecuteTemplate(w, "account_email", map[string]any{
				"Session": sess,
				"Error":   "Could not generate confirmation. Try again.",
			})
			return
		}
		// Build the confirmation URL — operator-configured site URL
		// from MailConfig.SiteURL when set; otherwise derive from the
		// request host (works for local dev).
		base := strings.TrimRight(s.mailer.SiteURL(), "/")
		if base == "" {
			scheme := "https"
			if r.TLS == nil && (strings.HasPrefix(r.Host, "localhost") || strings.HasPrefix(r.Host, "127.")) {
				scheme = "http"
			}
			base = scheme + "://" + r.Host
		}
		confirmURL := base + "/account/email/confirm?token=" + token
		if s.mailer != nil {
			if err := s.mailer.SendEmailConfirmation(email, sess.LoginName, confirmURL); err != nil {
				s.logger.Warn("failed to send confirmation email",
					"user_id", sess.UserID, "email", email, "error", err)
				// Don't fail the form — the token is in the DB and the
				// operator can resend manually if needed. User sees the
				// "check your inbox" banner regardless.
			}
		}
		http.Redirect(w, r, "/dashboard", http.StatusFound)
		return
	}

	// GET: show the form. If the user already has a confirmed email,
	// they don't need to be here — bounce them back to dashboard.
	if email, _ := s.store.GetUserEmail(r.Context(), sess.UserID); strings.TrimSpace(email) != "" {
		http.Redirect(w, r, "/dashboard", http.StatusFound)
		return
	}
	s.tmpl.ExecuteTemplate(w, "account_email", map[string]any{
		"Session": sess,
	})
}

// handleEmailConfirm consumes the v0.20.4 confirmation token from the
// query string, promotes email_pending to users.email, and redirects
// back to the dashboard. On token error (expired or unknown), redirects
// to /account/email with a flag so the form shows a fresh-start prompt.
func (s *Server) handleEmailConfirm(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)
	token := strings.TrimSpace(r.URL.Query().Get("token"))
	if token == "" {
		http.Redirect(w, r, "/account/email?expired=1", http.StatusFound)
		return
	}
	userID, email, err := s.store.ConsumeEmailConfirmation(r.Context(), token)
	if err != nil {
		s.logger.Info("email confirmation token rejected",
			"session_user_id", sess.UserID, "error", err)
		http.Redirect(w, r, "/account/email?expired=1", http.StatusFound)
		return
	}
	// Defense in depth: confirm the token belongs to the logged-in
	// user. A leaked link from another user's inbox shouldn't grant
	// the clicker an email change on their own account.
	if userID != sess.UserID {
		s.logger.Warn("email confirmation token user mismatch",
			"token_user_id", userID, "session_user_id", sess.UserID)
		http.Redirect(w, r, "/dashboard", http.StatusFound)
		return
	}
	if err := s.store.ConfirmUserEmail(r.Context(), userID, email); err != nil {
		s.logger.Warn("failed to confirm user email", "user_id", userID, "error", err)
		http.Redirect(w, r, "/account/email?error=1", http.StatusFound)
		return
	}
	http.Redirect(w, r, "/dashboard", http.StatusFound)
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
		s.logger.Warn("failed to create group", "error", err)
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
	if err != nil {
		http.NotFound(w, r)
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

	s.tmpl.ExecuteTemplate(w, "group", map[string]interface{}{
		"Session":    sess,
		"Group":      group,
		"Page":       page,
		"TotalPages": totalPages,
		"TotalRepos": totalRepos,
		"Query":      query,
		"PageWindow": pageWindow,
	})
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

	if raw != "" && groupID > 0 {
		added := 0
		var errors []string
		for _, line := range strings.Split(raw, "\n") {
			repoURL := strings.TrimSpace(line)
			if repoURL == "" {
				continue
			}

			// Validate the URL before adding.
			v := ValidateRepoURL(repoURL)
			if !v.Valid {
				errors = append(errors, fmt.Sprintf("%s: %s", repoURL, v.Error))
				continue
			}
			// Use the cleaned-up URL.
			repoURL = v.URL

			if err := s.store.AddRepoToGroup(r.Context(), sess.UserID, groupID, repoURL); err != nil {
				s.logger.Warn("failed to add repo to group", "url", repoURL, "error", err)
				errors = append(errors, fmt.Sprintf("%s: %s", repoURL, err.Error()))
				continue
			}
			added++
		}
		if added > 0 {
			s.logger.Info("bulk repo add", "group_id", groupID, "added", added)
		}
		if len(errors) > 0 {
			s.logger.Warn("some URLs were invalid", "errors", errors)
			// TODO: flash message support — for now, errors are logged server-side.
			// Invalid URLs are silently skipped; valid ones are added.
		}
	}
	http.Redirect(w, r, fmt.Sprintf("/groups/%d", groupID), http.StatusFound)
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
		if err := s.store.AddOrgToGroup(r.Context(), sess.UserID, groupID, orgURL); err != nil {
			s.logger.Warn("failed to add org to group", "error", err)
		}

		// Immediately scan the org for repos and add them.
		// Use a detached context — the HTTP request context gets canceled on redirect.
		go s.scanOrgRepos(context.Background(), groupID, orgURL)
	}
	http.Redirect(w, r, fmt.Sprintf("/groups/%d", groupID), http.StatusFound)
}

// scanOrgRepos fetches all repos from a GitHub org or user and adds them to the group + queue.
// Handles both orgs (/orgs/{name}/repos) and users (/users/{name}/repos).
func (s *Server) scanOrgRepos(ctx context.Context, groupID int64, orgURL string) {
	orgURL = strings.TrimSuffix(strings.TrimSpace(orgURL), "/")
	parts := strings.Split(strings.TrimPrefix(strings.TrimPrefix(orgURL, "https://"), "http://"), "/")
	if len(parts) < 2 {
		return
	}
	name := parts[1]
	isGitHub := strings.Contains(orgURL, "github.com")

	if !isGitHub || s.ghKeys == nil {
		return
	}

	httpClient := platform.NewHTTPClient("https://api.github.com", s.ghKeys, s.logger, platform.AuthGitHub)
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
			resp, err := httpClient.Get(ctx, path)
			if err != nil {
				// 404 means this isn't an org — try the /users/ path.
				if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "forbidden") {
					break
				}
				s.logger.Warn("scan API error", "name", name, "error", err)
				break
			}
			var items []struct {
				HTMLURL string `json:"html_url"`
				Name    string `json:"name"`
				Owner   struct {
					Login string `json:"login"`
				} `json:"owner"`
			}
			json.NewDecoder(resp.Body).Decode(&items)
			resp.Body.Close()

			if len(items) == 0 {
				break
			}
			foundRepos = true

			for _, item := range items {
				// Check if repo already exists in our database.
				repoID, _ := s.store.FindRepoByURL(ctx, item.HTMLURL)
				if repoID > 0 {
					// Already exists — just add the user_repos reference.
					s.store.AddRepoToGroupByID(ctx, groupID, repoID)
					alreadyExisted++
					added++
				} else {
					// New repo — create it and enqueue for collection.
					repoID, err = s.store.UpsertRepo(ctx, &model.Repo{
						Platform: model.PlatformGitHub,
						GitURL:   item.HTMLURL,
						Name:     item.Name,
						Owner:    item.Owner.Login,
					})
					if err != nil {
						continue
					}
					s.store.EnqueueRepo(ctx, repoID, 100)
					s.store.AddRepoToGroupByID(ctx, groupID, repoID)
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
		_ = s.store.RemoveRepoFromGroup(r.Context(), sess.UserID, groupID, repoID)
	}
	http.Redirect(w, r, fmt.Sprintf("/groups/%d", groupID), http.StatusFound)
}

// handleCompare renders the comparison page for up to 5 repos.
// Repos are specified by ID in the query string: /compare?repos=1,2,3,4,5
func (s *Server) handleCompare(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)

	// Get the user's groups and their repos for the search dropdown.
	groups, _ := s.store.GetUserGroups(r.Context(), sess.UserID)

	s.tmpl.ExecuteTemplate(w, "compare", map[string]interface{}{
		"Session": sess,
		"Groups":  groups,
		"RepoIDs": r.URL.Query().Get("repos"),
	})
}

// handleSBOMDownload generates and returns an SBOM for a repo.
// Only available to authenticated users who own the group containing the repo.
func (s *Server) handleSBOMDownload(w http.ResponseWriter, r *http.Request, sess *Session, groupID int64, repoIDStr string) {
	repoID, err := strconv.ParseInt(repoIDStr, 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	// Verify the user owns this group.
	if _, _, err := s.store.GetGroupDetail(r.Context(), sess.UserID, groupID, 1, 1, ""); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
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
	if err != nil {
		http.Error(w, "SBOM generation failed: "+err.Error(), http.StatusInternalServerError)
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

	// Verify group ownership.
	group, _, err := s.store.GetGroupDetail(r.Context(), sess.UserID, groupID, 1, 1, "")
	if err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	// Get repo details.
	repo, err := s.store.GetRepoByID(r.Context(), repoID)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	// Get stats.
	stats, _ := s.store.GetRepoStats(r.Context(), repoID)

	s.tmpl.ExecuteTemplate(w, "repo_detail", map[string]interface{}{
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

	stats, _ := s.store.QueueStats(r.Context())
	jobs, total, _ := s.store.ListQueuePage(r.Context(), monitorPageSize, offset, query)

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
	repos, _ := s.store.GetReposBatch(r.Context(), repoIDs)
	repoStats, _ := s.store.GetRepoStatsBatch(r.Context(), repoIDs)

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

	s.tmpl.ExecuteTemplate(w, "monitor", map[string]interface{}{
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
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	// Redirect back to the monitor page.
	http.Redirect(w, r, "/monitor", http.StatusFound)
}
