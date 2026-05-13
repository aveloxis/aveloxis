package web

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
)

// newTestServer builds a minimally-functional *Server suitable
// for handler tests in this package. It wires the embedded
// template set the same way New() does so handlers that call
// s.tmpl.ExecuteTemplate work. No DB, OAuth, or mailer is
// configured — handler tests touching those should provide
// fakes.
func newTestServer(t *testing.T) *Server {
	t.Helper()
	s := &Server{
		cfg:      config.WebConfig{},
		sessions: make(map[string]*Session),
	}
	s.tmpl = template.Must(template.New("").Funcs(template.FuncMap{
		"truncate": func(str string, n int) string {
			if len(str) <= n {
				return str
			}
			return str[:n] + "..."
		},
		"dict": func(values ...interface{}) map[string]interface{} {
			m := make(map[string]interface{})
			for i := 0; i < len(values)-1; i += 2 {
				m[values[i].(string)] = values[i+1]
			}
			return m
		},
		"add":      func(a, b int) int { return a + b },
		"subtract": func(a, b int) int { return a - b },
	}).Parse(allTemplates))
	return s
}

// silence unused import warning when the time package becomes
// unreferenced in a future refactor of this file.
var _ = time.Now

// v0.20.13: the login page now exposes a "Sign out" button when
// the visitor already has a session cookie. Use case: an operator
// wants to switch from user A to user B. Without this affordance
// they'd have to either know to navigate to /logout directly, or
// clear cookies manually in DevTools — a poor UX for what should
// be a one-click flow. The /logout handler already does the right
// thing (delete server-side session, expire the cookie); these
// tests pin (a) the login template surfaces the button only when
// a session is present, and (b) the handler defensively clears
// oauth_state too so any in-flight OAuth state from a partial
// login is wiped at the same time.

func TestLoginTemplateRendersSignOutWhenSessionPresent(t *testing.T) {
	srv := newTestServer(t)

	// Establish a session via the server's createSession helper
	// so the cookie's value matches a real entry in the session
	// map (otherwise getSession returns nil and the template
	// branch wouldn't fire).
	token := srv.createSession(1, "alice", "", "github", false)

	req := httptest.NewRequest(http.MethodGet, "/login", nil)
	req.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: token})
	rr := httptest.NewRecorder()

	srv.handleLogin(rr, req)

	body := rr.Body.String()
	// The button text and the form/link target are both load-
	// bearing. We pin both substrings.
	if !strings.Contains(body, "/logout") {
		t.Errorf("login page with active session must include a link to /logout so the visitor can clear their session before signing in as a different user. Body:\n%s", body)
	}
	// Pin descriptive text so the affordance is discoverable.
	// "Sign out" or "Clear" must appear in the rendered HTML.
	if !strings.Contains(body, "Sign out") && !strings.Contains(body, "Clear") {
		t.Errorf(`login page with active session must show "Sign out" or "Clear" text on the button so it's discoverable. Body:\n%s`, body)
	}
}

func TestLoginTemplateOmitsSignOutWhenNoSession(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/login", nil)
	// No session cookie.
	rr := httptest.NewRecorder()
	srv.handleLogin(rr, req)

	body := rr.Body.String()
	// With no session, the button has no purpose. It should be
	// hidden so the page stays focused on the OAuth sign-in CTAs.
	if strings.Contains(body, "/logout") {
		t.Errorf("login page with NO session must not include a /logout link — the button has no purpose for a fresh visitor and would clutter the OAuth sign-in flow. Body:\n%s", body)
	}
}

func TestLogoutHandlerExpiresOAuthStateCookie(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/logout", nil)
	rr := httptest.NewRecorder()
	srv.handleLogout(rr, req)

	cookies := rr.Result().Cookies()
	var sawSessionExpire, sawOAuthStateExpire bool
	for _, c := range cookies {
		// Expired cookies are emitted with MaxAge<0 (or Max-Age=0).
		// Either way the browser drops them. We accept both shapes.
		expired := c.MaxAge < 0 || c.MaxAge == 0
		if c.Name == "aveloxis_session" && expired {
			sawSessionExpire = true
		}
		if c.Name == "oauth_state" && expired {
			sawOAuthStateExpire = true
		}
	}

	if !sawSessionExpire {
		t.Error("handleLogout must emit an expire Set-Cookie for aveloxis_session — that's the whole point of the route")
	}
	if !sawOAuthStateExpire {
		t.Error("handleLogout must defensively expire oauth_state too — a partial-login attempt (where the user reached /auth/github but didn't complete the callback) leaves an oauth_state cookie behind. Clearing it alongside the session means the 'switch users' flow leaves no stale state.")
	}
}
