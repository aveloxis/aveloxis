// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/smtp"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailer"
)

// TestDashboardEmailGateThroughTheHandler (AVELOXIS_TEST_DB) drives GET
// /dashboard through the real handler, session and store, and asserts the
// response — the property two source pins failed to hold (a string count,
// then an AST pin, both escaped with every test green). A user without an
// address is sent to /account/email only where a confirmation can be sent;
// a lookup error never sends anyone there. No mail is sent on this path.
func TestDashboardEmailGateThroughTheHandler(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	const login = "_avdashboard_gate_probe"
	clean := func() {
		_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.email_confirmations WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}

	mailOn := func(site string) *mailer.Mailer {
		return mailer.New(mailer.Config{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop", SiteURL: site}, nil)
	}
	get := func(t *testing.T, st *db.PostgresStore, m *mailer.Mailer, devMode bool, host string) *httptest.ResponseRecorder {
		t.Helper()
		s := New(st, config.WebConfig{DevMode: devMode}, nil, logger)
		if m != nil {
			s.WithMailer(m)
		}
		s.sessions["probe-token"] = &Session{UserID: uid, LoginName: login, ExpiresAt: time.Now().Add(time.Hour)}
		r := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
		r.Host = host
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "probe-token"})
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	// Sent to the form: a 302 whose target PATH is /account/email (any
	// query). Not sent: the dashboard renders — 200, no redirect of any
	// kind, so no spelling of a redirect can slip past.
	toForm := func(t *testing.T, w *httptest.ResponseRecorder) bool {
		t.Helper()
		if w.Code == http.StatusOK {
			return false
		}
		u, err := url.Parse(w.Header().Get("Location"))
		if w.Code != http.StatusFound || err != nil || u.Path != "/account/email" {
			t.Fatalf("GET /dashboard = %d Location %q; want 200 or a redirect to /account/email", w.Code, w.Header().Get("Location"))
		}
		return true
	}

	for _, tc := range []struct {
		name    string
		m       *mailer.Mailer
		devMode bool
		host    string
		want    bool
	}{
		{name: "no mailer", host: "aveloxis.io", want: false},
		{name: "mail on, site_url set", m: mailOn("https://aveloxis.io"), host: "aveloxis.io", want: true},
		// A site_url that cannot start a link disables the mailer (Copilot
		// review of PR #207 on eb248eb: these sent users to the form, then
		// mailed a link that could not confirm).
		{name: "mail on, site_url without a scheme", m: mailOn("aveloxis.io"), host: "aveloxis.io", want: false},
		{name: "mail on, site_url with a query", m: mailOn("https://aveloxis.io?x=1"), host: "aveloxis.io", want: false},
		{name: "mail on, no site_url, same-host proxy, dev_mode off", m: mailOn(""), host: "127.0.0.1:8082", want: false},
		{name: "mail on, no site_url, loopback, dev_mode on", m: mailOn(""), devMode: true, host: "localhost:8082", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := toForm(t, get(t, store, tc.m, tc.devMode, tc.host)); got != tc.want {
				t.Errorf("redirect to /account/email = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("a live pending address renders the dashboard with its banner", func(t *testing.T) {
		if err := store.SetUserPendingEmail(ctx, uid, "p@example.com"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.CreateEmailConfirmation(ctx, uid, "p@example.com"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = store.ClearUserPendingEmailIf(ctx, uid, "p@example.com")
			_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.email_confirmations WHERE user_id = $1`, uid)
		})
		w := get(t, store, mailOn("https://aveloxis.io"), false, "aveloxis.io")
		if toForm(t, w) {
			t.Fatal("a user with a live pending address must see the dashboard, not the form")
		}
		if !strings.Contains(w.Body.String(), "Check your inbox to confirm your email") || !strings.Contains(w.Body.String(), "p@example.com") {
			t.Error("the dashboard must show the pending-address banner")
		}
		// The banner states how long links are valid, not a countdown it
		// cannot keep: it renders on every visit while the address is
		// pending (round-10 review: "expires in 24 hours" was shown 23h
		// later too).
		if want := "Confirmation links are valid for " + mailer.DurationPhrase(db.EmailConfirmationLifetime) + " after they are sent."; !strings.Contains(w.Body.String(), want) {
			t.Errorf("the banner must say %q", want)
		}
		if strings.Contains(w.Body.String(), "The link expires in") {
			t.Error("the banner must not count down from the full lifetime")
		}
	})

	t.Run("an expired link no longer counts as pending", func(t *testing.T) {
		if err := store.SetUserPendingEmail(ctx, uid, "x@example.com"); err != nil {
			t.Fatal(err)
		}
		tok, err := store.CreateEmailConfirmation(ctx, uid, "x@example.com")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = store.ClearUserPendingEmailIf(ctx, uid, "x@example.com")
			_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.email_confirmations WHERE user_id = $1`, uid)
		})
		if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.email_confirmations SET expires_at = NOW() - INTERVAL '1 minute' WHERE token = $1`, tok); err != nil {
			t.Fatal(err)
		}
		if !toForm(t, get(t, store, mailOn("https://aveloxis.io"), false, "aveloxis.io")) {
			t.Error("with only an expired link, the user must be sent to the form, not told to click it")
		}
	})

	t.Run("an anonymous request is sent to login", func(t *testing.T) {
		s := New(store, config.WebConfig{}, nil, logger)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/dashboard", nil))
		if w.Code != http.StatusFound || w.Header().Get("Location") != "/login" {
			t.Errorf("anonymous GET /dashboard = %d %q, want 302 /login", w.Code, w.Header().Get("Location"))
		}
	})

	t.Run("a lookup error never sends the user to the form", func(t *testing.T) {
		broken, err := db.NewPostgresStore(ctx, dsn, logger)
		if err != nil {
			t.Fatal(err)
		}
		broken.Close()
		if w := get(t, broken, mailOn("https://aveloxis.io"), false, "aveloxis.io"); toForm(t, w) {
			t.Errorf("a failed lookup redirected to the form (status %d)", w.Code)
		}
	})
}

// TestAccountEmailFlowThroughTheHandler (AVELOXIS_TEST_DB) drives POST
// /account/email and GET /account/email/confirm through the real handler,
// session and store, with the mailer's send captured by WithSendFunc so
// nothing can dial SMTP even under a mutation (round-6 review: the POST
// half of the shared policy was held only by a text pin, and a shadowed
// server with dev_mode on passed it).
func TestAccountEmailFlowThroughTheHandler(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	logins := []string{"_avaccount_email_flow_a", "_avaccount_email_flow_b"}
	clean := func() {
		for _, l := range logins {
			_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.email_confirmations WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, l)
			_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, l)
		}
	}
	clean()
	t.Cleanup(clean)
	uidA, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: logins[0], Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	uidB, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: logins[1], Provider: "gitlab"})
	if err != nil {
		t.Fatal(err)
	}

	type capture struct{ links []string }
	server := func(site string, devMode bool, sendErr error, c *capture) *Server {
		m := mailer.New(mailer.Config{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop", SiteURL: site}, nil).
			WithSendFunc(func(_ string, _ smtp.Auth, _ string, _ []string, msg []byte) error {
				for _, line := range strings.Split(string(msg), "\n") {
					if strings.Contains(line, "/account/email/confirm?token=") {
						c.links = append(c.links, strings.TrimSpace(line))
					}
				}
				return sendErr
			})
		s := New(store, config.WebConfig{DevMode: devMode}, nil, logger).WithMailer(m)
		s.sessions["a"] = &Session{UserID: uidA, LoginName: logins[0], ExpiresAt: time.Now().Add(time.Hour)}
		s.sessions["b"] = &Session{UserID: uidB, LoginName: logins[1], ExpiresAt: time.Now().Add(time.Hour)}
		return s
	}
	do := func(s *Server, method, target, host, session, form string) *httptest.ResponseRecorder {
		var body io.Reader
		if form != "" {
			body = strings.NewReader(url.Values{"email": {form}}.Encode())
		}
		r := httptest.NewRequest(method, target, body)
		if form != "" {
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
		r.Host = host
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: session})
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	reset := func() {
		_, _ = store.Pool().Exec(ctx, `UPDATE aveloxis_ops.users SET email = NULL, email_pending = NULL WHERE user_id = ANY($1)`, []int{uidA, uidB})
		_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.email_confirmations WHERE user_id = ANY($1)`, []int{uidA, uidB})
	}

	t.Run("same-host proxy without site_url or dev_mode is refused before storing", func(t *testing.T) {
		reset()
		c := &capture{}
		w := do(server("", false, nil, c), http.MethodPost, "/account/email", "127.0.0.1:8082", "a", "user@example.com")
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Email confirmation is not configured on this site") {
			t.Errorf("POST = %d, want the refusal form", w.Code)
		}
		if len(c.links) != 0 {
			t.Errorf("mailed %q, want nothing", c.links)
		}
		if got, _ := store.GetUserLivePendingEmail(ctx, uidA); got != "" {
			t.Errorf("stored a pending address %q on refusal", got)
		}
	})

	t.Run("site_url set: the link uses it whatever the Host", func(t *testing.T) {
		reset()
		c := &capture{}
		w := do(server("https://aveloxis.io", false, nil, c), http.MethodPost, "/account/email", "evil.example.com", "a", "user@example.com")
		if w.Code != http.StatusFound || w.Header().Get("Location") != "/dashboard" {
			t.Fatalf("POST = %d %q, want 302 /dashboard", w.Code, w.Header().Get("Location"))
		}
		if len(c.links) != 1 || !strings.HasPrefix(c.links[0], "https://aveloxis.io/account/email/confirm?token=") {
			t.Errorf("mailed links %q, want one site_url link", c.links)
		}
	})

	t.Run("dev_mode loopback builds a loopback link", func(t *testing.T) {
		reset()
		c := &capture{}
		w := do(server("", true, nil, c), http.MethodPost, "/account/email", "localhost:8082", "a", "user@example.com")
		if w.Code != http.StatusFound || len(c.links) != 1 || !strings.HasPrefix(c.links[0], "http://localhost:8082/account/email/confirm?token=") {
			t.Errorf("POST = %d, links %q", w.Code, c.links)
		}
	})

	t.Run("a failed send says so and leaves no live pending address", func(t *testing.T) {
		reset()
		c := &capture{}
		w := do(server("https://aveloxis.io", false, errors.New("535 rejected"), c), http.MethodPost, "/account/email", "aveloxis.io", "a", "user@example.com")
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "couldn") {
			t.Errorf("POST = %d, want the form with the send-failure message", w.Code)
		}
		if got, _ := store.GetUserLivePendingEmail(ctx, uidA); got != "" {
			t.Errorf("pending address %q survived a failed send", got)
		}
	})

	t.Run("another account's click does not burn the link, and the owner can still confirm", func(t *testing.T) {
		reset()
		c := &capture{}
		s := server("https://aveloxis.io", false, nil, c)
		do(s, http.MethodPost, "/account/email", "aveloxis.io", "a", "a@example.com")
		if len(c.links) != 1 {
			t.Fatalf("mailed %q, want one link", c.links)
		}
		u, _ := url.Parse(c.links[0])
		confirm := u.RequestURI()
		var logs strings.Builder
		s.logger = slog.New(slog.NewTextHandler(&logs, nil))
		w := do(s, http.MethodGet, confirm, "aveloxis.io", "b", "")
		if w.Code != http.StatusFound || w.Header().Get("Location") != "/account/email?expired=1" {
			t.Errorf("B's click = %d %q, want 302 /account/email?expired=1", w.Code, w.Header().Get("Location"))
		}
		// A replay of another account's link is logged as one, with its owner.
		if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), fmt.Sprintf("token_user_id=%d", uidA)) {
			t.Errorf("another account's click must be a WARN naming the token's owner; log:\n%s", logs.String())
		}
		if got, _ := store.GetUserLivePendingEmail(ctx, uidA); got != "a@example.com" {
			t.Errorf("A's link must still be live after B's click, pending = %q", got)
		}
		w = do(s, http.MethodGet, confirm, "aveloxis.io", "a", "")
		if w.Code != http.StatusFound || w.Header().Get("Location") != "/dashboard" {
			t.Errorf("A's click = %d %q, want 302 /dashboard", w.Code, w.Header().Get("Location"))
		}
		if got, _ := store.GetUserEmail(ctx, uidA); got != "a@example.com" {
			t.Errorf("A's email after confirming = %q", got)
		}
	})

	t.Run("the form explains an expired link and a failed confirmation", func(t *testing.T) {
		reset()
		s := server("https://aveloxis.io", false, nil, &capture{})
		if w := do(s, http.MethodGet, "/account/email?expired=1", "aveloxis.io", "a", ""); !strings.Contains(w.Body.String(), "expired") {
			t.Errorf("GET ?expired=1 does not explain the expired link")
		}
		if w := do(s, http.MethodGet, "/account/email?error=1", "aveloxis.io", "a", ""); !strings.Contains(w.Body.String(), "couldn") {
			t.Errorf("GET ?error=1 does not explain the failed confirmation")
		}
	})
}
