// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
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
		{name: "mail on, no site_url, same-host proxy, dev_mode off", m: mailOn(""), host: "127.0.0.1:8082", want: false},
		{name: "mail on, no site_url, loopback, dev_mode on", m: mailOn(""), devMode: true, host: "localhost:8082", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := toForm(t, get(t, store, tc.m, tc.devMode, tc.host)); got != tc.want {
				t.Errorf("redirect to /account/email = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("a pending address renders the dashboard", func(t *testing.T) {
		if err := store.SetUserPendingEmail(ctx, uid, "p@example.com"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.ClearUserPendingEmailIf(ctx, uid, "p@example.com") })
		if toForm(t, get(t, store, mailOn("https://aveloxis.io"), false, "aveloxis.io")) {
			t.Error("a user with a pending address must see the dashboard, not the form")
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
