// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/smtp"
	"os"
	"sync/atomic"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailer"
)

// TestWelcomeMailFollowsTheForgeIdentity (final review round 2 F2): the
// first-signup probe counted rows by login_name while the account is found
// by the forge's user ID, so a user who renamed on the forge was "new" —
// and got a welcome mail — at every sign-in. A welcome goes to a new
// identity once, never to a returning one under a new name, including when
// the new name is held by another account (the name cannot follow).
func TestWelcomeMailFollowsTheForgeIdentity(t *testing.T) {
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
	clean := func() {
		if _, err := store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name LIKE '\_avwel\_%'`); err != nil {
			t.Logf("cleanup: %v", err)
		}
	}
	clean()
	t.Cleanup(clean)

	var welcomes atomic.Int32
	m := mailer.New(mailer.Config{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop"}, nil).
		WithSendFunc(func(string, smtp.Auth, string, []string, []byte) error {
			welcomes.Add(1)
			return nil
		})
	s := New(store, config.WebConfig{}, nil, "", logger).WithMailer(m)
	login := func(info db.OAuthUserInfo) {
		t.Helper()
		info.Email = "person@example.com"
		rec := httptest.NewRecorder()
		s.completeOAuthLogin(rec, httptest.NewRequest(http.MethodGet, "/auth/github/callback", nil), info, info.Provider)
		if rec.Code != http.StatusFound {
			t.Fatalf("login %s answered %d: %s", info.Login, rec.Code, rec.Body.String())
		}
	}
	count := func(want int32, what string) {
		t.Helper()
		if got := welcomes.Swap(0); got != want {
			t.Errorf("%s: %d welcome mails; want %d", what, got, want)
		}
	}

	login(db.OAuthUserInfo{Login: "_avwel_new", GHUserID: 900401, Provider: "github"})
	count(1, "a first sign-in")
	login(db.OAuthUserInfo{Login: "_avwel_new", GHUserID: 900401, Provider: "github"})
	count(0, "the same user again")

	if _, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: "_avwel_bob", GHUserID: 900402, Provider: "github"}); err != nil {
		t.Fatal(err)
	}
	login(db.OAuthUserInfo{Login: "_avwel_robert", GHUserID: 900402, Provider: "github"})
	login(db.OAuthUserInfo{Login: "_avwel_robert", GHUserID: 900402, Provider: "github"})
	count(0, "a returning user renamed on the forge")

	for _, seed := range []db.OAuthUserInfo{
		{Login: "_avwel_held", GHUserID: 900403, Provider: "github"},
		{Login: "_avwel_carl", GLUserID: 900404, GLHost: "https://gitlab.com", Provider: "gitlab"},
	} {
		if _, err := store.UpsertOAuthUser(ctx, seed); err != nil {
			t.Fatal(err)
		}
	}
	// Guards a different property from the robert case (round 3 note): the
	// name cannot follow here, so the account keeps a name the forge no
	// longer uses — still no welcome.
	login(db.OAuthUserInfo{Login: "_avwel_held", GLUserID: 900404, Provider: "gitlab"})
	login(db.OAuthUserInfo{Login: "_avwel_held", GLUserID: 900404, Provider: "gitlab"})
	count(0, "a returning user renamed onto a name another account holds")

	// Round 3 F2: a new user on another GitLab instance whose numeric ID an
	// existing account carries is new — the store inserts — and is welcomed.
	login(db.OAuthUserInfo{Login: "_avwel_other_instance", GLUserID: 900404, GLHost: "https://gitlab.other.example", Provider: "gitlab"})
	count(1, "a new user on another GitLab instance with a known numeric ID")
}

// TestWebStartStampsLegacyGitLabAccounts (final review round 3): a GitLab
// account from before the instance was recorded matches no instance, so
// web start stamps such accounts with its configured instance.
func TestWebStartStampsLegacyGitLabAccounts(t *testing.T) {
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
	const login = "_avwel_legacy_gl"
	clean := func() {
		if _, err := store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login); err != nil {
			t.Logf("cleanup: %v", err)
		}
	}
	clean()
	t.Cleanup(clean)
	if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider, gl_user_id) VALUES ($1, 'gitlab', 900901)`, login); err != nil {
		t.Fatal(err)
	}
	s := New(store, config.WebConfig{GitLabClientID: "id", GitLabClientSecret: "secret", GitLabBaseURL: "https://GitLab.Example.org/"}, nil, "", logger)
	s.StampLegacyGitLabAccounts(ctx)
	var host string
	if err := store.Pool().QueryRow(ctx, `SELECT COALESCE(gl_oauth_host, '') FROM aveloxis_ops.users WHERE login_name = $1`, login).Scan(&host); err != nil {
		t.Fatal(err)
	}
	if want := db.GitLabOAuthHost("https://gitlab.example.org"); host != want {
		t.Errorf("gl_oauth_host = %q after web start; want %q", host, want)
	}
}

// TestGitLabCallbackRecordsItsInstance (final review round 2 F4): the
// callback hands the store the instance that answered, so a GitLab user ID
// is stored — and later matched — together with its host.
func TestGitLabCallbackRecordsItsInstance(t *testing.T) {
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
	const login = "_avwel_glhost"
	clean := func() {
		if _, err := store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login); err != nil {
			t.Logf("cleanup: %v", err)
		}
	}
	clean()
	t.Cleanup(clean)
	forge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth/token":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"tok","token_type":"bearer"}`))
		case "/api/v4/user":
			_, _ = w.Write([]byte(`{"id":900701,"username":"` + login + `","email":"gl@example.com"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(forge.Close)
	s := New(store, config.WebConfig{DevMode: true, GitLabClientID: "id", GitLabClientSecret: "secret", GitLabBaseURL: forge.URL + "/"}, nil, "", logger)
	req := httptest.NewRequest(http.MethodGet, "/auth/gitlab/callback?code=c&state=st", nil)
	req.AddCookie(&http.Cookie{Name: "oauth_state", Value: "st"})
	rec := httptest.NewRecorder()
	s.handleGitLabCallback(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("the GitLab callback answered %d: %s", rec.Code, rec.Body.String())
	}
	var host string
	var id int64
	if err := store.Pool().QueryRow(ctx, `SELECT COALESCE(gl_oauth_host, ''), COALESCE(gl_user_id, 0) FROM aveloxis_ops.users WHERE login_name = $1`, login).Scan(&host, &id); err != nil {
		t.Fatal(err)
	}
	if want := db.GitLabOAuthHost(forge.URL); host != want || id != 900701 {
		t.Errorf("stored gl_user_id %d on %q; want 900701 on %q", id, host, want)
	}
}
