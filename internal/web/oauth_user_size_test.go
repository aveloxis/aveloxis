// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestOAuthUserErrorAnswerIsNotASizeMark — v0.29.71 fix review V3 (the
// class of whole-branch review F5): the OAuth user reads noted their size
// before the status check, so a 401 "Bad credentials" body could be a
// source's first response-size high-water mark. Only a 200 is a data
// answer, for both forges.
func TestOAuthUserErrorAnswerIsNotASizeMark(t *testing.T) {
	refused := func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login/oauth/access_token", "/oauth/token":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"access_token":"tok","token_type":"bearer"}`)
		case "/user", "/api/v4/user":
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"message":"Bad credentials"}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
	check := func(t *testing.T, forge string, logs *bytes.Buffer, rec *httptest.ResponseRecorder) {
		t.Helper()
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("%s: a refused user read answered %d; want 502 (the fixture's premise); log:\n%s", forge, rec.Code, logs.String())
		}
		if strings.Contains(logs.String(), "response size high-water mark") {
			t.Errorf("%s: a 401 body was noted as a data response size:\n%s", forge, logs.String())
		}
	}

	platform.ResetResponseSizeMarksForTest(t, "oauth-github-user", "oauth-gitlab-user")
	// Subtests: the GitHub fixture refuses http.DefaultTransport until its
	// test ends, and the GitLab callback uses it.
	t.Run("github", func(t *testing.T) {
		ctx, _ := githubCallbackFixture(t, refused)
		var logs bytes.Buffer
		s := New(nil, config.WebConfig{DevMode: true, GitHubClientID: "id", GitHubClientSecret: "secret"}, nil, "", slog.New(slog.NewTextHandler(&logs, nil)))
		rec := httptest.NewRecorder()
		s.handleGitHubCallback(rec, githubCallbackRequest(ctx))
		check(t, "github", &logs, rec)
	})
	t.Run("gitlab", func(t *testing.T) {
		forge := httptest.NewServer(http.HandlerFunc(refused))
		t.Cleanup(forge.Close)
		var logs bytes.Buffer
		s := New(nil, config.WebConfig{DevMode: true, GitLabClientID: "id", GitLabClientSecret: "secret", GitLabBaseURL: forge.URL}, nil, "", slog.New(slog.NewTextHandler(&logs, nil)))
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/auth/gitlab/callback?code=c&state=st", nil)
		req.AddCookie(&http.Cookie{Name: "oauth_state", Value: "st"})
		s.handleGitLabCallback(rec, req)
		check(t, "gitlab", &logs, rec)
	})
}
