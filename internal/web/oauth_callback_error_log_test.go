// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
)

// TestOAuthCallbackFailuresAreLogged (final whole-tree review F4,
// 2026-09-28): the token exchange and the user request failed without a
// log line on both callbacks — the new 30 s bound expired silently — and
// the exchange's raw error (the token endpoint's response text) was echoed
// to the browser. Every arm logs the provider, the phase and the error; the
// browser gets a fixed message.
func TestOAuthCallbackFailuresAreLogged(t *testing.T) {
	saved := oauthCallbackTimeout
	oauthCallbackTimeout = 200 * time.Millisecond
	t.Cleanup(func() { oauthCallbackTimeout = saved })
	const leak = "token_endpoint_private_detail"

	forgeHandler := func(tokenPath, userPath string, failExchange bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case tokenPath:
				if failExchange {
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprint(w, leak)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"access_token":"tok","token_type":"bearer"}`)
			case userPath:
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
				}
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}
	}

	for _, tc := range []struct {
		name, provider string
		failExchange   bool
		wantPhase      string
	}{
		{"github exchange", "github", true, "exchange"},
		{"github user read", "github", false, "user"},
		{"gitlab exchange", "gitlab", true, "exchange"},
		{"gitlab user read", "gitlab", false, "user"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			rec := httptest.NewRecorder()
			if tc.provider == "github" {
				ctx, _ := githubCallbackFixture(t, forgeHandler("/login/oauth/access_token", "/user", tc.failExchange))
				s := New(nil, config.WebConfig{DevMode: true, GitHubClientID: "id", GitHubClientSecret: "secret"}, nil, "", logger)
				s.handleGitHubCallback(rec, githubCallbackRequest(ctx))
			} else {
				forge := httptest.NewServer(forgeHandler("/oauth/token", "/api/v4/user", tc.failExchange))
				t.Cleanup(forge.Close)
				s := New(nil, config.WebConfig{DevMode: true, GitLabClientID: "id", GitLabClientSecret: "secret", GitLabBaseURL: forge.URL}, nil, "", logger)
				req := httptest.NewRequest(http.MethodGet, "/auth/gitlab/callback?code=c&state=st", nil)
				req.AddCookie(&http.Cookie{Name: "oauth_state", Value: "st"})
				s.handleGitLabCallback(rec, req.WithContext(context.Background()))
			}
			if rec.Code < 400 {
				t.Fatalf("the failing callback answered %d", rec.Code)
			}
			out := logs.String()
			if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "provider="+tc.provider) || !strings.Contains(out, "phase="+tc.wantPhase) {
				t.Errorf("the %s failure was not logged with provider and phase:\n%s", tc.name, out)
			}
			if strings.Contains(rec.Body.String(), leak) {
				t.Errorf("the browser was shown the raw exchange error: %q", rec.Body.String())
			}
		})
	}
}
