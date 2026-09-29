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
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
)

// TestOAuthCallbackUserBodyStallIsAUserReadFailure — PR #218 review C9: both
// callbacks read the /user body with `body, _ := io.ReadAll(resp.Body)`. A
// forge that sends the headers and then stalls the body hits the callback
// bound in the READ, whose error was dropped: the partial body went on to
// the JSON decode and the browser was told the forge's payload was invalid
// (a 502 logged as an unmarshal failure), never that the forge did not
// answer in time. The read error is the user-read failure: logged with the
// provider and phase, and the fixed browser message.
func TestOAuthCallbackUserBodyStallIsAUserReadFailure(t *testing.T) {
	saved := oauthCallbackTimeout
	oauthCallbackTimeout = 200 * time.Millisecond
	t.Cleanup(func() { oauthCallbackTimeout = saved })

	stallingForge := func(tokenPath, userPath string, bodyStarted *atomic.Int32) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case tokenPath:
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"access_token":"tok","token_type":"bearer"}`)
			case userPath:
				// Headers and the start of the body, then a stall.
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, `{"id":1,"lo`)
				w.(http.Flusher).Flush()
				bodyStarted.Add(1)
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
				}
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}
	}

	for _, provider := range []string{"github", "gitlab"} {
		t.Run(provider, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			var bodyStarted atomic.Int32
			rec := httptest.NewRecorder()
			start := time.Now()
			if provider == "github" {
				ctx, refuse := githubCallbackFixture(t, stallingForge("/login/oauth/access_token", "/user", &bodyStarted))
				s := New(nil, config.WebConfig{DevMode: true, GitHubClientID: "id", GitHubClientSecret: "secret"}, nil, "", logger)
				s.handleGitHubCallback(rec, githubCallbackRequest(ctx))
				if hosts := refuse.leaked(); len(hosts) > 0 {
					t.Fatalf("a request to %v bypassed the fixture", hosts)
				}
			} else {
				forge := httptest.NewServer(stallingForge("/oauth/token", "/api/v4/user", &bodyStarted))
				t.Cleanup(forge.Close)
				s := New(nil, config.WebConfig{DevMode: true, GitLabClientID: "id", GitLabClientSecret: "secret", GitLabBaseURL: forge.URL}, nil, "", logger)
				req := httptest.NewRequest(http.MethodGet, "/auth/gitlab/callback?code=c&state=st", nil)
				req.AddCookie(&http.Cookie{Name: "oauth_state", Value: "st"})
				s.handleGitLabCallback(rec, req.WithContext(context.Background()))
			}
			if took := time.Since(start); took > 2*time.Second {
				t.Fatalf("the callback took %s against a stalled body; want it to return at the %s bound", took, oauthCallbackTimeout)
			}
			// Pin the phase: the headers arrived and the body had started.
			if bodyStarted.Load() < 1 {
				t.Fatal("the fixture never started the /user body — the stalled read was not exercised")
			}
			if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "Failed to get user info") {
				t.Errorf("a stalled /user body = %d %q; want 500 \"Failed to get user info\"", rec.Code, strings.TrimSpace(rec.Body.String()))
			}
			out := logs.String()
			if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "provider="+provider) || !strings.Contains(out, "phase=user") {
				t.Errorf("the stalled read was not logged with provider and phase:\n%s", out)
			}
			if strings.Contains(out, "unmarshal failed") {
				t.Errorf("the stalled read was logged as a payload the forge sent:\n%s", out)
			}
		})
	}
}
