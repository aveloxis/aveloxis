// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestOAuthCallbacksAreBounded pins batch 7c review round 3: oauth2 builds
// on http.DefaultClient (no timeout), so each callback wraps the request
// context with oauthCallbackTimeout and hands THAT ctx to the exchange and
// the client — never r.Context() — so a stalled forge cannot hold the
// callback for as long as the browser waits. The bound itself is proven
// by the runtime tests (TestGitLabCallbackUserReadIsBounded,
// TestGitHubCallbackUserReadIsBounded — round 6, after a wrapper, a request
// literal and a Background ctx escaped this list); this pin holds the
// structural facts — the WithTimeout wrap is present and r.Context() is
// never handed on — and bans the spellings rounds 4–5 met. The
// /user/emails read (fetchGitHubPrimaryEmail) has its DB-tier twin,
// TestGitHubCallbackEmailsReadIsBounded (round 7: reaching it completes the
// login, which needs a store).
func TestOAuthCallbacksAreBounded(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/web/server.go"))
	for _, fn := range []struct {
		sig     string
		handler bool // wraps r.Context() itself; the helper receives the bounded ctx
	}{
		{"func (s *Server) handleGitHubCallback(", true},
		{"func (s *Server) handleGitLabCallback(", true},
		{"func fetchGitHubPrimaryEmail(", false},
	} {
		body := srctest.FuncBody(t, src, fn.sig)
		if fn.handler && !strings.Contains(body, "context.WithTimeout(r.Context(), oauthCallbackTimeout)") {
			t.Errorf("%s must bound its forge round trips with context.WithTimeout(r.Context(), oauthCallbackTimeout)", fn.sig)
		}
		for _, bad := range []string{".Exchange(r.Context()", ".Client(r.Context()", "NewRequestWithContext(r.Context()"} {
			if strings.Contains(body, bad) {
				t.Errorf("%s hands the unbounded r.Context() to %s — use the bounded ctx", fn.sig, bad)
			}
		}
		// The bound travels on the REQUEST: oauth2's client has Timeout 0
		// and uses ctx to refresh a token and to select the base client
		// (oauth2.HTTPClient), never as a per-request deadline, so client.Get/Post run
		// unbounded (round 4: the GitLab /user read did), and a request
		// built with http.NewRequest carries context.Background (round 5).
		for _, bad := range []string{"client.Get(", "client.Post(", ".Get(glBase", ".Get(\"https://", "http.NewRequest("} {
			if strings.Contains(body, bad) {
				t.Errorf("%s reads the forge with %s — build the request with NewRequestWithContext(ctx, …) and client.Do", fn.sig, bad)
			}
		}
	}
}
