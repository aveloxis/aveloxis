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
// callback for as long as the browser waits.
func TestOAuthCallbacksAreBounded(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/web/server.go"))
	for _, fn := range []string{"func (s *Server) handleGitHubCallback(", "func (s *Server) handleGitLabCallback("} {
		body := srctest.FuncBody(t, src, fn)
		if !strings.Contains(body, "context.WithTimeout(r.Context(), oauthCallbackTimeout)") {
			t.Errorf("%s must bound its forge round trips with context.WithTimeout(r.Context(), oauthCallbackTimeout)", fn)
		}
		for _, bad := range []string{".Exchange(r.Context()", ".Client(r.Context()", "NewRequestWithContext(r.Context()"} {
			if strings.Contains(body, bad) {
				t.Errorf("%s hands the unbounded r.Context() to %s — use the bounded ctx", fn, bad)
			}
		}
	}
}
