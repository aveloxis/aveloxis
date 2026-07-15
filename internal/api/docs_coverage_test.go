// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.4 tripwire (operator ask: "are we documenting all the new API
// endpoints we are creating?"): every route registered in server.go
// must appear in docs/guide/api.md. Same philosophy as the v0.25.35
// commands.md coverage test — adding an endpoint forces the
// same-commit doc update.
//
// Scope note: the Augur-compatibility metric routes registered in
// metrics.go are documented as a route TABLE in api.md (v0.25.35) and
// follow Augur's swagger spec, not ours — they are deliberately
// outside this tripwire.

import (
	"regexp"
	"strings"
	"testing"
)

var placeholderRe = regexp.MustCompile(`\{[^}]+\}`)

func TestAPIDocsCoverEveryRoute(t *testing.T) {
	src := mustReadFile(t, "server.go")
	docs := mustReadFile(t, "../../docs/guide/api.md")
	docsNorm := placeholderRe.ReplaceAllString(docs, "{}")

	routeRe := regexp.MustCompile(`HandleFunc\("(?:GET|POST|PUT|DELETE) ([^"]+)"`)
	seen := map[string]bool{}
	for _, m := range routeRe.FindAllStringSubmatch(src, -1) {
		route := m[1]
		if seen[route] || route == "/{$}" {
			continue
		}
		seen[route] = true
		needle := strings.TrimSuffix(placeholderRe.ReplaceAllString(route, "{}"), "/")
		if !strings.Contains(docsNorm, needle) {
			t.Errorf("route %q is registered in server.go but not documented in docs/guide/api.md — document every endpoint in the same commit that adds it (path parameter names may differ; only the path shape must match)", route)
		}
	}
	if len(seen) < 25 {
		t.Fatalf("route extraction found only %d routes — the regex probably broke; fix it rather than letting the tripwire go blind", len(seen))
	}
}
