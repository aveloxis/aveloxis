// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"os"
	"strings"
	"testing"
)

// TestNoCORSWildcard verifies that the API server does not use
// Access-Control-Allow-Origin: * which allows any website the operator
// visits to read collected data via fetch() from any origin.
func TestNoCORSWildcard(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if strings.Contains(code, `Allow-Origin", "*"`) {
		t.Error("API server must not use Access-Control-Allow-Origin: * — " +
			"this allows any website the operator visits to read all collected data. " +
			"Use a configurable origin or restrict to localhost.")
	}
}

// TestCORSCentralized (v0.27.1) replaces the old
// TestCORSLocalhostOnlyFunction: per-handler CORS (the
// setCORSIfLocalhost helper and jsonResponse's wildcard) is GONE —
// the middleware in ratelimit.go is the single CORS authority, so
// the cors_origins allowlist cannot be bypassed route-by-route.
func TestCORSCentralized(t *testing.T) {
	for _, f := range []string{"server.go", "contributions.go", "metrics.go"} {
		src := mustReadFile(t, f)
		if strings.Contains(src, "setCORSIfLocalhost") {
			t.Errorf("%s still references setCORSIfLocalhost — per-handler CORS bypasses the allowlist", f)
		}
		if strings.Contains(src, `Set("Access-Control-Allow-Origin"`) {
			t.Errorf("%s sets Access-Control-Allow-Origin directly — only the middleware may", f)
		}
	}
}
