// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// History: this file used to pin the npm-CLI argument-injection
// protections (the `--` separator and the dash-prefix rejection on
// `npm view`). v0.27.19 removed the npm CLI entirely — it was never
// installed on collection hosts, so EVERY npm dependency silently
// failed to resolve since inception — and replaced it with a plain
// HTTP call to registry.npmjs.org. Argument injection is structurally
// impossible over HTTP with a path-escaped name, so the old pins are
// replaced by this one: the CLI must never come back.
//
// The HTTP path's own protections are pinned in
// libyear_registry_test.go (path-escaping of scoped names, behavioral
// httptest coverage, and the AVELOXIS_TEST_NETWORK live canary).
func TestNPMResolutionIsHTTPNotCLI(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	for _, needle := range []string{`"npm", "view"`, `"npm","view"`} {
		if strings.Contains(code, needle) {
			t.Fatalf("analysis.go execs the npm CLI again (%q) — it is not installed on collection hosts (the since-inception zero-npm-rows bug) and reintroduces the argument-injection surface the HTTP path eliminated", needle)
		}
	}
	if !strings.Contains(code, "npmRegistryBase") {
		t.Error("resolveNPMLibyear must resolve via the npm registry HTTP API (npmRegistryBase)")
	}
}
