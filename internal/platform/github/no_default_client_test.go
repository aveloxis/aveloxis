// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// summary/18 Phase 1d (v0.27.37): http.DefaultClient has NO timeout —
// the codebase's #1 documented hang class (curl v0.27.5, OSV v0.27.23,
// tools.go, and the bespoke graphqlRequest this release deleted, which
// the v0.18.1 changelog wrongly believed was already gone). Every HTTP
// call in the github package must ride the shared HTTPClient.

package github

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNoDefaultClientInGitHubPackage(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(src), "http.DefaultClient") {
			t.Errorf("%s uses http.DefaultClient — no timeout, invisible to the KeyPool and the error taxonomy; route through the shared HTTPClient", name)
		}
		// Phase 5c tripwire (5): no bespoke GraphQL path. The single
		// choke point HTTPClient.GraphQL owns the endpoint, the errors
		// array, retries, and key accounting — a hardcoded endpoint
		// means a second path that will skip all of that (the v0.18.1
		// graphqlRequest survivor pattern, deleted in v0.27.37).
		if strings.Contains(string(src), "api.github.com/graphql") {
			t.Errorf("%s hardcodes the GraphQL endpoint — all GraphQL traffic must ride HTTPClient.GraphQL", name)
		}
	}
}
