// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"strings"
	"testing"
)

// FuzzRepoURLParsers drives the forge-URL parsers on arbitrary input.
// These consume USER-PASTED strings (bulk-paste box, add-org form, CLI
// add-repo) — the purest untrusted input in the system, and the site of
// two real incident classes: case-variant duplicate repos (v0.25.32) and
// the schemeless org_url registrations that silently defeated every
// prefix matcher (v0.27.94).
//
// Contracts fuzzed: never panic; and ParseOrgURL's documented
// postconditions — on success the host is lowercase and the org name is
// non-empty (both load-bearing: LOWER-based matching everywhere, and an
// empty org would enumerate garbage API paths).
func FuzzRepoURLParsers(f *testing.F) {
	f.Add("https://github.com/chaoss/augur")
	f.Add("github.com/kubernetes")
	f.Add("https://GitHub.com/Azure/azure-sdk-for-java.git/")
	f.Add("https://gitlab.com/petsc/petsc")
	f.Add("git@github.com:owner/repo.git")
	f.Add("-flag-injection://x")
	f.Add("https://github.com/a_b/c%2Fd")
	f.Add("://///\x00")
	f.Fuzz(func(t *testing.T, raw string) {
		_, _ = ParseRepoURL(raw)
		_, _ = ParseAnyRepoURL(raw)
		host, org, err := ParseOrgURL(raw)
		if err == nil {
			if host != strings.ToLower(host) {
				t.Fatalf("ParseOrgURL(%q) host %q not lowercase — LOWER-based "+
					"org matching (IsOrgRegisteredAnywhere, ReconcileOrgRepoLinks) "+
					"depends on it", raw, host)
			}
			if org == "" {
				t.Fatalf("ParseOrgURL(%q) succeeded with empty org name", raw)
			}
		}
	})
}
