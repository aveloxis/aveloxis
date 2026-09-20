// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// v0.29.57 (Copilot review 5260961848 on PR #210 named the web org scan;
// this is the CLI sibling found from the dispatch point): `add-repo` on an
// org URL decides GitHub-or-not by comparing the host against the web host of
// the deployment's configured GitHub API base, not the literal "github.com" —
// on an Enterprise deployment the org lives on that host, and an org on
// public GitHub is not one the deployment's keys can enumerate.
func TestIsOrgURLUsesTheConfiguredGitHubHost(t *testing.T) {
	const enterprise = "https://ghe.example.invalid/api/v3"
	for _, tc := range []struct {
		url, ghBase string
		wantOrg     bool
		wantHost    string
		wantName    string
	}{
		{"https://github.com/chaoss", "", true, "github.com", "chaoss"},
		{"https://GitHub.com/chaoss/", "https://api.github.com", true, "github.com", "chaoss"},
		{"https://github.com/chaoss/augur", "", false, "", ""},
		{"https://ghe.example.invalid/chaoss", enterprise, true, "ghe.example.invalid", "chaoss"},
		{"https://ghe.example.invalid/chaoss", "", false, "", ""},
		{"https://github.com/chaoss", enterprise, false, "", ""},
		// A schemeless argument has no host: it is not an org on any
		// deployment, including one whose base is not a URL (both hosts
		// empty — round 1 of the 5260961848 fixes).
		{"chaoss", "", false, "", ""},
		{"chaoss", "://bad", false, "", ""},
		{"https://github.com/chaoss", "://bad", false, "", ""},
		{"chaoss", "https://:443/api/v3", false, "", ""},
	} {
		isOrg, host, name, plat := isOrgURL(tc.url, tc.ghBase)
		if isOrg != tc.wantOrg {
			t.Errorf("isOrgURL(%q, base %q) org = %v, want %v", tc.url, tc.ghBase, isOrg, tc.wantOrg)
			continue
		}
		if !isOrg {
			continue
		}
		if name != tc.wantName || host != tc.wantHost || plat != model.PlatformGitHub {
			t.Errorf("isOrgURL(%q, base %q) = (%q, %q, %v), want (%q, %q, GitHub)", tc.url, tc.ghBase, host, name, plat, tc.wantHost, tc.wantName)
		}
	}
}
