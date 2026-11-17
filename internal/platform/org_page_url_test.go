// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import "testing"

// An organization (or user) page of a forge is what people paste when they
// mean "all of it". It is not a repository URL; the add form says so by
// name instead of "not a repository URL" (0.29.146).
func TestIsForgeOwnerPageURL(t *testing.T) {
	for raw, want := range map[string]bool{
		"https://github.com/superelastix/":     true,
		"https://github.com/superelastix":      true,
		"github.com/superelastix":              true, // the form supplies the scheme
		"https://gitlab.com/gitlab-org":        true,
		"http://github.com/chaoss/":            true,
		"https://GitHub.com/CHAOSS":            true,
		"https://github.com/chaoss/augur":      false, // a repository
		"https://github.com/chaoss/augur/pull": false,
		"https://github.com/":                  false, // no owner
		"https://github.com":                   false,
		"https://git.example.invalid/team":     false, // not a forge this build recognizes by host
		"ftp://github.com/chaoss":              false,
		"https://user:pw@github.com/chaoss":    false, // credentials are refused by their own message
		"":                                     false,
		"not a url at all":                     false,
		"https://github.com/chaoss?tab=repos":  true,
		"https://github.com/orgs/chaoss":       false, // two segments: parsed as owner/repo elsewhere
	} {
		if got := IsForgeOwnerPageURL(raw); got != want {
			t.Errorf("IsForgeOwnerPageURL(%q) = %v, want %v", raw, got, want)
		}
	}
}
