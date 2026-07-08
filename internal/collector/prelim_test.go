// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import "testing"

func TestNormalizeRepoURL(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"https://github.com/chaoss/augur", "github.com/chaoss/augur"},
		{"https://github.com/chaoss/augur/", "github.com/chaoss/augur"},
		{"https://github.com/chaoss/augur.git", "github.com/chaoss/augur"},
		{"http://github.com/CHAOSS/Augur", "github.com/chaoss/augur"},
		{"https://gitlab.com/group/project.git/", "gitlab.com/group/project"},
	}

	for _, tt := range tests {
		got := normalizeRepoURL(tt.input)
		if got != tt.want {
			t.Errorf("normalizeRepoURL(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// TestParseOwnerName moved to internal/platform/repourl_any_test.go
// (TestParseAnyRepoURL_ForgeURLs) when the inline parser was consolidated
// into platform.ParseAnyRepoURL (v0.25.32).

func TestNormalizeRepoURL_DetectsRedirect(t *testing.T) {
	// Simulate a redirect: old URL and new URL should normalize differently
	// if the org or repo name changed.
	old := normalizeRepoURL("https://github.com/old-org/old-repo")
	new := normalizeRepoURL("https://github.com/new-org/new-repo")

	if old == new {
		t.Error("expected different normalized URLs for different orgs/repos")
	}

	// Same repo, just different casing or trailing slash — should match.
	a := normalizeRepoURL("https://github.com/Chaoss/Augur/")
	b := normalizeRepoURL("https://github.com/chaoss/augur")
	if a != b {
		t.Errorf("expected same normalized URL, got %q vs %q", a, b)
	}
}

func TestPrelimResult_Defaults(t *testing.T) {
	r := &PrelimResult{}
	if r.Skip {
		t.Error("default PrelimResult should not skip")
	}
	if r.Redirected {
		t.Error("default PrelimResult should not be redirected")
	}
}
