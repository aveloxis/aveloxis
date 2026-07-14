// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"testing"
)

// ============================================================
// normalizeRepoURL edge cases (beyond prelim_test.go coverage)
// ============================================================

func TestNormalizeRepoURL_DotGitAndTrailingSlash(t *testing.T) {
	// Trailing slash stripped first, then .git suffix — both are removed.
	n := normalizeRepoURL("https://github.com/org/repo.git/")
	if n != "github.com/org/repo" {
		t.Errorf("combined: %q", n)
	}
}

func TestNormalizeRepoURL_NoScheme(t *testing.T) {
	if n := normalizeRepoURL("github.com/org/repo"); n != "github.com/org/repo" {
		t.Errorf("no scheme: %q", n)
	}
}

func TestNormalizeRepoURL_GitLabNested(t *testing.T) {
	n := normalizeRepoURL("https://gitlab.com/group/subgroup/project.git")
	if n != "gitlab.com/group/subgroup/project" {
		t.Errorf("gitlab nested: %q", n)
	}
}

func TestNormalizeRepoURL_Empty(t *testing.T) {
	if n := normalizeRepoURL(""); n != "" {
		t.Errorf("empty: %q", n)
	}
}

func TestNormalizeRepoURL_HTTPOnly(t *testing.T) {
	if n := normalizeRepoURL("http://github.com/org/repo"); n != "github.com/org/repo" {
		t.Errorf("http: %q", n)
	}
}

// The parseOwnerName edge-case tests that used to live here moved to
// internal/platform/repourl_any_test.go when the inline parser was
// consolidated into platform.ParseAnyRepoURL (v0.25.32).

// ============================================================
// PrelimResult field combinations
// ============================================================

func TestPrelimResult_SkipWithRedirect(t *testing.T) {
	// A repo can be both redirected AND skipped (if the new URL is a duplicate).
	r := &PrelimResult{
		Skip:       true,
		Redirected: true,
		SkipReason: "redirected to existing repo",
		OldURL:     "https://github.com/old/repo",
		NewURL:     "https://github.com/new/repo",
	}
	if !r.Skip || !r.Redirected {
		t.Error("both flags should be true")
	}
}

func TestPrelimResult_RedirectWithoutSkip(t *testing.T) {
	// Normal redirect — update URL and continue.
	r := &PrelimResult{
		Skip:       false,
		Redirected: true,
		OldURL:     "https://github.com/old/name",
		NewURL:     "https://github.com/new/name",
	}
	if r.Skip {
		t.Error("redirect without skip should not skip")
	}
}
