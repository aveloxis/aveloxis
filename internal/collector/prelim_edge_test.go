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

// ============================================================
// parseOwnerName edge cases (beyond prelim_test.go coverage)
// ============================================================

func TestParseOwnerName_DeeplyNested(t *testing.T) {
	owner, name := parseOwnerName("https://gitlab.com/a/b/c/d/project")
	if owner != "a/b/c/d" {
		t.Errorf("deeply nested owner = %q, want a/b/c/d", owner)
	}
	if name != "project" {
		t.Errorf("name = %q", name)
	}
}

func TestParseOwnerName_WithDotGit(t *testing.T) {
	owner, name := parseOwnerName("https://github.com/org/repo.git")
	if owner != "org" {
		t.Errorf("owner = %q", owner)
	}
	if name != "repo" {
		t.Errorf("name with .git = %q, want repo", name)
	}
}

func TestParseOwnerName_TrailingSlash(t *testing.T) {
	owner, name := parseOwnerName("https://github.com/org/repo/")
	if owner != "org" || name != "repo" {
		t.Errorf("trailing slash: owner=%q name=%q", owner, name)
	}
}

func TestParseOwnerName_HostOnly(t *testing.T) {
	owner, name := parseOwnerName("github.com")
	if owner != "" || name != "" {
		t.Errorf("host only: owner=%q name=%q", owner, name)
	}
}

func TestParseOwnerName_HostAndOwnerOnly(t *testing.T) {
	// Only 2 path segments — not enough for owner + name.
	owner, name := parseOwnerName("https://github.com/org")
	if owner != "" || name != "" {
		t.Errorf("host+owner only: owner=%q name=%q", owner, name)
	}
}

func TestParseOwnerName_Empty(t *testing.T) {
	owner, name := parseOwnerName("")
	if owner != "" || name != "" {
		t.Errorf("empty: owner=%q name=%q", owner, name)
	}
}

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
