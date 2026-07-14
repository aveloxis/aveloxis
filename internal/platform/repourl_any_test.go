// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// ParseAnyRepoURL is the consolidation target for the inline owner/name
// parsers that used to live in collector/prelim.go (parseOwnerName),
// db/web_store.go (AddRepoToGroup), db/postgres.go (UpdateRepoURL), and
// web/server.go (scanOrgRepos). The forge cases below are ported verbatim
// from prelim_test.go TestParseOwnerName and prelim_edge_test.go so the
// behavior contract survives the consolidation.
func TestParseAnyRepoURL_ForgeURLs(t *testing.T) {
	tests := []struct {
		url          string
		wantPlatform model.Platform
		wantOwner    string
		wantRepo     string
	}{
		{"https://github.com/torvalds/linux", model.PlatformGitHub, "torvalds", "linux"},
		{"https://github.com/chaoss/augur.git", model.PlatformGitHub, "chaoss", "augur"},
		{"https://github.com/org/repo/", model.PlatformGitHub, "org", "repo"},
		// GitLab nested groups: everything but the last segment is the owner.
		{"https://gitlab.com/group/subgroup/project", model.PlatformGitLab, "group/subgroup", "project"},
		{"https://gitlab.com/a/b/c/d", model.PlatformGitLab, "a/b/c", "d"},
		{"https://gitlab.com/a/b/c/d/project", model.PlatformGitLab, "a/b/c/d", "project"},
	}

	for _, tt := range tests {
		got, err := ParseAnyRepoURL(tt.url)
		if err != nil {
			t.Errorf("ParseAnyRepoURL(%q) unexpected error: %v", tt.url, err)
			continue
		}
		if got.Platform != tt.wantPlatform {
			t.Errorf("ParseAnyRepoURL(%q) platform = %v, want %v", tt.url, got.Platform, tt.wantPlatform)
		}
		if got.Owner != tt.wantOwner {
			t.Errorf("ParseAnyRepoURL(%q) owner = %q, want %q", tt.url, got.Owner, tt.wantOwner)
		}
		if got.Repo != tt.wantRepo {
			t.Errorf("ParseAnyRepoURL(%q) repo = %q, want %q", tt.url, got.Repo, tt.wantRepo)
		}
	}
}

// Unknown hosts must NOT error the way ParseRepoURL does — they fall back
// to PlatformGenericGit so facade-only collection can proceed. This is the
// behavioral difference that justifies the "Any" name.
func TestParseAnyRepoURL_GenericGitFallback(t *testing.T) {
	tests := []struct {
		url       string
		wantOwner string
		wantRepo  string
	}{
		{"https://git.example.org/owner/repo", "owner", "repo"},
		{"https://code.company.io/team/sub/project.git", "team/sub", "project"},
	}

	for _, tt := range tests {
		got, err := ParseAnyRepoURL(tt.url)
		if err != nil {
			t.Errorf("ParseAnyRepoURL(%q) unexpected error: %v", tt.url, err)
			continue
		}
		if got.Platform != model.PlatformGenericGit {
			t.Errorf("ParseAnyRepoURL(%q) platform = %v, want PlatformGenericGit", tt.url, got.Platform)
		}
		if got.Owner != tt.wantOwner || got.Repo != tt.wantRepo {
			t.Errorf("ParseAnyRepoURL(%q) = (%q, %q), want (%q, %q)",
				tt.url, got.Owner, got.Repo, tt.wantOwner, tt.wantRepo)
		}
	}
}

func TestParseAnyRepoURL_Errors(t *testing.T) {
	bad := []string{
		"",                              // empty
		"https://github.com",            // no path
		"https://github.com/",           // empty path
		"https://github.com/org",        // single segment: no repo
		"github.com/org/repo",           // schemeless — callers validate/prepend scheme first
		"ssh://git@github.com/org/repo", // unsupported scheme
	}
	for _, u := range bad {
		if _, err := ParseAnyRepoURL(u); err == nil {
			t.Errorf("ParseAnyRepoURL(%q) expected error, got nil", u)
		}
	}
}

func TestParseOrgURL(t *testing.T) {
	tests := []struct {
		url      string
		wantHost string
		wantOrg  string
	}{
		{"https://github.com/torvalds", "github.com", "torvalds"},
		{"https://github.com/Azure/", "github.com", "Azure"},
		{"https://gitlab.com/gitlab-org", "gitlab.com", "gitlab-org"},
		// Schemeless tolerated: the web org-add path historically accepted it.
		{"github.com/chaoss", "github.com", "chaoss"},
	}

	for _, tt := range tests {
		host, org, err := ParseOrgURL(tt.url)
		if err != nil {
			t.Errorf("ParseOrgURL(%q) unexpected error: %v", tt.url, err)
			continue
		}
		if host != tt.wantHost || org != tt.wantOrg {
			t.Errorf("ParseOrgURL(%q) = (%q, %q), want (%q, %q)",
				tt.url, host, org, tt.wantHost, tt.wantOrg)
		}
	}
}

func TestParseOrgURL_Errors(t *testing.T) {
	for _, u := range []string{"", "https://github.com", "https://github.com/"} {
		if _, _, err := ParseOrgURL(u); err == nil {
			t.Errorf("ParseOrgURL(%q) expected error, got nil", u)
		}
	}
}
