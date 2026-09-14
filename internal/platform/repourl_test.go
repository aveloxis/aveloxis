// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

func TestParseRepoURL(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		wantPlat  model.Platform
		wantOwner string
		wantRepo  string
		wantErr   bool
	}{
		// GitHub
		{
			name:      "github simple",
			url:       "https://github.com/torvalds/linux",
			wantPlat:  model.PlatformGitHub,
			wantOwner: "torvalds",
			wantRepo:  "linux",
		},
		{
			name:      "github with .git suffix",
			url:       "https://github.com/torvalds/linux.git",
			wantPlat:  model.PlatformGitHub,
			wantOwner: "torvalds",
			wantRepo:  "linux",
		},
		{
			name:      "github with trailing slash",
			url:       "https://github.com/torvalds/linux/",
			wantPlat:  model.PlatformGitHub,
			wantOwner: "torvalds",
			wantRepo:  "linux",
		},
		{
			name:    "github too many segments",
			url:     "https://github.com/a/b/c",
			wantErr: true,
		},

		// GitLab - standard
		{
			name:      "gitlab simple",
			url:       "https://gitlab.com/fdroid/fdroidclient",
			wantPlat:  model.PlatformGitLab,
			wantOwner: "fdroid",
			wantRepo:  "fdroidclient",
		},
		{
			name:      "gitlab with .git suffix",
			url:       "https://gitlab.com/fdroid/fdroidclient.git",
			wantPlat:  model.PlatformGitLab,
			wantOwner: "fdroid",
			wantRepo:  "fdroidclient",
		},

		// GitLab - nested subgroups
		{
			name:      "gitlab one subgroup",
			url:       "https://gitlab.com/gitlab-org/security/gitlab",
			wantPlat:  model.PlatformGitLab,
			wantOwner: "gitlab-org/security",
			wantRepo:  "gitlab",
		},
		{
			name:      "gitlab deep subgroups",
			url:       "https://gitlab.com/a/b/c/d/project",
			wantPlat:  model.PlatformGitLab,
			wantOwner: "a/b/c/d",
			wantRepo:  "project",
		},

		// GitLab - self-hosted with "gitlab" in hostname
		{
			name:      "self-hosted gitlab",
			url:       "https://gitlab.freedesktop.org/mesa/mesa",
			wantPlat:  model.PlatformGitLab,
			wantOwner: "mesa",
			wantRepo:  "mesa",
		},

		// Errors
		{
			name:    "empty path",
			url:     "https://github.com/",
			wantErr: true,
		},
		{
			name:    "no scheme",
			url:     "github.com/torvalds/linux",
			wantErr: true,
		},
		{
			name:    "unknown host",
			url:     "https://bitbucket.org/owner/repo",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseRepoURL(tt.url)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Platform != tt.wantPlat {
				t.Errorf("platform = %v, want %v", got.Platform, tt.wantPlat)
			}
			if got.Owner != tt.wantOwner {
				t.Errorf("owner = %q, want %q", got.Owner, tt.wantOwner)
			}
			if got.Repo != tt.wantRepo {
				t.Errorf("repo = %q, want %q", got.Repo, tt.wantRepo)
			}
		})
	}
}

// v0.30.0 (multi-instance GitLab): hints are the configured GitLab
// instances' web bases — host plus the path prefix of an install under a
// sub-path. A URL under one belongs to the GitLab family with owner/repo
// taken from the path AFTER the prefix, and carries the matched WebBase.
func TestParseRepoURLWithHints(t *testing.T) {
	hints := []string{"https://code.internal.company.com", "https://code.example.edu/gitlab", "https://gitlab.com"}
	cases := []struct {
		url                           string
		wantPlat                      model.Platform
		wantOwner, wantRepo, wantBase string
		wantErr                       bool
	}{
		// Self-hosted GitLab without "gitlab" in the hostname.
		{"https://code.internal.company.com/infra/deploy-tools", model.PlatformGitLab, "infra", "deploy-tools", "https://code.internal.company.com", false},
		// Sub-path install: the prefix is not part of the owner.
		{"https://code.example.edu/gitlab/group/sub/repo.git", model.PlatformGitLab, "group/sub", "repo", "https://code.example.edu/gitlab", false},
		{"http://www.Code.Example.edu/gitlab/group/repo/", model.PlatformGitLab, "group", "repo", "https://code.example.edu/gitlab", false},
		// Outside the prefix on the same host, and no "gitlab" in the host.
		{"https://code.example.edu/other/repo", 0, "", "", "", true},
		// The prefix alone plus one segment is not owner/repo.
		{"https://code.example.edu/gitlab/repo", 0, "", "", "", true},
		// Unhinted gitlab.com still parses, now with its web base.
		{"https://gitlab.com/group/project", model.PlatformGitLab, "group", "project", "https://gitlab.com", false},
		// GitHub is never affected by GitLab hints.
		{"https://github.com/owner/repo", model.PlatformGitHub, "owner", "repo", "https://github.com", false},
	}
	for _, tc := range cases {
		got, err := ParseRepoURLWithHints(tc.url, hints)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v, wantErr %v", tc.url, err, tc.wantErr)
			continue
		}
		if tc.wantErr {
			continue
		}
		if got.Platform != tc.wantPlat || got.Owner != tc.wantOwner || got.Repo != tc.wantRepo || got.WebBase != tc.wantBase {
			t.Errorf("%s: got {%v %q %q base=%q}, want {%v %q %q base=%q}", tc.url, got.Platform, got.Owner, got.Repo, got.WebBase, tc.wantPlat, tc.wantOwner, tc.wantRepo, tc.wantBase)
		}
	}

	// Without hints a host lacking "gitlab" is not a forge, and a
	// "gitlab"-named host still is (its instance may be unconfigured:
	// classification decides).
	if _, err := ParseRepoURLWithHints("https://code.internal.company.com/infra/deploy-tools", nil); err == nil {
		t.Error("an unhinted host without \"gitlab\" in it must not parse as a forge")
	}
	if got, err := ParseRepoURLWithHints("https://gitlab.example.org/g/r", nil); err != nil || got.Platform != model.PlatformGitLab || got.WebBase != "https://gitlab.example.org" {
		t.Errorf("gitlab.example.org without hints = (%+v, %v), want GitLab family with its web base", got, err)
	}
}

func TestGitLabProjectPath(t *testing.T) {
	r := RepoURL{
		Platform: model.PlatformGitLab,
		Owner:    "group/subgroup",
		Repo:     "project",
	}
	want := "group%2Fsubgroup%2Fproject"
	if got := r.GitLabProjectPath(); got != want {
		t.Errorf("GitLabProjectPath() = %q, want %q", got, want)
	}
}
