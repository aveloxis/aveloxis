// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// v0.30.0: scorecard is given the repository's own URL. The scheduler used to
// rebuild https://<github.com|gitlab.com>/owner/name from the platform id,
// which sends a self-hosted GitLab repo to gitlab.com. But scorecard's remote
// mode accepts only the canonical github.com host ("unsupported host:
// www.github.com", "unsupported host: GitHub.com"), and the catalog stores
// URLs as typed (14 http://github.com rows on chaoss.tv, 2026-09-14), so a
// GitHub repo keeps its canonical form. Any other platform is passed through
// as stored.
func TestScorecardRepoURL(t *testing.T) {
	cases := []struct {
		name        string
		platform    model.Platform
		gitURL      string
		owner, repo string
		want        string
	}{
		{"github canonical", model.PlatformGitHub, "https://github.com/o/r", "o", "r", "https://github.com/o/r"},
		{"github http", model.PlatformGitHub, "http://github.com/thuem/THUNDER", "thuem", "THUNDER", "https://github.com/thuem/THUNDER"},
		{"github www and case", model.PlatformGitHub, "https://www.GitHub.com/o/r", "o", "r", "https://github.com/o/r"},
		{"github .git suffix", model.PlatformGitHub, "https://github.com/o/r.git", "o", "r", "https://github.com/o/r"},
		{"github subdomain host", model.PlatformGitHub, "https://gist.github.com/o/r", "o", "r", "https://github.com/o/r"},
		{"gitlab.com stored url", model.PlatformGitLab, "https://gitlab.com/group/sub/r", "group/sub", "r", "https://gitlab.com/group/sub/r"},
		{"self-hosted gitlab", model.GitLabInstanceIDMin, "https://gitlab.freedesktop.org/g/r", "g", "r", "https://gitlab.freedesktop.org/g/r"},
		{"sub-path gitlab", model.GitLabInstanceIDMin, "https://code.example.edu/gitlab/g/r", "g", "r", "https://code.example.edu/gitlab/g/r"},
		{"generic git", model.PlatformGenericGit, "https://git.example.org/x/y", "x", "y", "https://git.example.org/x/y"},
		{"github owner/name are the identity, not the stored spelling", model.PlatformGitHub, "://bad", "o", "r", "https://github.com/o/r"},
	}
	for _, tc := range cases {
		if got := ScorecardRepoURL(tc.platform, tc.gitURL, tc.owner, tc.repo); got != tc.want {
			t.Errorf("%s: ScorecardRepoURL = %q, want %q", tc.name, got, tc.want)
		}
	}
}
