// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// ScorecardRepoURL is the ONE rule (SR-17) for the URL scorecard is handed:
// the row's own repo_git, and the synthesised host/owner/name ONLY for a row
// that has none. The scheduler's phase had it and `aveloxis run-scorecard`
// had lost it — a legacy row with an empty repo_git sent `--repo ""`
// (Copilot review 5268977585). Both consumers call this now.
func TestScorecardRepoURLPrefersTheStoredURL(t *testing.T) {
	for _, tc := range []struct {
		gitURL string
		plat   model.Platform
		want   string
	}{
		{"https://ghe.example.invalid/o/n", model.PlatformGitHub, "https://ghe.example.invalid/o/n"},
		{"https://gitlab.example.invalid/g/sub/p", model.PlatformGitLab, "https://gitlab.example.invalid/g/sub/p"},
		// no stored URL: the synthesis, unchanged from the pre-v0.29.57 rule
		{"", model.PlatformGitHub, "https://github.com/o/n"},
		{"", model.PlatformGitLab, "https://gitlab.com/o/n"},
	} {
		if got := ScorecardRepoURL(tc.gitURL, tc.plat, "o", "n"); got != tc.want {
			t.Errorf("ScorecardRepoURL(%q, %v) = %q, want %q", tc.gitURL, tc.plat, got, tc.want)
		}
	}
}
