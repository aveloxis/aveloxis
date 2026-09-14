// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import "testing"

// v0.30.0: a GitLab project's org URL keeps the project's own host — the
// old gitlab.com literal turned a self-hosted project's group into a
// gitlab.com group of the same name.
func TestOrgURLForRepoKeepsTheInstanceHost(t *testing.T) {
	cases := map[string]string{
		"https://github.com/apache/arrow":             "https://github.com/apache",
		"https://gitlab.com/gitlab-org/gitlab-runner": "https://gitlab.com/gitlab-org",
		"https://gitlab.com/group/sub/project":        "https://gitlab.com/group/sub",
		"https://gitlab.freedesktop.org/mesa/mesa":    "https://gitlab.freedesktop.org/mesa",
		"https://git.example.org/x/y":                 "",
		// A configured instance under a sub-path, no "gitlab" in its host.
		"https://code.example.edu/gitlab/grp/proj": "https://code.example.edu/gitlab/grp",
		"not a url": "",
	}
	for in, want := range cases {
		if got := orgURLForRepo(in, []string{"https://code.example.edu/gitlab"}); got != want {
			t.Errorf("orgURLForRepo(%q) = %q, want %q", in, got, want)
		}
	}
}
