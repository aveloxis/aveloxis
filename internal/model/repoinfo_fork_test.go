// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.78: RepoInfo carries the forge's fork signal (IsFork +
// ForkParent) so Phase 0 can populate repos.forked_from. Before this,
// NOTHING in the pipeline captured fork status — 0 of 94,104
// production repos had forked_from set even though all three
// transports (GitHub GraphQL, GitHub REST, GitLab REST) deliver it.

package model

import "testing"

func TestRepoInfoForkedFrom(t *testing.T) {
	cases := []struct {
		name string
		info RepoInfo
		want string
	}{
		{"not a fork", RepoInfo{}, ""},
		{"fork with known parent", RepoInfo{IsFork: true, ForkParent: "NixOS/nixpkgs"}, "NixOS/nixpkgs"},
		// isFork=true with a null parent happens when the upstream was
		// deleted or is inaccessible — still a fork, and the filter
		// consumers must see a non-empty value, never a fabricated
		// upstream name.
		{"fork with unknown parent", RepoInfo{IsFork: true}, UnknownForkParent},
		// A parent without the flag still counts (GitLab's
		// forked_from_project has no separate boolean).
		{"parent without flag", RepoInfo{ForkParent: "torvalds/linux"}, "torvalds/linux"},
	}
	for _, c := range cases {
		if got := c.info.ForkedFrom(); got != c.want {
			t.Errorf("%s: ForkedFrom() = %q, want %q", c.name, got, c.want)
		}
	}
}
