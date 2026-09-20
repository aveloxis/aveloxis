// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.29.57 (Copilot review 5260880711 on PR #210) — routing the in-process
// /rate_limit probe was only half of scorecard: remote mode ALSO exports the
// borrowed pool token to the `scorecard` subprocess together with a repo URL,
// and that subprocess resolves its own host. On a deployment whose
// github.base_url is not public GitHub, that is an Enterprise token handed to
// a third party by a process we do not control.
//
// The fix this test pins is the one that can be VERIFIED from this side: a
// remote-primary run against a non-public host does not happen. Whether the
// installed scorecard honours an Enterprise host env var is its contract, not
// ours, and guessing at it is what would make the leak silent again — so the
// token simply is not lent. Local mode still runs when a clone exists, which
// is what a GitLab or generic-git repo already gets.

import "testing"

func TestRemoteScorecardOnlyOnPublicGitHub(t *testing.T) {
	for _, tc := range []struct {
		base   string
		remote bool
	}{
		{"", true},
		{"https://api.github.com", true},
		{"https://api.github.com/", true},
		{"https://api.github.com:443", true},
		{"http://api.github.com", true},
		{"https://ghe.example.invalid/api/v3", false},
		{"https://github.example.com/api/v3", false},
	} {
		if got := remoteScorecardSupported(tc.base); got != tc.remote {
			t.Errorf("remoteScorecardSupported(%q) = %v, want %v — the subprocess picks its own host, so a token may only be lent when that host is the one the keys belong to", tc.base, got, tc.remote)
		}
	}
}
