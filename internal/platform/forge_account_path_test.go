// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import "testing"

// A forge keeps its sign-in and account pages under a few first path
// segments that no owner can take. A URL there is not a repository's even
// with two segments: gitlab.com/users/sign_in is where a project that went
// private redirects (0.29.146, review round 2).
func TestIsForgeAccountURL(t *testing.T) {
	for raw, want := range map[string]bool{
		"https://gitlab.com/users/sign_in":       true,
		"https://gitlab.com/users/sign_in/":      true,
		"https://GitLab.com/Users/sign_in":       true,
		"https://github.com/login/oauth":         true,
		"https://github.com/sessions/new":        true,
		"https://gitlab.com/-/profile":           true,
		"https://gitlab.example.org/users/x":     true, // a GitLab by its host name
		"https://github.com/chaoss/augur":        false,
		"https://gitlab.com/gitlab-org/gitlab":   false,
		"https://gitlab.com/group/users/project": false, // only the first segment is the forge's
		"https://github.com/usersgroup/login":    false, // a whole segment, not a prefix
		"https://git.example.invalid/users/repo": false, // not a forge this build recognizes: any owner name is possible
		"https://github.com/users":               false, // one segment: not this predicate's case
		"not a url at all":                       false,
		"":                                       false,
	} {
		if got := IsForgeAccountURL(raw); got != want {
			t.Errorf("IsForgeAccountURL(%q) = %v, want %v", raw, got, want)
		}
	}
	for seg, want := range map[string]bool{"users": true, "LOGIN": true, "sessions": true, "signin": true, "sign_in": true, "auth": true, "-": true, "user": false, "": false, "chaoss": false} {
		if got := IsForgeAccountSegment(seg); got != want {
			t.Errorf("IsForgeAccountSegment(%q) = %v, want %v", seg, got, want)
		}
	}
}
