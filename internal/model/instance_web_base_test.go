// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "testing"

// v0.30.0 (multi-instance GitLab): a GitLab instance's identity is its web
// base URL — scheme, host and an optional path prefix for an install under a
// sub-path. One normalizer (SR-17) is used by config validation, the
// platforms registry, add-key, routing, classification and the
// instance-qualified system-account login.
func TestNormalizeInstanceWebBase(t *testing.T) {
	cases := []struct {
		in, want string
		wantErr  bool
	}{
		{"https://gitlab.com", "https://gitlab.com", false},
		{"https://GitLab.com/", "https://gitlab.com", false},
		{"HTTPS://gitlab.com", "https://gitlab.com", false},
		{"https://www.gitlab.com", "https://gitlab.com", false},
		{"https://gitlab.com.", "https://gitlab.com", false},
		{"https://gitlab.com:443", "https://gitlab.com", false},
		{"http://gitlab.example.org:80/", "http://gitlab.example.org", false},
		{"https://gitlab.example.org:8443", "https://gitlab.example.org:8443", false},
		{"https://code.example.edu/gitlab", "https://code.example.edu/gitlab", false},
		{"https://code.example.edu/GitLab/", "https://code.example.edu/gitlab", false},
		{"https://www.www.gitlab.com", "https://gitlab.com", false},
		{"https://gitlab.com..", "https://gitlab.com", false},
		{"https://code.example.edu/a%3Fb", "https://code.example.edu/a%3fb", false},
		{"https://[2001:db8::1]:8443/gl", "https://[2001:db8::1]:8443/gl", false},
		{"https://[2001:db8::1]:443", "https://[2001:db8::1]", false},
		{"https://code.example.edu//gitlab//", "https://code.example.edu/gitlab", false},
		{"  https://gitlab.freedesktop.org  ", "https://gitlab.freedesktop.org", false},
		{"", "", true},
		{"gitlab.com", "", true},
		{"ftp://gitlab.com", "", true},
		{"https://", "", true},
		{"https://gitlab.com/?x=1", "", true},
		{"https://gitlab.com/#frag", "", true},
		{"https://user:pw@gitlab.com", "", true},
		{"https://gitlab.com/api/v4", "", true},
		{"https://code.example.edu/gitlab/api/v4/", "", true},
	}
	for _, tc := range cases {
		got, err := NormalizeInstanceWebBase(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("NormalizeInstanceWebBase(%q) error = %v, wantErr %v", tc.in, err, tc.wantErr)
			continue
		}
		if got != tc.want {
			t.Errorf("NormalizeInstanceWebBase(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// Routing matches a repo URL to an instance by host and path prefix,
// ignoring the scheme (an http:// paste of an https instance is the same
// repository) and preferring the longest prefix.
func TestInstanceWebBaseMatch(t *testing.T) {
	bases := []string{"https://gitlab.com", "https://code.example.edu/gitlab", "https://code.example.edu", "https://gitlab.example.org:8443"}
	cases := []struct {
		url, wantBase, wantRest string
	}{
		{"https://gitlab.com/group/sub/repo", "https://gitlab.com", "group/sub/repo"},
		{"http://www.GitLab.com/group/repo.git", "https://gitlab.com", "group/repo"},
		{"https://code.example.edu/gitlab/group/repo", "https://code.example.edu/gitlab", "group/repo"},
		{"https://code.example.edu/gitlabx/repo", "https://code.example.edu", "gitlabx/repo"},
		{"https://code.example.edu/group/repo", "https://code.example.edu", "group/repo"},
		{"https://gitlab.example.org:8443/g/r", "https://gitlab.example.org:8443", "g/r"},
		{"https://gitlab.example.org/g/r", "", ""},
		{"https://github.com/o/r", "", ""},
		{"not a url", "", ""},
	}
	for _, tc := range cases {
		base, rest, ok := MatchInstanceWebBase(tc.url, bases)
		if ok != (tc.wantBase != "") || base != tc.wantBase || rest != tc.wantRest {
			t.Errorf("MatchInstanceWebBase(%q) = (%q, %q, %v), want (%q, %q)", tc.url, base, rest, ok, tc.wantBase, tc.wantRest)
		}
	}
}

// Review of B1–B5 (finding 5): normalizing a normalized base changes
// nothing, so a registry key read back and matched again is the same key.
func TestNormalizeInstanceWebBaseIsIdempotent(t *testing.T) {
	for _, in := range []string{
		"https://www.www.gitlab.com", "https://gitlab.com..", "HTTP://Code.Example.edu:80//GitLab//",
		"https://code.example.edu/a%3Fb", "https://code.example.edu/a%23b", "https://[2001:db8::1]:8443/gl",
		"https://gitlab.example.org:8443/x/y", "https://gitlab.com", "https://sub.www.example.org",
	} {
		once, err := NormalizeInstanceWebBase(in)
		if err != nil {
			t.Errorf("NormalizeInstanceWebBase(%q): %v", in, err)
			continue
		}
		twice, err := NormalizeInstanceWebBase(once)
		if err != nil || twice != once {
			t.Errorf("not idempotent: %q -> %q -> (%q, %v)", in, once, twice, err)
		}
	}
}

// Matching returns the caller's own base string (so a registry key is looked
// up as-is), compares the prefix case-insensitively (repo dedup lower-cases
// URLs), and respects the path-segment boundary.
func TestInstanceWebBaseMatchReturnsTheCallersBase(t *testing.T) {
	bases := []string{"https://code.example.edu/gitlab", "https://code.example.edu"}
	if base, rest, ok := MatchInstanceWebBase("https://code.example.edu/GitLab/g/r", bases); !ok || base != "https://code.example.edu/gitlab" || rest != "g/r" {
		t.Errorf("case-variant prefix = (%q, %q, %v), want the sub-path instance", base, rest, ok)
	}
	if base, _, ok := MatchInstanceWebBase("https://www.gitlab.com/g/r", []string{"https://gitlab.com"}); !ok || base != "https://gitlab.com" {
		t.Errorf("www host = (%q, %v)", base, ok)
	}
}
