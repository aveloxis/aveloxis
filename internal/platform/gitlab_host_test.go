// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import "testing"

// TestGitLabAPIBaseForHost — v0.29.11. GitLab API keys belong to the ONE
// instance configured as gitlab.base_url. A group discovered on another
// host must not receive them: the helper returns the configured API base
// only when the group's host is that instance's host, so a caller can
// never build a keyed GitLab client for a foreign host.
func TestGitLabAPIBaseForHost(t *testing.T) {
	for _, tc := range []struct {
		name, configured, host, wantBase string
		wantOK                           bool
	}{
		{"gitlab.com default", "https://gitlab.com/api/v4", "gitlab.com", "https://gitlab.com/api/v4", true},
		{"host is case-insensitive", "https://gitlab.com/api/v4", "GitLab.COM", "https://gitlab.com/api/v4", true},
		{"self-hosted instance", "https://git.example.org/api/v4", "git.example.org", "https://git.example.org/api/v4", true},
		{"trailing slash trimmed", "https://gitlab.com/api/v4/", "gitlab.com", "https://gitlab.com/api/v4", true},
		{"port is part of the authority", "http://127.0.0.1:8080/api/v4", "127.0.0.1:8080", "http://127.0.0.1:8080/api/v4", true},
		{"different port is a different instance", "http://127.0.0.1:8080/api/v4", "127.0.0.1:9090", "", false},
		{"foreign host", "https://gitlab.com/api/v4", "git.example.org", "", false},
		{"lookalike subdomain", "https://gitlab.com/api/v4", "gitlab.com.evil.example", "", false},
		{"no configured instance", "", "gitlab.com", "", false},
		{"unparseable configured base", "://nope", "gitlab.com", "", false},
		{"configured base without a host", "/api/v4", "gitlab.com", "", false},
		{"empty group host", "https://gitlab.com/api/v4", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, ok := GitLabAPIBaseForHost(tc.configured, tc.host)
			if ok != tc.wantOK || base != tc.wantBase {
				t.Errorf("GitLabAPIBaseForHost(%q, %q) = (%q, %v), want (%q, %v)", tc.configured, tc.host, base, ok, tc.wantBase, tc.wantOK)
			}
		})
	}
}
