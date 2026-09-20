// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// v0.29.57 (Copilot review 5260961848 on PR #210): three constructors each
// spelled the "empty base means public GitHub" default inline, and the web
// server's did not spell it at all — an empty base there produced a
// schemeless request URL. One shared normaliser (SR-17), pinned here.
func TestGitHubAPIBaseOrPublic(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", PublicGitHubAPIBase},
		{PublicGitHubAPIBase, PublicGitHubAPIBase},
		{"https://ghe.example.invalid/api/v3", "https://ghe.example.invalid/api/v3"},
	} {
		if got := GitHubAPIBaseOrPublic(tc.in); got != tc.want {
			t.Errorf("GitHubAPIBaseOrPublic(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// The web host of the deployment's GitHub API base is what an org URL is
// matched against before the deployment's keys are used to enumerate it.
// Before this pin the web org scan and the CLI org expansion compared
// against the literal "github.com", so an Enterprise deployment could never
// enumerate an org on its own host.
func TestGitHubWebHost(t *testing.T) {
	for _, tc := range []struct{ base, want string }{
		{"", "github.com"},
		{"https://api.github.com", "github.com"},
		{"https://api.github.com/", "github.com"},
		{"https://ghe.example.invalid/api/v3", "ghe.example.invalid"},
		{"https://GHE.Example.invalid/api/v3/", "ghe.example.invalid"},
		{"https://ghe.example.invalid:8443/api/v3", "ghe.example.invalid:8443"},
		{"http://127.0.0.1:8080", "127.0.0.1:8080"},
		// Public GitHub in any spelling that names its host (round 6: a
		// byte-exact compare read these as Enterprise and refused every
		// github.com org).
		{"https://api.github.com:443", "github.com"},
		{"https://API.github.com", "github.com"},
		{"http://api.github.com", "github.com"},
		// A base that is not a URL matches no host at all: nothing may be
		// enumerated under a guess.
		{"://bad", ""},
		{"ghe.example.invalid/api/v3", ""},
	} {
		if got := GitHubWebHost(tc.base); got != tc.want {
			t.Errorf("GitHubWebHost(%q) = %q, want %q", tc.base, got, tc.want)
		}
	}
}

// The derivation is the inverse of RepoURL.APIURL for every host shape that
// function produces, so an org URL on the deployment's host and the API base
// the deployment configured agree about which host that is.
func TestGitHubWebHostInvertsAPIURL(t *testing.T) {
	for _, host := range []string{"github.com", "ghe.example.invalid", "ghe.example.invalid:8443"} {
		base := RepoURL{Platform: model.PlatformGitHub, Host: host}.APIURL()
		if got := GitHubWebHost(base); got != host {
			t.Errorf("GitHubWebHost(APIURL(%q) = %q) = %q, want the host back", host, base, got)
		}
	}
}

// IsGitHubHost is the one gate every org path uses (web scan, add-org
// registration, CLI, both periodic refreshes): an empty web host — a base
// that is not a URL — matches nothing, so a schemeless org argument (empty
// host) cannot pair with it.
func TestIsGitHubHost(t *testing.T) {
	for _, tc := range []struct {
		host, base string
		want       bool
	}{
		{"github.com", "", true},
		{"github.com", "https://api.github.com", true},
		{"www.github.com", "", true},
		{"github.com:443", "", true},
		{"github.com:80", "", true},
		{"github.com:8443", "", false},
		// The base side is canonicalised the same way (round 4: trimming
		// one side only made a base with an explicit default port match
		// nothing, not even itself).
		{"ghe.example.invalid", "https://ghe.example.invalid:443/api/v3", true},
		{"ghe.example.invalid:443", "https://ghe.example.invalid:443/api/v3", true},
		{"www.ghe.example.invalid", "https://www.ghe.example.invalid/api/v3", true},
		{"ghe.example.invalid", "https://www.ghe.example.invalid/api/v3", true},
		{"127.0.0.1", "http://127.0.0.1:80", true},
		{"ghe.example.invalid", "https://ghe.example.invalid:8443/api/v3", false},
		// A base whose host canonicalises to nothing (url.Parse accepts a
		// hostless authority) matches nothing — the guard is on the
		// CANONICAL base, or a schemeless org argument pairs with it.
		{"", "https://:443/api/v3", false},
		{"", "https://www./api/v3", false},
		{"", "http://:80", false},
		{"ghe.example.invalid", "https://ghe.example.invalid/api/v3", true},
		{"github.com", "https://ghe.example.invalid/api/v3", false},
		{"ghe.example.invalid", "", false},
		{"", "://bad", false},
		{"", "", false},
	} {
		if got := IsGitHubHost(tc.host, tc.base); got != tc.want {
			t.Errorf("IsGitHubHost(%q, %q) = %v, want %v", tc.host, tc.base, got, tc.want)
		}
	}
}

// IsPublicGitHubBase is the ONE answer to "is this base public GitHub", for
// the web-host derivation and for scorecard's token loan alike.
func TestIsPublicGitHubBase(t *testing.T) {
	for _, tc := range []struct {
		base string
		want bool
	}{
		{"", true},
		{"https://api.github.com", true},
		{"https://api.github.com/", true},
		{"https://api.github.com:443", true},
		{"https://API.github.com", true},
		{"http://api.github.com", true},
		{"https://api.github.com:80", true},
		{"https://www.api.github.com", true},
		// A non-default port is a different host, as IsGitHubHost pins for
		// org hosts.
		{"https://api.github.com:8443", false},
		{"https://ghe.example.invalid/api/v3", false},
		{"https://github.example.com/api/v3", false},
		{"https://api.github.com.evil.invalid", false},
		{"://bad", false},
		{"https://:443", false},
	} {
		if got := IsPublicGitHubBase(tc.base); got != tc.want {
			t.Errorf("IsPublicGitHubBase(%q) = %v, want %v", tc.base, got, tc.want)
		}
	}
}
