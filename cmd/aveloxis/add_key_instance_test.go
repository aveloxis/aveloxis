// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"reflect"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
)

// v0.30.0: add-key files a GitLab key under the one instance that issued it.
// No --instance = the main instance (the tag every pre-v0.30.0 key has);
// --instance is normalized as a web base and never applies to GitHub; an
// instance that is not configured is still stored (with a WARN at the call
// site) but reported as not configured.
func TestResolveKeyInstance(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.GitLab.Instances = []config.GitLabInstanceConfig{{WebURL: "https://gitlab.freedesktop.org", APIURL: "https://api.freedesktop.example/api/v4"}}

	cases := []struct {
		name, plat, instance string
		want                 keyInstance
		wantErr              bool
	}{
		{"github", "github", "", keyInstance{configured: true}, false},
		{"github with --instance", "github", "https://gitlab.com", keyInstance{}, true},
		{"gitlab main instance", "gitlab", "", keyInstance{webBase: "https://gitlab.com", apiURL: "https://gitlab.com/api/v4", configured: true}, false},
		{"gitlab configured instance, normalized", "gitlab", "HTTPS://www.GitLab.FreeDesktop.org/", keyInstance{tag: "https://gitlab.freedesktop.org", webBase: "https://gitlab.freedesktop.org", apiURL: "https://api.freedesktop.example/api/v4", configured: true}, false},
		{"gitlab unconfigured instance", "gitlab", "https://salsa.debian.org", keyInstance{tag: "https://salsa.debian.org", webBase: "https://salsa.debian.org"}, false},
		// Review passes 2–3: naming the main instance explicitly keeps the
		// absolute tag (a pinned key must not follow a later base_url
		// change), and a scheme twin of a configured instance is that
		// instance.
		{"gitlab main instance named explicitly", "gitlab", "https://gitlab.com/", keyInstance{tag: "https://gitlab.com", webBase: "https://gitlab.com", apiURL: "https://gitlab.com/api/v4", configured: true}, false},
		{"gitlab instance named with the other scheme", "gitlab", "http://gitlab.freedesktop.org", keyInstance{tag: "https://gitlab.freedesktop.org", webBase: "https://gitlab.freedesktop.org", apiURL: "https://api.freedesktop.example/api/v4", configured: true}, false},
		{"gitlab instance given an API URL", "gitlab", "https://gitlab.freedesktop.org/api/v4", keyInstance{}, true},
		{"unknown platform", "bitbucket", "", keyInstance{}, true},
	}
	for _, tc := range cases {
		got, err := resolveKeyInstance(cfg, tc.plat, tc.instance)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v, wantErr %v", tc.name, err, tc.wantErr)
			continue
		}
		got.instances = nil // the configured instances ride along; not part of the expectation
		if !tc.wantErr && !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: %+v, want %+v", tc.name, got, tc.want)
		}
	}
}

// A move warning fires only when the instance a key loads into changes: ""
// and the main web base are the same instance, and so are http:// and
// https:// of one web URL.
func TestKeyInstanceMoveIsByResolvedInstance(t *testing.T) {
	instances := []config.GitLabInstance{
		{WebBase: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", Primary: true},
		{WebBase: "https://gitlab.freedesktop.org", APIURL: "https://gitlab.freedesktop.org/api/v4"},
	}
	k := keyInstance{instances: instances}
	same := [][2]string{
		{"", "https://gitlab.com"},
		{"http://gitlab.freedesktop.org", "https://gitlab.freedesktop.org"},
		// A tag add-key did not write (manual SQL, an older DB) loads into
		// the same pool, so it is not a move (review pass 4, finding 1).
		{"https://GitLab.FreeDesktop.org/", "https://gitlab.freedesktop.org"},
		{"", ""},
		// Unconfigured instances compare the same way (review pass 5).
		{"http://salsa.debian.org", "https://salsa.debian.org"},
		{"https://Salsa.Debian.org/", "https://salsa.debian.org"},
	}
	for _, p := range same {
		if k.resolvesTo(p[0]) != k.resolvesTo(p[1]) {
			t.Errorf("%q and %q must resolve to the same instance", p[0], p[1])
		}
	}
	if k.resolvesTo("") == k.resolvesTo("https://gitlab.freedesktop.org") {
		t.Error("the main instance and another instance must differ")
	}
	if k.resolvesTo("https://gone.example.org") == k.resolvesTo("https://gitlab.com") {
		t.Error("an unconfigured tag must not resolve to a configured instance")
	}
	// A tag that does not normalize (no scheme; only manual SQL writes one)
	// never loads, even once its host is configured, so it is not the same
	// instance as the loadable spelling (review pass 6, finding 2).
	if k.resolvesTo("salsa.debian.org") == k.resolvesTo("https://salsa.debian.org") {
		t.Error("an unloadable tag must differ from the loadable spelling of its host")
	}
}
