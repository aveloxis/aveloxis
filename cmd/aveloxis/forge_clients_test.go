// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
)

// v0.30.0 (multi-instance GitLab): every GitLab token belongs to exactly
// one instance. A token in an instance's config entry or stored with that
// instance's web URL goes to that instance's pool; a stored token without an
// instance tag goes to the MAIN instance only (what it meant before); a
// stored tag no configured instance has is never loaded; an instance with no
// tokens gets none (never another instance's); and a token found under two
// instances is refused rather than loaded twice.
func TestPartitionGitLabTokensNoCrossInstanceLeak(t *testing.T) {
	main := config.GitLabInstance{WebBase: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", APIKeys: []string{"main-cfg"}, Primary: true}
	fd := config.GitLabInstance{WebBase: "https://gitlab.freedesktop.org", APIURL: "https://gitlab.freedesktop.org/api/v4", APIKeys: []string{"fd-cfg"}}
	salsa := config.GitLabInstance{WebBase: "https://salsa.debian.org", APIURL: "https://salsa.debian.org/api/v4"}
	instances := []config.GitLabInstance{main, fd, salsa}

	stored := map[string][]string{
		"":                                {"main-db", "main-cfg"}, // a duplicate of a config key for the same instance is kept once
		"https://gitlab.freedesktop.org":  {"fd-db"},
		"https://GitLab.FreeDesktop.org/": {"fd-db2"}, // tags compare normalized
		"https://gone.example.org":        {"orphan-1", "orphan-2"},
		"http://gitlab.freedesktop.org":   {"fd-http"}, // a scheme twin of the configured instance
	}
	pools, orphans, err := partitionGitLabTokens(instances, stored)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"https://gitlab.com":             {"main-cfg", "main-db"},
		"https://gitlab.freedesktop.org": {"fd-cfg", "fd-db", "fd-db2", "fd-http"},
		"https://salsa.debian.org":       nil,
	}
	for base, toks := range want {
		got := append([]string(nil), pools[base]...)
		sort.Strings(got)
		sort.Strings(toks)
		if !reflect.DeepEqual(got, toks) && !(len(got) == 0 && len(toks) == 0) {
			t.Errorf("pool %s = %v, want %v", base, pools[base], toks)
		}
	}
	if len(pools) != len(want) {
		t.Errorf("pools has %d instances, want %d: %v", len(pools), len(want), pools)
	}
	if orphans["https://gone.example.org"] != 2 || len(orphans) != 1 {
		t.Errorf("orphans = %v, want 2 tokens under the unconfigured instance only", orphans)
	}

	// The same token under two instances is refused.
	_, _, err = partitionGitLabTokens(instances, map[string][]string{"https://salsa.debian.org": {"fd-cfg"}})
	if err == nil || !strings.Contains(err.Error(), "https://gitlab.freedesktop.org") || !strings.Contains(err.Error(), "https://salsa.debian.org") {
		t.Errorf("a token under two instances = %v, want an error naming both", err)
	}
	_, _, err = partitionGitLabTokens([]config.GitLabInstance{main, {WebBase: fd.WebBase, APIURL: fd.APIURL, APIKeys: []string{"main-cfg"}}}, nil)
	if err == nil {
		t.Error("a token in two instances' config entries must be refused")
	}

	// An unparseable stored tag is an orphan, never a match.
	_, orphans, err = partitionGitLabTokens(instances, map[string][]string{"not a url": {"x"}})
	if err != nil || orphans["not a url"] != 1 {
		t.Errorf("unparseable tag: orphans=%v err=%v", orphans, err)
	}
}
