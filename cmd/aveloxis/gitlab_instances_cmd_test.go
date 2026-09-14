// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

// v0.30.0: `aveloxis gitlab-instances` is the operator's check after a
// deploy — which platform_id each GitLab instance has, where its keys go,
// how many keys and repositories it has, what is registered but no longer
// configured, stored keys that load nowhere, and misrouted repositories.
func TestGitLabInstancesReport(t *testing.T) {
	instances := []config.GitLabInstance{
		{WebBase: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", APIKeys: []string{"a"}, Primary: true},
		{WebBase: "https://gitlab.freedesktop.org", APIURL: "https://api.freedesktop.example/api/v4"},
		{WebBase: "https://unregistered.example.org", APIURL: "https://unregistered.example.org/api/v4"},
	}
	registry := map[string]model.Platform{
		"https://gitlab.com":             model.PlatformGitLab,
		"https://gitlab.freedesktop.org": model.GitLabInstanceIDMin,
		"https://removed.example.org":    model.GitLabInstanceIDMin + 1,
	}
	pools := map[string][]string{"https://gitlab.com": {"a", "b"}, "https://gitlab.freedesktop.org": nil}
	repos := map[model.Platform]int{model.PlatformGitLab: 96, model.GitLabInstanceIDMin: 3, model.GitLabInstanceIDMin + 1: 1}
	orphans := map[string]int{"https://gone.example.org": 2}
	misrouted := []db.MisroutedRepo{{RepoID: 42, GitURL: "https://gitlab.freedesktop.org/g/r", Instance: "https://gitlab.freedesktop.org"}}

	var buf bytes.Buffer
	renderGitLabInstances(&buf, instances, registry, pools, repos, orphans, misrouted)
	out := buf.String()
	for _, want := range []string{
		"2\thttps://gitlab.com\thttps://gitlab.com/api/v4\tyes (main)\t2\t96",
		"100\thttps://gitlab.freedesktop.org\thttps://api.freedesktop.example/api/v4\tyes, NO KEYS\t0\t3",
		"-\thttps://unregistered.example.org\thttps://unregistered.example.org/api/v4\tNOT REGISTERED — run aveloxis migrate\t0\t0",
		"101\thttps://removed.example.org\t-\tregistered, not configured\t-\t1",
		"https://gone.example.org: 2 stored key(s) load nowhere",
		"repo 42 https://gitlab.freedesktop.org/g/r",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report is missing %q\n---\n%s", want, out)
		}
	}
}
