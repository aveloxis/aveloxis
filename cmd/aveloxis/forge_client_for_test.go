// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

type namedClient struct {
	platform.Client
	name string
}

func TestForgeClientForNeverCrossesInstances(t *testing.T) {
	gh, gl := &namedClient{name: "github"}, &namedClient{name: "gitlab"}
	cases := []struct {
		p    model.Platform
		want string
	}{
		{model.PlatformGitHub, "github"},
		{model.PlatformGitLab, "gitlab"},
		{model.GitLabInstanceIDMin, ""},
		{model.GitLabInstanceIDMax, ""},
		{model.PlatformGenericGit, ""},
		{4, ""},
	}
	for _, tc := range cases {
		c, ok := forgeClientFor(tc.p, gh, gl)
		got := ""
		if ok {
			got = c.(*namedClient).name
		}
		if got != tc.want || ok != (tc.want != "") {
			t.Errorf("forgeClientFor(%d) = (%q, %v), want %q", tc.p, got, ok, tc.want)
		}
	}
}

// Both operator commands route through forgeClientFor, so neither can fall
// through to the GitHub client for a GitLab instance repo.
func TestOperatorCommandsRouteThroughForgeClientFor(t *testing.T) {
	for _, f := range []string{"cmd/aveloxis/backfill_repo_metadata.go", "cmd/aveloxis/heal_collection_gaps.go"} {
		src := srctest.StripGoComments(srctest.Read(t, f))
		if !strings.Contains(src, "forgeClientFor(") {
			t.Errorf("%s must pick its client with forgeClientFor", f)
		}
		if strings.Contains(src, "client := ghClient") || strings.Contains(src, "case model.PlatformGitLab:") {
			t.Errorf("%s still selects a client inline — a GitLab instance repo could reach the wrong client", f)
		}
	}
}
