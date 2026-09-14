// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

type namedClient struct {
	platform.Client
	name string
}

func TestForgeClientForNeverCrossesInstances(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	com, err := gitlab.New(model.PlatformGitLab, "https://gitlab.com", "https://gitlab.com/api/v4", platform.NewKeyPool([]string{"com"}, logger), logger)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := gitlab.New(model.GitLabInstanceIDMin, "https://gitlab.freedesktop.invalid", "https://gitlab.freedesktop.invalid/api/v4", platform.NewKeyPool([]string{"fd"}, logger), logger)
	if err != nil {
		t.Fatal(err)
	}
	gls, err := gitlab.NewInstances([]*gitlab.Instance{
		{ID: model.PlatformGitLab, WebBase: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", Client: com},
		{ID: model.GitLabInstanceIDMin, WebBase: "https://gitlab.freedesktop.invalid", APIURL: "https://gitlab.freedesktop.invalid/api/v4", Client: fd},
		{ID: model.GitLabInstanceIDMin + 1, WebBase: "https://keyless.invalid", APIURL: "https://keyless.invalid/api/v4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	gh := &namedClient{name: "github"}

	cases := []struct {
		name    string
		p       model.Platform
		url     string
		want    platform.Client
		wantErr error
	}{
		{"github", model.PlatformGitHub, "https://github.com/o/r", gh, nil},
		{"gitlab.com", model.PlatformGitLab, "https://gitlab.com/g/r", com, nil},
		{"self-hosted instance", model.GitLabInstanceIDMin, "https://gitlab.freedesktop.invalid/g/r", fd, nil},
		{"keyless instance", model.GitLabInstanceIDMin + 1, "https://keyless.invalid/g/r", nil, gitlab.ErrInstanceNotConfigured},
		{"unregistered instance id", model.GitLabInstanceIDMax, "https://other.invalid/g/r", nil, gitlab.ErrInstanceNotConfigured},
		{"platform 2 row on another instance's URL", model.PlatformGitLab, "https://gitlab.freedesktop.invalid/g/r", nil, gitlab.ErrInstanceMismatch},
		{"generic git", model.PlatformGenericGit, "https://git.example.org/x/y", nil, errNoForgeAPI},
	}
	for _, tc := range cases {
		got, err := forgeClientFor(tc.p, tc.url, gh, gls)
		if tc.wantErr != nil {
			if !errors.Is(err, tc.wantErr) || got != nil {
				t.Errorf("%s: (%v, %v), want %v", tc.name, got, err, tc.wantErr)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%s: (%v, %v), want its own client", tc.name, got, err)
		}
	}
}

// Both operator commands route through forgeClientFor, so neither can fall
// through to the GitHub client — or another instance's — for a GitLab repo.
func TestOperatorCommandsRouteThroughForgeClientFor(t *testing.T) {
	for _, f := range []string{"cmd/aveloxis/backfill_repo_metadata.go", "cmd/aveloxis/heal_collection_gaps.go"} {
		src := srctest.StripGoComments(srctest.Read(t, f))
		if !strings.Contains(src, "forgeClientFor(") {
			t.Errorf("%s must pick its client with forgeClientFor", f)
		}
		if strings.Contains(src, "client := ghClient") || strings.Contains(src, "case model.PlatformGitLab:") || strings.Contains(src, "gitlab.New(") {
			t.Errorf("%s still selects or builds a client inline — a GitLab instance repo could reach the wrong client", f)
		}
	}
}
