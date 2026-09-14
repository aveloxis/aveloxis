// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
)

// fakeClient is a minimal implementation of platform.Client for testing
// ClientForRepo. Only the Platform() method is used; all other methods panic.
type fakeClient struct {
	platform.Client // embed to satisfy the interface without implementing every method
	plat            model.Platform
}

func (f *fakeClient) Platform() model.Platform { return f.plat }

// testGitLabInstances builds a router with gitlab.com (id 2) and a
// self-hosted instance under a sub-path whose API is on another host.
func testGitLabInstances(t *testing.T) (*gitlab.Instances, *gitlab.Client, *gitlab.Client) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	com, err := gitlab.New(model.PlatformGitLab, "https://gitlab.com", "https://gitlab.com/api/v4", platform.NewKeyPool([]string{"com"}, logger), logger)
	if err != nil {
		t.Fatal(err)
	}
	edu, err := gitlab.New(model.GitLabInstanceIDMin, "https://code.example.invalid/gitlab", "https://api.example.invalid/api/v4", platform.NewKeyPool([]string{"edu"}, logger), logger)
	if err != nil {
		t.Fatal(err)
	}
	router, err := gitlab.NewInstances([]*gitlab.Instance{
		{ID: model.PlatformGitLab, WebBase: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", Client: com},
		{ID: model.GitLabInstanceIDMin, WebBase: "https://code.example.invalid/gitlab", APIURL: "https://api.example.invalid/api/v4", Client: edu},
		{ID: model.GitLabInstanceIDMin + 1, WebBase: "https://keyless.example.invalid", APIURL: "https://keyless.example.invalid/api/v4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return router, com, edu
}

func TestClientForRepo_GitHub(t *testing.T) {
	gh := &fakeClient{plat: model.PlatformGitHub}
	gl, _, _ := testGitLabInstances(t)

	client, owner, repo, err := ClientForRepo("https://github.com/torvalds/linux", gh, gl)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client != gh {
		t.Error("expected GitHub client to be returned")
	}
	if owner != "torvalds" {
		t.Errorf("owner = %q, want %q", owner, "torvalds")
	}
	if repo != "linux" {
		t.Errorf("repo = %q, want %q", repo, "linux")
	}
}

// v0.30.0: a GitLab URL gets the client of the instance it lives under.
func TestClientForRepo_GitLabInstances(t *testing.T) {
	gh := &fakeClient{plat: model.PlatformGitHub}
	gl, com, edu := testGitLabInstances(t)

	cases := []struct {
		url                 string
		want                platform.Client
		wantOwner, wantRepo string
	}{
		{"https://gitlab.com/gnachman/iterm2", com, "gnachman", "iterm2"},
		{"https://gitlab.com/group/subgroup/project", com, "group/subgroup", "project"},
		{"https://code.example.invalid/gitlab/group/project.git", edu, "group", "project"},
	}
	for _, tc := range cases {
		client, owner, repo, err := ClientForRepo(tc.url, gh, gl)
		if err != nil || client != tc.want || owner != tc.wantOwner || repo != tc.wantRepo {
			t.Errorf("%s: (%v, %q, %q, %v), want its instance's client and %q/%q", tc.url, client, owner, repo, err, tc.wantOwner, tc.wantRepo)
		}
	}
	for _, u := range []string{
		"https://keyless.example.invalid/group/project", // configured, no keys
		"https://gitlab.unconfigured.invalid/g/p",       // looks like GitLab, no instance
	} {
		if _, _, _, err := ClientForRepo(u, gh, gl); !errors.Is(err, gitlab.ErrInstanceNotConfigured) {
			t.Errorf("%s: err = %v, want ErrInstanceNotConfigured", u, err)
		}
	}
}

func TestClientForRepo_InvalidURL(t *testing.T) {
	gh := &fakeClient{plat: model.PlatformGitHub}
	gl, _, _ := testGitLabInstances(t)

	_, _, _, err := ClientForRepo("not-a-url", gh, gl)
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

func TestClientForRepo_UnsupportedPlatform(t *testing.T) {
	gh := &fakeClient{plat: model.PlatformGitHub}
	gl, _, _ := testGitLabInstances(t)

	// Bitbucket is not recognized by ParseRepoURL, so this should return an error.
	_, _, _, err := ClientForRepo("https://bitbucket.org/atlassian/stash", gh, gl)
	if err == nil {
		t.Fatal("expected error for unsupported platform (Bitbucket)")
	}
}

func TestClientForRepo_EmptyURL(t *testing.T) {
	gh := &fakeClient{plat: model.PlatformGitHub}
	gl, _, _ := testGitLabInstances(t)

	_, _, _, err := ClientForRepo("", gh, gl)
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}
