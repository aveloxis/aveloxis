package collector

import (
	"testing"

	"github.com/augurlabs/aveloxis/internal/model"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// fakeClient is a minimal implementation of platform.Client for testing
// ClientForRepo. Only the Platform() method is used; all other methods panic.
type fakeClient struct {
	platform.Client // embed to satisfy the interface without implementing every method
	plat            model.Platform
}

func (f *fakeClient) Platform() model.Platform { return f.plat }

func TestClientForRepo_GitHub(t *testing.T) {
	gh := &fakeClient{plat: model.PlatformGitHub}
	gl := &fakeClient{plat: model.PlatformGitLab}

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

func TestClientForRepo_GitLab(t *testing.T) {
	gh := &fakeClient{plat: model.PlatformGitHub}
	gl := &fakeClient{plat: model.PlatformGitLab}

	client, owner, repo, err := ClientForRepo("https://gitlab.com/gnachman/iterm2", gh, gl)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client != gl {
		t.Error("expected GitLab client to be returned")
	}
	if owner != "gnachman" {
		t.Errorf("owner = %q, want %q", owner, "gnachman")
	}
	if repo != "iterm2" {
		t.Errorf("repo = %q, want %q", repo, "iterm2")
	}
}

func TestClientForRepo_GitLabNested(t *testing.T) {
	gh := &fakeClient{plat: model.PlatformGitHub}
	gl := &fakeClient{plat: model.PlatformGitLab}

	client, owner, repo, err := ClientForRepo("https://gitlab.com/group/subgroup/project", gh, gl)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client != gl {
		t.Error("expected GitLab client to be returned")
	}
	if owner != "group/subgroup" {
		t.Errorf("owner = %q, want %q", owner, "group/subgroup")
	}
	if repo != "project" {
		t.Errorf("repo = %q, want %q", repo, "project")
	}
}

func TestClientForRepo_InvalidURL(t *testing.T) {
	gh := &fakeClient{plat: model.PlatformGitHub}
	gl := &fakeClient{plat: model.PlatformGitLab}

	_, _, _, err := ClientForRepo("not-a-url", gh, gl)
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

func TestClientForRepo_UnsupportedPlatform(t *testing.T) {
	gh := &fakeClient{plat: model.PlatformGitHub}
	gl := &fakeClient{plat: model.PlatformGitLab}

	// Bitbucket is not recognized by ParseRepoURL, so this should return an error.
	_, _, _, err := ClientForRepo("https://bitbucket.org/atlassian/stash", gh, gl)
	if err == nil {
		t.Fatal("expected error for unsupported platform (Bitbucket)")
	}
}

func TestClientForRepo_EmptyURL(t *testing.T) {
	gh := &fakeClient{plat: model.PlatformGitHub}
	gl := &fakeClient{plat: model.PlatformGitLab}

	_, _, _, err := ClientForRepo("", gh, gl)
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}
