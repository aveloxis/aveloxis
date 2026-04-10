package platform

import (
	"testing"

	"github.com/augurlabs/aveloxis/internal/model"
)

func TestParseRepoURL(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		wantPlat  model.Platform
		wantOwner string
		wantRepo  string
		wantErr   bool
	}{
		// GitHub
		{
			name:      "github simple",
			url:       "https://github.com/torvalds/linux",
			wantPlat:  model.PlatformGitHub,
			wantOwner: "torvalds",
			wantRepo:  "linux",
		},
		{
			name:      "github with .git suffix",
			url:       "https://github.com/torvalds/linux.git",
			wantPlat:  model.PlatformGitHub,
			wantOwner: "torvalds",
			wantRepo:  "linux",
		},
		{
			name:      "github with trailing slash",
			url:       "https://github.com/torvalds/linux/",
			wantPlat:  model.PlatformGitHub,
			wantOwner: "torvalds",
			wantRepo:  "linux",
		},
		{
			name:    "github too many segments",
			url:     "https://github.com/a/b/c",
			wantErr: true,
		},

		// GitLab - standard
		{
			name:      "gitlab simple",
			url:       "https://gitlab.com/fdroid/fdroidclient",
			wantPlat:  model.PlatformGitLab,
			wantOwner: "fdroid",
			wantRepo:  "fdroidclient",
		},
		{
			name:      "gitlab with .git suffix",
			url:       "https://gitlab.com/fdroid/fdroidclient.git",
			wantPlat:  model.PlatformGitLab,
			wantOwner: "fdroid",
			wantRepo:  "fdroidclient",
		},

		// GitLab - nested subgroups
		{
			name:      "gitlab one subgroup",
			url:       "https://gitlab.com/gitlab-org/security/gitlab",
			wantPlat:  model.PlatformGitLab,
			wantOwner: "gitlab-org/security",
			wantRepo:  "gitlab",
		},
		{
			name:      "gitlab deep subgroups",
			url:       "https://gitlab.com/a/b/c/d/project",
			wantPlat:  model.PlatformGitLab,
			wantOwner: "a/b/c/d",
			wantRepo:  "project",
		},

		// GitLab - self-hosted with "gitlab" in hostname
		{
			name:      "self-hosted gitlab",
			url:       "https://gitlab.freedesktop.org/mesa/mesa",
			wantPlat:  model.PlatformGitLab,
			wantOwner: "mesa",
			wantRepo:  "mesa",
		},

		// Errors
		{
			name:    "empty path",
			url:     "https://github.com/",
			wantErr: true,
		},
		{
			name:    "no scheme",
			url:     "github.com/torvalds/linux",
			wantErr: true,
		},
		{
			name:    "unknown host",
			url:     "https://bitbucket.org/owner/repo",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseRepoURL(tt.url)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Platform != tt.wantPlat {
				t.Errorf("platform = %v, want %v", got.Platform, tt.wantPlat)
			}
			if got.Owner != tt.wantOwner {
				t.Errorf("owner = %q, want %q", got.Owner, tt.wantOwner)
			}
			if got.Repo != tt.wantRepo {
				t.Errorf("repo = %q, want %q", got.Repo, tt.wantRepo)
			}
		})
	}
}

func TestParseRepoURLWithHints(t *testing.T) {
	// Self-hosted GitLab without "gitlab" in hostname.
	hints := map[string]bool{"code.internal.company.com": true}
	got, err := ParseRepoURLWithHints("https://code.internal.company.com/infra/deploy-tools", hints)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Platform != model.PlatformGitLab {
		t.Errorf("platform = %v, want GitLab", got.Platform)
	}
	if got.Owner != "infra" {
		t.Errorf("owner = %q, want %q", got.Owner, "infra")
	}
	if got.Repo != "deploy-tools" {
		t.Errorf("repo = %q, want %q", got.Repo, "deploy-tools")
	}
}

func TestGitLabProjectPath(t *testing.T) {
	r := RepoURL{
		Platform: model.PlatformGitLab,
		Owner:    "group/subgroup",
		Repo:     "project",
	}
	want := "group%2Fsubgroup%2Fproject"
	if got := r.GitLabProjectPath(); got != want {
		t.Errorf("GitLabProjectPath() = %q, want %q", got, want)
	}
}
