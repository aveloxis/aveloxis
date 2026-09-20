// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"strings"
	"testing"
)

func TestValidateRepoURL_GitHub(t *testing.T) {
	result := ValidateRepoURL("https://github.com/chaoss/augur")
	if result.Platform != "github" {
		t.Errorf("platform = %q, want %q", result.Platform, "github")
	}
	if !result.Valid {
		t.Errorf("GitHub URL should be valid")
	}
	if result.GitOnly {
		t.Errorf("GitHub URL should not be git-only")
	}
}

func TestValidateRepoURL_GitLab(t *testing.T) {
	result := ValidateRepoURL("https://gitlab.com/fdroid/fdroidclient")
	if result.Platform != "gitlab" {
		t.Errorf("platform = %q, want %q", result.Platform, "gitlab")
	}
	if !result.Valid {
		t.Errorf("GitLab URL should be valid")
	}
}

func TestValidateRepoURL_GenericGit(t *testing.T) {
	result := ValidateRepoURL("https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git")
	if result.Platform != "git" {
		t.Errorf("platform = %q, want %q", result.Platform, "git")
	}
	if !result.Valid {
		t.Errorf("generic git URL should be valid")
	}
	if !result.GitOnly {
		t.Errorf("generic git URL should be git-only")
	}
}

func TestValidateRepoURL_Invalid(t *testing.T) {
	result := ValidateRepoURL("not-a-url")
	if result.Valid {
		t.Errorf("invalid URL should not be valid")
	}
}

func TestValidateRepoURL_NoPath(t *testing.T) {
	result := ValidateRepoURL("https://github.com")
	if result.Valid {
		t.Errorf("URL with no repo path should not be valid")
	}
}

func TestValidateRepoURL_MissingScheme(t *testing.T) {
	result := ValidateRepoURL("github.com/chaoss/augur")
	// Should auto-fix by prepending https://
	if !result.Valid {
		t.Errorf("URL without scheme should be auto-fixed and valid")
	}
}

// v0.29.57, Copilot review 5261384568: the GUI accepted and stored a repo
// URL carrying credentials, which run-scorecard then forwarded to a
// subprocess. Refused here for every platform branch — GitHub, GitLab and
// generic git — and for the schemeless spelling the validator auto-fixes.
func TestValidateRepoURL_RefusesUserinfo(t *testing.T) {
	for _, u := range []string{
		"https://user:token@github.com/owner/repo",
		"https://token@gitlab.com/group/project",
		"https://user@git.example.invalid/owner/repo",
		"user:token@github.com/owner/repo",
	} {
		result := ValidateRepoURL(u)
		if result.Valid {
			t.Errorf("ValidateRepoURL(%q) accepted a URL with userinfo", u)
		}
		if !strings.Contains(result.Error, "credentials") {
			t.Errorf("ValidateRepoURL(%q).Error = %q, want it to say the URL carries credentials", u, result.Error)
		}
	}
	if r := ValidateRepoURL("https://github.com/owner/repo@v1"); !r.Valid {
		t.Errorf("an @ in the path is not userinfo: %q", r.Error)
	}
}
