package web

import (
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
