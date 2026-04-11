package collector

import (
	"testing"
)

// TestValidateGitURL verifies that git URLs are validated before being passed
// to git commands. This prevents flag injection via URLs that start with "-".
func TestValidateGitURL_ValidHTTPS(t *testing.T) {
	if err := validateGitURL("https://github.com/org/repo.git"); err != nil {
		t.Errorf("valid HTTPS URL rejected: %v", err)
	}
}

func TestValidateGitURL_ValidHTTP(t *testing.T) {
	if err := validateGitURL("http://github.com/org/repo.git"); err != nil {
		t.Errorf("valid HTTP URL rejected: %v", err)
	}
}

func TestValidateGitURL_ValidGitProtocol(t *testing.T) {
	if err := validateGitURL("git://github.com/org/repo.git"); err != nil {
		t.Errorf("valid git:// URL rejected: %v", err)
	}
}

func TestValidateGitURL_ValidSSH(t *testing.T) {
	if err := validateGitURL("ssh://git@github.com/org/repo.git"); err != nil {
		t.Errorf("valid SSH URL rejected: %v", err)
	}
}

func TestValidateGitURL_FlagInjection(t *testing.T) {
	// A URL starting with "--" could be interpreted as a git flag.
	if err := validateGitURL("--upload-pack=evil"); err == nil {
		t.Error("flag injection URL should be rejected")
	}
}

func TestValidateGitURL_DashPrefix(t *testing.T) {
	if err := validateGitURL("-oProxyCommand=evil"); err == nil {
		t.Error("dash-prefix URL should be rejected")
	}
}

func TestValidateGitURL_Empty(t *testing.T) {
	if err := validateGitURL(""); err == nil {
		t.Error("empty URL should be rejected")
	}
}

func TestValidateGitURL_NoScheme(t *testing.T) {
	// Bare hostnames like "github.com/org/repo" are not valid git URLs for clone.
	if err := validateGitURL("github.com/org/repo"); err == nil {
		t.Error("URL without scheme should be rejected")
	}
}

func TestValidateGitURL_FileScheme(t *testing.T) {
	// file:// URLs could access local filesystem — reject for safety.
	if err := validateGitURL("file:///etc/passwd"); err == nil {
		t.Error("file:// URLs should be rejected")
	}
}

func TestValidateGitURL_ScpSyntax(t *testing.T) {
	// git@github.com:org/repo.git — SCP-style syntax used by SSH.
	if err := validateGitURL("git@github.com:org/repo.git"); err != nil {
		t.Errorf("SCP-style SSH URL rejected: %v", err)
	}
}

func TestValidateGitURL_Newlines(t *testing.T) {
	if err := validateGitURL("https://github.com/org/repo\n--upload-pack=evil"); err == nil {
		t.Error("URL with newlines should be rejected")
	}
}

func TestValidateGitURL_NullBytes(t *testing.T) {
	if err := validateGitURL("https://github.com/org/repo\x00--evil"); err == nil {
		t.Error("URL with null bytes should be rejected")
	}
}
