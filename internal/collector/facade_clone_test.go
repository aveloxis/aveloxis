package collector

import (
	"testing"
)

// TestCloneURLMismatch verifies that when a bare clone exists at repo_N/ but
// its remote.origin.url doesn't match the expected gitURL, we detect the mismatch
// and re-clone instead of reusing the stale clone.
func TestCloneOriginURL_Extraction(t *testing.T) {
	// cloneOriginURL should return the remote.origin.url from a git config.
	// For a bare repo, git config is at <path>/config.
	// We test the helper function that reads it.

	// Valid origin URL — should be extracted correctly.
	cfg := `[core]
	repositoryformatversion = 0
	filemode = true
	bare = true
[remote "origin"]
	url = https://github.com/augurlabs/augur.git
	fetch = +refs/*:refs/*
`
	url := parseOriginURL(cfg)
	if url != "https://github.com/augurlabs/augur.git" {
		t.Errorf("parseOriginURL = %q, want augur URL", url)
	}
}

func TestCloneOriginURL_NoOrigin(t *testing.T) {
	cfg := `[core]
	bare = true
`
	url := parseOriginURL(cfg)
	if url != "" {
		t.Errorf("no origin: %q, want empty", url)
	}
}

func TestCloneOriginURL_Empty(t *testing.T) {
	if url := parseOriginURL(""); url != "" {
		t.Errorf("empty: %q", url)
	}
}

func TestNormalizeCloneURL_Matches(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"https://github.com/org/repo.git", "https://github.com/org/repo", true},
		{"https://github.com/org/repo.git", "https://github.com/org/repo.git", true},
		{"https://github.com/Org/Repo.git", "https://github.com/org/repo", true},
		{"https://github.com/org/repo", "https://github.com/other/repo", false},
		{"https://github.com/augurlabs/augur.git", "https://github.com/aveloxis/OCDX-Specification.git", false},
	}
	for _, tt := range tests {
		got := normalizeCloneURL(tt.a) == normalizeCloneURL(tt.b)
		if got != tt.want {
			t.Errorf("normalizeCloneURL(%q) == normalizeCloneURL(%q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}
