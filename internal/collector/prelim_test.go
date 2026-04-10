package collector

import "testing"

func TestNormalizeRepoURL(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"https://github.com/chaoss/augur", "github.com/chaoss/augur"},
		{"https://github.com/chaoss/augur/", "github.com/chaoss/augur"},
		{"https://github.com/chaoss/augur.git", "github.com/chaoss/augur"},
		{"http://github.com/CHAOSS/Augur", "github.com/chaoss/augur"},
		{"https://gitlab.com/group/project.git/", "gitlab.com/group/project"},
	}

	for _, tt := range tests {
		got := normalizeRepoURL(tt.input)
		if got != tt.want {
			t.Errorf("normalizeRepoURL(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseOwnerName(t *testing.T) {
	tests := []struct {
		url       string
		wantOwner string
		wantName  string
	}{
		{"https://github.com/torvalds/linux", "torvalds", "linux"},
		{"https://github.com/chaoss/augur.git", "chaoss", "augur"},
		{"https://gitlab.com/group/subgroup/project", "group/subgroup", "project"},
		{"https://gitlab.com/a/b/c/d", "a/b/c", "d"},
		// Too few segments.
		{"https://github.com/", "", ""},
		{"https://github.com", "", ""},
	}

	for _, tt := range tests {
		owner, name := parseOwnerName(tt.url)
		if owner != tt.wantOwner {
			t.Errorf("parseOwnerName(%q) owner = %q, want %q", tt.url, owner, tt.wantOwner)
		}
		if name != tt.wantName {
			t.Errorf("parseOwnerName(%q) name = %q, want %q", tt.url, name, tt.wantName)
		}
	}
}

func TestNormalizeRepoURL_DetectsRedirect(t *testing.T) {
	// Simulate a redirect: old URL and new URL should normalize differently
	// if the org or repo name changed.
	old := normalizeRepoURL("https://github.com/old-org/old-repo")
	new := normalizeRepoURL("https://github.com/new-org/new-repo")

	if old == new {
		t.Error("expected different normalized URLs for different orgs/repos")
	}

	// Same repo, just different casing or trailing slash — should match.
	a := normalizeRepoURL("https://github.com/Chaoss/Augur/")
	b := normalizeRepoURL("https://github.com/chaoss/augur")
	if a != b {
		t.Errorf("expected same normalized URL, got %q vs %q", a, b)
	}
}

func TestPrelimResult_Defaults(t *testing.T) {
	r := &PrelimResult{}
	if r.Skip {
		t.Error("default PrelimResult should not skip")
	}
	if r.Redirected {
		t.Error("default PrelimResult should not be redirected")
	}
}
