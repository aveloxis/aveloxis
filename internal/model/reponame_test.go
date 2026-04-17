package model

import "testing"

func TestNormalizeRepoName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"clean name unchanged", "naturf", "naturf"},
		{"strips .git suffix", "naturf.git", "naturf"},
		{"strips trailing slash", "naturf/", "naturf"},
		{"strips trailing slash before .git not present", "hello-world/", "hello-world"},
		{"empty input", "", ""},
		{"whitespace trimmed", "  naturf  ", "naturf"},
		{"whitespace with .git", "  naturf.git  ", "naturf"},
		// The current rules only strip one .git — nested ".git.git" retains
		// an inner suffix. This prevents eating repo names that legitimately
		// end in ".git" (rare, but possible: "foo.git-backup").
		{"only strips one .git", "repo.git.git", "repo.git"},
		{"no suffix on odd name", "my.repo", "my.repo"},
		{"mid-string .git preserved", "dot.git.repo", "dot.git.repo"},
		{"dot in name preserved", "project.name", "project.name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeRepoName(tt.input)
			if got != tt.want {
				t.Errorf("NormalizeRepoName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestNormalizeRepoName_Idempotent(t *testing.T) {
	// Running NormalizeRepoName twice must equal running it once.
	inputs := []string{"naturf.git", "repo/", "already-clean", "", "  foo.git  "}
	for _, in := range inputs {
		once := NormalizeRepoName(in)
		twice := NormalizeRepoName(once)
		if once != twice {
			t.Errorf("not idempotent for %q: once=%q twice=%q", in, once, twice)
		}
	}
}
