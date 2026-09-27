// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

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

// TestNormalizeRepoGitURL: the one spelling of the stored clone URL
// (worklist follow-up 8). UpsertRepo, FindRepoByURL, the rename writers, the
// URL parser, the web validator and the comparison keys all call it, so a
// ".git" or trailing-"/" variant of a stored URL is the same key everywhere.
// The rule: trim space, then every trailing "/" and ".git" to a fixed point
// (unlike the NAME rule's one ".git": the stored spelling must round-trip
// through the lookup).
func TestNormalizeRepoGitURL(t *testing.T) {
	for _, tt := range []struct{ name, input, want string }{
		{"canonical unchanged", "https://github.com/o/r", "https://github.com/o/r"},
		{".git", "https://github.com/o/r.git", "https://github.com/o/r"},
		{"trailing slash", "https://github.com/o/r/", "https://github.com/o/r"},
		{".git then slash", "https://github.com/o/r.git/", "https://github.com/o/r"},
		{"surrounding space", "  https://github.com/o/r.git  ", "https://github.com/o/r"},
		// Every trailing ".git", unlike the NAME rule: the stored spelling must
		// be a fixed point, or "…/r.git" stored from "…/r.git.git" would miss
		// its own lookup.
		{"every .git", "https://github.com/o/r.git.git", "https://github.com/o/r"},
		{"mid-path .git kept", "https://github.com/o.git/r", "https://github.com/o.git/r"},
		{"case kept (the store resolves case itself)", "https://github.com/O/R.git", "https://github.com/O/R"},
		// A "/.git" tail (review round 1 of the fix): stripping ".git" exposes
		// a trailing slash, which the stored spelling never carries.
		{"slash then .git", "https://github.com/o/r/.git", "https://github.com/o/r"},
		{"empty", "", ""},
	} {
		got := NormalizeRepoGitURL(tt.input)
		if got != tt.want {
			t.Errorf("%s: NormalizeRepoGitURL(%q) = %q, want %q", tt.name, tt.input, got, tt.want)
		}
		// One stored spelling: normalizing it again changes nothing.
		if again := NormalizeRepoGitURL(got); again != got {
			t.Errorf("%s: NormalizeRepoGitURL is not idempotent: %q -> %q -> %q", tt.name, tt.input, got, again)
		}
	}
}
