package importers

import (
	"testing"
)

// TestNormalizeRepoURL — Apache projects.json often gives bug-database URLs
// like https://github.com/apache/accumulo/issues or /pulls. Our normalizer
// must strip those trailing segments and the .git suffix so downstream
// ParseRepoURL + UpsertRepo all see the canonical form.
func TestNormalizeRepoURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"https://github.com/apache/accumulo/issues", "https://github.com/apache/accumulo"},
		{"https://github.com/apache/accumulo/pulls", "https://github.com/apache/accumulo"},
		{"https://github.com/apache/accumulo.git", "https://github.com/apache/accumulo"},
		{"https://github.com/apache/accumulo/", "https://github.com/apache/accumulo"},
		{"https://github.com/apache/accumulo", "https://github.com/apache/accumulo"},
		{"  https://github.com/apache/accumulo  ", "https://github.com/apache/accumulo"},
		// Non-github URL passes through (stripping trailing slash only).
		{"https://gitbox.apache.org/repos/asf/accumulo.git", "https://gitbox.apache.org/repos/asf/accumulo"},
		{"", ""},
	}
	for _, c := range cases {
		got := NormalizeRepoURL(c.in)
		if got != c.want {
			t.Errorf("NormalizeRepoURL(%q) = %q, want %q — canonicalization is what makes the dedupe in UpsertRepo actually work", c.in, got, c.want)
		}
	}
}

// TestProjectStructExposesCoreFields — the shared Project type must be
// usable by cmd/aveloxis without additional getters. Keep it a plain struct.
func TestProjectStructExposesCoreFields(t *testing.T) {
	p := Project{
		Foundation: "cncf",
		Status:     "graduated",
		Name:       "Kubernetes",
		Homepage:   "https://kubernetes.io/",
		RepoURLs:   []string{"https://github.com/kubernetes/kubernetes"},
	}
	if p.Foundation != "cncf" || p.Status != "graduated" || p.Name != "Kubernetes" || len(p.RepoURLs) != 1 {
		t.Error("Project struct must expose Foundation, Status, Name, Homepage, RepoURLs as exported fields")
	}
}
