// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

func TestIsWorkflowPath(t *testing.T) {
	cases := map[string]bool{
		"/r/.github/workflows/ci.yml":       true,
		"/r/.github/workflows/release.yaml": true,
		"/r/.github/workflows/nested.txt":   false,
		"/r/.github/dependabot.yml":         false,
		"/r/workflows/ci.yml":               false,
		"/r/docs/ci.yml":                    false,
	}
	for path, want := range cases {
		if got := isWorkflowPath(path); got != want {
			t.Errorf("manifest.IsWorkflowPath(%q) = %v, want %v", path, got, want)
		}
	}
}

func TestParseWorkflowUses(t *testing.T) {
	wf := `name: CI
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: setup
        uses: actions/setup-python@v5.1.0
      - uses: github/codeql-action/init@8214744c546c1e5c8f03dde8fab3a7353211988d # v3.26.7
      - uses: ./local-action
      - uses: docker://alpine:3.19
      - uses: actions/checkout@v4
`
	deps := parseWorkflowUses(wf)
	byName := map[string]model.LibyearDep{}
	for _, d := range deps {
		byName[d.Name+"@"+d.Version] = d
	}
	if len(deps) != 3 {
		t.Fatalf("want 3 deps (dedup + local/docker skipped), got %d: %v", len(deps), byName)
	}
	if d, ok := byName["actions/checkout@v4"]; !ok || d.Type != model.ScopeBuild || d.Manager != "githubactions" {
		t.Errorf("actions/checkout@v4 must be build-scope githubactions, got %+v", d)
	}
	if _, ok := byName["actions/setup-python@v5.1.0"]; !ok {
		t.Error("versioned tag reference missing")
	}
	// Subdirectory actions attribute to the owning repo, SHA pin kept.
	if d, ok := byName["github/codeql-action@8214744c546c1e5c8f03dde8fab3a7353211988d"]; !ok {
		t.Errorf("subdir action must attribute to owner/repo with the SHA ref, got %v", byName)
	} else if d.Requirement != "github/codeql-action/init@8214744c546c1e5c8f03dde8fab3a7353211988d" {
		t.Errorf("requirement must keep the full uses ref, got %q", d.Requirement)
	}
}
