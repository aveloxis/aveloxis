// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.47 (summary/19 P4) — GitHub Actions inventory + OSV matching
// tests. The ref-evaluation matrix derives from the tj-actions/
// changed-files advisory (GHSA-mrrh-fwg8-r2c3: ECOSYSTEM range
// introduced 0 → fixed 46.0.1), the P4 canonical ground truth.

package collector

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
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
			t.Errorf("isWorkflowPath(%q) = %v, want %v", path, got, want)
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
	byName := map[string]libyearDep{}
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

func TestClassifyActionRef(t *testing.T) {
	cases := map[string]string{
		"8214744c546c1e5c8f03dde8fab3a7353211988d": resolutionLocked,
		"deadbeef1":  resolutionLocked, // abbreviated SHA
		"v4.1.2":     resolutionExact,
		"45.0.7":     resolutionExact,
		"v4":         resolutionUnpinned,
		"main":       resolutionUnpinned,
		"release-v2": resolutionUnpinned,
	}
	for ref, want := range cases {
		if got := classifyActionRef(ref); got != want {
			t.Errorf("classifyActionRef(%q) = %q, want %q", ref, got, want)
		}
	}
}

// tj-actions/changed-files shape: introduced 0, fixed 46.0.1.
func tjActionsAffected() []osvAffected {
	var aff osvAffected
	aff.Package.Name = "tj-actions/changed-files"
	aff.Package.Ecosystem = "GitHub Actions"
	aff.Ranges = []osvRange{{
		Type:   "ECOSYSTEM",
		Events: []osvEvent{{Introduced: "0"}, {Fixed: "46.0.1"}},
	}}
	return []osvAffected{aff}
}

func TestActionRefAffectedMatrix(t *testing.T) {
	aff := tjActionsAffected()
	name := "tj-actions/changed-files"
	cases := []struct {
		ref  string
		want bool
		why  string
	}{
		{"45.0.7", true, "full version inside the range"},
		{"v45.0.7", true, "v-prefixed full version inside the range"},
		{"46.0.1", false, "exactly the fixed version"},
		{"v46.2.0", false, "past the fix"},
		{"v45", true, "major-only tag: fix landed in a LATER major, the v45 line never heals"},
		{"v46", false, "major-only tag: fix is within this major, floating tag picks it up"},
		{"main", false, "branch ref floats to latest; a fix exists"},
		{"8214744c546c1e5c8f03dde8fab3a7353211988d", false, "SHA pins cannot be evaluated — no claim"},
	}
	for _, c := range cases {
		if got := actionRefAffected(c.ref, aff, name); got != c.want {
			t.Errorf("actionRefAffected(%q) = %v, want %v (%s)", c.ref, got, c.want, c.why)
		}
	}
	// Wrong action name never matches.
	if actionRefAffected("45.0.7", aff, "actions/checkout") {
		t.Error("advisory for a different action must never attach")
	}
	// No-fix advisory: even branch refs are affected.
	noFix := tjActionsAffected()
	noFix[0].Ranges[0].Events = []osvEvent{{Introduced: "0"}}
	if !actionRefAffected("main", noFix, name) {
		t.Error("branch ref must be affected when the advisory has no fix at all")
	}
	// Empty Affected (failed detail fetch): no claim.
	if actionRefAffected("45.0.7", nil, name) {
		t.Error("empty affected set must produce no claim")
	}
}

func TestCompareVersionish(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"4.1", "4.1.0", 0},
		{"45.0.7", "46.0.1", -1},
		{"46.0.1", "46.0.1", 0},
		{"v46.2.0", "46.0.1", 1},
		{"4.10", "4.9", 1}, // numeric, not lexical
	}
	for _, c := range cases {
		if got := compareVersionish(c.a, c.b); got != c.want {
			t.Errorf("compareVersionish(%q, %q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestActionTargetUsesEcosystemQueryForm(t *testing.T) {
	dep := db.VulnScanDep{
		Name: "tj-actions/changed-files", CurrentVersion: "45.0.7",
		PackageManager: "githubactions",
		Purl:           "pkg:githubactions/tj-actions/changed-files@45.0.7",
		Requirement:    "tj-actions/changed-files@45.0.7",
		Type:           model.ScopeBuild,
	}
	targets := vulnScanTargets(dep, nil)
	if len(targets) != 1 {
		t.Fatalf("want 1 target, got %d", len(targets))
	}
	tgt := targets[0]
	if tgt.OSVQueryEcosystem != "GitHub Actions" || tgt.OSVQueryName != dep.Name {
		t.Errorf("actions target must carry the {name, ecosystem} query form, got %+v", tgt)
	}
	if tgt.Scope != model.ScopeBuild || tgt.Kind != dependencyKindDirect {
		t.Errorf("actions target must be direct + build scope, got kind=%q scope=%q", tgt.Kind, tgt.Scope)
	}
	if tgt.Resolution != resolutionExact {
		t.Errorf("dotted tag must classify exact, got %q", tgt.Resolution)
	}

	// The batch builder must emit the name/ecosystem form for this
	// purl and the purl form for everything else.
	req := buildOSVBatchRequest(
		[]string{tgt.Purl, "pkg:pypi/flask@2.0.0"},
		map[string]vulnScanTarget{tgt.Purl: tgt},
	)
	if req.Queries[0].Package.Purl != "" || req.Queries[0].Package.Ecosystem != "GitHub Actions" {
		t.Errorf("actions query must use name/ecosystem, got %+v", req.Queries[0].Package)
	}
	if req.Queries[1].Package.Purl != "pkg:pypi/flask@2.0.0" || req.Queries[1].Package.Name != "" {
		t.Errorf("non-actions query must keep the purl form, got %+v", req.Queries[1].Package)
	}
}

// TestActionsInventoryIsKnobGated pins that the workflow walk hook is
// behind ac.GitHubActionsDeps and the resolver stores actions rows
// with NoLibyear (a fabricated 0.0 libyear would read as "perfectly
// fresh" and leak into the snapshot headline).
func TestActionsInventoryIsKnobGated(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	at := strings.Index(code, "parseWorkflowUses(")
	if at < 0 {
		t.Fatal("scanLibyear must call parseWorkflowUses")
	}
	window := code[max(0, at-400):at]
	if !strings.Contains(window, "ac.GitHubActionsDeps") {
		t.Error("the workflow walk hook must be gated on ac.GitHubActionsDeps")
	}
	if !strings.Contains(code, "NoLibyear:      true") && !strings.Contains(code, "NoLibyear: true") {
		t.Error("the githubactions resolver case must set NoLibyear — actions have no registry timeline")
	}
	// License table: actions rows are excluded (no license data).
	store, err := os.ReadFile("../db/timeseries.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(store), "package_manager <> 'githubactions'") {
		t.Error("GetRepoLicensesScoped must exclude githubactions rows — they carry no license data")
	}
}
