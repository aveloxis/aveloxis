// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.47 (summary/19 P4) — GitHub Actions inventory + OSV matching
// tests. The ref-evaluation matrix derives from the tj-actions/
// changed-files advisory (GHSA-mrrh-fwg8-r2c3: ECOSYSTEM range
// introduced 0 → fixed 46.0.1), the P4 canonical ground truth.

package collector

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/scantarget"
	"github.com/aveloxis/aveloxis/internal/srctest"
	"github.com/aveloxis/aveloxis/internal/versionuse"
)

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
	dep := model.VulnScanDep{
		Name: "tj-actions/changed-files", CurrentVersion: "45.0.7",
		PackageManager: "githubactions",
		Purl:           "pkg:githubactions/tj-actions/changed-files@45.0.7",
		Requirement:    "tj-actions/changed-files@45.0.7",
		Type:           model.ScopeBuild,
	}
	targets := scantarget.DirectTargets(dep, nil)
	if len(targets) != 1 {
		t.Fatalf("want 1 target, got %d", len(targets))
	}
	tgt := targets[0]
	if tgt.OSVQueryEcosystem != "GitHub Actions" || tgt.OSVQueryName != dep.Name {
		t.Errorf("actions target must carry the {name, ecosystem} query form, got %+v", tgt)
	}
	if tgt.Scope != model.ScopeBuild || tgt.Kind != model.DependencyKindDirect {
		t.Errorf("actions target must be direct + build scope, got kind=%q scope=%q", tgt.Kind, tgt.Scope)
	}
	if tgt.Resolution != versionuse.Exact {
		t.Errorf("dotted tag must classify exact, got %q", tgt.Resolution)
	}

	// The batch builder must emit the name/ecosystem form for this
	// purl and the purl form for everything else.
	req := buildOSVBatchRequest(
		[]string{tgt.Purl, "pkg:pypi/flask@2.0.0"},
		map[string]model.VulnScanTarget{tgt.Purl: tgt},
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
	// Behaviour, through the roster (0.29.153; this read analysis.go for the
	// guard within 400 characters of the call): a workflow's actions are
	// declared only with the knob on.
	path := filepath.Join(t.TempDir(), ".github", "workflows", "ci.yml")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("jobs:\n  b:\n    steps:\n      - uses: actions/checkout@v4\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	walk := manifest.NewWalk(filepath.Dir(path), nil)
	if deps, err := walk.Declared(path, manifest.Options{}); err != nil || len(deps) != 0 {
		t.Errorf("knob off: a workflow declared %+v (%v); want nothing", deps, err)
	}
	if deps, err := walk.Declared(path, manifest.Options{DevBuildDeps: true}); err != nil || len(deps) != 0 {
		t.Errorf("the dev/build knob alone: a workflow declared %+v (%v); want nothing", deps, err)
	}
	if deps, err := walk.Declared(path, manifest.Options{GitHubActionsDeps: true}); err != nil || len(deps) != 1 || deps[0].Name != "actions/checkout" || deps[0].Manager != "githubactions" {
		t.Errorf("knob on: a workflow declared %+v (%v); want actions/checkout", deps, err)
	}
	// And the walk hands the collector's knobs to the roster.
	code := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (ac *AnalysisCollector) scanLibyear("))
	// (0.29.213: through the child that reads the manifests. The request
	// carries them and the child hands them to the roster;
	// TestTheChildReadsUnderTheAnalysisKnobs drives it.)
	request := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (ac *AnalysisCollector) readClone("))
	if !strings.Contains(request, "DevBuildDeps: ac.DevBuildDeps, GitHubActionsDeps: ac.GitHubActionsDeps") {
		t.Error("readClone must put ac.DevBuildDeps and ac.GitHubActionsDeps in the manifest reader's request")
	}
	serve := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (c *manifestChild) serveDeclarations("))
	if !strings.Contains(serve, "manifest.Options{DevBuildDeps: req.DevBuildDeps, GitHubActionsDeps: req.GitHubActionsDeps}") {
		t.Error("the manifest reader must pass the request's two knobs to the roster as manifest.Options")
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
