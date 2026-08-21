// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.46 (summary/19 P3) — SBOM scope emission: SPDX 2.3 typed
// dependency relationships, CycloneDX component scope, and the
// runtime-only document filter. Expected relationship names come from
// the SPDX 2.3 spec's relationship table (ground-truth rule).

package collector

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

func scopeFixtureDeps() []db.SBOMDep {
	return []db.SBOMDep{
		{Name: "flask", CurrentVersion: "2.3.0", PackageManager: "pypi", Type: "runtime", Purl: "pkg:pypi/flask@2.3.0"},
		{Name: "pytest", CurrentVersion: "7.4.0", PackageManager: "pypi", Type: model.ScopeTest, Purl: "pkg:pypi/pytest@7.4.0"},
		{Name: "meson-python", CurrentVersion: "0.15.0", PackageManager: "pypi", Type: model.ScopeBuild, Purl: "pkg:pypi/meson-python@0.15.0"},
		{Name: "ruff", CurrentVersion: "0.4.4", PackageManager: "pypi", Type: model.ScopeDev, Purl: "pkg:pypi/ruff@0.4.4"},
		{Name: "react", CurrentVersion: "18.2.0", PackageManager: "npm", Type: model.ScopePeer, Purl: "pkg:npm/react@18.2.0"},
		{Name: "fsevents", CurrentVersion: "2.3.2", PackageManager: "npm", Type: model.ScopeOptional, Purl: "pkg:npm/fsevents@2.3.2"},
	}
}

func TestSPDXEmitsTypedScopeRelationships(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "app", Owner: "org", GitURL: "https://github.com/org/app"}
	data, err := generateSPDX(repo, scopeFixtureDeps(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Relationships []struct {
			SpdxElementId      string `json:"spdxElementId"`
			RelationshipType   string `json:"relationshipType"`
			RelatedSpdxElement string `json:"relatedSpdxElement"`
		} `json:"relationships"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	// Collect relationship types with direction info.
	type rel struct{ typ, subj, obj string }
	var rels []rel
	for _, r := range doc.Relationships {
		rels = append(rels, rel{r.RelationshipType, r.SpdxElementId, r.RelatedSpdxElement})
	}
	find := func(typ string) *rel {
		for i := range rels {
			if rels[i].typ == typ {
				return &rels[i]
			}
		}
		return nil
	}
	// Runtime dep keeps root DEPENDS_ON pkg.
	if r := find("DEPENDS_ON"); r == nil || r.subj != "SPDXRef-RootPackage" {
		t.Errorf("runtime dep must be root DEPENDS_ON pkg, got %+v", r)
	}
	// SPDX 2.3 typed forms are INVERTED: pkg <TYPE>_OF root.
	for _, typ := range []string{"TEST_DEPENDENCY_OF", "BUILD_DEPENDENCY_OF", "DEV_DEPENDENCY_OF", "OPTIONAL_DEPENDENCY_OF", "PROVIDED_DEPENDENCY_OF"} {
		r := find(typ)
		if r == nil {
			t.Errorf("missing relationship type %s", typ)
			continue
		}
		if r.obj != "SPDXRef-RootPackage" {
			t.Errorf("%s must point AT the root (pkg %s root); got subj=%s obj=%s — the typed forms are inverted per SPDX 2.3", typ, typ, r.subj, r.obj)
		}
	}
	// DESCRIBES must survive.
	if find("DESCRIBES") == nil {
		t.Error("DESCRIBES relationship lost")
	}
}

func TestCycloneDXComponentScopeMapping(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "app", Owner: "org", GitURL: "https://github.com/org/app"}
	data, err := generateCycloneDX(repo, scopeFixtureDeps(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Components []struct {
			Name  string `json:"name"`
			Scope string `json:"scope"`
		} `json:"components"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"flask":        "required",
		"pytest":       "excluded",
		"meson-python": "excluded",
		"ruff":         "excluded",
		"react":        "optional",
		"fsevents":     "optional",
	}
	for _, c := range doc.Components {
		if w, ok := want[c.Name]; ok && c.Scope != w {
			t.Errorf("component %s scope %q, want %q (CycloneDX enum: required/optional/excluded)", c.Name, c.Scope, w)
		}
	}
}

// TestSPDXScopeMappingLivesInModel pins the tripwire-compatible
// design: sbom.go must route scope decisions through the model
// helpers, never branch on literal scope values (the P0 contract).
func TestSPDXScopeMappingLivesInModel(t *testing.T) {
	data, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	for _, needle := range []string{"model.SPDXRelationshipForScope(", "model.CycloneDXScopeForScope("} {
		if !strings.Contains(src, needle) {
			t.Errorf("sbom.go must call %s — scope vocabulary lives in model (P0 contract)", needle)
		}
	}
}

func TestModelScopeMappings(t *testing.T) {
	spdxCases := map[string]struct {
		typ      string
		inverted bool
	}{
		"":                  {"DEPENDS_ON", false},
		"runtime":           {"DEPENDS_ON", false},
		"future-unknown":    {"DEPENDS_ON", false},
		model.ScopeDev:      {"DEV_DEPENDENCY_OF", true},
		model.ScopeTest:     {"TEST_DEPENDENCY_OF", true},
		model.ScopeBuild:    {"BUILD_DEPENDENCY_OF", true},
		model.ScopeOptional: {"OPTIONAL_DEPENDENCY_OF", true},
		model.ScopePeer:     {"PROVIDED_DEPENDENCY_OF", true},
	}
	for scope, want := range spdxCases {
		typ, inv := model.SPDXRelationshipForScope(scope)
		if typ != want.typ || inv != want.inverted {
			t.Errorf("SPDXRelationshipForScope(%q) = (%s, %v), want (%s, %v)", scope, typ, inv, want.typ, want.inverted)
		}
	}
	cdxCases := map[string]string{
		"": "required", "runtime": "required", "future-unknown": "required",
		model.ScopeDev: "excluded", model.ScopeTest: "excluded", model.ScopeBuild: "excluded",
		model.ScopeOptional: "optional", model.ScopePeer: "optional",
	}
	for scope, want := range cdxCases {
		if got := model.CycloneDXScopeForScope(scope); got != want {
			t.Errorf("CycloneDXScopeForScope(%q) = %q, want %q", scope, got, want)
		}
	}
	// v0.27.51: runtime stores as the WORD (operator decision — ''
	// was uninterpretable for direct table readers).
	storedCases := map[string]string{
		"": model.ScopeRuntime, "runtime": model.ScopeRuntime, "future-unknown": model.ScopeRuntime,
		model.ScopeDev: model.ScopeDev, model.ScopeTest: model.ScopeTest,
	}
	for scope, want := range storedCases {
		if got := model.StoredScope(scope); got != want {
			t.Errorf("StoredScope(%q) = %q, want %q", scope, got, want)
		}
	}
}

func TestVulnScanTargetsCarryScope(t *testing.T) {
	dep := db.VulnScanDep{
		Name: "pytest", CurrentVersion: "7.4.0", PackageManager: "pypi",
		Purl: "pkg:pypi/pytest@7.4.0", Requirement: "==7.4.0", Type: model.ScopeDev,
	}
	targets := vulnScanTargets(dep, nil)
	if len(targets) == 0 {
		t.Fatal("no targets")
	}
	for _, tgt := range targets {
		if tgt.Scope != model.ScopeDev {
			t.Errorf("direct target scope %q, want dev — findings' dependency_scope must work for direct deps", tgt.Scope)
		}
	}
	// v0.27.51: runtime deps stamp the WORD explicitly.
	dep.Type = "runtime"
	for _, tgt := range vulnScanTargets(dep, nil) {
		if tgt.Scope != model.ScopeRuntime {
			t.Errorf("runtime dep target scope %q, want 'runtime' (v0.27.51 StoredScope convention)", tgt.Scope)
		}
	}
}

// TestMergeDirectTargetRuntimeWinsCollision pins the local-canary
// catch (2026-07-21, pipenv/setuptools): when the same purl is
// reachable from a non-runtime AND a runtime declaration, the target
// scope must fold to runtime (the empty string) regardless of iteration order — a
// non-runtime first-writer was re-stamping runtime findings off the
// headline and out of the digest.
func TestMergeDirectTargetRuntimeWinsCollision(t *testing.T) {
	m := map[string]vulnScanTarget{}
	var purls []string
	buildFirst := vulnScanTarget{Purl: "pkg:pypi/setuptools@67", Scope: model.ScopeBuild}
	runtimeSecond := vulnScanTarget{Purl: "pkg:pypi/setuptools@67", Scope: model.ScopeRuntime}
	purls = mergeDirectTarget(m, purls, buildFirst)
	purls = mergeDirectTarget(m, purls, runtimeSecond)
	if len(purls) != 1 {
		t.Fatalf("collision must not duplicate the purl list, got %d", len(purls))
	}
	if got := m["pkg:pypi/setuptools@67"].Scope; !model.IsRuntimeScope(got) {
		t.Errorf("runtime declaration must win the scope fold, got %q", got)
	}
	// Reverse order: runtime first stays runtime.
	m2 := map[string]vulnScanTarget{}
	purls2 := mergeDirectTarget(m2, nil, runtimeSecond)
	purls2 = mergeDirectTarget(m2, purls2, buildFirst)
	if len(purls2) != 1 || !model.IsRuntimeScope(m2["pkg:pypi/setuptools@67"].Scope) {
		t.Error("runtime-first must stay runtime on later non-runtime collision")
	}
	// v0.27.51: a LEGACY ''-scope target (pre-word rows in flight)
	// must fold identically to the explicit word.
	m4 := map[string]vulnScanTarget{}
	legacyRuntime := vulnScanTarget{Purl: "pkg:pypi/setuptools@67", Scope: ""}
	_ = mergeDirectTarget(m4, nil, buildFirst)
	_ = mergeDirectTarget(m4, nil, legacyRuntime)
	if got := m4["pkg:pypi/setuptools@67"].Scope; !model.IsRuntimeScope(got) {
		t.Errorf("legacy '' runtime must win the fold too (IsRuntimeScope contract), got %q", got)
	}
	// Two non-runtime declarations keep the first scope (no runtime
	// evidence — nothing to fold toward).
	m3 := map[string]vulnScanTarget{}
	dev := vulnScanTarget{Purl: "pkg:npm/x@1", Scope: model.ScopeDev}
	test := vulnScanTarget{Purl: "pkg:npm/x@1", Scope: model.ScopeTest}
	_ = mergeDirectTarget(m3, nil, dev)
	_ = mergeDirectTarget(m3, nil, test)
	if got := m3["pkg:npm/x@1"].Scope; got != model.ScopeDev {
		t.Errorf("non-runtime collision keeps the first scope, got %q", got)
	}
}

// TestOSVBatchChunking pins the local-canary catch #3 (2026-07-21):
// OSV caps querybatch at 1000 queries; react-router's pnpm transitive
// closure exceeded it and the whole scan 400'd ("too many queries").
// The miss path must chunk at osvBatchMaxQueries.
func TestOSVBatchChunking(t *testing.T) {
	if got := chunkStrings(nil, 1000); got != nil {
		t.Errorf("empty list must produce no chunks, got %v", got)
	}
	small := []string{"a", "b", "c"}
	if got := chunkStrings(small, 1000); len(got) != 1 || len(got[0]) != 3 {
		t.Errorf("under-cap list must be one chunk, got %v", got)
	}
	big := make([]string, 2500)
	for i := range big {
		big[i] = fmt.Sprintf("pkg:npm/p%d@1", i)
	}
	chunks := chunkStrings(big, 1000)
	if len(chunks) != 3 {
		t.Fatalf("2500 purls at cap 1000 must be 3 chunks, got %d", len(chunks))
	}
	if len(chunks[0]) != 1000 || len(chunks[1]) != 1000 || len(chunks[2]) != 500 {
		t.Errorf("chunk sizes wrong: %d/%d/%d", len(chunks[0]), len(chunks[1]), len(chunks[2]))
	}
	// Order preserved end-to-end (results map back positionally).
	if chunks[2][499] != "pkg:npm/p2499@1" {
		t.Errorf("chunking must preserve order, last = %s", chunks[2][499])
	}
	// The scan's miss path must actually use the cap.
	src, err := os.ReadFile("vulnerability.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "chunkStrings(missPurls, osvBatchMaxQueries)") {
		t.Error("ScanVulnerabilities' miss path must chunk at osvBatchMaxQueries — OSV 400s beyond 1000 queries per batch")
	}
}
