// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.46 (summary/19 P3) — SBOM scope emission: SPDX 2.3 typed
// dependency relationships, CycloneDX component scope, and the
// runtime-only document filter. Expected relationship names come from
// the SPDX 2.3 spec's relationship table (ground-truth rule).

package collector

import (
	"encoding/json"
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
	data, err := generateSPDX(repo, scopeFixtureDeps(), nil)
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
	data, err := generateCycloneDX(repo, scopeFixtureDeps(), nil)
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
	storedCases := map[string]string{
		"": "", "runtime": "", "future-unknown": "",
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
	// Runtime dep stores '' per the column convention.
	dep.Type = "runtime"
	for _, tgt := range vulnScanTargets(dep, nil) {
		if tgt.Scope != "" {
			t.Errorf("runtime dep target scope %q, want '' (StoredScope column convention)", tgt.Scope)
		}
	}
}
