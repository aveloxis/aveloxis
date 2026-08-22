// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.31 (audit Phase 3, D1) — SBOM structural validation driven by
// the OFFICIAL specification schemas (CycloneDX 1.5 bom-1.5.schema.json
// + SPDX 2.3 spdx-schema.json, committed under testdata/sbom_schemas/,
// fetched from the spec repos 2026-07-21). The pre-existing SBOM tests
// asserted our output against our own struct definitions — if a struct
// tag drifted from the spec, both sides agreed on the wrong answer.
// This test reads the schemas' OWN required-field lists and enum
// values at run time and checks a representatively-seeded generated
// document against them, so refreshing the schema fixtures refreshes
// the constraints with zero test edits.
//
// Not a full JSON Schema validator (stdlib has none and the
// no-new-dependencies posture stands) — it enforces the normative
// facts that actually bit us: required fields at every level, the
// component-type enum, and the licenseChoice oneOf (an array is
// EITHER all license-objects OR exactly ONE expression item — the
// v0.27.29 restructure's contract, now pinned to the spec text).

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

func loadSchema(t *testing.T, name string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("testdata/sbom_schemas/" + name)
	if err != nil {
		t.Fatalf("schema fixture: %v", err)
	}
	var s map[string]any
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("schema fixture parse: %v", err)
	}
	return s
}

func requiredList(t *testing.T, node any, path string) []string {
	t.Helper()
	m, ok := node.(map[string]any)
	if !ok {
		t.Fatalf("%s: not an object", path)
	}
	raw, ok := m["required"].([]any)
	if !ok || len(raw) == 0 {
		t.Fatalf("%s: schema has no required list — fixture drift, re-check the extraction path", path)
	}
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		out = append(out, r.(string))
	}
	return out
}

func dig(node any, keys ...string) any {
	for _, k := range keys {
		m, ok := node.(map[string]any)
		if !ok {
			return nil
		}
		node = m[k]
	}
	return node
}

// sbomSeedInputs builds a document exercising every license shape the
// generator emits: valid compound (→ expression entry), single ids,
// registry commas, unknown names, plus scancode evidence.
func sbomSeedInputs() (*db.RepoForSBOM, []db.SBOMDep, *db.ScancodeForSBOM) {
	repo := &db.RepoForSBOM{Name: "schema-probe", Owner: "_avgt", GitURL: "https://github.com/_avgt/schema-probe"}
	deps := []db.SBOMDep{
		{Name: "flask", CurrentVersion: "2.0.0", PackageManager: "pypi", Purl: "pkg:pypi/flask@2.0.0", Type: "runtime", License: "BSD-3-Clause"},
		{Name: "cryptography", CurrentVersion: "42.0.0", Purl: "pkg:pypi/cryptography@42.0.0", Type: "runtime", License: "Apache-2.0 AND MIT"},
		{Name: "weird", CurrentVersion: "1.0", Purl: "pkg:npm/weird@1.0", Type: "runtime", License: "Custom Corp License"},
		{Name: "dual", CurrentVersion: "3.1", Purl: "pkg:npm/dual@3.1", Type: "dev", License: "MIT, Apache-2.0"},
	}
	scan := &db.ScancodeForSBOM{ConcludedLicenseSPDX: "MIT"}
	return repo, deps, scan
}

// sbomSeedGraph (v0.27.134) adds a C2 lockfile closure to the probe
// document so the schema constraints run against the graph-emission
// paths too: a transitive component/package plus a real edge.
func sbomSeedGraph() *sbomGraph {
	return &sbomGraph{
		Transitives: []db.RepoLockfilePackage{
			{Ecosystem: "pypi", PackageName: "werkzeug", ResolvedVersion: "2.3.7"},
		},
		Edges: []db.RepoLockfileEdge{
			{Ecosystem: "pypi", LockfilePath: "poetry.lock", ParentName: "flask", ParentVersion: "2.0.0", ChildName: "werkzeug"},
		},
	}
}

func TestCycloneDXSatisfiesOfficialSchemaConstraints(t *testing.T) {
	schema := loadSchema(t, "bom-1.5.schema.json")
	repo, deps, scan := sbomSeedInputs()
	raw, err := generateCycloneDX(repo, deps, scan, sbomSeedGraph())
	if err != nil {
		t.Fatalf("generateCycloneDX: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("generated CDX is not valid JSON: %v", err)
	}

	// 1. Top-level required fields, straight from the schema.
	for _, field := range requiredList(t, schema, "bom") {
		if _, ok := doc[field]; !ok {
			t.Errorf("CDX schema requires top-level %q — absent from generated document", field)
		}
	}

	// 2. Every component (metadata.component + components[]) must
	// carry the schema's component.required fields, and its type must
	// be in the schema's enum.
	compReq := requiredList(t, dig(schema, "definitions", "component"), "definitions.component")
	enumRaw, _ := dig(schema, "definitions", "component", "properties", "type", "enum").([]any)
	if len(enumRaw) == 0 {
		t.Fatal("schema fixture: component type enum not found")
	}
	typeEnum := map[string]bool{}
	for _, e := range enumRaw {
		typeEnum[e.(string)] = true
	}
	var comps []any
	if mc := dig(doc, "metadata", "component"); mc != nil {
		comps = append(comps, mc)
	}
	if list, ok := doc["components"].([]any); ok {
		comps = append(comps, list...)
	}
	if len(comps) < 6 {
		t.Fatalf("expected root + 4 dep + 1 transitive components, got %d", len(comps))
	}
	for i, c := range comps {
		cm := c.(map[string]any)
		for _, field := range compReq {
			if _, ok := cm[field]; !ok {
				t.Errorf("component %d (%v) missing schema-required field %q", i, cm["name"], field)
			}
		}
		if typ, _ := cm["type"].(string); !typeEnum[typ] {
			t.Errorf("component %d type %q is not in the schema's enum", i, cm["type"])
		}
	}

	// 3. licenseChoice oneOf — walk every "licenses" array anywhere in
	// the document. Per the schema: EITHER every item is a
	// license-object (required ["license"], no "expression") OR the
	// array is EXACTLY ONE item with required ["expression"].
	var checkLicenses func(node any, path string)
	checkLicenses = func(node any, path string) {
		switch v := node.(type) {
		case map[string]any:
			for k, child := range v {
				if k == "licenses" {
					arr, ok := child.([]any)
					if !ok {
						t.Errorf("%s.licenses is not an array", path)
						continue
					}
					exprCount, licCount := 0, 0
					for _, item := range arr {
						im, _ := item.(map[string]any)
						_, hasExpr := im["expression"]
						_, hasLic := im["license"]
						switch {
						case hasExpr && hasLic:
							t.Errorf("%s.licenses item has BOTH license and expression — violates licenseChoice oneOf", path)
						case hasExpr:
							exprCount++
						case hasLic:
							licCount++
						default:
							t.Errorf("%s.licenses item has NEITHER license nor expression", path)
						}
					}
					if exprCount > 0 && (licCount > 0 || len(arr) != 1) {
						t.Errorf("%s.licenses mixes an expression with other entries (expr=%d lic=%d len=%d) — the schema's expression branch is a tuple of EXACTLY ONE", path, exprCount, licCount, len(arr))
					}
				}
				checkLicenses(child, path+"."+k)
			}
		case []any:
			for i, item := range v {
				checkLicenses(item, fmt.Sprintf("%s[%d]", path, i))
			}
		}
	}
	checkLicenses(doc, "$")

	// The seed's compound license must actually have produced an
	// expression entry somewhere — otherwise the oneOf check above ran
	// against nothing interesting (dead-negative-control guard).
	if !containsExpression(doc) {
		t.Error("seeded 'Apache-2.0 AND MIT' produced no expression entry — the licenseChoice branch under test never executed")
	}
}

func containsExpression(node any) bool {
	switch v := node.(type) {
	case map[string]any:
		if _, ok := v["expression"]; ok {
			return true
		}
		for _, child := range v {
			if containsExpression(child) {
				return true
			}
		}
	case []any:
		for _, item := range v {
			if containsExpression(item) {
				return true
			}
		}
	}
	return false
}

func TestSPDXSatisfiesOfficialSchemaConstraints(t *testing.T) {
	schema := loadSchema(t, "spdx-2.3.schema.json")
	repo, deps, scan := sbomSeedInputs()
	raw, err := generateSPDX(repo, deps, scan, sbomSeedGraph())
	if err != nil {
		t.Fatalf("generateSPDX: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("generated SPDX is not valid JSON: %v", err)
	}

	for _, field := range requiredList(t, schema, "spdx") {
		if _, ok := doc[field]; !ok {
			t.Errorf("SPDX schema requires top-level %q — absent from generated document", field)
		}
	}

	pkgReq := requiredList(t, dig(schema, "properties", "packages", "items"), "packages.items")
	pkgs, _ := doc["packages"].([]any)
	if len(pkgs) < 6 {
		t.Fatalf("expected root + 4 dep + 1 transitive packages, got %d", len(pkgs))
	}
	for i, p := range pkgs {
		pm := p.(map[string]any)
		for _, field := range pkgReq {
			if val, ok := pm[field]; !ok || val == "" {
				t.Errorf("package %d (%v) missing/empty schema-required field %q", i, pm["name"], field)
			}
		}
		// SPDX 2.3 §7.8/§7.9 conditional cardinality — prose the JSON
		// schema cannot express: filesAnalyzed defaults to TRUE when
		// omitted, and true-or-omitted makes packageVerificationCode
		// MANDATORY. Since we never analyze package files, every
		// package must explicitly declare filesAnalyzed=false (or, if
		// that ever changes, carry a verification code).
		fa, present := pm["filesAnalyzed"]
		if !present {
			if _, hasVC := pm["packageVerificationCode"]; !hasVC {
				t.Errorf("package %d (%v) omits filesAnalyzed (defaults TRUE per §7.8) without a packageVerificationCode (§7.9 mandatory) — non-conformant", i, pm["name"])
			}
		} else if fa == true {
			if _, hasVC := pm["packageVerificationCode"]; !hasVC {
				t.Errorf("package %d (%v) claims filesAnalyzed=true without a packageVerificationCode", i, pm["name"])
			}
		}
	}

	relReq := requiredList(t, dig(schema, "properties", "relationships", "items"), "relationships.items")
	rels, _ := doc["relationships"].([]any)
	if len(rels) == 0 {
		t.Fatal("generated SPDX has no relationships — DESCRIBES at minimum is expected")
	}
	for i, r := range rels {
		rm := r.(map[string]any)
		for _, field := range relReq {
			if val, ok := rm[field]; !ok || val == "" {
				t.Errorf("relationship %d missing/empty schema-required field %q", i, field)
			}
		}
	}
}
