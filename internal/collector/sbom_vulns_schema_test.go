// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/vulnclass"
)

// The annotated documents go through the same schema harness as the bare ones
// (summary/70, 4.1.4). Before the annotators moved here, the harness never saw
// a document with findings in it: the annotators lived in internal/api and the
// harness in this package.

// sbomVulnSeed is the seed inventory plus one dependency that declares no
// version, and one finding of each class an annotator has to decide about.
func sbomVulnSeed() (*db.RepoForSBOM, []db.SBOMDep, *db.ScancodeForSBOM, []*model.VulnerabilityRow) {
	repo, deps, scan := sbomSeedInputs()
	deps = append(deps, db.SBOMDep{Name: "loose", PackageManager: "pypi", Purl: "pkg:pypi/loose", Type: "runtime", License: "MIT"})
	closedAt := time.Unix(1_700_000_000, 0)
	vulns := []*model.VulnerabilityRow{
		{ // exposure: a version in use
			VulnID: "GHSA-curr-0001", CVEID: "CVE-2023-30861", PackageName: "flask", PackagePurl: "pkg:pypi/flask@2.0.0",
			Ecosystem: "PyPI", Severity: "HIGH", CVSSScore: 7.5, CVSSVector: "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:N/A:N",
			Summary: "session cookie disclosure", FixedVersion: "2.2.5", Source: "osv.dev",
			VersionResolution: "locked", DependencyKind: "direct", DependencyScope: "runtime",
		},
		{ // version not determined
			VulnID: "PYSEC-2020-0001", PackageName: "loose", PackagePurl: "pkg:pypi/loose",
			Ecosystem: "PyPI", Severity: "MODERATE", CVSSScore: 5.3, CVSSVector: "CVSS:3.0/AV:N/AC:L/PR:N/UI:N/S:U/C:N/I:L/A:N",
			Summary: "an old advisory", Source: "osv.dev",
			VersionResolution: "unpinned", DependencyKind: "direct", DependencyScope: "runtime",
		},
		{ // self: the repository's own package
			VulnID: "PYSEC-self-0001", PackageName: "schema-probe", PackagePurl: "pkg:pypi/schema-probe",
			Ecosystem: "PyPI", Severity: "CRITICAL", CVSSScore: 9.8, Source: "osv.dev", DependencyKind: "self",
		},
		{ // closed
			VulnID: "GHSA-clos-0001", PackageName: "dual", PackagePurl: "pkg:npm/dual@3.1",
			Ecosystem: "npm", Severity: "LOW", Source: "osv.dev",
			VersionResolution: "locked", DependencyKind: "direct", ResolvedAt: &closedAt,
		},
	}
	return repo, deps, scan, vulns
}

func schemaEnum(t *testing.T, schema map[string]any, keys ...string) map[string]bool {
	t.Helper()
	raw, _ := dig(schema, keys...).([]any)
	if len(raw) == 0 {
		t.Fatalf("schema fixture: enum at %v not found", keys)
	}
	out := map[string]bool{}
	for _, e := range raw {
		out[e.(string)] = true
	}
	return out
}

func TestAnnotatedCycloneDXSatisfiesOfficialSchemaConstraints(t *testing.T) {
	schema := loadSchema(t, "bom-1.7.schema.json")
	repo, deps, scan, vulns := sbomVulnSeed()
	bare, err := generateCycloneDX(repo, deps, scan, sbomSeedGraph())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AnnotateCycloneDXWithVulns(bare, vulns)
	if err != nil {
		t.Fatalf("AnnotateCycloneDXWithVulns: %v", err)
	}
	doc := assertCycloneDXSchemaConstraints(t, schema, raw)

	severities := schemaEnum(t, schema, "definitions", "severity", "enum")
	methods := schemaEnum(t, schema, "definitions", "scoreMethod", "enum")
	states := schemaEnum(t, schema, "definitions", "impactAnalysisState", "enum")
	allowed, _ := dig(schema, "definitions", "vulnerability", "properties").(map[string]any)
	if len(allowed) == 0 {
		t.Fatal("schema fixture: vulnerability properties not found")
	}

	refs := map[string]bool{}
	for _, c := range doc["components"].([]any) {
		if ref, _ := c.(map[string]any)["bom-ref"].(string); ref != "" {
			refs[ref] = true
		}
	}

	entries, _ := doc["vulnerabilities"].([]any)
	byID := map[string]map[string]any{}
	for _, e := range entries {
		em := e.(map[string]any)
		id, _ := em["id"].(string)
		byID[id] = em
		for field := range em {
			if _, ok := allowed[field]; !ok {
				t.Errorf("vulnerability %s carries %q, which the schema's vulnerability object does not define", id, field)
			}
		}
		for _, r := range em["ratings"].([]any) {
			rm := r.(map[string]any)
			if s, _ := rm["severity"].(string); !severities[s] {
				t.Errorf("vulnerability %s rating severity %q is not in the schema's enum", id, s)
			}
			if m, _ := rm["method"].(string); !methods[m] {
				t.Errorf("vulnerability %s rating method %q is not in the schema's enum", id, m)
			}
		}
		if a, ok := em["analysis"].(map[string]any); ok {
			if s, _ := a["state"].(string); !states[s] {
				t.Errorf("vulnerability %s analysis state %q is not in the schema's enum", id, s)
			}
		}
		affects, _ := em["affects"].([]any)
		if len(affects) == 0 {
			t.Errorf("vulnerability %s affects nothing", id)
		}
		for _, a := range affects {
			ref, _ := a.(map[string]any)["ref"].(string)
			if !refs[ref] {
				t.Errorf("vulnerability %s affects %q, which is no component's bom-ref: the reference dangles", id, ref)
			}
		}
	}

	if len(byID) != 2 {
		t.Fatalf("annotated %d findings (%v), want the current one and the version-not-determined one", len(byID), byID)
	}
	if cur := byID["GHSA-curr-0001"]; cur == nil {
		t.Error("the current finding is missing")
	} else {
		if _, has := cur["analysis"]; has {
			t.Error("a finding at a version in use must carry no analysis state")
		}
		if cur["recommendation"] != "Upgrade flask to 2.2.5 or later" {
			t.Errorf("recommendation = %v", cur["recommendation"])
		}
		if m := cur["ratings"].([]any)[0].(map[string]any)["method"]; m != "CVSSv31" {
			t.Errorf("a CVSS:3.1 vector's method = %v, want CVSSv31", m)
		}
	}
	if loose := byID["PYSEC-2020-0001"]; loose == nil {
		t.Error("the version-not-determined finding is missing")
	} else {
		a, _ := loose["analysis"].(map[string]any)
		if a["state"] != "in_triage" || a["detail"] != vulnclass.VersionNotDeterminedDetail {
			t.Errorf("version-not-determined analysis = %v, want in_triage with the shared sentence", a)
		}
		if m := loose["ratings"].([]any)[0].(map[string]any); m["method"] != "CVSSv3" || m["severity"] != "medium" {
			t.Errorf("a CVSS:3.0 MODERATE rating = %v, want method CVSSv3 and severity medium", m)
		}
	}
}

func TestAnnotatedSPDXSatisfiesOfficialSchemaConstraints(t *testing.T) {
	schema := loadSchema(t, "spdx-2.3.schema.json")
	repo, deps, scan, vulns := sbomVulnSeed()
	bare, err := generateSPDX(repo, deps, scan, sbomSeedGraph())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AnnotateSPDXWithVulns(bare, vulns)
	if err != nil {
		t.Fatalf("AnnotateSPDXWithVulns: %v", err)
	}
	doc := assertSPDXSchemaConstraints(t, schema, raw)

	refSchema := dig(schema, "properties", "packages", "items", "properties", "externalRefs", "items")
	required := requiredList(t, refSchema, "packages.items.externalRefs.items")
	categories := schemaEnum(t, schema, "properties", "packages", "items", "properties", "externalRefs", "items", "properties", "referenceCategory", "enum")
	allowed, _ := dig(refSchema, "properties").(map[string]any)

	advisories := map[string][]map[string]any{} // package name -> its advisory refs
	for _, p := range doc["packages"].([]any) {
		pm := p.(map[string]any)
		name, _ := pm["name"].(string)
		refs, _ := pm["externalRefs"].([]any)
		for _, r := range refs {
			rm := r.(map[string]any)
			for _, field := range required {
				if v, ok := rm[field]; !ok || v == "" {
					t.Errorf("package %s externalRef missing schema-required %q: %v", name, field, rm)
				}
			}
			for field := range rm {
				if _, ok := allowed[field]; !ok {
					t.Errorf("package %s externalRef carries %q, which the schema does not define", name, field)
				}
			}
			if c, _ := rm["referenceCategory"].(string); !categories[c] {
				t.Errorf("package %s externalRef category %q is not in the schema's enum", name, c)
			}
			if rm["referenceType"] == "advisory" {
				advisories[name] = append(advisories[name], rm)
			}
		}
	}

	if got := advisories["flask"]; len(got) != 1 || got[0]["referenceLocator"] != "https://osv.dev/vulnerability/GHSA-curr-0001" {
		t.Errorf("flask advisory refs = %v, want the one current finding", got)
	} else if _, has := got[0]["comment"]; has {
		t.Error("a finding at a version in use must carry no comment")
	}
	if got := advisories["loose"]; len(got) != 1 || got[0]["comment"] != vulnclass.VersionNotDeterminedDetail {
		t.Errorf("loose advisory refs = %v, want one with the shared sentence as its comment", got)
	}
	total := 0
	for _, refs := range advisories {
		total += len(refs)
	}
	if total != 2 {
		t.Errorf("%d advisory references in the document (%v); the closed finding and the repository's own must add none", total, advisories)
	}
}
