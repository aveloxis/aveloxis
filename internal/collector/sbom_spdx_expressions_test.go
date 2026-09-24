// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// Worklist 53 (summary/39): the SBOM exporters discarded valid SPDX
// expressions (every OR, WITH and parenthesized license became
// NOASSERTION), inverted a real AND into OR, emitted LicenseRef-s without
// the extracted-licensing entries SPDX requires, and wrote license IDs the
// CycloneDX 1.5 schema's frozen enum rejects (Unicode-3.0). The export
// moves to CycloneDX 1.7 (decision 6) and validates against the official
// schemas.

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/spdx"
)

// licenseSeed is one dependency per license shape the exporters must get
// right, plus the ScanCode conclusion for the root.
func licenseSeed() (*db.RepoForSBOM, []db.SBOMDep, *db.ScancodeForSBOM) {
	repo := &db.RepoForSBOM{Name: "lic-probe", Owner: "_avgt", GitURL: "https://github.com/_avgt/lic-probe", License: "Apache-2.0"}
	dep := func(name, pm, lic string) db.SBOMDep {
		return db.SBOMDep{Name: name, CurrentVersion: "1.0.0", PackageManager: pm,
			Purl: "pkg:" + pm + "/" + name + "@1.0.0", Type: "runtime", License: lic}
	}
	deps := []db.SBOMDep{
		dep("or", "cargo", "MIT OR Apache-2.0"),
		dep("slash", "cargo", "MIT/Apache-2.0"),
		dep("with", "maven", "GPL-2.0-only WITH Classpath-exception-2.0"),
		dep("paren", "cargo", "(Apache-2.0 OR MIT) AND BSD-3-Clause"),
		dep("and", "npm", "Apache-2.0 AND MIT"),
		dep("unicode", "cargo", "Unicode-3.0"),
		dep("synonym", "pypi", "Apache 2.0"),
		dep("custom", "npm", "Custom Corp License"),
		dep("family", "pypi", "LGPL"),
		dep("ref", "npm", "LicenseRef-Corp-EULA"),
		dep("gem", "rubygems", "Ruby OR BSD-2-Clause"),
		dep("hexpkg", "hex", "MIT OR Apache-2.0"),
		dep("composer", "packagist", "MIT OR GPL-3.0-or-later"),
	}
	scan := &db.ScancodeForSBOM{ConcludedLicenseSPDX: "Apache-2.0 AND LicenseRef-scancode-public-domain"}
	return repo, deps, scan
}

func spdxDocFor(t *testing.T) spdxDoc {
	t.Helper()
	repo, deps, scan := licenseSeed()
	raw, err := generateSPDX(repo, deps, scan, nil)
	if err != nil {
		t.Fatal(err)
	}
	var doc spdxDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestSPDXLicenseDeclaredKeepsValidExpressions(t *testing.T) {
	doc := spdxDocFor(t)
	want := map[string]string{
		"or":       "MIT OR Apache-2.0",
		"slash":    "MIT OR Apache-2.0",
		"with":     "GPL-2.0-only WITH Classpath-exception-2.0",
		"paren":    "(Apache-2.0 OR MIT) AND BSD-3-Clause",
		"and":      "Apache-2.0 AND MIT", // a real conjunction is never inverted
		"unicode":  "Unicode-3.0",
		"synonym":  "Apache-2.0",
		"custom":   "NOASSERTION",
		"family":   "NOASSERTION", // a house label, not SPDX
		"ref":      "LicenseRef-Corp-EULA",
		"gem":      "Ruby OR BSD-2-Clause",
		"hexpkg":   "MIT OR Apache-2.0",
		"composer": "MIT OR GPL-3.0-or-later",
	}
	for _, p := range doc.Packages {
		w, ok := want[p.Name]
		if !ok {
			continue
		}
		if p.LicenseDeclared != w {
			t.Errorf("%s: licenseDeclared = %q, want %q", p.Name, p.LicenseDeclared, w)
		}
		delete(want, p.Name)
	}
	if len(want) > 0 {
		t.Errorf("packages missing from the document: %v", want)
	}
}

// TestSPDXDisclosesAssumedChoice — decision 2: RubyGems and Hex publish
// license lists with no stated relationship; they are stored as OR, and the
// SPDX document says so. Packagist documents its list as a choice, and a
// real expression (cargo) needs no note.
func TestSPDXDisclosesAssumedChoice(t *testing.T) {
	doc := spdxDocFor(t)
	for _, p := range doc.Packages {
		switch p.Name {
		case "gem", "hexpkg":
			if !strings.Contains(p.LicenseComments, "does not state how they combine") {
				t.Errorf("%s: licenseComments = %q; must disclose that OR was assumed", p.Name, p.LicenseComments)
			}
		case "composer", "or", "paren":
			if p.LicenseComments != "" {
				t.Errorf("%s: licenseComments = %q; no assumption was made", p.Name, p.LicenseComments)
			}
		}
	}
}

// TestSPDXEveryLicenseFieldIsValid — every license field is NOASSERTION,
// NONE or a valid expression, and every LicenseRef- it uses is declared in
// hasExtractedLicensingInfos (SPDX 2.3 §10), once.
func TestSPDXEveryLicenseFieldIsValid(t *testing.T) {
	doc := spdxDocFor(t)
	declared := map[string]int{}
	for _, e := range doc.ExtractedLicenses {
		declared[e.LicenseID]++
		if e.ExtractedText == "" {
			t.Errorf("%s: extractedText is mandatory", e.LicenseID)
		}
	}
	used := map[string]bool{}
	for _, p := range doc.Packages {
		for _, f := range []string{p.LicenseDeclared, p.LicenseConcluded} {
			if f == "NOASSERTION" || f == "NONE" {
				continue
			}
			if err := spdx.Validate(f); err != nil {
				t.Errorf("%s: %q is not a valid SPDX expression: %v", p.Name, f, err)
			}
			for _, tok := range strings.FieldsFunc(f, func(r rune) bool { return r == ' ' || r == '(' || r == ')' }) {
				if strings.HasPrefix(tok, "LicenseRef-") {
					used[tok] = true
				}
			}
		}
	}
	if !used["LicenseRef-scancode-public-domain"] || !used["LicenseRef-Corp-EULA"] {
		t.Fatalf("the seed's LicenseRef-s must appear in the document: %v", used)
	}
	for ref := range used {
		if declared[ref] != 1 {
			t.Errorf("%s is used but declared %d times in hasExtractedLicensingInfos", ref, declared[ref])
		}
	}
	for ref := range declared {
		if !used[ref] {
			t.Errorf("%s is declared but not used", ref)
		}
	}
}

func cdxFor(t *testing.T) cycloneDX {
	t.Helper()
	repo, deps, scan := licenseSeed()
	raw, err := generateCycloneDX(repo, deps, scan, nil)
	if err != nil {
		t.Fatal(err)
	}
	var bom cycloneDX
	if err := json.Unmarshal(raw, &bom); err != nil {
		t.Fatal(err)
	}
	return bom
}

// TestCycloneDXLicenseChoice — decision 6: CycloneDX 1.7; a license ID the
// target schema's enum lists goes in license.id, any other valid
// expression (a compound, or a single ID the enum lacks) in expression,
// and anything else in license.name. Registry licenses are acknowledged as
// declared, ScanCode's as concluded. One entry per licenseChoice.
func TestCycloneDXLicenseChoice(t *testing.T) {
	bom := cdxFor(t)
	if bom.SpecVersion != "1.7" || bom.Schema != "http://cyclonedx.org/schema/bom-1.7.schema.json" {
		t.Errorf("specVersion = %q, $schema = %q; want 1.7", bom.SpecVersion, bom.Schema)
	}
	type want struct{ id, expr, name string }
	wants := map[string]want{
		"unicode": {id: "Unicode-3.0"},
		"synonym": {id: "Apache-2.0"},
		"or":      {expr: "MIT OR Apache-2.0"},
		"slash":   {expr: "MIT OR Apache-2.0"},
		"and":     {expr: "Apache-2.0 AND MIT"},
		"with":    {expr: "GPL-2.0-only WITH Classpath-exception-2.0"},
		"custom":  {name: "Custom Corp License"},
		"family":  {name: "LGPL"},
	}
	for _, c := range bom.Components {
		w, ok := wants[c.Name]
		if !ok {
			continue
		}
		if len(c.Licenses) != 1 {
			t.Errorf("%s: %d licenseChoice entries, want exactly 1", c.Name, len(c.Licenses))
			continue
		}
		l := c.Licenses[0]
		switch {
		case w.expr != "":
			if l.Expression != w.expr || l.License != nil || l.Acknowledgement != "declared" {
				t.Errorf("%s: %+v, want expression %q acknowledged declared", c.Name, l, w.expr)
			}
		default:
			if l.License == nil || l.License.ID != w.id || l.License.Name != w.name || l.License.Acknowledgement != "declared" {
				t.Errorf("%s: %+v (license %+v), want id=%q name=%q acknowledged declared", c.Name, l, l.License, w.id, w.name)
			}
		}
	}
	root := bom.Metadata.Component
	if root == nil || root.Evidence == nil || len(root.Evidence.Licenses) != 1 ||
		root.Evidence.Licenses[0].Expression != "Apache-2.0 AND LicenseRef-scancode-public-domain" ||
		root.Evidence.Licenses[0].Acknowledgement != "concluded" {
		t.Errorf("root evidence must carry ScanCode's expression acknowledged concluded: %+v", root)
	}
}

// TestCycloneDXLicenseIDNeedsTheSchemaEnum — the version-aware half of
// decision 6: an SPDX ID the target schema's enum lacks (the 1.5 schema
// lacked many of SPDX 3.29.0's, Unicode-3.0 among them) is emitted as a one-term expression, which
// every schema version accepts, never as license.id.
func TestCycloneDXLicenseIDNeedsTheSchemaEnum(t *testing.T) {
	inEnum := func(id string) bool { return id == "MIT" }
	if l := cdxLicenseFor("MIT", "declared", inEnum); l.License == nil || l.License.ID != "MIT" {
		t.Errorf("an enum ID must use license.id: %+v", l)
	}
	if l := cdxLicenseFor("Unicode-3.0", "declared", inEnum); l.License != nil || l.Expression != "Unicode-3.0" {
		t.Errorf("an SPDX ID outside the enum must be an expression: %+v", l)
	}
	if !cdxLicenseIDs["Unicode-3.0"] || !cdxLicenseIDs["MIT"] || cdxLicenseIDs["LGPL"] {
		t.Error("the embedded CycloneDX 1.7 enum is wrong or missing")
	}
}

// compileSchema registers every official schema fixture under its own $id
// and compiles root.
func compileSchema(t *testing.T, root string) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	var rootID string
	for _, f := range []string{"bom-1.7.schema.json", "cyclonedx-1.7-spdx.schema.json", "jsf-0.82.schema.json",
		"cryptography-defs.schema.json", "spdx-2.3.schema.json"} {
		fh, err := os.Open("testdata/sbom_schemas/" + f)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := jsonschema.UnmarshalJSON(fh)
		fh.Close()
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		id, _ := doc.(map[string]any)["$id"].(string)
		if id == "" {
			t.Fatalf("%s has no $id", f)
		}
		if err := c.AddResource(id, doc); err != nil {
			t.Fatal(err)
		}
		if f == root {
			rootID = id
		}
	}
	s, err := c.Compile(rootID)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func validateAgainst(t *testing.T, s *jsonschema.Schema, raw []byte) {
	t.Helper()
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(doc); err != nil {
		t.Errorf("document fails the official schema:\n%v", err)
	}
}

// TestSBOMsValidateAgainstOfficialSchemas — the full official schemas, not
// just the required-field extracts of sbom_schema_groundtruth_test.go:
// both seeds, both formats. A negative control proves the validator bites.
func TestSBOMsValidateAgainstOfficialSchemas(t *testing.T) {
	cdxSchema := compileSchema(t, "bom-1.7.schema.json")
	spdxSchema := compileSchema(t, "spdx-2.3.schema.json")
	for _, seed := range []func() (*db.RepoForSBOM, []db.SBOMDep, *db.ScancodeForSBOM){licenseSeed, sbomSeedInputs} {
		repo, deps, scan := seed()
		cdx, err := generateCycloneDX(repo, deps, scan, sbomSeedGraph())
		if err != nil {
			t.Fatal(err)
		}
		validateAgainst(t, cdxSchema, cdx)
		sp, err := generateSPDX(repo, deps, scan, sbomSeedGraph())
		if err != nil {
			t.Fatal(err)
		}
		validateAgainst(t, spdxSchema, sp)
	}
	bad := []byte(`{"bomFormat":"CycloneDX","specVersion":"1.7","version":1,"components":[{"type":"library","name":"x","licenses":[{"license":{"id":"Not-A-License"}}]}]}`)
	doc, _ := jsonschema.UnmarshalJSON(strings.NewReader(string(bad)))
	if cdxSchema.Validate(doc) == nil {
		t.Error("negative control: an unknown license.id must fail the schema")
	}
}

// TestCycloneDXOmitsNoLicenseSentinels — v0.29.67 review round 9: a stored
// NOASSERTION / NONE / N/A normalizes to "Unknown", and CycloneDX emitted a
// license entry named "Unknown", which reads as a license. SPDX says
// NOASSERTION for the same input; CycloneDX now omits licenses.
func TestCycloneDXOmitsNoLicenseSentinels(t *testing.T) {
	for _, raw := range []string{"NOASSERTION", "NONE", "N/A", "unknown"} {
		if got := makeCDXLicenses(raw, "declared"); got != nil {
			t.Errorf("makeCDXLicenses(%q) = %+v; a no-license sentinel carries no license entry", raw, got)
		}
	}
	if got := makeCDXLicenses("MIT", "declared"); len(got) != 1 {
		t.Errorf("a real license still gets its entry: %+v", got)
	}
}

// TestRootEvidenceOnlyWhenThereIsSome — v0.29.67 review round 10: a ScanCode
// conclusion that normalizes to no license, with no copyrights, still
// allocated an empty "evidence": {} on the root component, which implies
// evidence was gathered.
func TestRootEvidenceOnlyWhenThereIsSome(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "ev", Owner: "_avgt", GitURL: "https://github.com/_avgt/ev"}
	raw, err := generateCycloneDX(repo, nil, &db.ScancodeForSBOM{ConcludedLicenseSPDX: "NOASSERTION"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var bom cycloneDX
	if err := json.Unmarshal(raw, &bom); err != nil {
		t.Fatal(err)
	}
	if bom.Metadata.Component == nil {
		t.Fatal("the document has no root component")
	}
	if ev := bom.Metadata.Component.Evidence; ev != nil {
		t.Errorf("root evidence = %+v; want none for a no-license conclusion with no copyrights", ev)
	}
}
