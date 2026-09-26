// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// sbomToolsEnv names the Python (with spdx-tools and cyclonedx-python-lib)
// the official-validator test runs. The ONE spelling: the weekly workflow's
// pin (TestOfficialSBOMValidatorsAreScheduled) reads it too, so renaming it
// here cannot leave CI exporting the old name and the test skipping green
// (review round 13).
const sbomToolsEnv = "AVELOXIS_TEST_SBOM_TOOLS"

// cdxValidatorScript runs cyclonedx-python-lib's strict JSON validator for the
// schema version the export targets; it exits 1 and prints the error when the
// document is invalid.
const cdxValidatorScript = `import sys
from cyclonedx.validation.json import JsonStrictValidator
from cyclonedx.schema import SchemaVersion
err = JsonStrictValidator(SchemaVersion.V1_7).validate_str(open(sys.argv[1]).read())
if err:
    print(err)
    sys.exit(1)
`

// TestSBOMsPassTheOfficialValidators — worklist 53 (summary/39 §9): the SPDX
// project's own validator (pyspdxtools, from spdx-tools) and CycloneDX's
// (cyclonedx-python-lib) check generated documents independently of our
// parser and our reading of the schemas. Dev/CI only, never a runtime
// component: set AVELOXIS_TEST_SBOM_TOOLS to a Python interpreter with both
// installed, e.g.
//
//	python3 -m venv /tmp/sbomtools && /tmp/sbomtools/bin/pip install spdx-tools 'cyclonedx-python-lib[json-validation]'
//	AVELOXIS_TEST_SBOM_TOOLS=/tmp/sbomtools/bin/python go test ./internal/collector/ -run TestSBOMsPassTheOfficialValidators
//
// A negative control per tool proves each one rejects a bad document, so a
// broken install cannot read as a pass.
func TestSBOMsPassTheOfficialValidators(t *testing.T) {
	python := os.Getenv(sbomToolsEnv)
	if python == "" {
		t.Skipf("set %s to a Python with spdx-tools and cyclonedx-python-lib to run the official SBOM validators", sbomToolsEnv)
	}
	pyspdxtools := filepath.Join(filepath.Dir(python), "pyspdxtools")
	if _, err := os.Stat(pyspdxtools); err != nil {
		t.Fatalf("%s is set but %s is missing: %v", sbomToolsEnv, pyspdxtools, err)
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "cdxval.py")
	if err := os.WriteFile(script, []byte(cdxValidatorScript), 0o600); err != nil {
		t.Fatal(err)
	}
	spdxOK := func(path string) (bool, string) {
		// The exit code decides: pyspdxtools exits 1 on an invalid document
		// (verified 2026-09-23). A substring check on the output could read a
		// tool warning that mentions "issues" as a rejection (mcp-gopls
		// review B2); the negative control proves the exit code still bites.
		out, err := exec.Command(pyspdxtools, "-i", path).CombinedOutput()
		return err == nil, string(out)
	}
	cdxOK := func(path string) (bool, string) {
		out, err := exec.Command(python, script, path).CombinedOutput()
		return err == nil, string(out)
	}
	write := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	repo, deps, scan := licenseSeed()
	cdx, err := generateCycloneDX(repo, deps, scan, sbomSeedGraph())
	if err != nil {
		t.Fatal(err)
	}
	sp, err := generateSPDX(repo, deps, scan, sbomSeedGraph())
	if err != nil {
		t.Fatal(err)
	}
	if ok, out := spdxOK(write("seed.spdx.json", sp)); !ok {
		t.Errorf("pyspdxtools rejects the generated SPDX document:\n%s", out)
	}
	if ok, out := cdxOK(write("seed.cdx.json", cdx)); !ok {
		t.Errorf("the CycloneDX validator rejects the generated document:\n%s", out)
	}

	// Negative controls: each tool must reject a known-bad document.
	badSPDX := strings.Replace(string(sp), `"licenseDeclared": "MIT OR Apache-2.0"`, `"licenseDeclared": "MIT or garbage((("`, 1)
	if badSPDX == string(sp) {
		t.Fatal("negative control did not apply to the SPDX document")
	}
	if ok, _ := spdxOK(write("bad.spdx.json", []byte(badSPDX))); ok {
		t.Error("negative control: pyspdxtools accepted an invalid license expression; the tool check is not working")
	}
	badCDX := strings.Replace(string(cdx), `"id": "Unicode-3.0"`, `"id": "Not-A-License"`, 1)
	if badCDX == string(cdx) {
		t.Fatal("negative control did not apply to the CycloneDX document")
	}
	if ok, _ := cdxOK(write("bad.cdx.json", []byte(badCDX))); ok {
		t.Error("negative control: the CycloneDX validator accepted an unknown license ID; the tool check is not working")
	}
}
