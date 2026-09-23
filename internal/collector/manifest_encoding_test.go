// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func utf16LEWithBOM(s string) []byte {
	out := []byte{0xff, 0xfe}
	for _, u := range utf16.Encode([]rune(s)) {
		out = append(out, byte(u), byte(u>>8))
	}
	return out
}

// TestLibyearManifestReadersDecodeTheFileEncoding — 2026-09-23 log review:
// 8 repositories had UTF-16LE requirements files and one a UTF-8 BOM; the
// libyear walk read every manifest with a bare os.ReadFile, so the names
// carried NULs and the BOM ("U+FEFF astroid"), and the registry lookups failed
// with "invalid control character in URL". The dependency-name path already
// decoded UTF-16 (decodeIfUTF16); the libyear path did not, and neither
// stripped a UTF-8 BOM. One reader, readManifest, serves both.
func TestLibyearManifestReadersDecodeTheFileEncoding(t *testing.T) {
	dir := t.TempDir()
	req16 := filepath.Join(dir, "a", "requirements.txt")
	reqBOM := filepath.Join(dir, "b", "requirements.txt")
	pkgBOM := filepath.Join(dir, "c", "package.json")
	for path, data := range map[string][]byte{
		req16:  utf16LEWithBOM("requests==2.31.0\r\nflask>=2.0\r\n"),
		reqBOM: append([]byte{0xef, 0xbb, 0xbf}, []byte("astroid==3.0.0\n")...),
		pkgBOM: append([]byte{0xef, 0xbb, 0xbf}, []byte(`{"dependencies":{"lodash":"4.17.21"}}`)...),
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	names := func(deps []libyearDep) string {
		var n []string
		for _, d := range deps {
			n = append(n, d.Name)
		}
		return strings.Join(n, ",")
	}
	if got := names(parseRequirementsTxtVersions(req16)); got != "requests,flask" {
		t.Errorf("UTF-16LE requirements.txt: names %q, want requests,flask", got)
	}
	if got := names(parseRequirementsTxtVersions(reqBOM)); got != "astroid" {
		t.Errorf("UTF-8 BOM requirements.txt: names %q, want astroid", got)
	}
	deps, err := parsePackageJSONVersions(pkgBOM)
	if err != nil || names(deps) != "lodash" {
		t.Errorf("UTF-8 BOM package.json: %q, %v", names(deps), err)
	}
	// The dependency-name path shares the reader, BOM included.
	if got, err := parseDependencyFile(reqBOM, "Python"); err != nil || strings.Join(got, ",") != "astroid" {
		t.Errorf("parseDependencyFile with a UTF-8 BOM: %v, %v", got, err)
	}
}

// TestAnalysisReadsManifestsThroughOneReader pins the shape that let the
// two paths disagree: analysis.go has exactly one raw os.ReadFile, inside
// readManifest.
func TestAnalysisReadsManifestsThroughOneReader(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := srctest.StripGoComments(string(src))
	if n := strings.Count(code, "os.ReadFile("); n != 1 {
		t.Errorf("analysis.go has %d raw os.ReadFile calls; manifests must be read through readManifest (one decoder for UTF-16 and a UTF-8 BOM)", n)
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, string(src), "func readManifest("))
	if !strings.Contains(body, "os.ReadFile(") || !strings.Contains(body, "decodeIfUTF16(") {
		t.Error("readManifest must be the one raw read and must decode the encoding")
	}
}

// TestRequirementsLinesMustNameAPackage — 2026-09-23 log review: conda
// exports saved as requirements.txt ("ujson=5.9.0", "_libgcc_mutex=0.1=main",
// 251 of them in one repo, "pytorch::cpuonly"), RST and prose
// (".. _prerequisites:" with 139 rows, "Copyright 2017 Google Inc."),
// templates ("{{ value }}") and space-separated lists ("pandas numpy") were
// stored as PyPI package names. splitPyNameSpec splits only at a version
// operator, so each whole line became the "name". A requirement's name must
// be a PEP 508 name; anything else is not a PyPI requirement. Both readers
// (libyear and dependency names) apply the one check.
func TestRequirementsLinesMustNameAPackage(t *testing.T) {
	content := strings.Join([]string{
		"requests==2.31.0",
		"Flask>=2.0",
		"anyio[trio]~=4.0",
		"zope.interface",
		"ruamel.yaml.clib==0.2.8",
		"ujson=5.9.0",
		"_libgcc_mutex=0.1=main",
		"pytorch::cpuonly",
		".. _prerequisites:",
		"Copyright 2017 Google Inc.",
		"NetKet is a light-weight framework",
		"] =",
		"{{ value }}",
		"pandas numpy",
		"cryptography --global-option=build_ext",
		"pyyaml==6.0.3 \\",
	}, "\n")
	want := "requests,Flask,anyio,zope.interface,ruamel.yaml.clib,cryptography,pyyaml"
	if got := strings.Join(parseRequirementsTxt(content), ","); got != want {
		t.Errorf("dependency names: %q, want %q", got, want)
	}
	path := filepath.Join(t.TempDir(), "requirements.txt")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, d := range parseRequirementsTxtVersions(path) {
		names = append(names, d.Name)
	}
	if got := strings.Join(names, ","); got != want {
		t.Errorf("libyear names: %q, want %q", got, want)
	}
}
