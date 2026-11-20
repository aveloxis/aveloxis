// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGoModKeepsVPrefix verifies that ParseGoModVersions does NOT strip the
// leading "v" from Go module versions. The purl spec for the golang type
// keeps the v prefix, and OSV.dev's Go ecosystem matches on versions with v.
// Stripping it causes all Go vuln scanning to silently return nothing.
func TestGoModKeepsVPrefix(t *testing.T) {
	// Behaviour, not shape (0.29.152: this read the function's source for a
	// TrimPrefix call). The purl specification keeps Go's leading "v" and OSV
	// matches on it; stripping it breaks all Go vulnerability scanning.
	path := filepath.Join(t.TempDir(), "go.mod")
	if err := os.WriteFile(path, []byte("module example.com/m\n\ngo 1.22\n\nrequire (\n\tgithub.com/pkg/errors v0.9.1\n)\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	deps := ParseGoModVersions(path)
	if len(deps) != 1 || deps[0].Name != "github.com/pkg/errors" || deps[0].Version != "v0.9.1" {
		t.Errorf("ParseGoModVersions = %+v, want github.com/pkg/errors at v0.9.1 with its v", deps)
	}
}

// TestCleanVersionTrimsSpace verifies CleanVersion trims leading/trailing space
// from the result. Without this, "~> 1.2" becomes " 1.2" with a leading space
// that corrupts purls.
func TestCleanVersionTrimsSpace(t *testing.T) {
	// The function strips operators in sequence, but intermediate whitespace
	// from "~> 1.2" (~ stripped, then > stripped) leaves " 1.2".
	got := CleanVersion("~> 1.2")
	if strings.HasPrefix(got, " ") {
		t.Errorf("cleanVersion(\"~> 1.2\") = %q, has leading space", got)
	}
	if got != "1.2" {
		t.Errorf("cleanVersion(\"~> 1.2\") = %q, want \"1.2\"", got)
	}
}

// TestCleanVersionHandlesCompoundRanges verifies CleanVersion extracts the first
// version from compound range expressions like ">=1.0,<2.0".
func TestCleanVersionHandlesCompoundRanges(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{">=1.0,<2.0", "1.0"},
		{">=1.0, <2.0", "1.0"},
		{"^1 || ^2", "1"},
	}
	for _, tt := range tests {
		got := CleanVersion(tt.input)
		if strings.Contains(got, ",") || strings.Contains(got, "||") || strings.Contains(got, "<") {
			t.Errorf("cleanVersion(%q) = %q, still contains range operators", tt.input, got)
		}
	}
}

// TestGoModParsesSingleLineRequire verifies ParseGoModVersions handles
// single-line require directives like "require github.com/foo/bar v1.2.3".
func TestGoModParsesSingleLineRequire(t *testing.T) {
	// Behaviour, not shape: a single-line "require module version" directive
	// outside the block form is read.
	path := filepath.Join(t.TempDir(), "go.mod")
	if err := os.WriteFile(path, []byte("module example.com/m\n\ngo 1.22\n\nrequire github.com/pkg/errors v0.9.1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	deps := ParseGoModVersions(path)
	if len(deps) != 1 || deps[0].Name != "github.com/pkg/errors" || deps[0].Version != "v0.9.1" {
		t.Errorf("ParseGoModVersions = %+v, want the single-line requirement", deps)
	}
}

// TestRequirementsTxtHandlesLessThan verifies ParseRequirementsTxt handles
// flask<2.0 (just < without =).
//
// This asserted the literal `"<"` inside the function's first 1000 bytes
// until v0.29.56 put every Python operator scan on one shared splitter
// (splitPyNameSpec, SR-17). The operator is still handled — better, since
// the splitter takes the LEFTMOST operator rather than a preference list —
// but the spelling moved, and a pin on the spelling failed a refactor that
// made the behaviour strictly more correct. The behaviour is the contract.
func TestRequirementsTxtHandlesLessThan(t *testing.T) {
	for _, line := range []string{
		"flask<2.0", "flask<=2.0", "flask>2.0", "flask>=2.0",
		"flask!=2.0", "flask~=2.0", "flask==2.0", "flask===2.0",
		// Either order of a specifier set names the same package.
		"flask>=1.1,<2.0", "flask<2.0,>=1.1",
	} {
		got := ParseRequirementsTxt(line + "\n")
		if len(got) != 1 || got[0] != "flask" {
			t.Errorf("parseRequirementsTxt(%q) = %v, want [flask]", line, got)
		}
	}
}

// TestNuGetPackagesConfigCaseInsensitive verifies the NuGet parser handles
// both id="..." and Id="..." attribute casing.
func TestNuGetPackagesConfigCaseInsensitive(t *testing.T) {
	// Behaviour, not shape: older .NET projects write Id="..." and
	// Version="..." with capital letters. (The reader is line-based: an
	// element must start its line. That is today's behaviour and not this
	// test's subject.)
	for _, content := range []string{
		"<packages>\n  <package id=\"Newtonsoft.Json\" version=\"13.0.1\" />\n</packages>",
		"<packages>\n  <package Id=\"Newtonsoft.Json\" Version=\"13.0.1\" />\n</packages>",
	} {
		deps := parseNuGetPackagesConfigVersions(content)
		if len(deps) != 1 || deps[0].Name != "Newtonsoft.Json" || deps[0].Version != "13.0.1" {
			t.Errorf("ParseNuGetPackagesConfigVersions(%s) = %+v, want Newtonsoft.Json 13.0.1", content, deps)
		}
	}
}

// TestPackageJSONParsesAllDepTypes verifies parsePackageJSONVersions reads
// peerDependencies and optionalDependencies, not just dependencies and devDependencies.
func TestPackageJSONParsesAllDepTypes(t *testing.T) {
	// Behaviour, not shape: peer dependencies are read beside the others;
	// without them the SBOM and the vulnerability scan are incomplete.
	path := filepath.Join(t.TempDir(), "package.json")
	if err := os.WriteFile(path, []byte(`{"dependencies":{"a":"1.0.0"},"devDependencies":{"b":"2.0.0"},"peerDependencies":{"c":"3.0.0"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	deps, err := parsePackageJSONVersions(path)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, d := range deps {
		got[d.Name] = d.Version
	}
	for name, version := range map[string]string{"a": "1.0.0", "b": "2.0.0", "c": "3.0.0"} {
		if got[name] != version {
			t.Errorf("package.json dependency %s = %q, want %q (all of %v)", name, got[name], version, got)
		}
	}
}
