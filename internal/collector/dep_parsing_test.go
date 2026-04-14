package collector

import (
	"os"
	"strings"
	"testing"
)

// --- Go purl v-prefix ---

// TestGoModKeepsVPrefix verifies that parseGoModVersions does NOT strip the
// leading "v" from Go module versions. The purl spec for the golang type
// keeps the v prefix, and OSV.dev's Go ecosystem matches on versions with v.
// Stripping it causes all Go vuln scanning to silently return nothing.
func TestGoModKeepsVPrefix(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func parseGoModVersions")
	if idx < 0 {
		t.Fatal("cannot find parseGoModVersions")
	}
	fnBody := code[idx:]
	if len(fnBody) > 2000 {
		fnBody = fnBody[:2000]
	}

	// Must NOT strip the v prefix from versions.
	if strings.Contains(fnBody, `TrimPrefix(parts[1], "v")`) {
		t.Error("parseGoModVersions must NOT strip the leading 'v' from Go versions — " +
			"the purl spec keeps it, and OSV.dev matches on versions with 'v'. " +
			"Stripping it breaks all Go vulnerability scanning.")
	}
}

// --- cleanVersion fixes ---

// TestCleanVersionTrimsSpace verifies cleanVersion trims leading/trailing space
// from the result. Without this, "~> 1.2" becomes " 1.2" with a leading space
// that corrupts purls.
func TestCleanVersionTrimsSpace(t *testing.T) {
	// The function strips operators in sequence, but intermediate whitespace
	// from "~> 1.2" (~ stripped, then > stripped) leaves " 1.2".
	got := cleanVersion("~> 1.2")
	if strings.HasPrefix(got, " ") {
		t.Errorf("cleanVersion(\"~> 1.2\") = %q, has leading space", got)
	}
	if got != "1.2" {
		t.Errorf("cleanVersion(\"~> 1.2\") = %q, want \"1.2\"", got)
	}
}

// TestCleanVersionHandlesCompoundRanges verifies cleanVersion extracts the first
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
		got := cleanVersion(tt.input)
		if strings.Contains(got, ",") || strings.Contains(got, "||") || strings.Contains(got, "<") {
			t.Errorf("cleanVersion(%q) = %q, still contains range operators", tt.input, got)
		}
	}
}

// --- Go single-line require ---

// TestGoModParsesSingleLineRequire verifies parseGoModVersions handles
// single-line require directives like "require github.com/foo/bar v1.2.3".
func TestGoModParsesSingleLineRequire(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func parseGoModVersions")
	if idx < 0 {
		t.Fatal("cannot find parseGoModVersions")
	}
	fnBody := code[idx:]
	if len(fnBody) > 2000 {
		fnBody = fnBody[:2000]
	}

	// Must handle single-line require outside block syntax.
	if !strings.Contains(fnBody, `"require "`) && !strings.Contains(fnBody, `HasPrefix(line, "require ")`) {
		t.Error("parseGoModVersions must handle single-line 'require module version' directives, " +
			"not just the 'require (' block form")
	}
}

// --- parseRequirementsTxt ---

// TestRequirementsTxtHandlesLessThan verifies parseRequirementsTxt handles
// flask<2.0 (just < without =).
func TestRequirementsTxtHandlesLessThan(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the parseRequirementsTxt function.
	idx := strings.Index(code, "func parseRequirementsTxt")
	if idx < 0 {
		t.Fatal("cannot find parseRequirementsTxt")
	}
	fnBody := code[idx:]
	if len(fnBody) > 1000 {
		fnBody = fnBody[:1000]
	}

	// Must handle bare "<" as a separator (not just "<=", ">=", "!=", "~=", ">").
	if !strings.Contains(fnBody, `"<"`) {
		t.Error("parseRequirementsTxt must handle '<' as a version separator — " +
			"flask<2.0 currently becomes dep name 'flask<2.0' which is invalid")
	}
}

// --- NuGet case sensitivity ---

// TestNuGetPackagesConfigCaseInsensitive verifies the NuGet parser handles
// both id="..." and Id="..." attribute casing.
func TestNuGetPackagesConfigCaseInsensitive(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func parseNuGetPackagesConfigVersions")
	if idx < 0 {
		t.Fatal("cannot find parseNuGetPackagesConfigVersions")
	}
	fnBody := code[idx:]
	if len(fnBody) > 1500 {
		fnBody = fnBody[:1500]
	}

	// Must handle case-insensitive attribute names.
	if !strings.Contains(fnBody, "ToLower") && !strings.Contains(fnBody, "(?i)") &&
		!strings.Contains(fnBody, `Id="`) {
		t.Error("parseNuGetPackagesConfigVersions must handle case-insensitive attribute names — " +
			"older .NET projects use Id=\"...\" and Version=\"...\" (capital letters)")
	}
}

// --- package.json peer deps ---

// TestPackageJSONParsesAllDepTypes verifies parsePackageJSONVersions reads
// peerDependencies and optionalDependencies, not just dependencies and devDependencies.
func TestPackageJSONParsesAllDepTypes(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func parsePackageJSONVersions")
	if idx < 0 {
		t.Fatal("cannot find parsePackageJSONVersions")
	}
	fnBody := code[idx:]
	if len(fnBody) > 1500 {
		fnBody = fnBody[:1500]
	}

	if !strings.Contains(fnBody, "peerDependencies") && !strings.Contains(fnBody, "PeerDependencies") {
		t.Error("parsePackageJSONVersions must unmarshal peerDependencies — " +
			"missing peer deps means the SBOM and vuln scan are incomplete")
	}
}

// --- Cargo missing tables ---

// TestCargoParsesBuildDependencies verifies parseCargoVersions handles
// [build-dependencies] in addition to [dependencies] and [dev-dependencies].
func TestCargoParsesBuildDependencies(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func parseCargoVersions")
	if idx < 0 {
		t.Fatal("cannot find parseCargoVersions")
	}
	fnBody := code[idx:]
	if len(fnBody) > 1500 {
		fnBody = fnBody[:1500]
	}

	if !strings.Contains(fnBody, "build-dependencies") {
		t.Error("parseCargoVersions must handle [build-dependencies] table — " +
			"build deps are needed for accurate SBOM and vuln scanning")
	}
}
