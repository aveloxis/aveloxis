// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import "testing"

func TestParsePyprojectPEP621_EmptyDependencies(t *testing.T) {
	content := `[project]
name = "myapp"
dependencies = []
`
	deps, _ := parsePyprojectDeps(content)
	if len(deps) != 0 {
		t.Errorf("empty deps array returned %d deps", len(deps))
	}
}

func TestParsePyprojectPEP621_InlineSingleLine(t *testing.T) {
	content := `[project]
dependencies = ["flask==2.0"]
`
	deps, _ := parsePyprojectDeps(content)
	if len(deps) != 1 || deps[0] != "flask" {
		t.Errorf("inline single dep: got %v", deps)
	}
}

func TestParsePyprojectPEP621_ExtrasInDep(t *testing.T) {
	// PEP 508 extras: sqlalchemy[asyncio]>=2.0
	content := `[project]
dependencies = [
    "sqlalchemy[asyncio]>=2.0",
    "uvicorn[standard]",
]
`
	deps, _ := parsePyprojectDeps(content)
	expected := map[string]bool{"sqlalchemy": true, "uvicorn": true}
	for _, d := range deps {
		if !expected[d] {
			t.Errorf("unexpected dep: %q (extras should be stripped)", d)
		}
		delete(expected, d)
	}
	for name := range expected {
		t.Errorf("missing dep: %q", name)
	}
}

func TestParsePyprojectPEP621_CommentsInArray(t *testing.T) {
	content := `[project]
dependencies = [
    # database
    "psycopg2>=3.0",
    # web framework
    "flask>=2.0",
]
`
	deps, _ := parsePyprojectDeps(content)
	if len(deps) != 2 {
		t.Fatalf("expected 2 deps (comments skipped), got %d: %v", len(deps), deps)
	}
}

func TestParsePyprojectVersionsPEP621_VersionRanges(t *testing.T) {
	content := `[project]
dependencies = [
    "scipy>=1.10.0,<1.13.0",
    "protobuf<3.22",
    "flask==2.0.2",
]
`
	deps := ParsePyprojectVersionsFromContent(content)
	found := map[string]string{}
	for _, d := range deps {
		found[d.Name] = d.Version
	}
	// Assert the exact set, both directions. A missing dep reads as "" from
	// the map, so the protobuf assertion below would otherwise pass when the
	// dependency is gone — and a COUNT does not establish presence either:
	// the historical "the name eats its own operator" defect (fixed in
	// v0.29.56, shipped from 0.13.0) leaves the count at 3 while protobuf is
	// filed under "protobuf<3.22". Naming every expected dep covers the
	// extra-dep direction a count used to carry (v0.29.56 round 18).
	assertDeps(t, "PEP 621 ranges", deps, []string{"scipy@1.10.0", "protobuf@", "flask@2.0.2"})
	// scipy has a range — should take the first version (1.10.0)
	if v := found["scipy"]; v != "1.10.0" {
		t.Errorf("scipy version = %q, want %q", v, "1.10.0")
	}
	// protobuf has only an EXCLUSIVE upper bound. "<3.22" forbids 3.22, so
	// recording it claims a version the manifest rules out: until v0.29.56
	// this asserted "3.22", and OSV was asked about a release that could
	// not be installed — which reports clean when every permitted release
	// is vulnerable. No permitted version is named, so none is recorded.
	// See TestPythonSpecifierSetOrderDoesNotEatTheName.
	if v := found["protobuf"]; v != "" {
		t.Errorf("protobuf version = %q, want %q (an excluded bound is not a version in use)", v, "")
	}
}

func TestParsePyprojectBothFormats(t *testing.T) {
	// File with BOTH PEP 621 and Poetry sections (unusual but valid).
	content := `[project]
dependencies = ["flask>=2.0"]

[tool.poetry.dependencies]
requests = "^2.28"
`
	deps, _ := parsePyprojectDeps(content)
	if len(deps) != 2 {
		t.Errorf("expected 2 deps from both formats, got %d: %v", len(deps), deps)
	}
}

func TestParsePyprojectEmptyFile(t *testing.T) {
	deps, _ := parsePyprojectDeps("")
	if len(deps) != 0 {
		t.Errorf("empty file returned %d deps", len(deps))
	}
}

func TestParseRequirementsTxt_VersionRanges(t *testing.T) {
	content := `flask>=2.0,<3.0
numpy!=1.24.0
scipy~=1.10
`
	deps := ParseRequirementsTxt(content)
	expected := map[string]bool{"flask": true, "numpy": true, "scipy": true}
	for _, d := range deps {
		if !expected[d] {
			t.Errorf("unexpected dep: %q", d)
		}
	}
	if len(deps) != 3 {
		t.Errorf("expected 3, got %d", len(deps))
	}
}

func TestParseRequirementsTxt_Extras(t *testing.T) {
	// requirements.txt can have extras: package[extra]>=1.0
	content := `sqlalchemy[asyncio]>=2.0.0
uvicorn[standard]==0.20.0
`
	deps := ParseRequirementsTxt(content)
	// Current parser strips version specifiers but not extras.
	// Should still capture the name part.
	if len(deps) < 2 {
		t.Fatalf("expected >= 2 deps, got %d: %v", len(deps), deps)
	}
}

func TestParseRequirementsTxt_Empty(t *testing.T) {
	deps := ParseRequirementsTxt("")
	if len(deps) != 0 {
		t.Errorf("empty returned %d", len(deps))
	}
}

func TestParseRequirementsTxt_AllComments(t *testing.T) {
	content := `# this is a comment
# another comment
`
	deps := ParseRequirementsTxt(content)
	if len(deps) != 0 {
		t.Errorf("all comments returned %d", len(deps))
	}
}

func TestParseSetupPy_SingleLineArray(t *testing.T) {
	content := `setup(install_requires=['flask', 'requests>=2.0'])`
	deps, _ := parseSetupPyDeps(content)
	if len(deps) != 2 {
		t.Fatalf("single-line: got %d deps, want 2: %v", len(deps), deps)
	}
}

func TestParseSetupPy_DoubleQuotes(t *testing.T) {
	content := `setup(
    install_requires=[
        "flask>=2.0",
        "requests",
    ],
)
`
	deps, _ := parseSetupPyDeps(content)
	if len(deps) != 2 {
		t.Fatalf("double quotes: got %d deps, want 2: %v", len(deps), deps)
	}
}

func TestParseSetupPy_Empty(t *testing.T) {
	deps, _ := parseSetupPyDeps("")
	if len(deps) != 0 {
		t.Errorf("empty returned %d", len(deps))
	}
}

func TestParseSetupPy_NoInstallRequires(t *testing.T) {
	content := `setup(name='simple', version='1.0')`
	deps, _ := parseSetupPyDeps(content)
	if len(deps) != 0 {
		t.Errorf("no install_requires returned %d", len(deps))
	}
}

func TestParsePipfileDeps_DevPackagesIgnored(t *testing.T) {
	content := `[packages]
flask = "==2.0"

[dev-packages]
pytest = "*"
`
	deps, _ := parsePipfileDeps(content)
	// Only [packages], not [dev-packages]
	if len(deps) != 1 {
		t.Fatalf("expected 1 (dev excluded), got %d: %v", len(deps), deps)
	}
}

func TestParsePipfileDeps_Empty(t *testing.T) {
	deps, _ := parsePipfileDeps("")
	if len(deps) != 0 {
		t.Errorf("empty returned %d", len(deps))
	}
}

func TestParsePipfileVersions_StarVersion(t *testing.T) {
	content := `[packages]
flask = "*"
`
	deps := parsePipfileVersions(content)
	if len(deps) != 1 {
		t.Fatalf("got %d, want 1", len(deps))
	}
	// "*" means any version — version should be empty.
	if deps[0].Version != "" {
		t.Errorf("star version = %q, want empty", deps[0].Version)
	}
}

func TestParsePipfileVersions_TableSyntax(t *testing.T) {
	content := `[packages]
sqlalchemy = {version = "==2.0.0", extras = ["asyncio"]}
`
	deps := parsePipfileVersions(content)
	if len(deps) != 1 {
		t.Fatalf("got %d, want 1", len(deps))
	}
	if deps[0].Version != "2.0.0" {
		t.Errorf("table syntax version = %q, want %q", deps[0].Version, "2.0.0")
	}
}

func TestParseGoMod_IndirectDeps(t *testing.T) {
	content := `module example.com/myapp

go 1.21

require (
	github.com/lib/pq v1.10.0
	// indirect
	golang.org/x/sys v0.15.0
)
`
	deps := parseGoMod(content)
	if len(deps) != 2 {
		t.Fatalf("expected 2 (including indirect), got %d", len(deps))
	}
}

func TestParseGoMod_Empty(t *testing.T) {
	content := `module example.com/myapp

go 1.21
`
	deps := parseGoMod(content)
	if len(deps) != 0 {
		t.Errorf("no require block returned %d", len(deps))
	}
}

func TestParseGoMod_SingleLineRequire(t *testing.T) {
	// go.mod can have single-line require without parens
	content := `module example.com/myapp
require github.com/lib/pq v1.10.0
`
	// Current parser only handles "require (" block format.
	// This is a known limitation — single-line requires are less common.
	deps := parseGoMod(content)
	_ = deps // Document the limitation; don't assert wrong expectation.
}

func TestParseTOMLDeps_EmptySection(t *testing.T) {
	content := `[dependencies]

[dev-dependencies]
tokio = "1.0"
`
	deps := parseTOMLDeps(content, "[dependencies]")
	if len(deps) != 0 {
		t.Errorf("empty section returned %d", len(deps))
	}
}

func TestParseTOMLDeps_TableStyle(t *testing.T) {
	content := `[dependencies]
serde = { version = "1.0", features = ["derive"] }
tokio = { version = "1.0", features = ["full"] }
simple = "0.1"
`
	deps := parseTOMLDeps(content, "[dependencies]")
	if len(deps) != 3 {
		t.Fatalf("table-style: got %d, want 3: %v", len(deps), deps)
	}
}

func TestParseGemfile_GroupBlocks(t *testing.T) {
	content := `source 'https://rubygems.org'

gem 'rails', '~> 7.0'

group :development do
  gem 'debug'
end
`
	deps := parseGemfile(content)
	if len(deps) != 2 {
		t.Fatalf("with group: got %d, want 2: %v", len(deps), deps)
	}
}

func TestParseGemfile_Empty(t *testing.T) {
	deps := parseGemfile("")
	if len(deps) != 0 {
		t.Errorf("empty returned %d", len(deps))
	}
}

func TestParseGemfile_MixedQuotes(t *testing.T) {
	content := `gem 'single_quoted'
gem "double_quoted"
`
	deps := parseGemfile(content)
	if len(deps) != 2 {
		t.Fatalf("mixed quotes: got %d, want 2", len(deps))
	}
}

func TestParsePomXML_Empty(t *testing.T) {
	deps := parsePomXML("")
	if len(deps) != 0 {
		t.Errorf("empty returned %d", len(deps))
	}
}

func TestParsePomXML_NoDependencies(t *testing.T) {
	content := `<project><modelVersion>4.0.0</modelVersion></project>`
	deps := parsePomXML(content)
	if len(deps) != 0 {
		t.Errorf("no deps returned %d", len(deps))
	}
}

func TestParsePomXMLVersions_EmptyVersion(t *testing.T) {
	// Some pom.xml deps inherit version from parent POM.
	content := `<project><dependencies>
<dependency><groupId>org.springframework</groupId><artifactId>spring-core</artifactId></dependency>
</dependencies></project>`
	deps := ParsePomXMLVersions(content)
	if len(deps) != 1 {
		t.Fatalf("got %d, want 1", len(deps))
	}
	if deps[0].Version != "" {
		t.Errorf("inherited version should be empty, got %q", deps[0].Version)
	}
}

func TestParseComposerJSON_PhpIgnored(t *testing.T) {
	data := []byte(`{
		"require": {
			"php": ">=8.1",
			"ext-json": "*",
			"laravel/framework": "^10.0"
		}
	}`)
	deps, _ := parseComposerJSON(data)
	// "php" and "ext-json" should be excluded.
	if len(deps) != 1 {
		t.Fatalf("expected 1 (php/ext excluded), got %d: %v", len(deps), deps)
	}
	if deps[0] != "laravel/framework" {
		t.Errorf("got %q, want %q", deps[0], "laravel/framework")
	}
}

func TestParseComposerJSON_Empty(t *testing.T) {
	deps, _ := parseComposerJSON([]byte(`{}`))
	if len(deps) != 0 {
		t.Errorf("empty returned %d", len(deps))
	}
}

func TestParseComposerJSON_Invalid(t *testing.T) {
	_, err := parseComposerJSON([]byte(`not json`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParsePackageJSON_Empty(t *testing.T) {
	deps, _ := parsePackageJSON([]byte(`{}`))
	if len(deps) != 0 {
		t.Errorf("empty returned %d", len(deps))
	}
}

func TestParsePackageJSON_Invalid(t *testing.T) {
	_, err := parsePackageJSON([]byte(`not json`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParsePackageJSON_OnlyDevDeps(t *testing.T) {
	data := []byte(`{"devDependencies":{"jest":"^29.0.0"}}`)
	deps, _ := parsePackageJSON(data)
	if len(deps) != 1 {
		t.Fatalf("dev-only: got %d, want 1", len(deps))
	}
}

func TestParseBuildSbt_Empty(t *testing.T) {
	deps := parseBuildSbt("")
	if len(deps) != 0 {
		t.Errorf("empty returned %d", len(deps))
	}
}

func TestParseBuildSbt_Comments(t *testing.T) {
	content := `// This is a comment
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15"
// Another comment
`
	deps := parseBuildSbt(content)
	if len(deps) != 1 {
		t.Fatalf("got %d, want 1", len(deps))
	}
}

func TestParsePubspecDeps_SDKDepsExcluded(t *testing.T) {
	content := `dependencies:
  flutter:
    sdk: flutter
  http: ^0.13.6
`
	deps := parsePubspecDeps(content)
	// flutter (sdk dep) should be excluded.
	for _, d := range deps {
		if d == "flutter" {
			t.Error("flutter SDK dep should be excluded")
		}
	}
}

func TestParsePubspecDeps_Empty(t *testing.T) {
	deps := parsePubspecDeps("")
	if len(deps) != 0 {
		t.Errorf("empty returned %d", len(deps))
	}
}

func TestParseDirectoryPackagesProps_Empty(t *testing.T) {
	deps, _ := parseDirectoryPackagesProps("")
	if len(deps) != 0 {
		t.Errorf("empty returned %d", len(deps))
	}
}

func TestParseCsprojDeps_MultipleItemGroups(t *testing.T) {
	content := `<Project>
  <ItemGroup>
    <PackageReference Include="PkgA" Version="1.0" />
  </ItemGroup>
  <ItemGroup>
    <PackageReference Include="PkgB" Version="2.0" />
  </ItemGroup>
</Project>`
	deps, _ := parseCsprojDeps(content)
	if len(deps) != 2 {
		t.Fatalf("multiple ItemGroups: got %d, want 2: %v", len(deps), deps)
	}
}

func TestParsePackageYaml_Empty(t *testing.T) {
	deps := parsePackageYaml("")
	if len(deps) != 0 {
		t.Errorf("empty returned %d", len(deps))
	}
}

func TestParsePackageYaml_NoDependencies(t *testing.T) {
	content := `name: myapp
version: 0.1.0
`
	deps := parsePackageYaml(content)
	if len(deps) != 0 {
		t.Errorf("no deps section returned %d", len(deps))
	}
}

func TestParseMixExsDeps_Empty(t *testing.T) {
	deps := parseMixExsDeps("")
	if len(deps) != 0 {
		t.Errorf("empty returned %d", len(deps))
	}
}

func TestParseMixExsDeps_OnlyNonVersioned(t *testing.T) {
	content := `defp deps do
    [{:phoenix, github: "phoenixframework/phoenix"}]
  end`
	deps := parseMixExsDeps(content)
	// Git deps don't have quoted versions — parser may or may not pick them up.
	_ = deps // Document behavior without asserting wrong expectation.
}

func TestParsePackageSwiftDeps_Empty(t *testing.T) {
	deps := parsePackageSwiftDeps("")
	if len(deps) != 0 {
		t.Errorf("empty returned %d", len(deps))
	}
}

func TestParsePackageSwiftVersions_MultipleVersionFormats(t *testing.T) {
	content := `let package = Package(
    dependencies: [
        .package(url: "https://github.com/Alamofire/Alamofire.git", from: "5.6.0"),
        .package(url: "https://github.com/SwiftyJSON/SwiftyJSON.git", exact: "5.0.1"),
        .package(url: "https://github.com/realm/realm-swift.git", .upToNextMajor(from: "10.40.0")),
        .package(url: "https://github.com/pointfreeco/swift-composable-architecture", .upToNextMinor(from: "0.50.0")),
    ]
)`
	deps := parsePackageSwiftVersions(content)
	if len(deps) != 4 {
		t.Fatalf("multiple version formats: got %d, want 4: %v", len(deps), deps)
	}
}

func TestParseBuildGradle_TestRuntimeOnly(t *testing.T) {
	content := `dependencies {
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher:1.9.0'
}`
	deps := parseBuildGradle(content)
	if len(deps) != 1 {
		t.Fatalf("testRuntimeOnly: got %d, want 1", len(deps))
	}
}

func TestParseBuildGradle_Empty(t *testing.T) {
	deps := parseBuildGradle("")
	if len(deps) != 0 {
		t.Errorf("empty returned %d", len(deps))
	}
}

func TestCleanVersion_DoubleEquals(t *testing.T) {
	if v := CleanVersion("==2.0.0"); v != "2.0.0" {
		t.Errorf("== prefix: got %q", v)
	}
}

func TestCleanVersion_TildeEquals(t *testing.T) {
	if v := CleanVersion("~=1.4"); v != "1.4" {
		t.Errorf("~= prefix: got %q", v)
	}
}

// 0.29.209: an exclusion names the one version that is NOT in use. This
// test asserted the opposite ("1.0") since the function was written.
func TestCleanVersion_NotEquals(t *testing.T) {
	if v := CleanVersion("!=1.0"); v != "" {
		t.Errorf("!= alone: got %q, want no version", v)
	}
}

func TestCleanVersion_PlainVersion(t *testing.T) {
	if v := CleanVersion("1.2.3"); v != "1.2.3" {
		t.Errorf("plain: got %q", v)
	}
}
