package collector

import (
	"testing"
)

// ============================================================
// build.gradle / build.gradle.kts — libyear version parser
// ============================================================

func TestParseBuildGradleVersions(t *testing.T) {
	content := `plugins {
    id 'java'
}

dependencies {
    implementation 'org.springframework:spring-core:5.3.20'
    implementation "com.google.guava:guava:31.1-jre"
    api 'junit:junit:4.13.2'
    testImplementation 'org.mockito:mockito-core:4.5.1'
    compileOnly 'org.projectlombok:lombok:1.18.24'
    runtimeOnly 'mysql:mysql-connector-java:8.0.29'
}
`
	deps := parseBuildGradleVersions(content)
	expected := map[string]string{
		"org.springframework:spring-core":    "5.3.20",
		"com.google.guava:guava":             "31.1-jre",
		"junit:junit":                        "4.13.2",
		"org.mockito:mockito-core":           "4.5.1",
		"org.projectlombok:lombok":           "1.18.24",
		"mysql:mysql-connector-java":         "8.0.29",
	}
	if len(deps) != len(expected) {
		t.Fatalf("parseBuildGradleVersions returned %d deps, want %d: %v", len(deps), len(expected), deps)
	}
	for _, d := range deps {
		key := d.Name
		if v, ok := expected[key]; !ok {
			t.Errorf("unexpected dep: %q", key)
		} else if d.Version != v {
			t.Errorf("dep %q version = %q, want %q", key, d.Version, v)
		}
		if d.Manager != "maven" {
			t.Errorf("dep %q manager = %q, want maven", key, d.Manager)
		}
	}
}

func TestParseBuildGradleKtsVersions(t *testing.T) {
	// Kotlin DSL uses parenthesized string syntax.
	content := `dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib:1.8.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
}
`
	deps := parseBuildGradleVersions(content)
	if len(deps) != 2 {
		t.Fatalf("got %d deps, want 2: %v", len(deps), deps)
	}
}

func TestParseBuildGradleVersionsNoVersion(t *testing.T) {
	// Some Gradle deps use BOM/platform and omit version.
	content := `dependencies {
    implementation 'org.springframework.boot:spring-boot-starter'
    implementation 'com.example:lib:1.0'
}
`
	deps := parseBuildGradleVersions(content)
	// The first dep has no version (2 parts only), should still be extracted with empty version.
	// The second has a version.
	if len(deps) < 1 {
		t.Fatalf("got %d deps, want >= 1: %v", len(deps), deps)
	}
	for _, d := range deps {
		if d.Name == "com.example:lib" && d.Version != "1.0" {
			t.Errorf("com.example:lib version = %q, want %q", d.Version, "1.0")
		}
	}
}

func TestParseBuildGradleVersionsEmpty(t *testing.T) {
	deps := parseBuildGradleVersions("")
	if len(deps) != 0 {
		t.Errorf("empty content returned %d deps", len(deps))
	}
}

func TestParseBuildGradleVersionsComments(t *testing.T) {
	content := `dependencies {
    // This is a comment
    implementation 'com.example:lib:1.0'
    /* block comment */ implementation 'com.example:other:2.0'
}
`
	deps := parseBuildGradleVersions(content)
	// Comment-only lines should be skipped, but the block comment line has a dep after it.
	found := false
	for _, d := range deps {
		if d.Name == "com.example:lib" {
			found = true
		}
	}
	if !found {
		t.Error("missing com.example:lib")
	}
}

// ============================================================
// setup.cfg — Python
// ============================================================

func TestParseSetupCfgDeps(t *testing.T) {
	content := `[metadata]
name = mypackage
version = 1.0.0

[options]
install_requires =
    flask>=2.0
    requests>=2.28.0
    numpy
    sqlalchemy==2.0.0

[options.extras_require]
dev =
    pytest
    black
`
	deps, _ := parseSetupCfgDeps(content)
	expected := map[string]bool{
		"flask":      true,
		"requests":   true,
		"numpy":      true,
		"sqlalchemy": true,
	}
	if len(deps) != len(expected) {
		t.Fatalf("parseSetupCfgDeps returned %d deps, want %d: %v", len(deps), len(expected), deps)
	}
	for _, dep := range deps {
		if !expected[dep] {
			t.Errorf("unexpected dep: %q", dep)
		}
	}
}

func TestParseSetupCfgVersions(t *testing.T) {
	content := `[options]
install_requires =
    flask>=2.0
    requests==2.28.0
    numpy
`
	deps := parseSetupCfgVersions(content)
	if len(deps) != 3 {
		t.Fatalf("got %d deps, want 3: %v", len(deps), deps)
	}
	found := map[string]string{}
	for _, d := range deps {
		found[d.Name] = d.Version
		if d.Manager != "pypi" {
			t.Errorf("dep %q manager = %q, want pypi", d.Name, d.Manager)
		}
	}
	if v := found["requests"]; v != "2.28.0" {
		t.Errorf("requests version = %q, want %q", v, "2.28.0")
	}
	if v := found["flask"]; v != "2.0" {
		t.Errorf("flask version = %q, want %q", v, "2.0")
	}
}

func TestParseSetupCfgEmpty(t *testing.T) {
	deps, _ := parseSetupCfgDeps("")
	if len(deps) != 0 {
		t.Errorf("empty content returned %d deps", len(deps))
	}
}

func TestParseSetupCfgNoInstallRequires(t *testing.T) {
	content := `[metadata]
name = simple
version = 0.1
`
	deps, _ := parseSetupCfgDeps(content)
	if len(deps) != 0 {
		t.Errorf("no install_requires returned %d deps", len(deps))
	}
}

// ============================================================
// .csproj — .NET PackageReference format
// ============================================================

func TestParseCsprojDeps(t *testing.T) {
	content := `<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net7.0</TargetFramework>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Newtonsoft.Json" Version="13.0.3" />
    <PackageReference Include="Microsoft.Extensions.Logging" Version="7.0.0" />
    <PackageReference Include="NUnit" Version="3.13.3" />
  </ItemGroup>
</Project>
`
	deps, _ := parseCsprojDeps(content)
	expected := map[string]bool{
		"Newtonsoft.Json":              true,
		"Microsoft.Extensions.Logging": true,
		"NUnit":                        true,
	}
	if len(deps) != len(expected) {
		t.Fatalf("parseCsprojDeps returned %d deps, want %d: %v", len(deps), len(expected), deps)
	}
	for _, dep := range deps {
		if !expected[dep] {
			t.Errorf("unexpected dep: %q", dep)
		}
	}
}

func TestParseCsprojVersions(t *testing.T) {
	content := `<Project Sdk="Microsoft.NET.Sdk">
  <ItemGroup>
    <PackageReference Include="Newtonsoft.Json" Version="13.0.3" />
    <PackageReference Include="NUnit" Version="3.13.3" />
  </ItemGroup>
</Project>
`
	deps := parseCsprojVersions(content)
	if len(deps) != 2 {
		t.Fatalf("got %d deps, want 2: %v", len(deps), deps)
	}
	found := map[string]string{}
	for _, d := range deps {
		found[d.Name] = d.Version
		if d.Manager != "nuget" {
			t.Errorf("dep %q manager = %q, want nuget", d.Name, d.Manager)
		}
	}
	if v := found["Newtonsoft.Json"]; v != "13.0.3" {
		t.Errorf("Newtonsoft.Json = %q, want %q", v, "13.0.3")
	}
}

func TestParseCsprojEmpty(t *testing.T) {
	deps, _ := parseCsprojDeps("")
	if len(deps) != 0 {
		t.Errorf("empty returned %d deps", len(deps))
	}
}

// Verify .csproj is picked up by extension-based matching.
func TestCsprojInManifestFiles(t *testing.T) {
	// .csproj files use extension-based detection, not the manifestFiles map.
	// Verify the extension list includes it.
	found := false
	for _, ext := range manifestExtensions {
		if ext == ".csproj" {
			found = true
		}
	}
	if !found {
		t.Error("manifestExtensions should include .csproj")
	}
}
