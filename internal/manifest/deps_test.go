// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"testing"
)

// TestParsePyprojectPEP621 verifies that pyproject.toml files using PEP 621
// [project] dependencies array format are parsed correctly. This is the format
// used by most modern Python projects (including Augur itself).
func TestParsePyprojectPEP621(t *testing.T) {
	content := `[build-system]
requires = ["setuptools>=61", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "myapp"
requires-python = ">=3.10"
dependencies = [
    "flask==2.0.2",
    "requests~=2.32",
    "numpy==1.26.0",
    "celery~=5.5",
    "click~=8.1",
]

[dependency-groups]
dev = [
    "pytest",
    "mypy>=1.18.2",
]
`
	deps, _ := parsePyprojectDeps(content)
	expected := map[string]bool{
		"flask":    true,
		"requests": true,
		"numpy":    true,
		"celery":   true,
		"click":    true,
	}
	if len(deps) != len(expected) {
		t.Fatalf("parsePyprojectDeps returned %d deps, want %d: %v", len(deps), len(expected), deps)
	}
	for _, dep := range deps {
		if !expected[dep] {
			t.Errorf("unexpected dep: %q", dep)
		}
		delete(expected, dep)
	}
	for name := range expected {
		t.Errorf("missing dep: %q", name)
	}
}

// TestParsePyprojectPoetry verifies that pyproject.toml files using Poetry
// [tool.poetry.dependencies] format are still parsed correctly.
func TestParsePyprojectPoetry(t *testing.T) {
	content := `[tool.poetry]
name = "myapp"

[tool.poetry.dependencies]
python = "^3.10"
flask = "^2.0"
requests = "^2.28"
numpy = "^1.24"
`
	deps, _ := parsePyprojectDeps(content)
	expected := map[string]bool{
		"flask":    true,
		"requests": true,
		"numpy":    true,
	}
	if len(deps) != len(expected) {
		t.Fatalf("parsePyprojectDeps returned %d deps, want %d: %v", len(deps), len(expected), deps)
	}
	for _, dep := range deps {
		if !expected[dep] {
			t.Errorf("unexpected dep: %q", dep)
		}
	}
}

// TestParsePyprojectVersionsPEP621 verifies version extraction from PEP 621 format.
func TestParsePyprojectVersionsPEP621(t *testing.T) {
	content := `[project]
name = "myapp"
dependencies = [
    "flask==2.0.2",
    "requests~=2.32",
    "numpy==1.26.0",
    "celery~=5.5",
    "tomli>=2.2.1 ; python_full_version < '3.11'",
]
`
	deps := ParsePyprojectVersionsFromContent(content)
	if len(deps) < 4 {
		t.Fatalf("parsePyprojectVersionsFromContent returned %d deps, want >= 4: %v", len(deps), deps)
	}

	found := map[string]string{}
	for _, d := range deps {
		found[d.Name] = d.Version
		if d.Manager != "pypi" {
			t.Errorf("dep %q has manager %q, want pypi", d.Name, d.Manager)
		}
	}

	if v, ok := found["flask"]; !ok || v != "2.0.2" {
		t.Errorf("flask version = %q, want %q (found=%v)", v, "2.0.2", ok)
	}
	if v, ok := found["numpy"]; !ok || v != "1.26.0" {
		t.Errorf("numpy version = %q, want %q (found=%v)", v, "1.26.0", ok)
	}
	// tomli has environment marker — should still be parsed
	if _, ok := found["tomli"]; !ok {
		t.Errorf("tomli not found in deps")
	}
}

// TestParseSetupPy verifies that setup.py install_requires are extracted.
func TestParseSetupPy(t *testing.T) {
	content := `from setuptools import setup

setup(
    name='mypackage',
    version='1.0.0',
    install_requires=[
        'flask>=2.0',
        'requests>=2.28.0',
        'numpy',
        'sqlalchemy==2.0.0',
    ],
    extras_require={
        'dev': ['pytest', 'black'],
    },
)
`
	deps, _ := parseSetupPyDeps(content)
	expected := map[string]bool{
		"flask":      true,
		"requests":   true,
		"numpy":      true,
		"sqlalchemy": true,
	}
	if len(deps) != len(expected) {
		t.Fatalf("parseSetupPyDeps returned %d deps, want %d: %v", len(deps), len(expected), deps)
	}
	for _, dep := range deps {
		if !expected[dep] {
			t.Errorf("unexpected dep: %q", dep)
		}
	}
}

// TestParseSetupPyVersions verifies version extraction from setup.py.
func TestParseSetupPyVersions(t *testing.T) {
	content := `from setuptools import setup

setup(
    name='mypackage',
    install_requires=[
        'flask>=2.0',
        'requests==2.28.0',
        'numpy',
    ],
)
`
	deps := parseSetupPyVersions(content)
	if len(deps) != 3 {
		t.Fatalf("parseSetupPyVersions returned %d deps, want 3: %v", len(deps), deps)
	}
	found := map[string]string{}
	for _, d := range deps {
		found[d.Name] = d.Version
		if d.Manager != "pypi" {
			t.Errorf("dep %q has manager %q, want pypi", d.Name, d.Manager)
		}
	}
	if v := found["requests"]; v != "2.28.0" {
		t.Errorf("requests version = %q, want %q", v, "2.28.0")
	}
	if v := found["flask"]; v != "2.0" {
		t.Errorf("flask version = %q, want %q", v, "2.0")
	}
}

// TestParsePipfileDeps verifies Pipfile dependency extraction.
func TestParsePipfileDeps(t *testing.T) {
	content := `[[source]]
url = "https://pypi.org/simple"
verify_ssl = true
name = "pypi"

[packages]
flask = "==2.0.2"
requests = "~=2.28"
numpy = "*"
sqlalchemy = {version = "==2.0.0", extras = ["asyncio"]}

[dev-packages]
pytest = "*"
black = "==23.0"

[requires]
python_version = "3.10"
`
	deps, _ := parsePipfileDeps(content)
	expected := map[string]bool{
		"flask":      true,
		"requests":   true,
		"numpy":      true,
		"sqlalchemy": true,
	}
	if len(deps) != len(expected) {
		t.Fatalf("parsePipfileDeps returned %d deps, want %d: %v", len(deps), len(expected), deps)
	}
	for _, dep := range deps {
		if !expected[dep] {
			t.Errorf("unexpected dep: %q", dep)
		}
	}
}

// TestParsePipfileVersions verifies Pipfile version extraction.
func TestParsePipfileVersions(t *testing.T) {
	content := `[packages]
flask = "==2.0.2"
requests = "~=2.28"
numpy = "*"

[dev-packages]
pytest = "*"
`
	deps := parsePipfileVersions(content)
	if len(deps) < 3 {
		t.Fatalf("parsePipfileVersions returned %d deps, want >= 3: %v", len(deps), deps)
	}
	found := map[string]string{}
	for _, d := range deps {
		found[d.Name] = d.Version
	}
	if v := found["flask"]; v != "2.0.2" {
		t.Errorf("flask version = %q, want %q", v, "2.0.2")
	}
}

// TestEveryManifestFileHasParser: every roster kind the name inventory counts
// has a name reader, except the ones that are recognised and deliberately not
// read (lockfiles, whose primary manifest is read instead, and build-system
// files that show an ecosystem is present but cannot be reliably parsed for
// names).
func TestEveryManifestFileHasParser(t *testing.T) {
	unread := map[string]bool{
		"yarn.lock": true, "go.sum": true, "poetry.lock": true, "Cargo.lock": true, "Gemfile.lock": true,
		"Makefile": true, "CMakeLists.txt": true, "stack.yaml": true,
	}
	examined := 0
	for _, k := range Roster() {
		if k.Language == "" {
			continue
		}
		examined++
		if hasReader := k.names != nil; hasReader == unread[k.Name] {
			t.Errorf("roster kind %q: name reader present = %v, on the deliberately-unread list = %v", k.Name, hasReader, unread[k.Name])
		}
	}
	if examined < 25 {
		t.Errorf("examined %d roster kinds with a language; the roster shrank", examined)
	}
}

// TestParseDependencyFileCoverage: each manifest that declares dependencies is
// a roster kind with both readers (names for the inventory, versioned
// declarations for libyear).
func TestParseDependencyFileCoverage(t *testing.T) {
	byName := map[string]Kind{}
	for _, k := range Roster() {
		byName[k.Name] = k
	}
	for _, name := range []string{
		"package.json", "requirements.txt", "go.mod", "Cargo.toml", "pyproject.toml", "Gemfile", "pom.xml",
		"build.gradle", "build.gradle.kts", "composer.json", "build.sbt", "packages.config", "package.yaml",
		"mix.exs", "pubspec.yaml", "Package.swift", "setup.py", "setup.cfg", "Pipfile", "Directory.Packages.props", "*.csproj",
	} {
		k, ok := byName[name]
		if !ok || k.names == nil || k.declared == nil || k.Ecosystem == "" || k.Language == "" {
			t.Errorf("roster kind %q: present %v, names %v, declared %v, ecosystem %q, language %q; want all",
				name, ok, k.names != nil, k.declared != nil, k.Ecosystem, k.Language)
		}
	}
}

// TestParsePyprojectPEP621WithEnvironmentMarkers verifies that deps with
// environment markers (e.g., '; python_version < "3.11"') are still extracted.
func TestParsePyprojectPEP621WithEnvironmentMarkers(t *testing.T) {
	content := `[project]
dependencies = [
    "tomli>=2.2.1 ; python_full_version < '3.11'",
    "importlib-metadata>=4.0 ; python_version < '3.10'",
    "flask==2.0.2",
]
`
	deps, _ := parsePyprojectDeps(content)
	expected := map[string]bool{
		"tomli":              true,
		"importlib-metadata": true,
		"flask":              true,
	}
	if len(deps) != len(expected) {
		t.Fatalf("got %d deps, want %d: %v", len(deps), len(expected), deps)
	}
	for _, dep := range deps {
		if !expected[dep] {
			t.Errorf("unexpected dep: %q", dep)
		}
	}
}

// TestParseDirectoryPackagesProps verifies .NET central package management parsing.
func TestParseDirectoryPackagesProps(t *testing.T) {
	content := `<Project>
  <ItemGroup>
    <PackageVersion Include="Newtonsoft.Json" Version="13.0.3" />
    <PackageVersion Include="NUnit" Version="3.13.3" />
    <PackageVersion Include="Microsoft.Extensions.Logging" Version="7.0.0" />
  </ItemGroup>
</Project>
`
	deps, _ := parseDirectoryPackagesProps(content)
	if len(deps) != 3 {
		t.Fatalf("parseDirectoryPackagesProps returned %d deps, want 3: %v", len(deps), deps)
	}
	expected := map[string]bool{
		"Newtonsoft.Json":              true,
		"NUnit":                        true,
		"Microsoft.Extensions.Logging": true,
	}
	for _, dep := range deps {
		if !expected[dep] {
			t.Errorf("unexpected dep: %q", dep)
		}
	}
}

// TestParseDirectoryPackagesPropsVersions verifies .NET version extraction.
func TestParseDirectoryPackagesPropsVersions(t *testing.T) {
	content := `<Project>
  <ItemGroup>
    <PackageVersion Include="Newtonsoft.Json" Version="13.0.3" />
    <PackageVersion Include="NUnit" Version="3.13.3" />
  </ItemGroup>
</Project>
`
	deps := parseDirectoryPackagesPropsVersions(content)
	if len(deps) != 2 {
		t.Fatalf("got %d deps, want 2: %v", len(deps), deps)
	}
	found := map[string]string{}
	for _, d := range deps {
		found[d.Name] = d.Version
	}
	if v := found["Newtonsoft.Json"]; v != "13.0.3" {
		t.Errorf("Newtonsoft.Json version = %q, want %q", v, "13.0.3")
	}
}
