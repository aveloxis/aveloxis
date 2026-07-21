// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.45 (summary/19 P2) — behavioral tests for the dev/build
// dependency expansion. Fixtures are shaped from real manifests
// (numpy's pyproject build-system + test extras, a poetry-groups
// project, PEP 735, Pipfile dev-packages) per the ground-truth rule.

package collector

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

func TestRequirementsFileScope(t *testing.T) {
	cases := []struct {
		base, path string
		wantScope  string
		wantOK     bool
	}{
		{"requirements.txt", "/r/requirements.txt", "", false}, // exact name owned by the walk's dedicated case
		{"requirements-dev.txt", "/r/requirements-dev.txt", model.ScopeDev, true},
		{"requirements_dev.txt", "/r/requirements_dev.txt", model.ScopeDev, true},
		{"test_requirements.txt", "/r/test_requirements.txt", model.ScopeTest, true},
		{"requirements-test.txt", "/r/requirements-test.txt", model.ScopeTest, true},
		{"build_requirements.txt", "/r/build_requirements.txt", model.ScopeBuild, true},
		{"requirements-ci.txt", "/r/requirements-ci.txt", model.ScopeBuild, true},
		{"requirements-docs.txt", "/r/requirements-docs.txt", model.ScopeDev, true},
		{"lint-requirements.txt", "/r/lint-requirements.txt", model.ScopeDev, true},
		{"requirements-prod.txt", "/r/requirements-prod.txt", "runtime", true},
		// requirements/ directory: bare names classify by token, else runtime
		{"test.txt", "/r/requirements/test.txt", model.ScopeTest, true},
		{"ci.txt", "/r/requirements/ci.txt", model.ScopeBuild, true},
		{"default.txt", "/r/requirements/default.txt", "runtime", true},
		// non-variants
		{"readme.txt", "/r/readme.txt", "", false},
		{"notes.txt", "/r/docs/notes.txt", "", false},
		{"requirements.in", "/r/requirements.in", "", false}, // only .txt
	}
	for _, c := range cases {
		scope, ok := requirementsFileScope(c.base, c.path)
		if ok != c.wantOK || scope != c.wantScope {
			t.Errorf("requirementsFileScope(%q, %q) = (%q, %v), want (%q, %v)",
				c.base, c.path, scope, ok, c.wantScope, c.wantOK)
		}
	}
}

func TestRequirementsScopedParserStampsScope(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "requirements-dev.txt")
	if err := os.WriteFile(path, []byte("ruff==0.4.4\nmypy>=1.10\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	deps := parseRequirementsTxtVersionsScoped(path, model.ScopeDev)
	if len(deps) != 2 {
		t.Fatalf("want 2 deps, got %d", len(deps))
	}
	for _, d := range deps {
		if d.Type != model.ScopeDev {
			t.Errorf("dep %s scope %q, want dev", d.Name, d.Type)
		}
	}
}

func TestPyprojectDevBuildVersions(t *testing.T) {
	// numpy-shaped: meson build backend + test/doc extras, plus PEP
	// 735 and poetry groups for the other families.
	toml := `[build-system]
build-backend = "mesonpy"
requires = [
    "meson-python>=0.15.0",
    "Cython>=3.0.6",
]

[project]
name = "demo"
dependencies = ["runtime-dep>=1.0"]

[project.optional-dependencies]
test = [
    "pytest==7.4.0",
    "hypothesis>=6.81",
]
doc = ["sphinx>=7.2"]

[dependency-groups]
dev = ["ruff>=0.4"]
test = ["coverage>=7.0"]

[tool.poetry.group.dev.dependencies]
black = "^24.0"

[tool.poetry.group.test.dependencies]
faker = "^25.0"

[tool.poetry.dev-dependencies]
flake8 = "^7.0"
`
	deps := parsePyprojectDevBuildVersions(toml)
	if got := scopeOf(t, deps, "meson-python"); got != model.ScopeBuild {
		t.Errorf("[build-system].requires meson-python → %q, want build", got)
	}
	if got := scopeOf(t, deps, "Cython"); got != model.ScopeBuild {
		t.Errorf("[build-system].requires Cython → %q, want build", got)
	}
	if got := scopeOf(t, deps, "pytest"); got != model.ScopeOptional {
		t.Errorf("[project.optional-dependencies] → %q, want optional", got)
	}
	if got := scopeOf(t, deps, "sphinx"); got != model.ScopeOptional {
		t.Errorf("[project.optional-dependencies] doc → %q, want optional", got)
	}
	if got := scopeOf(t, deps, "ruff"); got != model.ScopeDev {
		t.Errorf("PEP 735 dev group → %q, want dev", got)
	}
	if got := scopeOf(t, deps, "coverage"); got != model.ScopeTest {
		t.Errorf("PEP 735 test group → %q, want test", got)
	}
	if got := scopeOf(t, deps, "black"); got != model.ScopeDev {
		t.Errorf("poetry group.dev → %q, want dev", got)
	}
	if got := scopeOf(t, deps, "faker"); got != model.ScopeTest {
		t.Errorf("poetry group.test → %q, want test", got)
	}
	if got := scopeOf(t, deps, "flake8"); got != model.ScopeDev {
		t.Errorf("legacy [tool.poetry.dev-dependencies] → %q, want dev", got)
	}
	// The runtime dep from [project] must NOT be re-collected here —
	// this parser is additive to the base parser.
	for _, d := range deps {
		if d.Name == "runtime-dep" {
			t.Error("parsePyprojectDevBuildVersions must not collect [project].dependencies — the base parser owns those")
		}
	}
}

func TestPipfileDevPackages(t *testing.T) {
	pipfile := `[[source]]
url = "https://pypi.org/simple"

[packages]
requests = "==2.31.0"

[dev-packages]
pytest = "==7.4.0"
black = {version = "==24.4.2", extras = ["d"]}
`
	deps := parsePipfileDevPackages(pipfile)
	if got := scopeOf(t, deps, "pytest"); got != model.ScopeDev {
		t.Errorf("[dev-packages] pytest → %q, want dev", got)
	}
	if got := scopeOf(t, deps, "black"); got != model.ScopeDev {
		t.Errorf("[dev-packages] table-value black → %q, want dev", got)
	}
	for _, d := range deps {
		if d.Name == "requests" {
			t.Error("parsePipfileDevPackages must not collect [packages] — the base parser owns those")
		}
	}
	// The table-form version must survive the section re-wrap.
	for _, d := range deps {
		if d.Name == "black" && d.Version != "24.4.2" {
			t.Errorf("black version = %q, want 24.4.2", d.Version)
		}
	}
}

func TestSetupPyDevBuildVersions(t *testing.T) {
	setup := `from setuptools import setup

setup(
    name="demo",
    install_requires=["numpy>=1.21"],
    tests_require=[
        "pytest>=6.0",
        "mock",
    ],
    extras_require={
        "test": ["hypothesis>=6.0"],
        "docs": ["sphinx>=4.0", "furo"],
    },
)`
	deps := parseSetupPyDevBuildVersions(setup)
	if got := scopeOf(t, deps, "pytest"); got != model.ScopeTest {
		t.Errorf("tests_require → %q, want test", got)
	}
	if got := scopeOf(t, deps, "hypothesis"); got != model.ScopeOptional {
		t.Errorf("extras_require → %q, want optional", got)
	}
	if got := scopeOf(t, deps, "sphinx"); got != model.ScopeOptional {
		t.Errorf("extras_require docs → %q, want optional", got)
	}
	for _, d := range deps {
		if d.Name == "numpy" {
			t.Error("parseSetupPyDevBuildVersions must not collect install_requires — the base parser owns those")
		}
		// Dict KEYS ("test", "docs") must never parse as dep names.
		if d.Name == "test" || d.Name == "docs" {
			t.Errorf("extras_require dict key %q parsed as a dependency", d.Name)
		}
	}
}

func TestSetupCfgExtrasVersions(t *testing.T) {
	cfg := `[metadata]
name = demo

[options]
install_requires =
    numpy>=1.21

[options.extras_require]
test =
    pytest>=6.0
    coverage
docs = sphinx>=4.0
`
	deps := parseSetupCfgExtrasVersions(cfg)
	if got := scopeOf(t, deps, "pytest"); got != model.ScopeOptional {
		t.Errorf("[options.extras_require] multi-line → %q, want optional", got)
	}
	if got := scopeOf(t, deps, "sphinx"); got != model.ScopeOptional {
		t.Errorf("[options.extras_require] inline → %q, want optional", got)
	}
	for _, d := range deps {
		if d.Name == "numpy" {
			t.Error("parseSetupCfgExtrasVersions must not collect [options] install_requires")
		}
	}
}

func TestClassifyGoModTestOnlyDeps(t *testing.T) {
	dir := t.TempDir()
	mustWrite := func(rel, content string) {
		t.Helper()
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mustWrite("main.go", `package main

import (
	"fmt"

	"github.com/example/runtimemod/pkg"
)

func main() { fmt.Println(pkg.V) }
`)
	mustWrite("main_test.go", `package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/example/runtimemod/pkg"
)

func TestMain2(t *testing.T) { assert.Equal(t, pkg.V, pkg.V) }
`)
	deps := []libyearDep{
		{Name: "github.com/example/runtimemod", Version: "v1.0.0", Type: "runtime", Manager: "go"},
		{Name: "github.com/stretchr/testify", Version: "v1.9.0", Type: "runtime", Manager: "go"},
		{Name: "github.com/example/indirect", Version: "v0.3.0", Type: "runtime", Manager: "go"},
		{Name: "serde", Version: "1.0", Type: "runtime", Manager: "cargo"},
	}
	out := classifyGoModTestOnlyDeps(dir, deps)
	if got := scopeOf(t, out, "github.com/example/runtimemod"); got != "runtime" {
		t.Errorf("module imported from both test and non-test files → %q, want runtime", got)
	}
	if got := scopeOf(t, out, "github.com/stretchr/testify"); got != model.ScopeTest {
		t.Errorf("module imported ONLY from _test.go → %q, want test (the C1 classification)", got)
	}
	if got := scopeOf(t, out, "github.com/example/indirect"); got != "runtime" {
		t.Errorf("never-imported module → %q, want runtime (absence of evidence is not test evidence)", got)
	}
	if got := scopeOf(t, out, "serde"); got != "runtime" {
		t.Errorf("non-go dep must be untouched, got %q", got)
	}
}

// TestDevBuildAdditionsAreKnobGated pins that every P2 addition in
// the scanLibyear walk is inside an `if ac.DevBuildDeps` guard — the
// knob-off row set must stay byte-identical to pre-v0.27.45. The Go
// C1 relabel is deliberately NOT gated (relabel only, no new rows).
func TestDevBuildAdditionsAreKnobGated(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	for _, call := range []string{
		"parsePyprojectDevBuildVersions(",
		"parseSetupPyDevBuildVersions(",
		"parsePipfileDevPackages(",
		"parseSetupCfgExtrasVersions(",
		"requirementsFileScope(",
	} {
		idx := 0
		found := false
		for {
			at := indexFrom(code, call, idx)
			if at < 0 {
				break
			}
			found = true
			// The guard must appear within the preceding 400 chars.
			start := at - 400
			if start < 0 {
				start = 0
			}
			if !containsGuard(code[start:at]) {
				t.Errorf("call %s at offset %d is not gated on ac.DevBuildDeps — the knob-off row set must stay byte-identical", call, at)
			}
			idx = at + len(call)
		}
		if !found {
			t.Errorf("expected scanLibyear to call %s (knob-gated)", call)
		}
	}
	if !containsGuard(code) {
		t.Error("analysis.go must gate the P2 additions on ac.DevBuildDeps")
	}
}

func indexFrom(s, sub string, from int) int {
	if from >= len(s) {
		return -1
	}
	i := strings.Index(s[from:], sub)
	if i < 0 {
		return -1
	}
	return from + i
}

func containsGuard(s string) bool {
	return strings.Contains(s, "ac.DevBuildDeps")
}
