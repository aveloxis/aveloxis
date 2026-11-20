// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
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
		{"constraints.in", "/r/requirements/constraints.in", "", false},
		// The properties docs/architecture/analysis.md states (v0.29.57
		// review round 8): variant NAMES match in any casing, the
		// requirements DIRECTORY is byte-exact, requirements.txt is excluded
		// in every casing, and only the immediate parent counts.
		{"Requirements-Dev.txt", "/r/Requirements-Dev.txt", model.ScopeDev, true},
		{"REQUIREMENTS-DEV.TXT", "/r/REQUIREMENTS-DEV.TXT", model.ScopeDev, true},
		{"base.txt", "/r/Requirements/base.txt", "", false},
		{"Requirements.txt", "/r/Requirements.txt", "", false},
		{"REQUIREMENTS.TXT", "/r/requirements/REQUIREMENTS.TXT", "", false},
		{"base.txt", "/r/requirements/prod/base.txt", "", false},
	}
	for _, c := range cases {
		scope, ok := requirementsFileScope(c.base, c.path)
		if ok != c.wantOK || scope != c.wantScope {
			t.Errorf("manifest.RequirementsFileScope(%q, %q) = (%q, %v), want (%q, %v)",
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
test-slow = [
    "pytest-timeout>=2.3",
]
typecheck = [
    "mypy>=1.10",
]

[tool.poetry.group.dev.dependencies]
black = "^24.0"
"zope.interface" = "^6.0"
ruamel.yaml = "^0.18"
faker2 = "^25.0"   # trailing comment

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
	// A multiline group keeps the group's own scope: the continuation
	// items are not [dependency-groups]' default (Copilot on PR #210).
	if got := scopeOf(t, deps, "pytest-timeout"); got != model.ScopeTest {
		t.Errorf("PEP 735 multiline test-slow group → %q, want test", got)
	}
	if got := scopeOf(t, deps, "mypy"); got != model.ScopeDev {
		t.Errorf("PEP 735 multiline typecheck group → %q, want dev", got)
	}
	if got := scopeOf(t, deps, "black"); got != model.ScopeDev {
		t.Errorf("poetry group.dev → %q, want dev", got)
	}
	// A quoted TOML key is the package name without its quotes — the
	// shape splitTOMLDottedKey exists for (zope.interface, ruamel.yaml).
	if got := scopeOf(t, deps, "zope.interface"); got != model.ScopeDev {
		t.Errorf("poetry group.dev quoted key → %q, want dev", got)
	}
	// The same name written UNQUOTED is how these appear in the wild, and
	// it was collected before v0.29.57 touched this branch: keep it.
	if got := scopeOf(t, deps, "ruamel.yaml"); got != model.ScopeDev {
		t.Errorf("poetry group.dev unquoted dotted key → %q, want dev", got)
	}
	// A trailing comment is not part of the version (it reaches the purl).
	// scopeOf fails if the dep is missing, so this cannot pass vacuously.
	if got := scopeOf(t, deps, "faker2"); got != model.ScopeDev {
		t.Errorf("poetry group.dev with a trailing comment → %q, want dev", got)
	}
	for _, d := range deps {
		if d.Name == "faker2" && d.Version != "25.0" {
			t.Errorf("poetry group.dev version with a trailing comment → %q, want 25.0", d.Version)
		}
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

// TestDevBuildAdditionsAreKnobGated pins that every P2 addition in
// the scanLibyear walk is inside an `if ac.DevBuildDeps` guard — the
// knob-off row set must stay byte-identical to pre-v0.27.45. The Go
// C1 relabel is deliberately NOT gated (relabel only, no new rows).
func TestDevBuildAdditionsAreKnobGated(t *testing.T) {
	// Behaviour, through the roster (0.29.153; this read analysis.go for an
	// `ac.DevBuildDeps` guard near each call). For each manifest with a
	// dev/build addition: knob off returns exactly the runtime declarations,
	// knob on returns those plus the addition; and a requirements-variant
	// file declares nothing at all with the knob off.
	dir := t.TempDir()
	cases := []struct {
		file, body, runtime, added string
	}{
		{"pyproject.toml", "[project]\ndependencies = [\"httpx>=0.25\"]\n[project.optional-dependencies]\ndev = [\"ruff\"]\n", "httpx", "ruff"},
		// One keyword per line: the setup.py reader is line-based, and with
		// both keywords on one line it reads the extras (and the extra's
		// name) as runtime requirements. That is today's behaviour and not
		// this test's subject; it is noted in the 0.29.153 ledger entry.
		{"setup.py", "setup(\n    install_requires=['click>=8'],\n    extras_require={'dev': ['black']},\n)\n", "click", "black"},
		{"Pipfile", "[packages]\ndjango = \"==4.2\"\n[dev-packages]\nmypy = \"*\"\n", "django", "mypy"},
		{"setup.cfg", "[options]\ninstall_requires =\n    attrs>=23\n[options.extras_require]\ntest =\n    pytest\n", "attrs", "pytest"},
		{"requirements-dev.txt", "flake8==7.0.0\n", "", "flake8"},
	}
	names := func(deps []model.LibyearDep) string {
		var out []string
		for _, d := range deps {
			out = append(out, d.Name)
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	for _, c := range cases {
		path := filepath.Join(dir, c.file)
		if err := os.WriteFile(path, []byte(c.body), 0o600); err != nil {
			t.Fatal(err)
		}
		walk := NewWalk(dir, nil)
		off, err := walk.Declared(path, Options{})
		if err != nil || names(off) != c.runtime {
			t.Errorf("%s, knob off: declared %q (%v), want %q: the knob-off row set must not change", c.file, names(off), err, c.runtime)
		}
		on, err := walk.Declared(path, Options{DevBuildDeps: true})
		want := strings.Trim(strings.Join(sortedPair(c.runtime, c.added), ","), ",")
		if err != nil || names(on) != want {
			t.Errorf("%s, knob on: declared %q (%v), want %q", c.file, names(on), err, want)
		}
		// The other knob adds nothing here.
		if other, _ := walk.Declared(path, Options{GitHubActionsDeps: true}); names(other) != c.runtime {
			t.Errorf("%s, the Actions knob alone: declared %q, want %q", c.file, names(other), c.runtime)
		}
	}
}

// sortedPair returns the non-empty ones of a and b, sorted.
func sortedPair(a, b string) []string {
	var out []string
	for _, s := range []string{a, b} {
		if s != "" {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// TestRequirementsExtrasStripped pins the local-canary catch
// (2026-07-21): PEP 508 extras must never survive into the dep name —
// anyio[trio] 404s every registry and matches no OSV purl.
func TestRequirementsExtrasStripped(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "requirements.txt")
	if err := os.WriteFile(path, []byte("anyio[trio]==4.0.0\nhttpx[http2,brotli]>=0.27\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	deps := ParseRequirementsTxtVersions(path)
	if len(deps) != 2 {
		t.Fatalf("want 2 deps, got %d", len(deps))
	}
	if deps[0].Name != "anyio" || deps[0].Version != "4.0.0" {
		t.Errorf("extras must strip: got name=%q version=%q, want anyio 4.0.0", deps[0].Name, deps[0].Version)
	}
	if deps[1].Name != "httpx" {
		t.Errorf("multi-extras must strip: got %q, want httpx", deps[1].Name)
	}
}

// TestCasedRequirementsFileIsCollected (worklist §4, v0.29.68; through
// v0.29.67 it pinned the opposite): `Requirements.txt` and
// `REQUIREMENTS.TXT` are the requirements file at every dispatch — the
// inventory lookup, ParseDependencyFile and the libyear walk all compare
// through canonicalManifestName — and the variant arm still leaves the
// plain name to them. Every OTHER manifest name stays byte-exact
// (`gemfile` is not `Gemfile`): the requirements family is the decided class.
// The libyear walk (scanLibyear) needs a store to drive, so its dispatch is
// pinned at the source below (batch 7b review round 1: reverting only that
// operand left the package green).
func TestCasedRequirementsFileIsCollected(t *testing.T) {
	for _, name := range []string{"Requirements.txt", "REQUIREMENTS.TXT", "requirements.txt"} {
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, []byte("flask==2.0.0\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		// The name inventory ...
		lang, names, matched, err := Names(path)
		if err != nil || !matched || lang != "Python" || len(names) != 1 || names[0] != "flask" {
			t.Errorf("manifest.Names(%q) = (%q, %v, %v, %v), want Python [flask]", name, lang, names, matched, err)
		}
		// ... and the libyear walk's dispatch, which needed a store to drive
		// until the roster (0.29.153) and was pinned at the source instead.
		// Knobs off: only the plain-file kind may read it.
		deps, err := NewWalk(filepath.Dir(path), nil).Declared(path, Options{})
		if err != nil || len(deps) != 1 || deps[0].Name != "flask" || deps[0].Version != "2.0.0" {
			t.Errorf("Declared(%q) = (%+v, %v), want flask 2.0.0", name, deps, err)
		}
		// With the dev/build knob on it is still read once, as the plain
		// file, not a second time as a variant.
		if deps, _ := NewWalk(filepath.Dir(path), nil).Declared(path, Options{DevBuildDeps: true}); len(deps) != 1 {
			t.Errorf("Declared(%q) with DevBuildDeps = %+v, want the one declaration once", name, deps)
		}
		if scope, ok := requirementsFileScope(name, "/r/"+name); ok {
			t.Errorf("manifest.RequirementsFileScope(%q) = (%q, true) — the plain file belongs to the walks, not the variant arm", name, scope)
		}
	}
	if canonicalManifestName("gemfile") != "gemfile" || canonicalManifestName("Cargo.toml") != "Cargo.toml" {
		t.Error("canonicalManifestName must leave every other name byte-exact: the mixed-case keys are the real names")
	}
	// One dispatch is left, and both walks go through it.
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/manifest", "func kindsFor("))
	if !strings.Contains(body, "base = canonicalManifestName(filepath.Base(path))") || !strings.Contains(body, "byName[base]") {
		t.Error("the roster's lookup must key on CanonicalManifestName(filepath.Base(path)): a byte-exact key skips Requirements.txt")
	}
}

// A multiline array's CLOSING line can carry brackets of its own — an extra
// (`"pytest[all]>=7.0"]`) or a comment (`]  # see [docs]`) — and they are not
// array syntax. Detecting them as such leaves the walk believing it is still
// inside the array, and the scope it pinned for that array's items then
// follows every group after it: the `dev` group below came back as test
// (v0.29.57). That is the leak this test pins, and it is prevented by the
// quote-aware bracket scan — NOT by `arrayItemScope`, which fixes the other
// direction (a multiline `test` group emitting dev) and is pinned by
// TestPyprojectDevBuildVersions. Naming the wrong mechanism here would send
// the next reader to the wrong function.
func TestGroupScopeEndsWithItsArray(t *testing.T) {
	for _, tc := range []struct {
		name, content, item string
	}{
		{"bracket in an item's extras", "[dependency-groups]\ntest = [\n    \"pytest[all]>=7.0\"]\ndev = [\"ruff>=0.4\"]\n", "pytest"},
		{"bracket in a trailing comment", "[dependency-groups]\ntest = [\n    \"coverage>=7\"\n]   # see [docs]\ndev = [\"ruff>=0.4\"]\n", "coverage"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePyprojectDevBuildVersions(tc.content)
			if scopeOf(t, got, "ruff") != model.ScopeDev {
				t.Errorf("ruff is declared in the dev group but came back %q — the test group's array never closed", scopeOf(t, got, "ruff"))
			}
			// The multiline group's OWN item must keep its scope, or the
			// array could be closing too early instead of too late.
			if scopeOf(t, got, tc.item) != model.ScopeTest {
				t.Errorf("%s is declared in the test group but came back %q", tc.item, scopeOf(t, got, tc.item))
			}
		})
	}
}

// A dotted sub-key names an ATTRIBUTE of the dependency, and only `version`
// holds one. Treating any other sub-key's value as the version invents a
// version for a real package name — `"zope.interface".optional = true`
// became zope.interface@true, which reaches the purl and the OSV lookup
// (SR-6: never fabricate). The same rule the shared table scanner applies.
func TestNonVersionDottedSubKeyIsNotAVersion(t *testing.T) {
	content := "[tool.poetry.dependencies]\n\"zope.interface\".version  = \"^6.0\"\n\"zope.interface\".optional = true\n"
	for _, d := range parsePoetryVersions(content) {
		if d.Version == "true" || d.Version == "false" {
			t.Errorf("%s came back at version %q — that is the value of a non-version sub-key, not a release", d.Name, d.Version)
		}
	}
	// The version sub-key still works: this is the line the round before
	// this one was added for.
	found := false
	for _, d := range parsePoetryVersions(content) {
		if d.Name == "zope.interface" && d.Version == "6.0" {
			found = true
		}
	}
	if !found {
		t.Error(`"zope.interface".version = "^6.0" must still yield zope.interface@6.0`)
	}
}

// The `=` that selects this branch is looked for in the RAW line, but the
// value is split from the comment-stripped one, so a line whose only `=` is
// inside a comment reaches the split with nothing to split. It must be
// dropped, not indexed into (the FuzzManifestParsers contract: never panic).
//
// The fixture has to carry something BEFORE the comment: a line that starts
// with `#` is rejected by the arm's own `!HasPrefix("#")` test and never
// reaches the guard, so it would prove nothing about it (v0.29.57).
func TestCommentOnlyEqualsIsNotADependency(t *testing.T) {
	for _, content := range []string{
		"[tool.poetry.group.dev.dependencies]\nblack  # = \"^24\"\n",
		"[tool.poetry.group.dev.dependencies]\nruff   #  = \n",
	} {
		if got := parsePyprojectDevBuildVersions(content); len(got) != 0 {
			t.Errorf("parsePyprojectDevBuildVersions(%q) = %+v, want no dependencies: the only `=` is inside a comment", content, got)
		}
	}
}
